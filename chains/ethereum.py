"""
Ethereum chain watcher using web3.py (AsyncWeb3).
Fetches blocks + ERC-20/ERC-721 Transfer events via eth_getLogs.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional

from monitoring.logger import get_logger
from monitoring.metrics import CHAIN_EVENTS_FETCHED

from .base import BaseChainWatcher, ChainBlock, NormalizedEvent

log = get_logger(__name__)

# ERC-20 / ERC-721 Transfer event signature
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# ERC-4626 Deposit
DEPOSIT_TOPIC  = "0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7"
# UniswapV2 Swap
SWAP_TOPIC     = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"


def _hex_or_zero(val) -> str:
    try:
        return hex(val) if isinstance(val, int) else str(val)
    except Exception:
        return "0x0"


class EthereumWatcher(BaseChainWatcher):
    """
    Watches Ethereum mainnet (or any EVM-compatible chain).
    Uses eth_getBlockByNumber + eth_getLogs for event collection.
    """

    def __init__(self, config) -> None:
        super().__init__(
            chain_name="ethereum",
            poll_interval=config.poll_interval,
            max_retries=config.max_retries,
        )
        self._config = config
        self._w3 = None
        self._start_block = config.start_block

    # ── Connection ────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        from web3 import AsyncWeb3, AsyncHTTPProvider
        from web3.middleware import ExtraDataToPOAMiddleware

        self._w3 = AsyncWeb3(
            AsyncHTTPProvider(
                self._config.rpc_url,
                request_kwargs={"timeout": self._config.request_timeout},
            )
        )
        # Add PoA middleware for BSC/Polygon/etc
        self._w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

        connected = await self._w3.is_connected()
        if not connected:
            raise ConnectionError(f"Cannot connect to Ethereum RPC: {self._config.rpc_url}")

        chain_id = await self._w3.eth.chain_id
        log.info(
            "ethereum.connected",
            rpc=self._config.rpc_url,
            chain_id=chain_id,
        )

        # Initialize start block if not set
        if self._start_block is None:
            self._last_block = await self._w3.eth.block_number - 1
        else:
            self._last_block = self._start_block - 1

    async def disconnect(self) -> None:
        if self._w3:
            try:
                await self._w3.provider.disconnect()
            except Exception:
                pass
            self._w3 = None

    # ── Block fetching ────────────────────────────────────────────────────────

    async def fetch_latest_block(self) -> ChainBlock:
        latest_num = await self._w3.eth.block_number
        if latest_num == self._last_block:
            return None  # no new block

        target = self._last_block + 1
        raw_block = await self._w3.eth.get_block(target, full_transactions=False)

        events = await self.fetch_events(target, target, self._config.contract_addresses or None)
        CHAIN_EVENTS_FETCHED.labels(chain="ethereum").inc(len(events))

        return ChainBlock(
            chain="ethereum",
            block_number=raw_block.number,
            block_hash=raw_block.hash.hex(),
            parent_hash=raw_block.parentHash.hex(),
            timestamp=raw_block.timestamp,
            tx_count=len(raw_block.transactions),
            gas_used=raw_block.gasUsed,
            events=events,
            raw={
                "miner":      raw_block.miner,
                "difficulty": str(getattr(raw_block, "difficulty", 0)),
                "base_fee":   str(getattr(raw_block, "baseFeePerGas", 0) or getattr(raw_block, "base_fee_per_gas", 0)),
                "extra_data": (lambda e: e.hex() if e else "")(
                    getattr(raw_block, "extraData", None) or getattr(raw_block, "extra_data", None)
                ),
            },
        )

    # ── Event fetching ────────────────────────────────────────────────────────

    async def fetch_events(
        self,
        from_block: int,
        to_block: int,
        contract_addresses: Optional[List[str]] = None,
    ) -> List[NormalizedEvent]:
        filter_params: Dict[str, Any] = {
            "fromBlock": from_block,
            "toBlock":   to_block,
            "topics":    [[TRANSFER_TOPIC, SWAP_TOPIC]],
        }
        if contract_addresses:
            filter_params["address"] = [
                self._w3.to_checksum_address(a) for a in contract_addresses
            ]

        try:
            logs = await self._w3.eth.get_logs(filter_params)
        except Exception as exc:
            log.warning("ethereum.get_logs_failed", error=str(exc), from_block=from_block)
            return []

        events = []
        for raw_log in logs:
            event = self._normalize_log(raw_log)
            if event:
                events.append(event)
        return events

    def _normalize_log(self, raw_log) -> Optional[NormalizedEvent]:
        try:
            topics = raw_log.topics
            topic0 = topics[0].hex() if topics else ""

            if topic0 == TRANSFER_TOPIC and len(topics) >= 3:
                # ERC-20 Transfer(address indexed from, address indexed to, uint256 value)
                from_addr = "0x" + topics[1].hex()[-40:]
                to_addr   = "0x" + topics[2].hex()[-40:]
                value = str(int(raw_log.data.hex() or "0x0", 16)) if raw_log.data else "0"
                return NormalizedEvent(
                    chain="ethereum",
                    tx_hash=raw_log.transactionHash.hex(),
                    event_type="Transfer",
                    block_number=raw_log.blockNumber,
                    log_index=raw_log.logIndex,
                    contract_address=raw_log.address,
                    from_address=from_addr,
                    to_address=to_addr,
                    value=value,
                    raw_data={"topic0": topic0},
                )
            elif topic0 == SWAP_TOPIC:
                return NormalizedEvent(
                    chain="ethereum",
                    tx_hash=raw_log.transactionHash.hex(),
                    event_type="Swap",
                    block_number=raw_log.blockNumber,
                    log_index=raw_log.logIndex,
                    contract_address=raw_log.address,
                    raw_data={"topic0": topic0, "data": raw_log.data.hex()},
                )
            else:
                return NormalizedEvent(
                    chain="ethereum",
                    tx_hash=raw_log.transactionHash.hex(),
                    event_type="Log",
                    block_number=raw_log.blockNumber,
                    log_index=raw_log.logIndex,
                    contract_address=raw_log.address,
                    raw_data={"topic0": topic0},
                )
        except Exception as exc:
            log.warning("ethereum.normalize_log_failed", error=str(exc))
            return None

    # ── Block range fetch (for sync) ──────────────────────────────────────────

    async def fetch_block_range(self, from_block: int, to_block: int) -> List[ChainBlock]:
        """Fetch multiple blocks (used for catch-up on reconnect)."""
        blocks = []
        for num in range(from_block, min(to_block, from_block + 10) + 1):
            try:
                raw = await self._w3.eth.get_block(num, full_transactions=False)
                events = await self.fetch_events(num, num)
                blocks.append(ChainBlock(
                    chain="ethereum",
                    block_number=raw.number,
                    block_hash=raw.hash.hex(),
                    parent_hash=raw.parentHash.hex(),
                    timestamp=raw.timestamp,
                    tx_count=len(raw.transactions),
                    gas_used=raw.gasUsed,
                    events=events,
                ))
            except Exception as exc:
                log.warning("ethereum.range_fetch_failed", block=num, error=str(exc))
        return blocks
