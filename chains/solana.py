"""
Solana chain watcher using solana-py (AsyncClient).
Polls slots and fetches block + program logs.
"""
from __future__ import annotations

import asyncio
import hashlib
import time
from typing import Any, Dict, List, Optional

from monitoring.logger import get_logger
from monitoring.metrics import CHAIN_EVENTS_FETCHED

from .base import BaseChainWatcher, ChainBlock, NormalizedEvent

log = get_logger(__name__)

# Well-known program IDs
TOKEN_PROGRAM_ID     = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM   = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
SYSTEM_PROGRAM_ID    = "11111111111111111111111111111111"


class SolanaWatcher(BaseChainWatcher):
    """
    Watches Solana by polling the latest confirmed slot.
    Uses solana-py AsyncClient for all RPC calls.
    """

    def __init__(self, config) -> None:
        super().__init__(
            chain_name="solana",
            poll_interval=config.poll_interval,
            max_retries=config.max_retries,
        )
        self._config = config
        self._client = None
        self._program_ids = config.program_ids or [TOKEN_PROGRAM_ID]

    # ── Connection ────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        from solana.rpc.async_api import AsyncClient
        from solana.rpc.commitment import Confirmed

        self._client = AsyncClient(
            self._config.rpc_url,
            commitment=self._config.commitment,
            timeout=self._config.request_timeout,
        )

        # Test connection
        resp = await self._client.get_slot()
        if resp.value is None:
            raise ConnectionError(f"Cannot connect to Solana RPC: {self._config.rpc_url}")

        self._last_block = resp.value - 1
        log.info(
            "solana.connected",
            rpc=self._config.rpc_url,
            slot=resp.value,
        )

    async def disconnect(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None

    # ── Slot / Block fetching ─────────────────────────────────────────────────

    async def fetch_latest_block(self) -> Optional[ChainBlock]:
        slot_resp = await self._client.get_slot()
        latest_slot = slot_resp.value
        if latest_slot is None or latest_slot == self._last_block:
            return None

        target_slot = self._last_block + 1

        # Fetch block with transactions
        try:
            block_resp = await self._client.get_block(
                target_slot,
                encoding="json",
                max_supported_transaction_version=0,
            )
        except Exception as exc:
            # Slot may be skipped (leader missed it)
            log.debug("solana.slot_skipped", slot=target_slot, error=str(exc))
            self._last_block = target_slot  # advance anyway
            return None

        raw_block = block_resp.value
        if raw_block is None:
            self._last_block = target_slot
            return None

        events = self._extract_events(target_slot, raw_block)
        CHAIN_EVENTS_FETCHED.labels(chain="solana").inc(len(events))

        # block_time can be None for recent slots
        block_time = getattr(raw_block, "block_time", None) or int(time.time())
        block_hash = getattr(raw_block, "blockhash", "") or ""
        prev_hash  = getattr(raw_block, "previous_blockhash", "") or ""
        txs        = getattr(raw_block, "transactions", []) or []

        return ChainBlock(
            chain="solana",
            block_number=target_slot,
            block_hash=str(block_hash),
            parent_hash=str(prev_hash),
            timestamp=int(block_time),
            tx_count=len(txs),
            events=events,
            raw={"slot": target_slot},
        )

    async def fetch_events(
        self,
        from_block: int,
        to_block: int,
        contract_addresses: Optional[List[str]] = None,
    ) -> List[NormalizedEvent]:
        """Fetch signatures for program(s) and return normalized events."""
        events = []
        programs = contract_addresses or self._program_ids
        for program_id in programs:
            try:
                from solana.rpc.types import TokenAccountOpts
                from solders.pubkey import Pubkey

                pubkey = Pubkey.from_string(program_id)
                sigs_resp = await self._client.get_signatures_for_address(
                    pubkey,
                    limit=50,
                )
                for sig_info in (sigs_resp.value or []):
                    if sig_info.slot and from_block <= sig_info.slot <= to_block:
                        evt = NormalizedEvent(
                            chain="solana",
                            tx_hash=str(sig_info.signature),
                            event_type="ProgramLog",
                            block_number=sig_info.slot,
                            contract_address=program_id,
                            raw_data={
                                "slot": sig_info.slot,
                                "err":  str(sig_info.err) if sig_info.err else None,
                            },
                        )
                        events.append(evt)
            except Exception as exc:
                log.warning("solana.fetch_events_failed", program=program_id, error=str(exc))
        return events

    def _extract_events(self, slot: int, raw_block: Any) -> List[NormalizedEvent]:
        """Extract events from block transactions (SPL token transfers, etc.)."""
        events = []
        txs = getattr(raw_block, "transactions", []) or []

        for tx_wrapper in txs[:100]:  # cap at 100 tx per block
            try:
                tx = getattr(tx_wrapper, "transaction", tx_wrapper)
                meta = getattr(tx_wrapper, "meta", None)
                if meta and getattr(meta, "err", None):
                    continue  # skip failed txs

                # Extract signature
                sig = ""
                if hasattr(tx, "signatures"):
                    sigs = tx.signatures
                    sig = str(sigs[0]) if sigs else ""

                # Extract pre/post token balances for transfer detection
                if meta:
                    pre_balances  = list(getattr(meta, "pre_token_balances",  []) or [])
                    post_balances = list(getattr(meta, "post_token_balances", []) or [])

                    for pre in pre_balances:
                        acct_idx = getattr(pre, "account_index", -1)
                        mint     = str(getattr(pre, "mint", ""))

                        post = next(
                            (p for p in post_balances if getattr(p, "account_index", -1) == acct_idx),
                            None,
                        )
                        if post:
                            pre_amt  = getattr(getattr(pre,  "ui_token_amount", None), "amount", "0") or "0"
                            post_amt = getattr(getattr(post, "ui_token_amount", None), "amount", "0") or "0"
                            delta = int(post_amt) - int(pre_amt)
                            if delta != 0:
                                events.append(NormalizedEvent(
                                    chain="solana",
                                    tx_hash=sig,
                                    event_type="TokenTransfer",
                                    block_number=slot,
                                    token_address=mint,
                                    value=str(abs(delta)),
                                    raw_data={
                                        "account_index": acct_idx,
                                        "mint":          mint,
                                        "delta":         delta,
                                    },
                                ))
            except Exception:
                pass

        return events
