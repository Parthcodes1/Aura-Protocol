"""
Sui chain watcher using the Sui JSON-RPC API directly via httpx.
Fetches checkpoints (Sui's finality unit) and transaction events.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
from typing import Any, Dict, List, Optional

import httpx
from tenacity import (
    AsyncRetrying,
    RetryError,
    stop_after_attempt,
    wait_exponential,
)

from monitoring.logger import get_logger
from monitoring.metrics import CHAIN_EVENTS_FETCHED

from .base import BaseChainWatcher, ChainBlock, NormalizedEvent

log = get_logger(__name__)


class SuiRpcClient:
    """Minimal async Sui JSON-RPC 2.0 client."""

    def __init__(self, rpc_url: str, timeout: float = 10.0) -> None:
        self._url = rpc_url
        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers={"Content-Type": "application/json"},
        )
        self._id = 0

    async def call(self, method: str, params: List[Any]) -> Any:
        self._id += 1
        body = {
            "jsonrpc": "2.0",
            "id":      self._id,
            "method":  method,
            "params":  params,
        }
        resp = await self._client.post(self._url, content=json.dumps(body))
        resp.raise_for_status()
        result = resp.json()
        if "error" in result:
            raise RuntimeError(f"Sui RPC error: {result['error']}")
        return result.get("result")

    async def close(self) -> None:
        await self._client.aclose()


class SuiWatcher(BaseChainWatcher):
    """
    Watches Sui blockchain by polling the latest checkpoint.
    Maps checkpoints → ChainBlock; SuiEvent → NormalizedEvent.
    """

    def __init__(self, config) -> None:
        super().__init__(
            chain_name="sui",
            poll_interval=config.poll_interval,
            max_retries=config.max_retries,
        )
        self._config = config
        self._rpc: Optional[SuiRpcClient] = None
        self._package_ids = config.package_ids or []

    # ── Connection ────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        self._rpc = SuiRpcClient(
            self._config.rpc_url,
            timeout=self._config.request_timeout,
        )
        # Verify connectivity
        seq = await self._rpc.call("sui_getLatestCheckpointSequenceNumber", [])
        log.info(
            "sui.connected",
            rpc=self._config.rpc_url,
            latest_checkpoint=seq,
        )
        self._last_block = int(seq) - 1

    async def disconnect(self) -> None:
        if self._rpc:
            await self._rpc.close()
            self._rpc = None

    # ── Block / Checkpoint fetching ───────────────────────────────────────────

    async def fetch_latest_block(self) -> Optional[ChainBlock]:
        seq_str = await self._rpc.call("sui_getLatestCheckpointSequenceNumber", [])
        latest = int(seq_str)
        if latest == self._last_block:
            return None

        target = self._last_block + 1
        checkpoint = await self._rpc.call("sui_getCheckpoint", [str(target)])
        if not checkpoint:
            return None

        events = await self._fetch_checkpoint_events(checkpoint)
        CHAIN_EVENTS_FETCHED.labels(chain="sui").inc(len(events))

        # Sui checkpoint timestamp is in ms
        ts_ms = int(checkpoint.get("timestampMs", 0))
        ts_s = ts_ms // 1000

        digest = checkpoint.get("digest", "")

        return ChainBlock(
            chain="sui",
            block_number=target,
            block_hash=digest,
            parent_hash=checkpoint.get("previousDigest", ""),
            timestamp=ts_s,
            tx_count=int(checkpoint.get("networkTotalTransactions", 0)),
            validator=checkpoint.get("validatorSignature", "")[:64],
            events=events,
            raw={
                "epoch":           checkpoint.get("epoch"),
                "sequence_number": checkpoint.get("sequenceNumber"),
                "tx_digests":      (checkpoint.get("transactions") or [])[:20],
            },
        )

    async def fetch_events(
        self,
        from_block: int,
        to_block: int,
        contract_addresses: Optional[List[str]] = None,
    ) -> List[NormalizedEvent]:
        events = []
        for seq in range(from_block, to_block + 1):
            try:
                cp = await self._rpc.call("sui_getCheckpoint", [str(seq)])
                if cp:
                    events.extend(await self._fetch_checkpoint_events(cp))
            except Exception as exc:
                log.warning("sui.fetch_events_failed", seq=seq, error=str(exc))
        return events

    async def _fetch_checkpoint_events(self, checkpoint: Dict[str, Any]) -> List[NormalizedEvent]:
        """Fetch events for all transactions in a checkpoint."""
        events = []
        tx_digests: List[str] = (checkpoint.get("transactions") or [])[:50]

        for tx_digest in tx_digests:
            try:
                tx_events = await self._rpc.call(
                    "suix_getTransactionBlock",
                    [
                        tx_digest,
                        {
                            "showInput":   False,
                            "showEffects": False,
                            "showEvents":  True,
                        },
                    ],
                )
                if tx_events and "events" in tx_events:
                    for raw_evt in (tx_events["events"] or []):
                        norm = self._normalize_event(tx_digest, raw_evt, checkpoint)
                        if norm:
                            events.append(norm)
            except Exception as exc:
                log.debug("sui.tx_events_failed", tx=tx_digest[:12], error=str(exc))

        return events

    def _normalize_event(
        self,
        tx_digest: str,
        raw_evt: Dict[str, Any],
        checkpoint: Dict[str, Any],
    ) -> Optional[NormalizedEvent]:
        try:
            event_type = raw_evt.get("type", "Unknown")
            seq = int(checkpoint.get("sequenceNumber", 0))

            # Try to extract common fields from parsedJson
            parsed = raw_evt.get("parsedJson", {}) or {}
            from_addr  = parsed.get("from")  or raw_evt.get("sender")
            to_addr    = parsed.get("to")
            value_raw  = parsed.get("amount") or parsed.get("value")
            value      = str(value_raw) if value_raw is not None else None

            return NormalizedEvent(
                chain="sui",
                tx_hash=tx_digest,
                event_type=event_type.split("::")[-1] if "::" in event_type else event_type,
                block_number=seq,
                contract_address=raw_evt.get("packageId"),
                from_address=from_addr,
                to_address=to_addr,
                value=value,
                raw_data={
                    "type":       event_type,
                    "sender":     raw_evt.get("sender"),
                    "module":     raw_evt.get("transactionModule"),
                },
            )
        except Exception as exc:
            log.debug("sui.normalize_event_failed", error=str(exc))
            return None
