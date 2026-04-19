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

    def _active_package_ids(self, contract_addresses: Optional[List[str]] = None) -> List[str]:
        packages = list(self._package_ids)
        if contract_addresses:
            packages.extend(contract_addresses)
        return list(dict.fromkeys(packages))

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
        packages = self._active_package_ids(contract_addresses)
        for seq in range(from_block, to_block + 1):
            try:
                cp = await self._rpc.call("sui_getCheckpoint", [str(seq)])
                if cp:
                    events.extend(await self._fetch_checkpoint_events(cp, packages))
            except Exception as exc:
                log.warning("sui.fetch_events_failed", seq=seq, error=str(exc))
        return events

    async def _fetch_checkpoint_events(
        self,
        checkpoint: Dict[str, Any],
        package_ids: Optional[List[str]] = None,
    ) -> List[NormalizedEvent]:
        """Fetch events for all transactions in a checkpoint."""
        events = []
        tx_digests: List[str] = list(checkpoint.get("transactions") or [])
        max_txs = int(getattr(self._config, "max_checkpoint_transactions", 0) or 0)
        if max_txs > 0:
            tx_digests = tx_digests[:max_txs]

        if len(tx_digests) != len(checkpoint.get("transactions") or []):
            log.info(
                "sui.checkpoint_tx_scan_limited",
                checkpoint=checkpoint.get("sequenceNumber"),
                scanned=len(tx_digests),
                total=len(checkpoint.get("transactions") or []),
            )

        for tx_digest in tx_digests:
            try:
                raw_events = await self._fetch_tx_events(tx_digest)
                for raw_evt in raw_events:
                    if package_ids and raw_evt.get("packageId") not in package_ids:
                        continue
                    norm = self._normalize_event(tx_digest, raw_evt, checkpoint)
                    if norm:
                        events.append(norm)
            except Exception as exc:
                log.debug("sui.tx_events_failed", tx=tx_digest[:12], error=str(exc))

        if not events:
            log.info(
                "sui.checkpoint_no_events",
                checkpoint=checkpoint.get("sequenceNumber"),
                txs=len(tx_digests),
                packages=package_ids or [],
            )

        return events

    async def _fetch_tx_events(self, tx_digest: str) -> List[Dict[str, Any]]:
        """Fetch raw events for a single transaction with RPC fallbacks."""
        fallbacks = [
            (
                "suix_getTransactionBlock",
                [
                    tx_digest,
                    {
                        "showInput":   False,
                        "showEffects": False,
                        "showEvents":  True,
                    },
                ],
            ),
            (
                "sui_getTransactionBlock",
                [
                    tx_digest,
                    {
                        "showInput":   False,
                        "showEffects": False,
                        "showEvents":  True,
                    },
                ],
            ),
        ]

        last_error: Optional[Exception] = None
        for method, params in fallbacks:
            try:
                result = await self._rpc.call(method, params)
                if result and "events" in result:
                    return result.get("events") or []
            except Exception as exc:
                last_error = exc

        if self._package_ids:
            try:
                query_result = await self._rpc.call(
                    "suix_queryEvents",
                    [
                        {"MoveModule": {"package": self._package_ids[0], "module": "state_anchor"}},
                        None,
                        100,
                        False,
                    ],
                )
                data = (query_result or {}).get("data", [])
                if data:
                    return data
            except Exception as exc:
                last_error = exc

        if last_error:
            log.debug("sui.tx_events_fallback_failed", tx=tx_digest[:12], error=str(last_error))
        return []

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
