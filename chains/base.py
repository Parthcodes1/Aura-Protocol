"""
Abstract base class for chain watchers.
All chain-specific watchers inherit from this.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from monitoring.logger import get_logger

log = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Normalized data models
# ─────────────────────────────────────────────────────────────────────────────

class NormalizedEvent(BaseModel):
    """Chain-agnostic event representation."""
    chain: str
    tx_hash: str
    event_type: str                    # "Transfer", "Swap", "Call", etc.
    block_number: int
    log_index: int = 0
    contract_address: Optional[str] = None
    from_address: Optional[str] = None
    to_address: Optional[str] = None
    value: Optional[str] = None        # as string; large numbers safe
    token_address: Optional[str] = None
    raw_data: Dict[str, Any] = {}


class ChainBlock(BaseModel):
    """Normalized block / slot / checkpoint."""
    chain: str
    block_number: int
    block_hash: str
    parent_hash: str
    timestamp: int                     # unix seconds
    tx_count: int
    gas_used: Optional[int] = None
    validator: Optional[str] = None
    events: List[NormalizedEvent] = []
    raw: Dict[str, Any] = {}          # original chain-specific data

    def canonical_hash(self) -> str:
        """SHA-256 of the canonical representation (deterministic)."""
        canonical = {
            "chain":        self.chain,
            "block_number": self.block_number,
            "block_hash":   self.block_hash,
            "timestamp":    self.timestamp,
            "tx_count":     self.tx_count,
        }
        return hashlib.sha256(
            json.dumps(canonical, sort_keys=True).encode()
        ).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# Base watcher
# ─────────────────────────────────────────────────────────────────────────────

class BaseChainWatcher(ABC):
    """
    Polls a blockchain for new blocks/events on a configurable interval.
    Emits ChainBlock objects via the `on_new_block` callback.
    """

    def __init__(
        self,
        chain_name: str,
        poll_interval: float,
        max_retries: int = 3,
    ) -> None:
        self.chain_name = chain_name
        self.poll_interval = poll_interval
        self.max_retries = max_retries
        self.max_catch_up_blocks = 25

        self._last_block: Optional[int] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._callbacks: List[Any] = []
        self._error_count = 0
        self._fetch_count = 0

    # ── Interface ─────────────────────────────────────────────────────────────

    @abstractmethod
    async def connect(self) -> None:
        """Establish RPC connection. Raise on failure."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close RPC connection."""

    @abstractmethod
    async def fetch_latest_block(self) -> ChainBlock:
        """Fetch the latest finalized block. Raise on error."""

    @abstractmethod
    async def fetch_events(
        self,
        from_block: int,
        to_block: int,
        contract_addresses: Optional[List[str]] = None,
    ) -> List[NormalizedEvent]:
        """Fetch events in [from_block, to_block] range."""

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self, on_new_block) -> None:
        """Start polling. `on_new_block` is an async callable(ChainBlock)."""
        await self.connect()
        self._callbacks.append(on_new_block)
        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        log.info("chain_watcher.started", chain=self.chain_name, interval=self.poll_interval)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self.disconnect()
        log.info("chain_watcher.stopped", chain=self.chain_name)

    # ── Poll loop ─────────────────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        while self._running:
            try:
                processed = 0
                while self._running and processed < self.max_catch_up_blocks:
                    t0 = time.monotonic()
                    block = await self._fetch_with_retry()
                    latency = (time.monotonic() - t0) * 1000

                    if not block or block.block_number == self._last_block:
                        break

                    self._last_block = block.block_number
                    self._fetch_count += 1
                    self._error_count = 0

                    from monitoring.metrics import (
                        CHAIN_BLOCKS_FETCHED,
                        CHAIN_BLOCK_HEIGHT,
                        CHAIN_FETCH_LATENCY,
                    )
                    CHAIN_BLOCKS_FETCHED.labels(chain=self.chain_name).inc()
                    CHAIN_BLOCK_HEIGHT.labels(chain=self.chain_name).set(block.block_number)
                    CHAIN_FETCH_LATENCY.labels(chain=self.chain_name).observe(latency / 1000)

                    log.info(
                        "chain.new_block",
                        chain=self.chain_name,
                        block=block.block_number,
                        hash=block.block_hash[:16] + "…",
                        txs=block.tx_count,
                        latency_ms=round(latency, 1),
                    )

                    for cb in self._callbacks:
                        try:
                            await cb(block)
                        except Exception as exc:
                            log.error("chain.callback_error", chain=self.chain_name, error=str(exc))

                    processed += 1

                if processed >= self.max_catch_up_blocks:
                    log.debug(
                        "chain.catch_up_limited",
                        chain=self.chain_name,
                        limit=self.max_catch_up_blocks,
                    )

            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._error_count += 1
                from monitoring.metrics import CHAIN_FETCH_ERRORS
                CHAIN_FETCH_ERRORS.labels(
                    chain=self.chain_name, error_type=type(exc).__name__
                ).inc()
                backoff = min(self.poll_interval * (2 ** min(self._error_count, 5)), 60.0)
                log.warning(
                    "chain.fetch_error",
                    chain=self.chain_name,
                    error=str(exc),
                    backoff_s=round(backoff, 1),
                    consecutive=self._error_count,
                )
                await asyncio.sleep(backoff)
                continue

            await asyncio.sleep(self.poll_interval)

    async def _fetch_with_retry(self) -> Optional[ChainBlock]:
        last_exc = None
        for attempt in range(self.max_retries):
            try:
                return await self.fetch_latest_block()
            except Exception as exc:
                last_exc = exc
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
        raise last_exc

    # ── Stats ─────────────────────────────────────────────────────────────────

    @property
    def stats(self) -> Dict[str, Any]:
        return {
            "chain":        self.chain_name,
            "last_block":   self._last_block,
            "fetch_count":  self._fetch_count,
            "error_count":  self._error_count,
            "running":      self._running,
        }
