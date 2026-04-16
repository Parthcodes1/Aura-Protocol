"""
Nonce management for replay-attack prevention.
Tracks (peer_id, nonce) pairs with a configurable TTL.
Expired entries are pruned periodically so memory stays bounded.
"""
from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import Dict, Set, Tuple

from monitoring.logger import get_logger

log = get_logger(__name__)


class NonceManager:
    """
    Thread-safe nonce tracker.

    Each nonce is stored as (peer_id, nonce_hex) → expiry_time.
    `is_replay()` returns True if the nonce was seen before OR the timestamp is stale.
    """

    def __init__(self, ttl_seconds: float = 60.0, prune_interval: float = 30.0) -> None:
        self._ttl = ttl_seconds
        self._prune_interval = prune_interval
        # (peer_id, nonce) → expiry timestamp
        self._seen: Dict[Tuple[str, str], float] = {}
        self._lock = asyncio.Lock()
        self._prune_task: asyncio.Task | None = None

    async def start(self) -> None:
        self._prune_task = asyncio.create_task(self._prune_loop())
        log.debug("nonce_manager.started", ttl=self._ttl)

    async def stop(self) -> None:
        if self._prune_task:
            self._prune_task.cancel()
            try:
                await self._prune_task
            except asyncio.CancelledError:
                pass

    async def is_replay(self, peer_id: str, nonce: str, timestamp_ms: int) -> bool:
        """
        Returns True (and rejects) if:
        - Timestamp is outside the TTL window
        - This (peer_id, nonce) pair was already seen
        Otherwise records the nonce and returns False.
        """
        now = time.time()
        msg_time = timestamp_ms / 1000.0
        age = now - msg_time

        if abs(age) > self._ttl:
            log.debug("nonce.stale_timestamp", peer=peer_id, age_s=age)
            return True

        key = (peer_id, nonce)
        async with self._lock:
            if key in self._seen:
                log.warning("nonce.replay_detected", peer=peer_id, nonce=nonce)
                return True
            self._seen[key] = now + self._ttl

        return False

    # ── Pruning ───────────────────────────────────────────────────────────────

    async def _prune_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._prune_interval)
                await self._prune()
            except asyncio.CancelledError:
                break

    async def _prune(self) -> None:
        now = time.time()
        async with self._lock:
            expired = [k for k, exp in self._seen.items() if exp < now]
            for k in expired:
                del self._seen[k]
        if expired:
            log.debug("nonce.pruned", count=len(expired))

    @property
    def size(self) -> int:
        return len(self._seen)
