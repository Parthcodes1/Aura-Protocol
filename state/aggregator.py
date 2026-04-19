"""
Cross-chain state aggregator.
Collects verified states from all chains and produces a
unified cross-chain view with a combined state hash.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Dict, List, Optional

from monitoring.logger import get_logger
from state.hasher import StateHasher

log = get_logger(__name__)


class CrossChainState:
    """The unified, verified state across all chains."""

    def __init__(self) -> None:
        # chain → latest verified VerifiedStatePayload dict
        self.chains: Dict[str, Dict[str, Any]] = {}
        self.last_updated: float = time.time()
        self.aggregate_hash: str = ""
        self.version: int = 0

    def update_chain(self, chain: str, verified_state) -> None:
        self.chains[chain] = {
            "chain":           verified_state.chain,
            "block_number":    verified_state.block_number,
            "block_hash":      verified_state.block_hash,
            "block_timestamp": verified_state.block_timestamp,
            "events_count":    len(verified_state.events),
            "state_hash":      verified_state.state_hash,
            "verified":        verified_state.verified,
            "consensus_count": verified_state.consensus_count,
            "updated_at":      time.time(),
        }
        self.last_updated   = time.time()
        self.version       += 1
        self.aggregate_hash = StateHasher.hash_cross_chain(
            {c: d["state_hash"] for c, d in self.chains.items()}
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chains":         dict(self.chains),
            "aggregate_hash": self.aggregate_hash,
            "version":        self.version,
            "last_updated":   self.last_updated,
        }

    def summary(self) -> Dict[str, Any]:
        return {
            c: {
                "block":    d["block_number"],
                "verified": d["verified"],
                "hash":     d["state_hash"][:16] + "…",
            }
            for c, d in self.chains.items()
        }


class CrossChainAggregator:
    """
    Listens for verified chain states from OracleConsensus and
    maintains the unified cross-chain view.

    Triggers `on_cross_chain_update(state)` when any chain advances.
    """

    def __init__(
        self,
        agent_id: str,
        store,                          # StateStore
        blockchain_anchor=None,         # BlockchainAnchor (optional)
    ) -> None:
        self._id     = agent_id
        self._store  = store
        self._anchor = blockchain_anchor
        self._state  = CrossChainState()
        self._lock   = asyncio.Lock()

        self.on_cross_chain_update: Optional[Callable] = None

    async def start(self) -> None:
        # Restore from DB
        summary = await self._store.get_cross_chain_summary()
        for chain, row in summary.items():
            if row:
                # Reconstruct a minimal VerifiedStatePayload-like object
                class _V:
                    pass
                v = _V()
                v.chain           = chain
                v.block_number    = row.get("block_number", 0)
                v.block_hash      = row.get("block_hash", "")
                v.block_timestamp = row.get("block_timestamp", 0)
                v.events          = []
                v.state_hash      = row.get("state_hash", "")
                v.verified        = bool(row.get("verified", 0))
                v.consensus_count = row.get("consensus_count", 0)
                self._state.update_chain(chain, v)
        log.info("aggregator.started", chains=list(self._state.chains.keys()))

    async def on_verified_state(self, verified_state) -> None:
        """Called by OracleConsensus when a block is verified."""
        chain = verified_state.chain

        async with self._lock:
            current = self._state.chains.get(chain, {})
            old_block = current.get("block_number", 0)
            old_timestamp = current.get("block_timestamp", 0)
            if verified_state.block_number <= old_block:
                if verified_state.block_timestamp <= old_timestamp:
                    return  # already have a newer state for this chain
                log.warning(
                    "aggregator.chain_height_reset",
                    chain=chain,
                    old_block=old_block,
                    new_block=verified_state.block_number,
                )

            self._state.update_chain(chain, verified_state)

        log.info(
            "aggregator.chain_updated",
            chain=chain,
            block=verified_state.block_number,
            agg_hash=self._state.aggregate_hash[:16] + "…",
        )

        # Persist
        await self._store.save_chain_state(verified_state)

        # Log hash for anchoring
        log_id = await self._store.log_state_hash(
            self._state.aggregate_hash,
            verified_state.state_hash,
        )

        # Anchor to blockchain (async, non-blocking)
        if self._anchor:
            asyncio.create_task(
                self._try_anchor(log_id, self._state.aggregate_hash)
            )

        if self.on_cross_chain_update:
            await self.on_cross_chain_update(self._state.to_dict())

    async def _try_anchor(self, log_id: int, state_hash: str) -> None:
        try:
            if self._anchor:
                tx = await self._anchor.anchor(state_hash)
                if tx:
                    await self._store.mark_anchored(log_id, tx)
        except Exception as exc:
            log.warning("aggregator.anchor_failed", error=str(exc))

    def get_state(self) -> Dict[str, Any]:
        return self._state.to_dict()

    def get_chain_state(self, chain: str) -> Optional[Dict[str, Any]]:
        return self._state.chains.get(chain)

    def get_summary(self) -> Dict[str, Any]:
        return self._state.summary()
