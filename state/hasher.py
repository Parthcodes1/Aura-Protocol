"""
Deterministic SHA-256 hashing of chain blocks and swarm state.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict


class StateHasher:
    """Canonical hash utilities for blocks and swarm state."""

    @staticmethod
    def hash_block(block) -> str:
        """SHA-256 of the canonical representation of a ChainBlock."""
        canonical = {
            "chain":        block.chain,
            "block_number": block.block_number,
            "block_hash":   block.block_hash,
            "timestamp":    block.timestamp,
            "tx_count":     block.tx_count,
        }
        return StateHasher._sha256(canonical)

    @staticmethod
    def hash_swarm_state(state: Dict[str, Any]) -> str:
        """SHA-256 of the canonical swarm state dict."""
        canonical = {
            "leader":        state.get("leader", ""),
            "epoch":         state.get("epoch", 0),
            "peers":         sorted(state.get("peers", [])),
            "state_version": state.get("state_version", 0),
        }
        return StateHasher._sha256(canonical)

    @staticmethod
    def hash_chain_state(
        chain: str,
        block_number: int,
        block_hash: str,
        events_count: int,
    ) -> str:
        canonical = {
            "chain":        chain,
            "block_number": block_number,
            "block_hash":   block_hash,
            "events_count": events_count,
        }
        return StateHasher._sha256(canonical)

    @staticmethod
    def hash_cross_chain(states: Dict[str, Any]) -> str:
        """Aggregate hash of all chain states (for anchoring)."""
        sorted_chains = sorted(states.items())
        payload = json.dumps(sorted_chains, sort_keys=True, default=str).encode()
        return hashlib.sha256(payload).hexdigest()

    @staticmethod
    def _sha256(obj: Dict[str, Any]) -> str:
        payload = json.dumps(obj, sort_keys=True, default=str).encode()
        return hashlib.sha256(payload).hexdigest()

    @staticmethod
    def verify(data: Dict[str, Any], expected_hash: str) -> bool:
        """Verify that hash(data) == expected_hash."""
        return StateHasher._sha256(data) == expected_hash
