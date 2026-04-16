"""
Oracle consensus engine.
Agents broadcast their observed chain state; when a quorum agrees on the
same (chain, block_number, state_hash), the state is marked "verified".
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from monitoring.logger import get_logger
from monitoring.metrics import (
    CONSENSUS_FAILURES,
    CONSENSUS_LATENCY,
    CONSENSUS_ROUNDS,
    CONSENSUS_SUCCESSES,
    ORACLE_DISCREPANCIES,
)
from network.protocols import (
    ChainReportPayload,
    MsgType,
    OracleVotePayload,
    Topic,
    VerifiedStatePayload,
)

log = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Vote tracking per (chain, block_number)
# ─────────────────────────────────────────────────────────────────────────────

class BlockVote:
    """Accumulates votes for a specific (chain, block_number)."""

    def __init__(self, chain: str, block_number: int) -> None:
        self.chain        = chain
        self.block_number = block_number
        self.started_at   = time.monotonic()

        # state_hash → set of voter agent IDs
        self.votes: Dict[str, Set[str]] = defaultdict(set)
        # state_hash → ChainReportPayload (first seen)
        self.reports: Dict[str, ChainReportPayload] = {}
        self.verified = False

    def add_vote(self, voter_id: str, state_hash: str, report: Optional[ChainReportPayload]) -> None:
        self.votes[state_hash].add(voter_id)
        if report and state_hash not in self.reports:
            self.reports[state_hash] = report

    def leading_hash(self) -> Tuple[Optional[str], int]:
        """Return (winning_hash, vote_count)."""
        if not self.votes:
            return None, 0
        best_hash = max(self.votes, key=lambda h: len(self.votes[h]))
        return best_hash, len(self.votes[best_hash])

    def age(self) -> float:
        return time.monotonic() - self.started_at


class OracleConsensus:
    """
    Cross-chain oracle consensus engine.

    Per chain, per block:
    1. Receive ChainReport from an agent
    2. Cast our own OracleVote (state_hash)
    3. When quorum reached → emit VerifiedState + call on_verified_state

    Quorum = max(oracle_quorum, ceil(active_peers * consensus_threshold))
    """

    def __init__(
        self,
        agent_id: str,
        config,              # ConsensusConfig
        pubsub,              # ReliablePubSub
        heartbeat_mgr,       # HeartbeatManager
        reputation,          # ReputationManager
    ) -> None:
        self._id        = agent_id
        self._cfg       = config
        self._ps        = pubsub
        self._hb        = heartbeat_mgr
        self._rep       = reputation

        # (chain, block_number) → BlockVote
        self._pending: Dict[Tuple[str, int], BlockVote] = {}
        self._verified_blocks: Dict[str, int] = {}  # chain → latest verified block
        self._lock = asyncio.Lock()
        self._running = False
        self._tasks: List[asyncio.Task] = []

        # Callback: called when a block is verified
        self.on_verified_state: Optional[Callable] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        self._tasks = [asyncio.create_task(self._gc_loop())]
        log.info("oracle_consensus.started")

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    # ── Ingest a new chain report (from any agent including self) ─────────────

    async def ingest_report(self, from_agent: str, report: ChainReportPayload) -> None:
        """
        Called when we receive a ChainReport (either via pubsub or locally).
        Casts an oracle vote and checks for quorum.
        """
        if self._rep.is_blacklisted(from_agent):
            log.debug("oracle.blacklisted_report", from_agent=from_agent)
            return

        key = (report.chain, report.block_number)
        CONSENSUS_ROUNDS.inc()

        async with self._lock:
            if key not in self._pending:
                self._pending[key] = BlockVote(report.chain, report.block_number)
            bv = self._pending[key]

            if bv.verified:
                return  # already verified

            bv.add_vote(from_agent, report.state_hash, report)

        log.debug(
            "oracle.vote_received",
            chain=report.chain,
            block=report.block_number,
            from_agent=from_agent,
            hash=report.state_hash[:16] + "…",
        )

        # Broadcast our own vote if we haven't already
        await self._maybe_cast_vote(report)

        # Check quorum
        await self._check_quorum(key)

    async def _maybe_cast_vote(self, report: ChainReportPayload) -> None:
        """Broadcast our own oracle vote for this block."""
        key = (report.chain, report.block_number)
        async with self._lock:
            bv = self._pending.get(key)
            if not bv or self._id in bv.votes.get(report.state_hash, set()):
                return  # already voted

        vote = OracleVotePayload(
            chain=report.chain,
            block_number=report.block_number,
            state_hash=report.state_hash,
            voter_id=self._id,
            term=0,
        )
        await self._ps.publish(Topic.ORACLE_CONSENSUS, {
            "type":    MsgType.ORACLE_VOTE,
            "payload": vote.model_dump(),
        })

    # ── Handle incoming OracleVote ─────────────────────────────────────────────

    async def handle_oracle_vote(self, from_agent: str, msg: dict) -> None:
        if self._rep.is_blacklisted(from_agent):
            return
        payload = msg.get("payload", {})
        try:
            vote = OracleVotePayload(**payload)
        except Exception:
            return

        key = (vote.chain, vote.block_number)
        async with self._lock:
            if key not in self._pending:
                self._pending[key] = BlockVote(vote.chain, vote.block_number)
            bv = self._pending[key]
            if bv.verified:
                return
            bv.add_vote(from_agent, vote.state_hash, None)

        await self._check_quorum(key)

    # ── Quorum check ──────────────────────────────────────────────────────────

    async def _check_quorum(self, key: Tuple[str, int]) -> None:
        async with self._lock:
            bv = self._pending.get(key)
            if not bv or bv.verified:
                return

            needed = max(
                self._cfg.oracle_quorum,
                max(1, int((self._hb.active_count + 1) * self._cfg.consensus_threshold)),
            )
            winning_hash, vote_count = bv.leading_hash()

            if vote_count < needed:
                log.debug(
                    "oracle.quorum_pending",
                    chain=key[0],
                    block=key[1],
                    votes=vote_count,
                    needed=needed,
                )
                return

            # ── Detect discrepancies ───────────────────────────────────────
            if len(bv.votes) > 1:
                ORACLE_DISCREPANCIES.labels(chain=key[0]).inc()
                log.warning(
                    "oracle.discrepancy",
                    chain=key[0],
                    block=key[1],
                    hashes=list(bv.votes.keys()),
                )
                # Penalise minority voters
                for h, voters in bv.votes.items():
                    if h != winning_hash:
                        for v in voters:
                            asyncio.create_task(self._rep.record_invalid_report(v))

            # ── Quorum reached ─────────────────────────────────────────────
            bv.verified = True
            report = bv.reports.get(winning_hash)
            consensus_agents = list(bv.votes[winning_hash])
            latency = bv.age()
            CONSENSUS_LATENCY.observe(latency)
            CONSENSUS_SUCCESSES.inc()

            # Reward voters in consensus
            for v in consensus_agents:
                asyncio.create_task(self._rep.record_consensus_match(v))

            self._verified_blocks[key[0]] = max(
                self._verified_blocks.get(key[0], 0), key[1]
            )

        # ── Emit verified state ───────────────────────────────────────────
        if report:
            verified = VerifiedStatePayload(
                chain=report.chain,
                block_number=report.block_number,
                block_hash=report.block_hash,
                block_timestamp=int(time.time()),
                events=report.events,
                state_hash=winning_hash,
                verified=True,
                consensus_count=vote_count,
                consensus_agents=consensus_agents,
                epoch=0,
            )

            log.info(
                "oracle.verified",
                chain=key[0],
                block=key[1],
                hash=winning_hash[:16] + "…",
                agents=len(consensus_agents),
                latency_s=round(latency, 2),
            )

            await self._ps.publish(Topic.ORACLE_VERIFIED, {
                "type":    MsgType.VERIFIED_STATE,
                "payload": verified.model_dump(),
            })

            if self.on_verified_state:
                await self.on_verified_state(verified)

    # ── Handle incoming ChainReport ───────────────────────────────────────────

    async def handle_chain_report(self, from_agent: str, msg: dict) -> None:
        payload = msg.get("payload", {})
        try:
            report = ChainReportPayload(**payload)
        except Exception as exc:
            log.warning("oracle.bad_report", from_agent=from_agent, error=str(exc))
            return
        await self.ingest_report(from_agent, report)

    # ── GC: remove old pending entries ────────────────────────────────────────

    async def _gc_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(60)
                async with self._lock:
                    stale = [
                        k for k, bv in self._pending.items()
                        if bv.age() > 300 or bv.verified
                    ]
                    for k in stale:
                        del self._pending[k]
                    if stale:
                        log.debug("oracle.gc", evicted=len(stale))
            except asyncio.CancelledError:
                break

    # ── Build a report from a local ChainBlock ────────────────────────────────

    @staticmethod
    def build_report(agent_id: str, block, fetch_latency_ms: float) -> ChainReportPayload:
        from chains.base import ChainBlock
        from state.hasher import StateHasher

        events_list = [e.model_dump() for e in block.events]
        state_hash = StateHasher.hash_block(block)

        return ChainReportPayload(
            chain=block.chain,
            block_number=block.block_number,
            block_hash=block.block_hash,
            block_timestamp=block.timestamp,
            tx_count=block.tx_count,
            events=events_list,
            state_hash=state_hash,
            agent_id=agent_id,
            fetch_latency_ms=fetch_latency_ms,
        )

    # ── Stats ─────────────────────────────────────────────────────────────────

    def get_status(self) -> Dict[str, Any]:
        return {
            "verified_blocks": dict(self._verified_blocks),
            "pending_rounds":  len(self._pending),
        }
