"""
Raft-inspired leader election for the oracle agent swarm.

States:
  FOLLOWER  → default; waits for leader heartbeats
  CANDIDATE → starts an election when leader is silent
  LEADER    → broadcasts state + heartbeats; drives consensus

Weighted scoring:
  election_score = uptime * 0.4 + reputation * 0.4 + random_tiebreaker * 0.2
"""
from __future__ import annotations

import asyncio
import random
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

from monitoring.logger import get_logger
from monitoring.metrics import ELECTION_TERM, IS_LEADER
from network.protocols import (
    LeaderAnnouncePayload,
    MsgType,
    Topic,
    VoteGrantPayload,
    VoteRequestPayload,
)

log = get_logger(__name__)


class RaftRole(str, Enum):
    FOLLOWER  = "follower"
    CANDIDATE = "candidate"
    LEADER    = "leader"


class RaftNode:
    """
    Implements Raft-inspired leader election.

    External triggers:
      - `handle_vote_request(from_agent, payload)` — received VoteRequest
      - `handle_vote_grant(from_agent, payload)`   — received VoteGrant
      - `handle_leader_heartbeat(from_agent, term)` — received leader heartbeat
      - `on_new_peer(peer_id)` / `on_peer_lost(peer_id)` — peer changes

    Callbacks set by owner:
      - `on_become_leader`  → async callable
      - `on_lose_leader`    → async callable
      - `on_new_leader(leader_id, term)` → async callable
    """

    def __init__(
        self,
        agent_id: str,
        config,                        # ConsensusConfig
        pubsub,                        # ReliablePubSub
        reputation_manager,
    ) -> None:
        self._id          = agent_id
        self._cfg         = config
        self._ps          = pubsub
        self._rep         = reputation_manager

        self.role         = RaftRole.FOLLOWER
        self.term         = 0
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None
        self._votes_received: Set[str] = set()

        self._peers: Set[str] = set()
        self._start_time = time.monotonic()
        self._last_heartbeat = time.monotonic()

        self._election_task: Optional[asyncio.Task] = None
        self._running = False

        # Callbacks
        self.on_become_leader: Optional[Callable] = None
        self.on_lose_leader:   Optional[Callable] = None
        self.on_new_leader:    Optional[Callable] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        self._election_task = asyncio.create_task(self._election_timeout_loop())
        log.info("raft.started", id=self._id, role=self.role)

    async def stop(self) -> None:
        self._running = False
        if self._election_task:
            self._election_task.cancel()
            try:
                await self._election_task
            except asyncio.CancelledError:
                pass

    # ── Peer management ───────────────────────────────────────────────────────

    def on_new_peer(self, peer_id: str) -> None:
        self._peers.add(peer_id)
        log.debug("raft.peer_added", peer=peer_id, total=len(self._peers))

    def on_peer_lost(self, peer_id: str) -> None:
        self._peers.discard(peer_id)
        if peer_id == self.leader_id:
            log.warning("raft.leader_lost", leader=peer_id)
            self.leader_id = None
            IS_LEADER.set(0)
            # Leader gone → trigger election
            asyncio.create_task(self._start_election())

    # ── Election timeout loop ─────────────────────────────────────────────────

    async def _election_timeout_loop(self) -> None:
        # Generate ONE random timeout per heartbeat cycle so nodes don't fire in lockstep
        heartbeat_epoch = self._last_heartbeat
        timeout = random.uniform(self._cfg.election_timeout_min, self._cfg.election_timeout_max)

        while self._running:
            await asyncio.sleep(0.2)

            if self.role == RaftRole.LEADER:
                heartbeat_epoch = self._last_heartbeat
                continue

            # Heartbeat reset (leader message or election start) → pick a new timeout
            if self._last_heartbeat != heartbeat_epoch:
                heartbeat_epoch = self._last_heartbeat
                timeout = random.uniform(
                    self._cfg.election_timeout_min,
                    self._cfg.election_timeout_max,
                )

            elapsed = time.monotonic() - self._last_heartbeat
            if elapsed > timeout:
                log.info(
                    "raft.election_timeout",
                    elapsed=round(elapsed, 2),
                    timeout=round(timeout, 2),
                )
                asyncio.create_task(self._start_election())

    # ── Election ──────────────────────────────────────────────────────────────

    async def _start_election(self) -> None:
        if self.role == RaftRole.LEADER:
            return
        if self._rep.is_blacklisted(self._id):
            log.warning("raft.blacklisted_cannot_run")
            return

        self.term     += 1
        self.role      = RaftRole.CANDIDATE
        self.voted_for = self._id
        self._votes_received = {self._id}
        self._last_heartbeat = time.monotonic()  # reset timeout

        ELECTION_TERM.set(self.term)
        log.info("raft.election_started", term=self.term, id=self._id)

        score = self._election_score()
        req = VoteRequestPayload(
            candidate_id=self._id,
            term=self.term,
            election_score=score,
            uptime_seconds=self._uptime,
            reputation_score=self._rep.my_score,
        )

        await self._ps.publish(Topic.SWARM_ELECTION, {
            "type":    MsgType.VOTE_REQUEST,
            "payload": req.model_dump(),
        })

        # Wait for votes
        await asyncio.sleep(self._cfg.election_timeout_min)
        await self._tally_votes()

    async def _tally_votes(self) -> None:
        if self.role != RaftRole.CANDIDATE:
            return

        total_nodes = len(self._peers) + 1  # including self
        needed = max(2, int(total_nodes * 0.5) + 1)

        log.info(
            "raft.vote_tally",
            votes=len(self._votes_received),
            needed=needed,
            total=total_nodes,
        )

        if len(self._votes_received) >= needed:
            await self._become_leader()
        else:
            # Lost — revert to follower
            self.role = RaftRole.FOLLOWER
            log.info("raft.election_lost", term=self.term, votes=len(self._votes_received))

    async def _become_leader(self) -> None:
        self.role      = RaftRole.LEADER
        self.leader_id = self._id

        IS_LEADER.set(1)
        log.info("raft.became_leader", term=self.term, id=self._id)

        # Announce
        announce = LeaderAnnouncePayload(
            leader_id=self._id,
            term=self.term,
            state_version=0,
            state_hash="",
        )
        await self._ps.publish(Topic.SWARM_ELECTION, {
            "type":    MsgType.LEADER_ANNOUNCE,
            "payload": announce.model_dump(),
        })

        if self.on_become_leader:
            await self.on_become_leader()

    # ── Message handlers ──────────────────────────────────────────────────────

    async def handle_vote_request(self, from_agent: str, msg: dict) -> None:
        payload = msg.get("payload", {})
        req = VoteRequestPayload(**payload)

        # Don't vote for blacklisted agents
        if self._rep.is_blacklisted(from_agent):
            return

        grant = False

        # Advance to higher term if needed — reset vote so we can vote in the new term
        if req.term > self.term:
            self.term = req.term
            self.voted_for = None
            self.role = RaftRole.FOLLOWER

        if (
            req.term == self.term
            and self.role != RaftRole.CANDIDATE  # candidates already voted for themselves
            and (self.voted_for is None or self.voted_for == req.candidate_id)
        ):
            self.voted_for = req.candidate_id
            self._last_heartbeat = time.monotonic()
            grant = True
            log.info("raft.vote_granted", to=req.candidate_id, term=req.term)
        else:
            log.debug(
                "raft.vote_denied",
                to=req.candidate_id,
                term=req.term,
                our_term=self.term,
                voted_for=self.voted_for,
            )

        resp = VoteGrantPayload(
            voter_id=self._id,
            candidate_id=req.candidate_id,
            term=req.term,
            granted=grant,
        )
        await self._ps.publish(Topic.SWARM_ELECTION, {
            "type":    MsgType.VOTE_GRANT,
            "payload": resp.model_dump(),
        })

    async def handle_vote_grant(self, from_agent: str, msg: dict) -> None:
        payload = msg.get("payload", {})
        grant = VoteGrantPayload(**payload)

        if (
            self.role == RaftRole.CANDIDATE
            and grant.candidate_id == self._id
            and grant.term == self.term
            and grant.granted
        ):
            self._votes_received.add(from_agent)
            log.debug("raft.vote_received", from_agent=from_agent, count=len(self._votes_received))

    async def handle_leader_heartbeat(self, from_agent: str, term: int) -> None:
        if term >= self.term:
            if self.role == RaftRole.LEADER and from_agent != self._id:
                # Another leader with >= term → step down
                log.warning("raft.stepping_down", other_leader=from_agent, term=term)
                self.role = RaftRole.FOLLOWER
                IS_LEADER.set(0)
                if self.on_lose_leader:
                    await self.on_lose_leader()

            old_leader = self.leader_id
            self.term = term
            self.leader_id = from_agent
            self._last_heartbeat = time.monotonic()
            self.voted_for = None

            if self.on_new_leader and from_agent != old_leader:
                await self.on_new_leader(from_agent, term)

    async def handle_leader_announce(self, from_agent: str, msg: dict) -> None:
        payload = msg.get("payload", {})
        ann = LeaderAnnouncePayload(**payload)
        await self.handle_leader_heartbeat(ann.leader_id, ann.term)

    # ── Utilities ─────────────────────────────────────────────────────────────

    def _election_score(self) -> float:
        uptime_norm = min(self._uptime / 3600.0, 1.0)   # cap at 1 hour
        rep_norm    = self._rep.my_score / 100.0
        tiebreak    = random.random() * 0.1
        return uptime_norm * 0.4 + rep_norm * 0.4 + tiebreak

    @property
    def _uptime(self) -> float:
        return time.monotonic() - self._start_time

    @property
    def is_leader(self) -> bool:
        return self.role == RaftRole.LEADER

    def get_status(self) -> Dict[str, Any]:
        return {
            "id":       self._id,
            "role":     self.role.value,
            "term":     self.term,
            "leader":   self.leader_id,
            "peers":    len(self._peers),
            "uptime_s": round(self._uptime, 1),
            "score":    round(self._rep.my_score, 2),
        }
