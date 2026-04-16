"""
Heartbeat manager: sends periodic heartbeats, detects stale peers,
and triggers re-election when the leader goes dark.
"""
from __future__ import annotations

import asyncio
import time
from typing import Callable, Dict, List, Optional, Set

from monitoring.logger import get_logger
from monitoring.metrics import PEERS_CONNECTED, PEERS_STALE
from network.protocols import HeartbeatPayload, MsgType, Topic

log = get_logger(__name__)


class PeerRecord:
    __slots__ = ("peer_id", "last_seen", "role", "term", "state_version", "reputation", "stale")

    def __init__(self, peer_id: str) -> None:
        self.peer_id       = peer_id
        self.last_seen     = time.monotonic()
        self.role          = "follower"
        self.term          = 0
        self.state_version = 0
        self.reputation    = 50.0
        self.stale         = False

    @property
    def age(self) -> float:
        return time.monotonic() - self.last_seen

    def to_dict(self) -> dict:
        return {
            "peer_id":       self.peer_id,
            "role":          self.role,
            "term":          self.term,
            "state_version": self.state_version,
            "reputation":    round(self.reputation, 2),
            "stale":         self.stale,
            "age_s":         round(self.age, 1),
        }


class HeartbeatManager:
    """
    - Sends a signed HEARTBEAT message every `interval` seconds
    - Tracks `PeerRecord` for each known peer
    - Marks peers stale after `stale_timeout` seconds of silence
    - Calls `on_peer_stale(peer_id)` and `on_peer_recovered(peer_id)` callbacks
    """

    def __init__(
        self,
        agent_id: str,
        config,             # ConsensusConfig
        pubsub,             # ReliablePubSub
        raft_node,          # RaftNode
        reputation,         # ReputationManager
    ) -> None:
        self._id       = agent_id
        self._cfg      = config
        self._ps       = pubsub
        self._raft     = raft_node
        self._rep      = reputation

        self._peers: Dict[str, PeerRecord] = {}
        self._running = False
        self._tasks: List[asyncio.Task] = []

        # Callbacks
        self.on_peer_stale:     Optional[Callable] = None
        self.on_peer_recovered: Optional[Callable] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        self._tasks = [
            asyncio.create_task(self._send_loop()),
            asyncio.create_task(self._check_loop()),
        ]
        log.info(
            "heartbeat.started",
            interval=self._cfg.heartbeat_interval,
            stale_timeout=self._cfg.stale_timeout,
        )

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    # ── Send ──────────────────────────────────────────────────────────────────

    async def _send_loop(self) -> None:
        while self._running:
            try:
                await self._send_heartbeat()
            except Exception as exc:
                log.warning("heartbeat.send_error", error=str(exc))
            await asyncio.sleep(self._cfg.heartbeat_interval)

    async def _send_heartbeat(self) -> None:
        payload = HeartbeatPayload(
            agent_id=self._id,
            role=self._raft.role.value,
            term=self._raft.term,
            state_version=0,     # updated by state machine
            state_hash="",       # updated by state machine
            uptime_seconds=self._raft._uptime,
            reputation_score=self._rep.my_score,
        )
        msg = {
            "type":    MsgType.HEARTBEAT,
            "payload": payload.model_dump(),
        }
        await self._ps.publish(Topic.SWARM_HEARTBEAT, msg)

        # Also used as leader heartbeat for Raft
        if self._raft.is_leader:
            await self._raft.handle_leader_heartbeat(self._id, self._raft.term)

    # ── Check for stale peers ─────────────────────────────────────────────────

    async def _check_loop(self) -> None:
        while self._running:
            try:
                await self._check_staleness()
            except Exception as exc:
                log.warning("heartbeat.check_error", error=str(exc))
            await asyncio.sleep(2.0)

    async def _check_staleness(self) -> None:
        now_peers  = 0
        stale_cnt  = 0

        for peer_id, record in list(self._peers.items()):
            if self._rep.is_blacklisted(peer_id):
                continue

            if record.age > self._cfg.stale_timeout:
                if not record.stale:
                    record.stale = True
                    log.warning(
                        "heartbeat.peer_stale",
                        peer=peer_id,
                        age_s=round(record.age, 1),
                    )
                    await self._rep.record_consensus_miss(peer_id)
                    if self.on_peer_stale:
                        await self.on_peer_stale(peer_id)
                    # If stale peer was leader → trigger re-election
                    if record.role == "leader":
                        self._raft.on_peer_lost(peer_id)
                stale_cnt += 1
            else:
                now_peers += 1

        PEERS_CONNECTED.set(now_peers)
        PEERS_STALE.set(stale_cnt)

    # ── Receive ───────────────────────────────────────────────────────────────

    async def handle_heartbeat(self, from_agent: str, msg: dict) -> None:
        """Called when a HEARTBEAT message is received."""
        if from_agent == self._id:
            return

        payload_data = msg.get("payload", {})
        try:
            hb = HeartbeatPayload(**payload_data)
        except Exception:
            return

        record = self._peers.get(from_agent)
        was_stale = record.stale if record else False

        if record is None:
            record = PeerRecord(from_agent)
            self._peers[from_agent] = record
            self._raft.on_new_peer(from_agent)
            log.info("heartbeat.new_peer", peer=from_agent, role=hb.role)

        record.last_seen     = time.monotonic()
        record.role          = hb.role
        record.term          = hb.term
        record.state_version = hb.state_version
        record.reputation    = hb.reputation_score
        record.stale         = False

        if was_stale:
            log.info("heartbeat.peer_recovered", peer=from_agent)
            if self.on_peer_recovered:
                await self.on_peer_recovered(from_agent)

        await self._rep.record_heartbeat(from_agent)

        # Propagate to Raft if this is from a leader
        if hb.role == "leader":
            await self._raft.handle_leader_heartbeat(from_agent, hb.term)

    # ── Queries ───────────────────────────────────────────────────────────────

    def get_active_peers(self) -> List[str]:
        return [
            pid for pid, r in self._peers.items()
            if not r.stale and not self._rep.is_blacklisted(pid)
        ]

    def get_all_peers(self) -> List[dict]:
        return [r.to_dict() for r in self._peers.values()]

    @property
    def active_count(self) -> int:
        return len(self.get_active_peers())
