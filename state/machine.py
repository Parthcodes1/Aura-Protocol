"""
Distributed swarm state machine.
Maintains:
  {leader, epoch, peers, state_version, state_hash, timestamp}

State transitions are deterministic and version-stamped.
The leader is responsible for broadcasting state updates.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Dict, List, Optional

from monitoring.logger import get_logger
from monitoring.metrics import STATE_HASH_CHANGES, STATE_VERSION
from network.protocols import MsgType, SwarmStatePayload, Topic
from state.hasher import StateHasher

log = get_logger(__name__)


class SwarmState:
    """Mutable swarm state with version tracking."""

    def __init__(self) -> None:
        self.leader:        str  = ""
        self.epoch:         int  = 0
        self.peers:         List[str] = []
        self.state_version: int  = 0
        self.state_hash:    str  = ""
        self.timestamp:     int  = int(time.time())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "leader":        self.leader,
            "epoch":         self.epoch,
            "peers":         list(self.peers),
            "state_version": self.state_version,
            "state_hash":    self.state_hash,
            "timestamp":     self.timestamp,
        }

    def compute_hash(self) -> str:
        return StateHasher.hash_swarm_state(self.to_dict())

    def apply_update(self, update: Dict[str, Any]) -> bool:
        """
        Apply an incoming state update.
        Returns True if accepted (newer version), False if rejected.
        """
        incoming_version = update.get("state_version", 0)
        if incoming_version <= self.state_version:
            return False  # stale update

        self.leader        = update.get("leader", self.leader)
        self.epoch         = update.get("epoch", self.epoch)
        self.peers         = update.get("peers", self.peers)
        self.state_version = incoming_version
        self.state_hash    = update.get("state_hash", "")
        self.timestamp     = update.get("timestamp", int(time.time()))
        return True


class StateMachine:
    """
    Manages swarm state transitions and synchronisation.

    - Leader increments version and broadcasts state changes
    - Followers accept updates with higher version numbers
    - Persists state to the store on every change
    """

    def __init__(
        self,
        agent_id: str,
        pubsub,          # ReliablePubSub
        raft_node,       # RaftNode
        heartbeat_mgr,   # HeartbeatManager
        store,           # StateStore
    ) -> None:
        self._id      = agent_id
        self._ps      = pubsub
        self._raft    = raft_node
        self._hb      = heartbeat_mgr
        self._store   = store

        self._state   = SwarmState()
        self._lock    = asyncio.Lock()
        self._running = False
        self._tasks:  List[asyncio.Task] = []

        # External callbacks
        self.on_state_change: Optional[Callable] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True

        # Load from persistence
        saved = await self._store.load_latest_swarm_state()
        if saved:
            self._state.apply_update(saved)
            log.info(
                "state_machine.restored",
                version=self._state.state_version,
                leader=self._state.leader,
            )

        # Wire Raft callbacks
        self._raft.on_become_leader = self._on_become_leader
        self._raft.on_lose_leader   = self._on_lose_leader
        self._raft.on_new_leader    = self._on_new_leader

        self._tasks = [asyncio.create_task(self._leader_broadcast_loop())]
        log.info("state_machine.started", version=self._state.state_version)

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    # ── Leader callbacks ──────────────────────────────────────────────────────

    async def _on_become_leader(self) -> None:
        async with self._lock:
            self._state.leader = self._id
            self._state.epoch += 1
            await self._increment_version()
        log.info(
            "state_machine.became_leader",
            epoch=self._state.epoch,
            version=self._state.state_version,
        )

    async def _on_lose_leader(self) -> None:
        log.info("state_machine.lost_leadership")

    async def _on_new_leader(self, leader_id: str, term: int) -> None:
        async with self._lock:
            if leader_id != self._state.leader:
                self._state.leader = leader_id
                self._state.epoch  = max(self._state.epoch, term)
                await self._increment_version(broadcast=False)

    # ── Version management ────────────────────────────────────────────────────

    async def _increment_version(self, broadcast: bool = True) -> None:
        """Caller holds self._lock."""
        self._state.peers         = self._hb.get_active_peers()
        self._state.state_version += 1
        self._state.timestamp     = int(time.time())
        self._state.state_hash    = self._state.compute_hash()

        STATE_VERSION.set(self._state.state_version)
        STATE_HASH_CHANGES.inc()

        await self._store.save_swarm_state(self._state.to_dict())

        if broadcast and self._raft.is_leader:
            asyncio.create_task(self._broadcast_state())

        if self.on_state_change:
            asyncio.create_task(self.on_state_change(self._state.to_dict()))

    # ── Broadcast loop (leader only) ──────────────────────────────────────────

    async def _leader_broadcast_loop(self) -> None:
        while self._running:
            await asyncio.sleep(10.0)  # broadcast every 10s even without changes
            if self._raft.is_leader:
                await self._broadcast_state()

    async def _broadcast_state(self) -> None:
        async with self._lock:
            state_dict = self._state.to_dict()

        payload = SwarmStatePayload(**state_dict)
        await self._ps.publish(Topic.SWARM_STATE, {
            "type":    MsgType.STATE_UPDATE,
            "payload": payload.model_dump(),
        })
        log.debug(
            "state_machine.broadcast",
            version=state_dict["state_version"],
            peers=len(state_dict["peers"]),
        )

    # ── Handle incoming state update ──────────────────────────────────────────

    async def handle_state_update(self, from_agent: str, msg: dict) -> None:
        payload = msg.get("payload", {})
        try:
            update = SwarmStatePayload(**payload)
        except Exception:
            return

        async with self._lock:
            accepted = self._state.apply_update(update.model_dump())
            if accepted:
                STATE_VERSION.set(self._state.state_version)
                await self._store.save_swarm_state(self._state.to_dict())
                log.debug(
                    "state_machine.update_accepted",
                    from_agent=from_agent,
                    version=self._state.state_version,
                )

    # ── Peer change handlers ──────────────────────────────────────────────────

    async def on_peer_joined(self, peer_id: str) -> None:
        if self._raft.is_leader:
            async with self._lock:
                if peer_id not in self._state.peers:
                    self._state.peers.append(peer_id)
                    await self._increment_version()

    async def on_peer_left(self, peer_id: str) -> None:
        if self._raft.is_leader:
            async with self._lock:
                if peer_id in self._state.peers:
                    self._state.peers.remove(peer_id)
                    await self._increment_version()

    # ── Accessors ─────────────────────────────────────────────────────────────

    def get_state(self) -> Dict[str, Any]:
        return self._state.to_dict()

    @property
    def state_version(self) -> int:
        return self._state.state_version

    @property
    def state_hash(self) -> str:
        return self._state.state_hash
