"""
SQLite-backed persistent state store.
Saves:
  - Swarm state snapshots
  - Verified chain states (cross-chain oracle output)
  - State history (last N entries)
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

import aiosqlite

from monitoring.logger import get_logger

log = get_logger(__name__)

MAX_HISTORY = 1000  # keep last 1000 entries per chain


class StateStore:
    """Async SQLite state persistence."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def start(self) -> None:
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._init_schema()
        log.info("state_store.started", db=self._db_path)

    async def stop(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    # ── Schema ────────────────────────────────────────────────────────────────

    async def _init_schema(self) -> None:
        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS swarm_state (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                leader          TEXT,
                epoch           INTEGER,
                peers           TEXT,       -- JSON array
                state_version   INTEGER,
                state_hash      TEXT,
                timestamp       REAL,
                saved_at        REAL
            );

            CREATE TABLE IF NOT EXISTS chain_state (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                chain           TEXT NOT NULL,
                block_number    INTEGER NOT NULL,
                block_hash      TEXT,
                block_timestamp INTEGER,
                tx_count        INTEGER,
                events          TEXT,       -- JSON array
                state_hash      TEXT,
                verified        INTEGER,
                consensus_count INTEGER,
                consensus_agents TEXT,      -- JSON array
                epoch           INTEGER,
                saved_at        REAL,
                UNIQUE(chain, block_number)
            );

            CREATE INDEX IF NOT EXISTS idx_chain_state_chain
                ON chain_state(chain, block_number DESC);

            CREATE TABLE IF NOT EXISTS state_hash_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                state_hash  TEXT NOT NULL,
                chain_hash  TEXT,
                timestamp   REAL,
                anchored    INTEGER DEFAULT 0,
                anchor_tx   TEXT
            );
        """)
        await self._db.commit()

    # ── Swarm state ───────────────────────────────────────────────────────────

    async def save_swarm_state(self, state: Dict[str, Any]) -> None:
        await self._db.execute("""
            INSERT INTO swarm_state
                (leader, epoch, peers, state_version, state_hash, timestamp, saved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            state.get("leader", ""),
            state.get("epoch", 0),
            json.dumps(state.get("peers", [])),
            state.get("state_version", 0),
            state.get("state_hash", ""),
            state.get("timestamp", time.time()),
            time.time(),
        ))
        await self._db.commit()

    async def load_latest_swarm_state(self) -> Optional[Dict[str, Any]]:
        async with self._db.execute(
            "SELECT * FROM swarm_state ORDER BY id DESC LIMIT 1"
        ) as cur:
            row = await cur.fetchone()
            if row:
                return {
                    "leader":        row["leader"],
                    "epoch":         row["epoch"],
                    "peers":         json.loads(row["peers"]),
                    "state_version": row["state_version"],
                    "state_hash":    row["state_hash"],
                    "timestamp":     row["timestamp"],
                }
        return None

    # ── Chain state ───────────────────────────────────────────────────────────

    async def save_chain_state(self, verified_state) -> None:
        """Save a VerifiedStatePayload to the DB."""
        await self._db.execute("""
            INSERT INTO chain_state
                (chain, block_number, block_hash, block_timestamp, tx_count,
                 events, state_hash, verified, consensus_count, consensus_agents,
                 epoch, saved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chain, block_number) DO UPDATE SET
                verified        = excluded.verified,
                consensus_count = excluded.consensus_count,
                state_hash      = excluded.state_hash,
                saved_at        = excluded.saved_at
        """, (
            verified_state.chain,
            verified_state.block_number,
            verified_state.block_hash,
            verified_state.block_timestamp,
            len(verified_state.events),
            json.dumps(verified_state.events[:50]),  # cap events stored
            verified_state.state_hash,
            int(verified_state.verified),
            verified_state.consensus_count,
            json.dumps(verified_state.consensus_agents),
            verified_state.epoch,
            time.time(),
        ))
        await self._db.commit()
        await self._prune_chain_state(verified_state.chain)

    async def get_latest_chain_state(self, chain: str) -> Optional[Dict[str, Any]]:
        async with self._db.execute(
            """SELECT * FROM chain_state
               WHERE chain = ? AND verified = 1
               ORDER BY saved_at DESC, id DESC LIMIT 1""",
            (chain,),
        ) as cur:
            row = await cur.fetchone()
            if row:
                return dict(row)
        return None

    async def get_chain_history(
        self, chain: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        async with self._db.execute(
            """SELECT chain, block_number, block_hash, block_timestamp,
                      tx_count, state_hash, verified, consensus_count, saved_at
               FROM chain_state WHERE chain = ? ORDER BY block_number DESC LIMIT ?""",
            (chain, limit),
        ) as cur:
            return [dict(row) async for row in cur]

    async def _prune_chain_state(self, chain: str) -> None:
        await self._db.execute("""
            DELETE FROM chain_state
            WHERE chain = ?
              AND id NOT IN (
                  SELECT id FROM chain_state
                  WHERE chain = ? ORDER BY block_number DESC LIMIT ?
              )
        """, (chain, chain, MAX_HISTORY))
        await self._db.commit()

    # ── Hash log (for blockchain anchoring) ──────────────────────────────────

    async def log_state_hash(self, state_hash: str, chain_hash: str) -> int:
        cur = await self._db.execute("""
            INSERT INTO state_hash_log (state_hash, chain_hash, timestamp)
            VALUES (?, ?, ?)
        """, (state_hash, chain_hash, time.time()))
        await self._db.commit()
        return cur.lastrowid

    async def mark_anchored(self, log_id: int, anchor_tx: str) -> None:
        await self._db.execute("""
            UPDATE state_hash_log SET anchored = 1, anchor_tx = ?
            WHERE id = ?
        """, (anchor_tx, log_id))
        await self._db.commit()

    async def get_unanchored_hashes(self) -> List[Dict[str, Any]]:
        async with self._db.execute(
            "SELECT * FROM state_hash_log WHERE anchored = 0 ORDER BY id ASC LIMIT 20"
        ) as cur:
            return [dict(row) async for row in cur]

    # ── Cross-chain summary ────────────────────────────────────────────────────

    async def get_cross_chain_summary(self) -> Dict[str, Any]:
        """Return latest verified state for each chain."""
        summary = {}
        for chain in ("ethereum", "sui", "solana"):
            row = await self.get_latest_chain_state(chain)
            if row:
                summary[chain] = row
        return summary
