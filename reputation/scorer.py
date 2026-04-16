"""
Reputation scoring system for oracle agents.
Tracks per-agent scores, updates on behaviour, and maintains a blacklist.
Scores are persisted to SQLite for continuity across restarts.
"""
from __future__ import annotations

import asyncio
import time
from typing import Dict, List, Optional, Set

import aiosqlite

from monitoring.logger import get_logger
from monitoring.metrics import BLACKLISTED_PEERS, REPUTATION_SCORE

log = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Scoring constants (overridden by ReputationConfig at runtime)
# ─────────────────────────────────────────────────────────────────────────────

INITIAL_SCORE          = 50.0
MAX_SCORE              = 100.0
MIN_SCORE              = 0.0
BLACKLIST_THRESHOLD    = 10.0

REWARD_VALID_REPORT    =  1.0
REWARD_VALID_HEARTBEAT =  0.5
REWARD_CONSENSUS_MATCH =  2.0

PENALTY_INVALID_SIG    = 10.0
PENALTY_REPLAY_ATTACK  = 20.0
PENALTY_BAD_REPORT     =  5.0
PENALTY_MISSED_HB      =  2.0
PENALTY_CONSENSUS_MISS =  3.0


class AgentScore:
    __slots__ = (
        "agent_id", "score", "total_reports", "valid_reports",
        "invalid_reports", "blacklisted", "blacklisted_at",
        "last_seen", "joined_at",
    )

    def __init__(self, agent_id: str) -> None:
        self.agent_id       = agent_id
        self.score          = INITIAL_SCORE
        self.total_reports  = 0
        self.valid_reports  = 0
        self.invalid_reports = 0
        self.blacklisted    = False
        self.blacklisted_at: Optional[float] = None
        self.last_seen      = time.time()
        self.joined_at      = time.time()

    def to_dict(self) -> dict:
        return {
            "agent_id":       self.agent_id,
            "score":          round(self.score, 2),
            "total_reports":  self.total_reports,
            "valid_reports":  self.valid_reports,
            "invalid_reports": self.invalid_reports,
            "blacklisted":    self.blacklisted,
            "last_seen":      self.last_seen,
            "joined_at":      self.joined_at,
        }


class ReputationManager:
    """
    Thread-safe reputation manager with SQLite persistence.
    """

    def __init__(self, agent_id: str, config, db_path: str) -> None:
        self._my_id    = agent_id
        self._config   = config
        self._db_path  = db_path
        self._scores: Dict[str, AgentScore] = {}
        self._blacklist: Set[str] = set()
        self._lock = asyncio.Lock()
        self._db: Optional[aiosqlite.Connection] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._db = await aiosqlite.connect(self._db_path)
        await self._init_db()
        await self._load_scores()
        log.info("reputation.started", db=self._db_path)

    async def stop(self) -> None:
        await self._flush_scores()
        if self._db:
            await self._db.close()

    async def _init_db(self) -> None:
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS reputation (
                agent_id        TEXT PRIMARY KEY,
                score           REAL NOT NULL DEFAULT 50.0,
                total_reports   INTEGER NOT NULL DEFAULT 0,
                valid_reports   INTEGER NOT NULL DEFAULT 0,
                invalid_reports INTEGER NOT NULL DEFAULT 0,
                blacklisted     INTEGER NOT NULL DEFAULT 0,
                blacklisted_at  REAL,
                last_seen       REAL,
                joined_at       REAL
            )
        """)
        await self._db.commit()

    async def _load_scores(self) -> None:
        async with self._db.execute("SELECT * FROM reputation") as cur:
            async for row in cur:
                (aid, score, total, valid, invalid,
                 bl, bl_at, last_seen, joined_at) = row
                s = AgentScore(aid)
                s.score          = score
                s.total_reports  = total
                s.valid_reports  = valid
                s.invalid_reports = invalid
                s.blacklisted    = bool(bl)
                s.blacklisted_at = bl_at
                s.last_seen      = last_seen or time.time()
                s.joined_at      = joined_at or time.time()
                self._scores[aid] = s
                if s.blacklisted:
                    self._blacklist.add(aid)

    async def _flush_scores(self) -> None:
        if not self._db:
            return
        async with self._lock:
            for s in self._scores.values():
                await self._db.execute("""
                    INSERT INTO reputation
                        (agent_id, score, total_reports, valid_reports, invalid_reports,
                         blacklisted, blacklisted_at, last_seen, joined_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(agent_id) DO UPDATE SET
                        score           = excluded.score,
                        total_reports   = excluded.total_reports,
                        valid_reports   = excluded.valid_reports,
                        invalid_reports = excluded.invalid_reports,
                        blacklisted     = excluded.blacklisted,
                        blacklisted_at  = excluded.blacklisted_at,
                        last_seen       = excluded.last_seen
                """, (
                    s.agent_id, s.score, s.total_reports, s.valid_reports,
                    s.invalid_reports, int(s.blacklisted), s.blacklisted_at,
                    s.last_seen, s.joined_at,
                ))
            await self._db.commit()

    # ── Score updates ─────────────────────────────────────────────────────────

    async def _get_or_create(self, agent_id: str) -> AgentScore:
        if agent_id not in self._scores:
            self._scores[agent_id] = AgentScore(agent_id)
            log.info("reputation.new_agent", agent=agent_id)
        return self._scores[agent_id]

    def _clamp(self, s: AgentScore) -> None:
        cfg = self._config
        s.score = max(cfg.min_score, min(cfg.max_score, s.score))

    async def _check_blacklist(self, s: AgentScore) -> None:
        if s.score <= self._config.blacklist_threshold and not s.blacklisted:
            s.blacklisted    = True
            s.blacklisted_at = time.time()
            self._blacklist.add(s.agent_id)
            BLACKLISTED_PEERS.set(len(self._blacklist))
            log.warning(
                "reputation.blacklisted",
                agent=s.agent_id,
                score=s.score,
            )

    async def record_valid_report(self, agent_id: str) -> None:
        async with self._lock:
            s = await self._get_or_create(agent_id)
            s.score += self._config.valid_report_reward
            s.total_reports += 1
            s.valid_reports  += 1
            s.last_seen = time.time()
            self._clamp(s)
            if agent_id == self._my_id:
                REPUTATION_SCORE.set(s.score)

    async def record_invalid_report(self, agent_id: str) -> None:
        async with self._lock:
            s = await self._get_or_create(agent_id)
            s.score -= self._config.invalid_report_penalty
            s.total_reports  += 1
            s.invalid_reports += 1
            self._clamp(s)
            await self._check_blacklist(s)
            log.warning("reputation.invalid_report", agent=agent_id, score=s.score)

    async def record_invalid_signature(self, agent_id: str) -> None:
        async with self._lock:
            s = await self._get_or_create(agent_id)
            s.score -= PENALTY_INVALID_SIG
            s.invalid_reports += 1
            self._clamp(s)
            await self._check_blacklist(s)
            log.warning("reputation.bad_signature", agent=agent_id, score=s.score)

    async def record_replay_attack(self, agent_id: str) -> None:
        async with self._lock:
            s = await self._get_or_create(agent_id)
            s.score -= self._config.replay_attack_penalty
            self._clamp(s)
            await self._check_blacklist(s)
            log.warning("reputation.replay_attack", agent=agent_id, score=s.score)

    async def record_heartbeat(self, agent_id: str) -> None:
        async with self._lock:
            s = await self._get_or_create(agent_id)
            s.score = min(s.score + REWARD_VALID_HEARTBEAT, self._config.max_score)
            s.last_seen = time.time()

    async def record_consensus_match(self, agent_id: str) -> None:
        async with self._lock:
            s = await self._get_or_create(agent_id)
            s.score = min(s.score + REWARD_CONSENSUS_MATCH, self._config.max_score)

    async def record_consensus_miss(self, agent_id: str) -> None:
        async with self._lock:
            s = await self._get_or_create(agent_id)
            s.score -= PENALTY_CONSENSUS_MISS
            self._clamp(s)
            await self._check_blacklist(s)

    # ── Queries ───────────────────────────────────────────────────────────────

    def is_blacklisted(self, agent_id: str) -> bool:
        return agent_id in self._blacklist

    def get_score(self, agent_id: str) -> float:
        return self._scores.get(agent_id, AgentScore(agent_id)).score

    def get_all(self) -> List[dict]:
        return [s.to_dict() for s in self._scores.values()]

    def get_trusted_peers(self) -> List[str]:
        return [
            aid for aid, s in self._scores.items()
            if not s.blacklisted and s.score >= self._config.blacklist_threshold * 2
        ]

    @property
    def my_score(self) -> float:
        return self._scores.get(self._my_id, AgentScore(self._my_id)).score

    @property
    def blacklist(self) -> Set[str]:
        return set(self._blacklist)
