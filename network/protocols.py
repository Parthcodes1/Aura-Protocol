"""
Protocol IDs, topic names, and canonical message types.
All inter-agent communication uses these definitions.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

import msgpack
from pydantic import BaseModel

# ─────────────────────────────────────────────────────────────────────────────
# libp2p protocol IDs
# ─────────────────────────────────────────────────────────────────────────────

PROTOCOL_GOSSIP        = "/oracle/gossip/1.0.0"
PROTOCOL_DIRECT        = "/oracle/direct/1.0.0"
PROTOCOL_SYNC          = "/oracle/state-sync/1.0.0"
PROTOCOL_ELECTION      = "/oracle/election/1.0.0"

# ─────────────────────────────────────────────────────────────────────────────
# GossipSub topic names
# ─────────────────────────────────────────────────────────────────────────────

class Topic:
    SWARM_HELLO       = "swarm/hello"
    SWARM_HEARTBEAT   = "swarm/heartbeat"
    SWARM_STATE       = "swarm/state"
    SWARM_ELECTION    = "swarm/election"

    ORACLE_ETH        = "oracle/ethereum"
    ORACLE_SUI        = "oracle/sui"
    ORACLE_SOL        = "oracle/solana"
    ORACLE_CONSENSUS  = "oracle/consensus"
    ORACLE_VERIFIED   = "oracle/verified"

    REPUTATION        = "swarm/reputation"

    ALL_TOPICS = [
        SWARM_HELLO, SWARM_HEARTBEAT, SWARM_STATE, SWARM_ELECTION,
        ORACLE_ETH, ORACLE_SUI, ORACLE_SOL, ORACLE_CONSENSUS, ORACLE_VERIFIED,
        REPUTATION,
    ]

    @staticmethod
    def for_chain(chain: str) -> str:
        return {
            "ethereum": Topic.ORACLE_ETH,
            "sui":      Topic.ORACLE_SUI,
            "solana":   Topic.ORACLE_SOL,
        }.get(chain.lower(), f"oracle/{chain.lower()}")


# ─────────────────────────────────────────────────────────────────────────────
# Message types
# ─────────────────────────────────────────────────────────────────────────────

class MsgType(str, Enum):
    # Swarm / P2P
    HELLO          = "HELLO"
    HEARTBEAT      = "HEARTBEAT"
    ROLE_CHANGE    = "ROLE_CHANGE"
    VOTE_REQUEST   = "VOTE_REQUEST"
    VOTE_GRANT     = "VOTE_GRANT"
    LEADER_ANNOUNCE = "LEADER_ANNOUNCE"
    STATE_UPDATE   = "STATE_UPDATE"
    STATE_SYNC_REQ = "STATE_SYNC_REQ"
    STATE_SYNC_RESP = "STATE_SYNC_RESP"

    # Oracle / Cross-chain
    CHAIN_REPORT   = "CHAIN_REPORT"
    ORACLE_VOTE    = "ORACLE_VOTE"
    VERIFIED_STATE = "VERIFIED_STATE"

    # Reputation
    REPUTATION_UPDATE = "REPUTATION_UPDATE"
    BLACKLIST        = "BLACKLIST"


# ─────────────────────────────────────────────────────────────────────────────
# Payload models
# ─────────────────────────────────────────────────────────────────────────────

class HelloPayload(BaseModel):
    agent_id: str
    public_key: str
    listen_addrs: List[str]
    chains: List[str]          # which chains this agent watches
    version: str = "1.0.0"


class HeartbeatPayload(BaseModel):
    agent_id: str
    role: str                  # "leader" | "follower" | "candidate"
    term: int
    state_version: int
    state_hash: str
    uptime_seconds: float
    reputation_score: float


class ChainReportPayload(BaseModel):
    chain: str
    block_number: int
    block_hash: str
    block_timestamp: int
    tx_count: int
    events: List[Dict[str, Any]]
    state_hash: str            # SHA256 of this report
    agent_id: str
    fetch_latency_ms: float


class OracleVotePayload(BaseModel):
    chain: str
    block_number: int
    state_hash: str            # vote for this state hash
    voter_id: str
    term: int


class VerifiedStatePayload(BaseModel):
    chain: str
    block_number: int
    block_hash: str
    block_timestamp: int
    events: List[Dict[str, Any]]
    state_hash: str
    verified: bool
    consensus_count: int
    consensus_agents: List[str]
    epoch: int


class VoteRequestPayload(BaseModel):
    candidate_id: str
    term: int
    election_score: float
    uptime_seconds: float
    reputation_score: float


class VoteGrantPayload(BaseModel):
    voter_id: str
    candidate_id: str
    term: int
    granted: bool


class LeaderAnnouncePayload(BaseModel):
    leader_id: str
    term: int
    state_version: int
    state_hash: str


class SwarmStatePayload(BaseModel):
    leader: str
    epoch: int
    peers: List[str]
    state_version: int
    state_hash: str
    timestamp: int


# ─────────────────────────────────────────────────────────────────────────────
# Serialization helpers
# ─────────────────────────────────────────────────────────────────────────────

def encode_payload(obj: BaseModel) -> bytes:
    return msgpack.packb(obj.model_dump(), use_bin_type=True)


def decode_payload(raw: bytes, model_class):
    data = msgpack.unpackb(raw, raw=False)
    return model_class(**data)
