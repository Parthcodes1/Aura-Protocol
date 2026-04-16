"""
Prometheus metrics for the cross-chain oracle agent.
Exposes an HTTP /metrics endpoint on metrics_port.
"""
from __future__ import annotations

import asyncio
from typing import Optional

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    Summary,
    generate_latest,
    start_http_server,
)

# ─────────────────────────────────────────────────────────────────────────────
# Metric definitions
# ─────────────────────────────────────────────────────────────────────────────

# P2P
PEERS_CONNECTED = Gauge("agent_peers_connected_total", "Current connected peers")
PEERS_STALE = Gauge("agent_peers_stale_total", "Stale / unreachable peers")
MESSAGES_SENT = Counter("agent_messages_sent_total", "Messages sent", ["topic"])
MESSAGES_RECEIVED = Counter("agent_messages_received_total", "Messages received", ["topic"])
MESSAGES_REJECTED = Counter("agent_messages_rejected_total", "Invalid/rejected messages", ["reason"])

# Chain watching
CHAIN_BLOCKS_FETCHED = Counter(
    "agent_chain_blocks_fetched_total", "Blocks successfully fetched", ["chain"]
)
CHAIN_EVENTS_FETCHED = Counter(
    "agent_chain_events_fetched_total", "Events fetched from chain", ["chain"]
)
CHAIN_FETCH_ERRORS = Counter(
    "agent_chain_fetch_errors_total", "Chain fetch errors", ["chain", "error_type"]
)
CHAIN_FETCH_LATENCY = Histogram(
    "agent_chain_fetch_latency_seconds",
    "Latency for chain data fetch",
    ["chain"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
)
CHAIN_BLOCK_HEIGHT = Gauge(
    "agent_chain_block_height", "Latest observed block/slot/checkpoint", ["chain"]
)

# Consensus
CONSENSUS_ROUNDS = Counter("agent_consensus_rounds_total", "Consensus rounds started")
CONSENSUS_SUCCESSES = Counter("agent_consensus_success_total", "Successful consensus rounds")
CONSENSUS_FAILURES = Counter(
    "agent_consensus_failures_total", "Failed consensus rounds", ["reason"]
)
CONSENSUS_LATENCY = Histogram(
    "agent_consensus_latency_seconds",
    "Time from broadcast to consensus",
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0],
)
ORACLE_DISCREPANCIES = Counter(
    "agent_oracle_discrepancies_total",
    "State discrepancies detected across agents",
    ["chain"],
)

# Leader election
ELECTION_TERM = Gauge("agent_election_term", "Current Raft term")
IS_LEADER = Gauge("agent_is_leader", "1 if this node is leader, 0 otherwise")

# Reputation
REPUTATION_SCORE = Gauge("agent_reputation_score", "Current reputation score")
BLACKLISTED_PEERS = Gauge("agent_blacklisted_peers_total", "Number of blacklisted peers")

# State
STATE_VERSION = Gauge("agent_state_version", "Current state version")
STATE_HASH_CHANGES = Counter("agent_state_hash_changes_total", "State hash changes")

# Blockchain anchoring
ANCHOR_SUBMISSIONS = Counter(
    "agent_anchor_submissions_total", "State hash submissions to blockchain", ["status"]
)

# Node uptime
NODE_UPTIME = Gauge("agent_node_start_timestamp_seconds", "Unix timestamp when node started")


def start_metrics_server(port: int) -> None:
    """Start Prometheus HTTP server on given port."""
    start_http_server(port)
