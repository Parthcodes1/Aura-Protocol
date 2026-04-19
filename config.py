"""
Unified configuration for the cross-chain P2P oracle agent.
Loaded from TOML file with environment variable overrides.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import toml
from dotenv import load_dotenv
from pydantic import BaseModel, Field

load_dotenv()


SUI_RPC_ENDPOINTS = {
    "devnet": "https://fullnode.devnet.sui.io:443",
    "testnet": "https://fullnode.testnet.sui.io:443",
    "mainnet": "https://fullnode.mainnet.sui.io:443",
}


def resolve_sui_rpc_url(network: str) -> str:
    return SUI_RPC_ENDPOINTS.get((network or "devnet").strip().lower(), SUI_RPC_ENDPOINTS["devnet"])


# ─────────────────────────────────────────────────────────────────────────────
# Sub-configs
# ─────────────────────────────────────────────────────────────────────────────

class NetworkConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 4001
    bootstrap_peers: List[str] = []
    max_connections: int = 50
    connection_timeout: float = 30.0
    enable_mdns: bool = True
    enable_dht: bool = True
    heartbeat_interval: float = 5.0


class EthereumConfig(BaseModel):
    enabled: bool = False
    rpc_url: str = "https://ethereum.publicnode.com"
    ws_url: Optional[str] = None
    chain_id: int = 1
    poll_interval: float = 12.0
    start_block: Optional[int] = None
    contract_addresses: List[str] = []
    event_signatures: List[str] = []
    request_timeout: float = 10.0
    max_retries: int = 3


class SuiConfig(BaseModel):
    enabled: bool = False
    network: str = "devnet"
    rpc_url: str = "https://fullnode.devnet.sui.io:443"
    poll_interval: float = 3.0
    package_ids: List[str] = []
    event_filters: List[Dict[str, Any]] = []
    request_timeout: float = 10.0
    max_retries: int = 3
    max_checkpoint_transactions: int = 0


class SolanaConfig(BaseModel):
    enabled: bool = False
    rpc_url: str = "https://api.mainnet-beta.solana.com"
    ws_url: str = "wss://api.mainnet-beta.solana.com"
    poll_interval: float = 1.0
    program_ids: List[str] = []
    commitment: str = "confirmed"
    request_timeout: float = 10.0
    max_retries: int = 3


class ConsensusConfig(BaseModel):
    heartbeat_interval: float = 5.0
    stale_timeout: float = 10.0
    election_timeout_min: float = 3.0
    election_timeout_max: float = 6.0
    min_consensus_peers: int = 2
    consensus_threshold: float = 0.67
    oracle_quorum: int = 2
    max_clock_skew_ms: int = 30000


class AnchorConfig(BaseModel):
    enabled: bool = False
    sui_rpc_url: str = "https://fullnode.testnet.sui.io:443"
    package_id: Optional[str] = None
    keypair_path: Optional[str] = None
    anchor_every_n_epochs: int = 10


class MonitoringConfig(BaseModel):
    dashboard_port: int = 8080
    metrics_port: int = 9090
    log_level: str = "INFO"
    log_format: str = "json"  # "json" | "pretty"


class ReputationConfig(BaseModel):
    initial_score: float = 50.0
    min_score: float = 0.0
    max_score: float = 100.0
    blacklist_threshold: float = 10.0
    valid_report_reward: float = 1.0
    invalid_report_penalty: float = 10.0
    miss_penalty: float = 2.0
    replay_attack_penalty: float = 20.0


# ─────────────────────────────────────────────────────────────────────────────
# Root config
# ─────────────────────────────────────────────────────────────────────────────

class AgentConfig(BaseModel):
    agent_id: str
    data_dir: str = "./data"
    network: NetworkConfig = NetworkConfig()
    ethereum: EthereumConfig = EthereumConfig()
    sui: SuiConfig = SuiConfig()
    solana: SolanaConfig = SolanaConfig()
    consensus: ConsensusConfig = ConsensusConfig()
    anchor: AnchorConfig = AnchorConfig()
    monitoring: MonitoringConfig = MonitoringConfig()
    reputation: ReputationConfig = ReputationConfig()

    def ensure_data_dir(self) -> None:
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)


def load_config(agent_id: str, config_path: Optional[str] = None) -> AgentConfig:
    """
    Load AgentConfig from:
    1. TOML config file (if provided / if exists)
    2. Environment variables (override file values)
    """
    data: Dict[str, Any] = {"agent_id": agent_id}

    # ── Load TOML file ────────────────────────────────────────────────────
    if config_path and Path(config_path).exists():
        with open(config_path) as f:
            data.update(toml.load(f))
    else:
        default_path = Path("config.toml")
        if default_path.exists():
            with open(default_path) as f:
                data.update(toml.load(f))

    # ── Env overrides ─────────────────────────────────────────────────────
    net = data.setdefault("network", {})
    if os.getenv("P2P_PORT"):
        net["port"] = int(os.environ["P2P_PORT"])
    if os.getenv("BOOTSTRAP_PEERS"):
        net["bootstrap_peers"] = [
            p.strip() for p in os.environ["BOOTSTRAP_PEERS"].split(",") if p.strip()
        ]
    if os.getenv("DATA_DIR"):
        data["data_dir"] = os.environ["DATA_DIR"]

    eth = data.setdefault("ethereum", {})
    if os.getenv("ETH_RPC_URL"):
        eth["rpc_url"] = os.environ["ETH_RPC_URL"]
        eth["enabled"] = True
    if os.getenv("ETH_WS_URL"):
        eth["ws_url"] = os.environ["ETH_WS_URL"]
    if os.getenv("ETH_CHAIN_ID"):
        eth["chain_id"] = int(os.environ["ETH_CHAIN_ID"])
    if os.getenv("ETH_POLL_INTERVAL"):
        eth["poll_interval"] = float(os.environ["ETH_POLL_INTERVAL"])
    if os.getenv("ETH_ENABLED", "").lower() == "true":
        eth["enabled"] = True

    sui = data.setdefault("sui", {})
    if os.getenv("SUI_NETWORK"):
        sui["network"] = os.environ["SUI_NETWORK"].strip().lower()
    if os.getenv("SUI_RPC_URL"):
        sui["rpc_url"] = os.environ["SUI_RPC_URL"]
        sui["enabled"] = True
    elif sui.get("network"):
        sui["rpc_url"] = resolve_sui_rpc_url(str(sui.get("network")))
    if os.getenv("SUI_POLL_INTERVAL"):
        sui["poll_interval"] = float(os.environ["SUI_POLL_INTERVAL"])
    if os.getenv("SUI_MAX_CHECKPOINT_TXS"):
        sui["max_checkpoint_transactions"] = int(os.environ["SUI_MAX_CHECKPOINT_TXS"])
    if os.getenv("SUI_PACKAGE_IDS"):
        sui["package_ids"] = [
            item.strip() for item in os.environ["SUI_PACKAGE_IDS"].split(",") if item.strip()
        ]
    if os.getenv("SUI_EVENT_FILTERS"):
        import json

        sui["event_filters"] = json.loads(os.environ["SUI_EVENT_FILTERS"])
    if os.getenv("SUI_ENABLED", "").lower() == "true":
        sui["enabled"] = True

    sol = data.setdefault("solana", {})
    if os.getenv("SOL_RPC_URL"):
        sol["rpc_url"] = os.environ["SOL_RPC_URL"]
        sol["enabled"] = True
    if os.getenv("SOL_WS_URL"):
        sol["ws_url"] = os.environ["SOL_WS_URL"]
    if os.getenv("SOL_POLL_INTERVAL"):
        sol["poll_interval"] = float(os.environ["SOL_POLL_INTERVAL"])
    if os.getenv("SOL_ENABLED", "").lower() == "true":
        sol["enabled"] = True

    con = data.setdefault("consensus", {})
    if os.getenv("MIN_CONSENSUS_PEERS"):
        con["min_consensus_peers"] = int(os.environ["MIN_CONSENSUS_PEERS"])
    if os.getenv("CONSENSUS_THRESHOLD"):
        con["consensus_threshold"] = float(os.environ["CONSENSUS_THRESHOLD"])

    anc = data.setdefault("anchor", {})
    if os.getenv("ANCHOR_ENABLED", "").lower() == "true":
        anc["enabled"] = True
    if os.getenv("ANCHOR_SUI_RPC_URL"):
        anc["sui_rpc_url"] = os.environ["ANCHOR_SUI_RPC_URL"]
    if os.getenv("ANCHOR_PACKAGE_ID"):
        anc["package_id"] = os.environ["ANCHOR_PACKAGE_ID"]
    if os.getenv("ANCHOR_KEYPAIR_PATH"):
        anc["keypair_path"] = os.environ["ANCHOR_KEYPAIR_PATH"]

    mon = data.setdefault("monitoring", {})
    if os.getenv("DASHBOARD_PORT"):
        mon["dashboard_port"] = int(os.environ["DASHBOARD_PORT"])
    if os.getenv("METRICS_PORT"):
        mon["metrics_port"] = int(os.environ["METRICS_PORT"])
    if os.getenv("LOG_LEVEL"):
        mon["log_level"] = os.environ["LOG_LEVEL"]
    if os.getenv("LOG_FORMAT"):
        mon["log_format"] = os.environ["LOG_FORMAT"]

    return AgentConfig(**data)
