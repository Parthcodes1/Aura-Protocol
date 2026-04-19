"""
Cross-Chain Oracle Agent — main entry point.

Orchestrates:
  P2P network → chain watchers → oracle consensus → state machine → dashboard

Usage:
  python agent.py --agent-id agent_1 --port 4001 --chain ethereum --chain solana
  python agent.py --agent-id agent_1 --config config.toml
"""
from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
from pathlib import Path
from typing import List, Optional

import click

# ── Ensure project root is importable ─────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))


# ─────────────────────────────────────────────────────────────────────────────
# Agent orchestrator
# ─────────────────────────────────────────────────────────────────────────────

class CrossChainOracleAgent:
    """
    Top-level agent that wires all subsystems together.

    Subsystems:
      - KeyManager          (crypto)
      - NonceManager        (crypto)
      - MessageSigner       (crypto)
      - P2PHost + PubSubManager  (network)
      - PeerDiscovery       (network)
      - ReliablePubSub      (network)
      - ReputationManager   (reputation)
      - RaftNode            (consensus)
      - HeartbeatManager    (consensus)
      - OracleConsensus     (consensus)
      - StateMachine        (state)
      - StateStore          (state)
      - CrossChainAggregator(state)
      - Chain watchers      (chains)
      - BlockchainAnchor    (blockchain)
      - Dashboard           (monitoring)
      - Metrics server      (monitoring)
    """

    def __init__(self, config) -> None:
        self._cfg  = config
        self._log  = None
        self._running = False
        self._start_time = time.time()

        # Components (set up in start())
        self.key_mgr    = None
        self.nonce_mgr  = None
        self.signer     = None
        self.p2p_host   = None
        self.discovery  = None
        self.pubsub     = None
        self.reputation = None
        self.raft       = None
        self.heartbeat  = None
        self.oracle     = None
        self.state_machine = None
        self.store      = None
        self.aggregator = None
        self.chain_watchers = []
        self.anchor     = None
        self.dashboard  = None
        self._last_hello_sent: float = 0.0   # rate-limit hello broadcasts

    async def start(self) -> None:
        cfg = self._cfg
        cfg.ensure_data_dir()

        # ── Logging ────────────────────────────────────────────────────────
        from monitoring.logger import get_logger, setup_logging
        setup_logging(
            level=cfg.monitoring.log_level,
            fmt=cfg.monitoring.log_format,
            agent_id=cfg.agent_id,
        )
        self._log = get_logger("agent")
        log = self._log
        log.info("agent.starting", agent_id=cfg.agent_id)

        # ── Metrics ────────────────────────────────────────────────────────
        from monitoring.metrics import NODE_UPTIME, start_metrics_server
        NODE_UPTIME.set(time.time())
        try:
            start_metrics_server(cfg.monitoring.metrics_port)
            log.info("metrics.started", port=cfg.monitoring.metrics_port)
        except Exception as exc:
            log.warning("metrics.failed", error=str(exc))

        # ── Keys ───────────────────────────────────────────────────────────
        from crypto.keys import KeyManager
        self.key_mgr = KeyManager(cfg.agent_id, cfg.data_dir)
        self.key_mgr.load_or_generate()

        # ── Nonce manager ──────────────────────────────────────────────────
        from crypto.nonce import NonceManager
        self.nonce_mgr = NonceManager(ttl_seconds=60.0)
        await self.nonce_mgr.start()

        # ── Message signer ─────────────────────────────────────────────────
        from crypto.signing import MessageSigner
        self.signer = MessageSigner(
            agent_id=cfg.agent_id,
            private_key=self.key_mgr.private_key,
            public_key_b64=self.key_mgr.public_key_b64,
            max_clock_skew_ms=cfg.consensus.max_clock_skew_ms,
        )

        # ── P2P host ───────────────────────────────────────────────────────
        from network.host import P2PHost
        kp = self.key_mgr.get_libp2p_key_pair()
        self.p2p_host = P2PHost(
            agent_id=cfg.agent_id,
            host_str=cfg.network.host,
            port=cfg.network.port,
            key_pair=kp,
            max_connections=cfg.network.max_connections,
        )
        await self.p2p_host.start()
        log.info("p2p.started", peer_id=self.p2p_host.peer_id, port=cfg.network.port)

        # ── Reliable pub/sub ───────────────────────────────────────────────
        from network.pubsub import ReliablePubSub
        self.pubsub = ReliablePubSub(
            pubsub_manager=self.p2p_host.pubsub,
            signer=self.signer,
            nonce_manager=self.nonce_mgr,
            agent_id=cfg.agent_id,
        )

        # ── State store ────────────────────────────────────────────────────
        from state.store import StateStore
        db_path = str(Path(cfg.data_dir) / f"{cfg.agent_id}.db")
        self.store = StateStore(db_path)
        await self.store.start()

        # ── Reputation ─────────────────────────────────────────────────────
        from reputation.scorer import ReputationManager
        rep_db = str(Path(cfg.data_dir) / f"{cfg.agent_id}_reputation.db")
        self.reputation = ReputationManager(cfg.agent_id, cfg.reputation, rep_db)
        await self.reputation.start()

        # ── Raft consensus ─────────────────────────────────────────────────
        from consensus.raft import RaftNode
        self.raft = RaftNode(cfg.agent_id, cfg.consensus, self.pubsub, self.reputation)

        # ── Heartbeat manager ──────────────────────────────────────────────
        from consensus.heartbeat import HeartbeatManager
        self.heartbeat = HeartbeatManager(
            cfg.agent_id, cfg.consensus, self.pubsub, self.raft, self.reputation
        )
        self.heartbeat.on_peer_stale     = self.state_machine_peer_left
        self.heartbeat.on_peer_recovered = None

        # ── Oracle consensus ───────────────────────────────────────────────
        from consensus.oracle import OracleConsensus
        self.oracle = OracleConsensus(
            cfg.agent_id, cfg.consensus, self.pubsub, self.heartbeat, self.reputation
        )

        # ── State machine ──────────────────────────────────────────────────
        from state.machine import StateMachine
        self.state_machine = StateMachine(
            cfg.agent_id, self.pubsub, self.raft, self.heartbeat, self.store
        )

        # ── Cross-chain aggregator ─────────────────────────────────────────
        from blockchain.anchoring import BlockchainAnchor
        self.anchor = BlockchainAnchor(cfg.anchor, self.store)
        await self.anchor.start()

        from state.aggregator import CrossChainAggregator
        self.aggregator = CrossChainAggregator(cfg.agent_id, self.store, self.anchor)
        await self.aggregator.start()

        # Connect oracle → aggregator
        self.oracle.on_verified_state = self.aggregator.on_verified_state

        # ── Pub/sub handlers ───────────────────────────────────────────────
        self._register_handlers()

        # ── Start pub/sub ──────────────────────────────────────────────────
        from network.protocols import Topic
        await self.pubsub.start(topics=Topic.ALL_TOPICS)

        # ── Start consensus ────────────────────────────────────────────────
        await self.raft.start()
        await self.heartbeat.start()
        await self.oracle.start()
        await self.state_machine.start()

        # ── Peer discovery ─────────────────────────────────────────────────
        from network.discovery import PeerDiscovery
        self.discovery = PeerDiscovery(
            agent_id=cfg.agent_id,
            p2p_port=cfg.network.port,
            bootstrap_peers=cfg.network.bootstrap_peers,
            on_peer_found=self._on_peer_found,
            enable_dht=cfg.network.enable_dht,
            enable_mdns=cfg.network.enable_mdns,
        )
        await self.discovery.start()

        # ── Chain watchers ─────────────────────────────────────────────────
        await self._start_chain_watchers()

        # ── Dashboard ─────────────────────────────────────────────────────
        from monitoring.dashboard import Dashboard
        self.dashboard = Dashboard(
            agent_id=cfg.agent_id,
            port=cfg.monitoring.dashboard_port,
            state_fn=self.state_machine.get_state,
            peers_fn=self.heartbeat.get_all_peers,
            chains_fn=self.aggregator.get_state,
            reputation_fn=self.reputation.get_all,
            raft_fn=self.raft.get_status,
            metrics_fn=self._get_metrics_snapshot,
        )
        await self.dashboard.start()

        # Connect aggregator → dashboard broadcast
        self.aggregator.on_cross_chain_update = self._on_cross_chain_update

        # ── Send HELLO ─────────────────────────────────────────────────────
        await self._send_hello()

        self._running = True
        log.info(
            "agent.started",
            agent_id=cfg.agent_id,
            peer_id=self.p2p_host.peer_id,
            dashboard=f"http://localhost:{cfg.monitoring.dashboard_port}",
            metrics=f"http://localhost:{cfg.monitoring.metrics_port}/metrics",
        )

    # ── Handler registration ──────────────────────────────────────────────────

    def _register_handlers(self) -> None:
        from network.protocols import MsgType, Topic

        # Heartbeat
        self.pubsub.on(Topic.SWARM_HEARTBEAT, self._handle_heartbeat)

        # Election
        self.pubsub.on(Topic.SWARM_ELECTION, self._handle_election)

        # State sync
        self.pubsub.on(Topic.SWARM_STATE, self._handle_state_update)

        # Hello
        self.pubsub.on(Topic.SWARM_HELLO, self._handle_hello)

        # Oracle
        self.pubsub.on(Topic.ORACLE_ETH,       self._handle_chain_report)
        self.pubsub.on(Topic.ORACLE_SUI,       self._handle_chain_report)
        self.pubsub.on(Topic.ORACLE_SOL,       self._handle_chain_report)
        self.pubsub.on(Topic.ORACLE_CONSENSUS, self._handle_oracle_vote)

    # ── Message handlers ──────────────────────────────────────────────────────

    async def _handle_heartbeat(self, from_agent: str, msg: dict) -> None:
        await self.heartbeat.handle_heartbeat(from_agent, msg)

    async def _handle_election(self, from_agent: str, msg: dict) -> None:
        from network.protocols import MsgType
        msg_type = msg.get("type")
        if msg_type == MsgType.VOTE_REQUEST:
            await self.raft.handle_vote_request(from_agent, msg)
        elif msg_type == MsgType.VOTE_GRANT:
            await self.raft.handle_vote_grant(from_agent, msg)
        elif msg_type == MsgType.LEADER_ANNOUNCE:
            await self.raft.handle_leader_announce(from_agent, msg)

    async def _handle_state_update(self, from_agent: str, msg: dict) -> None:
        await self.state_machine.handle_state_update(from_agent, msg)

    async def _handle_hello(self, from_agent: str, msg: dict) -> None:
        payload = msg.get("payload", {})
        self._log.info("hello.received", from_agent=from_agent, chains=payload.get("chains", []))
        # Send hello back if this is a new peer
        await self._send_hello()

    async def _handle_chain_report(self, from_agent: str, msg: dict) -> None:
        if self.reputation.is_blacklisted(from_agent):
            return
        await self.oracle.handle_chain_report(from_agent, msg)
        await self.reputation.record_valid_report(from_agent)

    async def _handle_oracle_vote(self, from_agent: str, msg: dict) -> None:
        await self.oracle.handle_oracle_vote(from_agent, msg)

    # ── Chain watchers ────────────────────────────────────────────────────────

    async def _start_chain_watchers(self) -> None:
        cfg = self._cfg
        log = self._log

        if cfg.ethereum.enabled:
            try:
                from chains.ethereum import EthereumWatcher
                w = EthereumWatcher(cfg.ethereum)
                await w.start(self._on_new_block)
                self.chain_watchers.append(w)
                log.info("chain_watcher.started", chain="ethereum")
            except ImportError:
                log.warning("chain_watcher.eth_requires_web3")
            except Exception as exc:
                log.error("chain_watcher.eth_failed", error=str(exc))

        if cfg.sui.enabled:
            try:
                from chains.sui import SuiWatcher
                w = SuiWatcher(cfg.sui)
                await w.start(self._on_new_block)
                self.chain_watchers.append(w)
                log.info("chain_watcher.started", chain="sui")
            except Exception as exc:
                log.error("chain_watcher.sui_failed", error=str(exc))

        if cfg.solana.enabled:
            try:
                from chains.solana import SolanaWatcher
                w = SolanaWatcher(cfg.solana)
                await w.start(self._on_new_block)
                self.chain_watchers.append(w)
                log.info("chain_watcher.started", chain="solana")
            except ImportError:
                log.warning("chain_watcher.sol_requires_solana_py")
            except Exception as exc:
                log.error("chain_watcher.sol_failed", error=str(exc))

        if not self.chain_watchers:
            log.warning("agent.no_chain_watchers", msg="Enable at least one chain in config or env")

    async def _on_new_block(self, block) -> None:
        """Called by chain watchers when a new block is fetched."""
        import time as t
        fetch_start = t.monotonic()

        # Build report
        from consensus.oracle import OracleConsensus
        report = OracleConsensus.build_report(
            self._cfg.agent_id, block,
            fetch_latency_ms=(t.monotonic() - fetch_start) * 1000,
        )

        # Broadcast to peers
        from network.protocols import MsgType, Topic
        topic = Topic.for_chain(block.chain)
        await self.pubsub.publish(topic, {
            "type":    MsgType.CHAIN_REPORT,
            "payload": report.model_dump(),
        })

        # Also ingest our own report
        await self.oracle.ingest_report(self._cfg.agent_id, report)
        await self.reputation.record_valid_report(self._cfg.agent_id)

        # Push to dashboard event feed
        if self.dashboard and block.events:
            for evt in block.events[:5]:
                await self.dashboard.broadcast("chain_event", {
                    "chain":       evt.chain,
                    "event_type":  evt.event_type,
                    "block_number": evt.block_number,
                    "tx_hash":     evt.tx_hash,
                    "timestamp":   block.timestamp,
                })

    # ── Peer discovery callbacks ──────────────────────────────────────────────

    async def _on_peer_found(self, multiaddr: str) -> None:
        success = await self.p2p_host.connect(multiaddr)
        if success:
            self._log.info("agent.peer_connected", addr=multiaddr)

    async def state_machine_peer_left(self, peer_id: str) -> None:
        await self.state_machine.on_peer_left(peer_id)

    # ── Hello ─────────────────────────────────────────────────────────────────

    async def _send_hello(self) -> None:
        # Rate-limit: send at most one hello every 10 seconds to prevent storms
        now = time.time()
        if now - self._last_hello_sent < 10.0:
            return
        self._last_hello_sent = now

        from network.protocols import HelloPayload, MsgType, Topic

        chains = []
        if self._cfg.ethereum.enabled: chains.append("ethereum")
        if self._cfg.sui.enabled:      chains.append("sui")
        if self._cfg.solana.enabled:   chains.append("solana")

        hello = HelloPayload(
            agent_id=self._cfg.agent_id,
            public_key=self.key_mgr.public_key_b64,
            listen_addrs=self.p2p_host.listen_addrs,
            chains=chains,
        )
        await self.pubsub.publish(Topic.SWARM_HELLO, {
            "type":    MsgType.HELLO,
            "payload": hello.model_dump(),
        })

    # ── Cross-chain update ────────────────────────────────────────────────────

    async def _on_cross_chain_update(self, state: dict) -> None:
        if self.dashboard:
            await self.dashboard.broadcast("cross_chain_update", state)

    # ── Metrics snapshot ──────────────────────────────────────────────────────

    def _get_metrics_snapshot(self) -> dict:
        return {
            "uptime_s":    round(time.time() - self._start_time, 1),
            "peer_count":  self.heartbeat.active_count if self.heartbeat else 0,
            "my_score":    self.reputation.my_score if self.reputation else 50.0,
            "chain_watchers": [w.stats for w in self.chain_watchers],
            "oracle":      self.oracle.get_status() if self.oracle else {},
        }

    # ── Stop ──────────────────────────────────────────────────────────────────

    async def stop(self) -> None:
        log = self._log
        if log:
            log.info("agent.stopping")

        self._running = False

        for w in self.chain_watchers:
            try: await w.stop()
            except Exception: pass

        if self.dashboard:    await self.dashboard.stop()
        if self.oracle:       await self.oracle.stop()
        if self.state_machine: await self.state_machine.stop()
        if self.heartbeat:    await self.heartbeat.stop()
        if self.raft:         await self.raft.stop()
        if self.pubsub:       await self.pubsub.stop()
        if self.discovery:    await self.discovery.stop()
        if self.p2p_host:     await self.p2p_host.stop()
        if self.nonce_mgr:    await self.nonce_mgr.stop()
        if self.reputation:   await self.reputation.stop()
        if self.store:        await self.store.stop()
        if self.anchor:       await self.anchor.stop()

        if log:
            log.info("agent.stopped")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

@click.command()
@click.option("--agent-id",   required=True,  help="Unique agent identifier")
@click.option("--port",       default=4001,   type=int,  help="P2P listen port")
@click.option("--config",     default=None,   help="Path to config.toml")
@click.option("--chain",      multiple=True,  help="Chains to watch: ethereum, sui, solana")
@click.option("--eth-rpc",    default=None,   envvar="ETH_RPC_URL",  help="Ethereum RPC URL")
@click.option("--sui-rpc",    default=None,   envvar="SUI_RPC_URL",  help="Sui RPC URL")
@click.option("--sui-network", default=None,   envvar="SUI_NETWORK",
              type=click.Choice(["devnet", "testnet", "mainnet"], case_sensitive=False),
              help="Preset Sui network to use when no explicit RPC URL is provided")
@click.option("--sui-max-checkpoint-txs", default=None, type=int,
              envvar="SUI_MAX_CHECKPOINT_TXS",
              help="Max Sui transactions to scan per checkpoint (0 scans all)")
@click.option("--sol-rpc",    default=None,   envvar="SOL_RPC_URL",  help="Solana RPC URL")
@click.option("--bootstrap",  default=None,   multiple=True, help="Bootstrap peer multiaddr")
@click.option("--dashboard-port", default=8080, type=int,  help="Dashboard HTTP port")
@click.option("--metrics-port",   default=9090, type=int,  help="Prometheus metrics port")
@click.option("--log-level",  default="INFO", help="Logging level")
@click.option("--log-format", default="pretty", type=click.Choice(["json", "pretty"]),
              help="Log output format")
@click.option("--data-dir",   default="./data", help="Data directory for keys/state")
def main(
    agent_id, port, config, chain, eth_rpc, sui_rpc, sui_network, sui_max_checkpoint_txs, sol_rpc,
    bootstrap, dashboard_port, metrics_port, log_level, log_format, data_dir,
):
    """Cross-chain oracle agent — decentralized multi-chain state sync."""
    from config import load_config

    cfg = load_config(agent_id, config)

    # CLI overrides
    cfg.network.port = port
    cfg.data_dir     = data_dir
    cfg.monitoring.dashboard_port = dashboard_port
    cfg.monitoring.metrics_port   = metrics_port
    cfg.monitoring.log_level      = log_level
    cfg.monitoring.log_format     = log_format

    if bootstrap:
        cfg.network.bootstrap_peers = list(bootstrap)

    # Chain enable flags
    chains = set(c.lower() for c in chain)
    if "ethereum" in chains or eth_rpc:
        cfg.ethereum.enabled = True
        if eth_rpc: cfg.ethereum.rpc_url = eth_rpc
    if sui_network:
        cfg.sui.network = sui_network.lower()
    if sui_max_checkpoint_txs is not None:
        cfg.sui.max_checkpoint_transactions = sui_max_checkpoint_txs
    if "sui" in chains or sui_rpc or sui_network:
        cfg.sui.enabled = True
        if sui_rpc:
            cfg.sui.rpc_url = sui_rpc
        else:
            from config import resolve_sui_rpc_url

            cfg.sui.rpc_url = resolve_sui_rpc_url(cfg.sui.network)
    if "solana" in chains or sol_rpc:
        cfg.solana.enabled = True
        if sol_rpc: cfg.solana.rpc_url = sol_rpc

    asyncio.run(_run(cfg))


async def _run(cfg) -> None:
    agent = CrossChainOracleAgent(cfg)
    loop  = asyncio.get_running_loop()

    # Graceful shutdown on SIGINT / SIGTERM
    stop_event = asyncio.Event()

    def _handle_signal():
        if not stop_event.is_set():
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass  # Windows

    try:
        await agent.start()
        await stop_event.wait()
    except Exception as exc:
        print(f"[FATAL] {exc}", file=sys.stderr)
        raise
    finally:
        await agent.stop()


if __name__ == "__main__":
    main()
