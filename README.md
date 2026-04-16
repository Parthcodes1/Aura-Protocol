# Cross-Chain Oracle Agent Network

A Byzantine fault-tolerant, peer-to-peer oracle swarm that watches **Ethereum**, **Sui**, and **Solana** simultaneously. Ten agents self-organize, elect a leader via Raft consensus, and reach quorum (67% agreement) on observed chain state before publishing a verified cross-chain aggregate hash.

---

## Quick Start

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose)
- That's it — Python and dependencies are inside the container

### Run the full 10-agent swarm

```bash
# 1. Build and start all 10 agents in the background
docker compose up --build -d

# 2. Watch the swarm come online (takes ~15 seconds)
docker compose logs -f agent_1

# 3. Open the dashboard
open http://localhost:8080
```

**Stop everything:**

```bash
docker compose down
```

---

## Running It Perfectly (Step by Step)

### Step 1 — Build

```bash
docker compose build
```

This builds the Python image once. All 10 agents share it.

> First build takes 3–5 minutes (downloads Python 3.11-slim and installs all dependencies).  
> Subsequent builds take ~10 seconds (only changed files are rebuilt).

### Step 2 — Start

```bash
docker compose up -d
```

This starts all 10 agents in detached mode (background).

### Step 3 — Verify the swarm is healthy

```bash
# All 10 containers should show "Up"
docker compose ps

# Check leader election + chain verification
curl -s http://localhost:8080/api/status | python3 -m json.tool
```

You should see something like:

```json
{
  "agent_id": "agent_1",
  "raft": {
    "role": "follower",
    "leader": "agent_6",
    "peers": 9
  },
  "chains": {
    "chains": {
      "ethereum": { "verified": true, "block_number": 21900000 },
      "sui":      { "verified": true, "block_number": 264448000 },
      "solana":   { "verified": true, "block_number": 412895000 }
    },
    "aggregate_hash": "a26389558d567cd3..."
  }
}
```

### Step 4 — Open dashboards

| Agent | Dashboard | Metrics |
|-------|-----------|---------|
| agent_1 (seed) | http://localhost:8080 | http://localhost:9090/metrics |
| agent_2 | http://localhost:8081 | http://localhost:9091/metrics |
| agent_3 | http://localhost:8082 | http://localhost:9092/metrics |
| agent_4 | http://localhost:8083 | http://localhost:9093/metrics |
| agent_5 | http://localhost:8084 | http://localhost:9094/metrics |
| agent_6 | http://localhost:8085 | http://localhost:9095/metrics |
| agent_7 | http://localhost:8086 | http://localhost:9096/metrics |
| agent_8 | http://localhost:8087 | http://localhost:9097/metrics |
| agent_9 | http://localhost:8088 | http://localhost:9098/metrics |
| agent_10 | http://localhost:8089 | http://localhost:9099/metrics |

### Step 5 — Watch logs

```bash
# All agents together
docker compose logs -f

# Specific agent
docker compose logs -f agent_1

# Only errors
docker compose logs --since 60s agent_1 | grep -E "error|ERROR"
```

### Step 6 — Stop or restart

```bash
docker compose down           # stop and remove containers
docker compose restart        # restart all agents
docker compose restart agent_1  # restart just one agent
```

---

## What Happens After Launch

| Time | Event |
|------|-------|
| 0–5 s | Agents start, TCP mesh forms, peers connect |
| 5–15 s | Raft election: one agent wins majority vote and becomes leader |
| 10–30 s | Chain watchers fetch first blocks; oracle votes reach quorum |
| 15 s+ | All 3 chains show **Verified**; aggregate hash updates every block |

The dashboard at http://localhost:8080 shows:

- **Swarm State** — leader, epoch, term, peer count, state hash
- **Chain Cards** — ETH / SUI / SOL block number, hash, Verified status
- **Network Graph** — live D3 force-directed peer topology
- **Peer Table** — per-agent role, reputation score, age
- **Aggregate Hash** — combined SHA-256 of all verified chain states
- **Live Event Feed** — real-time transfers, swaps, Solana program logs

---

## Using Your Own RPC Endpoints (Recommended)

The default public RPCs (`ethereum.publicnode.com`, `fullnode.mainnet.sui.io`, `api.mainnet-beta.solana.com`) have rate limits. For reliable operation, use private endpoints:

**Option A — Environment variables before `docker compose up`:**

```bash
export ETH_RPC_URL="https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY"
export SUI_RPC_URL="https://fullnode.mainnet.sui.io:443"
export SOL_RPC_URL="https://solana-mainnet.g.alchemy.com/v2/YOUR_KEY"
docker compose up -d
```

**Option B — Edit `docker-compose.yml` directly:**

```yaml
x-agent-base: &agent-base
  environment:
    ETH_RPC_URL: "https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY"
    SUI_RPC_URL: "https://fullnode.mainnet.sui.io:443"
    SOL_RPC_URL: "https://solana-mainnet.g.alchemy.com/v2/YOUR_KEY"
```

Free RPC providers: [Alchemy](https://www.alchemy.com), [Infura](https://infura.io), [QuickNode](https://www.quicknode.com)

---

## Running Without Docker (dev mode)

```bash
# Install Python 3.10+ dependencies
pip install -r requirements.txt

# Single agent with live dashboard
python3 agent.py \
  --agent-id dev_node \
  --port 4001 \
  --chain ethereum \
  --chain sui \
  --chain solana \
  --dashboard-port 8080 \
  --metrics-port 9090 \
  --log-format pretty \
  --log-level DEBUG

# Or via npm
npm run dev
```

**Manual 3-agent local swarm (3 terminals):**

```bash
# Terminal 1 — seed node
python3 agent.py --agent-id agent_1 --port 4001 \
  --chain ethereum --chain sui --chain solana \
  --dashboard-port 8080 --metrics-port 9090 --log-format pretty

# Terminal 2
python3 agent.py --agent-id agent_2 --port 4002 \
  --bootstrap /ip4/127.0.0.1/tcp/4001 \
  --chain ethereum --chain sui --chain solana \
  --dashboard-port 8081 --metrics-port 9091 --log-format pretty

# Terminal 3
python3 agent.py --agent-id agent_3 --port 4003 \
  --bootstrap /ip4/127.0.0.1/tcp/4001 \
  --chain ethereum --chain sui --chain solana \
  --dashboard-port 8082 --metrics-port 9092 --log-format pretty
```

---

## Hosting / Deployment

### ⚠️ Why Vercel won't work for this project

Vercel is designed for **serverless functions and static sites**. This project requires:

- Persistent TCP servers (P2P mesh networking on port 4001)
- Long-lived WebSocket connections (dashboard real-time push)
- SQLite on the filesystem (state + reputation persistence)
- Multi-container Docker orchestration
- Inter-container networking between 10 agents

None of these are supported by Vercel's serverless runtime. Vercel functions time out after 10–60 seconds and have no persistent filesystem.

**You CAN deploy the static dashboard to Vercel** if the backend runs elsewhere — see below.

---

### Option 1 — Railway (easiest, recommended)

Railway supports Docker Compose natively.

```bash
# 1. Install Railway CLI
npm install -g @railway/cli

# 2. Login
railway login

# 3. Deploy from project root
railway init
railway up
```

Set environment variables in the Railway dashboard:

```
ETH_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
SUI_RPC_URL=https://fullnode.mainnet.sui.io:443
SOL_RPC_URL=https://api.mainnet-beta.solana.com
```

Railway will expose agent_1's dashboard port (8080) on a public URL.

> **Cost:** Railway free tier gives $5/month credit. 10 agents use ~2 vCPU and 2 GB RAM total.

---

### Option 2 — Fly.io

Fly.io supports persistent Docker containers with global distribution.

```bash
# Install flyctl
curl -L https://fly.io/install.sh | sh

# Login
fly auth login

# Deploy agent_1 as the seed node
fly launch --dockerfile Dockerfile \
  --name oracle-agent-1 \
  --region iad \
  --no-deploy

# Set secrets
fly secrets set ETH_RPC_URL="https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY"

# Deploy
fly deploy

# Scale to multiple regions for a true decentralized swarm
fly scale count 3 --region iad,lhr,nrt
```

Each Fly.io machine runs one agent. Bootstrap subsequent agents with the seed node's Fly.io internal address.

> **Cost:** ~$5–10/month for 3 agents on shared-cpu-1x machines.

---

### Option 3 — DigitalOcean Droplet (VPS, most control)

Any Linux VPS with Docker works. DigitalOcean, Linode, Vultr, Hetzner are all good options.

```bash
# On the VPS (Ubuntu 22.04)
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker

# Clone the project
git clone <your-repo-url>
cd foxmq-warmup-upgraded-main

# Set your RPC endpoints
export ETH_RPC_URL="https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY"

# Start
docker compose up --build -d

# Dashboard is now at http://YOUR_VPS_IP:8080
```

Open the firewall for dashboard ports:

```bash
ufw allow 8080:8089/tcp   # dashboards
ufw allow 4001:4010/tcp   # P2P ports (only needed for external agent bootstrap)
```

> **Cost:** A $12/month DigitalOcean droplet (2 vCPU / 2 GB RAM) handles 10 agents comfortably.

---

### Option 4 — Deploy dashboard to Vercel + backend on VPS

If you want the dashboard publicly accessible via HTTPS on a Vercel URL while running the backend on a VPS:

**Step 1 — Deploy backend on a VPS (see Option 3)**

**Step 2 — Update dashboard to point to your VPS**

Edit `static/app.js` and change the WebSocket and API base URL:

```js
// Change this line
const API_BASE = window.location.origin;
const WS_URL   = `ws://${window.location.host}/ws`;

// To your VPS IP or domain
const API_BASE = "http://YOUR_VPS_IP:8080";
const WS_URL   = "ws://YOUR_VPS_IP:8080/ws";
```

**Step 3 — Deploy static files to Vercel**

```bash
# Install Vercel CLI
npm install -g vercel

# Deploy static folder
cd static
vercel deploy
```

This gives you a `https://your-app.vercel.app` URL serving the dashboard, which connects to your VPS backend over WebSocket.

---

## REST API

| Endpoint | Description |
|----------|-------------|
| `GET /api/status` | Full agent status snapshot |
| `GET /api/state` | Swarm state (leader, epoch, version, hash) |
| `GET /api/peers` | All known peers with reputation and staleness |
| `GET /api/chains` | Per-chain verified state + aggregate hash |
| `GET /api/reputation` | Reputation scores for all agents |
| `GET /api/metrics` | Runtime metrics snapshot |
| `GET /ws` | WebSocket — real-time state push every 2 s |

```bash
# Examples
curl http://localhost:8080/api/status  | python3 -m json.tool
curl http://localhost:8080/api/chains  | python3 -m json.tool
curl http://localhost:8080/api/peers   | python3 -m json.tool
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      Oracle Agent                            │
│                                                              │
│  ┌─────────────┐   ┌──────────────┐   ┌──────────────────┐  │
│  │ Chain        │   │ Oracle       │   │ State Machine    │  │
│  │ Watchers     │──▶│ Consensus    │──▶│ (Raft Leader     │  │
│  │ ETH/SUI/SOL  │   │ (quorum 67%) │   │  Election)       │  │
│  └─────────────┘   └──────────────┘   └──────────────────┘  │
│         │                 │                     │             │
│         ▼                 ▼                     ▼             │
│  ┌─────────────┐   ┌──────────────┐   ┌──────────────────┐  │
│  │ P2P Host    │   │ Cross-Chain  │   │ Reputation       │  │
│  │ (asyncio    │   │ Aggregator   │   │ Manager          │  │
│  │  TCP mesh)  │   │ SHA-256      │   │ (SQLite)         │  │
│  └─────────────┘   └──────────────┘   └──────────────────┘  │
│                                                              │
│  Dashboard :8080  ·  Metrics :9090  ·  P2P :4001            │
└──────────────────────────────────────────────────────────────┘
```

**10-node layout:**

| Agents | Chains | Purpose |
|--------|--------|---------|
| 1, 2, 3 | ETH + SUI + SOL | Full watchers (seed + primaries) |
| 4, 5, 6 | ETH + SUI | Ethereum/Sui quorum backup |
| 7, 8 | SUI + SOL | Sui/Solana quorum backup |
| 9, 10 | None | Consensus-only (Raft voters, no chain I/O) |

---

## Configuration

All settings accept CLI flags, environment variables, or `config.toml`.

**`config.toml`:**

```toml
[network]
port             = 4001
max_connections  = 50

[ethereum]
enabled       = true
rpc_url       = "https://ethereum.publicnode.com"
poll_interval = 12.0

[sui]
enabled       = true
rpc_url       = "https://fullnode.mainnet.sui.io:443"
poll_interval = 3.0

[solana]
enabled       = true
rpc_url       = "https://api.mainnet-beta.solana.com"
poll_interval = 1.0

[consensus]
heartbeat_interval   = 5.0
election_timeout_min = 3.0
election_timeout_max = 6.0
consensus_threshold  = 0.67
oracle_quorum        = 2

[monitoring]
dashboard_port = 8080
metrics_port   = 9090
log_level      = "INFO"
log_format     = "pretty"
```

**Environment variables:**

| Variable | Description | Default |
|----------|-------------|---------|
| `ETH_RPC_URL` | Ethereum JSON-RPC endpoint | `ethereum.publicnode.com` |
| `SUI_RPC_URL` | Sui fullnode RPC | `fullnode.mainnet.sui.io` |
| `SOL_RPC_URL` | Solana RPC | `api.mainnet-beta.solana.com` |
| `MIN_CONSENSUS_PEERS` | Min peers before quorum | `6` |
| `CONSENSUS_THRESHOLD` | Quorum fraction | `0.67` |
| `LOG_LEVEL` | `DEBUG` / `INFO` / `WARNING` | `INFO` |
| `LOG_FORMAT` | `pretty` / `json` | `json` |
| `ANCHOR_ENABLED` | Anchor hashes on Sui testnet | `false` |

---

## Prometheus Monitoring

```bash
# Start with Prometheus sidecar
docker compose --profile monitoring up -d

# Prometheus UI
open http://localhost:9100
```

Key metrics at `:9090/metrics`:

| Metric | Description |
|--------|-------------|
| `oracle_consensus_successes_total` | Rounds reaching quorum |
| `oracle_chain_block_height` | Latest verified block per chain |
| `oracle_peers_connected` | Active peer count |
| `oracle_election_term` | Current Raft term |
| `oracle_is_leader` | 1 if this agent is the current leader |

---

## Troubleshooting

**Ethereum shows "Pending" after 30 s**
→ The public Ethereum RPC is rate-limited. Set `ETH_RPC_URL` to an Alchemy or Infura key.

**Peers show 0**
→ Wait 15 s for bootstrap. Check: `docker compose logs agent_2 | grep tcp.connected`

**Leader election not converging**
→ Normal in first 15 s. If stuck after 30 s: `docker compose restart`

**`chain.fetch_error` for Solana**
→ Public Solana RPC has strict rate limits. This is expected and handled with exponential backoff. Chain verification continues via other agents in the swarm.

**Docker says "port already in use"**
→ Something is already using ports 4001–4010 or 8080–8089. Stop it or change the port mappings in `docker-compose.yml`.

---

## Project Structure

```
.
├── agent.py              # Main entry point & orchestrator
├── config.py             # Pydantic config (TOML + env + CLI)
├── chains/
│   ├── base.py           # Abstract BaseChainWatcher, ChainBlock
│   ├── ethereum.py       # Ethereum watcher (web3.py v6)
│   ├── sui.py            # Sui watcher (JSON-RPC via httpx)
│   └── solana.py         # Solana watcher (solana-py)
├── consensus/
│   ├── raft.py           # Raft leader election
│   ├── heartbeat.py      # Peer heartbeat & stale detection
│   └── oracle.py         # Oracle quorum consensus engine
├── crypto/
│   ├── keys.py           # Ed25519 keypair management
│   ├── signing.py        # Message signing & verification
│   └── nonce.py          # Replay-attack prevention
├── network/
│   ├── host.py           # P2PHost (asyncio TCP mesh)
│   ├── pubsub.py         # Reliable pub/sub with signing & retry
│   ├── discovery.py      # Bootstrap + mDNS peer discovery
│   └── protocols.py      # Message types & Pydantic models
├── state/
│   ├── machine.py        # Distributed swarm state machine
│   ├── store.py          # SQLite persistence (aiosqlite)
│   ├── aggregator.py     # Cross-chain state aggregation
│   └── hasher.py         # Deterministic SHA-256 hashing
├── reputation/
│   └── scorer.py         # Per-agent reputation (SQLite)
├── blockchain/
│   └── anchoring.py      # On-chain hash anchoring (Sui)
├── monitoring/
│   ├── dashboard.py      # aiohttp REST + WebSocket server
│   ├── logger.py         # structlog structured logging
│   └── metrics.py        # Prometheus metrics registry
├── static/
│   ├── index.html        # Dashboard SPA
│   ├── app.js            # D3 network graph + WebSocket client
│   └── style.css         # Dashboard styles
├── docker-compose.yml    # 10-node Docker cluster
├── Dockerfile
├── requirements.txt
└── package.json
```

---

## License

MIT
