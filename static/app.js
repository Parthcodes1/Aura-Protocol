"use strict";

// ── State ─────────────────────────────────────────────────────────────────
let ws = null;
let lastState = {};
const MAX_EVENTS = 100;
const eventLog = [];

// D3 graph state
let graphNodes = [];
let graphLinks = [];
let simulation = null;
let svgSel = null;

// ── WebSocket ─────────────────────────────────────────────────────────────
function connectWS() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.onopen = () => {
    setStatus(true);
    console.log("[WS] connected");
  };

  ws.onmessage = (ev) => {
    try {
      const msg = JSON.parse(ev.data);
      if (msg.type === "state_update") handleStateUpdate(msg);
      else if (msg.type === "chain_event") pushEvent(msg.data);
    } catch(e) { console.error("[WS] parse error", e); }
  };

  ws.onclose = () => {
    setStatus(false);
    setTimeout(connectWS, 3000);
  };

  ws.onerror = (e) => { console.error("[WS] error", e); };
}

function setStatus(connected) {
  const el = document.getElementById("conn-status");
  el.innerHTML = connected
    ? `<span class="status-dot"></span> Live`
    : `<span class="status-dot"></span> Disconnected`;
  el.className = "badge " + (connected ? "badge-green badge-live live" : "badge badge-red badge-live offline");
}

// ── Main update handler ────────────────────────────────────────────────────
function handleStateUpdate(msg) {
  lastState = msg;

  // Agent ID
  setText("agent-id", shortId(msg.agent_id));

  // ── Swarm state ────────────────────────────────────────────────────────
  const sw = msg.swarm || {};
  setText("s-leader",  shortId(sw.leader  || "—"));
  setText("s-epoch",   `${sw.epoch ?? "—"} / ${msg.raft?.term ?? "—"}`);
  setText("s-version", sw.state_version ?? "—");
  setText("s-peers",   (sw.peers || []).length);
  const role = msg.raft?.role || "—";
  document.getElementById("s-role").innerHTML = rolePill(role);
  setText("s-hash",    shortHash(sw.state_hash));

  const leaderCard = document.getElementById("s-leader")?.closest(".card");
  if (leaderCard) leaderCard.classList.toggle("leader-card", !!sw.leader && sw.leader !== "—");

  // ── Chain cards ────────────────────────────────────────────────────────
  const chains = msg.chains?.chains || {};
  updateChainCard("ethereum", "eth", "Block", chains.ethereum);
  updateChainCard("sui",      "sui", "Checkpoint", chains.sui);
  updateChainCard("solana",   "sol", "Slot", chains.solana);

  // Aggregate hash
  const aggHash = msg.chains?.aggregate_hash || "—";
  setText("agg-hash", aggHash);
  const upd = msg.chains?.last_updated;
  setText("agg-updated", upd ? `Updated ${timeSince(upd * 1000)} ago` : "—");

  // ── Peers table ────────────────────────────────────────────────────────
  updatePeersTable(msg.peers || []);

  // ── Network graph ──────────────────────────────────────────────────────
  updateGraph(msg.agent_id, msg.peers || [], sw.leader);
}

function updateChainCard(chain, prefix, blockLabel, data) {
  const card = document.getElementById(`card-${chain}`);
  if (!data) {
    setText(`${prefix}-block`,  `${blockLabel} —`);
    setText(`${prefix}-hash`,   "—");
    setText(`${prefix}-events`, "0 events");
    document.getElementById(`${prefix}-status`).innerHTML = dotStatus(false);
    card.classList.remove("verified");
    card.classList.remove("alert");
    return;
  }
  setText(`${prefix}-block`,  `${blockLabel} ${fmtNum(data.block_number)}`);
  setText(`${prefix}-hash`,   shortHash(data.state_hash || data.block_hash));
  const eventCount = data.events_count ?? 0;
  setText(`${prefix}-events`, `${eventCount} events`);
  const verified = !!data.verified;
  document.getElementById(`${prefix}-status`).innerHTML = dotStatus(verified);
  card.classList.toggle("verified", verified);

  const needsAlert = chain === "sui" && eventCount === 0;
  card.classList.toggle("alert", needsAlert);
}

function dotStatus(verified) {
  return verified
    ? `<span class="dot dot-green ping"></span> Verified`
    : `<span class="dot dot-yellow ping"></span> Pending`;
}

function updatePeersTable(peers) {
  const tbody = document.getElementById("peers-tbody");
  if (!peers.length) { tbody.innerHTML = `<tr><td colspan="5" style="color:var(--text2);text-align:center">No peers</td></tr>`; return; }
  tbody.innerHTML = peers.map(p => `
    <tr>
      <td class="mono">${shortId(p.peer_id || p.agent_id || "?")}</td>
      <td>${rolePill(p.role || "—")}</td>
      <td class="${scoreClass(p.reputation ?? 50)}">${(p.reputation ?? 50).toFixed(1)}</td>
      <td>${p.age_s != null ? p.age_s.toFixed(1) + "s" : "—"}</td>
      <td>${p.stale ? '<span class="dot dot-red"></span>' : '<span class="dot dot-green"></span>'}</td>
    </tr>
  `).join("");
}

function scoreClass(s) { return s > 60 ? "green" : s > 30 ? "" : "orange"; }

// ── Event feed ─────────────────────────────────────────────────────────────
function pushEvent(data) {
  eventLog.unshift(data);
  if (eventLog.length > MAX_EVENTS) eventLog.pop();
  renderEvents();
}

function renderEvents() {
  const feed = document.getElementById("event-feed");
  feed.innerHTML = eventLog.map(e => {
    const chain = (e.chain || "").toLowerCase();
    const cls   = { ethereum: "eth", sui: "sui", solana: "sol" }[chain] || "";
    return `<div class="event-item">
      <span class="event-ts">${fmtTime(e.timestamp)}</span>
      <span class="event-chain ${cls}">${chain.toUpperCase() || "?"}</span>
      <span class="event-text">${e.event_type || "Event"} @ block ${fmtNum(e.block_number)}</span>
    </div>`;
  }).join("");
}

// ── D3 Network Graph ───────────────────────────────────────────────────────
function initGraph() {
  const svg = d3.select("#network-graph");
  const rect = svg.node().getBoundingClientRect();
  const W = rect.width || 480, H = 320;

  svg.attr("viewBox", `0 0 ${W} ${H}`);
  svgSel = svg;

  const defs = svg.append("defs");
  const glow = defs.append("filter").attr("id", "node-glow").attr("x", "-60%").attr("y", "-60%").attr("width", "220%").attr("height", "220%");
  glow.append("feGaussianBlur").attr("stdDeviation", 4).attr("result", "blur");
  glow.append("feColorMatrix").attr("in", "blur").attr("type", "matrix").attr("values", "1 0 0 0 0  0 1 0 0 0.02  0 0 1 0 0.15  0 0 0 1 0").attr("result", "glow");
  const merge = glow.append("feMerge");
  merge.append("feMergeNode").attr("in", "glow");
  merge.append("feMergeNode").attr("in", "SourceGraphic");

  simulation = d3.forceSimulation()
    .force("link",   d3.forceLink().id(d => d.id).distance(90))
    .force("charge", d3.forceManyBody().strength(-160))
    .force("center", d3.forceCenter(W/2, H/2))
    .force("collision", d3.forceCollide(28));
}

function updateGraph(myId, peers, leader) {
  if (!svgSel) return;
  const svg = svgSel;

  // Build node/link data
  const nodeMap = {};
  const addNode = (id, isMe) => {
    if (!nodeMap[id]) nodeMap[id] = { id, isMe, isLeader: id === leader };
  };

  addNode(myId, true);
  peers.forEach(p => { const pid = p.peer_id || p.agent_id; if (pid) addNode(pid, false); });

  const nodes = Object.values(nodeMap);
  const links = peers
    .map(p => ({ source: myId, target: p.peer_id || p.agent_id }))
    .filter(l => l.target);

  // Preserve positions for stable animation
  const existing = {};
  graphNodes.forEach(n => { existing[n.id] = n; });
  nodes.forEach(n => {
    if (existing[n.id]) { n.x = existing[n.id].x; n.y = existing[n.id].y; }
  });

  graphNodes = nodes;
  graphLinks = links;

  // Render links
  const link = svg.selectAll(".link")
    .data(links, d => `${d.source}-${d.target}`)
    .join("line")
    .attr("class", "link active")
    .attr("stroke-dasharray", "10 12");

  // Render nodes
  const node = svg.selectAll(".node")
    .data(nodes, d => d.id)
    .join(enter => {
      const g = enter.append("g").attr("class", "node");
      g.append("polygon");
      g.append("text").attr("class", "node-label").attr("dy", 28).attr("text-anchor", "middle");
      return g;
    });

  node
    .classed("is-me", d => d.isMe)
    .classed("is-leader", d => d.isLeader)
    .attr("filter", d => d.isMe ? "url(#node-glow)" : null);

  node.select("polygon")
    .attr("points", d => hexPoints(d.isMe ? 18 : d.isLeader ? 16 : 12))
    .attr("fill", d => d.isMe ? "#0f4c64" : d.isLeader ? "#5a4211" : "#111827")
    .attr("stroke", d => d.isMe ? "#00f2ff" : d.isLeader ? "#ffb800" : "#8ea2ba");

  node.select("text")
    .text(d => (d.id || "").slice(-6));

  simulation
    .nodes(nodes)
    .on("tick", () => {
      link.attr("x1", d => d.source.x).attr("y1", d => d.source.y)
          .attr("x2", d => d.target.x).attr("y2", d => d.target.y);
      node.attr("transform", d => `translate(${d.x ?? 0},${d.y ?? 0})`);
    });

  simulation.force("link").links(links);
  simulation.alpha(0.3).restart();
}

// ── Helpers ────────────────────────────────────────────────────────────────
function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val ?? "—";
}

function shortId(id) {
  if (!id || id === "—") return "—";
  return id.length > 16 ? id.slice(0, 8) + "…" + id.slice(-6) : id;
}

function shortHash(h) {
  if (!h || h === "—") return "—";
  return h.slice(0, 12) + "…" + h.slice(-6);
}

function hexPoints(r) {
  const points = [];
  for (let i = 0; i < 6; i += 1) {
    const angle = (Math.PI / 3) * i + Math.PI / 6;
    points.push(`${Math.cos(angle) * r},${Math.sin(angle) * r}`);
  }
  return points.join(" ");
}

function rolePill(r) {
  const cls = { leader: "role-leader", follower: "role-follower", candidate: "role-candidate" }[r] || "";
  return `<span class="${cls}">${r}</span>`;
}

function fmtNum(n) {
  if (n == null) return "—";
  return Number(n).toLocaleString();
}

function fmtTime(ts) {
  if (!ts) return "—";
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString();
}

function timeSince(ms) {
  const s = Math.floor((Date.now() - ms) / 1000);
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s/60)}m`;
  return `${Math.floor(s/3600)}h`;
}

// ── Boot ───────────────────────────────────────────────────────────────────
window.addEventListener("DOMContentLoaded", () => {
  initGraph();
  connectWS();

  // Also poll REST for initial load
  fetch("/api/status")
    .then(r => r.json())
    .then(d => {
      handleStateUpdate({
        agent_id: d.agent_id,
        swarm:    d.swarm,
        raft:     d.raft,
        chains:   d.chains,
        peers:    [],
      });
    })
    .catch(console.error);
});
