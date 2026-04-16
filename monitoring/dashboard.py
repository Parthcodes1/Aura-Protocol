"""
Async web dashboard: REST API + WebSocket for real-time updates.
Serves the static frontend from /static and streams state via WS.
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from aiohttp import web, WSMsgType

from monitoring.logger import get_logger

log = get_logger(__name__)

STATIC_DIR = Path(__file__).parent.parent / "static"


class Dashboard:
    """
    aiohttp-based dashboard.
    REST endpoints:
      GET /api/status       — full agent status
      GET /api/state        — swarm state
      GET /api/peers        — peer list
      GET /api/chains       — cross-chain summary
      GET /api/reputation   — reputation table
      GET /api/metrics      — basic metrics snapshot
    WebSocket:
      GET /ws               — real-time push (state updates every 2s)
    """

    def __init__(
        self,
        agent_id: str,
        port: int,
        state_fn,          # callable → dict (swarm state)
        peers_fn,          # callable → list
        chains_fn,         # callable → dict (cross-chain)
        reputation_fn,     # callable → list
        raft_fn,           # callable → dict (raft status)
        metrics_fn,        # callable → dict
    ) -> None:
        self._agent_id    = agent_id
        self._port        = port
        self._state_fn    = state_fn
        self._peers_fn    = peers_fn
        self._chains_fn   = chains_fn
        self._reputation_fn = reputation_fn
        self._raft_fn     = raft_fn
        self._metrics_fn  = metrics_fn

        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None
        self._ws_clients: Set[web.WebSocketResponse] = set()
        self._push_task: Optional[asyncio.Task] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._app = web.Application()
        self._app.router.add_get("/",           self._serve_index)
        self._app.router.add_get("/ws",         self._ws_handler)
        self._app.router.add_get("/api/status", self._api_status)
        self._app.router.add_get("/api/state",  self._api_state)
        self._app.router.add_get("/api/peers",  self._api_peers)
        self._app.router.add_get("/api/chains", self._api_chains)
        self._app.router.add_get("/api/reputation", self._api_reputation)
        self._app.router.add_get("/api/metrics", self._api_metrics)
        self._app.router.add_static("/static",  STATIC_DIR, show_index=False)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self._port)
        await site.start()

        self._push_task = asyncio.create_task(self._push_loop())
        log.info("dashboard.started", port=self._port, url=f"http://0.0.0.0:{self._port}")

    async def stop(self) -> None:
        if self._push_task:
            self._push_task.cancel()
        for ws in list(self._ws_clients):
            await ws.close()
        if self._runner:
            await self._runner.cleanup()

    # ── HTTP routes ───────────────────────────────────────────────────────────

    async def _serve_index(self, request: web.Request) -> web.FileResponse:
        return web.FileResponse(STATIC_DIR / "index.html")

    async def _api_status(self, request: web.Request) -> web.Response:
        data = {
            "agent_id":  self._agent_id,
            "timestamp": time.time(),
            "swarm":     self._state_fn(),
            "raft":      self._raft_fn(),
            "chains":    self._chains_fn(),
            "peer_count": len(self._peers_fn()),
        }
        return self._json(data)

    async def _api_state(self, request: web.Request) -> web.Response:
        return self._json(self._state_fn())

    async def _api_peers(self, request: web.Request) -> web.Response:
        return self._json({"peers": self._peers_fn()})

    async def _api_chains(self, request: web.Request) -> web.Response:
        return self._json(self._chains_fn())

    async def _api_reputation(self, request: web.Request) -> web.Response:
        return self._json({"agents": self._reputation_fn()})

    async def _api_metrics(self, request: web.Request) -> web.Response:
        return self._json(self._metrics_fn())

    # ── WebSocket ─────────────────────────────────────────────────────────────

    async def _ws_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=15)
        await ws.prepare(request)
        self._ws_clients.add(ws)
        log.debug("ws.client_connected", total=len(self._ws_clients))
        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT and msg.data == "ping":
                    await ws.send_str("pong")
                elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                    break
        finally:
            self._ws_clients.discard(ws)
            log.debug("ws.client_disconnected", total=len(self._ws_clients))
        return ws

    async def _push_loop(self) -> None:
        """Push state updates to all WebSocket clients every 2 seconds."""
        while True:
            try:
                await asyncio.sleep(2)
                if not self._ws_clients:
                    continue

                payload = json.dumps({
                    "type":      "state_update",
                    "timestamp": time.time(),
                    "agent_id":  self._agent_id,
                    "swarm":     self._state_fn(),
                    "raft":      self._raft_fn(),
                    "chains":    self._chains_fn(),
                    "peers":     self._peers_fn(),
                }, default=str)

                dead = set()
                for ws in list(self._ws_clients):
                    try:
                        await ws.send_str(payload)
                    except Exception:
                        dead.add(ws)
                self._ws_clients -= dead

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning("ws.push_error", error=str(exc))

    # ── Utilities ─────────────────────────────────────────────────────────────

    @staticmethod
    def _json(data: Any) -> web.Response:
        return web.Response(
            body=json.dumps(data, default=str),
            content_type="application/json",
            headers={"Access-Control-Allow-Origin": "*"},
        )

    async def broadcast(self, event: str, data: Any) -> None:
        """Push a custom event to all connected WS clients."""
        payload = json.dumps({"type": event, "data": data, "timestamp": time.time()}, default=str)
        dead = set()
        for ws in list(self._ws_clients):
            try:
                await ws.send_str(payload)
            except Exception:
                dead.add(ws)
        self._ws_clients -= dead
