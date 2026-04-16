"""
libp2p host wrapper.
Handles lifecycle, connection management, and stream protocol setup.
Falls back to a lightweight asyncio-TCP transport when py-libp2p is unavailable.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Callable, Dict, List, Optional, Set

from monitoring.logger import get_logger

log = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Try to import py-libp2p; graceful fallback if missing
# ─────────────────────────────────────────────────────────────────────────────
try:
    from libp2p import new_host as _libp2p_new_host
    from libp2p.peer.peerinfo import info_from_p2p_addr
    from libp2p.pubsub.gossipsub import GossipSub
    from libp2p.pubsub.pubsub import Pubsub
    from multiaddr import Multiaddr
    HAS_LIBP2P = True
except ImportError:
    HAS_LIBP2P = False
    log.warning("libp2p_unavailable", msg="py-libp2p not installed; using asyncio-TCP fallback")


# ─────────────────────────────────────────────────────────────────────────────
# Connection record
# ─────────────────────────────────────────────────────────────────────────────

class PeerConnection:
    __slots__ = ("peer_id", "address", "reader", "writer", "connected_at")

    def __init__(self, peer_id: str, address: str, reader, writer) -> None:
        import time
        self.peer_id = peer_id
        self.address = address
        self.reader = reader
        self.writer = writer
        self.connected_at = time.time()

    async def close(self) -> None:
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# P2PHost — main class
# ─────────────────────────────────────────────────────────────────────────────

class P2PHost:
    """
    Wraps a libp2p BasicHost (or asyncio-TCP fallback).
    Provides:
      - start / stop lifecycle
      - connect_to_peer
      - open_stream for direct protocols
      - delegate to PubSubManager for topic pub/sub
    """

    def __init__(
        self,
        agent_id: str,
        host_str: str,
        port: int,
        key_pair,                # libp2p KeyPair or None
        max_connections: int = 50,
    ) -> None:
        self.agent_id = agent_id
        self.host_str = host_str
        self.port = port
        self._key_pair = key_pair
        self._max_connections = max_connections

        self._host = None          # libp2p BasicHost
        self._pubsub: Optional[PubSubManager] = None
        self._connections: Dict[str, PeerConnection] = {}
        self._stream_handlers: Dict[str, Callable] = {}
        self._peer_id: Optional[str] = None
        self._running = False
        self._server = None        # asyncio TCP server (fallback)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if HAS_LIBP2P and self._key_pair is not None:
            try:
                await self._start_libp2p()
            except Exception as exc:
                # py-libp2p uses the trio event loop internally; it cannot run
                # inside an asyncio event loop.  Fall back to the asyncio-TCP
                # transport which is fully functional for this project.
                log.warning(
                    "libp2p.start_failed",
                    error=str(exc),
                    fallback="asyncio-tcp",
                )
                await self._start_tcp_fallback()
        else:
            await self._start_tcp_fallback()
        self._running = True
        log.info("p2p_host.started", peer_id=self._peer_id, port=self.port, backend="libp2p" if HAS_LIBP2P else "tcp")

    async def stop(self) -> None:
        self._running = False
        if self._pubsub:
            await self._pubsub.stop()
        for conn in list(self._connections.values()):
            await conn.close()
        if self._host:
            try:
                await self._host.close()
            except Exception:
                pass
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        log.info("p2p_host.stopped")

    # ── libp2p backend ────────────────────────────────────────────────────────

    async def _start_libp2p(self) -> None:
        listen_addr = Multiaddr(f"/ip4/{self.host_str}/tcp/{self.port}")
        try:
            self._host = await _libp2p_new_host(
                key_pair=self._key_pair,
                listen_addrs=[listen_addr],
            )
        except TypeError:
            # Some versions use sync new_host
            self._host = _libp2p_new_host(key_pair=self._key_pair)
            await self._host.get_network().listen(listen_addr)

        self._peer_id = str(self._host.get_id())

        # Register any pre-declared stream handlers
        for proto, handler in self._stream_handlers.items():
            self._host.set_stream_handler(proto, handler)

        # Set up GossipSub
        self._pubsub = PubSubManager(self._host, self._key_pair, self.agent_id, connections=self._connections)
        await self._pubsub.start()

    # ── asyncio-TCP fallback ──────────────────────────────────────────────────

    async def _start_tcp_fallback(self) -> None:
        import hashlib
        self._peer_id = hashlib.sha256(self.agent_id.encode()).hexdigest()[:40]

        self._server = await asyncio.start_server(
            self._handle_incoming_tcp,
            self.host_str,
            self.port,
        )
        # Pass the connections dict so PubSubManager can broadcast outbound messages
        self._pubsub = PubSubManager(
            None, None, self.agent_id, connections=self._connections
        )
        await self._pubsub.start()

    # ── TCP wire helpers ──────────────────────────────────────────────────────
    # Message frame (after the initial handshake):
    #   [4 bytes: topic_len][topic UTF-8][4 bytes: data_len][data bytes]
    #
    # Handshake frame (first message on a new connection):
    #   [4 bytes: payload_len][JSON bytes]  (no topic prefix)

    @staticmethod
    async def _read_exactly(reader: asyncio.StreamReader, n: int, timeout: float) -> bytes:
        """Read exactly n bytes; raises on EOF or timeout."""
        return await asyncio.wait_for(reader.readexactly(n), timeout=timeout)

    @staticmethod
    def _encode_frame(topic: str, data: bytes) -> bytes:
        tb = topic.encode("utf-8")
        return (
            len(tb).to_bytes(4, "big") + tb +
            len(data).to_bytes(4, "big") + data
        )

    async def _handle_incoming_tcp(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        try:
            length = int.from_bytes(
                await self._read_exactly(reader, 4, timeout=10.0), "big"
            )
            handshake = json.loads(await self._read_exactly(reader, length, timeout=10.0))
            peer_id = handshake.get("peer_id", str(addr))
            conn = PeerConnection(peer_id, str(addr), reader, writer)
            self._connections[peer_id] = conn
            log.info("tcp.peer_connected", peer=peer_id, addr=str(addr))
            asyncio.create_task(self._read_tcp_messages(conn))
        except Exception as exc:
            log.warning("tcp.handshake_failed", addr=str(addr), error=str(exc))
            writer.close()

    async def _read_tcp_messages(self, conn: PeerConnection) -> None:
        try:
            while self._running:
                # Read topic
                topic_len = int.from_bytes(
                    await self._read_exactly(conn.reader, 4, timeout=30.0), "big"
                )
                topic = (await self._read_exactly(conn.reader, topic_len, timeout=10.0)).decode("utf-8")
                # Read data
                data_len = int.from_bytes(
                    await self._read_exactly(conn.reader, 4, timeout=10.0), "big"
                )
                data = await self._read_exactly(conn.reader, data_len, timeout=10.0)
                if self._pubsub:
                    await self._pubsub._deliver_raw(conn.peer_id, data, topic=topic)
        except (asyncio.CancelledError, asyncio.IncompleteReadError,
                asyncio.TimeoutError, ConnectionResetError):
            pass
        finally:
            self._connections.pop(conn.peer_id, None)
            await conn.close()
            log.info("tcp.peer_disconnected", peer=conn.peer_id)

    # ── Peer management ───────────────────────────────────────────────────────

    async def connect(self, multiaddr_str: str) -> bool:
        """Connect to a peer given its multiaddr string."""
        if HAS_LIBP2P and self._host:
            try:
                peer_info = info_from_p2p_addr(Multiaddr(multiaddr_str))
                await self._host.connect(peer_info)
                log.info("p2p.connected", addr=multiaddr_str)
                return True
            except Exception as exc:
                log.warning("p2p.connect_failed", addr=multiaddr_str, error=str(exc))
                return False
        else:
            return await self._tcp_connect(multiaddr_str)

    async def _tcp_connect(self, multiaddr_str: str) -> bool:
        """Parse /ip4/HOST/tcp/PORT[/p2p/ID] and connect via raw TCP."""
        try:
            parts = multiaddr_str.strip("/").split("/")
            host_idx = parts.index("ip4") + 1
            port_idx = parts.index("tcp") + 1
            host = parts[host_idx]
            port = int(parts[port_idx])
            peer_id = parts[parts.index("p2p") + 1] if "p2p" in parts else f"{host}:{port}"

            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=10.0
            )
            # Send handshake (old framing: 4-byte len + JSON — server expects this)
            handshake = json.dumps({"peer_id": self._peer_id}).encode()
            writer.write(len(handshake).to_bytes(4, "big") + handshake)
            await writer.drain()

            conn = PeerConnection(peer_id, f"{host}:{port}", reader, writer)
            self._connections[peer_id] = conn
            asyncio.create_task(self._read_tcp_messages(conn))
            log.info("tcp.connected", peer=peer_id, addr=f"{host}:{port}")
            return True
        except Exception as exc:
            log.warning("tcp.connect_failed", addr=multiaddr_str, error=str(exc))
            return False

    # ── Stream protocols ──────────────────────────────────────────────────────

    def set_stream_handler(self, protocol: str, handler: Callable) -> None:
        self._stream_handlers[protocol] = handler
        if self._host and HAS_LIBP2P:
            self._host.set_stream_handler(protocol, handler)

    async def new_stream(self, peer_id: str, protocol: str):
        if self._host and HAS_LIBP2P:
            try:
                peer_id_obj = self._host.get_network().connections.get(peer_id)
                if peer_id_obj:
                    return await self._host.new_stream(peer_id_obj, [protocol])
            except Exception as exc:
                log.warning("p2p.stream_failed", peer=peer_id, proto=protocol, error=str(exc))
        return None

    # ── Accessors ─────────────────────────────────────────────────────────────

    @property
    def peer_id(self) -> str:
        return self._peer_id or self.agent_id

    @property
    def pubsub(self) -> "PubSubManager":
        return self._pubsub

    @property
    def connected_peers(self) -> List[str]:
        if self._host and HAS_LIBP2P:
            try:
                return [str(p) for p in self._host.get_network().connections]
            except Exception:
                pass
        return list(self._connections.keys())

    @property
    def listen_addrs(self) -> List[str]:
        if self._host and HAS_LIBP2P:
            try:
                return [str(a) for a in self._host.get_addrs()]
            except Exception:
                pass
        return [f"/ip4/{self.host_str}/tcp/{self.port}"]


# ─────────────────────────────────────────────────────────────────────────────
# PubSubManager — GossipSub wrapper
# ─────────────────────────────────────────────────────────────────────────────

class PubSubManager:
    """
    Wraps libp2p GossipSub pub/sub.
    Falls back to an asyncio-TCP broadcast bus when libp2p unavailable.
    In TCP-fallback mode `connections` must be passed so that publish()
    can forward messages to all connected peers.
    """

    def __init__(
        self,
        host,
        key_pair,
        agent_id: str,
        connections: Optional[Dict] = None,
    ) -> None:
        self._host = host
        self._key_pair = key_pair
        self._agent_id = agent_id
        self._connections = connections  # shared ref to P2PHost._connections
        self._pubsub = None
        self._subscriptions: Dict[str, asyncio.Queue] = {}
        self._handlers: Dict[str, List[Callable]] = {}
        self._running = False
        self._tasks: List[asyncio.Task] = []

    async def start(self) -> None:
        self._running = True
        if self._host and HAS_LIBP2P:
            try:
                from libp2p.typing import TProtocol
                gossipsub = GossipSub(
                    protocols=[TProtocol("/meshsub/1.1.0")],
                    degree=6,
                    degree_low=4,
                    degree_high=12,
                    time_to_live=30,
                )
                self._pubsub = Pubsub(
                    host=self._host,
                    router=gossipsub,
                    my_id=self._host.get_id(),
                    cache_size=256,
                    strict_signing=False,
                )
                log.info("pubsub.started", backend="gossipsub")
            except Exception as exc:
                log.warning("pubsub.gossipsub_failed", error=str(exc), fallback="local_bus")
                self._pubsub = None
        else:
            log.info("pubsub.started", backend="local_bus")

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def subscribe(self, topic: str) -> asyncio.Queue:
        if topic in self._subscriptions:
            return self._subscriptions[topic]

        q: asyncio.Queue = asyncio.Queue(maxsize=16384)
        self._subscriptions[topic] = q

        if self._pubsub and HAS_LIBP2P:
            try:
                libp2p_sub = await self._pubsub.subscribe(topic)
                t = asyncio.create_task(self._bridge_libp2p_to_queue(libp2p_sub, q, topic))
                self._tasks.append(t)
                return q
            except Exception as exc:
                log.warning("pubsub.subscribe_failed", topic=topic, error=str(exc))

        # Fallback: queue is fed manually via _deliver_raw
        return q

    async def publish(self, topic: str, data: bytes) -> None:
        if self._pubsub and HAS_LIBP2P:
            try:
                await self._pubsub.publish(topic, data)
                return
            except Exception as exc:
                log.warning("pubsub.publish_failed", topic=topic, error=str(exc))

        # TCP fallback: deliver locally AND broadcast to all connected peers
        await self._deliver_raw(self._agent_id, data, topic=topic)
        if self._connections:
            frame = P2PHost._encode_frame(topic, data)
            dead: List[str] = []
            for peer_id, conn in list(self._connections.items()):
                try:
                    conn.writer.write(frame)
                    await conn.writer.drain()
                except Exception as exc:
                    log.warning("tcp.send_failed", peer=peer_id, error=str(exc))
                    dead.append(peer_id)
            for pid in dead:
                self._connections.pop(pid, None)

    async def _deliver_raw(self, from_peer: str, data: bytes, topic: str = "") -> None:
        """Deliver raw bytes to all subscribed queues (TCP fallback path)."""
        for t, q in self._subscriptions.items():
            if not topic or t == topic:
                try:
                    q.put_nowait((from_peer, data))
                except asyncio.QueueFull:
                    log.warning("pubsub.queue_full", topic=t)

    async def _bridge_libp2p_to_queue(self, libp2p_sub, q: asyncio.Queue, topic: str) -> None:
        try:
            while self._running:
                msg = await libp2p_sub.get()
                await q.put((str(getattr(msg, "from_id", "")), msg.data))
        except (asyncio.CancelledError, Exception):
            pass

    def add_handler(self, topic: str, handler: Callable) -> None:
        self._handlers.setdefault(topic, []).append(handler)
