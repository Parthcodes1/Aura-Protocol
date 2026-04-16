"""
Peer discovery: bootstrap peers, Kademlia DHT, mDNS.
"""
from __future__ import annotations

import asyncio
import json
import socket
import struct
import time
from typing import Callable, Dict, List, Optional, Set

from monitoring.logger import get_logger

log = get_logger(__name__)

try:
    from kademlia.network import Server as KademliaServer
    HAS_KADEMLIA = True
except ImportError:
    HAS_KADEMLIA = False
    log.warning("kademlia_not_installed", msg="DHT discovery disabled")

MDNS_GROUP = "224.0.0.251"
MDNS_PORT  = 5353


class PeerDiscovery:
    """
    Multi-mechanism peer discovery:
    1. Bootstrap peer list (from config)
    2. Kademlia DHT (if kademlia package installed)
    3. mDNS (local network, LAN agents)
    """

    def __init__(
        self,
        agent_id: str,
        p2p_port: int,
        bootstrap_peers: List[str],
        on_peer_found: Callable[[str], None],
        enable_dht: bool = True,
        enable_mdns: bool = True,
        dht_port: int = 0,
    ) -> None:
        self._agent_id = agent_id
        self._p2p_port = p2p_port
        self._bootstrap = bootstrap_peers
        self._on_peer_found = on_peer_found
        self._enable_dht = enable_dht and HAS_KADEMLIA
        self._enable_mdns = enable_mdns
        self._dht_port = dht_port or (p2p_port + 1000)

        self._dht: Optional[KademliaServer] = None
        self._known_peers: Set[str] = set()
        self._running = False
        self._tasks: List[asyncio.Task] = []

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True

        # 1. Connect bootstrap peers immediately
        for addr in self._bootstrap:
            asyncio.create_task(self._notify_bootstrap(addr))

        # 2. DHT
        if self._enable_dht:
            t = asyncio.create_task(self._start_dht())
            self._tasks.append(t)

        # 3. mDNS
        if self._enable_mdns:
            t = asyncio.create_task(self._mdns_announce_loop())
            self._tasks.append(t)
            t2 = asyncio.create_task(self._mdns_listen_loop())
            self._tasks.append(t2)

        log.info(
            "discovery.started",
            bootstrap=len(self._bootstrap),
            dht=self._enable_dht,
            mdns=self._enable_mdns,
        )

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._dht:
            self._dht.stop()

    # ── Bootstrap ─────────────────────────────────────────────────────────────

    async def _notify_bootstrap(self, multiaddr: str) -> None:
        if multiaddr and multiaddr not in self._known_peers:
            self._known_peers.add(multiaddr)
            await self._on_peer_found(multiaddr)
            log.debug("discovery.bootstrap_peer", addr=multiaddr)

    # ── Kademlia DHT ──────────────────────────────────────────────────────────

    async def _start_dht(self) -> None:
        try:
            self._dht = KademliaServer()
            await self._dht.listen(self._dht_port)
            log.info("dht.listening", port=self._dht_port)

            # Bootstrap DHT from known addresses
            bootstrap_nodes = []
            for addr in self._bootstrap:
                parsed = self._parse_ip_port(addr)
                if parsed:
                    bootstrap_nodes.append(parsed)

            if bootstrap_nodes:
                await self._dht.bootstrap(bootstrap_nodes)
                log.info("dht.bootstrapped", nodes=len(bootstrap_nodes))

            # Announce self
            my_key = f"oracle-agent:{self._agent_id}"
            my_value = json.dumps({
                "agent_id": self._agent_id,
                "p2p_port": self._p2p_port,
                "timestamp": int(time.time()),
            })
            await self._dht.set(my_key, my_value)

            # Periodic discovery loop
            while self._running:
                await asyncio.sleep(30)
                await self._dht_refresh()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.error("dht.error", error=str(exc))

    async def _dht_refresh(self) -> None:
        """Look up other oracle agents in the DHT."""
        if not self._dht:
            return
        try:
            # Re-announce ourselves
            my_key = f"oracle-agent:{self._agent_id}"
            my_value = json.dumps({
                "agent_id": self._agent_id,
                "p2p_port": self._p2p_port,
                "timestamp": int(time.time()),
            })
            await self._dht.set(my_key, my_value)
        except Exception as exc:
            log.warning("dht.refresh_failed", error=str(exc))

    def _parse_ip_port(self, multiaddr: str) -> Optional[tuple]:
        """Extract (host, port) from /ip4/HOST/tcp/PORT[/...] multiaddr."""
        try:
            parts = multiaddr.strip("/").split("/")
            host = parts[parts.index("ip4") + 1]
            port = int(parts[parts.index("tcp") + 1])
            return (host, port + 1000)  # DHT port = P2P port + 1000
        except (ValueError, IndexError):
            return None

    # ── mDNS ──────────────────────────────────────────────────────────────────

    async def _mdns_announce_loop(self) -> None:
        """Periodically broadcast our presence via mDNS."""
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

            announcement = json.dumps({
                "type":     "oracle_announce",
                "agent_id": self._agent_id,
                "port":     self._p2p_port,
            }).encode()

            while self._running:
                try:
                    sock.sendto(announcement, (MDNS_GROUP, MDNS_PORT))
                except OSError:
                    pass
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.warning("mdns.announce_error", error=str(exc))
        finally:
            if sock:
                sock.close()

    async def _mdns_listen_loop(self) -> None:
        """Listen for mDNS announcements from other agents."""
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, "SO_REUSEPORT"):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            sock.bind(("", MDNS_PORT))

            mreq = struct.pack("4sL", socket.inet_aton(MDNS_GROUP), socket.INADDR_ANY)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            sock.setblocking(False)

            loop = asyncio.get_event_loop()
            while self._running:
                try:
                    data, addr = await loop.run_in_executor(None, sock.recvfrom, 1024)
                    msg = json.loads(data.decode())
                    if (
                        msg.get("type") == "oracle_announce"
                        and msg.get("agent_id") != self._agent_id
                    ):
                        peer_addr = f"/ip4/{addr[0]}/tcp/{msg['port']}"
                        if peer_addr not in self._known_peers:
                            self._known_peers.add(peer_addr)
                            await self._on_peer_found(peer_addr)
                            log.info("mdns.peer_found", agent=msg["agent_id"], addr=peer_addr)
                except asyncio.CancelledError:
                    raise
                except json.JSONDecodeError:
                    pass
                except Exception:
                    await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.warning("mdns.listen_error", error=str(exc))
        finally:
            if sock:
                sock.close()

    # ── State ─────────────────────────────────────────────────────────────────

    @property
    def known_peers(self) -> Set[str]:
        return set(self._known_peers)
