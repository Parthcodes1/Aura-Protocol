"""
Reliable pub/sub layer on top of PubSubManager.
Adds:
- At-least-once delivery with retry queue
- Per-topic message handlers
- Signed message routing
- Backpressure control
"""
from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

import msgpack

from crypto.signing import MessageSigner
from monitoring.logger import get_logger
from monitoring.metrics import MESSAGES_RECEIVED, MESSAGES_REJECTED, MESSAGES_SENT
from network.protocols import Topic

log = get_logger(__name__)

# Type alias for message handlers
Handler = Callable[[str, dict], Awaitable[None]]  # (from_agent_id, payload)


class RetryMessage:
    __slots__ = ("topic", "data", "attempts", "next_try", "max_attempts")

    def __init__(self, topic: str, data: bytes, max_attempts: int = 3) -> None:
        self.topic = topic
        self.data = data
        self.attempts = 0
        self.max_attempts = max_attempts
        self.next_try = time.monotonic()


class ReliablePubSub:
    """
    Wraps PubSubManager with:
    - Handler registration per topic
    - Automatic signed message verification on receive
    - Retry queue for failed publishes
    - Dead-letter logging for undeliverable messages
    """

    def __init__(
        self,
        pubsub_manager,                  # network.host.PubSubManager
        signer: MessageSigner,
        nonce_manager,                   # crypto.nonce.NonceManager
        agent_id: str,
    ) -> None:
        self._ps = pubsub_manager
        self._signer = signer
        self._nonce = nonce_manager
        self._agent_id = agent_id

        self._handlers: Dict[str, List[Handler]] = defaultdict(list)
        self._retry_queue: asyncio.Queue[RetryMessage] = asyncio.Queue()
        self._tasks: List[asyncio.Task] = []
        self._running = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self, topics: Optional[List[str]] = None) -> None:
        self._running = True
        topics = topics or Topic.ALL_TOPICS

        for topic in topics:
            q = await self._ps.subscribe(topic)
            t = asyncio.create_task(self._consume(topic, q))
            self._tasks.append(t)

        retry_task = asyncio.create_task(self._retry_loop())
        self._tasks.append(retry_task)

        log.info("reliable_pubsub.started", topics=len(topics))

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    # ── Publish ───────────────────────────────────────────────────────────────

    async def publish(self, topic: str, payload: dict, max_retries: int = 3) -> None:
        """Sign and publish payload to topic."""
        try:
            data = self._signer.sign(topic, payload)
            await self._ps.publish(topic, data)
            MESSAGES_SENT.labels(topic=topic).inc()
            log.debug("pubsub.published", topic=topic)
        except Exception as exc:
            log.warning("pubsub.publish_failed", topic=topic, error=str(exc))
            # Queue for retry
            msg = RetryMessage(topic, self._signer.sign(topic, payload), max_retries)
            msg.attempts = 1
            msg.next_try = time.monotonic() + 1.0
            await self._retry_queue.put(msg)

    # ── Subscribe ─────────────────────────────────────────────────────────────

    def on(self, topic: str, handler: Handler) -> None:
        """Register an async handler for a topic."""
        self._handlers[topic].append(handler)

    # ── Consumption loop ──────────────────────────────────────────────────────

    async def _consume(self, topic: str, queue: asyncio.Queue) -> None:
        while self._running:
            try:
                raw = await asyncio.wait_for(queue.get(), timeout=1.0)
                # raw can be (from_peer, bytes) tuple or just bytes
                if isinstance(raw, tuple):
                    from_peer, data = raw
                else:
                    from_peer, data = "", raw

                await self._dispatch(topic, from_peer, data)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error("pubsub.consume_error", topic=topic, error=str(exc))

    async def _dispatch(self, topic: str, from_peer: str, raw_data: bytes) -> None:
        MESSAGES_RECEIVED.labels(topic=topic).inc()

        # Verify signature
        decoded = self._signer.verify_and_decode(raw_data)
        if decoded is None:
            MESSAGES_REJECTED.labels(reason="invalid_signature").inc()
            log.warning("pubsub.rejected", topic=topic, reason="bad_sig")
            return

        # Nonce / replay check
        is_replay = await self._nonce.is_replay(
            decoded["agent_id"], decoded["nonce"], decoded["timestamp"]
        )
        if is_replay:
            MESSAGES_REJECTED.labels(reason="replay").inc()
            return

        # Skip own messages
        if decoded["agent_id"] == self._agent_id:
            return

        payload = decoded["payload"]

        # Dispatch to handlers
        handlers = self._handlers.get(topic, [])
        for handler in handlers:
            try:
                await handler(decoded["agent_id"], payload)
            except Exception as exc:
                log.error(
                    "pubsub.handler_error",
                    topic=topic,
                    handler=getattr(handler, "__name__", "?"),
                    error=str(exc),
                )

    # ── Retry loop ────────────────────────────────────────────────────────────

    async def _retry_loop(self) -> None:
        while self._running:
            try:
                msg = await asyncio.wait_for(self._retry_queue.get(), timeout=0.5)
                now = time.monotonic()
                if now < msg.next_try:
                    # Not yet — put back and wait
                    await self._retry_queue.put(msg)
                    await asyncio.sleep(0.1)
                    continue

                try:
                    await self._ps.publish(msg.topic, msg.data)
                    MESSAGES_SENT.labels(topic=msg.topic).inc()
                    log.debug("pubsub.retry_success", topic=msg.topic, attempt=msg.attempts)
                except Exception as exc:
                    msg.attempts += 1
                    if msg.attempts >= msg.max_attempts:
                        log.error(
                            "pubsub.dead_letter",
                            topic=msg.topic,
                            attempts=msg.attempts,
                            error=str(exc),
                        )
                    else:
                        msg.next_try = time.monotonic() + (2 ** msg.attempts)
                        await self._retry_queue.put(msg)

            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
