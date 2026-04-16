"""
Blockchain anchoring service.
Periodically submits the aggregate cross-chain state hash to Sui.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, Optional

from blockchain.sui_client import SuiTransactionClient
from monitoring.logger import get_logger
from monitoring.metrics import ANCHOR_SUBMISSIONS

log = get_logger(__name__)


class BlockchainAnchor:
    """
    Anchors the aggregate swarm state hash to Sui at configurable intervals.
    Safe to call even when Sui isn't reachable — failures are non-fatal.
    """

    def __init__(
        self,
        config,          # AnchorConfig
        store,           # StateStore
    ) -> None:
        self._cfg   = config
        self._store = store
        self._tx_client: Optional[SuiTransactionClient] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_anchor_epoch = -1
        self._operational = False

    async def start(self) -> None:
        if not self._cfg.enabled:
            log.info("anchor.disabled")
            return

        self._tx_client = SuiTransactionClient(
            rpc_url=self._cfg.sui_rpc_url,
            package_id=self._cfg.package_id,
            keypair_path=self._cfg.keypair_path,
        )
        self._operational = await self._tx_client.start()

        if self._operational:
            self._running = True
            self._task = asyncio.create_task(self._anchor_loop())
            log.info("anchor.started", rpc=self._cfg.sui_rpc_url)
        else:
            log.warning("anchor.not_operational", msg="State hashing will still occur but not anchored")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._tx_client:
            await self._tx_client.stop()

    async def _anchor_loop(self) -> None:
        """Submit pending unanchored state hashes."""
        while self._running:
            try:
                await asyncio.sleep(30)
                await self._process_pending()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error("anchor.loop_error", error=str(exc))

    async def _process_pending(self) -> None:
        pending = await self._store.get_unanchored_hashes()
        if not pending:
            return

        # Submit latest N pending hashes
        for entry in pending[:5]:
            tx = await self.anchor(entry["state_hash"], epoch=int(entry["id"]))
            if tx:
                await self._store.mark_anchored(entry["id"], tx)
                ANCHOR_SUBMISSIONS.labels(status="success").inc()
            else:
                ANCHOR_SUBMISSIONS.labels(status="failed").inc()

    async def anchor(
        self,
        state_hash: str,
        epoch: int = 0,
        chain_summary: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Submit a state hash. Returns tx digest or None."""
        if not self._operational or not self._tx_client:
            return None
        try:
            return await self._tx_client.submit_state_hash(
                state_hash=state_hash,
                epoch=epoch,
                chain_summary=chain_summary,
            )
        except Exception as exc:
            log.warning("anchor.submit_failed", error=str(exc))
            return None

    async def get_history(self, limit: int = 10):
        if not self._tx_client:
            return []
        return await self._tx_client.query_anchor_history(limit)
