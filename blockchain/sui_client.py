"""
Sui blockchain client for state hash anchoring.
Uses direct JSON-RPC (no external SDK dependency for reads).
For tx submission, optionally uses pysui if available.
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

import httpx

from monitoring.logger import get_logger

log = get_logger(__name__)


class SuiJsonRpcClient:
    """Minimal Sui JSON-RPC 2.0 client (no SDK required)."""

    def __init__(self, rpc_url: str, timeout: float = 15.0) -> None:
        self._url    = rpc_url
        self._client = httpx.AsyncClient(timeout=timeout)
        self._id     = 0

    async def call(self, method: str, params: List[Any]) -> Any:
        self._id += 1
        payload = {
            "jsonrpc": "2.0",
            "id":      self._id,
            "method":  method,
            "params":  params,
        }
        resp = await self._client.post(
            self._url,
            content=json.dumps(payload),
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        result = resp.json()
        if "error" in result:
            raise RuntimeError(f"Sui RPC error [{method}]: {result['error']}")
        return result.get("result")

    async def get_latest_checkpoint(self) -> int:
        seq = await self.call("sui_getLatestCheckpointSequenceNumber", [])
        return int(seq)

    async def get_object(self, object_id: str) -> Optional[Dict[str, Any]]:
        return await self.call(
            "sui_getObject",
            [object_id, {"showContent": True, "showType": True}],
        )

    async def dry_run_transaction(self, tx_bytes: str) -> Dict[str, Any]:
        return await self.call("sui_dryRunTransactionBlock", [tx_bytes])

    async def close(self) -> None:
        await self._client.aclose()


class SuiTransactionClient:
    """
    Handles Sui transaction construction and submission for state anchoring.
    Requires pysui for key management and tx building.
    """

    def __init__(
        self,
        rpc_url: str,
        package_id: Optional[str],
        keypair_path: Optional[str],
    ) -> None:
        self._rpc_url     = rpc_url
        self._package_id  = package_id
        self._keypair_path = keypair_path
        self._client: Optional[SuiJsonRpcClient] = None
        self._pysui_client = None
        self._signer = None

    async def start(self) -> bool:
        """Initialize Sui client. Returns True if fully operational."""
        self._client = SuiJsonRpcClient(self._rpc_url)

        if not self._package_id:
            log.warning("sui_tx.no_package_id", msg="Anchoring disabled (no package_id)")
            return False

        try:
            from pysui import SuiConfig, SyncClient
            from pysui.sui.sui_clients.sync_client import SuiClient as _SyncClient

            # Try to load keypair from file
            if self._keypair_path:
                import os
                if os.path.exists(self._keypair_path):
                    cfg = SuiConfig.user_config(rpc_url=self._rpc_url)
                    self._pysui_client = _SyncClient(cfg)
                    log.info("sui_tx.pysui_loaded", rpc=self._rpc_url)
                    return True

            log.warning("sui_tx.pysui_no_keypair", msg="pysui available but no keypair configured")
            return False
        except ImportError:
            log.warning("sui_tx.pysui_not_installed", msg="Install pysui for tx submission")
            return False
        except Exception as exc:
            log.warning("sui_tx.init_failed", error=str(exc))
            return False

    async def submit_state_hash(
        self,
        state_hash: str,
        epoch: int,
        chain_summary: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """
        Call the on-chain state_anchor::record_hash Move function.
        Returns transaction digest on success, None on failure.
        """
        if not self._pysui_client or not self._package_id:
            return None

        try:
            from pysui.sui.sui_txn import SyncTransaction
            from pysui.sui.sui_types.scalars import SuiString, SuiU64

            txn = SyncTransaction(client=self._pysui_client)
            txn.move_call(
                target=f"{self._package_id}::state_anchor::record_hash",
                arguments=[
                    SuiString(state_hash),
                    SuiU64(epoch),
                    SuiString(json.dumps(chain_summary or {})),
                ],
                type_arguments=[],
            )
            result = txn.execute(gas_budget=10_000_000)

            if result.is_ok():
                digest = result.result_data.digest
                log.info(
                    "sui_tx.anchored",
                    digest=digest,
                    hash=state_hash[:16] + "…",
                    epoch=epoch,
                )
                return digest
            else:
                log.warning("sui_tx.failed", error=str(result.result_string))
                return None

        except Exception as exc:
            log.error("sui_tx.submit_error", error=str(exc))
            return None

    async def query_anchor_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Query historical anchor events from the chain."""
        if not self._client or not self._package_id:
            return []
        try:
            events = await self._client.call(
                "suix_queryEvents",
                [
                    {"MoveModule": {"package": self._package_id, "module": "state_anchor"}},
                    None,
                    limit,
                    False,
                ],
            )
            return events.get("data", []) if events else []
        except Exception as exc:
            log.warning("sui_tx.query_failed", error=str(exc))
            return []

    async def stop(self) -> None:
        if self._client:
            await self._client.close()
