"""
Message signing and verification using Ed25519.
Every outbound message is signed; every inbound message is verified.

Wire format (msgpack-encoded):
{
  "v":   1,                      # schema version
  "pid": "<agent_id>",           # sender peer ID
  "pk":  "<base64 public key>",  # sender's Ed25519 public key
  "ts":  1234567890123,          # unix ms timestamp
  "nn":  "<hex nonce>",          # 16-byte random nonce
  "tp":  "<topic>",              # message topic
  "pl":  <bytes>,                # msgpack-encoded payload
  "sig": <bytes>,                # 64-byte Ed25519 signature over canonical body
}
"""
from __future__ import annotations

import base64
import hashlib
import os
import time
from typing import Any, Dict, Optional

import msgpack
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from monitoring.logger import get_logger

log = get_logger(__name__)

SCHEMA_VERSION = 1
MAX_CLOCK_SKEW_MS = 30000  # 30 seconds (allows for startup queue drain)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _now_ms() -> int:
    return int(time.time() * 1000)


def _canonical_body(envelope: Dict[str, Any]) -> bytes:
    """Deterministic bytes to sign (everything except 'sig')."""
    body = {k: v for k, v in envelope.items() if k != "sig"}
    return msgpack.packb(body, use_bin_type=True)


def _pub_key_from_b64(b64: str) -> Ed25519PublicKey:
    raw = base64.urlsafe_b64decode(b64 + "==")
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    from cryptography.hazmat.primitives.serialization import load_der_public_key
    # Raw 32-byte public key
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey as _PK
    return _PK.from_public_bytes(raw) if hasattr(_PK, "from_public_bytes") else _load_raw_pub(raw)


def _load_raw_pub(raw: bytes) -> Ed25519PublicKey:
    """Load Ed25519PublicKey from raw 32 bytes via cryptography library."""
    # cryptography >= 2.6 supports this
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    # Use internal DER construction for libraries without from_public_bytes
    import struct
    # RFC 8032 DER prefix for Ed25519 public key
    DER_PREFIX = bytes.fromhex("302a300506032b6570032100")
    der = DER_PREFIX + raw
    from cryptography.hazmat.primitives.serialization import load_der_public_key
    return load_der_public_key(der, backend=default_backend())


# ─────────────────────────────────────────────────────────────────────────────
# MessageSigner
# ─────────────────────────────────────────────────────────────────────────────

class MessageSigner:
    """Signs outbound messages and verifies inbound ones."""

    def __init__(
        self,
        agent_id: str,
        private_key: Ed25519PrivateKey,
        public_key_b64: str,
        max_clock_skew_ms: int = MAX_CLOCK_SKEW_MS,
    ) -> None:
        self.agent_id = agent_id
        self._private_key = private_key
        self._public_key_b64 = public_key_b64
        self._max_clock_skew_ms = max_clock_skew_ms

    # ── Sign ─────────────────────────────────────────────────────────────────

    def sign(self, topic: str, payload: Any) -> bytes:
        """
        Serialize `payload` + sign → return msgpack-encoded wire envelope.
        """
        pl_bytes = msgpack.packb(payload, use_bin_type=True)
        nonce = os.urandom(16).hex()
        ts = _now_ms()

        envelope: Dict[str, Any] = {
            "v":   SCHEMA_VERSION,
            "pid": self.agent_id,
            "pk":  self._public_key_b64,
            "ts":  ts,
            "nn":  nonce,
            "tp":  topic,
            "pl":  pl_bytes,
        }

        body_bytes = _canonical_body(envelope)
        sig = self._private_key.sign(body_bytes)
        envelope["sig"] = sig

        return msgpack.packb(envelope, use_bin_type=True)

    # ── Verify + decode ───────────────────────────────────────────────────────

    def verify_and_decode(
        self,
        raw: bytes,
        seen_nonces: Optional[set] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Verify wire message. Returns decoded payload dict on success, None on failure.
        Checks: schema version, required fields, timestamp skew, signature, nonce replay.
        """
        try:
            envelope = msgpack.unpackb(raw, raw=False)
        except Exception as exc:
            log.warning("msg.decode_failed", error=str(exc))
            return None

        # ── Schema ───────────────────────────────────────────────────────────
        if envelope.get("v") != SCHEMA_VERSION:
            log.warning("msg.bad_version", v=envelope.get("v"))
            return None

        required = ("pid", "pk", "ts", "nn", "tp", "pl", "sig")
        if any(k not in envelope for k in required):
            log.warning("msg.missing_fields")
            return None

        # ── Timestamp skew ────────────────────────────────────────────────────
        ts_delta = abs(_now_ms() - envelope["ts"])
        if ts_delta > self._max_clock_skew_ms:
            log.warning("msg.clock_skew", delta_ms=ts_delta)
            return None

        # ── Nonce replay ──────────────────────────────────────────────────────
        nonce_key = f"{envelope['pid']}:{envelope['nn']}"
        if seen_nonces is not None:
            if nonce_key in seen_nonces:
                log.warning("msg.replay_attack", pid=envelope["pid"])
                return None
            seen_nonces.add(nonce_key)

        # ── Signature ─────────────────────────────────────────────────────────
        try:
            sig = envelope["sig"]
            if isinstance(sig, str):
                sig = bytes.fromhex(sig)
            pub_key = _pub_key_from_b64(envelope["pk"])
            body_bytes = _canonical_body(envelope)
            pub_key.verify(sig, body_bytes)
        except InvalidSignature:
            log.warning("msg.invalid_signature", pid=envelope["pid"])
            return None
        except Exception as exc:
            log.warning("msg.sig_error", error=str(exc), pid=envelope.get("pid"))
            return None

        # ── Decode payload ────────────────────────────────────────────────────
        try:
            payload = msgpack.unpackb(envelope["pl"], raw=False)
        except Exception as exc:
            log.warning("msg.payload_decode_failed", error=str(exc))
            return None

        return {
            "agent_id":  envelope["pid"],
            "public_key": envelope["pk"],
            "timestamp": envelope["ts"],
            "nonce":     envelope["nn"],
            "topic":     envelope["tp"],
            "payload":   payload,
        }
