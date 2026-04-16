"""
Ed25519 keypair management.
Keys are persisted to disk in PEM format and loaded on startup.
Provides conversion between the `cryptography` library and libp2p key types.
"""
from __future__ import annotations

import base64
import os
from pathlib import Path
from typing import Optional, Tuple

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

from monitoring.logger import get_logger

log = get_logger(__name__)


class KeyManager:
    """
    Manages an Ed25519 keypair for a node.
    Generates on first run, persists to `<data_dir>/keys/<agent_id>.pem`.
    """

    def __init__(self, agent_id: str, data_dir: str = "./data") -> None:
        self.agent_id = agent_id
        self._key_dir = Path(data_dir) / "keys"
        self._key_path = self._key_dir / f"{agent_id}.pem"
        self._pub_path = self._key_dir / f"{agent_id}.pub"

        self._private_key: Optional[Ed25519PrivateKey] = None
        self._public_key: Optional[Ed25519PublicKey] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def load_or_generate(self) -> None:
        """Load existing keypair from disk or generate a new one."""
        self._key_dir.mkdir(parents=True, exist_ok=True)

        if self._key_path.exists() and self._pub_path.exists():
            self._load()
            log.info("keypair.loaded", agent=self.agent_id, path=str(self._key_path))
        else:
            self._generate()
            self._save()
            log.info("keypair.generated", agent=self.agent_id, path=str(self._key_path))

    def _generate(self) -> None:
        self._private_key = Ed25519PrivateKey.generate()
        self._public_key = self._private_key.public_key()

    def _save(self) -> None:
        # Private key (raw PKCS8, no passphrase)
        priv_bytes = self._private_key.private_bytes(
            encoding=Encoding.PEM,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption(),
        )
        self._key_path.write_bytes(priv_bytes)
        self._key_path.chmod(0o600)

        # Public key
        pub_bytes = self._public_key.public_bytes(
            encoding=Encoding.PEM,
            format=PublicFormat.SubjectPublicKeyInfo,
        )
        self._pub_path.write_bytes(pub_bytes)

    def _load(self) -> None:
        from cryptography.hazmat.primitives.serialization import load_pem_private_key

        priv_bytes = self._key_path.read_bytes()
        self._private_key = load_pem_private_key(priv_bytes, password=None)
        self._public_key = self._private_key.public_key()

    # ── Accessors ─────────────────────────────────────────────────────────────

    @property
    def private_key(self) -> Ed25519PrivateKey:
        if self._private_key is None:
            raise RuntimeError("KeyManager.load_or_generate() not called")
        return self._private_key

    @property
    def public_key(self) -> Ed25519PublicKey:
        if self._public_key is None:
            raise RuntimeError("KeyManager.load_or_generate() not called")
        return self._public_key

    @property
    def public_key_bytes(self) -> bytes:
        """Raw 32-byte Ed25519 public key."""
        return self._public_key.public_bytes(
            encoding=Encoding.Raw,
            format=PublicFormat.Raw,
        )

    @property
    def public_key_b64(self) -> str:
        """Base64-encoded public key (URL-safe, no padding)."""
        return base64.urlsafe_b64encode(self.public_key_bytes).rstrip(b"=").decode()

    def get_libp2p_keypair(self):
        """
        Return a py-libp2p KeyPair built from our raw private key bytes.
        Requires `libp2p` package.
        """
        try:
            from libp2p.crypto.ed25519 import Ed25519PrivateKey as LibP2PPrivKey

            raw_priv = self._private_key.private_bytes(
                encoding=Encoding.Raw,
                format=PrivateFormat.Raw,
                encryption_algorithm=NoEncryption(),
            )
            return LibP2PPrivKey.from_bytes(raw_priv).get_public_key().__class__, LibP2PPrivKey.from_bytes(raw_priv)
        except Exception as exc:
            log.warning("keypair.libp2p_unavailable", error=str(exc))
            return None

    def get_libp2p_key_pair(self):
        """Return libp2p KeyPair object."""
        try:
            from libp2p.crypto.ed25519 import Ed25519PrivateKey as LibP2PPrivKey

            raw_priv = self._private_key.private_bytes(
                encoding=Encoding.Raw,
                format=PrivateFormat.Raw,
                encryption_algorithm=NoEncryption(),
            )
            priv = LibP2PPrivKey.from_bytes(raw_priv)
            from libp2p.crypto.keys import KeyPair
            return KeyPair(priv, priv.get_public_key())
        except ImportError:
            log.warning("libp2p_not_installed")
            return None
        except Exception as exc:
            log.warning("keypair.libp2p_error", error=str(exc))
            return None
