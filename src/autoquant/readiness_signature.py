from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from autoquant.clock import to_utc
from autoquant.data.models import _require_lowercase_sha256
from autoquant.readiness_artifact import read_operations_readiness_artifact

_SIGNATURE_VERSION = "operations-readiness-signature-v1"
_SIGNATURE_KEYS = frozenset(
    {
        "algorithm",
        "artifact_report_hash",
        "artifact_sha256",
        "key_id",
        "signature",
        "signed_at",
        "version",
    }
)
_MAXIMUM_KEY_BYTES = 32 * 1024
_MAXIMUM_SIGNATURE_BYTES = 64 * 1024
_FUTURE_TOLERANCE = timedelta(seconds=2)


def _canonical_bytes(payload: dict[str, object]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _safe_target(path: Path, *, suffix: str, replace: bool) -> None:
    if path.suffix.casefold() != suffix or not path.name.strip():
        raise ValueError("readiness signature path has an invalid suffix")
    if not path.parent.is_dir() or path.parent.is_symlink():
        raise ValueError("readiness signature directory is invalid")
    if path.exists() and (not replace or path.is_symlink() or not path.is_file()):
        raise ValueError("readiness signature target already exists or is unsafe")


def _write_atomic(path: Path, encoded: bytes, *, mode: int, replace: bool) -> None:
    _safe_target(path, suffix=path.suffix.casefold(), replace=replace)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            os.chmod(temporary.name, mode)
            temporary.write(encoded)
            temporary.flush()
            os.fsync(temporary.fileno())
        if path.exists() and not replace:
            raise ValueError("readiness signature target already exists")
        os.replace(temporary_path, path)
        temporary_path = None
    except (OSError, ValueError):
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


def _read_regular_file(path: Path, *, maximum_bytes: int, name: str) -> bytes:
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"{name} file is invalid")
    try:
        size = path.stat().st_size
        encoded = path.read_bytes()
    except OSError:
        raise ValueError(f"{name} file is unavailable") from None
    if size < 2 or size > maximum_bytes or len(encoded) != size:
        raise ValueError(f"{name} file size is invalid")
    return encoded


def _public_key_id(public_key: Ed25519PublicKey) -> str:
    raw = public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return hashlib.sha256(raw).hexdigest()


def _load_private_key(path: Path) -> Ed25519PrivateKey:
    encoded = _read_regular_file(
        path,
        maximum_bytes=_MAXIMUM_KEY_BYTES,
        name="readiness private key",
    )
    if os.name != "nt" and path.stat().st_mode & 0o077:
        raise ValueError("readiness private key permissions are too broad")
    try:
        key = serialization.load_pem_private_key(encoded, password=None)
    except (TypeError, ValueError):
        raise ValueError("readiness private key is invalid") from None
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError("readiness private key must use Ed25519")
    return key


def _load_public_key(path: Path) -> Ed25519PublicKey:
    encoded = _read_regular_file(
        path,
        maximum_bytes=_MAXIMUM_KEY_BYTES,
        name="readiness public key",
    )
    try:
        key = serialization.load_pem_public_key(encoded)
    except (TypeError, ValueError):
        raise ValueError("readiness public key is invalid") from None
    if not isinstance(key, Ed25519PublicKey):
        raise ValueError("readiness public key must use Ed25519")
    return key


def generate_readiness_signing_key_pair(
    private_key_path: Path,
    public_key_path: Path,
) -> str:
    """Generate a new non-overwriting Ed25519 trust anchor pair."""

    if private_key_path.absolute() == public_key_path.absolute():
        raise ValueError("readiness private and public key paths must differ")
    _safe_target(private_key_path, suffix=".pem", replace=False)
    _safe_target(public_key_path, suffix=".pem", replace=False)
    private_key = Ed25519PrivateKey.generate()
    private_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_key = private_key.public_key()
    public_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    _write_atomic(public_key_path, public_bytes, mode=0o644, replace=False)
    try:
        _write_atomic(private_key_path, private_bytes, mode=0o600, replace=False)
    except (OSError, ValueError):
        public_key_path.unlink(missing_ok=True)
        raise
    return _public_key_id(public_key)


def _signature_binding(signature: dict[str, object]) -> dict[str, object]:
    return {
        key: value
        for key, value in signature.items()
        if key != "signature"
    }


def sign_operations_readiness_artifact(
    artifact_path: Path,
    private_key_path: Path,
    signature_path: Path,
    *,
    now: datetime,
    replace: bool,
) -> tuple[dict[str, object], dict[str, object]]:
    """Create a detached Ed25519 signature over the exact artifact bytes."""

    artifact, encoded = read_operations_readiness_artifact(artifact_path)
    private_key = _load_private_key(private_key_path)
    signed_at = to_utc(now, name="readiness signature time")
    binding: dict[str, object] = {
        "algorithm": "ed25519",
        "artifact_report_hash": artifact["report_hash"],
        "artifact_sha256": hashlib.sha256(encoded).hexdigest(),
        "key_id": _public_key_id(private_key.public_key()),
        "signed_at": signed_at.isoformat(timespec="microseconds"),
        "version": _SIGNATURE_VERSION,
    }
    signature = {
        **binding,
        "signature": private_key.sign(_canonical_bytes(binding)).hex(),
    }
    _safe_target(signature_path, suffix=".json", replace=replace)
    signature_bytes = _canonical_bytes(signature) + b"\n"
    _write_atomic(signature_path, signature_bytes, mode=0o644, replace=replace)
    return artifact, signature


def _load_signature(path: Path) -> dict[str, object]:
    encoded = _read_regular_file(
        path,
        maximum_bytes=_MAXIMUM_SIGNATURE_BYTES,
        name="readiness signature",
    )
    try:
        payload = json.loads(encoded.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError):
        raise ValueError("readiness signature JSON is invalid") from None
    if not isinstance(payload, dict) or set(payload) != _SIGNATURE_KEYS:
        raise ValueError("readiness signature fields are invalid")
    signature = cast(dict[str, object], payload)
    if signature["version"] != _SIGNATURE_VERSION or signature["algorithm"] != "ed25519":
        raise ValueError("readiness signature algorithm is invalid")
    for key in ("artifact_report_hash", "artifact_sha256", "key_id"):
        value = signature[key]
        if not isinstance(value, str):
            raise ValueError("readiness signature hash is invalid")
        _require_lowercase_sha256(value, name=f"readiness signature {key}")
    value = signature["signature"]
    if (
        not isinstance(value, str)
        or len(value) != 128
        or value.casefold() != value
    ):
        raise ValueError("readiness signature value is invalid")
    try:
        raw_signature = bytes.fromhex(value)
    except ValueError:
        raise ValueError("readiness signature value is invalid") from None
    if len(raw_signature) != 64:
        raise ValueError("readiness signature value is invalid")
    signed_at = signature["signed_at"]
    if not isinstance(signed_at, str):
        raise ValueError("readiness signature timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(signed_at)
    except ValueError:
        raise ValueError("readiness signature timestamp is invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("readiness signature timestamp is invalid")
    return signature


def verify_operations_readiness_signature(
    artifact_path: Path,
    signature_path: Path,
    public_key_path: Path,
    *,
    now: datetime,
    maximum_age: timedelta,
) -> tuple[dict[str, object], dict[str, object]]:
    """Verify origin, exact bytes, pinned signer, and bounded freshness offline."""

    if maximum_age <= timedelta(0) or maximum_age > timedelta(days=7):
        raise ValueError("readiness signature maximum age is invalid")
    artifact, encoded = read_operations_readiness_artifact(artifact_path)
    signature = _load_signature(signature_path)
    public_key = _load_public_key(public_key_path)
    if signature["key_id"] != _public_key_id(public_key):
        raise ValueError("readiness signature key identity is invalid")
    if signature["artifact_report_hash"] != artifact["report_hash"]:
        raise ValueError("readiness signature report binding is invalid")
    if signature["artifact_sha256"] != hashlib.sha256(encoded).hexdigest():
        raise ValueError("readiness signature artifact binding is invalid")

    instant = to_utc(now, name="readiness signature verification time")
    signed_at = to_utc(
        datetime.fromisoformat(str(signature["signed_at"])),
        name="readiness signature timestamp",
    )
    generated_at = to_utc(
        datetime.fromisoformat(str(artifact["generated_at"])),
        name="readiness artifact timestamp",
    )
    if signed_at < generated_at - _FUTURE_TOLERANCE:
        raise ValueError("readiness signature predates its artifact")
    for observed in (generated_at, signed_at):
        if observed > instant + _FUTURE_TOLERANCE:
            raise ValueError("readiness signed evidence is from the future")
        if instant - observed > maximum_age:
            raise ValueError("readiness signed evidence is stale")

    signature_hex = cast(str, signature["signature"])
    try:
        public_key.verify(
            bytes.fromhex(signature_hex),
            _canonical_bytes(_signature_binding(signature)),
        )
    except InvalidSignature:
        raise ValueError("readiness signature verification failed") from None
    return artifact, signature
