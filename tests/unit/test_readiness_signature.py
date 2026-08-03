from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from autoquant.data.models import _canonical_hash
from autoquant.readiness_artifact import write_operations_readiness_artifact
from autoquant.readiness_signature import (
    generate_readiness_signing_key_pair,
    sign_operations_readiness_artifact,
    verify_operations_readiness_signature,
)

NOW = datetime(2026, 8, 3, 8, tzinfo=UTC)


def _artifact() -> dict[str, object]:
    sections = {
        "paper_runtime": {"status": "unavailable"},
        "paper_watchdog": {"status": "blocked"},
        "promotion": {"status": "blocked"},
        "qmt": {"status": "blocked"},
    }
    version = "operations-readiness-report-v1"
    return {
        "blockers": ["paper_runtime.unavailable", "qmt.windows_runtime"],
        "broker_mutation_allowed": False,
        "collection_started": False,
        "generated_at": NOW.isoformat(),
        "live_trading_locked": True,
        "report_hash": _canonical_hash({"sections": sections, "version": version}),
        "sections": sections,
        "status": "blocked",
        "storage_mutation_allowed": False,
        "vendor_request_started": False,
        "version": version,
    }


def _signed_paths(tmp_path: Path) -> tuple[Path, Path, Path, Path, str]:
    artifact_path = tmp_path / "readiness.json"
    private_key_path = tmp_path / "readiness-private.pem"
    public_key_path = tmp_path / "readiness-public.pem"
    signature_path = tmp_path / "readiness.sig.json"
    write_operations_readiness_artifact(
        artifact_path,
        _artifact(),
        replace=False,
    )
    key_id = generate_readiness_signing_key_pair(
        private_key_path,
        public_key_path,
    )
    return (
        artifact_path,
        private_key_path,
        public_key_path,
        signature_path,
        key_id,
    )


def test_ed25519_readiness_signature_binds_origin_exact_bytes_and_freshness(
    tmp_path: Path,
) -> None:
    artifact_path, private_key, public_key, signature_path, key_id = _signed_paths(
        tmp_path
    )

    artifact, signature = sign_operations_readiness_artifact(
        artifact_path,
        private_key,
        signature_path,
        now=NOW + timedelta(minutes=1),
        replace=False,
    )
    verified_artifact, verified_signature = verify_operations_readiness_signature(
        artifact_path,
        signature_path,
        public_key,
        now=NOW + timedelta(minutes=2),
        maximum_age=timedelta(hours=24),
    )

    assert verified_artifact == artifact == _artifact()
    assert verified_signature == signature
    assert signature["algorithm"] == "ed25519"
    assert signature["key_id"] == key_id
    assert private_key.stat().st_mode & 0o777 == 0o600
    assert public_key.stat().st_mode & 0o777 == 0o644
    assert signature_path.stat().st_mode & 0o777 == 0o644


def test_readiness_signature_rejects_exact_byte_tampering_and_wrong_key(
    tmp_path: Path,
) -> None:
    artifact_path, private_key, public_key, signature_path, _ = _signed_paths(tmp_path)
    sign_operations_readiness_artifact(
        artifact_path,
        private_key,
        signature_path,
        now=NOW,
        replace=False,
    )
    artifact_path.write_bytes(artifact_path.read_bytes() + b"\n")

    with pytest.raises(ValueError, match="artifact binding"):
        verify_operations_readiness_signature(
            artifact_path,
            signature_path,
            public_key,
            now=NOW,
            maximum_age=timedelta(hours=24),
        )

    artifact_path.write_text(
        json.dumps(_artifact(), ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    other_private = tmp_path / "other-private.pem"
    other_public = tmp_path / "other-public.pem"
    generate_readiness_signing_key_pair(other_private, other_public)
    with pytest.raises(ValueError, match="key identity"):
        verify_operations_readiness_signature(
            artifact_path,
            signature_path,
            other_public,
            now=NOW,
            maximum_age=timedelta(hours=24),
        )


def test_readiness_signature_rejects_stale_evidence_and_broad_private_key_permissions(
    tmp_path: Path,
) -> None:
    artifact_path, private_key, public_key, signature_path, _ = _signed_paths(tmp_path)
    if os.name != "nt":
        private_key.chmod(0o644)
        with pytest.raises(ValueError, match="permissions"):
            sign_operations_readiness_artifact(
                artifact_path,
                private_key,
                signature_path,
                now=NOW,
                replace=False,
            )
        private_key.chmod(0o600)
    sign_operations_readiness_artifact(
        artifact_path,
        private_key,
        signature_path,
        now=NOW,
        replace=False,
    )
    with pytest.raises(ValueError, match="stale"):
        verify_operations_readiness_signature(
            artifact_path,
            signature_path,
            public_key,
            now=NOW + timedelta(hours=25),
            maximum_age=timedelta(hours=24),
        )


def test_readiness_signing_keys_never_overwrite_existing_trust_anchors(
    tmp_path: Path,
) -> None:
    _, private_key, public_key, _, key_id = _signed_paths(tmp_path)

    with pytest.raises(ValueError, match="already exists"):
        generate_readiness_signing_key_pair(private_key, public_key)

    assert len(key_id) == 64
