from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoquant.data.models import _canonical_hash
from autoquant.readiness_artifact import (
    load_operations_readiness_artifact,
    validate_operations_readiness_artifact,
    write_operations_readiness_artifact,
)


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
        "generated_at": "2026-08-03T08:00:00+00:00",
        "live_trading_locked": True,
        "report_hash": _canonical_hash({"sections": sections, "version": version}),
        "sections": sections,
        "status": "blocked",
        "storage_mutation_allowed": False,
        "vendor_request_started": False,
        "version": version,
    }


def test_readiness_artifact_validates_canonical_hash_and_safety_boundary() -> None:
    artifact = _artifact()

    assert validate_operations_readiness_artifact(artifact) == artifact

    tampered = dict(artifact)
    tampered["sections"] = {"qmt": {"status": "ok"}}
    with pytest.raises(ValueError, match="hash"):
        validate_operations_readiness_artifact(tampered)

    unsafe = dict(artifact)
    unsafe["broker_mutation_allowed"] = True
    with pytest.raises(ValueError, match="safety boundary"):
        validate_operations_readiness_artifact(unsafe)


def test_readiness_artifact_write_is_bounded_private_and_round_trips(
    tmp_path: Path,
) -> None:
    output = tmp_path / "readiness.json"
    artifact = _artifact()

    written = write_operations_readiness_artifact(
        output,
        artifact,
        replace=False,
    )

    assert written == artifact
    assert output.stat().st_mode & 0o777 == 0o600
    assert load_operations_readiness_artifact(output) == artifact
    assert output.read_text(encoding="utf-8").endswith("\n")
    assert " " not in output.read_text(encoding="utf-8")

    with pytest.raises(ValueError, match="already exists"):
        write_operations_readiness_artifact(output, artifact, replace=False)
    assert write_operations_readiness_artifact(output, artifact, replace=True) == artifact


def test_readiness_artifact_loader_rejects_symlinks_and_invalid_json(
    tmp_path: Path,
) -> None:
    invalid = tmp_path / "invalid.json"
    invalid.write_text("{not-json}", encoding="utf-8")
    with pytest.raises(ValueError, match="JSON"):
        load_operations_readiness_artifact(invalid)

    target = tmp_path / "target.json"
    target.write_text(json.dumps(_artifact()), encoding="utf-8")
    link = tmp_path / "link.json"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="file is invalid"):
        load_operations_readiness_artifact(link)
