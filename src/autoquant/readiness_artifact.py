from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import cast

from autoquant.data.models import _canonical_hash, _require_lowercase_sha256

_MAXIMUM_ARTIFACT_BYTES = 5 * 1024 * 1024
_VERSION = "operations-readiness-report-v1"
_REQUIRED_KEYS = frozenset(
    {
        "blockers",
        "broker_mutation_allowed",
        "collection_started",
        "generated_at",
        "live_trading_locked",
        "report_hash",
        "sections",
        "status",
        "storage_mutation_allowed",
        "vendor_request_started",
        "version",
    }
)


def validate_operations_readiness_artifact(payload: object) -> dict[str, object]:
    """Validate one complete redacted report and its canonical evidence hash."""

    if not isinstance(payload, dict) or set(payload) != _REQUIRED_KEYS:
        raise ValueError("operations readiness artifact fields are invalid")
    report = cast(dict[str, object], payload)
    if report["version"] != _VERSION:
        raise ValueError("operations readiness artifact version is invalid")
    if report["live_trading_locked"] is not True:
        raise ValueError("operations readiness artifact live lock is invalid")
    for key in (
        "broker_mutation_allowed",
        "collection_started",
        "storage_mutation_allowed",
        "vendor_request_started",
    ):
        if report[key] is not False:
            raise ValueError("operations readiness artifact safety boundary is invalid")

    blockers = report["blockers"]
    if (
        not isinstance(blockers, list)
        or any(not isinstance(value, str) or not value.strip() for value in blockers)
        or blockers != sorted(set(blockers))
    ):
        raise ValueError("operations readiness artifact blockers are invalid")
    expected_status = "ready" if not blockers else "blocked"
    if report["status"] != expected_status:
        raise ValueError("operations readiness artifact status is invalid")

    sections = report["sections"]
    if (
        not isinstance(sections, dict)
        or not sections
        or any(
            not isinstance(name, str)
            or not name.strip()
            or not isinstance(section, dict)
            or not isinstance(section.get("status"), str)
            for name, section in sections.items()
        )
    ):
        raise ValueError("operations readiness artifact sections are invalid")

    generated_at = report["generated_at"]
    if not isinstance(generated_at, str):
        raise ValueError("operations readiness artifact timestamp is invalid")
    try:
        instant = datetime.fromisoformat(generated_at)
    except ValueError:
        raise ValueError("operations readiness artifact timestamp is invalid") from None
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("operations readiness artifact timestamp is invalid")

    report_hash = report["report_hash"]
    if not isinstance(report_hash, str):
        raise ValueError("operations readiness artifact hash is invalid")
    _require_lowercase_sha256(report_hash, name="operations readiness report hash")
    expected_hash = _canonical_hash(
        {
            "sections": sections,
            "version": report["version"],
        }
    )
    if report_hash != expected_hash:
        raise ValueError("operations readiness artifact hash is invalid")
    return report


def load_operations_readiness_artifact(path: Path) -> dict[str, object]:
    """Load a bounded UTF-8 JSON artifact without accepting partial content."""

    if not path.is_file() or path.is_symlink():
        raise ValueError("operations readiness artifact file is invalid")
    try:
        size = path.stat().st_size
    except OSError:
        raise ValueError("operations readiness artifact file is unavailable") from None
    if size < 2 or size > _MAXIMUM_ARTIFACT_BYTES:
        raise ValueError("operations readiness artifact file size is invalid")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise ValueError("operations readiness artifact JSON is invalid") from None
    return validate_operations_readiness_artifact(payload)


def write_operations_readiness_artifact(
    path: Path,
    payload: object,
    *,
    replace: bool,
) -> dict[str, object]:
    """Atomically write a validated report with owner-only file permissions."""

    report = validate_operations_readiness_artifact(payload)
    if path.suffix.casefold() != ".json" or not path.name.strip():
        raise ValueError("operations readiness artifact path must end in .json")
    parent = path.parent
    if not parent.is_dir() or parent.is_symlink():
        raise ValueError("operations readiness artifact directory is invalid")
    if path.exists() and (not replace or path.is_symlink() or not path.is_file()):
        raise ValueError("operations readiness artifact already exists or is unsafe")

    encoded = (
        json.dumps(
            report,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(encoded) > _MAXIMUM_ARTIFACT_BYTES:
        raise ValueError("operations readiness artifact is too large")

    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            os.chmod(temporary.name, 0o600)
            temporary.write(encoded)
            temporary.flush()
            os.fsync(temporary.fileno())
        if path.exists() and not replace:
            raise ValueError("operations readiness artifact already exists")
        os.replace(temporary_path, path)
        temporary_path = None
    except (OSError, ValueError):
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise
    return report
