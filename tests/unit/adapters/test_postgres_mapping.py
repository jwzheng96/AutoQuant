from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from open_quant.adapters.postgres import (
    audit_event_hash,
    source_evidence_parameters,
)
from open_quant.data.models import SourceEvidence

OCCURRED_AT = datetime(2026, 7, 21, 8, 0, 0, 123456, tzinfo=UTC)
PREVIOUS_HASH = "a" * 64


def test_postgres_migration_is_forward_only_and_guards_manifest_quality() -> None:
    migration = Path("migrations/postgres/001_phase1.sql").read_text(encoding="utf-8")

    assert "DROP TABLE" not in migration.upper()
    assert "DROP TRIGGER" not in migration.upper()
    assert (
        "BEFORE INSERT OR UPDATE OF quality_report_hash, production_complete, payload"
        in migration
    )
    assert "production manifest requires passing complete quality report" in migration


def test_audit_hash_binds_previous_hash_type_time_and_canonical_payload() -> None:
    payload = {"z": [1, True, None], "a": {"value": "ok"}}

    actual = audit_event_hash(
        previous_hash=PREVIOUS_HASH,
        event_type="ingestion_completed",
        occurred_at=OCCURRED_AT,
        payload=payload,
    )

    canonical = json.dumps(
        {
            "event_type": "ingestion_completed",
            "occurred_at": OCCURRED_AT.isoformat(timespec="microseconds"),
            "payload": payload,
            "previous_hash": PREVIOUS_HASH,
        },
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    assert actual == hashlib.sha256(canonical).hexdigest()
    assert actual != audit_event_hash(
        previous_hash="b" * 64,
        event_type="ingestion_completed",
        occurred_at=OCCURRED_AT,
        payload=payload,
    )
    assert actual != audit_event_hash(
        previous_hash=PREVIOUS_HASH,
        event_type="ingestion_rejected",
        occurred_at=OCCURRED_AT,
        payload=payload,
    )
    assert actual != audit_event_hash(
        previous_hash=PREVIOUS_HASH,
        event_type="ingestion_completed",
        occurred_at=OCCURRED_AT + timedelta(microseconds=1),
        payload=payload,
    )


@pytest.mark.parametrize(
    "payload",
    [
        {"value": float("nan")},
        {"value": float("inf")},
        {"value": b"secret"},
        {"value": OCCURRED_AT},
        {1: "non-string key"},
        {"value": {1, 2}},
    ],
)
def test_audit_hash_rejects_non_canonical_json(payload: object) -> None:
    with pytest.raises((TypeError, ValueError), match="JSON-safe"):
        audit_event_hash(
            previous_hash=PREVIOUS_HASH,
            event_type="ingestion_completed",
            occurred_at=OCCURRED_AT,
            payload=payload,
        )


def test_audit_hash_rejects_naive_timestamp_and_malformed_identity() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        audit_event_hash(
            previous_hash=PREVIOUS_HASH,
            event_type="ingestion_completed",
            occurred_at=OCCURRED_AT.replace(tzinfo=None),
            payload={},
        )
    with pytest.raises(ValueError, match="event_type"):
        audit_event_hash(
            previous_hash=PREVIOUS_HASH,
            event_type=" ",
            occurred_at=OCCURRED_AT,
            payload={},
        )
    with pytest.raises(ValueError, match="previous_hash"):
        audit_event_hash(
            previous_hash="not-a-hash",
            event_type="ingestion_completed",
            occurred_at=OCCURRED_AT,
            payload={},
        )


def test_source_evidence_maps_exact_verified_body_without_auth_material() -> None:
    body = b"date,000001.XSHE\n2026-07-20,False\n"
    evidence = SourceEvidence(
        source="rqdata",
        method="is_suspended",
        requested_at=OCCURRED_AT,
        response_body=body,
        response_hash=hashlib.sha256(body).hexdigest(),
    )

    assert source_evidence_parameters(evidence) == {
        "evidence_hash": evidence.response_hash,
        "source": "rqdata",
        "method": "is_suspended",
        "requested_at": OCCURRED_AT,
        "response_body": body,
    }


@pytest.mark.parametrize(
    "method",
    ["auth", "authenticate", "authorization", "token", "password", "get_auth_headers"],
)
def test_source_evidence_rejects_authentication_shaped_methods(method: str) -> None:
    body = b"not a credential"
    evidence = SourceEvidence(
        source="rqdata",
        method=method,
        requested_at=OCCURRED_AT,
        response_body=body,
        response_hash=hashlib.sha256(body).hexdigest(),
    )

    with pytest.raises(ValueError, match="authentication"):
        source_evidence_parameters(evidence)
