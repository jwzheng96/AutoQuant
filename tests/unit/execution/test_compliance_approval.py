from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.compliance_approval import (
    COMPLIANCE_APPROVAL_VERSION,
    COMPLIANCE_REVOCATION_VERSION,
    ComplianceApproval,
    ComplianceRevocation,
    ComplianceRevocationReason,
    _approval_from_row,
    _revocation_from_row,
)

APPROVED_AT = datetime(2026, 7, 24, 3, tzinfo=UTC)


def _approval() -> ComplianceApproval:
    return ComplianceApproval(
        account_id="paper-main",
        strategy_id="validated-sma-paper",
        registration_hash="a" * 64,
        policy_hash="b" * 64,
        external_artifact_hash="c" * 64,
        approval_reference="GRC/AQ/2026-0001",
        approved_by="independent-compliance",
        approved_at=APPROVED_AT,
        valid_until=APPROVED_AT + timedelta(days=7),
    )


def test_compliance_approval_round_trips_and_is_scope_bound() -> None:
    approval = _approval()

    restored = ComplianceApproval.from_payload(approval.payload())

    assert restored == approval
    assert approval.version == COMPLIANCE_APPROVAL_VERSION
    assert approval.registration_hash == "a" * 64
    assert approval.policy_hash == "b" * 64


def test_compliance_approval_rejects_long_or_empty_validity() -> None:
    with pytest.raises(ValueError, match="validity"):
        replace(
            _approval(),
            valid_until=APPROVED_AT + timedelta(days=32),
        )
    with pytest.raises(ValueError, match="validity"):
        replace(
            _approval(),
            valid_until=APPROVED_AT,
        )


def test_compliance_revocation_round_trips() -> None:
    revocation = ComplianceRevocation(
        approval_hash=_approval().approval_hash,
        revoked_by="risk-operator",
        revoked_at=APPROVED_AT + timedelta(hours=1),
        reason=(ComplianceRevocationReason.OPERATOR_SAFETY_ACTION),
    )

    restored = ComplianceRevocation.from_payload(revocation.payload())

    assert restored == revocation
    assert revocation.version == COMPLIANCE_REVOCATION_VERSION


def test_compliance_store_rows_verify_integrity() -> None:
    approval = _approval()
    approval_row: dict[str, object] = {
        **approval.payload(),
        "approval_hash": approval.approval_hash,
        "approval_version": approval.version,
        "approved_at": approval.approved_at,
        "live_trading_locked": True,
        "payload": approval.payload(),
        "valid_until": approval.valid_until,
    }

    assert _approval_from_row(cast(Any, approval_row)) == approval

    approval_row["live_trading_locked"] = False
    with pytest.raises(
        PersistenceUnavailableError,
        match="integrity",
    ):
        _approval_from_row(cast(Any, approval_row))

    revocation = ComplianceRevocation(
        approval_hash=approval.approval_hash,
        revoked_by="risk-operator",
        revoked_at=APPROVED_AT + timedelta(hours=1),
        reason=ComplianceRevocationReason.RISK_CHANGED,
    )
    revocation_row: dict[str, object] = {
        **revocation.payload(),
        "reason": revocation.reason.value,
        "revocation_hash": revocation.revocation_hash,
        "revocation_version": revocation.version,
        "revoked_at": revocation.revoked_at,
        "payload": revocation.payload(),
    }
    assert _revocation_from_row(cast(Any, revocation_row)) == revocation


def test_compliance_migration_is_append_only() -> None:
    sql = Path("migrations/postgres/036_paper_compliance_approvals.sql").read_text(encoding="utf-8")

    assert ("CREATE TABLE IF NOT EXISTS paper_compliance_approvals") in sql
    assert ("CREATE TABLE IF NOT EXISTS paper_compliance_revocations") in sql
    assert sql.count("autoquant_reject_immutable_change()") == 2
    assert "live_trading_locked" in sql
    assert "VALUES ('postgres', 36)" in sql
