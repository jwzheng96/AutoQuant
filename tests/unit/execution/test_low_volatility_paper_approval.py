from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.low_volatility_paper_approval import (
    LOW_VOLATILITY_PAPER_APPROVAL_VERSION,
    LOW_VOLATILITY_PAPER_REVOCATION_VERSION,
    LowVolatilityPaperCandidateApproval,
    LowVolatilityPaperCandidateRevocation,
    LowVolatilityPaperRevocationReason,
)
from autoquant.execution.low_volatility_paper_approval_store import (
    LowVolatilityPaperCandidateRecord,
    _approval,
    _revocation,
)

APPROVED_AT = datetime(2026, 7, 24, 6, tzinfo=UTC)


def _candidate() -> LowVolatilityPaperCandidateApproval:
    return LowVolatilityPaperCandidateApproval(
        account_id="paper-main",
        strategy_id="low-volatility-paper",
        forward_spec_hash="a" * 64,
        evaluation_result_hash="b" * 64,
        evaluation_assessment_hash="c" * 64,
        evaluation_dataset_manifest_hash="d" * 64,
        source_spec_hash="e" * 64,
        risk_policy_hash="f" * 64,
        instruments=("000001.XSHE", "600000.XSHG"),
        approved_by="risk-operator",
        approved_at=APPROVED_AT,
    )


def test_candidate_round_trips_without_runtime_authority() -> None:
    candidate = _candidate()

    restored = LowVolatilityPaperCandidateApproval.from_payload(candidate.payload())

    assert restored == candidate
    assert candidate.version == LOW_VOLATILITY_PAPER_APPROVAL_VERSION
    assert candidate.evidence_status == "paper_candidate"
    assert candidate.minimum_paper_sessions == 60
    assert candidate.execution_mode == "paper"
    assert candidate.daily_signal_evidence_required is True
    assert candidate.runtime_activation_allowed is False
    assert candidate.live_trading_locked is True


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("evidence_status", "approved"),
        ("minimum_paper_sessions", 59),
        ("execution_mode", "live"),
        ("daily_signal_evidence_required", False),
        ("runtime_activation_allowed", True),
        ("live_trading_locked", False),
    ),
)
def test_candidate_rejects_governance_weakening(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match="candidate approval"):
        replace(_candidate(), **{field: value})


def test_candidate_requires_sorted_unique_instruments() -> None:
    with pytest.raises(ValueError, match="candidate approval"):
        replace(
            _candidate(),
            instruments=("600000.XSHG", "000001.XSHE"),
        )
    with pytest.raises(ValueError, match="candidate approval"):
        replace(
            _candidate(),
            instruments=("000001.XSHE", "000001.XSHE"),
        )


def test_revocation_is_append_only_safety_evidence() -> None:
    revocation = LowVolatilityPaperCandidateRevocation(
        approval_hash=_candidate().approval_hash,
        revoked_by="risk-operator",
        revoked_at=APPROVED_AT + timedelta(minutes=1),
        reason=LowVolatilityPaperRevocationReason.RISK_CHANGED,
    )
    record = LowVolatilityPaperCandidateRecord(
        approval=_candidate(),
        revocation=revocation,
    )

    assert revocation.version == LOW_VOLATILITY_PAPER_REVOCATION_VERSION
    assert revocation.live_trading_locked is True
    assert record.active is False


def test_candidate_store_rows_verify_payload_and_columns() -> None:
    candidate = _candidate()
    row: dict[str, object] = {
        **candidate.payload(),
        "approval_hash": candidate.approval_hash,
        "approval_version": candidate.version,
        "approved_at": candidate.approved_at,
        "instrument_count": len(candidate.instruments),
        "payload": candidate.payload(),
    }

    assert _approval(cast(Any, row)) == candidate

    row["approved_by"] = "different-operator"
    with pytest.raises(PersistenceUnavailableError, match="integrity"):
        _approval(cast(Any, row))


def test_revocation_store_rows_verify_payload_and_columns() -> None:
    revocation = LowVolatilityPaperCandidateRevocation(
        approval_hash=_candidate().approval_hash,
        revoked_by="risk-operator",
        revoked_at=APPROVED_AT + timedelta(minutes=1),
        reason=(LowVolatilityPaperRevocationReason.OPERATOR_SAFETY_ACTION),
    )
    row: dict[str, object] = {
        **revocation.payload(),
        "revocation_hash": revocation.revocation_hash,
        "revocation_version": revocation.version,
        "revoked_at": revocation.revoked_at,
        "payload": revocation.payload(),
    }

    assert _revocation(cast(Any, row)) == revocation

    row["live_trading_locked"] = False
    with pytest.raises(PersistenceUnavailableError, match="integrity"):
        _revocation(cast(Any, row))


def test_candidate_migration_hard_locks_runtime_and_evidence() -> None:
    sql = Path("migrations/postgres/046_low_volatility_paper_candidate_approvals.sql").read_text(
        encoding="utf-8"
    )

    assert ("CREATE TABLE IF NOT EXISTS low_volatility_paper_candidate_approvals") in sql
    assert ("CREATE TABLE IF NOT EXISTS low_volatility_paper_candidate_revocations") in sql
    assert "runtime_activation_allowed" in sql
    assert "NOT runtime_activation_allowed" in sql
    assert "daily_signal_evidence_required" in sql
    assert "autoquant_validate_low_volatility_paper_candidate" in sql
    assert "evidence.evidence_status <> 'paper_candidate'" in sql
    assert "NEW.approved_at < evidence.completed_at" in sql
    assert sql.count("autoquant_reject_immutable_change()") == 2
    assert "VALUES ('postgres', 46)" in sql
