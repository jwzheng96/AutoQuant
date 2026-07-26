from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.control import KillSwitchControl, KillSwitchReason
from autoquant.execution.low_volatility_decision_signal import (
    LOW_VOLATILITY_DECISION_TIME_SIGNAL_VERSION,
    LowVolatilityDecisionTimePaperSignal,
)
from autoquant.execution.low_volatility_decision_signal_compiler import (
    LowVolatilityDecisionTimeSignalCompiler,
)
from autoquant.execution.low_volatility_decision_signal_store import (
    _parameters,
    _signal,
)
from autoquant.execution.paper_policy import default_paper_policy
from autoquant.execution.reconciliation import (
    AccountPosition,
    ExecutionAccountSnapshot,
    ReconciliationReport,
)

SESSION_DATE = date(2026, 7, 27)
PREPARED_AT = datetime(2026, 7, 27, 1, 5, tzinfo=UTC)
EVIDENCE_AT = PREPARED_AT - timedelta(seconds=1)
INSTRUMENTS = tuple(f"{index:06d}.XSHE" for index in range(1, 61))


def _decision_signal() -> LowVolatilityDecisionTimePaperSignal:
    return LowVolatilityDecisionTimePaperSignal(
        deployment_contract_hash="a" * 64,
        candidate_approval_hash="b" * 64,
        compatibility_run_hash="c" * 64,
        observation_signal_hash="d" * 64,
        reconciliation_report_hash="e" * 64,
        internal_account_snapshot_hash="f" * 64,
        broker_account_snapshot_hash="1" * 64,
        kill_switch_event_hash="2" * 64,
        account_id="paper-main",
        strategy_id="low-volatility-paper",
        source_spec_hash="3" * 64,
        forward_spec_hash="4" * 64,
        compatibility_spec_hash="5" * 64,
        risk_policy_hash="6" * 64,
        snapshot_hash="7" * 64,
        dataset_manifest_hash="8" * 64,
        rule_set_hash="9" * 64,
        session_sequence=1,
        session_date=SESSION_DATE,
        signal_date=SESSION_DATE - timedelta(days=1),
        account_evidence_at=EVIDENCE_AT,
        kill_switch_changed_at=EVIDENCE_AT - timedelta(minutes=1),
        selected_instruments=(),
        held_instruments=(INSTRUMENTS[0],),
        valuation_instruments=INSTRUMENTS,
        prepared_by="paper-risk-operator",
        prepared_at=PREPARED_AT,
    )


def test_decision_signal_round_trips_without_authority() -> None:
    signal = _decision_signal()

    restored = LowVolatilityDecisionTimePaperSignal.from_payload(
        signal.payload()
    )

    assert restored == signal
    assert signal.version == LOW_VOLATILITY_DECISION_TIME_SIGNAL_VERSION
    assert signal.execution_timing_compatible is True
    assert signal.paper_activation_authority_granted is False
    assert signal.runtime_activation_allowed is False
    assert signal.live_trading_locked is True


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("held_position_valuation_coverage_verified", False),
        ("point_in_time_universe_verified", False),
        ("exact_risk_policy_verified", False),
        ("exact_session_rules_verified", False),
        ("decision_time_inputs_verified", False),
        ("account_reconciled", False),
        ("no_open_orders_verified", False),
        ("kill_switch_active", False),
        ("execution_timing_compatible", False),
        ("paper_activation_authority_granted", True),
        ("runtime_activation_allowed", True),
        ("live_trading_locked", False),
    ),
)
def test_decision_signal_rejects_weakened_gate(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match="decision-time paper signal"):
        replace(
            _decision_signal(),
            **{field: value},
        )


def test_decision_signal_rejects_unvalued_holding_or_stale_account() -> None:
    with pytest.raises(ValueError, match="decision-time paper signal"):
        replace(
            _decision_signal(),
            held_instruments=("999999.XSHE",),
        )
    with pytest.raises(ValueError, match="decision-time paper signal"):
        replace(
            _decision_signal(),
            account_evidence_at=PREPARED_AT - timedelta(minutes=6),
        )


def test_decision_signal_store_verifies_every_hard_lock() -> None:
    signal = _decision_signal()
    row: dict[str, object] = {
        **signal.payload(),
        "account_evidence_at": signal.account_evidence_at,
        "held_position_count": len(signal.held_instruments),
        "kill_switch_changed_at": signal.kill_switch_changed_at,
        "payload": signal.payload(),
        "prepared_at": signal.prepared_at,
        "selected_count": len(signal.selected_instruments),
        "session_date": signal.session_date,
        "signal_hash": signal.signal_hash,
        "signal_date": signal.signal_date,
        "signal_version": signal.version,
        "valuation_count": len(signal.valuation_instruments),
    }

    assert _signal(cast(Any, row)) == signal
    assert _parameters(signal)["signal_hash"] == signal.signal_hash

    row["runtime_activation_allowed"] = True
    with pytest.raises(PersistenceUnavailableError, match="integrity"):
        _signal(cast(Any, row))


def _account(*, evidence_hash: str) -> ExecutionAccountSnapshot:
    return ExecutionAccountSnapshot(
        account_id="paper-main",
        as_of=EVIDENCE_AT - timedelta(seconds=1),
        cash=Decimal("999000"),
        equity=Decimal("1000000"),
        positions=(
            AccountPosition(
                instrument=INSTRUMENTS[0],
                total_quantity=100,
                sellable_quantity=100,
                market_value=Decimal("1000"),
            ),
        ),
        projection_version="paper-account-projection-v1",
        evidence_hash=evidence_hash,
    )


def _compiler_evidence() -> dict[str, object]:
    policy = default_paper_policy(INSTRUMENTS)
    internal = _account(evidence_hash="a" * 64)
    broker = _account(evidence_hash="b" * 64)
    reconciliation = ReconciliationReport(
        account_id="paper-main",
        evaluated_at=EVIDENCE_AT,
        internal_snapshot_hash=internal.snapshot_hash,
        broker_snapshot_hash=broker.snapshot_hash,
        issues=(),
    )
    contract = SimpleNamespace(
        contract_hash="c" * 64,
        source_spec_hash="d" * 64,
        forward_spec_hash="e" * 64,
        compatibility_spec_hash="f" * 64,
        required_order_policy_version=(
            "low-volatility-prior-close-order-intents-v1"
        ),
        frozen_at=PREPARED_AT - timedelta(days=4),
    )
    candidate = SimpleNamespace(
        approval_hash="1" * 64,
        account_id="paper-main",
        strategy_id="low-volatility-paper",
        source_spec_hash=contract.source_spec_hash,
        forward_spec_hash=contract.forward_spec_hash,
        evaluation_result_hash="2" * 64,
        risk_policy_hash=policy.policy_hash,
        instruments=INSTRUMENTS,
        approved_at=PREPARED_AT - timedelta(days=1),
        runtime_activation_allowed=False,
        live_trading_locked=True,
    )
    compatibility = SimpleNamespace(
        run_hash="3" * 64,
        compatibility_spec_hash=contract.compatibility_spec_hash,
        original_evaluation_result_hash=(
            candidate.evaluation_result_hash
        ),
        forward_spec_hash=contract.forward_spec_hash,
        source_spec_hash=contract.source_spec_hash,
        decision_order_policy_version=(
            contract.required_order_policy_version
        ),
        completed_at=PREPARED_AT - timedelta(days=2),
        execution_timing_compatible=True,
    )
    observation = SimpleNamespace(
        signal_hash="4" * 64,
        candidate_approval_hash=candidate.approval_hash,
        account_id=candidate.account_id,
        strategy_id=candidate.strategy_id,
        source_spec_hash=candidate.source_spec_hash,
        risk_policy_hash=policy.policy_hash,
        session_sequence=1,
        session_date=SESSION_DATE,
        signal_date=SESSION_DATE - timedelta(days=1),
        snapshot_hash="5" * 64,
        dataset_manifest_hash="6" * 64,
        rule_set_hash="7" * 64,
        selected_instruments=(),
        valuations=tuple(
            SimpleNamespace(instrument=value) for value in INSTRUMENTS
        ),
        prepared_at=PREPARED_AT - timedelta(minutes=1),
        execution_timing_compatible=False,
        runtime_activation_allowed=False,
        live_trading_locked=True,
    )
    kill_switch = KillSwitchControl(
        account_id="paper-main",
        active=True,
        version=1,
        reason=KillSwitchReason.INITIALIZING,
        changed_at=PREPARED_AT - timedelta(minutes=2),
        changed_by="system",
        last_event_hash="8" * 64,
    )
    return {
        "broker_account": broker,
        "candidate": candidate,
        "compatibility": compatibility,
        "contract": contract,
        "internal_account": internal,
        "kill_switch": kill_switch,
        "observation": observation,
        "reconciliation": reconciliation,
        "risk_policy": policy,
    }


def test_compiler_binds_reconciled_held_position_evidence() -> None:
    evidence = _compiler_evidence()

    signal = LowVolatilityDecisionTimeSignalCompiler().compile(
        **cast(Any, evidence),
        prepared_by="paper-risk-operator",
        prepared_at=PREPARED_AT,
    )

    assert signal.held_instruments == (INSTRUMENTS[0],)
    assert signal.valuation_instruments == INSTRUMENTS
    assert signal.account_reconciled is True
    assert signal.kill_switch_active is True
    assert signal.runtime_activation_allowed is False


def test_compiler_rejects_unvalued_or_unreconciled_account() -> None:
    evidence = _compiler_evidence()
    observation = cast(Any, evidence["observation"])
    observation.valuations = tuple(
        value
        for value in observation.valuations
        if value.instrument != INSTRUMENTS[0]
    )
    with pytest.raises(ValueError, match="value every held"):
        LowVolatilityDecisionTimeSignalCompiler().compile(
            **cast(Any, evidence),
            prepared_by="paper-risk-operator",
            prepared_at=PREPARED_AT,
        )

    evidence = _compiler_evidence()
    reconciliation = cast(Any, evidence["reconciliation"])
    evidence["reconciliation"] = replace(
        reconciliation,
        issues=(),
        evaluated_at=PREPARED_AT - timedelta(minutes=1),
    )
    with pytest.raises(ValueError, match="account evidence"):
        LowVolatilityDecisionTimeSignalCompiler().compile(
            **cast(Any, evidence),
            prepared_by="paper-risk-operator",
            prepared_at=PREPARED_AT,
        )


def test_decision_signal_migration_is_exact_and_immutable() -> None:
    sql = Path(
        "migrations/postgres/051_low_volatility_decision_time_signals.sql"
    ).read_text(encoding="utf-8")

    assert "low_volatility_decision_time_paper_signals" in sql
    assert "compatibility.completed_at >" in sql
    assert "candidate.approved_at" in sql
    assert "execution_reconciliation_reports" in sql
    assert "execution_account_snapshots" in sql
    assert "execution_control_state" in sql
    assert "held_instruments" in sql
    assert "NOT paper_activation_authority_granted" in sql
    assert "NOT runtime_activation_allowed" in sql
    assert sql.count("autoquant_reject_immutable_change()") == 1
    assert "VALUES ('postgres', 51)" in sql
