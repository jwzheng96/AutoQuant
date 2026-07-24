from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from autoquant.backtest.codec import encode_backtest_result
from autoquant.backtest.low_volatility_execution_compatibility import (
    DECISION_TIME_INPUTS,
    FORBIDDEN_INTENT_INPUTS,
    LOW_VOLATILITY_EXECUTION_COMPATIBILITY_RUN_VERSION,
    LOW_VOLATILITY_EXECUTION_COMPATIBILITY_VERSION,
    LowVolatilityExecutionCompatibilityRun,
    LowVolatilityExecutionCompatibilitySpec,
)
from autoquant.backtest.models import AccountSnapshot, BacktestResult
from autoquant.errors import PersistenceUnavailableError
from autoquant.web.low_volatility_execution_compatibility_run_store import (
    _parameters,
    _run,
)
from autoquant.web.low_volatility_execution_compatibility_store import (
    _spec,
)

FROZEN_AT = datetime(2026, 7, 24, 8, tzinfo=UTC)


def _compatibility() -> LowVolatilityExecutionCompatibilitySpec:
    return LowVolatilityExecutionCompatibilitySpec(
        source_spec_hash="a" * 64,
        forward_spec_hash="b" * 64,
        observed_forward_session_count=1,
        frozen_by="research-operator",
        frozen_at=FROZEN_AT,
    )


def _strategy_result() -> BacktestResult:
    snapshots = tuple(
        AccountSnapshot(
            session_date=date(2026, 1, 1) + timedelta(days=index),
            cash=Decimal("1000000"),
            market_value=Decimal("0"),
            equity=Decimal("1000000"),
            positions=(),
            ledger_hash="0" * 64,
        )
        for index in range(126)
    )
    return BacktestResult(
        strategy_id=("dynamic-universe-low-volatility-v4:decision-time-execution-v1"),
        manifest_hash="8" * 64,
        as_of=FROZEN_AT,
        initial_cash=Decimal("1000000"),
        ending_equity=Decimal("1000000"),
        total_return=Decimal("0"),
        max_drawdown=Decimal("0"),
        turnover=Decimal("0"),
        total_fees=Decimal("0"),
        reports=(),
        snapshots=snapshots,
        events=(),
        rule_versions=("ashare-test",),
        fee_version="fee-test",
        execution_version="execution-test",
        ledger_hash="0" * 64,
    )


def _compatibility_run() -> LowVolatilityExecutionCompatibilityRun:
    return LowVolatilityExecutionCompatibilityRun(
        compatibility_spec_hash="a" * 64,
        original_evaluation_result_hash="b" * 64,
        corrected_forward_result_hash="c" * 64,
        corrected_assessment_hash="d" * 64,
        forward_spec_hash="e" * 64,
        source_spec_hash="f" * 64,
        evaluation_dataset_manifest_hash="7" * 64,
        panel_hash="8" * 64,
        strategy_result=_strategy_result(),
        gate_failures=(),
        completed_by="research-operator",
        completed_at=FROZEN_AT,
    )


def test_compatibility_spec_round_trips_with_partial_disclosure() -> None:
    spec = _compatibility()

    restored = LowVolatilityExecutionCompatibilitySpec.from_payload(spec.payload())

    assert restored == spec
    assert spec.version == (LOW_VOLATILITY_EXECUTION_COMPATIBILITY_VERSION)
    assert spec.partial_outcome_observed_before_freeze is True
    assert spec.terminal_outcome_observed_before_freeze is False
    assert spec.decision_time_inputs == DECISION_TIME_INPUTS
    assert spec.forbidden_intent_inputs == FORBIDDEN_INTENT_INPUTS
    assert spec.compatibility_can_only_disqualify is True
    assert spec.paper_activation_allowed is False
    assert spec.live_trading_locked is True


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("observed_forward_session_count", 126),
        ("order_intent_invariance_required", False),
        ("same_forward_window_required", False),
        ("compatibility_can_only_disqualify", False),
        ("terminal_outcome_observed_before_freeze", True),
        ("historical_reclassification_allowed", True),
        ("paper_activation_allowed", True),
        ("live_trading_locked", False),
    ),
)
def test_compatibility_spec_rejects_late_or_weakened_freeze(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match="compatibility spec"):
        replace(_compatibility(), **{field: value})


def test_compatibility_store_row_verifies_integrity() -> None:
    spec = _compatibility()
    row: dict[str, object] = {
        **spec.payload(),
        "compatibility_version": spec.version,
        "frozen_at": spec.frozen_at,
        "payload": spec.payload(),
        "spec_hash": spec.spec_hash,
    }

    assert _spec(cast(Any, row)) == spec

    row["paper_activation_allowed"] = True
    with pytest.raises(PersistenceUnavailableError, match="integrity"):
        _spec(cast(Any, row))


def test_compatibility_migration_is_early_append_only_gate() -> None:
    sql = Path("migrations/postgres/048_low_volatility_execution_compatibility.sql").read_text(
        encoding="utf-8"
    )

    assert "low_volatility_execution_compatibility_specs" in sql
    assert "observed_forward_session_count BETWEEN 0 AND 125" in sql
    assert "frozen_session_count >= 126" in sql
    assert "low_volatility_forward_evaluation_runs" in sql
    assert "compatibility_can_only_disqualify" in sql
    assert "NOT paper_activation_allowed" in sql
    assert sql.count("autoquant_reject_immutable_change()") == 1
    assert "VALUES ('postgres', 48)" in sql


def test_compatibility_run_round_trips_without_enabling_runtime() -> None:
    run = _compatibility_run()

    restored = LowVolatilityExecutionCompatibilityRun.from_payload(
        run.payload(),
        strategy_result=run.strategy_result,
    )

    assert restored == run
    assert run.version == (LOW_VOLATILITY_EXECUTION_COMPATIBILITY_RUN_VERSION)
    assert run.compatibility_status == "compatible"
    assert run.execution_timing_compatible is True
    assert run.paper_activation_allowed is False
    assert run.runtime_activation_allowed is False
    assert run.live_trading_locked is True


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("order_intent_invariance_verified", False),
        ("paper_activation_allowed", True),
        ("runtime_activation_allowed", True),
        ("live_trading_locked", False),
        ("session_count", 125),
    ),
)
def test_compatibility_run_rejects_weakened_evidence(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match="compatibility run"):
        replace(_compatibility_run(), **{field: value})


def test_compatibility_run_rejects_unsubstantiated_execution_gate() -> None:
    with pytest.raises(ValueError, match="compatibility run"):
        replace(
            _compatibility_run(),
            gate_failures=("execution_rejections",),
        )


def test_compatibility_run_store_row_verifies_full_artifact() -> None:
    run = _compatibility_run()
    row: dict[str, object] = {
        **run.payload(),
        "run_hash": run.run_hash,
        "run_version": run.version,
        "completed_at": run.completed_at,
        "strategy_payload": encode_backtest_result(run.strategy_result),
        "payload": run.payload(),
    }

    assert _run(cast(Any, row)) == run
    assert _parameters(run)["run_hash"] == run.run_hash

    row["runtime_activation_allowed"] = True
    with pytest.raises(PersistenceUnavailableError, match="integrity"):
        _run(cast(Any, row))


def test_compatibility_run_migration_is_immutable_and_locked() -> None:
    sql = Path("migrations/postgres/049_low_volatility_execution_compatibility_runs.sql").read_text(
        encoding="utf-8"
    )

    assert "low_volatility_execution_compatibility_runs" in sql
    assert "evaluation.evidence_status <> 'paper_candidate'" in sql
    assert "NOT paper_activation_allowed" in sql
    assert "NOT runtime_activation_allowed" in sql
    assert "live_trading_locked" in sql
    assert sql.count("autoquant_reject_immutable_change()") == 1
    assert "VALUES ('postgres', 49)" in sql
