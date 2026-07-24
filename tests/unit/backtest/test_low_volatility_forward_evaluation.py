from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from autoquant.backtest.codec import encode_backtest_result
from autoquant.backtest.dynamic_portfolio import (
    DYNAMIC_PORTFOLIO_BENCHMARK_VERSION,
)
from autoquant.backtest.low_volatility_forward import (
    LowVolatilityForwardEvidenceSpec,
    LowVolatilityForwardSessionBinding,
)
from autoquant.backtest.low_volatility_forward_evaluation import (
    LowVolatilityForwardAssessment,
    assess_low_volatility_forward,
    build_low_volatility_forward_result,
)
from autoquant.backtest.low_volatility_validation import (
    LowVolatilityValidationEvidence,
    LowVolatilityValidationFold,
    LowVolatilityValidationResult,
)
from autoquant.backtest.models import (
    AccountSnapshot,
    BacktestResult,
    PositionSnapshot,
)
from autoquant.errors import PersistenceUnavailableError
from autoquant.web.low_volatility_forward_evaluation_store import (
    LowVolatilityForwardEvaluationRecord,
    _binding_parameters,
    _block_parameters,
    _record,
    _run_parameters,
)

_INITIAL_CASH = Decimal("1000000")


def _backtest(
    *,
    strategy_id: str,
    manifest_hash: str,
    as_of: datetime,
    dates: tuple[date, ...],
    block_return: Decimal,
    unresolved: bool = False,
    max_drawdown: Decimal = Decimal("0"),
) -> BacktestResult:
    equity = _INITIAL_CASH
    snapshots: list[AccountSnapshot] = []
    for index, session_date in enumerate(dates):
        if index % 21 == 20:
            equity *= Decimal("1") + block_return
        snapshots.append(
            AccountSnapshot(
                session_date=session_date,
                cash=equity,
                market_value=Decimal("0"),
                equity=equity,
                positions=(),
                ledger_hash="0" * 64,
            )
        )
    if unresolved:
        position = PositionSnapshot(
            instrument="000001.XSHE",
            total_quantity=100,
            sellable_quantity=100,
            average_cost=Decimal("10"),
            market_price=Decimal("10"),
            market_value=Decimal("1000"),
            unrealized_pnl=Decimal("0"),
        )
        snapshots[-1] = replace(
            snapshots[-1],
            cash=equity - Decimal("1000"),
            market_value=Decimal("1000"),
            positions=(position,),
        )
    return BacktestResult(
        strategy_id=strategy_id,
        manifest_hash=manifest_hash,
        as_of=as_of,
        initial_cash=_INITIAL_CASH,
        ending_equity=equity,
        total_return=equity / _INITIAL_CASH - Decimal("1"),
        max_drawdown=max_drawdown,
        turnover=Decimal("0"),
        total_fees=Decimal("0"),
        reports=(),
        snapshots=tuple(snapshots),
        events=(),
        rule_versions=("ashare-test",),
        fee_version="fee-test",
        execution_version="execution-test",
        ledger_hash="0" * 64,
    )


def _source_validation() -> LowVolatilityValidationResult:
    as_of = datetime(2026, 7, 22, tzinfo=UTC)
    panel_hash = "1" * 64
    train_dates = tuple(
        date(2020, 1, 1) + timedelta(days=index)
        for index in range(504)
    )
    test_dates = tuple(
        date(2022, 1, 1) + timedelta(days=index)
        for index in range(63)
    )
    training = _backtest(
        strategy_id="dynamic-universe-low-volatility-v4",
        manifest_hash=panel_hash,
        as_of=as_of,
        dates=train_dates,
        block_return=Decimal("0.008"),
    )
    test = _backtest(
        strategy_id="dynamic-universe-low-volatility-v4",
        manifest_hash=panel_hash,
        as_of=as_of,
        dates=test_dates,
        block_return=Decimal("0.004"),
    )
    benchmark = _backtest(
        strategy_id=DYNAMIC_PORTFOLIO_BENCHMARK_VERSION,
        manifest_hash=panel_hash,
        as_of=as_of,
        dates=test_dates,
        block_return=Decimal("0.002"),
    )
    fold = LowVolatilityValidationFold(
        sequence=1,
        train_start=train_dates[0],
        train_end=train_dates[-1],
        test_start=test_dates[0],
        test_end=test_dates[-1],
        training_result=training,
        test_result=test,
        benchmark_result=benchmark,
    )
    return LowVolatilityValidationResult(
        panel_hash=panel_hash,
        market_panel_hash="2" * 64,
        spec_hash="3" * 64,
        as_of=as_of,
        folds=(fold,),
        compounded_oos_return=test.total_return,
        benchmark_compounded_oos_return=benchmark.total_return,
        excess_oos_return=test.total_return - benchmark.total_return,
        profitable_fold_rate=Decimal("1"),
        worst_oos_drawdown=Decimal("0"),
        mean_training_return=training.total_return,
        train_test_gap=training.total_return - test.total_return,
        strategy_rejected_order_count=0,
        benchmark_rejected_order_count=0,
        strategy_unresolved_position_count=0,
        benchmark_unresolved_position_count=0,
    )


def _source_evidence(
    source: LowVolatilityValidationResult,
) -> LowVolatilityValidationEvidence:
    return LowVolatilityValidationEvidence(
        result_hash=source.result_hash,
        policy_hash="4" * 64,
        fold_count=1,
        oos_sessions=63,
        strategy_rejected_order_count=0,
        benchmark_rejected_order_count=0,
        strategy_unresolved_position_count=0,
        benchmark_unresolved_position_count=0,
        evidence_status="rejected",
        gate_failures=("train_test_gap",),
    )


def _forward_spec(
    source: LowVolatilityValidationResult,
    evidence: LowVolatilityValidationEvidence,
) -> LowVolatilityForwardEvidenceSpec:
    return LowVolatilityForwardEvidenceSpec(
        predecessor_result_hash=source.result_hash,
        predecessor_assessment_hash=evidence.assessment_hash,
        source_spec_hash=source.spec_hash,
        source_dataset_manifest_hash="5" * 64,
        forward_start_date=date(2026, 7, 23),
        maximum_annualized_stability_gap=Decimal("0.15"),
    )


def _bindings(
    spec: LowVolatilityForwardEvidenceSpec,
) -> tuple[LowVolatilityForwardSessionBinding, ...]:
    return tuple(
        LowVolatilityForwardSessionBinding(
            forward_spec_hash=spec.spec_hash,
            dataset_manifest_hash=f"{index + 1:064x}",
            policy_hash="6" * 64,
            session_date=spec.forward_start_date + timedelta(days=index),
            snapshot_hash=f"{index + 1000:064x}",
            snapshot_reference_date=(
                spec.forward_start_date + timedelta(days=index - 1)
            ),
            calendar_content_hash=f"{index + 2000:064x}",
            instruments=("000001.XSHE",),
        )
        for index in range(126)
    )


def _evaluation(
    *,
    strategy_block_return: Decimal = Decimal("0.01"),
    benchmark_block_return: Decimal = Decimal("0.005"),
    unresolved: bool = False,
    max_drawdown: Decimal = Decimal("0"),
):
    source = _source_validation()
    evidence = _source_evidence(source)
    spec = _forward_spec(source, evidence)
    bindings = _bindings(spec)
    dates = tuple(value.session_date for value in bindings)
    as_of = datetime(2027, 1, 25, tzinfo=UTC)
    panel_hash = "7" * 64
    strategy = _backtest(
        strategy_id="dynamic-universe-low-volatility-v4",
        manifest_hash=panel_hash,
        as_of=as_of,
        dates=dates,
        block_return=strategy_block_return,
        unresolved=unresolved,
        max_drawdown=max_drawdown,
    )
    benchmark = _backtest(
        strategy_id=DYNAMIC_PORTFOLIO_BENCHMARK_VERSION,
        manifest_hash=panel_hash,
        as_of=as_of,
        dates=dates,
        block_return=benchmark_block_return,
    )
    result = build_low_volatility_forward_result(
        panel_hash=panel_hash,
        market_panel_hash="8" * 64,
        as_of=as_of,
        forward_spec=spec,
        evaluation_dataset_manifest_hash="9" * 64,
        bindings=bindings,
        predecessor_result=source,
        strategy_result=strategy,
        benchmark_result=benchmark,
    )
    return spec, result


def test_complete_forward_window_produces_paper_candidate() -> None:
    spec, result = _evaluation()

    assessment = assess_low_volatility_forward(result, spec=spec)

    assert len(result.session_bindings) == 126
    assert len(result.blocks) == 6
    assert all(
        value.session_count == 21 and value.strategy_return == Decimal("0.01")
        for value in result.blocks
    )
    assert result.profitable_block_rate == Decimal("1")
    assert assessment.evidence_status == "paper_candidate"
    assert assessment.paper_trading_eligible is True
    assert assessment.live_trading_locked is True
    assert assessment.gate_failures == ()


def test_incomplete_forward_window_cannot_produce_a_result() -> None:
    source = _source_validation()
    evidence = _source_evidence(source)
    spec = _forward_spec(source, evidence)
    bindings = _bindings(spec)[:-1]
    dates = tuple(value.session_date for value in bindings)
    as_of = datetime(2027, 1, 25, tzinfo=UTC)
    panel_hash = "7" * 64

    with pytest.raises(ValueError, match="exactly 126"):
        build_low_volatility_forward_result(
            panel_hash=panel_hash,
            market_panel_hash="8" * 64,
            as_of=as_of,
            forward_spec=spec,
            evaluation_dataset_manifest_hash="9" * 64,
            bindings=bindings,
            predecessor_result=source,
            strategy_result=_backtest(
                strategy_id="dynamic-universe-low-volatility-v4",
                manifest_hash=panel_hash,
                as_of=as_of,
                dates=dates,
                block_return=Decimal("0.01"),
            ),
            benchmark_result=_backtest(
                strategy_id=DYNAMIC_PORTFOLIO_BENCHMARK_VERSION,
                manifest_hash=panel_hash,
                as_of=as_of,
                dates=dates,
                block_return=Decimal("0.005"),
            ),
        )


def test_out_of_order_binding_window_cannot_be_reclassified() -> None:
    _, result = _evaluation()
    reordered = (
        result.session_bindings[1],
        result.session_bindings[0],
        *result.session_bindings[2:],
    )

    with pytest.raises(ValueError, match="inconsistent"):
        replace(result, session_bindings=reordered)


def test_failed_forward_gates_remain_rejected_and_live_locked() -> None:
    spec, result = _evaluation(
        strategy_block_return=Decimal("-0.01"),
        benchmark_block_return=Decimal("0.005"),
        unresolved=True,
        max_drawdown=Decimal("0.20"),
    )

    assessment = assess_low_volatility_forward(result, spec=spec)

    assert isinstance(assessment, LowVolatilityForwardAssessment)
    assert assessment.evidence_status == "rejected"
    assert assessment.paper_trading_eligible is False
    assert assessment.live_trading_locked is True
    assert set(assessment.gate_failures) == {
        "annualized_stability_gap",
        "forward_drawdown_limit",
        "nonpositive_forward_excess_return",
        "nonpositive_forward_return",
        "profitable_block_rate",
        "unresolved_positions",
    }


def test_derived_metrics_detect_tampering() -> None:
    _, result = _evaluation()

    with pytest.raises(ValueError, match="not derived"):
        replace(
            result,
            profitable_block_rate=Decimal("0.99"),
        )


def test_forward_evaluation_store_round_trips_and_detects_tampering() -> None:
    spec, result = _evaluation()
    assessment = assess_low_volatility_forward(result, spec=spec)
    completed_at = datetime(2027, 1, 25, 8, tzinfo=UTC)
    expected = LowVolatilityForwardEvaluationRecord(
        result=result,
        assessment=assessment,
        requested_by="operator",
        completed_at=completed_at,
    )
    run: dict[str, object] = {
        **_run_parameters(expected),
        "paper_deployment_allowed": False,
        "live_trading_locked": True,
    }
    binding_rows = tuple(
        cast(
            Any,
            _binding_parameters(
                result.result_hash,
                sequence,
                binding,
            ),
        )
        for sequence, binding in enumerate(
            result.session_bindings,
            start=1,
        )
    )
    block_rows = tuple(
        cast(
            Any,
            _block_parameters(result.result_hash, block),
        )
        for block in result.blocks
    )

    assert (
        _record(
            cast(Any, run),
            binding_rows,
            block_rows,
        )
        == expected
    )

    run["paper_deployment_allowed"] = True
    with pytest.raises(
        PersistenceUnavailableError,
        match="integrity",
    ):
        _record(
            cast(Any, run),
            binding_rows,
            block_rows,
        )


def test_forward_evaluation_store_commits_full_backtest_artifacts() -> None:
    spec, result = _evaluation()
    assessment = assess_low_volatility_forward(result, spec=spec)
    record = LowVolatilityForwardEvaluationRecord(
        result=result,
        assessment=assessment,
        requested_by="operator",
        completed_at=datetime(2027, 1, 25, 8, tzinfo=UTC),
    )
    parameters = _run_parameters(record)

    assert parameters["strategy_payload"] == (
        json.dumps(
            encode_backtest_result(result.strategy_result),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    assert parameters["benchmark_payload"] == (
        json.dumps(
            encode_backtest_result(result.benchmark_result),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    )


def test_forward_evaluation_migration_is_immutable_and_cannot_deploy() -> None:
    sql = Path(
        "migrations/postgres/044_low_volatility_forward_evaluation.sql"
    ).read_text(encoding="utf-8")

    assert (
        "CREATE TABLE IF NOT EXISTS "
        "low_volatility_forward_evaluation_runs"
    ) in sql
    assert "session_count = 126" in sql
    assert "block_count = 6" in sql
    assert "paper_deployment_allowed boolean NOT NULL DEFAULT false" in sql
    assert "NOT paper_deployment_allowed" in sql
    assert "live_trading_locked boolean NOT NULL DEFAULT true" in sql
    assert "autoquant_reject_immutable_change()" in sql
    assert "VALUES ('postgres', 44)" in sql

    dataset_sql = Path(
        "migrations/postgres/"
        "045_low_volatility_forward_evaluation_dataset.sql"
    ).read_text(encoding="utf-8")
    assert "evaluation_dataset_manifest_hash" in dataset_sql
    assert "ALTER COLUMN evaluation_dataset_manifest_hash SET NOT NULL" in dataset_sql
    assert "VALUES ('postgres', 45)" in dataset_sql
