from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

import autoquant.backtest.low_volatility_validation as validation
from autoquant.backtest.dynamic_portfolio import (
    DYNAMIC_PORTFOLIO_BENCHMARK_VERSION,
)
from autoquant.backtest.low_volatility_portfolio import (
    LowVolatilityResearchSpec,
)
from autoquant.backtest.low_volatility_strategy import (
    LowVolatilityExecutablePanel,
    LowVolatilityExecutableSession,
)
from autoquant.backtest.models import (
    AccountSnapshot,
    BacktestResult,
    MarketState,
    PositionSnapshot,
)
from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.data.daily_models import DailyBarRevision
from autoquant.errors import PersistenceUnavailableError
from autoquant.web.low_volatility_validation_store import (
    LowVolatilityValidationRecord,
    _fold_parameters,
    _record,
    _run_parameters,
)


def _spec() -> LowVolatilityResearchSpec:
    return LowVolatilityResearchSpec(
        predecessor_result_hash="a" * 64,
        dataset_manifest_hash="b" * 64,
        plan_hash="c" * 64,
        policy_hash="d" * 64,
        start_date=date(2020, 1, 1),
        end_date=date(2026, 7, 22),
    )


def _market(session_date: date) -> MarketState:
    event_time = datetime.combine(
        session_date,
        datetime.min.time(),
        tzinfo=UTC,
    ) + timedelta(hours=7)
    return MarketState(
        bar=DailyBarRevision.from_values(
            source="tushare",
            instrument="000001.XSHE",
            session_date=session_date,
            event_time=event_time,
            available_at=event_time + timedelta(hours=1),
            ingested_at=event_time + timedelta(hours=2),
            source_revision="low-volatility-validation-test",
            availability_policy="test-v1",
            evidence_hash="e" * 64,
            open_price="10",
            high_price="11",
            low_price="9",
            close_price="10",
            pre_close="10",
            volume=1_000_000,
            turnover="10000000",
        ),
        rules=AshareRuleBook().resolve(
            "000001.XSHE",
            session_date,
            SecurityStatus(
                risk_warning=False,
                listing_session_number=1000,
            ),
        ),
        suspended=False,
    )


def _panel() -> LowVolatilityExecutablePanel:
    sessions = []
    start = date(2023, 1, 2)
    for index in range(825):
        session_date = start + timedelta(days=index)
        sessions.append(
            LowVolatilityExecutableSession(
                session_date=session_date,
                snapshot_hash="f" * 64,
                active_members=("000001.XSHE",),
                observations=(),
                markets=(_market(session_date),),
            )
        )
    return LowVolatilityExecutablePanel(
        spec_hash=_spec().spec_hash,
        market_panel_hash="1" * 64,
        as_of=datetime(2026, 7, 23, tzinfo=UTC),
        sessions=tuple(sessions),
    )


def _result(
    *,
    panel: LowVolatilityExecutablePanel,
    strategy_id: str,
    start_index: int,
    count: int,
    total_return: Decimal,
    unresolved: bool = False,
) -> BacktestResult:
    snapshots = tuple(
        AccountSnapshot(
            session_date=panel.sessions[index].session_date,
            cash=Decimal("1000000"),
            market_value=Decimal("0"),
            equity=Decimal("1000000"),
            positions=(),
            ledger_hash="0" * 64,
        )
        for index in range(
            start_index,
            start_index + count,
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
        snapshots = (
            *snapshots[:-1],
            replace(
                snapshots[-1],
                cash=Decimal("999000"),
                market_value=Decimal("1000"),
                positions=(position,),
            ),
        )
    return BacktestResult(
        strategy_id=strategy_id,
        manifest_hash=panel.panel_hash,
        as_of=panel.as_of,
        initial_cash=Decimal("1000000"),
        ending_equity=Decimal("1000000") * (Decimal("1") + total_return),
        total_return=total_return,
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


def test_fixed_validator_reserves_warmup_and_does_not_gate_benchmark_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    panel = _panel()
    spec = _spec()

    def run_strategy(
        *,
        panel: LowVolatilityExecutablePanel,
        spec: LowVolatilityResearchSpec,
        start_index: int,
        trade_session_count: int,
    ) -> BacktestResult:
        return _result(
            panel=panel,
            strategy_id=spec.strategy_id,
            start_index=start_index,
            count=trade_session_count,
            total_return=Decimal("0.01"),
        )

    def run_benchmark(
        *,
        panel: LowVolatilityExecutablePanel,
        spec: LowVolatilityResearchSpec,
        start_index: int,
        trade_session_count: int,
    ) -> BacktestResult:
        return _result(
            panel=panel,
            strategy_id=DYNAMIC_PORTFOLIO_BENCHMARK_VERSION,
            start_index=start_index,
            count=trade_session_count,
            total_return=Decimal("0.005"),
            unresolved=True,
        )

    monkeypatch.setattr(
        validation,
        "_run_strategy",
        run_strategy,
    )
    monkeypatch.setattr(
        validation,
        "_run_benchmark",
        run_benchmark,
    )

    result = validation.LowVolatilityWalkForwardValidator().run(
        panel=panel,
        spec=spec,
    )
    evidence = validation.assess_low_volatility_validation(
        result,
        spec=spec,
    )

    assert len(result.folds) == 1
    assert result.folds[0].train_start == panel.sessions[253].session_date
    assert (result.folds[0].test_start - result.folds[0].train_end).days == 6
    assert result.excess_oos_return == Decimal("0.005")
    assert result.strategy_unresolved_position_count == 0
    assert result.benchmark_unresolved_position_count == 1
    assert evidence.evidence_status == "insufficient"
    assert evidence.gate_failures == (
        "minimum_fold_count",
        "minimum_oos_sessions",
    )


def test_low_volatility_store_round_trips_and_detects_tampering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    panel = _panel()
    spec = _spec()

    def run_strategy(
        *,
        panel: LowVolatilityExecutablePanel,
        spec: LowVolatilityResearchSpec,
        start_index: int,
        trade_session_count: int,
    ) -> BacktestResult:
        return _result(
            panel=panel,
            strategy_id=spec.strategy_id,
            start_index=start_index,
            count=trade_session_count,
            total_return=Decimal("0.01"),
        )

    def run_benchmark(
        *,
        panel: LowVolatilityExecutablePanel,
        spec: LowVolatilityResearchSpec,
        start_index: int,
        trade_session_count: int,
    ) -> BacktestResult:
        return _result(
            panel=panel,
            strategy_id=DYNAMIC_PORTFOLIO_BENCHMARK_VERSION,
            start_index=start_index,
            count=trade_session_count,
            total_return=Decimal("0.005"),
            unresolved=True,
        )

    monkeypatch.setattr(
        validation,
        "_run_strategy",
        run_strategy,
    )
    monkeypatch.setattr(
        validation,
        "_run_benchmark",
        run_benchmark,
    )
    result = validation.LowVolatilityWalkForwardValidator().run(
        panel=panel,
        spec=spec,
    )
    evidence = validation.assess_low_volatility_validation(
        result,
        spec=spec,
    )
    expected = LowVolatilityValidationRecord(
        result=result,
        evidence=evidence,
        requested_by="operator",
        completed_at=panel.as_of,
    )
    run = _run_parameters(expected)
    run["live_trading_locked"] = True
    folds = tuple(_fold_parameters(result.result_hash, fold) for fold in result.folds)

    assert (
        _record(
            cast(Any, run),
            cast(Any, folds),
        )
        == expected
    )

    folds[0]["fold_hash"] = "0" * 64
    with pytest.raises(
        PersistenceUnavailableError,
        match="integrity",
    ):
        _record(cast(Any, run), cast(Any, folds))


def test_low_volatility_validation_migration_is_immutable() -> None:
    sql = Path("migrations/postgres/033_low_volatility_validation.sql").read_text(encoding="utf-8")

    assert ("CREATE TABLE IF NOT EXISTS low_volatility_validation_runs") in sql
    assert ("CREATE TABLE IF NOT EXISTS low_volatility_validation_folds") in sql
    assert sql.count("autoquant_reject_immutable_change()") == 2
    assert "live_trading_locked" in sql
    assert "VALUES ('postgres', 33)" in sql
