from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

import autoquant.backtest.fundamental_validation as validation
from autoquant.backtest.dynamic_portfolio import (
    DYNAMIC_PORTFOLIO_BENCHMARK_VERSION,
)
from autoquant.backtest.fundamental_panel import (
    FundamentalFeatureSession,
)
from autoquant.backtest.fundamental_portfolio import (
    FundamentalPortfolioResearchSpec,
)
from autoquant.backtest.fundamental_strategy import (
    FundamentalExecutablePanel,
    FundamentalExecutableSession,
)
from autoquant.backtest.models import (
    AccountSnapshot,
    BacktestResult,
    MarketState,
)
from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.data.daily_models import DailyBarRevision


def _spec() -> FundamentalPortfolioResearchSpec:
    return FundamentalPortfolioResearchSpec(
        predecessor_result_hash="a" * 64,
        daily_dataset_manifest_hash="b" * 64,
        plan_hash="c" * 64,
        universe_policy_hash="d" * 64,
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
            source_revision="fundamental-validation-test",
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


def _panel() -> FundamentalExecutablePanel:
    sessions = []
    start = date(2024, 1, 2)
    for index in range(572):
        session_date = start + timedelta(days=index)
        features = FundamentalFeatureSession(
            execution_date=session_date,
            signal_date=session_date - timedelta(days=1),
            snapshot_hash="f" * 64,
            active_member_count=1,
            observations=(),
        )
        sessions.append(
            FundamentalExecutableSession(
                session_date=session_date,
                snapshot_hash="f" * 64,
                active_members=("000001.XSHE",),
                features=features,
                markets=(_market(session_date),),
            )
        )
    return FundamentalExecutablePanel(
        spec_hash=_spec().spec_hash,
        feature_panel_hash="1" * 64,
        market_panel_hash="2" * 64,
        as_of=datetime(2026, 7, 23, tzinfo=UTC),
        sessions=tuple(sessions),
    )


def _result(
    *,
    panel: FundamentalExecutablePanel,
    strategy_id: str,
    start_index: int,
    count: int,
    total_return: Decimal,
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


def test_fixed_validator_builds_one_embargoed_fold_without_search(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    panel = _panel()
    spec = _spec()

    def run_strategy(
        *,
        panel: FundamentalExecutablePanel,
        spec: FundamentalPortfolioResearchSpec,
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
        panel: FundamentalExecutablePanel,
        spec: FundamentalPortfolioResearchSpec,
        start_index: int,
        trade_session_count: int,
    ) -> BacktestResult:
        return _result(
            panel=panel,
            strategy_id=DYNAMIC_PORTFOLIO_BENCHMARK_VERSION,
            start_index=start_index,
            count=trade_session_count,
            total_return=Decimal("0.005"),
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

    result = validation.FundamentalWalkForwardValidator().run(
        panel=panel,
        spec=spec,
    )
    evidence = validation.assess_fundamental_validation(
        result,
        spec=spec,
    )

    assert len(result.folds) == 1
    assert (result.folds[0].test_start - result.folds[0].train_end).days == 6
    assert result.compounded_oos_return == Decimal("0.01")
    assert result.excess_oos_return == Decimal("0.005")
    assert evidence.evidence_status == "insufficient"
    assert evidence.gate_failures == (
        "minimum_fold_count",
        "minimum_oos_sessions",
    )
