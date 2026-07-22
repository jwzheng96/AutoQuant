from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from autoquant.backtest.engine import BacktestEngine
from autoquant.backtest.models import (
    AccountSnapshot,
    BacktestSession,
    MarketState,
    OrderIntent,
    OrderSide,
    backtest_artifact_hash,
)
from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.data.daily_models import DailyBarRevision

DAY = date(2026, 7, 22)
INSTRUMENT = "000001.XSHE"


def market(
    session_date: date = DAY,
    *,
    open_price: str = "10",
    high_price: str = "10.5",
    low_price: str = "9.8",
    close_price: str = "10.2",
    pre_close: str = "10",
) -> MarketState:
    event = datetime.combine(session_date, datetime.min.time(), tzinfo=UTC) + timedelta(
        hours=7
    )
    return MarketState(
        bar=DailyBarRevision.from_values(
            source="tushare",
            instrument=INSTRUMENT,
            session_date=session_date,
            event_time=event,
            available_at=event + timedelta(hours=1),
            ingested_at=event + timedelta(hours=2),
            source_revision="daily-test",
            availability_policy="test-v1",
            evidence_hash="f" * 64,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            pre_close=pre_close,
            volume=100_000,
            turnover="1000000",
        ),
        rules=AshareRuleBook().resolve(
            INSTRUMENT,
            session_date,
            SecurityStatus(risk_warning=False, listing_session_number=1_000),
        ),
        suspended=False,
    )


def order(order_id: str, side: OrderSide, session_date: date = DAY) -> OrderIntent:
    return OrderIntent(
        client_order_id=order_id,
        instrument=INSTRUMENT,
        side=side,
        quantity=100,
        session_date=session_date,
        submitted_at=datetime.combine(
            session_date - timedelta(days=1), datetime.min.time(), tzinfo=UTC
        ),
    )


def sessions() -> tuple[BacktestSession, ...]:
    following = DAY + timedelta(days=1)
    return (
        BacktestSession(
            session_date=DAY,
            markets=(market(),),
            orders=(order("buy", OrderSide.BUY),),
        ),
        BacktestSession(
            session_date=following,
            markets=(
                market(
                    following,
                    open_price="11",
                    high_price="11.2",
                    low_price="10.8",
                    close_price="11.1",
                    pre_close="10.2",
                ),
            ),
            orders=(order("sell", OrderSide.SELL, following),),
        ),
    )


def test_backtest_is_deterministic_and_records_versions() -> None:
    engine = BacktestEngine()
    arguments = {
        "strategy_id": "scheduled-orders-v1",
        "manifest_hash": "a" * 64,
        "as_of": datetime(2026, 7, 24, tzinfo=UTC),
        "initial_cash": Decimal("10000"),
        "sessions": sessions(),
    }

    first = engine.run(**arguments)
    second = engine.run(**arguments)

    assert first == second
    assert first.ending_equity == Decimal("10087.43")
    assert first.total_return == Decimal("0.008743")
    assert first.max_drawdown == Decimal("0")
    assert first.total_fees == Decimal("10.57")
    assert first.turnover == Decimal("0.2100")
    assert first.result_hash == second.result_hash
    assert first.execution_version == "daily-open-conservative-v1"
    assert "sse-szse-cash-equity-2026-07-06" in first.rule_versions


def test_backtest_rejects_data_not_visible_at_as_of() -> None:
    with pytest.raises(ValueError, match="not visible"):
        BacktestEngine().run(
            strategy_id="scheduled-orders-v1",
            manifest_hash="a" * 64,
            as_of=datetime(2026, 7, 22, 7, 30, tzinfo=UTC),
            initial_cash=Decimal("10000"),
            sessions=sessions(),
        )


def test_artifact_hash_commits_daily_snapshots_beyond_semantic_result_hash() -> None:
    result = BacktestEngine().run(
        strategy_id="scheduled-orders-v1",
        manifest_hash="a" * 64,
        as_of=datetime(2026, 7, 24, tzinfo=UTC),
        initial_cash=Decimal("10000"),
        sessions=sessions(),
    )
    original = result.snapshots[0]
    changed = AccountSnapshot(
        session_date=original.session_date,
        cash=original.cash,
        market_value=original.market_value,
        equity=original.equity + Decimal("1"),
        positions=original.positions,
        ledger_hash=original.ledger_hash,
    )
    altered = type(result)(
        strategy_id=result.strategy_id,
        manifest_hash=result.manifest_hash,
        as_of=result.as_of,
        initial_cash=result.initial_cash,
        ending_equity=result.ending_equity,
        total_return=result.total_return,
        max_drawdown=result.max_drawdown,
        turnover=result.turnover,
        total_fees=result.total_fees,
        reports=result.reports,
        snapshots=(changed, *result.snapshots[1:]),
        events=result.events,
        rule_versions=result.rule_versions,
        fee_version=result.fee_version,
        execution_version=result.execution_version,
        ledger_hash=result.ledger_hash,
    )

    assert altered.result_hash == result.result_hash
    assert backtest_artifact_hash(altered) != backtest_artifact_hash(result)
