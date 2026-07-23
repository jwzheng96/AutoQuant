from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from autoquant.backtest.dynamic_panel import DynamicMarketSession
from autoquant.backtest.dynamic_portfolio import (
    DynamicPortfolioResearchSpec,
)
from autoquant.backtest.dynamic_strategy import (
    DynamicMomentumOrderPolicy,
)
from autoquant.backtest.models import MarketState, OrderSide
from autoquant.backtest.portfolio_validation import (
    CrossSectionalMomentumParameters,
)
from autoquant.backtest.rules import (
    AshareRuleBook,
    SecurityStatus,
)
from autoquant.data.daily_models import DailyBarRevision

INSTRUMENTS = ("000001.XSHE", "600000.XSHG", "600519.XSHG")
PARAMETERS = CrossSectionalMomentumParameters(20, 5, 2)


def _market(
    instrument: str,
    index: int,
    *,
    close_shift: Decimal = Decimal("0"),
    volume: int = 1_000_000,
) -> MarketState:
    session_date = date(2026, 1, 1) + timedelta(days=index)
    rank = Decimal(INSTRUMENTS.index(instrument) + 1)
    previous = Decimal("10") + rank * Decimal(index - 1) / Decimal("100")
    close = (
        Decimal("10")
        + rank * Decimal(index) / Decimal("100")
        + close_shift
    )
    event = datetime.combine(
        session_date,
        datetime.min.time(),
        tzinfo=UTC,
    ) + timedelta(hours=7)
    return MarketState(
        bar=DailyBarRevision.from_values(
            source="tushare",
            instrument=instrument,
            session_date=session_date,
            event_time=event,
            available_at=event + timedelta(hours=1),
            ingested_at=event + timedelta(hours=2),
            source_revision="dynamic-strategy-test",
            availability_policy="test-v1",
            evidence_hash="a" * 64,
            open_price=str(previous),
            high_price=str(max(previous, close) + Decimal("1")),
            low_price=str(min(previous, close) - Decimal("1")),
            close_price=str(close),
            pre_close=str(previous),
            volume=volume,
            turnover="10000000",
        ),
        rules=AshareRuleBook().resolve(
            instrument,
            session_date,
            SecurityStatus(
                risk_warning=False,
                listing_session_number=1000,
            ),
        ),
        suspended=False,
    )


def _sessions(
    *,
    current_shift: Decimal = Decimal("0"),
    omit_current: str | None = None,
) -> tuple[DynamicMarketSession, ...]:
    values = []
    for index in range(23):
        markets = tuple(
            _market(
                instrument,
                index,
                close_shift=(
                    current_shift
                    if index == 21 and instrument == INSTRUMENTS[0]
                    else Decimal("0")
                ),
            )
            for instrument in INSTRUMENTS
            if not (index == 21 and instrument == omit_current)
        )
        values.append(
            DynamicMarketSession(
                session_date=date(2026, 1, 1) + timedelta(days=index),
                snapshot_hash=f"{index + 1:064x}",
                active_members=INSTRUMENTS,
                markets=markets,
            )
        )
    return tuple(values)


def _spec() -> DynamicPortfolioResearchSpec:
    return DynamicPortfolioResearchSpec(
        dataset_manifest_hash="a" * 64,
        plan_hash="b" * 64,
        policy_hash="c" * 64,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 12, 31),
        gross_allocation=Decimal("0.10"),
        maximum_position_weight=Decimal("0.05"),
        minimum_member_history_sessions=20,
        candidates=(PARAMETERS,),
    )


def _orders(
    sessions: tuple[DynamicMarketSession, ...],
) -> tuple[tuple[str, OrderSide], ...]:
    policy = DynamicMomentumOrderPolicy(
        sessions=sessions,
        start_index=21,
        trade_session_count=2,
        parameters=PARAMETERS,
        spec=_spec(),
    )
    return tuple(
        (value.instrument, value.side)
        for value in policy(0, sessions[21].markets, None)
    )


def test_dynamic_signal_uses_lagged_closes_and_point_in_time_members() -> None:
    original = _orders(_sessions())
    current_close_changed = _orders(
        _sessions(current_shift=Decimal("1000"))
    )

    assert original == current_close_changed
    assert original == (
        ("600000.XSHG", OrderSide.BUY),
        ("600519.XSHG", OrderSide.BUY),
    )


def test_dynamic_signal_does_not_fabricate_missing_execution_market() -> None:
    orders = _orders(_sessions(omit_current="600519.XSHG"))

    assert orders == (("600000.XSHG", OrderSide.BUY),)
