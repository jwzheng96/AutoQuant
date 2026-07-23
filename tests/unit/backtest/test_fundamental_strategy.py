from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from autoquant.backtest.fundamental_panel import (
    FundamentalFeatureObservation,
    FundamentalFeatureSession,
)
from autoquant.backtest.fundamental_portfolio import (
    FundamentalPortfolioResearchSpec,
)
from autoquant.backtest.fundamental_strategy import (
    FundamentalExecutableSession,
    FundamentalQualityValueOrderPolicy,
    fundamental_ranking_payload,
)
from autoquant.backtest.models import MarketState, OrderSide
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


def _observation(
    instrument: str,
    execution_date: date,
    value: int,
) -> FundamentalFeatureObservation:
    return FundamentalFeatureObservation(
        instrument=instrument,
        signal_date=execution_date - timedelta(days=1),
        execution_date=execution_date,
        report_period=date(2025, 12, 31),
        earnings_yield=Decimal(value + 1),
        book_to_price=Decimal(value + 1),
        roe_diluted_percent=Decimal(value),
        roa_percent=Decimal(value),
        operating_cashflow_to_revenue_percent=Decimal(value),
        valuation_hash=f"{value + 1:064x}",
        indicator_hash=f"{value + 101:064x}",
    )


def _market(instrument: str, session_date: date) -> MarketState:
    event_time = datetime.combine(
        session_date,
        datetime.min.time(),
        tzinfo=UTC,
    ) + timedelta(hours=7)
    return MarketState(
        bar=DailyBarRevision.from_values(
            source="tushare",
            instrument=instrument,
            session_date=session_date,
            event_time=event_time,
            available_at=event_time + timedelta(hours=1),
            ingested_at=event_time + timedelta(hours=2),
            source_revision="fundamental-strategy-test",
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
            instrument,
            session_date,
            SecurityStatus(
                risk_warning=False,
                listing_session_number=1000,
            ),
        ),
        suspended=False,
    )


def _session(
    instruments: tuple[str, ...],
    execution_date: date,
) -> FundamentalExecutableSession:
    features = FundamentalFeatureSession(
        execution_date=execution_date,
        signal_date=execution_date - timedelta(days=1),
        snapshot_hash="f" * 64,
        active_member_count=len(instruments),
        observations=tuple(
            _observation(instrument, execution_date, index)
            for index, instrument in enumerate(instruments)
        ),
    )
    return FundamentalExecutableSession(
        session_date=execution_date,
        snapshot_hash="f" * 64,
        active_members=instruments,
        features=features,
        markets=tuple(_market(instrument, execution_date) for instrument in instruments),
    )


def test_fundamental_ranking_is_fixed_and_deterministic() -> None:
    execution_date = date(2026, 7, 21)
    observations = (
        _observation("000003.XSHE", execution_date, 3),
        _observation("000001.XSHE", execution_date, 1),
        _observation("000002.XSHE", execution_date, 2),
    )

    payload = fundamental_ranking_payload(observations)

    assert payload["ranked_instruments"] == [
        "000003.XSHE",
        "000002.XSHE",
        "000001.XSHE",
    ]
    assert len(str(payload["score_evidence_hash"])) == 64


def test_fundamental_policy_buys_only_frozen_top_twenty() -> None:
    instruments = tuple(f"{index:06d}.XSHE" for index in range(1, 61))
    first = _session(instruments, date(2026, 7, 21))
    second = _session(instruments, date(2026, 7, 22))
    policy = FundamentalQualityValueOrderPolicy(
        sessions=(first, second),
        start_index=0,
        trade_session_count=2,
        spec=_spec(),
    )

    orders = policy(0, first.markets, None)

    assert len(orders) == 20
    assert tuple(value.instrument for value in orders) == tuple(sorted(instruments[-20:]))
    assert all(value.side is OrderSide.BUY for value in orders)
    assert all(value.quantity * Decimal("10") <= _spec().maximum_order_notional for value in orders)


def test_fundamental_policy_stays_in_cash_below_eligibility_gate() -> None:
    instruments = tuple(f"{index:06d}.XSHE" for index in range(1, 60))
    first = _session(instruments, date(2026, 7, 21))
    second = _session(instruments, date(2026, 7, 22))
    policy = FundamentalQualityValueOrderPolicy(
        sessions=(first, second),
        start_index=0,
        trade_session_count=2,
        spec=_spec(),
    )

    assert policy(0, first.markets, None) == ()
