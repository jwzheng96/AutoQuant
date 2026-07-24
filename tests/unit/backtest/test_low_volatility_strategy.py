from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from autoquant.backtest.dynamic_panel import (
    DynamicMarketPanel,
    DynamicMarketSession,
    InstrumentMarketHistory,
)
from autoquant.backtest.low_volatility_portfolio import (
    LowVolatilityResearchSpec,
)
from autoquant.backtest.low_volatility_strategy import (
    LowVolatilityExecutableSession,
    LowVolatilityObservation,
    LowVolatilityOrderPolicy,
    _realized_volatility,
    compile_low_volatility_executable_panel,
)
from autoquant.backtest.models import MarketState, OrderSide
from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.data.daily_models import DailyBarRevision


def _spec() -> LowVolatilityResearchSpec:
    return LowVolatilityResearchSpec(
        predecessor_result_hash="a" * 64,
        dataset_manifest_hash="b" * 64,
        plan_hash="c" * 64,
        policy_hash="d" * 64,
        start_date=date(2020, 1, 1),
        end_date=date(2026, 7, 22),
    )


def _market(
    instrument: str,
    session_date: date,
    *,
    price: Decimal = Decimal("10"),
) -> MarketState:
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
            source_revision="low-volatility-test",
            availability_policy="test-v1",
            evidence_hash="e" * 64,
            open_price=str(price),
            high_price=str(price + 1),
            low_price=str(price - 1),
            close_price=str(price),
            pre_close=str(price),
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
    session_date: date,
    instruments: tuple[str, ...],
    *,
    observations: tuple[LowVolatilityObservation, ...] = (),
) -> LowVolatilityExecutableSession:
    return LowVolatilityExecutableSession(
        session_date=session_date,
        snapshot_hash="f" * 64,
        active_members=instruments,
        observations=observations,
        markets=tuple(_market(instrument, session_date) for instrument in instruments),
    )


def test_realized_volatility_is_zero_for_constant_close() -> None:
    closes = tuple(Decimal("10") for _ in range(253))

    assert _realized_volatility(closes) == 0


def test_low_volatility_panel_uses_only_previous_session_window() -> None:
    spec = _spec()
    instrument = "000001.XSHE"
    start = date(2025, 1, 1)
    markets = tuple(
        _market(
            instrument,
            start + timedelta(days=index),
            price=Decimal("10") + Decimal(index) / Decimal("100"),
        )
        for index in range(254)
    )
    panel = DynamicMarketPanel(
        dataset_manifest_hash=spec.dataset_manifest_hash,
        plan_hash=spec.plan_hash,
        spec_hash=spec.spec_hash,
        as_of=datetime(2026, 7, 23, tzinfo=UTC),
        sessions=tuple(
            DynamicMarketSession(
                session_date=value.bar.session_date,
                snapshot_hash="f" * 64,
                active_members=(instrument,),
                markets=(value,),
            )
            for value in markets
        ),
        histories=(
            InstrumentMarketHistory(
                instrument=instrument,
                markets=markets,
                list_date=start,
                delist_date=None,
            ),
        ),
    )

    compiled = compile_low_volatility_executable_panel(
        spec=spec,
        markets=panel,
    )

    assert compiled.sessions[252].observations == ()
    observation = compiled.sessions[253].observations[0]
    assert observation.signal_date == markets[252].bar.session_date
    assert observation.execution_date == markets[253].bar.session_date
    assert observation.volatility > 0
    assert len(observation.window_hash) == 64


def test_low_volatility_policy_selects_lowest_twenty() -> None:
    instruments = tuple(f"{index:06d}.XSHE" for index in range(1, 61))
    start = date(2025, 1, 1)
    padding = tuple(
        _session(
            start + timedelta(days=index),
            (instruments[0],),
        )
        for index in range(253)
    )
    execution_date = start + timedelta(days=253)
    observations = tuple(
        LowVolatilityObservation(
            instrument=instrument,
            signal_date=execution_date - timedelta(days=1),
            execution_date=execution_date,
            volatility=Decimal(index) / Decimal("1000"),
            window_hash=f"{index + 1:064x}",
        )
        for index, instrument in enumerate(instruments)
    )
    first = _session(
        execution_date,
        instruments,
        observations=observations,
    )
    second = _session(
        execution_date + timedelta(days=1),
        instruments,
    )
    sessions = (*padding, first, second)
    policy = LowVolatilityOrderPolicy(
        sessions=sessions,
        start_index=253,
        trade_session_count=2,
        spec=_spec(),
    )

    orders = policy(0, first.markets, None)

    assert len(orders) == 20
    assert tuple(value.instrument for value in orders) == instruments[:20]
    assert all(value.side is OrderSide.BUY for value in orders)


def test_low_volatility_policy_stays_in_cash_below_gate() -> None:
    instruments = tuple(f"{index:06d}.XSHE" for index in range(1, 60))
    start = date(2025, 1, 1)
    padding = tuple(
        _session(
            start + timedelta(days=index),
            (instruments[0],),
        )
        for index in range(253)
    )
    execution_date = start + timedelta(days=253)
    observations = tuple(
        LowVolatilityObservation(
            instrument=instrument,
            signal_date=execution_date - timedelta(days=1),
            execution_date=execution_date,
            volatility=Decimal(index) / Decimal("1000"),
            window_hash=f"{index + 1:064x}",
        )
        for index, instrument in enumerate(instruments)
    )
    first = _session(
        execution_date,
        instruments,
        observations=observations,
    )
    second = _session(
        execution_date + timedelta(days=1),
        instruments,
    )
    policy = LowVolatilityOrderPolicy(
        sessions=(*padding, first, second),
        start_index=253,
        trade_session_count=2,
        spec=_spec(),
    )

    assert policy(0, first.markets, None) == ()
