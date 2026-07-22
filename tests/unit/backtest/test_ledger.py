from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from autoquant.backtest.ledger import ExecutionModel, PortfolioLedger
from autoquant.backtest.models import (
    ExecutionState,
    MarketState,
    OrderIntent,
    OrderSide,
    RejectionCode,
)
from autoquant.backtest.rules import AshareRuleBook, FeeSchedule, SecurityStatus
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
    volume: int = 100_000,
    suspended: bool = False,
) -> MarketState:
    event = datetime.combine(session_date, datetime.min.time(), tzinfo=UTC) + timedelta(
        hours=7
    )
    bar = DailyBarRevision.from_values(
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
        volume=volume,
        turnover="1000000",
    )
    rules = AshareRuleBook().resolve(
        INSTRUMENT,
        session_date,
        SecurityStatus(risk_warning=False, listing_session_number=1_000),
    )
    return MarketState(bar=bar, rules=rules, suspended=suspended)


def order(
    order_id: str,
    side: OrderSide,
    session_date: date = DAY,
    *,
    quantity: int = 100,
    submitted_at: datetime | None = None,
) -> OrderIntent:
    return OrderIntent(
        client_order_id=order_id,
        instrument=INSTRUMENT,
        side=side,
        quantity=quantity,
        session_date=session_date,
        submitted_at=submitted_at
        or datetime.combine(
            session_date - timedelta(days=1), datetime.min.time(), tzinfo=UTC
        ),
    )


def ledger(initial_cash: str = "10000") -> PortfolioLedger:
    return PortfolioLedger(
        initial_cash=Decimal(initial_cash),
        fees=FeeSchedule(),
        execution=ExecutionModel(),
    )


def test_buy_is_not_sellable_until_next_session_and_cash_is_reconciled() -> None:
    account = ledger()
    account.start_session(DAY)

    buy = account.execute(order("buy-1", OrderSide.BUY), market())
    same_day_sell = account.execute(order("sell-too-soon", OrderSide.SELL), market())

    assert buy.state is ExecutionState.FILLED
    assert buy.fill_price == Decimal("10.01")
    assert buy.fees.total == Decimal("5.01")
    assert account.cash == Decimal("8993.99")
    assert same_day_sell.rejection_code is RejectionCode.NOT_SELLABLE

    following = DAY + timedelta(days=1)
    account.start_session(following)
    sell = account.execute(
        order("sell-1", OrderSide.SELL, following),
        market(
            following,
            open_price="11",
            high_price="11.2",
            low_price="10.8",
            close_price="11.1",
            pre_close="10.2",
        ),
    )

    assert sell.state is ExecutionState.FILLED
    assert sell.fill_price == Decimal("10.99")
    assert sell.fees.total == Decimal("5.56")
    assert account.cash == Decimal("10087.43")
    assert account.snapshot(
        (
            market(
                following,
                open_price="11",
                high_price="11.2",
                low_price="10.8",
                close_price="11.1",
                pre_close="10.2",
            ),
        )
    ).positions == ()


def test_buy_quantity_cash_and_liquidity_fail_closed() -> None:
    account = ledger(initial_cash="1000")
    account.start_session(DAY)

    invalid_lot = account.execute(
        order("lot", OrderSide.BUY, quantity=150), market()
    )
    insufficient = account.execute(
        order("cash", OrderSide.BUY, quantity=100), market()
    )
    illiquid = account.execute(
        order("liquidity", OrderSide.BUY, quantity=100), market(volume=500)
    )

    assert invalid_lot.rejection_code is RejectionCode.INVALID_BUY_QUANTITY
    assert insufficient.rejection_code is RejectionCode.CASH_INSUFFICIENT
    assert illiquid.rejection_code is RejectionCode.LIQUIDITY_LIMIT
    assert account.cash == Decimal("1000.00")


def test_suspension_and_locked_limits_are_rejected() -> None:
    suspended_account = ledger()
    suspended_account.start_session(DAY)
    suspended = suspended_account.execute(
        order("suspended", OrderSide.BUY), market(suspended=True)
    )

    locked_account = ledger()
    locked_account.start_session(DAY)
    locked = locked_account.execute(
        order("locked", OrderSide.BUY),
        market(
            open_price="11",
            high_price="11",
            low_price="11",
            close_price="11",
            pre_close="10",
        ),
    )

    assert suspended.rejection_code is RejectionCode.SUSPENDED
    assert locked.rejection_code is RejectionCode.LIMIT_UP_LOCKED


def test_order_replay_is_idempotent_and_hash_chain_is_linked() -> None:
    account = ledger()
    account.start_session(DAY)
    intent = order("stable-order", OrderSide.BUY)

    first = account.execute(intent, market())
    repeated = account.execute(intent, market())

    assert repeated == first
    assert len(account.events) == 1
    assert account.events[0].previous_hash == "0" * 64
    assert dict(account.events[0].payload)["instrument"] == INSTRUMENT
    assert account.ledger_hash == first.ledger_hash


def test_order_submitted_at_or_after_open_cannot_use_daily_open_fill() -> None:
    account = ledger()
    account.start_session(DAY)
    at_open = datetime(2026, 7, 22, 1, 30, tzinfo=UTC)

    report = account.execute(
        order("lookahead", OrderSide.BUY, submitted_at=at_open), market()
    )

    assert report.rejection_code is RejectionCode.OUTSIDE_SESSION
