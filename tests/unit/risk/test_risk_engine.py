"""Deterministic tests for the shared pre-trade risk engine."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from autoquant.backtest.models import OrderSide
from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.risk.engine import PreTradeRiskEngine
from autoquant.risk.models import (
    ExecutionMode,
    MarketQuote,
    ProposedOrder,
    RiskAccountState,
    RiskCode,
    RiskDecisionState,
    RiskEvaluationInput,
    RiskPolicy,
    RiskPosition,
)

NOW = datetime(2026, 7, 22, 2, 0, tzinfo=UTC)
INSTRUMENT = "000001.XSHE"


def _policy(**updates: object) -> RiskPolicy:
    values: dict[str, object] = {"allowed_instruments": (INSTRUMENT,)}
    values.update(updates)
    return RiskPolicy(**values)  # type: ignore[arg-type]


def _account(
    *,
    cash: str = "1000000",
    equity: str = "1000000",
    day_start_equity: str = "1000000",
    peak_equity: str = "1000000",
    positions: tuple[RiskPosition, ...] = (),
    reconciled: bool = True,
    kill_switch: bool = False,
    as_of: datetime = NOW,
    seen: tuple[str, ...] = (),
) -> RiskAccountState:
    gross = sum((position.market_value for position in positions), Decimal("0"))
    return RiskAccountState(
        account_id="paper-main",
        as_of=as_of,
        cash=Decimal(cash),
        equity=Decimal(equity),
        day_start_equity=Decimal(day_start_equity),
        peak_equity=Decimal(peak_equity),
        gross_exposure=gross,
        daily_turnover=Decimal("0"),
        open_order_count=0,
        reconciled=reconciled,
        kill_switch=kill_switch,
        positions=positions,
        seen_client_order_ids=seen,
    )


def _quote(*, as_of: datetime = NOW, market_open: bool = True) -> MarketQuote:
    return MarketQuote(
        instrument=INSTRUMENT,
        as_of=as_of,
        last_price=Decimal("10"),
        bid_price=Decimal("9.99"),
        ask_price=Decimal("10.01"),
        market_open=market_open,
    )


def _order(
    *,
    side: OrderSide = OrderSide.BUY,
    quantity: int = 100,
    order_id: str = "paper-order-0001",
    submitted_at: datetime = NOW,
    limit_price: Decimal | None = None,
) -> ProposedOrder:
    return ProposedOrder(
        client_order_id=order_id,
        instrument=INSTRUMENT,
        side=side,
        quantity=quantity,
        submitted_at=submitted_at,
        limit_price=limit_price,
    )


def _evaluate(
    *,
    mode: ExecutionMode = ExecutionMode.PAPER,
    policy: RiskPolicy | None = None,
    account: RiskAccountState | None = None,
    order: ProposedOrder | None = None,
    quote: MarketQuote | None = None,
):  # type: ignore[no-untyped-def]
    rules = AshareRuleBook().resolve(
        INSTRUMENT,
        date(2026, 7, 22),
        SecurityStatus(risk_warning=False, listing_session_number=1000),
    )
    return PreTradeRiskEngine().evaluate(
        RiskEvaluationInput(
            mode=mode,
            policy=policy or _policy(),
            account=account or _account(),
            order=order or _order(),
            quote=quote or _quote(),
            rules=rules,
            now=NOW,
        )
    )


def test_valid_paper_order_is_accepted_and_decision_is_deterministic() -> None:
    first = _evaluate()
    second = _evaluate()

    assert first.state is RiskDecisionState.ACCEPTED
    assert first.violations == ()
    assert first.order_notional == Decimal("1001.00")
    assert first.decision_hash == second.decision_hash


def test_live_mode_is_hard_locked_even_when_every_other_control_passes() -> None:
    decision = _evaluate(mode=ExecutionMode.LIVE)

    assert decision.state is RiskDecisionState.REJECTED
    assert decision.violations == (RiskCode.LIVE_MODE_LOCKED,)


def test_kill_switch_reconciliation_and_freshness_fail_closed_together() -> None:
    stale = NOW - timedelta(seconds=10)
    decision = _evaluate(
        account=_account(
            reconciled=False,
            kill_switch=True,
            as_of=stale,
        ),
        quote=_quote(as_of=stale, market_open=False),
    )

    assert decision.violations == (
        RiskCode.KILL_SWITCH_ACTIVE,
        RiskCode.RECONCILIATION_UNHEALTHY,
        RiskCode.STALE_ACCOUNT_STATE,
        RiskCode.STALE_MARKET_DATA,
        RiskCode.MARKET_CLOSED,
    )


def test_duplicate_and_future_orders_are_rejected_before_gateway_submission() -> None:
    order = _order(submitted_at=NOW + timedelta(seconds=1))
    decision = _evaluate(
        account=_account(seen=(order.client_order_id,)),
        order=order,
    )

    assert RiskCode.DUPLICATE_ORDER in decision.violations
    assert RiskCode.FUTURE_ORDER in decision.violations


def test_notional_concentration_exposure_and_cash_limits_are_independent() -> None:
    decision = _evaluate(order=_order(quantity=100_000))

    assert RiskCode.ORDER_NOTIONAL_LIMIT in decision.violations
    assert RiskCode.INSUFFICIENT_CASH in decision.violations
    assert RiskCode.POSITION_WEIGHT_LIMIT in decision.violations
    assert RiskCode.GROSS_EXPOSURE_LIMIT in decision.violations
    assert RiskCode.DAILY_TURNOVER_LIMIT in decision.violations


def test_sell_requires_position_and_t_plus_one_sellability() -> None:
    no_position = _evaluate(order=_order(side=OrderSide.SELL))
    position = RiskPosition(
        instrument=INSTRUMENT,
        total_quantity=1000,
        sellable_quantity=0,
        market_value=Decimal("10000"),
    )
    unavailable = _evaluate(
        account=_account(
            cash="990000",
            positions=(position,),
        ),
        order=_order(side=OrderSide.SELL),
    )

    assert RiskCode.NO_POSITION in no_position.violations
    assert RiskCode.NOT_SELLABLE in unavailable.violations


def test_daily_loss_drawdown_and_price_fat_finger_are_hard_limits() -> None:
    decision = _evaluate(
        account=_account(
            cash="900000",
            equity="900000",
            day_start_equity="1000000",
            peak_equity="1100000",
        ),
        order=_order(limit_price=Decimal("12")),
    )

    assert RiskCode.DAILY_LOSS_LIMIT in decision.violations
    assert RiskCode.DRAWDOWN_LIMIT in decision.violations
    assert RiskCode.PRICE_DEVIATION_LIMIT in decision.violations
