from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from autoquant.backtest.models import OrderSide
from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.execution.models import (
    ApprovedPaperOrder,
    BrokerOrderUpdate,
    PaperOrderProjection,
    PaperOrderState,
)
from autoquant.execution.reconciliation import (
    AccountPosition,
    AccountReconciler,
    ExecutionAccountSnapshot,
    ReconciliationCode,
)
from autoquant.execution.state_machine import PaperOrderStateMachine
from autoquant.risk.engine import PreTradeRiskEngine
from autoquant.risk.models import (
    ExecutionMode,
    MarketQuote,
    ProposedOrder,
    RiskAccountState,
    RiskEvaluationInput,
    RiskPolicy,
)

NOW = datetime(2026, 7, 22, 2, tzinfo=UTC)
INSTRUMENT = "000001.XSHE"


def _approved_order() -> ApprovedPaperOrder:
    account = RiskAccountState(
        account_id="paper-main",
        as_of=NOW,
        cash=Decimal("100000"),
        equity=Decimal("100000"),
        day_start_equity=Decimal("100000"),
        peak_equity=Decimal("100000"),
        gross_exposure=Decimal("0"),
        daily_turnover=Decimal("0"),
        open_order_count=0,
        reconciled=True,
        kill_switch=False,
    )
    order = ProposedOrder(
        client_order_id="paper-order-0001",
        instrument=INSTRUMENT,
        side=OrderSide.BUY,
        quantity=100,
        submitted_at=NOW,
    )
    quote = MarketQuote(
        instrument=INSTRUMENT,
        as_of=NOW,
        last_price=Decimal("10"),
        bid_price=Decimal("9.99"),
        ask_price=Decimal("10.01"),
        market_open=True,
    )
    rules = AshareRuleBook().resolve(
        INSTRUMENT,
        date(2026, 7, 22),
        SecurityStatus(risk_warning=False, listing_session_number=1000),
    )
    decision = PreTradeRiskEngine().evaluate(
        RiskEvaluationInput(
            mode=ExecutionMode.PAPER,
            policy=RiskPolicy(allowed_instruments=(INSTRUMENT,)),
            account=account,
            order=order,
            quote=quote,
            rules=rules,
            now=NOW,
        )
    )
    return ApprovedPaperOrder.from_risk_decision(decision)


def _update(
    sequence: int,
    state: PaperOrderState,
    *,
    filled: int = 0,
    price: str | None = None,
    occurred_at: datetime | None = None,
    rejection_code: str | None = None,
) -> BrokerOrderUpdate:
    return BrokerOrderUpdate(
        account_id="paper-main",
        client_order_id="paper-order-0001",
        broker_order_id="paper-broker-0001",
        broker_sequence=sequence,
        state=state,
        cumulative_filled_quantity=filled,
        average_fill_price=None if price is None else Decimal(price),
        occurred_at=occurred_at or NOW + timedelta(seconds=sequence),
        rejection_code=rejection_code,
    )


def test_order_lifecycle_is_hash_chained_and_duplicate_updates_are_idempotent() -> None:
    machine = PaperOrderStateMachine()
    projection = PaperOrderProjection.create(_approved_order())

    submitted = machine.apply(
        projection, _update(1, PaperOrderState.SUBMITTED)
    )
    partial = machine.apply(
        submitted.projection,
        _update(2, PaperOrderState.PARTIALLY_FILLED, filled=40, price="10.02"),
    )
    filled = machine.apply(
        partial.projection,
        _update(3, PaperOrderState.FILLED, filled=100, price="10.03"),
    )
    duplicate = machine.apply(
        filled.projection,
        _update(3, PaperOrderState.FILLED, filled=100, price="10.03"),
    )

    assert submitted.event is not None
    assert partial.event is not None
    assert partial.event.previous_hash == submitted.event.event_hash
    assert filled.projection.state is PaperOrderState.FILLED
    assert filled.projection.cumulative_filled_quantity == 100
    assert duplicate.applied is False
    assert duplicate.projection == filled.projection


def test_conflicting_duplicate_out_of_order_and_post_terminal_updates_fail_closed() -> None:
    machine = PaperOrderStateMachine()
    projection = PaperOrderProjection.create(_approved_order())
    submitted = machine.apply(
        projection, _update(1, PaperOrderState.SUBMITTED)
    ).projection

    with pytest.raises(ValueError, match="sequence already belongs"):
        machine.apply(
            submitted,
            _update(1, PaperOrderState.UNKNOWN),
        )
    partial = machine.apply(
        submitted,
        _update(2, PaperOrderState.PARTIALLY_FILLED, filled=40, price="10.02"),
    ).projection
    with pytest.raises(ValueError, match="out-of-order"):
        machine.apply(partial, _update(1, PaperOrderState.SUBMITTED))
    filled = machine.apply(
        partial,
        _update(3, PaperOrderState.FILLED, filled=100, price="10.03"),
    ).projection
    with pytest.raises(ValueError, match="terminal"):
        machine.apply(filled, _update(4, PaperOrderState.UNKNOWN, filled=100, price="10.03"))


def test_unknown_state_can_recover_only_from_a_newer_broker_fact() -> None:
    machine = PaperOrderStateMachine()
    projection = PaperOrderProjection.create(_approved_order())
    unknown = machine.apply(
        projection, _update(1, PaperOrderState.UNKNOWN)
    ).projection
    recovered = machine.apply(
        unknown, _update(2, PaperOrderState.SUBMITTED)
    ).projection

    assert recovered.state is PaperOrderState.SUBMITTED


def _snapshot(
    *,
    cash: str = "90000",
    equity: str = "100000",
    quantity: int = 1000,
    sellable: int = 1000,
    market_value: str = "10000",
    orders: tuple[str, ...] = ("paper-order-0001",),
    as_of: datetime = NOW,
) -> ExecutionAccountSnapshot:
    return ExecutionAccountSnapshot(
        account_id="paper-main",
        as_of=as_of,
        cash=Decimal(cash),
        equity=Decimal(equity),
        positions=(
            AccountPosition(
                instrument=INSTRUMENT,
                total_quantity=quantity,
                sellable_quantity=sellable,
                market_value=Decimal(market_value),
            ),
        ),
        open_client_order_ids=orders,
    )


def test_matching_account_snapshots_reconcile_deterministically() -> None:
    report = AccountReconciler().reconcile(
        internal=_snapshot(),
        broker=_snapshot(),
        now=NOW,
    )

    assert report.reconciled is True
    assert report.issues == ()


def test_reconciliation_reports_all_stable_mismatch_codes() -> None:
    report = AccountReconciler().reconcile(
        internal=_snapshot(as_of=NOW - timedelta(seconds=10)),
        broker=_snapshot(
            cash="80000",
            equity="95000",
            quantity=900,
            sellable=800,
            market_value="9000",
            orders=(),
            as_of=NOW - timedelta(seconds=10),
        ),
        now=NOW,
    )

    assert report.reconciled is False
    assert report.issues == (
        ReconciliationCode.STALE_INTERNAL_SNAPSHOT,
        ReconciliationCode.STALE_BROKER_SNAPSHOT,
        ReconciliationCode.BROKER_ACCOUNT_UNBALANCED,
        ReconciliationCode.CASH_MISMATCH,
        ReconciliationCode.EQUITY_MISMATCH,
        ReconciliationCode.POSITION_MISMATCH,
        ReconciliationCode.SELLABLE_MISMATCH,
        ReconciliationCode.MARKET_VALUE_MISMATCH,
        ReconciliationCode.OPEN_ORDER_MISMATCH,
    )
