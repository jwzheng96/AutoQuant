from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from autoquant.backtest.models import OrderSide
from autoquant.execution.account_projection import PaperAccountProjector
from autoquant.execution.models import (
    ApprovedPaperOrder,
    BrokerOrderUpdate,
    PaperOrderHistory,
    PaperOrderState,
)

NOW = datetime(2026, 7, 22, 5, tzinfo=UTC)
INSTRUMENT = "000001.XSHE"


def _order(*, side: OrderSide, order_id: str = "paper-order-0001") -> ApprovedPaperOrder:
    return ApprovedPaperOrder(
        account_id="paper-main",
        client_order_id=order_id,
        risk_decision_hash="a" * 64,
        instrument=INSTRUMENT,
        side=side,
        quantity=100,
        limit_price=None,
        approved_at=NOW,
    )


def _update(
    order: ApprovedPaperOrder,
    *,
    sequence: int,
    state: PaperOrderState,
    filled: int = 0,
    price: str | None = None,
    seconds: int | None = None,
) -> BrokerOrderUpdate:
    return BrokerOrderUpdate(
        account_id=order.account_id,
        client_order_id=order.client_order_id,
        broker_order_id=f"broker-{order.client_order_id}",
        broker_sequence=sequence,
        state=state,
        cumulative_filled_quantity=filled,
        average_fill_price=None if price is None else Decimal(price),
        occurred_at=NOW + timedelta(seconds=seconds or sequence),
    )


def _filled_history(
    *,
    side: OrderSide = OrderSide.BUY,
    order_id: str = "paper-order-0001",
    price: str = "10.01",
    seconds: int = 2,
) -> PaperOrderHistory:
    order = _order(side=side, order_id=order_id)
    return PaperOrderHistory(
        order=order,
        state=PaperOrderState.FILLED,
        updates=(
            _update(order, sequence=1, state=PaperOrderState.SUBMITTED),
            _update(
                order,
                sequence=2,
                state=PaperOrderState.FILLED,
                filled=100,
                price=price,
                seconds=seconds,
            ),
        ),
    )


def test_empty_account_projects_initial_cash() -> None:
    snapshot = PaperAccountProjector().project(
        account_id="paper-main",
        initial_cash=Decimal("100000"),
        histories=(),
        marks={},
        as_of=NOW,
    )

    assert snapshot.cash == Decimal("100000")
    assert snapshot.equity == Decimal("100000")
    assert snapshot.positions == ()
    assert snapshot.open_client_order_ids == ()
    assert snapshot.evidence_hash != "0" * 64


def test_buy_fill_projects_fees_position_and_t_plus_one() -> None:
    projector = PaperAccountProjector()
    history = _filled_history()

    same_day = projector.project(
        account_id="paper-main",
        initial_cash=Decimal("100000"),
        histories=(history,),
        marks={INSTRUMENT: Decimal("10.01")},
        as_of=NOW + timedelta(minutes=1),
    )
    next_day = projector.project(
        account_id="paper-main",
        initial_cash=Decimal("100000"),
        histories=(history,),
        marks={INSTRUMENT: Decimal("10.01")},
        as_of=NOW + timedelta(days=1),
    )

    assert same_day.cash == Decimal("98993.99")
    assert same_day.equity == Decimal("99994.99")
    assert same_day.positions[0].total_quantity == 100
    assert same_day.positions[0].sellable_quantity == 0
    assert next_day.positions[0].sellable_quantity == 100
    assert same_day.open_client_order_ids == ()


def test_partial_fills_charge_minimum_commission_once_per_order() -> None:
    order = _order(side=OrderSide.BUY)
    history = PaperOrderHistory(
        order=order,
        state=PaperOrderState.FILLED,
        updates=(
            _update(order, sequence=1, state=PaperOrderState.SUBMITTED),
            _update(
                order,
                sequence=2,
                state=PaperOrderState.PARTIALLY_FILLED,
                filled=40,
                price="10",
            ),
            _update(
                order,
                sequence=3,
                state=PaperOrderState.FILLED,
                filled=100,
                price="10",
            ),
        ),
    )

    snapshot = PaperAccountProjector().project(
        account_id="paper-main",
        initial_cash=Decimal("100000"),
        histories=(history,),
        marks={INSTRUMENT: Decimal("10")},
        as_of=NOW + timedelta(minutes=1),
    )

    assert snapshot.cash == Decimal("98994.99")


def test_projection_fails_closed_for_missing_mark_and_same_day_sell() -> None:
    projector = PaperAccountProjector()
    buy = _filled_history()
    sell = _filled_history(
        side=OrderSide.SELL,
        order_id="paper-order-0002",
        price="9.99",
        seconds=3,
    )

    with pytest.raises(ValueError, match="current mark"):
        projector.project(
            account_id="paper-main",
            initial_cash=Decimal("100000"),
            histories=(buy,),
            marks={},
            as_of=NOW + timedelta(minutes=1),
        )
    with pytest.raises(ValueError, match=r"T\+1"):
        projector.project(
            account_id="paper-main",
            initial_cash=Decimal("100000"),
            histories=(buy, sell),
            marks={INSTRUMENT: Decimal("10")},
            as_of=NOW + timedelta(minutes=1),
        )


def test_evidence_hash_is_deterministic_and_commits_to_marks_and_history() -> None:
    projector = PaperAccountProjector()
    filled = _filled_history()
    approved = PaperOrderHistory(
        order=_order(side=OrderSide.BUY, order_id="paper-order-0002"),
        state=PaperOrderState.APPROVED,
        updates=(),
    )
    common = {
        "account_id": "paper-main",
        "initial_cash": Decimal("100000"),
        "as_of": NOW + timedelta(minutes=1),
    }

    first = projector.project(
        **common,
        histories=(approved, filled),
        marks={INSTRUMENT: Decimal("10.01")},
    )
    reordered = projector.project(
        **common,
        histories=(filled, approved),
        marks={INSTRUMENT: Decimal("10.01")},
    )
    remarked = projector.project(
        **common,
        histories=(filled, approved),
        marks={INSTRUMENT: Decimal("10.02")},
    )

    assert first.evidence_hash == reordered.evidence_hash
    assert first.snapshot_hash == reordered.snapshot_hash
    assert first.evidence_hash != remarked.evidence_hash
    assert first.open_client_order_ids == ("paper-order-0002",)
