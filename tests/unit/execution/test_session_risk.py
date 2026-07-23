from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from autoquant.backtest.models import OrderSide
from autoquant.execution.models import (
    ApprovedPaperOrder,
    BrokerOrderUpdate,
    PaperOrderHistory,
    PaperOrderState,
)
from autoquant.execution.session_risk import (
    SessionRiskObservation,
    apply_session_observation,
    derive_session_turnover,
)

NOW = datetime(2026, 7, 22, 5, tzinfo=UTC)
SESSION_DATE = date(2026, 7, 22)


def _history() -> PaperOrderHistory:
    order = ApprovedPaperOrder(
        account_id="paper-main",
        client_order_id="session-order-0001",
        risk_decision_hash="a" * 64,
        instrument="000001.XSHE",
        side=OrderSide.BUY,
        quantity=100,
        limit_price=None,
        approved_at=NOW,
    )

    def update(
        sequence: int,
        state: PaperOrderState,
        filled: int,
        price: str | None,
    ) -> BrokerOrderUpdate:
        return BrokerOrderUpdate(
            account_id=order.account_id,
            client_order_id=order.client_order_id,
            broker_order_id="session-broker-0001",
            broker_sequence=sequence,
            state=state,
            cumulative_filled_quantity=filled,
            average_fill_price=None if price is None else Decimal(price),
            occurred_at=NOW + timedelta(seconds=sequence),
        )

    return PaperOrderHistory(
        order=order,
        state=PaperOrderState.FILLED,
        updates=(
            update(1, PaperOrderState.SUBMITTED, 0, None),
            update(2, PaperOrderState.PARTIALLY_FILLED, 40, "10"),
            update(3, PaperOrderState.FILLED, 100, "10.01"),
        ),
    )


def _observation(
    *, equity: str, turnover: str, seconds: int
) -> SessionRiskObservation:
    return SessionRiskObservation(
        account_id="paper-main",
        session_date=SESSION_DATE,
        as_of=NOW + timedelta(seconds=seconds),
        equity=Decimal(equity),
        cumulative_turnover=Decimal(turnover),
        snapshot_hash=("b" if seconds == 0 else "c") * 64,
        turnover_evidence_hash=("d" if seconds == 0 else "e") * 64,
    )


def test_turnover_is_derived_from_cumulative_fill_deltas() -> None:
    evidence = derive_session_turnover(
        account_id="paper-main",
        session_date=SESSION_DATE,
        histories=(_history(),),
    )

    assert evidence.cumulative_turnover == Decimal("1001.00")
    assert len(evidence.fill_evidence) == 2
    assert evidence.evidence_hash != "0" * 64


def test_session_state_preserves_opening_equity_and_advances_peak_and_turnover() -> None:
    initial, first_event = apply_session_observation(
        None,
        _observation(equity="100000", turnover="0", seconds=0),
    )
    advanced, second_event = apply_session_observation(
        initial,
        _observation(equity="100100", turnover="1001", seconds=1),
    )

    assert initial.day_start_equity == Decimal("100000")
    assert advanced.day_start_equity == Decimal("100000")
    assert advanced.peak_equity == Decimal("100100")
    assert advanced.cumulative_turnover == Decimal("1001")
    assert advanced.version == 2
    assert second_event.previous_hash == first_event.event_hash
    assert advanced.last_event_hash == second_event.event_hash


def test_session_state_rejects_late_initialization_and_decreasing_turnover() -> None:
    with pytest.raises(ValueError, match="before its first fill"):
        apply_session_observation(
            None,
            _observation(equity="100000", turnover="1", seconds=0),
        )
    initial, _ = apply_session_observation(
        None,
        _observation(equity="100000", turnover="0", seconds=0),
    )
    advanced, _ = apply_session_observation(
        initial,
        _observation(equity="99900", turnover="10", seconds=1),
    )
    with pytest.raises(ValueError, match="cannot decrease"):
        apply_session_observation(
            advanced,
            _observation(equity="99900", turnover="9", seconds=2),
        )
