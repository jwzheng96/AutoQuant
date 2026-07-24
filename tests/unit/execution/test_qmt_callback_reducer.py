from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from autoquant.backtest.models import OrderSide
from autoquant.execution.models import ZERO_HASH, PaperOrderState
from autoquant.execution.qmt_callback_inbox import QmtCallbackInboxEvent, sanitize_qmt_callback
from autoquant.execution.qmt_callback_reducer import (
    QmtOrderConvergence,
    apply_qmt_order_callback,
    apply_qmt_trade_fact,
    initial_qmt_order_projection,
    qmt_trade_fact_from_callback,
    unknown_qmt_order_projection,
)
from autoquant.execution.qmt_gateway import QmtCallbackBuffer, QmtCallbackKind

NOW = datetime(2026, 7, 24, 1, tzinfo=UTC)
BROKER_ACCOUNT = "broker-account"
LOGICAL_ACCOUNT = "live-canary"
CANDIDATE_HASH = "a" * 64
REMARK = "AQ1234567890abcdef123456"


def _event(
    kind: QmtCallbackKind,
    payload: dict[str, object],
    *,
    sequence: int,
    previous_hash: str,
) -> QmtCallbackInboxEvent:
    buffer = QmtCallbackBuffer()
    for index in range(sequence - 1):
        buffer.capture(
            QmtCallbackKind.DISCONNECTED,
            {"reason": f"prior_{index}"},
            received_at=NOW,
        )
    envelope = buffer.capture(kind, payload, received_at=NOW)
    callback = sanitize_qmt_callback(
        envelope,
        expected_broker_account_id=BROKER_ACCOUNT,
        logical_account_id=LOGICAL_ACCOUNT,
    )
    return QmtCallbackInboxEvent(
        callback=callback,
        gateway_holder_id="windows-qmt-canary-01",
        qmt_session_id=20260724,
        qmt_lease_generation=1,
        previous_hash=previous_hash,
    )


def _projection(event: QmtCallbackInboxEvent):
    return initial_qmt_order_projection(
        event=event,
        candidate_hash=CANDIDATE_HASH,
        client_order_id="canary-001",
        broker_order_id="88001",
        instrument="600000.XSHG",
        side=OrderSide.BUY,
        quantity=100,
        limit_price=Decimal("10.5"),
        order_remark=REMARK,
    )


def _order_payload(*, status: int, traded_volume: int, traded_price: float) -> dict[str, object]:
    return {
        "account_id": BROKER_ACCOUNT,
        "order_id": 88001,
        "order_remark": REMARK,
        "order_status": status,
        "order_volume": 100,
        "price": 10.5,
        "side": "buy",
        "status_msg": "",
        "stock_code": "600000.SH",
        "traded_price": traded_price,
        "traded_volume": traded_volume,
    }


def _trade_payload(*, trade_id: str, volume: int, price: float) -> dict[str, object]:
    return {
        "account_id": BROKER_ACCOUNT,
        "order_id": 88001,
        "order_remark": REMARK,
        "side": "buy",
        "stock_code": "600000.SH",
        "traded_amount": price * volume,
        "traded_id": trade_id,
        "traded_price": price,
        "traded_volume": volume,
    }


def test_trade_before_order_stays_pending_then_converges() -> None:
    trade_event = _event(
        QmtCallbackKind.TRADE,
        _trade_payload(trade_id="T-1", volume=40, price=10.5),
        sequence=1,
        previous_hash=ZERO_HASH,
    )
    fact = qmt_trade_fact_from_callback(
        trade_event,
        candidate_hash=CANDIDATE_HASH,
        client_order_id="canary-001",
    )
    with_trade = apply_qmt_trade_fact(_projection(trade_event), fact, trade_event)
    assert with_trade.convergence is QmtOrderConvergence.PENDING
    assert with_trade.trade_volume == 40

    order_event = _event(
        QmtCallbackKind.ORDER,
        _order_payload(status=55, traded_volume=40, traded_price=10.5),
        sequence=2,
        previous_hash=trade_event.event_hash,
    )
    converged = apply_qmt_order_callback(with_trade, order_event)
    assert converged.convergence is QmtOrderConvergence.CONVERGED
    assert converged.order_state is PaperOrderState.PARTIALLY_FILLED


def test_missing_trade_and_average_conflict_do_not_claim_convergence() -> None:
    order_event = _event(
        QmtCallbackKind.ORDER,
        _order_payload(status=55, traded_volume=40, traded_price=10.5),
        sequence=1,
        previous_hash=ZERO_HASH,
    )
    projection = apply_qmt_order_callback(_projection(order_event), order_event)
    assert projection.convergence is QmtOrderConvergence.PENDING

    trade_event = _event(
        QmtCallbackKind.TRADE,
        _trade_payload(trade_id="T-1", volume=40, price=10.4),
        sequence=2,
        previous_hash=order_event.event_hash,
    )
    fact = qmt_trade_fact_from_callback(
        trade_event,
        candidate_hash=CANDIDATE_HASH,
        client_order_id="canary-001",
    )
    conflicted = apply_qmt_trade_fact(projection, fact, trade_event)
    assert conflicted.convergence is QmtOrderConvergence.UNKNOWN


def test_order_regression_and_identity_conflict_fail_closed() -> None:
    first_event = _event(
        QmtCallbackKind.ORDER,
        _order_payload(status=55, traded_volume=40, traded_price=10.5),
        sequence=1,
        previous_hash=ZERO_HASH,
    )
    projection = apply_qmt_order_callback(_projection(first_event), first_event)
    regressed = _event(
        QmtCallbackKind.ORDER,
        _order_payload(status=50, traded_volume=0, traded_price=0),
        sequence=2,
        previous_hash=first_event.event_hash,
    )
    with pytest.raises(ValueError, match="regressed"):
        apply_qmt_order_callback(projection, regressed)

    wrong_terms = _order_payload(status=55, traded_volume=40, traded_price=10.5)
    wrong_terms["order_volume"] = 200
    conflicted = _event(
        QmtCallbackKind.ORDER,
        wrong_terms,
        sequence=2,
        previous_hash=first_event.event_hash,
    )
    with pytest.raises(ValueError, match="terms"):
        apply_qmt_order_callback(projection, conflicted)


def test_aggregate_trade_quantity_cannot_exceed_staged_order() -> None:
    first = _event(
        QmtCallbackKind.TRADE,
        _trade_payload(trade_id="T-1", volume=60, price=10.5),
        sequence=1,
        previous_hash=ZERO_HASH,
    )
    first_fact = qmt_trade_fact_from_callback(
        first,
        candidate_hash=CANDIDATE_HASH,
        client_order_id="canary-001",
    )
    projection = apply_qmt_trade_fact(_projection(first), first_fact, first)
    second = _event(
        QmtCallbackKind.TRADE,
        _trade_payload(trade_id="T-2", volume=50, price=10.5),
        sequence=2,
        previous_hash=first.event_hash,
    )
    second_fact = qmt_trade_fact_from_callback(
        second,
        candidate_hash=CANDIDATE_HASH,
        client_order_id="canary-001",
    )
    with pytest.raises(ValueError, match="exceed"):
        apply_qmt_trade_fact(projection, second_fact, second)


def test_terminal_or_unknown_projection_cannot_be_washed_by_later_callback() -> None:
    filled_event = _event(
        QmtCallbackKind.ORDER,
        _order_payload(status=56, traded_volume=100, traded_price=10.5),
        sequence=1,
        previous_hash=ZERO_HASH,
    )
    filled_projection = apply_qmt_order_callback(
        _projection(filled_event),
        filled_event,
    )
    later_submitted = _event(
        QmtCallbackKind.ORDER,
        _order_payload(status=50, traded_volume=100, traded_price=10.5),
        sequence=2,
        previous_hash=filled_event.event_hash,
    )
    with pytest.raises(ValueError, match="terminal"):
        apply_qmt_order_callback(filled_projection, later_submitted)

    unknown_projection = unknown_qmt_order_projection(
        filled_projection,
        later_submitted,
    )
    with pytest.raises(ValueError, match="cannot recover"):
        apply_qmt_order_callback(unknown_projection, later_submitted)
