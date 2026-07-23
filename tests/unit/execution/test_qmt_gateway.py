from datetime import UTC, datetime

import pytest

from autoquant.errors import BrokerStateUnknownError, LiveTradingLockedError
from autoquant.execution.qmt_gateway import (
    LockedQmtGateway,
    QmtCallbackBuffer,
    QmtCallbackKind,
)


def test_callback_capture_preserves_local_order_and_copies_payload() -> None:
    buffer = QmtCallbackBuffer()
    payload = {"order_id": "first"}
    first = buffer.capture(
        QmtCallbackKind.ORDER,
        payload,
        received_at=datetime(2026, 7, 23, 1, 0, tzinfo=UTC),
    )
    payload["order_id"] = "mutated"
    second = buffer.capture(QmtCallbackKind.TRADE, {"trade_id": "second"})

    drained = buffer.drain()

    assert [event.local_sequence for event in drained] == [1, 2]
    assert buffer.cursor == 2
    assert first.payload["order_id"] == "first"
    assert second.local_sequence == 2
    assert buffer.drain() == ()


def test_callback_buffer_validates_limit() -> None:
    with pytest.raises(ValueError, match="positive"):
        QmtCallbackBuffer().drain(limit=0)


def test_callback_buffer_overflow_requires_a_full_reconnect() -> None:
    buffer = QmtCallbackBuffer(capacity=1)
    buffer.capture(QmtCallbackKind.ORDER, {"order_id": 1})

    with pytest.raises(BrokerStateUnknownError, match="overflow"):
        buffer.capture(QmtCallbackKind.ORDER, {"order_id": 2})
    with pytest.raises(BrokerStateUnknownError, match="overflow"):
        buffer.capture(QmtCallbackKind.ORDER, {"order_id": 3})
    assert not buffer.healthy
    with pytest.raises(BrokerStateUnknownError, match="overflow"):
        buffer.drain()


@pytest.mark.parametrize(
    "payload",
    [{"nested": []}, {"price": float("nan")}, {"": "missing-key"}],
)
def test_callback_capture_rejects_mutable_or_nonfinite_payloads(
    payload: dict[str, object],
) -> None:
    with pytest.raises((TypeError, ValueError)):
        QmtCallbackBuffer().capture(QmtCallbackKind.ORDER, payload)  # type: ignore[arg-type]


def test_qmt_gateway_never_submits_or_cancels_live_orders() -> None:
    gateway = LockedQmtGateway()

    assert gateway.gateway_available is False
    with pytest.raises(LiveTradingLockedError, match="hard-locked"):
        gateway.submit_order("anything")
    with pytest.raises(LiveTradingLockedError, match="hard-locked"):
        gateway.cancel_order("anything")
