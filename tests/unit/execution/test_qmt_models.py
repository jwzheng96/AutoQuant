from datetime import UTC, datetime
from decimal import Decimal

import pytest

from autoquant.backtest.models import OrderSide
from autoquant.errors import BrokerStateUnknownError
from autoquant.execution.models import PaperOrderState
from autoquant.execution.qmt_models import (
    QmtOrderObservation,
    QmtOrderStatus,
    from_qmt_instrument,
    map_qmt_order_state,
    require_qmt_query_records,
    to_qmt_instrument,
)

NOW = datetime(2026, 7, 23, 1, 2, tzinfo=UTC)


@pytest.mark.parametrize(
    ("internal", "qmt"),
    [("600000.XSHG", "600000.SH"), ("000001.XSHE", "000001.SZ")],
)
def test_instrument_mapping_is_explicit_and_reversible(internal: str, qmt: str) -> None:
    assert to_qmt_instrument(internal) == qmt
    assert from_qmt_instrument(qmt.lower()) == internal


@pytest.mark.parametrize(
    "instrument",
    ["430047.XBSE", "430047.BJ", "600000", "ABCDEF.XSHG", "60000.XSHG"],
)
def test_instrument_mapping_rejects_unsupported_or_malformed_codes(
    instrument: str,
) -> None:
    with pytest.raises(ValueError):
        if instrument.endswith((".XSHG", ".XSHE", ".XBSE")):
            to_qmt_instrument(instrument)
        else:
            from_qmt_instrument(instrument)


@pytest.mark.parametrize(
    ("status", "traded", "expected"),
    [
        (QmtOrderStatus.REPORTED, 0, PaperOrderState.SUBMITTED),
        (QmtOrderStatus.PARTIALLY_FILLED, 100, PaperOrderState.PARTIALLY_FILLED),
        (QmtOrderStatus.PARTIALLY_CANCELLED, 100, PaperOrderState.CANCELLED),
        (QmtOrderStatus.CANCELLED, 0, PaperOrderState.CANCELLED),
        (QmtOrderStatus.FILLED, 200, PaperOrderState.FILLED),
        (QmtOrderStatus.REJECTED, 0, PaperOrderState.REJECTED),
        (QmtOrderStatus.UNKNOWN, 0, PaperOrderState.UNKNOWN),
        (999, 0, PaperOrderState.UNKNOWN),
        (QmtOrderStatus.FILLED, 100, PaperOrderState.UNKNOWN),
        (QmtOrderStatus.REPORTED, 100, PaperOrderState.UNKNOWN),
        (QmtOrderStatus.PARTIALLY_CANCELLED, 0, PaperOrderState.UNKNOWN),
        (QmtOrderStatus.CANCELLED, 100, PaperOrderState.UNKNOWN),
    ],
)
def test_qmt_order_status_mapping_fails_closed_on_inconsistent_facts(
    status: int, traded: int, expected: PaperOrderState
) -> None:
    assert map_qmt_order_state(status, order_volume=200, traded_volume=traded) is expected


def test_qmt_observation_converts_cumulative_fill_to_internal_update() -> None:
    observation = QmtOrderObservation(
        account_id="broker-account",
        client_order_id="client-1",
        broker_order_id="broker-1",
        instrument="600000.XSHG",
        side=OrderSide.BUY,
        order_volume=200,
        traded_volume=100,
        average_traded_price=Decimal("10.25"),
        order_price=Decimal("10.50"),
        raw_status=QmtOrderStatus.PARTIALLY_FILLED,
        status_message="partial fill",
        observed_at=NOW,
        order_remark="AQ1234567890abcdef123456",
    )

    update = observation.to_broker_update(broker_sequence=7)

    assert update.state is PaperOrderState.PARTIALLY_FILLED
    assert update.cumulative_filled_quantity == 100
    assert update.average_fill_price == Decimal("10.25")
    assert update.broker_sequence == 7
    assert observation.order_remark == "AQ1234567890abcdef123456"


def test_none_query_result_is_unknown_not_an_empty_account() -> None:
    with pytest.raises(BrokerStateUnknownError, match="cannot be distinguished"):
        require_qmt_query_records(None, query_name="positions")

    assert require_qmt_query_records([], query_name="positions") == ()
