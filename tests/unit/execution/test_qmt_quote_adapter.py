from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from autoquant.errors import QuoteStreamUnavailableError
from autoquant.execution.market_clock import AShareTradingPhase
from autoquant.execution.qmt_quote_adapter import QmtWholeQuoteBridge, normalize_qmt_tick
from autoquant.execution.quote_book import ContinuousQuoteBook

NOW = datetime(2026, 7, 23, 1, 30, 0, 500_000, tzinfo=UTC)
TIME_MS = 1_774_403_800_000
INSTRUMENT = "600000.XSHG"


def _tick(
    *,
    timestamp: object = TIME_MS,
    last: object = 10.0,
    bid: object = (9.99, 9.98),
    ask: object = (10.01, 10.02),
    status: object = 13,
) -> dict[str, object]:
    return {
        "time": timestamp,
        "lastPrice": last,
        "bidPrice": bid,
        "askPrice": ask,
        "stockStatus": status,
    }


def _bridge(*, capacity: int = 10) -> tuple[ContinuousQuoteBook, QmtWholeQuoteBridge]:
    book = ContinuousQuoteBook(source="qmt")
    return book, QmtWholeQuoteBridge(
        quote_book=book,
        instruments=(INSTRUMENT,),
        queue_capacity=capacity,
    )


def test_normalize_qmt_tick_uses_documented_fields_and_both_open_signals() -> None:
    quote = normalize_qmt_tick(
        qmt_instrument="600000.sh",
        tick=_tick(),  # type: ignore[arg-type]
        received_at=NOW,
        phase=AShareTradingPhase.MORNING_CONTINUOUS,
    )

    assert quote.instrument == INSTRUMENT
    assert quote.last_price == Decimal("10.0")
    assert quote.bid_price == Decimal("9.99")
    assert quote.ask_price == Decimal("10.01")
    assert quote.market_open
    assert quote.as_of == datetime(2026, 3, 25, 1, 56, 40, tzinfo=UTC)

    auction = normalize_qmt_tick(
        qmt_instrument="600000.SH",
        tick=_tick(),  # type: ignore[arg-type]
        received_at=NOW,
        phase=AShareTradingPhase.OPENING_AUCTION,
    )
    halted = normalize_qmt_tick(
        qmt_instrument="600000.SH",
        tick=_tick(status=17),  # type: ignore[arg-type]
        received_at=NOW,
        phase=AShareTradingPhase.MORNING_CONTINUOUS,
    )
    assert not auction.market_open
    assert not halted.market_open


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("time", True),
        ("time", float("nan")),
        ("time", 1),
        ("lastPrice", 0),
        ("lastPrice", float("inf")),
        ("bidPrice", ()),
        ("bidPrice", (0,)),
        ("askPrice", "10.01"),
        ("stockStatus", True),
        ("stockStatus", 13.5),
    ],
)
def test_normalize_qmt_tick_rejects_malformed_or_unsafe_values(
    field: str, value: object
) -> None:
    tick = _tick()
    tick[field] = value

    with pytest.raises((TypeError, ValueError)):
        normalize_qmt_tick(
            qmt_instrument="600000.SH",
            tick=tick,  # type: ignore[arg-type]
            received_at=NOW,
            phase=AShareTradingPhase.MORNING_CONTINUOUS,
        )


def test_full_tick_baseline_must_exactly_cover_universe() -> None:
    book = ContinuousQuoteBook(source="qmt")
    bridge = QmtWholeQuoteBridge(
        quote_book=book,
        instruments=("600000.XSHG", "000001.XSHE"),
    )

    with pytest.raises(ValueError, match="does not cover"):
        bridge.reset_from_full_tick(
            {"600000.SH": _tick()},
            received_at=NOW,
            phase=AShareTradingPhase.MORNING_CONTINUOUS,
            reset_id="incomplete",
        )
    assert not book.connected


def test_invalid_replacement_baseline_disconnects_an_existing_stream() -> None:
    book, bridge = _bridge()
    bridge.reset_from_full_tick(
        {"600000.SH": _tick()},
        received_at=NOW,
        phase=AShareTradingPhase.MORNING_CONTINUOUS,
        reset_id="valid",
    )
    assert book.connected

    with pytest.raises(ValueError):
        bridge.reset_from_full_tick(
            {"600000.SH": _tick(last=0)},
            received_at=NOW,
            phase=AShareTradingPhase.MORNING_CONTINUOUS,
            reset_id="invalid",
        )
    assert not book.connected


def test_callbacks_are_copied_then_published_with_contiguous_local_sequence() -> None:
    book, bridge = _bridge()
    baseline = _tick(last=10)
    bridge.reset_from_full_tick(
        {"600000.SH": baseline},
        received_at=NOW,
        phase=AShareTradingPhase.MORNING_CONTINUOUS,
        reset_id="full-tick-001",
    )
    first = _tick(timestamp=TIME_MS + 100, last=10.01, bid=(10.0,), ask=(10.02,))
    envelope = bridge.capture(
        {"600000.SH": first},
        received_at=NOW,
        phase=AShareTradingPhase.MORNING_CONTINUOUS,
    )
    first["lastPrice"] = 999
    bridge.capture(
        {
            "600000.SH": _tick(
                timestamp=TIME_MS + 200,
                last=10.02,
                bid=(10.01,),
                ask=(10.03,),
            )
        },
        received_at=NOW,
        phase=AShareTradingPhase.MORNING_CONTINUOUS,
    )

    observations = bridge.drain()

    assert envelope.callback_sequence == 1
    assert [item.source_sequence for item in observations] == [2, 3]
    assert [item.quote.last_price for item in observations] == [
        Decimal("10.01"),
        Decimal("10.02"),
    ]
    snapshot = book.snapshot(
        instruments=(INSTRUMENT,),
        now=NOW,
        max_age=NOW - datetime(2026, 1, 1, tzinfo=UTC),
        require_market_open=True,
    )
    assert snapshot.source_sequence == 3


def test_invalid_callback_disconnects_stream_until_a_new_baseline() -> None:
    book, bridge = _bridge()
    bridge.reset_from_full_tick(
        {"600000.SH": _tick()},
        received_at=NOW,
        phase=AShareTradingPhase.MORNING_CONTINUOUS,
        reset_id="full-tick-001",
    )
    bridge.capture(
        {"600000.SH": _tick(bid=(0,))},
        received_at=NOW,
        phase=AShareTradingPhase.MORNING_CONTINUOUS,
    )

    with pytest.raises(QuoteStreamUnavailableError):
        bridge.drain()
    assert not book.connected
    with pytest.raises(QuoteStreamUnavailableError, match="unavailable"):
        book.snapshot(
            instruments=(INSTRUMENT,),
            now=NOW,
            max_age=NOW - datetime(2026, 1, 1, tzinfo=UTC),
            require_market_open=True,
        )


def test_callback_queue_overflow_disconnects_stream() -> None:
    book, bridge = _bridge(capacity=1)
    bridge.reset_from_full_tick(
        {"600000.SH": _tick()},
        received_at=NOW,
        phase=AShareTradingPhase.MORNING_CONTINUOUS,
        reset_id="full-tick-001",
    )
    bridge.capture(
        {"600000.SH": _tick(timestamp=TIME_MS + 1)},
        received_at=NOW,
        phase=AShareTradingPhase.MORNING_CONTINUOUS,
    )

    with pytest.raises(QuoteStreamUnavailableError, match="overflow"):
        bridge.capture(
            {"600000.SH": _tick(timestamp=TIME_MS + 2)},
            received_at=NOW,
            phase=AShareTradingPhase.MORNING_CONTINUOUS,
        )
    assert not book.connected
