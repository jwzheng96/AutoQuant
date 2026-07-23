from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from autoquant.errors import QuoteStreamUnavailableError
from autoquant.execution.quote_book import ContinuousQuoteBook, QuoteObservation
from autoquant.risk.models import MarketQuote

NOW = datetime(2026, 7, 23, 1, 30, tzinfo=UTC)


def _quote(
    instrument: str,
    *,
    as_of: datetime = NOW,
    market_open: bool = True,
    price: str = "10.00",
) -> MarketQuote:
    value = Decimal(price)
    return MarketQuote(
        instrument=instrument,
        as_of=as_of,
        last_price=value,
        bid_price=value - Decimal("0.01"),
        ask_price=value + Decimal("0.01"),
        market_open=market_open,
    )


def _observation(
    sequence: int,
    *,
    instrument: str = "600000.XSHG",
    as_of: datetime = NOW,
    received_at: datetime = NOW,
    price: str = "10.00",
) -> QuoteObservation:
    return QuoteObservation(
        source="qmt",
        source_sequence=sequence,
        received_at=received_at,
        quote=_quote(instrument, as_of=as_of, price=price),
    )


def test_quote_book_requires_baseline_and_returns_hash_committed_snapshot() -> None:
    book = ContinuousQuoteBook(source="qmt")
    with pytest.raises(QuoteStreamUnavailableError, match="unavailable"):
        book.snapshot(
            instruments=("600000.XSHG",),
            now=NOW,
            max_age=timedelta(seconds=3),
            require_market_open=True,
        )

    book.reset(
        quotes=(_quote("600000.XSHG"), _quote("000001.XSHE", price="12.00")),
        source_sequence=10,
        received_at=NOW,
        reset_id="baseline-1",
    )
    snapshot = book.snapshot(
        instruments=("600000.XSHG", "000001.XSHE"),
        now=NOW + timedelta(seconds=1),
        max_age=timedelta(seconds=3),
        require_market_open=True,
    )

    assert book.connected is True
    assert snapshot.source_sequence == 10
    assert snapshot.marks == {
        "000001.XSHE": Decimal("12.00"),
        "600000.XSHG": Decimal("10.00"),
    }
    assert len(snapshot.evidence_hash) == 64


def test_quote_book_enforces_contiguous_sequence_and_requires_reset_after_gap() -> None:
    book = ContinuousQuoteBook(source="qmt")
    book.reset(
        quotes=(_quote("600000.XSHG"),),
        source_sequence=10,
        received_at=NOW,
        reset_id="baseline-1",
    )

    with pytest.raises(QuoteStreamUnavailableError, match="gap"):
        book.publish(_observation(12, received_at=NOW + timedelta(seconds=1)))

    assert book.connected is False
    with pytest.raises(QuoteStreamUnavailableError, match="new baseline"):
        book.publish(_observation(11, received_at=NOW + timedelta(seconds=1)))
    book.reset(
        quotes=(_quote("600000.XSHG", as_of=NOW + timedelta(seconds=2)),),
        source_sequence=20,
        received_at=NOW + timedelta(seconds=2),
        reset_id="baseline-2",
    )
    assert book.connected is True


def test_quote_book_is_idempotent_only_for_exact_last_observation() -> None:
    book = ContinuousQuoteBook(source="qmt")
    book.reset(
        quotes=(_quote("600000.XSHG"),),
        source_sequence=10,
        received_at=NOW,
        reset_id="baseline-1",
    )
    observation = _observation(
        11,
        as_of=NOW + timedelta(seconds=1),
        received_at=NOW + timedelta(seconds=1),
        price="10.01",
    )
    book.publish(observation)
    book.publish(observation)

    with pytest.raises(QuoteStreamUnavailableError, match="conflicting"):
        book.publish(
            _observation(
                11,
                as_of=NOW + timedelta(seconds=1),
                received_at=NOW + timedelta(seconds=1),
                price="10.02",
            )
        )


def test_quote_book_rejects_time_regression_and_future_or_stale_quotes() -> None:
    book = ContinuousQuoteBook(source="qmt")
    book.reset(
        quotes=(_quote("600000.XSHG"),),
        source_sequence=10,
        received_at=NOW,
        reset_id="baseline-1",
    )
    with pytest.raises(QuoteStreamUnavailableError, match="backwards"):
        book.publish(
            _observation(
                11,
                as_of=NOW - timedelta(seconds=1),
                received_at=NOW + timedelta(seconds=1),
            )
        )

    book.reset(
        quotes=(_quote("600000.XSHG"),),
        source_sequence=20,
        received_at=NOW,
        reset_id="baseline-2",
    )
    with pytest.raises(QuoteStreamUnavailableError, match="stale"):
        book.snapshot(
            instruments=("600000.XSHG",),
            now=NOW + timedelta(seconds=4),
            max_age=timedelta(seconds=3),
            require_market_open=True,
        )


def test_quote_book_rejects_missing_or_closed_instruments() -> None:
    book = ContinuousQuoteBook(source="qmt")
    book.reset(
        quotes=(_quote("600000.XSHG", market_open=False),),
        source_sequence=1,
        received_at=NOW,
        reset_id="closed-baseline",
    )
    with pytest.raises(QuoteStreamUnavailableError, match="closed"):
        book.snapshot(
            instruments=("600000.XSHG",),
            now=NOW,
            max_age=timedelta(seconds=3),
            require_market_open=True,
        )
    with pytest.raises(QuoteStreamUnavailableError, match="omitted"):
        book.snapshot(
            instruments=("000001.XSHE",),
            now=NOW,
            max_age=timedelta(seconds=3),
            require_market_open=False,
        )


def test_quote_observation_rejects_future_event_time() -> None:
    with pytest.raises(ValueError, match="receipt"):
        _observation(
            1,
            as_of=NOW + timedelta(seconds=1),
            received_at=NOW,
        )
