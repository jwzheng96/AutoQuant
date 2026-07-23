from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import pytest

from autoquant.data.daily_models import TradingSession
from autoquant.errors import MissingCapabilityError, QuoteStreamUnavailableError
from autoquant.execution.market_clock import AShareMarketClock
from autoquant.execution.qmt_quote_adapter import QmtWholeQuoteBridge
from autoquant.execution.qmt_quote_runtime import (
    ImportedXtDataClient,
    QmtQuoteCallback,
    QmtWholeQuoteRuntime,
)
from autoquant.execution.quote_book import ContinuousQuoteBook

NOW = datetime(2026, 7, 23, 1, 30, 0, 500_000, tzinfo=UTC)
SESSION_DATE = date(2026, 7, 23)
INSTRUMENT = "600000.XSHG"


def _tick(*, ask: object = (10.01, 10.02)) -> dict[str, object]:
    return {
        "time": int((NOW - timedelta(milliseconds=100)).timestamp() * 1000),
        "lastPrice": 10.0,
        "bidPrice": (9.99, 9.98),
        "askPrice": ask,
        "stockStatus": 13,
    }


class FakeXtData:
    def __init__(
        self,
        *,
        baseline: dict[str, object] | None = None,
        callback_payload: dict[str, object] | None = None,
        subscription: int = 7,
    ) -> None:
        self.baseline = baseline or {"600000.SH": _tick()}
        self.callback_payload = callback_payload
        self.subscription = subscription
        self.unsubscribed: list[int] = []

    def get_full_tick(self, code_list: list[str]) -> dict[str, object]:
        assert code_list == ["600000.SH"]
        return self.baseline

    def subscribe_whole_quote(
        self,
        code_list: list[str],
        callback: QmtQuoteCallback | None = None,
    ) -> int:
        assert code_list == ["600000.SH"]
        if self.callback_payload is not None and callback is not None:
            callback(self.callback_payload)
        return self.subscription

    def unsubscribe_quote(self, sequence: int) -> None:
        self.unsubscribed.append(sequence)


def _runtime(
    client: FakeXtData,
) -> tuple[QmtWholeQuoteRuntime, ContinuousQuoteBook]:
    book = ContinuousQuoteBook(source="qmt")
    bridge = QmtWholeQuoteBridge(
        quote_book=book,
        instruments=(INSTRUMENT,),
    )
    calendar = AsyncMock()
    calendar.return_value = TradingSession(
        source="tushare",
        session_date=SESSION_DATE,
        is_open=True,
        available_at=NOW - timedelta(days=1),
        response_hash="a" * 64,
    )
    return (
        QmtWholeQuoteRuntime(
            client=client,
            bridge=bridge,
            instruments=(INSTRUMENT,),
            calendar=calendar,
            market_clock=AShareMarketClock(),
            now=lambda: NOW,
            pump_interval=timedelta(milliseconds=1),
        ),
        book,
    )


@pytest.mark.asyncio
async def test_quote_runtime_baselines_subscribes_drains_and_unsubscribes() -> None:
    client = FakeXtData(callback_payload={"600000.SH": _tick()})
    runtime, book = _runtime(client)

    await runtime.open()
    stop = asyncio.Event()
    task = asyncio.create_task(runtime.pump(stop=stop))
    await asyncio.sleep(0)
    stop.set()
    await task

    snapshot = book.snapshot(
        instruments=(INSTRUMENT,),
        now=NOW,
        max_age=timedelta(seconds=1),
        require_market_open=True,
    )
    assert snapshot.quotes[INSTRUMENT].last_price == Decimal("10.0")
    await runtime.close()
    assert client.unsubscribed == [7]
    assert not book.connected


@pytest.mark.asyncio
async def test_malformed_callback_invalidates_stream_and_stops_pump() -> None:
    client = FakeXtData(
        callback_payload={"600000.SH": _tick(ask=())},
    )
    runtime, book = _runtime(client)
    await runtime.open()

    with pytest.raises(QuoteStreamUnavailableError, match="callback"):
        await runtime.pump(stop=asyncio.Event())

    assert not book.connected
    await runtime.close()


@pytest.mark.asyncio
async def test_rejected_subscription_invalidates_baseline() -> None:
    client = FakeXtData(subscription=-1)
    runtime, book = _runtime(client)

    with pytest.raises(QuoteStreamUnavailableError, match="rejected"):
        await runtime.open()

    assert not book.connected
    await runtime.close()


def test_vendor_module_loader_refuses_non_windows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "autoquant.execution.qmt_quote_runtime.platform.system",
        lambda: "Darwin",
    )

    with pytest.raises(MissingCapabilityError, match="Windows"):
        ImportedXtDataClient.load()


def test_imported_client_rejects_malformed_vendor_results() -> None:
    module = AnyModule(
        get_full_tick=lambda _codes: None,
        subscribe_whole_quote=lambda _codes, callback=None: True,
        unsubscribe_quote=lambda _sequence: None,
    )
    client = ImportedXtDataClient(module)  # type: ignore[arg-type]

    with pytest.raises(QuoteStreamUnavailableError, match="malformed"):
        client.get_full_tick(["600000.SH"])
    with pytest.raises(QuoteStreamUnavailableError, match="invalid"):
        client.subscribe_whole_quote(["600000.SH"])


class AnyModule:
    def __init__(self, **values: Any) -> None:
        self.__dict__.update(values)
