from __future__ import annotations

import asyncio
import importlib
import platform
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime, timedelta
from threading import Lock
from types import ModuleType
from typing import Protocol
from uuid import uuid4

from autoquant.clock import to_shanghai, to_utc
from autoquant.data.daily_models import TradingSession
from autoquant.errors import MissingCapabilityError, QuoteStreamUnavailableError
from autoquant.execution.market_clock import AShareMarketClock, AShareTradingPhase
from autoquant.execution.paper_runtime import RuntimeCalendarReader
from autoquant.execution.qmt_models import to_qmt_instrument
from autoquant.execution.qmt_quote_adapter import QmtWholeQuoteBridge

QmtQuoteCallback = Callable[[Mapping[str, object]], None]


class XtDataClient(Protocol):
    def get_full_tick(
        self,
        code_list: list[str],
    ) -> Mapping[str, object]: ...

    def subscribe_whole_quote(
        self,
        code_list: list[str],
        callback: QmtQuoteCallback | None = None,
    ) -> int: ...

    def unsubscribe_quote(self, sequence: int) -> None: ...


class ImportedXtDataClient:
    """Narrow read-only facade around the vendor module loaded only on Windows."""

    def __init__(self, module: ModuleType) -> None:
        self._module = module

    @classmethod
    def load(cls) -> ImportedXtDataClient:
        if platform.system().casefold() != "windows":
            raise MissingCapabilityError(
                "XtData paper quotes require the authorized Windows host"
            )
        try:
            module = importlib.import_module("xtquant.xtdata")
        except (ImportError, ModuleNotFoundError):
            raise MissingCapabilityError(
                "the broker-provided xtquant module is unavailable"
            ) from None
        return cls(module)

    def get_full_tick(
        self,
        code_list: list[str],
    ) -> Mapping[str, object]:
        payload = self._module.get_full_tick(code_list)
        if not isinstance(payload, Mapping):
            raise QuoteStreamUnavailableError(
                "XtData get_full_tick returned a malformed baseline"
            )
        return payload

    def subscribe_whole_quote(
        self,
        code_list: list[str],
        callback: QmtQuoteCallback | None = None,
    ) -> int:
        result = self._module.subscribe_whole_quote(
            code_list,
            callback=callback,
        )
        if not isinstance(result, int) or isinstance(result, bool):
            raise QuoteStreamUnavailableError(
                "XtData subscribe_whole_quote returned an invalid identifier"
            )
        return result

    def unsubscribe_quote(self, sequence: int) -> None:
        self._module.unsubscribe_quote(sequence)


class QmtWholeQuoteRuntime:
    """Maintain one XtData full-quote subscription for the paper simulator only."""

    def __init__(
        self,
        *,
        client: XtDataClient,
        bridge: QmtWholeQuoteBridge,
        instruments: tuple[str, ...],
        calendar: RuntimeCalendarReader,
        market_clock: AShareMarketClock,
        now: Callable[[], datetime] | None = None,
        pump_interval: timedelta = timedelta(milliseconds=50),
        drain_limit: int = 1_000,
    ) -> None:
        normalized = tuple(sorted(instruments))
        if (
            not normalized
            or len(set(normalized)) != len(normalized)
            or any(not value.strip() for value in normalized)
        ):
            raise ValueError("QMT quote runtime instruments must be nonempty and unique")
        if pump_interval <= timedelta(0):
            raise ValueError("QMT quote pump interval must be positive")
        if drain_limit < len(normalized):
            raise ValueError("QMT quote drain limit cannot split a universe batch")
        self._client = client
        self._bridge = bridge
        self._instruments = normalized
        self._qmt_instruments = [
            to_qmt_instrument(value) for value in normalized
        ]
        self._calendar = calendar
        self._clock = market_clock
        self._now = now or (lambda: datetime.now(UTC))
        self._pump_interval = pump_interval
        self._drain_limit = drain_limit
        self._state_lock = Lock()
        self._session: TradingSession | None = None
        self._callback_failure: BaseException | None = None
        self._subscription: int | None = None
        self._opened = False

    async def open(self) -> None:
        if self._opened or self._subscription is not None:
            raise RuntimeError("QMT quote runtime is already open")
        instant = to_utc(self._now(), name="QMT quote open time")
        session = await self._calendar(to_shanghai(instant).date(), instant)
        with self._state_lock:
            self._session = session
            self._callback_failure = None
        try:
            baseline = await asyncio.to_thread(
                self._client.get_full_tick,
                list(self._qmt_instruments),
            )
            baseline_received_at = to_utc(
                self._now(),
                name="QMT quote baseline receipt time",
            )
            if (
                to_shanghai(baseline_received_at).date()
                != session.session_date
            ):
                session = await self._calendar(
                    to_shanghai(baseline_received_at).date(),
                    baseline_received_at,
                )
                with self._state_lock:
                    self._session = session
            self._bridge.reset_from_full_tick(
                baseline,
                received_at=baseline_received_at,
                phase=self._phase(baseline_received_at),
                reset_id=f"xtdata-{uuid4()}",
            )
            subscription = await asyncio.to_thread(
                self._client.subscribe_whole_quote,
                list(self._qmt_instruments),
                self._capture,
            )
            if subscription <= 0:
                raise QuoteStreamUnavailableError(
                    "XtData full-quote subscription was rejected"
                )
        except Exception:
            self._bridge.disconnect(reason="qmt_quote_open_failed")
            raise
        self._subscription = subscription
        self._opened = True

    async def pump(self, *, stop: asyncio.Event) -> None:
        if not self._opened or self._subscription is None:
            raise QuoteStreamUnavailableError("QMT quote runtime is not open")
        session_date = self._phase_session_date()
        while not stop.is_set():
            instant = to_utc(self._now(), name="QMT quote pump time")
            current_date = to_shanghai(instant).date()
            if current_date != session_date:
                session = await self._calendar(current_date, instant)
                with self._state_lock:
                    self._session = session
                session_date = current_date
            failure = self._read_callback_failure()
            if failure is not None:
                raise QuoteStreamUnavailableError(
                    "XtData quote callback failed"
                ) from failure
            self._bridge.drain(limit=self._drain_limit)
            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=self._pump_interval.total_seconds(),
                )
            except TimeoutError:
                pass

    async def close(self) -> None:
        subscription = self._subscription
        self._subscription = None
        self._opened = False
        with self._state_lock:
            self._session = None
        self._bridge.disconnect(reason="qmt_quote_runtime_closed")
        if subscription is not None:
            try:
                await asyncio.to_thread(
                    self._client.unsubscribe_quote,
                    subscription,
                )
            except Exception:
                raise QuoteStreamUnavailableError(
                    "XtData quote unsubscribe failed"
                ) from None

    def _capture(self, payload: Mapping[str, object]) -> None:
        received_at = to_utc(self._now(), name="QMT quote callback time")
        try:
            self._bridge.capture(
                payload,
                phase=self._phase(received_at),
                received_at=received_at,
            )
        except BaseException as error:
            self._bridge.disconnect(reason="qmt_quote_callback_failed")
            with self._state_lock:
                if self._callback_failure is None:
                    self._callback_failure = error

    def _phase(self, instant: datetime) -> AShareTradingPhase:
        with self._state_lock:
            session = self._session
        if (
            session is None
            or session.session_date != to_shanghai(instant).date()
            or session.available_at > instant
        ):
            raise QuoteStreamUnavailableError(
                "QMT quote callback has no current trusted calendar"
            )
        return self._clock.phase(now=instant, session=session)

    def _phase_session_date(self) -> date:
        with self._state_lock:
            session = self._session
        if session is None:
            raise QuoteStreamUnavailableError(
                "QMT quote runtime has no calendar baseline"
            )
        return session.session_date

    def _read_callback_failure(self) -> BaseException | None:
        with self._state_lock:
            return self._callback_failure
