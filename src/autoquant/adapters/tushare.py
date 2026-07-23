from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from time import monotonic as monotonic_clock
from typing import Protocol
from urllib.parse import urlsplit

import httpx

from autoquant.clock import SHANGHAI, to_utc
from autoquant.config import TushareCredentials
from autoquant.data.daily_availability import NextTradingSessionOpenPolicy
from autoquant.data.daily_models import (
    AdjustmentFactorRevision,
    DailyBarRevision,
    DailyCoverageEvidence,
    DailyDatasetBatch,
    DailyPriceLimit,
    DailySuspensionStatus,
    InstrumentLifecycle,
    SessionReferenceBatch,
    TradingCalendarBatch,
    TradingSession,
)
from autoquant.data.models import SourceEvidence
from autoquant.data.universe import (
    DailyLiquidityMetric,
    IndexConstituent,
    IndexUniverseBatch,
)
from autoquant.errors import (
    AutoQuantError,
    VendorAuthenticationError,
    VendorPermissionError,
    VendorRateLimitError,
    VendorResponseError,
)

_SOURCE = "tushare"
_PERMISSION_CODE = 2002
_DEFAULT_RETRY_DELAYS = (0.25, 1.0, 2.0)
_CANONICAL_CODE = re.compile(r"^(?P<symbol>\d{6})\.(?P<exchange>XSHE|XSHG)$")
_TUSHARE_CODE = re.compile(r"^(?P<symbol>\d{6})\.(?P<exchange>SZ|SH)$")

Sleep = Callable[[float], Awaitable[None]]
Monotonic = Callable[[], float]


@dataclass(frozen=True, slots=True)
class TushareApiResult:
    rows: tuple[dict[str, object], ...]
    evidence: SourceEvidence


class TushareClient(Protocol):
    async def post(
        self,
        api_name: str,
        *,
        params: Mapping[str, object],
        fields: tuple[str, ...],
    ) -> TushareApiResult: ...

    async def close(self) -> None: ...


def to_tushare_code(instrument: str) -> str:
    match = _CANONICAL_CODE.fullmatch(instrument)
    if match is None:
        raise ValueError("invalid or unsupported AutoQuant instrument")
    suffix = "SZ" if match.group("exchange") == "XSHE" else "SH"
    return f"{match.group('symbol')}.{suffix}"


def from_tushare_code(ts_code: str) -> str:
    match = _TUSHARE_CODE.fullmatch(ts_code)
    if match is None:
        raise ValueError("invalid or unsupported Tushare instrument")
    suffix = "XSHE" if match.group("exchange") == "SZ" else "XSHG"
    return f"{match.group('symbol')}.{suffix}"


class TushareHttpClient:
    def __init__(
        self,
        *,
        credentials: TushareCredentials,
        api_url: str,
        timeout: float = 15.0,
        retry_delays: tuple[float, ...] = _DEFAULT_RETRY_DELAYS,
        min_request_interval: float = 1.25,
        monotonic: Monotonic = monotonic_clock,
        sleep: Sleep = asyncio.sleep,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not isinstance(credentials, TushareCredentials):
            raise TypeError("credentials must be TushareCredentials")
        parsed = urlsplit(api_url)
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ValueError("Tushare API URL must use HTTPS without embedded credentials")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if any(delay < 0 for delay in retry_delays):
            raise ValueError("retry delays cannot be negative")
        if min_request_interval < 0:
            raise ValueError("minimum request interval cannot be negative")
        self._credentials = credentials
        self._api_url = api_url.rstrip("/")
        self._retry_delays = tuple(retry_delays)
        self._min_request_interval = min_request_interval
        self._monotonic = monotonic
        self._sleep = sleep
        self._last_request_started: float | None = None
        self._rate_lock = asyncio.Lock()
        self._client = client or httpx.AsyncClient(timeout=timeout, follow_redirects=False)

    def __repr__(self) -> str:
        return f"TushareHttpClient(api_url={self._api_url!r})"

    async def post(
        self,
        api_name: str,
        *,
        params: Mapping[str, object],
        fields: tuple[str, ...],
    ) -> TushareApiResult:
        if not isinstance(api_name, str) or not api_name.strip():
            raise ValueError("api_name cannot be empty")
        if any(not isinstance(field, str) or not field.strip() for field in fields):
            raise ValueError("fields must contain nonempty strings")
        if len(set(fields)) != len(fields):
            raise ValueError("fields must be unique")
        requested_at = datetime.now(UTC)
        token = self._credentials.token.get_secret_value()
        payload: dict[str, object] = {
            "api_name": api_name,
            "token": token,
            "params": dict(params),
            "fields": ",".join(fields),
        }
        response = await self._post_with_retry(api_name, payload)
        parsed = self._parse_json(response, api_name=api_name)
        rows = self._parse_rows(parsed, api_name=api_name)
        evidence_body = self._evidence_body(
            api_name,
            parsed,
            token=token,
            params=params,
            fields=fields,
        )
        response_hash = hashlib.sha256(evidence_body).hexdigest()
        return TushareApiResult(
            rows=rows,
            evidence=SourceEvidence(
                source=_SOURCE,
                method=api_name,
                requested_at=requested_at,
                response_body=evidence_body,
                response_hash=response_hash,
            ),
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def _post_with_retry(
        self, api_name: str, payload: Mapping[str, object]
    ) -> httpx.Response:
        attempts = len(self._retry_delays) + 1
        for attempt in range(attempts):
            try:
                await self._wait_for_rate_limit()
                response = await self._client.post(self._api_url, json=payload)
            except httpx.TransportError:
                if attempt == attempts - 1:
                    raise VendorResponseError(
                        f"Tushare {api_name} transport failed after retries"
                    ) from None
                await self._sleep(self._retry_delays[attempt])
                continue

            if response.status_code == 429:
                if attempt == attempts - 1:
                    raise VendorRateLimitError(
                        f"Tushare {api_name} rate limit remained exhausted"
                    )
                await self._sleep(self._retry_delays[attempt])
                continue
            if 500 <= response.status_code <= 599:
                if attempt == attempts - 1:
                    raise VendorResponseError(
                        f"Tushare {api_name} server failed after retries"
                    )
                await self._sleep(self._retry_delays[attempt])
                continue
            if response.status_code == 401:
                raise VendorAuthenticationError(
                    f"Tushare {api_name} authentication failed"
                )
            if response.status_code == 403:
                raise VendorPermissionError(f"Tushare {api_name} permission denied")
            if response.status_code < 200 or response.status_code >= 300:
                raise VendorResponseError(
                    f"Tushare {api_name} returned HTTP status {response.status_code}"
                )
            return response
        raise AssertionError("retry loop exhausted without a result")

    async def _wait_for_rate_limit(self) -> None:
        async with self._rate_lock:
            now = self._monotonic()
            previous = self._last_request_started
            if previous is not None:
                remaining = self._min_request_interval - (now - previous)
                if remaining > 0:
                    await self._sleep(remaining)
                    now = self._monotonic()
            self._last_request_started = now

    @staticmethod
    def _parse_json(response: httpx.Response, *, api_name: str) -> dict[str, object]:
        try:
            payload = response.json()
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            raise VendorResponseError(
                f"Tushare {api_name} returned invalid JSON"
            ) from None
        if not isinstance(payload, dict) or any(
            not isinstance(key, str) for key in payload
        ):
            raise VendorResponseError(f"Tushare {api_name} returned malformed JSON")
        return payload

    @staticmethod
    def _parse_rows(
        payload: Mapping[str, object], *, api_name: str
    ) -> tuple[dict[str, object], ...]:
        code = payload.get("code")
        if not isinstance(code, int) or isinstance(code, bool):
            raise VendorResponseError(
                f"Tushare {api_name} returned malformed business status"
            )
        if code == _PERMISSION_CODE:
            raise VendorPermissionError(f"Tushare {api_name} permission denied")
        if code != 0:
            raise VendorResponseError(f"Tushare {api_name} returned business code {code}")

        data = payload.get("data")
        if not isinstance(data, dict):
            raise VendorResponseError(f"Tushare {api_name} returned malformed data")
        field_values = data.get("fields")
        item_values = data.get("items")
        if (
            not isinstance(field_values, list)
            or any(not isinstance(field, str) or not field for field in field_values)
            or len(set(field_values)) != len(field_values)
            or not isinstance(item_values, list)
        ):
            raise VendorResponseError(f"Tushare {api_name} returned malformed data")

        rows: list[dict[str, object]] = []
        for item in item_values:
            if not isinstance(item, list) or len(item) != len(field_values):
                raise VendorResponseError(f"Tushare {api_name} returned malformed row")
            rows.append(dict(zip(field_values, item, strict=True)))
        return tuple(rows)

    @classmethod
    def _evidence_body(
        cls,
        api_name: str,
        payload: Mapping[str, object],
        *,
        token: str,
        params: Mapping[str, object],
        fields: tuple[str, ...],
    ) -> bytes:
        normalized = {
            "api_name": api_name,
            "request": {
                "params": cls._redact(dict(params), token=token),
                "fields": list(fields),
            },
            "response": cls._redact(payload, token=token),
        }
        try:
            return json.dumps(
                normalized,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError):
            raise VendorResponseError(
                f"Tushare {api_name} returned malformed JSON values"
            ) from None

    @classmethod
    def _redact(cls, value: object, *, token: str) -> object:
        if isinstance(value, str):
            return value.replace(token, "***") if token else value
        if isinstance(value, list):
            return [cls._redact(item, token=token) for item in value]
        if isinstance(value, dict):
            return {
                str(key): cls._redact(item, token=token)
                for key, item in value.items()
            }
        return value


class TushareDailySource:
    DAILY_FIELDS = (
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "vol",
        "amount",
    )
    FACTOR_FIELDS = ("ts_code", "trade_date", "adj_factor")
    CALENDAR_FIELDS = ("exchange", "cal_date", "is_open")
    BASIC_FIELDS = ("ts_code", "list_status", "list_date", "delist_date")
    SUSPEND_FIELDS = ("ts_code", "trade_date", "suspend_type", "suspend_timing")
    LIMIT_FIELDS = ("ts_code", "trade_date", "pre_close", "up_limit", "down_limit")
    INDEX_WEIGHT_FIELDS = (
        "index_code",
        "con_code",
        "trade_date",
        "weight",
    )
    DAILY_BASIC_FIELDS = (
        "ts_code",
        "trade_date",
        "turnover_rate_f",
        "volume_ratio",
        "circ_mv",
    )

    def __init__(
        self,
        *,
        client: TushareClient,
        now: Callable[[], datetime],
        availability: NextTradingSessionOpenPolicy | None = None,
    ) -> None:
        self._client = client
        self._now = now
        self._availability = availability or NextTradingSessionOpenPolicy()

    async def close(self) -> None:
        await self._client.close()

    async def fetch_index_universe(
        self,
        *,
        index_code: str,
        reference_date: date,
    ) -> IndexUniverseBatch:
        constituent_lookback_start = (
            reference_date - timedelta(days=93)
        )
        weight_result = await self._client.post(
            "index_weight",
            params={
                "index_code": index_code,
                "start_date": self._date_text(
                    constituent_lookback_start
                ),
                "end_date": self._date_text(reference_date),
            },
            fields=self.INDEX_WEIGHT_FIELDS,
        )
        basic_result = await self._client.post(
            "daily_basic",
            params={
                "trade_date": self._date_text(reference_date),
            },
            fields=self.DAILY_BASIC_FIELDS,
        )
        constituents = self._map_index_constituents(
                weight_result,
                requested_index=index_code,
                reference_date=reference_date,
            )
        return IndexUniverseBatch(
            constituents=constituents,
            liquidity=self._map_daily_liquidity(
                basic_result,
                reference_date=reference_date,
                requested=frozenset(
                    value.instrument for value in constituents
                ),
            ),
            source_evidence=(
                weight_result.evidence,
                basic_result.evidence,
            ),
        )

    async def fetch_daily_dataset(
        self, instruments: tuple[str, ...], start: date, end: date
    ) -> DailyDatasetBatch:
        self._validate_request(instruments, start, end)
        vendor_codes = tuple(to_tushare_code(value) for value in instruments)
        evidence: list[SourceEvidence] = []

        calendar_result = await self._client.post(
            "trade_cal",
            params={
                "exchange": "SSE",
                "start_date": self._date_text(start),
                "end_date": self._date_text(end + timedelta(days=14)),
            },
            fields=self.CALENDAR_FIELDS,
        )
        evidence.append(calendar_result.evidence)
        sessions = self._map_sessions(calendar_result)

        lifecycles: list[InstrumentLifecycle] = []
        for status in ("L", "D", "P"):
            result = await self._client.post(
                "stock_basic",
                params={"exchange": "", "list_status": status},
                fields=self.BASIC_FIELDS,
            )
            evidence.append(result.evidence)
            lifecycles.extend(
                self._map_lifecycles(result, requested=frozenset(instruments))
            )
        lifecycle_by_instrument = {value.instrument: value for value in lifecycles}
        if len(lifecycle_by_instrument) != len(instruments):
            raise VendorResponseError("Tushare stock_basic omitted a requested instrument")
        if len(lifecycle_by_instrument) != len(lifecycles):
            raise VendorResponseError("Tushare stock_basic returned conflicting lifecycle rows")

        bars: list[DailyBarRevision] = []
        factors: list[AdjustmentFactorRevision] = []
        suspensions: list[DailySuspensionStatus] = []
        price_limits: list[DailyPriceLimit] = []
        for instrument, vendor_code in zip(instruments, vendor_codes, strict=True):
            for window_start, window_end in self.date_windows(start, end):
                daily_result = await self._client.post(
                    "daily",
                    params={
                        "ts_code": vendor_code,
                        "start_date": self._date_text(window_start),
                        "end_date": self._date_text(window_end),
                    },
                    fields=self.DAILY_FIELDS,
                )
                evidence.append(daily_result.evidence)
                bars.extend(
                    self._map_bars(
                        daily_result,
                        requested=instrument,
                        start=window_start,
                        end=window_end,
                        sessions=sessions,
                    )
                )

                factor_result = await self._client.post(
                    "adj_factor",
                    params={
                        "ts_code": vendor_code,
                        "start_date": self._date_text(window_start),
                        "end_date": self._date_text(window_end),
                    },
                    fields=self.FACTOR_FIELDS,
                )
                evidence.append(factor_result.evidence)
                factors.extend(
                    self._map_factors(
                        factor_result,
                        requested=instrument,
                        start=window_start,
                        end=window_end,
                        sessions=sessions,
                    )
                )

            suspension_result = await self._client.post(
                "suspend_d",
                params={"ts_code": vendor_code, "end_date": self._date_text(end)},
                fields=self.SUSPEND_FIELDS,
            )
            evidence.append(suspension_result.evidence)
            suspensions.extend(
                self._map_suspensions(
                    suspension_result,
                    requested=instrument,
                    start=start,
                    end=end,
                    sessions=sessions,
                )
            )
            limit_result = await self._client.post(
                "stk_limit",
                params={
                    "ts_code": vendor_code,
                    "start_date": self._date_text(start),
                    "end_date": self._date_text(end),
                },
                fields=self.LIMIT_FIELDS,
            )
            evidence.append(limit_result.evidence)
            price_limits.extend(
                self._map_price_limits(
                    limit_result,
                    requested=instrument,
                    start=start,
                    end=end,
                    bar_pre_closes={
                        value.session_date: value.pre_close
                        for value in bars
                        if value.instrument == instrument
                    },
                )
            )

        return DailyDatasetBatch(
            bars=tuple(sorted(bars, key=lambda value: (value.instrument, value.session_date))),
            factors=tuple(
                sorted(factors, key=lambda value: (value.instrument, value.session_date))
            ),
            coverage=DailyCoverageEvidence(
                sessions=tuple(sorted(sessions, key=lambda value: value.session_date)),
                lifecycles=tuple(
                    sorted(lifecycles, key=lambda value: value.instrument)
                ),
                suspensions=tuple(
                    sorted(
                        suspensions,
                        key=lambda value: (value.instrument, value.session_date),
                    )
                ),
                price_limits=tuple(
                    sorted(
                        price_limits,
                        key=lambda value: (value.instrument, value.session_date),
                    )
                ),
            ),
            source_evidence=tuple(evidence),
        )

    async def fetch_trading_calendar(
        self,
        start: date,
        end: date,
    ) -> TradingCalendarBatch:
        if start > end:
            raise ValueError("calendar start cannot follow end")
        if (end - start).days > 31:
            raise ValueError("calendar refresh cannot exceed 32 calendar days")
        result = await self._client.post(
            "trade_cal",
            params={
                "exchange": "SSE",
                "start_date": self._date_text(start),
                "end_date": self._date_text(end),
            },
            fields=self.CALENDAR_FIELDS,
        )
        sessions = self._map_sessions(result)
        expected_dates = {
            start + timedelta(days=offset)
            for offset in range((end - start).days + 1)
        }
        observed_dates = {value.session_date for value in sessions}
        if len(sessions) != len(observed_dates) or observed_dates != expected_dates:
            raise VendorResponseError(
                "Tushare trade_cal did not exactly cover the requested calendar interval"
            )
        return TradingCalendarBatch(
            sessions=tuple(sorted(sessions, key=lambda value: value.session_date)),
            source_evidence=(result.evidence,),
        )

    async def fetch_session_reference(
        self,
        instruments: tuple[str, ...],
        session_date: date,
    ) -> SessionReferenceBatch:
        """Fetch exact current-session controls without requesting an incomplete daily bar."""

        self._validate_request(instruments, session_date, session_date)
        day = self._date_text(session_date)
        calendar = await self._client.post(
            "trade_cal",
            params={
                "exchange": "SSE",
                "start_date": day,
                "end_date": day,
            },
            fields=self.CALENDAR_FIELDS,
        )
        sessions = self._map_sessions(calendar)
        if (
            len(sessions) != 1
            or sessions[0].session_date != session_date
            or not sessions[0].is_open
        ):
            raise VendorResponseError(
                "Tushare trade_cal does not prove an open requested session"
            )
        evidence = [calendar.evidence]
        lifecycles: list[InstrumentLifecycle] = []
        suspensions: list[DailySuspensionStatus] = []
        price_limits: list[DailyPriceLimit] = []
        for instrument in instruments:
            vendor_code = to_tushare_code(instrument)
            basic = await self._client.post(
                "stock_basic",
                params={"ts_code": vendor_code},
                fields=self.BASIC_FIELDS,
            )
            evidence.append(basic.evidence)
            mapped_lifecycles = self._map_lifecycles(
                basic,
                requested=frozenset((instrument,)),
            )
            if len(mapped_lifecycles) != 1:
                raise VendorResponseError(
                    "Tushare stock_basic did not exactly cover the requested instrument"
                )
            lifecycles.extend(mapped_lifecycles)

            suspension = await self._client.post(
                "suspend_d",
                params={"ts_code": vendor_code, "end_date": day},
                fields=self.SUSPEND_FIELDS,
            )
            evidence.append(suspension.evidence)
            mapped_suspensions = self._map_suspensions(
                suspension,
                requested=instrument,
                start=session_date,
                end=session_date,
                sessions=sessions,
            )
            if len(mapped_suspensions) != 1:
                raise VendorResponseError(
                    "Tushare suspend_d did not exactly cover the requested instrument"
                )
            suspensions.extend(mapped_suspensions)

            limit = await self._client.post(
                "stk_limit",
                params={"ts_code": vendor_code, "trade_date": day},
                fields=self.LIMIT_FIELDS,
            )
            evidence.append(limit.evidence)
            mapped_limits = self._map_price_limits(
                limit,
                requested=instrument,
                start=session_date,
                end=session_date,
            )
            if len(mapped_limits) != 1:
                raise VendorResponseError(
                    "Tushare stk_limit did not exactly cover the requested instrument"
                )
            price_limits.extend(mapped_limits)
        return SessionReferenceBatch(
            session=sessions[0],
            lifecycles=tuple(lifecycles),
            suspensions=tuple(suspensions),
            price_limits=tuple(price_limits),
            source_evidence=tuple(evidence),
        )

    async def probe_capabilities(
        self, *, instrument: str, session_date: date
    ) -> dict[str, str]:
        vendor_code = to_tushare_code(instrument)
        day = self._date_text(session_date)
        probes: tuple[tuple[str, dict[str, object], tuple[str, ...]], ...] = (
            ("daily", {"ts_code": vendor_code, "trade_date": day}, self.DAILY_FIELDS),
            (
                "adj_factor",
                {"ts_code": vendor_code, "trade_date": day},
                self.FACTOR_FIELDS,
            ),
            (
                "trade_cal",
                {"exchange": "SSE", "start_date": day, "end_date": day},
                self.CALENDAR_FIELDS,
            ),
            (
                "stock_basic",
                {"ts_code": vendor_code, "list_status": "L"},
                self.BASIC_FIELDS,
            ),
            (
                "suspend_d",
                {"ts_code": vendor_code, "trade_date": day},
                self.SUSPEND_FIELDS,
            ),
            (
                "stk_limit",
                {"ts_code": vendor_code, "trade_date": day},
                self.LIMIT_FIELDS,
            ),
        )
        statuses: dict[str, str] = {}
        for api_name, params, fields in probes:
            try:
                await self._client.post(api_name, params=params, fields=fields)
            except VendorPermissionError:
                statuses[api_name] = "permission_denied"
            except AutoQuantError:
                statuses[api_name] = "error"
            else:
                statuses[api_name] = "available"
        return dict(sorted(statuses.items()))

    @staticmethod
    def date_windows(
        start: date, end: date, *, max_days: int = 3650
    ) -> tuple[tuple[date, date], ...]:
        if start > end:
            raise ValueError("start cannot follow end")
        if max_days <= 0:
            raise ValueError("max_days must be positive")
        windows: list[tuple[date, date]] = []
        current = start
        while current <= end:
            window_end = min(end, current + timedelta(days=max_days - 1))
            windows.append((current, window_end))
            current = window_end + timedelta(days=1)
        return tuple(windows)

    @staticmethod
    def _validate_request(instruments: tuple[str, ...], start: date, end: date) -> None:
        if not instruments or len(set(instruments)) != len(instruments):
            raise ValueError("instruments must be nonempty and unique")
        if start > end:
            raise ValueError("start cannot follow end")
        for instrument in instruments:
            to_tushare_code(instrument)

    @classmethod
    def _map_sessions(cls, result: TushareApiResult) -> tuple[TradingSession, ...]:
        values: list[TradingSession] = []
        for row in result.rows:
            if cls._text(row, "exchange") != "SSE":
                raise VendorResponseError("Tushare trade_cal returned unexpected exchange")
            open_value = cls._text(row, "is_open")
            if open_value not in {"0", "1"}:
                raise VendorResponseError("Tushare trade_cal returned invalid is_open")
            values.append(
                TradingSession(
                    source=_SOURCE,
                    session_date=cls._date(row, "cal_date"),
                    is_open=open_value == "1",
                    available_at=result.evidence.requested_at,
                    response_hash=result.evidence.response_hash,
                )
            )
        return tuple(values)

    @classmethod
    def _map_lifecycles(
        cls, result: TushareApiResult, *, requested: frozenset[str]
    ) -> tuple[InstrumentLifecycle, ...]:
        values: list[InstrumentLifecycle] = []
        requested_vendor_codes = frozenset(to_tushare_code(value) for value in requested)
        for row in result.rows:
            vendor_code = cls._text(row, "ts_code")
            if vendor_code not in requested_vendor_codes:
                continue
            instrument = from_tushare_code(vendor_code)
            raw_delist = row.get("delist_date")
            delist_date = (
                None if raw_delist in (None, "") else cls._parse_date(str(raw_delist))
            )
            values.append(
                InstrumentLifecycle(
                    source=_SOURCE,
                    instrument=instrument,
                    list_date=cls._date(row, "list_date"),
                    delist_date=delist_date,
                    available_at=result.evidence.requested_at,
                    response_hash=result.evidence.response_hash,
                )
            )
        return tuple(values)

    @classmethod
    def _map_index_constituents(
        cls,
        result: TushareApiResult,
        *,
        requested_index: str,
        reference_date: date,
    ) -> tuple[IndexConstituent, ...]:
        values: list[IndexConstituent] = []
        for row in result.rows:
            index_code = cls._text(row, "index_code")
            trade_date = cls._date(row, "trade_date")
            if (
                index_code != requested_index
                or trade_date > reference_date
            ):
                raise VendorResponseError(
                    "Tushare index_weight returned a row outside request"
                )
            try:
                instrument = from_tushare_code(
                    cls._text(row, "con_code")
                )
            except ValueError:
                raise VendorResponseError(
                    "Tushare index_weight returned unsupported constituent"
                ) from None
            values.append(
                IndexConstituent(
                    source=_SOURCE,
                    index_code=index_code,
                    instrument=instrument,
                    trade_date=trade_date,
                    weight=cls._decimal(row, "weight"),
                    available_at=result.evidence.requested_at,
                    response_hash=result.evidence.response_hash,
                )
            )
        if len(
            {
                (value.trade_date, value.instrument)
                for value in values
            }
        ) != len(values):
            raise VendorResponseError(
                "Tushare index_weight returned duplicate constituents"
            )
        return tuple(values)

    @classmethod
    def _map_daily_liquidity(
        cls,
        result: TushareApiResult,
        *,
        reference_date: date,
        requested: frozenset[str],
    ) -> tuple[DailyLiquidityMetric, ...]:
        values: list[DailyLiquidityMetric] = []
        for row in result.rows:
            trade_date = cls._date(row, "trade_date")
            if trade_date != reference_date:
                raise VendorResponseError(
                    "Tushare daily_basic returned a row outside request"
                )
            try:
                instrument = from_tushare_code(
                    cls._text(row, "ts_code")
                )
            except ValueError:
                continue
            if instrument not in requested:
                continue
            raw_volume_ratio = row.get("volume_ratio")
            values.append(
                DailyLiquidityMetric(
                    source=_SOURCE,
                    instrument=instrument,
                    trade_date=trade_date,
                    turnover_rate_f=cls._decimal(
                        row,
                        "turnover_rate_f",
                    ),
                    volume_ratio=(
                        None
                        if raw_volume_ratio in (None, "")
                        else cls._decimal(row, "volume_ratio")
                    ),
                    circulating_market_value=cls._decimal(
                        row,
                        "circ_mv",
                    ),
                    available_at=result.evidence.requested_at,
                    response_hash=result.evidence.response_hash,
                )
            )
        if len({value.instrument for value in values}) != len(values):
            raise VendorResponseError(
                "Tushare daily_basic returned duplicate instruments"
            )
        return tuple(values)

    def _map_bars(
        self,
        result: TushareApiResult,
        *,
        requested: str,
        start: date,
        end: date,
        sessions: tuple[TradingSession, ...],
    ) -> tuple[DailyBarRevision, ...]:
        values: list[DailyBarRevision] = []
        for row in result.rows:
            instrument = from_tushare_code(self._text(row, "ts_code"))
            session_date = self._date(row, "trade_date")
            if instrument != requested or not start <= session_date <= end:
                raise VendorResponseError("Tushare daily returned a row outside request")
            volume_lots = self._decimal(row, "vol")
            shares = volume_lots * Decimal(100)
            if shares != shares.to_integral_value():
                raise VendorResponseError("Tushare daily volume is not whole shares")
            event_time = self._session_close(session_date)
            try:
                available_at = self._availability.assign(
                    session_date=session_date, sessions=sessions
                )
            except ValueError:
                raise VendorResponseError(
                    "Tushare trade_cal omitted the next open session"
                ) from None
            values.append(
                DailyBarRevision.from_values(
                    source=_SOURCE,
                    instrument=instrument,
                    session_date=session_date,
                    event_time=event_time,
                    available_at=available_at,
                    ingested_at=to_utc(self._now(), name="ingested_at"),
                    source_revision=f"tushare:daily:{result.evidence.response_hash}",
                    availability_policy=self._availability.version,
                    evidence_hash=result.evidence.response_hash,
                    open_price=self._decimal(row, "open"),
                    high_price=self._decimal(row, "high"),
                    low_price=self._decimal(row, "low"),
                    close_price=self._decimal(row, "close"),
                    pre_close=self._decimal(row, "pre_close"),
                    volume=int(shares),
                    turnover=self._decimal(row, "amount") * Decimal(1000),
                )
            )
        return tuple(values)

    def _map_factors(
        self,
        result: TushareApiResult,
        *,
        requested: str,
        start: date,
        end: date,
        sessions: tuple[TradingSession, ...],
    ) -> tuple[AdjustmentFactorRevision, ...]:
        values: list[AdjustmentFactorRevision] = []
        for row in result.rows:
            instrument = from_tushare_code(self._text(row, "ts_code"))
            session_date = self._date(row, "trade_date")
            if instrument != requested or not start <= session_date <= end:
                raise VendorResponseError("Tushare adj_factor returned a row outside request")
            try:
                available_at = self._availability.assign(
                    session_date=session_date, sessions=sessions
                )
            except ValueError:
                raise VendorResponseError(
                    "Tushare trade_cal omitted the next open session"
                ) from None
            values.append(
                AdjustmentFactorRevision.from_values(
                    source=_SOURCE,
                    instrument=instrument,
                    session_date=session_date,
                    event_time=self._session_close(session_date),
                    available_at=available_at,
                    ingested_at=to_utc(self._now(), name="ingested_at"),
                    source_revision=f"tushare:adj_factor:{result.evidence.response_hash}",
                    availability_policy=self._availability.version,
                    evidence_hash=result.evidence.response_hash,
                    factor=self._decimal(row, "adj_factor"),
                )
            )
        return tuple(values)

    @classmethod
    def _map_price_limits(
        cls,
        result: TushareApiResult,
        *,
        requested: str,
        start: date,
        end: date,
        bar_pre_closes: Mapping[date, Decimal] | None = None,
    ) -> tuple[DailyPriceLimit, ...]:
        values: list[DailyPriceLimit] = []
        for row in result.rows:
            instrument = from_tushare_code(cls._text(row, "ts_code"))
            session_date = cls._date(row, "trade_date")
            if instrument != requested or not start <= session_date <= end:
                raise VendorResponseError("Tushare stk_limit returned a row outside request")
            bar_pre_close = (
                None
                if bar_pre_closes is None
                else bar_pre_closes.get(session_date)
            )
            raw_pre_close = row.get("pre_close")
            if raw_pre_close is None:
                if bar_pre_close is None:
                    raise VendorResponseError(
                        "Tushare stk_limit returned invalid pre_close"
                    )
                pre_close = bar_pre_close
            else:
                pre_close = cls._decimal(row, "pre_close")
                if (
                    bar_pre_close is not None
                    and pre_close != bar_pre_close
                ):
                    raise VendorResponseError(
                        "Tushare daily and stk_limit pre_close disagree"
                    )
            values.append(
                DailyPriceLimit(
                    source=_SOURCE,
                    instrument=instrument,
                    session_date=session_date,
                    pre_close=pre_close,
                    up_limit=cls._decimal(row, "up_limit"),
                    down_limit=cls._decimal(row, "down_limit"),
                    available_at=result.evidence.requested_at,
                    response_hash=result.evidence.response_hash,
                )
            )
        return tuple(values)

    @classmethod
    def _map_suspensions(
        cls,
        result: TushareApiResult,
        *,
        requested: str,
        start: date,
        end: date,
        sessions: tuple[TradingSession, ...],
    ) -> tuple[DailySuspensionStatus, ...]:
        events: list[tuple[date, str, object]] = []
        for row in result.rows:
            instrument = from_tushare_code(cls._text(row, "ts_code"))
            if instrument != requested:
                raise VendorResponseError("Tushare suspend_d returned a row outside request")
            event_date = cls._date(row, "trade_date")
            if event_date > end:
                raise VendorResponseError("Tushare suspend_d returned a row outside request")
            event_type = cls._text(row, "suspend_type")
            if event_type not in {"S", "R"}:
                raise VendorResponseError("Tushare suspend_d returned invalid suspend_type")
            events.append((event_date, event_type, row.get("suspend_timing")))
        events.sort(key=lambda value: value[0])

        active = False
        event_index = 0
        values: list[DailySuspensionStatus] = []
        requested_sessions = sorted(
            value.session_date
            for value in sessions
            if value.is_open and start <= value.session_date <= end
        )
        for session_date in requested_sessions:
            while event_index < len(events) and events[event_index][0] <= session_date:
                _, event_type, timing = events[event_index]
                if event_type == "R":
                    active = False
                elif timing in (None, ""):
                    active = True
                event_index += 1
            values.append(
                DailySuspensionStatus(
                    source=_SOURCE,
                    instrument=requested,
                    session_date=session_date,
                    suspended=active,
                    available_at=result.evidence.requested_at,
                    response_hash=result.evidence.response_hash,
                )
            )
        return tuple(values)

    @staticmethod
    def _session_close(session_date: date) -> datetime:
        return to_utc(datetime.combine(session_date, time(15, 0), tzinfo=SHANGHAI))

    @staticmethod
    def _date_text(value: date) -> str:
        return value.strftime("%Y%m%d")

    @classmethod
    def _date(cls, row: Mapping[str, object], field: str) -> date:
        return cls._parse_date(cls._text(row, field))

    @staticmethod
    def _parse_date(value: str) -> date:
        try:
            return datetime.strptime(value, "%Y%m%d").date()
        except ValueError:
            raise VendorResponseError("Tushare returned an invalid date") from None

    @staticmethod
    def _text(row: Mapping[str, object], field: str) -> str:
        value = row.get(field)
        if value is None or isinstance(value, bool):
            raise VendorResponseError(f"Tushare returned invalid {field}")
        text = str(value).strip()
        if not text:
            raise VendorResponseError(f"Tushare returned invalid {field}")
        return text

    @staticmethod
    def _decimal(row: Mapping[str, object], field: str) -> Decimal:
        value = row.get(field)
        if value is None or isinstance(value, bool):
            raise VendorResponseError(f"Tushare returned invalid {field}")
        try:
            parsed = Decimal(str(value))
        except InvalidOperation:
            raise VendorResponseError(f"Tushare returned invalid {field}") from None
        if not parsed.is_finite():
            raise VendorResponseError(f"Tushare returned invalid {field}")
        return parsed
