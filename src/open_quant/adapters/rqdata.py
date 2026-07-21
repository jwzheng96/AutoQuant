from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from itertools import pairwise
from typing import Any

import httpx

from open_quant.clock import SHANGHAI, to_shanghai, to_utc
from open_quant.config import RqdataCredentials
from open_quant.data.availability import HistoricalMinutePolicy
from open_quant.data.models import (
    CoverageBatch,
    MarketCoverageEvidence,
    MinuteBarBatch,
    MinuteBarRevision,
    SourceEvidence,
    SuspensionStatus,
    TradingPeriod,
)
from open_quant.errors import VendorAuthenticationError, VendorResponseError

_SOURCE = "rqdata"
_PRICE_FIELDS = ("open", "high", "low", "close", "volume", "total_turnover")
_PRICE_COLUMNS = frozenset(("order_book_id", "datetime", *_PRICE_FIELDS))
_PERIOD_COLUMNS = frozenset(("order_book_id", "date", "trading_hours"))
_MAX_ATTEMPTS = 3
_RETRY_BASE_SECONDS = 0.25
_VENDOR_REVISION_HEADERS = ("x-rqdata-revision", "etag")
_SENSITIVE_RESPONSE_HEADERS = frozenset(
    {
        "authorization",
        "cookie",
        "proxy-authenticate",
        "proxy-authorization",
        "set-cookie",
        "token",
        "www-authenticate",
    }
)


class RqdataHttpSource:
    """Read-only HTTP adapter for RQData's authenticated CSV API."""

    def __init__(
        self,
        *,
        credentials: RqdataCredentials,
        auth_url: str,
        api_url: str,
        availability: HistoricalMinutePolicy,
    ) -> None:
        username = credentials.username.strip()
        password = credentials.password.get_secret_value()
        if not username or not password.strip():
            raise ValueError("RQData credentials cannot be empty")
        self._validate_url(auth_url, name="auth_url")
        self._validate_url(api_url, name="api_url")

        self._credentials = credentials
        self._auth_url = auth_url
        self._api_url = api_url
        self._availability = availability
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0))
        self._token: str | None = None
        self._authentication_lock = asyncio.Lock()

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(auth_url={self._auth_url!r}, "
            f"api_url={self._api_url!r}, availability={self._availability!r})"
        )

    async def authenticate(self) -> None:
        """Authenticate once and retain the returned token only in memory."""
        if self._token is not None:
            return
        async with self._authentication_lock:
            if self._token is not None:
                return
            response = await self._post_with_retry(
                self._auth_url,
                payload={
                    "user_name": self._credentials.username,
                    "password": self._credentials.password.get_secret_value(),
                },
                headers=None,
                authenticating=True,
            )
            self._token = self._parse_auth_token(response.content)

    async def fetch_minute_bars(
        self,
        instruments: tuple[str, ...],
        start: datetime,
        end: datetime,
    ) -> MinuteBarBatch:
        normalized_instruments, start_utc, end_utc = self._validate_query(
            instruments, start, end
        )
        start_text = self._shanghai_datetime_text(start_utc)
        end_text = self._shanghai_datetime_text(end_utc)
        records: list[MinuteBarRevision] = []
        evidence: list[SourceEvidence] = []

        for instrument in normalized_instruments:
            response, response_evidence, received_at = await self._market_post(
                {
                    "method": "get_price",
                    "order_book_ids": [instrument],
                    "start_date": start_text,
                    "end_date": end_text,
                    "frequency": "1m",
                    "fields": list(_PRICE_FIELDS),
                    "adjust_type": "none",
                    "skip_suspended": True,
                    "market": "cn",
                }
            )
            source_revision = self._source_revision(
                response.headers, response_evidence.response_hash
            )
            records.extend(
                self._parse_price_rows(
                    response_evidence.response_body,
                    expected_instrument=instrument,
                    start=start_utc,
                    end=end_utc,
                    received_at=received_at,
                    source_revision=source_revision,
                )
            )
            evidence.append(response_evidence)

        return MinuteBarBatch(records=tuple(records), source_evidence=tuple(evidence))

    async def fetch_coverage_evidence(
        self,
        instruments: tuple[str, ...],
        start: datetime,
        end: datetime,
    ) -> CoverageBatch:
        normalized_instruments, start_utc, end_utc = self._validate_query(
            instruments, start, end
        )
        first_date = to_shanghai(start_utc).date()
        last_date = to_shanghai(end_utc).date()
        common_payload: dict[str, Any] = {
            "order_book_ids": list(normalized_instruments),
            "start_date": first_date.isoformat(),
            "end_date": last_date.isoformat(),
            "market": "cn",
        }

        _, period_evidence, periods_received_at = await self._market_post(
            {
                "method": "get_trading_periods",
                **common_payload,
                "frequency": "1m",
            }
        )
        periods = self._parse_period_rows(
            period_evidence.response_body,
            expected_instruments=frozenset(normalized_instruments),
            first_date=first_date,
            last_date=last_date,
            received_at=periods_received_at,
            response_hash=period_evidence.response_hash,
        )

        _, suspension_evidence, suspensions_received_at = await self._market_post(
            {"method": "is_suspended", **common_payload}
        )
        suspensions = self._parse_suspension_rows(
            suspension_evidence.response_body,
            instruments=normalized_instruments,
            first_date=first_date,
            last_date=last_date,
            received_at=suspensions_received_at,
            response_hash=suspension_evidence.response_hash,
        )

        return CoverageBatch(
            coverage=MarketCoverageEvidence(
                periods=tuple(periods),
                suspensions=tuple(suspensions),
            ),
            source_evidence=(period_evidence, suspension_evidence),
        )

    async def close(self) -> None:
        self._token = None
        await self._client.aclose()

    async def _market_post(
        self, payload: Mapping[str, Any]
    ) -> tuple[httpx.Response, SourceEvidence, datetime]:
        await self.authenticate()
        token = self._token
        if token is None:  # Defensive: authenticate either sets a token or raises.
            raise VendorAuthenticationError("RQData authentication returned no token")

        requested_at = datetime.now(UTC)
        response = await self._post_with_retry(
            self._api_url,
            payload=payload,
            headers={"token": token},
            authenticating=False,
        )
        received_at = datetime.now(UTC)
        body = response.content
        self._require_market_body(body)
        response_hash = hashlib.sha256(body).hexdigest()
        method = payload.get("method")
        if not isinstance(method, str) or not method:
            raise ValueError("market payload method is required")
        return (
            response,
            SourceEvidence(
                source=_SOURCE,
                method=method,
                requested_at=requested_at,
                response_body=body,
                response_hash=response_hash,
            ),
            received_at,
        )

    async def _post_with_retry(
        self,
        url: str,
        *,
        payload: Mapping[str, Any],
        headers: Mapping[str, str] | None,
        authenticating: bool,
    ) -> httpx.Response:
        for attempt in range(_MAX_ATTEMPTS):
            connection_failed = False
            request_failed = False
            try:
                response = await self._client.post(url, json=payload, headers=headers)
            except (httpx.ConnectError, httpx.ConnectTimeout):
                connection_failed = True
            except httpx.RequestError:
                request_failed = True

            if connection_failed:
                if attempt + 1 < _MAX_ATTEMPTS:
                    await asyncio.sleep(_RETRY_BASE_SECONDS * (2**attempt))
                    continue
                raise VendorResponseError("RQData connection failed after retries")
            if request_failed:
                raise VendorResponseError("RQData request failed")

            transient = response.status_code == 429 or 500 <= response.status_code < 600
            if transient and attempt + 1 < _MAX_ATTEMPTS:
                await asyncio.sleep(_RETRY_BASE_SECONDS * (2**attempt))
                continue
            if not 200 <= response.status_code < 300:
                if authenticating:
                    raise VendorAuthenticationError(
                        f"RQData authentication failed with status {response.status_code}"
                    )
                raise VendorResponseError(
                    f"RQData API failed with status {response.status_code}"
                )
            return response

        raise AssertionError("unreachable retry state")

    def _parse_price_rows(
        self,
        body: bytes,
        *,
        expected_instrument: str,
        start: datetime,
        end: datetime,
        received_at: datetime,
        source_revision: str,
    ) -> list[MinuteBarRevision]:
        rows = self._csv_rows(body, required_columns=_PRICE_COLUMNS)
        records: list[MinuteBarRevision] = []
        seen_times: set[datetime] = set()
        for row in rows:
            instrument = self._required_cell(row, "order_book_id")
            if instrument != expected_instrument:
                raise VendorResponseError("RQData returned an unexpected instrument")
            event_time = self._parse_shanghai_timestamp(
                self._required_cell(row, "datetime")
            )
            if event_time < start or event_time > end:
                raise VendorResponseError("RQData returned a timestamp outside query bounds")
            if event_time in seen_times:
                raise VendorResponseError("RQData returned a duplicate minute bar")
            seen_times.add(event_time)

            open_price = self._parse_decimal(row, "open")
            high_price = self._parse_decimal(row, "high")
            low_price = self._parse_decimal(row, "low")
            close_price = self._parse_decimal(row, "close")
            volume_decimal = self._parse_decimal(row, "volume")
            if volume_decimal != volume_decimal.to_integral_value():
                raise VendorResponseError("RQData volume must be an integer numeric value")
            turnover = self._parse_decimal(row, "total_turnover")
            try:
                records.append(
                    MinuteBarRevision(
                        source=_SOURCE,
                        instrument=instrument,
                        event_time=event_time,
                        published_at=None,
                        available_at=self._availability.assign(bar_end=event_time),
                        ingested_at=received_at,
                        source_revision=source_revision,
                        availability_policy=self._availability.version,
                        open_price=open_price,
                        high_price=high_price,
                        low_price=low_price,
                        close_price=close_price,
                        volume=int(volume_decimal),
                        turnover=turnover,
                    )
                )
            except (ValueError, InvalidOperation) as error:
                raise VendorResponseError("RQData returned invalid minute-bar values") from error
        return records

    def _parse_period_rows(
        self,
        body: bytes,
        *,
        expected_instruments: frozenset[str],
        first_date: date,
        last_date: date,
        received_at: datetime,
        response_hash: str,
    ) -> list[TradingPeriod]:
        rows = self._csv_rows(body, required_columns=_PERIOD_COLUMNS)
        periods: list[TradingPeriod] = []
        seen: set[tuple[str, date]] = set()
        for row in rows:
            instrument = self._required_cell(row, "order_book_id")
            if instrument not in expected_instruments:
                raise VendorResponseError("RQData returned an unexpected instrument")
            session_date = self._parse_date(self._required_cell(row, "date"))
            if not first_date <= session_date <= last_date:
                raise VendorResponseError("RQData returned a date outside query bounds")
            key = (instrument, session_date)
            if key in seen:
                raise VendorResponseError("RQData returned duplicate trading periods")
            seen.add(key)
            minute_ends = self._expand_trading_hours(
                session_date, self._required_cell(row, "trading_hours")
            )
            try:
                periods.append(
                    TradingPeriod(
                        source=_SOURCE,
                        instrument=instrument,
                        session_date=session_date,
                        minute_ends=minute_ends,
                        available_at=received_at,
                        response_hash=response_hash,
                    )
                )
            except ValueError as error:
                raise VendorResponseError("RQData returned invalid trading periods") from error
        return periods

    def _parse_suspension_rows(
        self,
        body: bytes,
        *,
        instruments: tuple[str, ...],
        first_date: date,
        last_date: date,
        received_at: datetime,
        response_hash: str,
    ) -> list[SuspensionStatus]:
        rows = self._csv_rows(body, required_columns=frozenset(("date", *instruments)))
        suspensions: list[SuspensionStatus] = []
        seen_dates: set[date] = set()
        for row in rows:
            session_date = self._parse_date(self._required_cell(row, "date"))
            if not first_date <= session_date <= last_date:
                raise VendorResponseError("RQData returned a date outside query bounds")
            if session_date in seen_dates:
                raise VendorResponseError("RQData returned duplicate suspension dates")
            seen_dates.add(session_date)
            for instrument in instruments:
                value = self._required_cell(row, instrument).casefold()
                if value not in {"true", "false"}:
                    raise VendorResponseError("RQData returned a non-boolean suspension value")
                suspensions.append(
                    SuspensionStatus(
                        source=_SOURCE,
                        instrument=instrument,
                        session_date=session_date,
                        suspended=value == "true",
                        available_at=received_at,
                        response_hash=response_hash,
                    )
                )
        return suspensions

    @classmethod
    def _csv_rows(
        cls, body: bytes, *, required_columns: frozenset[str]
    ) -> list[dict[str, str | None]]:
        cls._require_market_body(body)
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError as error:
            raise VendorResponseError("RQData returned non-UTF-8 CSV") from error
        reader = csv.DictReader(io.StringIO(text, newline=""))
        fieldnames = reader.fieldnames
        if fieldnames is None:
            raise VendorResponseError("RQData CSV has no header")
        if len(fieldnames) != len(set(fieldnames)):
            raise VendorResponseError("RQData CSV contains duplicate columns")
        if any(not field.strip() for field in fieldnames):
            raise VendorResponseError("RQData CSV contains a blank column")
        missing = required_columns.difference(fieldnames)
        if missing:
            raise VendorResponseError("RQData CSV is missing required columns")
        if set(fieldnames).difference(required_columns):
            raise VendorResponseError("RQData CSV contains unexpected columns")

        rows: list[dict[str, str | None]] = []
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise VendorResponseError("RQData CSV contains a malformed row")
            rows.append(row)
        return rows

    @staticmethod
    def _required_cell(row: Mapping[str, str | None], column: str) -> str:
        value = row.get(column)
        if value is None or not value.strip():
            raise VendorResponseError(f"RQData CSV contains an empty {column} value")
        return value.strip()

    @classmethod
    def _parse_decimal(cls, row: Mapping[str, str | None], column: str) -> Decimal:
        value = cls._required_cell(row, column)
        try:
            parsed = Decimal(value)
        except InvalidOperation as error:
            raise VendorResponseError("RQData returned a non-numeric value") from error
        if not parsed.is_finite():
            raise VendorResponseError("RQData returned a non-numeric value")
        return parsed

    @staticmethod
    def _parse_shanghai_timestamp(value: str) -> datetime:
        parsed = RqdataHttpSource._parse_exact_datetime(value, "%Y-%m-%d %H:%M:%S")
        return parsed.replace(tzinfo=SHANGHAI).astimezone(UTC)

    @staticmethod
    def _parse_date(value: str) -> date:
        return RqdataHttpSource._parse_exact_datetime(value, "%Y-%m-%d").date()

    @staticmethod
    def _parse_exact_datetime(value: str, format_string: str) -> datetime:
        try:
            parsed = datetime.strptime(value, format_string)
        except ValueError as error:
            raise VendorResponseError("RQData returned an invalid timestamp") from error
        if parsed.strftime(format_string) != value:
            raise VendorResponseError("RQData returned an invalid timestamp")
        return parsed

    @classmethod
    def _expand_trading_hours(cls, session_date: date, value: str) -> tuple[datetime, ...]:
        minute_ends: list[datetime] = []
        for raw_period in value.split(","):
            parts = raw_period.strip().split("-")
            if len(parts) != 2:
                raise VendorResponseError("RQData returned invalid trading hours")
            start_time = cls._parse_time(parts[0])
            end_time = cls._parse_time(parts[1])
            start = datetime.combine(session_date, start_time, tzinfo=SHANGHAI)
            end = datetime.combine(session_date, end_time, tzinfo=SHANGHAI)
            if end < start:
                raise VendorResponseError("RQData overnight trading hours are ambiguous")
            current = start
            while current <= end:
                minute_ends.append(current.astimezone(UTC))
                current += timedelta(minutes=1)
        if not minute_ends or any(
            current >= following
            for current, following in pairwise(minute_ends)
        ):
            raise VendorResponseError("RQData returned unordered trading hours")
        return tuple(minute_ends)

    @staticmethod
    def _parse_time(value: str) -> time:
        try:
            parsed = datetime.strptime(value, "%H:%M")
        except ValueError as error:
            raise VendorResponseError("RQData returned invalid trading hours") from error
        if parsed.strftime("%H:%M") != value:
            raise VendorResponseError("RQData returned invalid trading hours")
        return parsed.time()

    @staticmethod
    def _parse_auth_token(body: bytes) -> str:
        stripped = body.strip()
        if not stripped:
            raise VendorAuthenticationError("RQData authentication returned an empty token")
        RqdataHttpSource._reject_html(stripped, authenticating=True)
        try:
            text: str | None = stripped.decode("utf-8")
        except UnicodeDecodeError:
            text = None
        if text is None:
            raise VendorAuthenticationError(
                "RQData authentication returned an invalid token"
            )

        if text.startswith("{"):
            try:
                payload: Any | None = json.loads(text)
            except json.JSONDecodeError:
                payload = None
            if payload is None:
                raise VendorAuthenticationError(
                    "RQData authentication returned invalid JSON"
                )
            if not isinstance(payload, dict):
                raise VendorAuthenticationError(
                    "RQData authentication response has no token"
                )
            token_value = payload.get("token")
            if not isinstance(token_value, str):
                raise VendorAuthenticationError(
                    "RQData authentication response has no token"
                )
            token = token_value.strip()
        else:
            token = text.strip()
        if not token or any(character.isspace() for character in token):
            raise VendorAuthenticationError("RQData authentication returned an invalid token")
        return token

    @staticmethod
    def _source_revision(headers: httpx.Headers, response_hash: str) -> str:
        for header in _VENDOR_REVISION_HEADERS:
            revision = headers.get(header)
            if revision and revision.strip():
                digest = hashlib.sha256(revision.strip().encode("utf-8")).hexdigest()
                return f"rqdata:{digest}"
        safe_headers = sorted(
            (name.casefold(), value)
            for name, value in headers.multi_items()
            if name.casefold() not in _SENSITIVE_RESPONSE_HEADERS
        )
        payload = json.dumps(
            {"headers": safe_headers, "response_hash": response_hash},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"derived:{hashlib.sha256(payload).hexdigest()}"

    @staticmethod
    def _validate_query(
        instruments: tuple[str, ...], start: datetime, end: datetime
    ) -> tuple[tuple[str, ...], datetime, datetime]:
        if isinstance(instruments, (str, bytes)) or not instruments:
            raise ValueError("instruments cannot be empty")
        if any(not isinstance(item, str) or not item.strip() for item in instruments):
            raise ValueError("instruments must contain nonblank strings")
        normalized = tuple(item.strip() for item in instruments)
        if len(set(normalized)) != len(normalized):
            raise ValueError("instruments must be unique")
        start_utc = to_utc(start, name="start")
        end_utc = to_utc(end, name="end")
        if start_utc > end_utc:
            raise ValueError("start cannot follow end")
        return normalized, start_utc, end_utc

    @staticmethod
    def _validate_url(value: str, *, name: str) -> None:
        parsed = httpx.URL(value)
        if parsed.scheme != "https" or not parsed.host or parsed.userinfo:
            raise ValueError(f"{name} must be an HTTPS URL without embedded credentials")

    @staticmethod
    def _shanghai_datetime_text(value: datetime) -> str:
        return to_shanghai(value).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _require_market_body(body: bytes) -> None:
        if not body.strip():
            raise VendorResponseError("RQData returned an empty market response")
        RqdataHttpSource._reject_html(body.lstrip(), authenticating=False)

    @staticmethod
    def _reject_html(body: bytes, *, authenticating: bool) -> None:
        lowered = body[:256].lower()
        if lowered.startswith(b"<"):
            if authenticating:
                raise VendorAuthenticationError("RQData authentication returned HTML")
            raise VendorResponseError("RQData returned HTML instead of CSV")
