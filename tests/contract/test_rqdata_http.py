import hashlib
import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx
from pydantic import SecretStr

from autoquant.adapters.rqdata import RqdataHttpSource
from autoquant.config import RqdataCredentials
from autoquant.data.availability import HistoricalMinutePolicy
from autoquant.errors import VendorAuthenticationError, VendorResponseError

AUTH_URL = "https://rqdata.example/auth"
API_URL = "https://rqdata.example/api"
START = datetime(2026, 7, 20, 1, 30, tzinfo=UTC)
END = datetime(2026, 7, 20, 1, 31, tzinfo=UTC)
PRICE_CSV = (
    b"order_book_id,datetime,open,high,low,close,volume,total_turnover\n"
    b"000001.XSHE,2026-07-20 09:31:00,10,10.1,9.9,10.05,1000,10050\n"
)


def make_source() -> RqdataHttpSource:
    return RqdataHttpSource(
        credentials=RqdataCredentials(
            username="u", password=SecretStr("super-secret-password")
        ),
        auth_url=AUTH_URL,
        api_url=API_URL,
        availability=HistoricalMinutePolicy("rqdata-minute-v1", timedelta(seconds=5)),
    )


def mock_json_auth() -> None:
    respx.post(AUTH_URL).mock(return_value=httpx.Response(200, json={"token": "secret-token"}))


@pytest.mark.asyncio
@respx.mock
async def test_fetch_uses_unadjusted_one_minute_contract_and_maps_csv() -> None:
    mock_json_auth()
    api = respx.post(API_URL).mock(
        return_value=httpx.Response(200, content=PRICE_CSV, headers={"x-request-id": "req-1"})
    )
    source = make_source()
    try:
        batch = await source.fetch_minute_bars(("000001.XSHE",), START, END)
    finally:
        await source.close()

    request = api.calls.last.request
    payload = json.loads(request.content)
    assert request.headers["token"] == "secret-token"
    assert payload == {
        "method": "get_price",
        "order_book_ids": ["000001.XSHE"],
        "start_date": "2026-07-20 09:30:00",
        "end_date": "2026-07-20 09:31:00",
        "frequency": "1m",
        "fields": ["open", "high", "low", "close", "volume", "total_turnover"],
        "adjust_type": "none",
        "skip_suspended": True,
        "market": "cn",
    }
    bar = batch.records[0]
    assert bar.instrument == "000001.XSHE"
    assert bar.event_time == datetime(2026, 7, 20, 1, 31, tzinfo=UTC)
    assert bar.available_at == datetime(2026, 7, 20, 1, 31, 5, tzinfo=UTC)
    assert bar.availability_policy == "rqdata-minute-v1"
    assert bar.source == "rqdata"
    assert bar.source_revision
    evidence = batch.source_evidence[0]
    assert evidence.response_body == PRICE_CSV
    assert evidence.response_hash == hashlib.sha256(PRICE_CSV).hexdigest()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_calls_get_price_once_per_instrument() -> None:
    mock_json_auth()
    api = respx.post(API_URL).mock(
        side_effect=[
            httpx.Response(200, content=PRICE_CSV),
            httpx.Response(
                200,
                text=(
                    "order_book_id,datetime,open,high,low,close,volume,total_turnover\n"
                    "600000.XSHG,2026-07-20 09:31:00,11,11.1,10.9,11.05,20.0,221\n"
                ),
            ),
        ]
    )
    source = make_source()
    try:
        batch = await source.fetch_minute_bars(
            ("000001.XSHE", "600000.XSHG"), START, END
        )
    finally:
        await source.close()

    assert len(api.calls) == 2
    assert [json.loads(call.request.content)["order_book_ids"] for call in api.calls] == [
        ["000001.XSHE"],
        ["600000.XSHG"],
    ]
    assert [record.instrument for record in batch.records] == [
        "000001.XSHE",
        "600000.XSHG",
    ]
    assert len(batch.source_evidence) == 2


@pytest.mark.asyncio
@respx.mock
async def test_coverage_uses_trading_periods_and_suspension_contracts() -> None:
    mock_json_auth()
    period_body = (
        b"order_book_id,date,trading_hours\n"
        b'000001.XSHE,2026-07-20,"09:31-11:30,13:01-15:00"\n'
    )
    suspension_body = b"date,000001.XSHE\n2026-07-20,False\n"
    api = respx.post(API_URL).mock(
        side_effect=[
            httpx.Response(200, content=period_body),
            httpx.Response(200, content=suspension_body),
        ]
    )
    source = make_source()
    try:
        batch = await source.fetch_coverage_evidence(
            ("000001.XSHE",),
            START,
            datetime(2026, 7, 20, 7, 0, tzinfo=UTC),
        )
    finally:
        await source.close()

    payloads = [json.loads(call.request.content) for call in api.calls]
    assert payloads == [
        {
            "method": "get_trading_periods",
            "order_book_ids": ["000001.XSHE"],
            "start_date": "2026-07-20",
            "end_date": "2026-07-20",
            "frequency": "1m",
            "market": "cn",
        },
        {
            "method": "is_suspended",
            "order_book_ids": ["000001.XSHE"],
            "start_date": "2026-07-20",
            "end_date": "2026-07-20",
            "market": "cn",
        },
    ]
    assert batch.coverage.suspensions[0].suspended is False
    period = batch.coverage.periods[0]
    assert period.minute_ends[0] == datetime(2026, 7, 20, 1, 31, tzinfo=UTC)
    assert period.minute_ends[-1] == datetime(2026, 7, 20, 7, 0, tzinfo=UTC)
    assert len(period.minute_ends) == 240
    assert period.available_at >= datetime(2026, 7, 20, 7, 0, tzinfo=UTC)
    assert len(batch.source_evidence) == 2
    assert batch.source_evidence[0].response_body == period_body
    assert batch.source_evidence[1].response_body == suspension_body


@pytest.mark.asyncio
@respx.mock
async def test_auth_supports_documented_raw_token_without_persisting_auth_body() -> None:
    auth_body = b"documented-raw-token\n"
    respx.post(AUTH_URL).mock(return_value=httpx.Response(200, content=auth_body))
    respx.post(API_URL).mock(return_value=httpx.Response(200, content=PRICE_CSV))
    source = make_source()
    try:
        batch = await source.fetch_minute_bars(("000001.XSHE",), START, END)
        representation = repr(source)
    finally:
        await source.close()

    assert batch.source_evidence[0].response_body == PRICE_CSV
    assert auth_body.strip() not in batch.source_evidence[0].response_body
    assert "documented-raw-token" not in representation
    assert "secret-token" not in representation
    assert "super-secret-password" not in representation


@pytest.mark.asyncio
@respx.mock
async def test_api_retries_only_transient_statuses() -> None:
    mock_json_auth()
    api = respx.post(API_URL).mock(
        side_effect=[
            httpx.Response(500),
            httpx.Response(429),
            httpx.Response(200, content=PRICE_CSV),
        ]
    )
    source = make_source()
    try:
        batch = await source.fetch_minute_bars(("000001.XSHE",), START, END)
    finally:
        await source.close()

    assert batch.records
    assert len(api.calls) == 3


@pytest.mark.asyncio
@respx.mock
async def test_api_retries_599_as_the_upper_5xx_boundary() -> None:
    mock_json_auth()
    api = respx.post(API_URL).mock(
        side_effect=[
            httpx.Response(599),
            httpx.Response(200, content=PRICE_CSV),
        ]
    )
    source = make_source()
    try:
        batch = await source.fetch_minute_bars(("000001.XSHE",), START, END)
    finally:
        await source.close()

    assert batch.records
    assert len(api.calls) == 2


@pytest.mark.asyncio
@respx.mock
async def test_api_does_not_retry_600_outside_the_5xx_range() -> None:
    mock_json_auth()
    api = respx.post(API_URL).mock(return_value=httpx.Response(600))
    source = make_source()
    try:
        with pytest.raises(VendorResponseError, match="status 600"):
            await source.fetch_minute_bars(("000001.XSHE",), START, END)
    finally:
        await source.close()

    assert len(api.calls) == 1


@pytest.mark.asyncio
@respx.mock
async def test_api_retries_connection_error_then_succeeds() -> None:
    mock_json_auth()
    request = httpx.Request("POST", API_URL)
    api = respx.post(API_URL).mock(
        side_effect=[
            httpx.ConnectError("temporary connection failure", request=request),
            httpx.Response(200, content=PRICE_CSV),
        ]
    )
    source = make_source()
    try:
        batch = await source.fetch_minute_bars(("000001.XSHE",), START, END)
    finally:
        await source.close()

    assert batch.records
    assert len(api.calls) == 2


@pytest.mark.asyncio
@respx.mock
async def test_api_stops_after_connection_retry_budget_is_exhausted() -> None:
    mock_json_auth()
    request = httpx.Request("POST", API_URL)
    api = respx.post(API_URL).mock(
        side_effect=[
            httpx.ConnectError("first connection failure", request=request),
            httpx.ConnectError("second connection failure", request=request),
            httpx.ConnectError("third connection failure", request=request),
        ]
    )
    source = make_source()
    try:
        with pytest.raises(VendorResponseError) as captured:
            await source.fetch_minute_bars(("000001.XSHE",), START, END)
    finally:
        await source.close()

    assert str(captured.value) == "RQData connection failed after retries"
    assert captured.value.__cause__ is None
    assert len(api.calls) == 3


@pytest.mark.asyncio
@respx.mock
async def test_api_does_not_retry_or_expose_non_connection_transport_errors() -> None:
    mock_json_auth()
    request = httpx.Request("POST", API_URL)
    api = respx.post(API_URL).mock(
        side_effect=httpx.ReadTimeout("sensitive transport detail", request=request)
    )
    source = make_source()
    try:
        with pytest.raises(VendorResponseError) as captured:
            await source.fetch_minute_bars(("000001.XSHE",), START, END)
    finally:
        await source.close()

    assert "sensitive transport detail" not in str(captured.value)
    assert captured.value.__cause__ is None
    assert len(api.calls) == 1


@pytest.mark.asyncio
@respx.mock
@pytest.mark.parametrize("status", [400, 401, 403])
async def test_api_does_not_retry_permanent_statuses(status: int) -> None:
    mock_json_auth()
    api = respx.post(API_URL).mock(return_value=httpx.Response(status))
    source = make_source()
    try:
        with pytest.raises(VendorResponseError, match=f"status {status}"):
            await source.fetch_minute_bars(("000001.XSHE",), START, END)
    finally:
        await source.close()

    assert len(api.calls) == 1


@pytest.mark.asyncio
@respx.mock
async def test_authentication_rejection_uses_safe_error_and_is_not_retried() -> None:
    auth = respx.post(AUTH_URL).mock(return_value=httpx.Response(401, text="secret detail"))
    source = make_source()
    try:
        with pytest.raises(VendorAuthenticationError) as captured:
            await source.authenticate()
    finally:
        await source.close()

    assert "secret detail" not in str(captured.value)
    assert len(auth.calls) == 1


@pytest.mark.asyncio
@respx.mock
async def test_malformed_authentication_body_is_not_retained_as_an_error_cause() -> None:
    respx.post(AUTH_URL).mock(
        return_value=httpx.Response(200, content=b'{"token":"secret",broken')
    )
    source = make_source()
    try:
        with pytest.raises(VendorAuthenticationError) as captured:
            await source.authenticate()
    finally:
        await source.close()

    assert "secret" not in str(captured.value)
    assert captured.value.__cause__ is None


@pytest.mark.asyncio
@respx.mock
@pytest.mark.parametrize(
    ("body", "message"),
    [
        (b"", "empty"),
        (b"<html>gateway</html>", "HTML"),
        (b"<body>gateway</body>", "HTML"),
        (
            b"order_book_id,datetime,open,open,high,low,close,volume,total_turnover\n",
            "duplicate",
        ),
        (
            b"order_book_id,datetime,open,high,low,close,volume,total_turnover,extra\n"
            b"000001.XSHE,2026-07-20 09:31:00,10,10.1,9.9,10.05,1000,10050,x\n",
            "unexpected columns",
        ),
        (
            b"order_book_id,datetime,open,high,low,close,volume,total_turnover\n"
            b"000001.XSHE,2026-07-20 09:31:00,nope,10.1,9.9,10.05,1000,10050\n",
            "numeric",
        ),
        (
            b"order_book_id,datetime,open,high,low,close,volume,total_turnover\n"
            b"600000.XSHG,2026-07-20 09:31:00,10,10.1,9.9,10.05,1000,10050\n",
            "unexpected instrument",
        ),
        (
            b"order_book_id,datetime,open,high,low,close,volume,total_turnover\n"
            b"000001.XSHE,2026-07-20,10,10.1,9.9,10.05,1000,10050\n",
            "timestamp",
        ),
    ],
)
async def test_market_csv_contract_failures_are_rejected(body: bytes, message: str) -> None:
    mock_json_auth()
    respx.post(API_URL).mock(return_value=httpx.Response(200, content=body))
    source = make_source()
    try:
        with pytest.raises(VendorResponseError, match=message):
            await source.fetch_minute_bars(("000001.XSHE",), START, END)
    finally:
        await source.close()


@pytest.mark.asyncio
@respx.mock
async def test_query_rejects_naive_bounds_before_authentication() -> None:
    auth = respx.post(AUTH_URL).mock(return_value=httpx.Response(200, text="token"))
    source = make_source()
    try:
        with pytest.raises(ValueError, match="timezone-aware"):
            await source.fetch_minute_bars(
                ("000001.XSHE",), datetime(2026, 7, 20, 9, 30), END
            )
    finally:
        await source.close()

    assert len(auth.calls) == 0


@pytest.mark.asyncio
@respx.mock
async def test_coverage_rejects_ambiguous_overnight_periods() -> None:
    mock_json_auth()
    respx.post(API_URL).mock(
        side_effect=[
            httpx.Response(
                200,
                text=(
                    "order_book_id,date,trading_hours\n"
                    '000001.XSHE,2026-07-20,"21:01-02:30"\n'
                ),
            ),
            httpx.Response(200, text="date,000001.XSHE\n2026-07-20,False\n"),
        ]
    )
    source = make_source()
    try:
        with pytest.raises(VendorResponseError, match="overnight"):
            await source.fetch_coverage_evidence(("000001.XSHE",), START, END)
    finally:
        await source.close()


@pytest.mark.asyncio
@respx.mock
async def test_vendor_revision_or_hash_bound_fallback_is_deterministic() -> None:
    mock_json_auth()
    respx.post(API_URL).mock(
        side_effect=[
            httpx.Response(200, content=PRICE_CSV, headers={"x-rqdata-revision": "rev-7"}),
            httpx.Response(200, content=PRICE_CSV, headers={"x-request-id": "request-8"}),
        ]
    )
    source = make_source()
    try:
        vendor = await source.fetch_minute_bars(("000001.XSHE",), START, END)
        derived = await source.fetch_minute_bars(("000001.XSHE",), START, END)
    finally:
        await source.close()

    assert vendor.records[0].source_revision.startswith("rqdata:")
    assert "rev-7" not in vendor.records[0].source_revision
    assert derived.records[0].source_revision.startswith("derived:")
    assert derived.records[0].source_revision != vendor.records[0].source_revision
