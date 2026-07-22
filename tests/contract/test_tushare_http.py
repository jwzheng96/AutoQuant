from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable

import httpx
import pytest
import respx
from pydantic import SecretStr

from autoquant.adapters.tushare import TushareHttpClient
from autoquant.config import TushareCredentials
from autoquant.errors import (
    VendorAuthenticationError,
    VendorPermissionError,
    VendorRateLimitError,
    VendorResponseError,
)

API_URL = "https://tushare.example"
FAKE_TOKEN = "unit-test-secret-token"


async def no_sleep(_: float) -> None:
    return None


def make_client(
    *, sleep: Callable[[float], Awaitable[None]] = no_sleep
) -> TushareHttpClient:
    return TushareHttpClient(
        credentials=TushareCredentials(token=SecretStr(FAKE_TOKEN)),
        api_url=API_URL,
        retry_delays=(0.0, 0.0),
        sleep=sleep,
    )


def success_response(
    *, fields: list[str] | None = None, items: list[list[object]] | None = None
) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "code": 0,
            "msg": None,
            "data": {
                "fields": ["ts_code", "trade_date"] if fields is None else fields,
                "items": [["000001.SZ", "20260720"]] if items is None else items,
            },
        },
    )


@pytest.mark.asyncio
@respx.mock
async def test_post_uses_official_json_contract_and_maps_fields() -> None:
    route = respx.post(API_URL).mock(return_value=success_response())
    client = make_client()
    try:
        result = await client.post(
            "daily",
            params={"ts_code": "000001.SZ", "start_date": "20260720"},
            fields=("ts_code", "trade_date"),
        )
    finally:
        await client.close()

    assert json.loads(route.calls.last.request.content) == {
        "api_name": "daily",
        "token": FAKE_TOKEN,
        "params": {"ts_code": "000001.SZ", "start_date": "20260720"},
        "fields": "ts_code,trade_date",
    }
    assert result.rows == ({"ts_code": "000001.SZ", "trade_date": "20260720"},)
    assert result.evidence.source == "tushare"
    assert result.evidence.method == "daily"
    assert result.evidence.response_hash == hashlib.sha256(
        result.evidence.response_body
    ).hexdigest()
    assert FAKE_TOKEN.encode() not in result.evidence.response_body


@pytest.mark.asyncio
@respx.mock
async def test_response_mapping_uses_returned_field_names_and_preserves_empty_items() -> None:
    route = respx.post(API_URL).mock(
        side_effect=[
            success_response(
                fields=["trade_date", "ts_code"],
                items=[["20260720", "000001.SZ"]],
            ),
            success_response(fields=["ts_code"], items=[]),
        ]
    )
    client = make_client()
    try:
        reordered = await client.post("daily", params={}, fields=("ts_code",))
        empty = await client.post("daily", params={}, fields=())
    finally:
        await client.close()

    assert len(route.calls) == 2
    assert reordered.rows == (
        {"trade_date": "20260720", "ts_code": "000001.SZ"},
    )
    assert empty.rows == ()


@pytest.mark.asyncio
@respx.mock
async def test_evidence_identity_includes_credential_free_request_contract() -> None:
    respx.post(API_URL).mock(
        side_effect=[
            success_response(fields=["ts_code"], items=[]),
            success_response(fields=["ts_code"], items=[]),
        ]
    )
    client = make_client()
    try:
        listed = await client.post(
            "stock_basic", params={"list_status": "L"}, fields=("ts_code",)
        )
        delisted = await client.post(
            "stock_basic", params={"list_status": "D"}, fields=("ts_code",)
        )
    finally:
        await client.close()

    assert listed.evidence.response_hash != delisted.evidence.response_hash
    assert b'"list_status":"L"' in listed.evidence.response_body
    assert b'"list_status":"D"' in delisted.evidence.response_body
    assert FAKE_TOKEN.encode() not in listed.evidence.response_body
    assert FAKE_TOKEN.encode() not in delisted.evidence.response_body


@pytest.mark.asyncio
@respx.mock
@pytest.mark.parametrize(
    "payload",
    [
        {"code": 0, "msg": None},
        {"code": 0, "msg": None, "data": None},
        {"code": 0, "msg": None, "data": {"fields": "ts_code", "items": []}},
        {"code": 0, "msg": None, "data": {"fields": ["ts_code"], "items": {}}},
        {
            "code": 0,
            "msg": None,
            "data": {"fields": ["ts_code", "ts_code"], "items": [["a", "b"]]},
        },
        {
            "code": 0,
            "msg": None,
            "data": {"fields": ["ts_code"], "items": [["a", "extra"]]},
        },
    ],
)
async def test_malformed_success_payload_is_rejected(payload: object) -> None:
    respx.post(API_URL).mock(return_value=httpx.Response(200, json=payload))
    client = make_client()
    try:
        with pytest.raises(VendorResponseError, match="malformed"):
            await client.post("daily", params={}, fields=())
    finally:
        await client.close()


@pytest.mark.asyncio
@respx.mock
async def test_invalid_json_is_rejected_without_exposing_body() -> None:
    respx.post(API_URL).mock(
        return_value=httpx.Response(200, content=b"not-json unit-test-body")
    )
    client = make_client()
    try:
        with pytest.raises(VendorResponseError, match="invalid JSON") as caught:
            await client.post("daily", params={}, fields=())
    finally:
        await client.close()

    assert "unit-test-body" not in str(caught.value)


@pytest.mark.asyncio
@respx.mock
async def test_permission_code_is_stable_and_redacted() -> None:
    respx.post(API_URL).mock(
        return_value=httpx.Response(
            200,
            json={"code": 2002, "msg": f"denied {FAKE_TOKEN}", "data": None},
        )
    )
    client = make_client()
    try:
        with pytest.raises(VendorPermissionError, match="daily") as caught:
            await client.post("daily", params={}, fields=())
    finally:
        await client.close()

    assert FAKE_TOKEN not in str(caught.value)


@pytest.mark.asyncio
@respx.mock
async def test_unknown_business_code_is_not_retried() -> None:
    route = respx.post(API_URL).mock(
        return_value=httpx.Response(
            200, json={"code": 1234, "msg": "unknown", "data": None}
        )
    )
    client = make_client()
    try:
        with pytest.raises(VendorResponseError, match="code 1234"):
            await client.post("daily", params={}, fields=())
    finally:
        await client.close()

    assert len(route.calls) == 1


@pytest.mark.asyncio
@respx.mock
async def test_http_authentication_and_permission_errors_are_classified() -> None:
    route = respx.post(API_URL).mock(
        side_effect=[httpx.Response(401), httpx.Response(403)]
    )
    client = make_client()
    try:
        with pytest.raises(VendorAuthenticationError):
            await client.post("daily", params={}, fields=())
        with pytest.raises(VendorPermissionError):
            await client.post("daily", params={}, fields=())
    finally:
        await client.close()

    assert len(route.calls) == 2


@pytest.mark.asyncio
@respx.mock
async def test_retries_transient_http_statuses_then_succeeds() -> None:
    delays: list[float] = []

    async def record_sleep(delay: float) -> None:
        delays.append(delay)

    route = respx.post(API_URL).mock(
        side_effect=[httpx.Response(500), httpx.Response(429), success_response()]
    )
    client = make_client(sleep=record_sleep)
    try:
        result = await client.post("daily", params={}, fields=())
    finally:
        await client.close()

    assert result.rows
    assert len(route.calls) == 3
    assert delays == [0.0, 0.0]


@pytest.mark.asyncio
@respx.mock
async def test_exhausted_429_raises_rate_limit_error() -> None:
    route = respx.post(API_URL).mock(return_value=httpx.Response(429))
    client = make_client()
    try:
        with pytest.raises(VendorRateLimitError, match="rate limit"):
            await client.post("daily", params={}, fields=())
    finally:
        await client.close()

    assert len(route.calls) == 3


@pytest.mark.asyncio
@respx.mock
async def test_connection_failure_is_retried_but_bad_request_is_not() -> None:
    request = httpx.Request("POST", API_URL)
    route = respx.post(API_URL).mock(
        side_effect=[
            httpx.ConnectError("temporary", request=request),
            success_response(),
            httpx.Response(400),
        ]
    )
    client = make_client()
    try:
        assert (await client.post("daily", params={}, fields=())).rows
        with pytest.raises(VendorResponseError, match="status 400"):
            await client.post("daily", params={}, fields=())
    finally:
        await client.close()

    assert len(route.calls) == 3


def test_client_requires_https_and_repr_hides_token() -> None:
    credentials = TushareCredentials(token=SecretStr(FAKE_TOKEN))

    with pytest.raises(ValueError, match="HTTPS"):
        TushareHttpClient(credentials=credentials, api_url="http://tushare.example")

    client = TushareHttpClient(credentials=credentials, api_url=API_URL)
    try:
        assert FAKE_TOKEN not in repr(client)
    finally:
        # No request has been made; closing is covered by async tests.
        pass
