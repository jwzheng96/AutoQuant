from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from itertools import pairwise

import pytest

from autoquant.adapters.tushare import (
    TushareApiResult,
    TushareDailySource,
    from_tushare_code,
    to_tushare_code,
)
from autoquant.data.daily_quality import DailyQualityGate
from autoquant.data.models import SourceEvidence
from autoquant.errors import VendorPermissionError, VendorResponseError

NOW = datetime(2026, 7, 22, 8, 0, tzinfo=UTC)


class FakeClient:
    def __init__(self, responses: Mapping[str, list[object]]) -> None:
        self.responses = {key: list(values) for key, values in responses.items()}
        self.calls: list[tuple[str, dict[str, object], tuple[str, ...]]] = []
        self.counts: defaultdict[str, int] = defaultdict(int)

    async def post(
        self,
        api_name: str,
        *,
        params: Mapping[str, object],
        fields: tuple[str, ...],
    ) -> TushareApiResult:
        self.calls.append((api_name, dict(params), fields))
        if not self.responses.get(api_name):
            raise AssertionError(f"unexpected {api_name} call")
        value = self.responses[api_name].pop(0)
        if isinstance(value, Exception):
            raise value
        if not isinstance(value, list):
            raise AssertionError("fake rows must be a list")
        self.counts[api_name] += 1
        body = json.dumps(
            {"api_name": api_name, "call": self.counts[api_name], "rows": value},
            sort_keys=True,
            default=str,
        ).encode()
        evidence = SourceEvidence(
            source="tushare",
            method=api_name,
            requested_at=NOW,
            response_body=body,
            response_hash=hashlib.sha256(body).hexdigest(),
        )
        return TushareApiResult(rows=tuple(value), evidence=evidence)

    async def close(self) -> None:
        return None


def base_responses(
    *,
    daily: list[dict[str, object]] | None = None,
    factors: list[dict[str, object]] | None = None,
    suspend: list[dict[str, object]] | None = None,
) -> dict[str, list[object]]:
    return {
        "trade_cal": [
            [
                {"exchange": "SSE", "cal_date": "20260720", "is_open": "1"},
                {"exchange": "SSE", "cal_date": "20260721", "is_open": "1"},
            ]
        ],
        "stock_basic": [
            [
                {
                    "ts_code": "000001.SZ",
                    "list_status": "L",
                    "list_date": "19910403",
                    "delist_date": None,
                }
            ],
            [],
            [],
        ],
        "daily": [
            daily
            if daily is not None
            else [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20260720",
                    "open": 10,
                    "high": "10.20",
                    "low": "9.90",
                    "close": "10.10",
                    "pre_close": "9.95",
                    "vol": "123.45",
                    "amount": "100.125",
                }
            ]
        ],
        "adj_factor": [
            factors
            if factors is not None
            else [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20260720",
                    "adj_factor": "123.456",
                }
            ]
        ],
        "suspend_d": [suspend if suspend is not None else []],
        "stk_limit": [
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20260720",
                    "pre_close": "9.95",
                    "up_limit": "10.95",
                    "down_limit": "8.96",
                }
            ]
        ],
    }


def source(client: FakeClient) -> TushareDailySource:
    return TushareDailySource(client=client, now=lambda: NOW)


@pytest.mark.parametrize(
    ("canonical", "vendor"),
    [
        ("000001.XSHE", "000001.SZ"),
        ("300001.XSHE", "300001.SZ"),
        ("600000.XSHG", "600000.SH"),
        ("688001.XSHG", "688001.SH"),
    ],
)
def test_symbol_mapping_is_bidirectional(canonical: str, vendor: str) -> None:
    assert to_tushare_code(canonical) == vendor
    assert from_tushare_code(vendor) == canonical


@pytest.mark.parametrize("value", ["000001", "000001.BJ", "000001.XBSE", "bad.SZ"])
def test_symbol_mapping_rejects_unsupported_or_malformed_values(value: str) -> None:
    with pytest.raises(ValueError, match=r"Tushare|AutoQuant"):
        if value.endswith((".SZ", ".SH", ".BJ")):
            from_tushare_code(value)
        else:
            to_tushare_code(value)


@pytest.mark.asyncio
async def test_fetch_maps_daily_units_factor_coverage_and_next_open_visibility() -> None:
    client = FakeClient(base_responses())

    batch = await source(client).fetch_daily_dataset(
        ("000001.XSHE",), date(2026, 7, 20), date(2026, 7, 20)
    )

    assert [call[0] for call in client.calls] == [
        "trade_cal",
        "stock_basic",
        "stock_basic",
        "stock_basic",
        "daily",
        "adj_factor",
        "suspend_d",
        "stk_limit",
    ]
    assert [call[1]["list_status"] for call in client.calls[1:4]] == ["L", "D", "P"]
    bar = batch.bars[0]
    assert bar.instrument == "000001.XSHE"
    assert bar.event_time == datetime(2026, 7, 20, 7, 0, tzinfo=UTC)
    assert bar.available_at == datetime(2026, 7, 21, 1, 30, tzinfo=UTC)
    assert bar.volume == 12345
    assert bar.turnover == Decimal("100125.000")
    assert batch.factors[0].factor == Decimal("123.456")
    assert batch.coverage.sessions[0].session_date == date(2026, 7, 20)
    assert batch.coverage.lifecycles[0].list_date == date(1991, 4, 3)
    assert batch.coverage.suspensions[0].suspended is False
    assert batch.coverage.price_limits[0].up_limit == Decimal("10.95")
    assert {value.method for value in batch.source_evidence} == {
        "daily",
        "adj_factor",
        "trade_cal",
        "stock_basic",
        "suspend_d",
        "stk_limit",
    }


@pytest.mark.asyncio
async def test_repeated_daily_interval_reuses_calendar_and_lifecycle_evidence() -> None:
    responses = base_responses()
    for method in ("daily", "adj_factor", "suspend_d", "stk_limit"):
        responses[method].append(responses[method][0])
    client = FakeClient(responses)
    adapter = source(client)

    await adapter.fetch_daily_dataset(
        ("000001.XSHE",), date(2026, 7, 20), date(2026, 7, 20)
    )
    await adapter.fetch_daily_dataset(
        ("000001.XSHE",), date(2026, 7, 20), date(2026, 7, 20)
    )

    methods = [call[0] for call in client.calls]
    assert methods.count("trade_cal") == 1
    assert methods.count("stock_basic") == 3
    assert methods.count("daily") == 2
    assert methods.count("adj_factor") == 2


@pytest.mark.asyncio
async def test_historical_limit_uses_matching_daily_pre_close_when_vendor_omits_it() -> None:
    responses = base_responses()
    limit_rows = responses["stk_limit"][0]
    assert isinstance(limit_rows, list)
    limit_rows[0]["pre_close"] = None

    batch = await source(FakeClient(responses)).fetch_daily_dataset(
        ("000001.XSHE",),
        date(2026, 7, 20),
        date(2026, 7, 20),
    )

    assert batch.coverage.price_limits[0].pre_close == Decimal("9.95")


@pytest.mark.asyncio
async def test_historical_limit_ignores_null_pre_close_without_a_daily_bar() -> None:
    responses = base_responses(
        daily=[],
        factors=[
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260720",
                "adj_factor": "123.456",
            }
        ],
        suspend=[
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260720",
                "suspend_type": "S",
                "suspend_timing": None,
            }
        ],
    )
    limit_rows = responses["stk_limit"][0]
    assert isinstance(limit_rows, list)
    limit_rows[0]["pre_close"] = None

    batch = await source(FakeClient(responses)).fetch_daily_dataset(
        ("000001.XSHE",),
        date(2026, 7, 20),
        date(2026, 7, 20),
    )

    assert batch.bars == ()
    assert batch.factors == ()
    assert batch.coverage.suspensions[0].suspended is True
    assert batch.coverage.price_limits == ()
    report = DailyQualityGate().evaluate(
        batch=batch,
        requested_instruments=("000001.XSHE",),
        start=date(2026, 7, 20),
        end=date(2026, 7, 20),
        as_of=NOW,
    )
    assert report.passed is True


@pytest.mark.asyncio
async def test_historical_limit_rejects_disagreement_with_daily_pre_close() -> None:
    responses = base_responses()
    limit_rows = responses["stk_limit"][0]
    assert isinstance(limit_rows, list)
    limit_rows[0]["pre_close"] = "9.94"

    with pytest.raises(VendorResponseError, match="disagree"):
        await source(FakeClient(responses)).fetch_daily_dataset(
            ("000001.XSHE",),
            date(2026, 7, 20),
            date(2026, 7, 20),
        )


@pytest.mark.asyncio
async def test_calendar_refresh_fetches_only_exact_trade_calendar_interval() -> None:
    client = FakeClient(
        {
            "trade_cal": [
                [
                    {"exchange": "SSE", "cal_date": "20260722", "is_open": "1"},
                    {"exchange": "SSE", "cal_date": "20260723", "is_open": "1"},
                ]
            ]
        }
    )

    batch = await source(client).fetch_trading_calendar(
        date(2026, 7, 22),
        date(2026, 7, 23),
    )

    assert [value.session_date for value in batch.sessions] == [
        date(2026, 7, 22),
        date(2026, 7, 23),
    ]
    assert [value.is_open for value in batch.sessions] == [True, True]
    assert len(batch.source_evidence) == 1
    assert [call[0] for call in client.calls] == ["trade_cal"]


@pytest.mark.asyncio
async def test_calendar_refresh_rejects_missing_calendar_dates() -> None:
    client = FakeClient(
        {
            "trade_cal": [
                [{"exchange": "SSE", "cal_date": "20260722", "is_open": "1"}]
            ]
        }
    )

    with pytest.raises(VendorResponseError, match="exactly cover"):
        await source(client).fetch_trading_calendar(
            date(2026, 7, 22),
            date(2026, 7, 23),
        )


@pytest.mark.asyncio
async def test_session_reference_fetches_controls_without_unfinished_daily_bar() -> None:
    client = FakeClient(
        {
            "trade_cal": [
                [{"exchange": "SSE", "cal_date": "20260723", "is_open": "1"}]
            ],
            "stock_basic": [
                [
                    {
                        "ts_code": "000001.SZ",
                        "list_status": "L",
                        "list_date": "19910403",
                        "delist_date": None,
                    }
                ]
            ],
            "suspend_d": [
                [
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": "20260722",
                        "suspend_type": "S",
                        "suspend_timing": None,
                    }
                ]
            ],
            "stk_limit": [
                [
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": "20260723",
                        "pre_close": "10",
                        "up_limit": "11",
                        "down_limit": "9",
                    }
                ]
            ],
        }
    )

    batch = await source(client).fetch_session_reference(
        ("000001.XSHE",),
        date(2026, 7, 23),
    )

    assert [call[0] for call in client.calls] == [
        "trade_cal",
        "stock_basic",
        "suspend_d",
        "stk_limit",
    ]
    assert batch.session.is_open is True
    assert batch.suspensions[0].suspended is True
    assert batch.price_limits[0].up_limit == Decimal("11")
    assert {value.method for value in batch.source_evidence} == {
        "trade_cal",
        "stock_basic",
        "suspend_d",
        "stk_limit",
    }


@pytest.mark.asyncio
async def test_session_reference_rejects_a_closed_session() -> None:
    client = FakeClient(
        {
            "trade_cal": [
                [{"exchange": "SSE", "cal_date": "20260725", "is_open": "0"}]
            ]
        }
    )

    with pytest.raises(VendorResponseError, match="open requested session"):
        await source(client).fetch_session_reference(
            ("000001.XSHE",),
            date(2026, 7, 25),
        )


@pytest.mark.asyncio
async def test_stock_basic_ignores_out_of_scope_bse_rows_before_symbol_mapping() -> None:
    responses = base_responses()
    listed = responses["stock_basic"][0]
    assert isinstance(listed, list)
    listed.append(
        {
            "ts_code": "920000.BJ",
            "list_status": "L",
            "list_date": "20240101",
            "delist_date": None,
        }
    )
    client = FakeClient(responses)

    batch = await source(client).fetch_daily_dataset(
        ("000001.XSHE",), date(2026, 7, 20), date(2026, 7, 20)
    )

    assert tuple(item.instrument for item in batch.coverage.lifecycles) == (
        "000001.XSHE",
    )


@pytest.mark.asyncio
async def test_fetch_sorts_descending_vendor_rows_and_tracks_suspension_interval() -> None:
    responses = base_responses(
        daily=[
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260721",
                "open": "10",
                "high": "10",
                "low": "10",
                "close": "10",
                "pre_close": "10",
                "vol": "1",
                "amount": "1",
            }
        ],
        factors=[
            {"ts_code": "000001.SZ", "trade_date": "20260721", "adj_factor": "1"}
        ],
        suspend=[
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260719",
                "suspend_type": "S",
                "suspend_timing": None,
            },
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260721",
                "suspend_type": "R",
                "suspend_timing": None,
            },
        ],
    )
    responses["trade_cal"] = [
        [
            {"exchange": "SSE", "cal_date": "20260722", "is_open": 1},
            {"exchange": "SSE", "cal_date": "20260721", "is_open": 1},
            {"exchange": "SSE", "cal_date": "20260720", "is_open": 1},
        ]
    ]
    client = FakeClient(responses)

    batch = await source(client).fetch_daily_dataset(
        ("000001.XSHE",), date(2026, 7, 20), date(2026, 7, 21)
    )

    assert [value.session_date for value in batch.bars] == [date(2026, 7, 21)]
    assert [(value.session_date, value.suspended) for value in batch.coverage.suspensions] == [
        (date(2026, 7, 20), True),
        (date(2026, 7, 21), False),
    ]


@pytest.mark.asyncio
async def test_daily_bar_resets_stale_suspension_without_resume_event() -> None:
    responses = base_responses(
        suspend=[
            {
                "ts_code": "000001.SZ",
                "trade_date": "20200101",
                "suspend_type": "S",
                "suspend_timing": None,
            },
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260719",
                "suspend_type": "S",
                "suspend_timing": "09:31-10:31",
            },
        ],
    )

    batch = await source(FakeClient(responses)).fetch_daily_dataset(
        ("000001.XSHE",),
        date(2026, 7, 20),
        date(2026, 7, 20),
    )

    assert batch.coverage.suspensions[0].suspended is False
    assert DailyQualityGate().evaluate(
        batch=batch,
        requested_instruments=("000001.XSHE",),
        start=date(2026, 7, 20),
        end=date(2026, 7, 20),
        as_of=NOW,
    ).passed is True


@pytest.mark.asyncio
async def test_daily_bar_rejects_same_day_full_suspension_conflict() -> None:
    responses = base_responses(
        suspend=[
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260720",
                "suspend_type": "S",
                "suspend_timing": None,
            }
        ],
    )

    with pytest.raises(VendorResponseError, match="conflicts"):
        await source(FakeClient(responses)).fetch_daily_dataset(
            ("000001.XSHE",),
            date(2026, 7, 20),
            date(2026, 7, 20),
        )


@pytest.mark.asyncio
async def test_fractional_normalized_shares_are_rejected_instead_of_rounded() -> None:
    responses = base_responses()
    daily = responses["daily"][0]
    assert isinstance(daily, list)
    daily[0]["vol"] = "0.001"

    with pytest.raises(VendorResponseError, match="whole shares"):
        await source(FakeClient(responses)).fetch_daily_dataset(
            ("000001.XSHE",), date(2026, 7, 20), date(2026, 7, 20)
        )


@pytest.mark.asyncio
async def test_rows_outside_request_are_rejected() -> None:
    responses = base_responses()
    daily = responses["daily"][0]
    assert isinstance(daily, list)
    daily[0]["ts_code"] = "600000.SH"

    with pytest.raises(VendorResponseError, match="outside request"):
        await source(FakeClient(responses)).fetch_daily_dataset(
            ("000001.XSHE",), date(2026, 7, 20), date(2026, 7, 20)
        )


@pytest.mark.asyncio
async def test_missing_next_open_session_fails_closed() -> None:
    responses = base_responses()
    responses["trade_cal"] = [
        [{"exchange": "SSE", "cal_date": "20260720", "is_open": "1"}]
    ]

    with pytest.raises(VendorResponseError, match="next open session"):
        await source(FakeClient(responses)).fetch_daily_dataset(
            ("000001.XSHE",), date(2026, 7, 20), date(2026, 7, 20)
        )


@pytest.mark.asyncio
async def test_capability_probe_reports_each_endpoint_without_short_circuiting() -> None:
    client = FakeClient(
        {
            "daily": [[]],
            "adj_factor": [VendorPermissionError("denied")],
            "trade_cal": [[]],
            "stock_basic": [VendorResponseError("bad response")],
            "suspend_d": [[]],
            "stk_limit": [[]],
        }
    )

    statuses = await source(client).probe_capabilities(
        instrument="000001.XSHE", session_date=date(2026, 7, 20)
    )

    assert statuses == {
        "adj_factor": "permission_denied",
        "daily": "available",
        "stock_basic": "error",
        "stk_limit": "available",
        "suspend_d": "available",
        "trade_cal": "available",
    }


def test_date_windows_are_contiguous_and_bounded() -> None:
    windows = TushareDailySource.date_windows(
        date(2010, 1, 1), date(2026, 7, 20), max_days=3650
    )

    assert windows[0][0] == date(2010, 1, 1)
    assert windows[-1][1] == date(2026, 7, 20)
    assert all((end - start).days < 3650 for start, end in windows)
    assert all(
        current[1].toordinal() + 1 == following[0].toordinal()
        for current, following in pairwise(windows)
    )
