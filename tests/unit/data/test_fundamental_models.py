from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from autoquant.data.daily_models import TradingSession
from autoquant.data.fundamental_models import (
    DailyValuationRevision,
    FinancialIndicatorRevision,
    FundamentalDatasetBatch,
)
from autoquant.data.models import SourceEvidence


def valuation(**overrides: object) -> DailyValuationRevision:
    values: dict[str, object] = {
        "source": "tushare",
        "instrument": "600519.XSHG",
        "session_date": date(2026, 7, 20),
        "event_time": datetime(2026, 7, 20, 7, tzinfo=UTC),
        "available_at": datetime(2026, 7, 21, 1, 30, tzinfo=UTC),
        "ingested_at": datetime(2026, 7, 22, 8, tzinfo=UTC),
        "source_revision": "tushare:daily_basic:revision",
        "availability_policy": "tushare-daily-v1",
        "evidence_hash": "a" * 64,
        "close_price": "1500",
        "free_float_turnover_rate_percent": "0.3",
        "pe_ttm": "22",
        "pb": "8",
        "ps_ttm": "12",
        "dividend_yield_ttm_percent": "2",
        "total_market_value_cny": "1900000000000",
        "circulating_market_value_cny": "1900000000000",
    }
    values.update(overrides)
    return DailyValuationRevision.from_values(**values)  # type: ignore[arg-type]


def indicator(**overrides: object) -> FinancialIndicatorRevision:
    values: dict[str, object] = {
        "source": "tushare",
        "instrument": "600519.XSHG",
        "report_period": date(2026, 3, 31),
        "announced_date": date(2026, 4, 25),
        "updated": False,
        "event_time": datetime(2026, 4, 25, 7, tzinfo=UTC),
        "available_at": datetime(2026, 4, 27, 1, 30, tzinfo=UTC),
        "ingested_at": datetime(2026, 7, 22, 8, tzinfo=UTC),
        "source_revision": "tushare:fina_indicator:revision",
        "availability_policy": "tushare-daily-v1",
        "evidence_hash": "b" * 64,
        "roe_diluted_percent": "8.2",
        "roa_percent": "6.1",
        "gross_profit_margin_percent": "90",
        "debt_to_assets_percent": "18",
        "operating_cashflow_to_revenue_percent": "45",
    }
    values.update(overrides)
    return FinancialIndicatorRevision.from_values(**values)  # type: ignore[arg-type]


def evidence(method: str, marker: bytes) -> SourceEvidence:
    return SourceEvidence(
        source="tushare",
        method=method,
        requested_at=datetime(2026, 7, 22, 8, tzinfo=UTC),
        response_body=marker,
        response_hash=hashlib.sha256(marker).hexdigest(),
    )


def test_fundamental_revisions_are_canonical_and_preserve_nulls() -> None:
    first = valuation(pe_ttm=None)
    second = valuation(pe_ttm=None)
    financial = indicator(operating_cashflow_to_revenue_percent=None)

    assert first == second
    assert first.content_hash == second.content_hash
    assert first.pe_ttm is None
    assert financial.operating_cashflow_to_revenue_percent is None
    assert financial.content_hash == indicator(
        operating_cashflow_to_revenue_percent=None
    ).content_hash


def test_valuation_rejects_impossible_market_value_relationship() -> None:
    with pytest.raises(ValueError, match="circulating_market_value"):
        valuation(
            total_market_value_cny="100",
            circulating_market_value_cny="101",
        )


def test_indicator_rejects_lookback_dated_announcement() -> None:
    with pytest.raises(ValueError, match="announced_date"):
        indicator(announced_date=date(2026, 3, 1))


def test_fundamental_batch_requires_evidence_for_every_record() -> None:
    calendar = evidence("trade_cal", b"calendar")
    daily = evidence("daily_basic", b"daily")
    financial = evidence("fina_indicator", b"financial")
    value = valuation(evidence_hash=daily.response_hash)
    metric = indicator(evidence_hash=financial.response_hash)
    session = TradingSession(
        source="tushare",
        session_date=date(2026, 7, 20),
        is_open=True,
        available_at=calendar.requested_at,
        response_hash=calendar.response_hash,
    )

    batch = FundamentalDatasetBatch(
        valuations=(value,),
        indicators=(metric,),
        sessions=(session,),
        source_evidence=(calendar, daily, financial),
    )

    assert batch.valuations == (value,)
    with pytest.raises(ValueError, match="daily_basic evidence"):
        FundamentalDatasetBatch(
            valuations=(valuation(),),
            indicators=(metric,),
            sessions=(session,),
            source_evidence=(calendar, daily, financial),
        )


def test_numeric_types_are_explicit_decimals() -> None:
    value = valuation()

    assert value.close_price == Decimal("1500")
    assert value.total_market_value_cny == Decimal("1900000000000")
