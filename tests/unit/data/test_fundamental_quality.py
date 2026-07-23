from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime

from autoquant.data.daily_models import TradingSession
from autoquant.data.fundamental_models import (
    DailyValuationRevision,
    FundamentalDatasetBatch,
)
from autoquant.data.fundamental_quality import FundamentalQualityGate
from autoquant.data.models import SourceEvidence
from autoquant.data.quality import QualitySeverity


def _evidence(method: str) -> SourceEvidence:
    body = method.encode()
    return SourceEvidence(
        source="tushare",
        method=method,
        requested_at=datetime(2026, 7, 22, 8, tzinfo=UTC),
        response_body=body,
        response_hash=hashlib.sha256(body).hexdigest(),
    )


def test_market_value_mismatch_is_audited_without_rejecting_shard() -> None:
    calendar = _evidence("trade_cal")
    daily = _evidence("daily_basic")
    financial = _evidence("fina_indicator")
    value = DailyValuationRevision.from_values(
        source="tushare",
        instrument="600519.XSHG",
        session_date=date(2026, 7, 20),
        event_time=datetime(2026, 7, 20, 7, tzinfo=UTC),
        available_at=datetime(2026, 7, 21, 1, 30, tzinfo=UTC),
        ingested_at=datetime(2026, 7, 22, 8, tzinfo=UTC),
        source_revision="tushare:daily_basic:revision",
        availability_policy="tushare-daily-v1",
        evidence_hash=daily.response_hash,
        close_price="1500",
        free_float_turnover_rate_percent="0.3",
        pe_ttm="22",
        pb="8",
        ps_ttm="12",
        dividend_yield_ttm_percent="2",
        total_market_value_cny="100",
        circulating_market_value_cny="101",
    )
    session = TradingSession(
        source="tushare",
        session_date=value.session_date,
        is_open=True,
        available_at=calendar.requested_at,
        response_hash=calendar.response_hash,
    )
    report = FundamentalQualityGate().evaluate(
        batch=FundamentalDatasetBatch(
            valuations=(value,),
            indicators=(),
            sessions=(session,),
            source_evidence=(calendar, daily, financial),
        ),
        requested_instruments=(value.instrument,),
        start=value.session_date,
        end=value.session_date,
        as_of=datetime(2026, 7, 22, 8, tzinfo=UTC),
    )

    assert report.passed
    assert report.production_complete
    assert len(report.issues) == 1
    assert report.issues[0].severity is QualitySeverity.WARNING
    assert (
        report.issues[0].code
        == "circulating_market_value_exceeds_total"
    )
