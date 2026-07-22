from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from autoquant.data.daily_models import (
    AdjustmentFactorRevision,
    DailyBarRevision,
    DailyCoverageEvidence,
    DailyDatasetBatch,
    DailySuspensionStatus,
    InstrumentLifecycle,
    TradingSession,
)
from autoquant.data.models import SourceEvidence

REQUESTED_AT = datetime(2026, 7, 22, 8, 0, tzinfo=UTC)
EVENT_TIME = datetime(2026, 7, 20, 7, 0, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 7, 21, 1, 30, tzinfo=UTC)
INGESTED_AT = datetime(2026, 7, 22, 8, 1, tzinfo=UTC)


def evidence(method: str) -> SourceEvidence:
    body = f'{method}:{{"code":0}}'.encode()
    return SourceEvidence(
        source="tushare",
        method=method,
        requested_at=REQUESTED_AT,
        response_body=body,
        response_hash=hashlib.sha256(body).hexdigest(),
    )


def bar(daily_evidence: SourceEvidence, **updates: object) -> DailyBarRevision:
    values: dict[str, object] = {
        "source": "tushare",
        "instrument": "000001.XSHE",
        "session_date": date(2026, 7, 20),
        "event_time": EVENT_TIME,
        "available_at": AVAILABLE_AT,
        "ingested_at": INGESTED_AT,
        "source_revision": "tushare:daily:revision-1",
        "availability_policy": "tushare-daily-v1",
        "evidence_hash": daily_evidence.response_hash,
        "open_price": "10.00",
        "high_price": "10.20",
        "low_price": "9.90",
        "close_price": "10.10",
        "pre_close": "9.95",
        "volume": 12300,
        "turnover": "124230.00",
    }
    values.update(updates)
    return DailyBarRevision.from_values(**values)  # type: ignore[arg-type]


def factor(factor_evidence: SourceEvidence, **updates: object) -> AdjustmentFactorRevision:
    values: dict[str, object] = {
        "source": "tushare",
        "instrument": "000001.XSHE",
        "session_date": date(2026, 7, 20),
        "event_time": EVENT_TIME,
        "available_at": AVAILABLE_AT,
        "ingested_at": INGESTED_AT,
        "source_revision": "tushare:adj-factor:revision-1",
        "availability_policy": "tushare-daily-v1",
        "evidence_hash": factor_evidence.response_hash,
        "factor": "123.456789",
    }
    values.update(updates)
    return AdjustmentFactorRevision.from_values(**values)  # type: ignore[arg-type]


def coverage(
    trade_evidence: SourceEvidence,
    basic_evidence: SourceEvidence,
    suspension_evidence: SourceEvidence,
) -> DailyCoverageEvidence:
    return DailyCoverageEvidence(
        sessions=(
            TradingSession(
                source="tushare",
                session_date=date(2026, 7, 20),
                is_open=True,
                available_at=REQUESTED_AT,
                response_hash=trade_evidence.response_hash,
            ),
            TradingSession(
                source="tushare",
                session_date=date(2026, 7, 21),
                is_open=True,
                available_at=REQUESTED_AT,
                response_hash=trade_evidence.response_hash,
            ),
        ),
        lifecycles=(
            InstrumentLifecycle(
                source="tushare",
                instrument="000001.XSHE",
                list_date=date(1991, 4, 3),
                delist_date=None,
                available_at=REQUESTED_AT,
                response_hash=basic_evidence.response_hash,
            ),
        ),
        suspensions=(
            DailySuspensionStatus(
                source="tushare",
                instrument="000001.XSHE",
                session_date=date(2026, 7, 20),
                suspended=False,
                available_at=REQUESTED_AT,
                response_hash=suspension_evidence.response_hash,
            ),
        ),
    )


def test_daily_bar_hash_is_decimal_canonical_and_ingestion_independent() -> None:
    daily = evidence("daily")

    first = bar(daily, open_price=Decimal("10.0000"))
    second = bar(
        daily,
        open_price=Decimal("10"),
        ingested_at=datetime(2026, 7, 22, 9, 0, tzinfo=UTC),
    )

    assert first.content_hash == second.content_hash
    assert first.volume == 12300
    assert first.turnover == Decimal("124230.00")


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"low_price": "10.11"}, "low_price"),
        ({"high_price": "10.09"}, "high_price"),
        ({"pre_close": "0"}, "prices"),
        ({"volume": -1}, "volume"),
        ({"volume": True}, "volume"),
        ({"turnover": "-0.01"}, "turnover"),
    ],
)
def test_daily_bar_rejects_invalid_values(
    updates: dict[str, object], message: str
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        bar(evidence("daily"), **updates)


def test_adjustment_factor_must_be_positive_and_finite() -> None:
    adj = evidence("adj_factor")

    assert factor(adj).factor == Decimal("123.456789")
    for invalid in ("0", "-1", "NaN", "Infinity"):
        with pytest.raises(ValueError, match="factor"):
            factor(adj, factor=invalid)


def test_coverage_rejects_conflicting_session_values() -> None:
    trade = evidence("trade_cal")
    session = TradingSession(
        source="tushare",
        session_date=date(2026, 7, 20),
        is_open=True,
        available_at=REQUESTED_AT,
        response_hash=trade.response_hash,
    )

    with pytest.raises(ValueError, match="conflicting coverage"):
        DailyCoverageEvidence(
            sessions=(
                session,
                TradingSession(
                    source=session.source,
                    session_date=session.session_date,
                    is_open=False,
                    available_at=session.available_at,
                    response_hash=session.response_hash,
                ),
            ),
            lifecycles=(),
            suspensions=(),
        )


def test_lifecycle_rejects_delisting_before_listing() -> None:
    with pytest.raises(ValueError, match="delist_date"):
        InstrumentLifecycle(
            source="tushare",
            instrument="000001.XSHE",
            list_date=date(2020, 1, 2),
            delist_date=date(2020, 1, 1),
            available_at=REQUESTED_AT,
            response_hash=evidence("stock_basic").response_hash,
        )


def test_dataset_batch_requires_records_to_be_backed_by_matching_methods() -> None:
    source_evidence = tuple(
        evidence(method)
        for method in ("daily", "adj_factor", "trade_cal", "stock_basic", "suspend_d")
    )
    daily, adj, trade, basic, suspend = source_evidence

    batch = DailyDatasetBatch(
        bars=(bar(daily),),
        factors=(factor(adj),),
        coverage=coverage(trade, basic, suspend),
        source_evidence=source_evidence,
    )

    assert batch.bars[0].evidence_hash == daily.response_hash
    assert batch.factors[0].evidence_hash == adj.response_hash


def test_dataset_batch_rejects_wrong_evidence_method() -> None:
    wrong = evidence("adj_factor")

    with pytest.raises(ValueError, match="daily evidence"):
        DailyDatasetBatch(
            bars=(bar(wrong),),
            factors=(),
            coverage=DailyCoverageEvidence((), (), ()),
            source_evidence=(wrong,),
        )


def test_dataset_batch_rejects_duplicate_bar_key() -> None:
    daily = evidence("daily")

    with pytest.raises(ValueError, match="duplicate daily bar"):
        DailyDatasetBatch(
            bars=(bar(daily), bar(daily, close_price="10.11")),
            factors=(),
            coverage=DailyCoverageEvidence((), (), ()),
            source_evidence=(daily,),
        )
