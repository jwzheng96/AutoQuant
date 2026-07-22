from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from autoquant.data.daily_ingestion import (
    DailyIngestionRequest,
    DailyIngestionService,
    ValidatedDailyDatasetReader,
)
from autoquant.data.daily_models import (
    AdjustmentFactorRevision,
    DailyBarRevision,
    DailyCoverageEvidence,
    DailyDatasetBatch,
    DailyPriceLimit,
    DailySuspensionStatus,
    InstrumentLifecycle,
    TradingSession,
)
from autoquant.data.daily_quality import DailyQualityGate
from autoquant.data.models import DatasetManifest, SourceEvidence
from autoquant.data.quality import QualityReport
from autoquant.errors import PersistenceUnavailableError

DAY = date(2026, 7, 20)
EVENT = datetime(2026, 7, 20, 7, 0, tzinfo=UTC)
AVAILABLE = datetime(2026, 7, 21, 1, 30, tzinfo=UTC)
AS_OF = datetime(2026, 7, 22, 8, 0, tzinfo=UTC)
INSTRUMENT = "000001.XSHE"


def evidence(method: str) -> SourceEvidence:
    body = method.encode()
    return SourceEvidence(
        source="tushare",
        method=method,
        requested_at=AS_OF,
        response_body=body,
        response_hash=hashlib.sha256(body).hexdigest(),
    )


def batch(*, complete: bool = True) -> DailyDatasetBatch:
    daily, adj, trade, basic, suspend, limit = (
        evidence(method)
        for method in (
            "daily",
            "adj_factor",
            "trade_cal",
            "stock_basic",
            "suspend_d",
            "stk_limit",
        )
    )
    bars = (
        DailyBarRevision.from_values(
            source="tushare",
            instrument=INSTRUMENT,
            session_date=DAY,
            event_time=EVENT,
            available_at=AVAILABLE,
            ingested_at=AS_OF,
            source_revision="daily-1",
            availability_policy="tushare-daily-v1",
            evidence_hash=daily.response_hash,
            open_price="10",
            high_price="10.2",
            low_price="9.9",
            close_price="10.1",
            pre_close="9.95",
            volume=100,
            turnover="1000",
        ),
    ) if complete else ()
    factors = (
        AdjustmentFactorRevision.from_values(
            source="tushare",
            instrument=INSTRUMENT,
            session_date=DAY,
            event_time=EVENT,
            available_at=AVAILABLE,
            ingested_at=AS_OF,
            source_revision="adj-1",
            availability_policy="tushare-daily-v1",
            evidence_hash=adj.response_hash,
            factor="123.4",
        ),
    ) if complete else ()
    return DailyDatasetBatch(
        bars=bars,
        factors=factors,
        coverage=DailyCoverageEvidence(
            sessions=(
                TradingSession(
                    "tushare", DAY, True, AS_OF, trade.response_hash
                ),
            ),
            lifecycles=(
                InstrumentLifecycle(
                    "tushare", INSTRUMENT, date(1991, 4, 3), None,
                    AS_OF, basic.response_hash
                ),
            ),
            suspensions=(
                DailySuspensionStatus(
                    "tushare", INSTRUMENT, DAY, False, AS_OF, suspend.response_hash
                ),
            ),
            price_limits=(
                DailyPriceLimit(
                    "tushare",
                    INSTRUMENT,
                    DAY,
                    Decimal("9.95"),
                    Decimal("10.95"),
                    Decimal("8.96"),
                    AS_OF,
                    limit.response_hash,
                ),
            ),
        ),
        source_evidence=(daily, adj, trade, basic, suspend, limit),
    )


class RecordingSource:
    def __init__(self, calls: list[str], dataset: DailyDatasetBatch) -> None:
        self.calls = calls
        self.dataset = dataset

    async def fetch_daily_dataset(
        self, instruments: tuple[str, ...], start: date, end: date
    ) -> DailyDatasetBatch:
        self.calls.append("fetch")
        return self.dataset


class RecordingMarket:
    def __init__(self, calls: list[str], *, fail_factors: bool = False) -> None:
        self.calls = calls
        self.fail_factors = fail_factors
        self.bars: tuple[DailyBarRevision, ...] = ()
        self.factors: tuple[AdjustmentFactorRevision, ...] = ()

    async def append_bars(self, records: tuple[DailyBarRevision, ...]) -> int:
        self.calls.append("append_bars")
        self.bars = records
        return len(records)

    async def append_factors(
        self, records: tuple[AdjustmentFactorRevision, ...]
    ) -> int:
        self.calls.append("append_factors")
        if self.fail_factors:
            raise PersistenceUnavailableError("factor append failed")
        self.factors = records
        return len(records)

    async def query_bars_as_of(
        self,
        instruments: tuple[str, ...],
        start: date,
        end: date,
        as_of: datetime,
    ) -> tuple[DailyBarRevision, ...]:
        self.calls.append("query_bars")
        return self.bars

    async def query_factors_as_of(
        self,
        instruments: tuple[str, ...],
        start: date,
        end: date,
        as_of: datetime,
    ) -> tuple[AdjustmentFactorRevision, ...]:
        self.calls.append("query_factors")
        return self.factors


class RecordingControl:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.manifests: dict[str, DatasetManifest] = {}
        self.reports: dict[str, QualityReport] = {}
        self.checkpoints: list[str] = []

    async def save_source_evidence(self, item: SourceEvidence) -> None:
        if not self.calls or self.calls[-1] != "save_evidence":
            self.calls.append("save_evidence")

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[RecordingControl]:
        self.calls.append("begin")
        yield self
        self.calls.append("commit")

    async def save_quality_report(self, report: QualityReport) -> None:
        self.calls.append("save_report")
        self.reports[report.report_hash] = report

    async def save_manifest(self, manifest: DatasetManifest) -> None:
        self.calls.append("save_manifest")
        self.manifests[manifest.manifest_hash] = manifest

    async def advance_checkpoint(
        self,
        source: str,
        stream: str,
        instrument: str,
        event_time: datetime,
        content_hash: str,
    ) -> None:
        self.calls.append(f"checkpoint:{stream}")
        self.checkpoints.append(stream)

    async def append_audit_event(
        self, event_type: str, occurred_at: datetime, payload: object
    ) -> str:
        self.calls.append(f"audit:{event_type}")
        return "f" * 64

    async def read_manifest(self, manifest_hash: str) -> DatasetManifest:
        return self.manifests[manifest_hash]

    async def read_quality_report(self, report_hash: str) -> QualityReport:
        return self.reports[report_hash]


def setup(
    *, complete: bool = True, fail_factors: bool = False
) -> tuple[
    DailyIngestionService,
    DailyIngestionRequest,
    list[str],
    RecordingMarket,
    RecordingControl,
]:
    calls: list[str] = []
    market = RecordingMarket(calls, fail_factors=fail_factors)
    control = RecordingControl(calls)
    service = DailyIngestionService(
        source=RecordingSource(calls, batch(complete=complete)),
        quality_gate=DailyQualityGate(),
        market_repository=market,
        control_repository=control,
        now=lambda: AS_OF,
    )
    request = DailyIngestionRequest(
        instruments=(INSTRUMENT,),
        start=DAY,
        end=DAY,
        as_of=AS_OF,
        production_complete_requested=True,
    )
    return service, request, calls, market, control


@pytest.mark.asyncio
async def test_complete_ingestion_finalizes_two_streams_atomically() -> None:
    service, request, calls, _, control = setup()

    result = await service.run(request)

    assert result.status == "completed"
    assert result.fetched_bars == 1
    assert result.fetched_factors == 1
    assert result.manifest_hash is not None
    assert control.checkpoints == ["daily", "adj-factor"]
    assert calls == [
        "fetch",
        "save_evidence",
        "append_bars",
        "append_factors",
        "begin",
        "save_report",
        "save_manifest",
        "checkpoint:daily",
        "checkpoint:adj-factor",
        "audit:daily_ingestion_completed",
        "commit",
    ]


@pytest.mark.asyncio
async def test_online_ingestion_captures_cutoff_after_fetch() -> None:
    service, request, calls, _, control = setup()
    request = DailyIngestionRequest(
        instruments=request.instruments,
        start=request.start,
        end=request.end,
        as_of=None,
        production_complete_requested=request.production_complete_requested,
    )

    result = await service.run(request)

    assert result.status == "completed"
    assert result.manifest_hash is not None
    assert control.manifests[result.manifest_hash].as_of == AS_OF
    assert calls[0] == "fetch"


@pytest.mark.asyncio
async def test_quality_rejection_keeps_raw_data_without_manifest_or_checkpoint() -> None:
    service, request, calls, _, control = setup(complete=False)

    result = await service.run(request)

    assert result.status == "quality_rejected"
    assert result.manifest_hash is None
    assert control.checkpoints == []
    assert "save_manifest" not in calls
    assert "audit:daily_ingestion_rejected" in calls


@pytest.mark.asyncio
async def test_partial_market_persistence_never_finalizes_metadata() -> None:
    service, request, calls, _, control = setup(fail_factors=True)

    result = await service.run(request)

    assert result.status == "persistence_failed"
    assert result.manifest_hash is None
    assert control.checkpoints == []
    assert "begin" not in calls


def test_request_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="instruments"):
        DailyIngestionRequest((), DAY, DAY, AS_OF, True)
    with pytest.raises(ValueError, match="start"):
        DailyIngestionRequest((INSTRUMENT,), DAY, date(2026, 7, 19), AS_OF, True)
    with pytest.raises(ValueError, match="as_of"):
        DailyIngestionRequest(
            (INSTRUMENT,), DAY, DAY, datetime(2026, 7, 20, 7, 0), True
        )


@pytest.mark.asyncio
async def test_validated_reader_returns_exact_manifest_streams() -> None:
    service, request, _, market, control = setup()
    result = await service.run(request)
    assert result.manifest_hash is not None
    reader = ValidatedDailyDatasetReader(
        control_repository=control, market_repository=market
    )

    dataset = await reader.query(result.manifest_hash, AS_OF)

    assert dataset.bars == market.bars
    assert dataset.factors == market.factors
    market.factors = ()
    with pytest.raises(PersistenceUnavailableError, match="manifest"):
        await reader.query(result.manifest_hash, AS_OF)
