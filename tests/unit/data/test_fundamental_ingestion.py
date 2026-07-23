from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime

import pytest

from autoquant.data.daily_models import TradingSession
from autoquant.data.fundamental_ingestion import (
    FundamentalIngestionRequest,
    FundamentalIngestionService,
)
from autoquant.data.fundamental_models import (
    DailyValuationRevision,
    FinancialIndicatorRevision,
    FundamentalDatasetBatch,
)
from autoquant.data.fundamental_quality import FundamentalQualityGate
from autoquant.data.models import (
    DatasetManifest,
    SourceEvidence,
)
from autoquant.data.quality import QualityReport
from autoquant.errors import PersistenceUnavailableError

DAY = date(2026, 7, 20)
EVENT = datetime(2026, 7, 20, 7, tzinfo=UTC)
AVAILABLE = datetime(2026, 7, 21, 1, 30, tzinfo=UTC)
AS_OF = datetime(2026, 7, 22, 8, tzinfo=UTC)
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


def batch(
    *,
    include_valuation: bool = True,
    include_indicator: bool = True,
) -> FundamentalDatasetBatch:
    calendar = evidence("trade_cal")
    daily = evidence("daily_basic")
    financial = evidence("fina_indicator")
    valuations = (
        (
            DailyValuationRevision.from_values(
                source="tushare",
                instrument=INSTRUMENT,
                session_date=DAY,
                event_time=EVENT,
                available_at=AVAILABLE,
                ingested_at=AS_OF,
                source_revision="daily-basic-1",
                availability_policy="tushare-daily-v1",
                evidence_hash=daily.response_hash,
                close_price="10",
                free_float_turnover_rate_percent="0.5",
                pe_ttm="6",
                pb="0.8",
                ps_ttm="1.1",
                dividend_yield_ttm_percent="2",
                total_market_value_cny="250000000000",
                circulating_market_value_cny="240000000000",
            ),
        )
        if include_valuation
        else ()
    )
    indicators = (
        (
            FinancialIndicatorRevision.from_values(
                source="tushare",
                instrument=INSTRUMENT,
                report_period=date(2026, 6, 30),
                announced_date=DAY,
                updated=False,
                event_time=EVENT,
                available_at=AVAILABLE,
                ingested_at=AS_OF,
                source_revision="fina-indicator-1",
                availability_policy="tushare-daily-v1",
                evidence_hash=financial.response_hash,
                roe_diluted_percent="5",
                roa_percent="1",
                gross_profit_margin_percent=None,
                debt_to_assets_percent="90",
                operating_cashflow_to_revenue_percent="12",
            ),
        )
        if include_indicator
        else ()
    )
    return FundamentalDatasetBatch(
        valuations=valuations,
        indicators=indicators,
        sessions=(
            TradingSession(
                source="tushare",
                session_date=DAY,
                is_open=True,
                available_at=AS_OF,
                response_hash=calendar.response_hash,
            ),
        ),
        source_evidence=(calendar, daily, financial),
    )


class RecordingSource:
    def __init__(
        self,
        calls: list[str],
        dataset: FundamentalDatasetBatch,
    ) -> None:
        self.calls = calls
        self.dataset = dataset

    async def fetch_fundamental_dataset(
        self,
        instruments: tuple[str, ...],
        start: date,
        end: date,
    ) -> FundamentalDatasetBatch:
        self.calls.append("fetch")
        return self.dataset


class RecordingFundamentals:
    def __init__(
        self,
        calls: list[str],
        *,
        fail_indicators: bool = False,
    ) -> None:
        self.calls = calls
        self.fail_indicators = fail_indicators

    async def append_valuations(
        self,
        records: tuple[DailyValuationRevision, ...],
    ) -> int:
        self.calls.append("append_valuations")
        return len(records)

    async def append_indicators(
        self,
        records: tuple[FinancialIndicatorRevision, ...],
    ) -> int:
        self.calls.append("append_indicators")
        if self.fail_indicators:
            raise PersistenceUnavailableError("indicator failure")
        return len(records)

    async def query_valuations_as_of(
        self,
        instruments: tuple[str, ...],
        start: date,
        end: date,
        as_of: datetime,
    ) -> tuple[DailyValuationRevision, ...]:
        return ()

    async def query_indicator_revisions_as_of(
        self,
        instruments: tuple[str, ...],
        announced_start: date,
        announced_end: date,
        as_of: datetime,
    ) -> tuple[FinancialIndicatorRevision, ...]:
        return ()


class RecordingControl:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.manifests: dict[str, DatasetManifest] = {}
        self.reports: dict[str, QualityReport] = {}
        self.checkpoints: list[str] = []

    async def save_source_evidence(
        self,
        item: SourceEvidence,
    ) -> None:
        if not self.calls or self.calls[-1] != "save_evidence":
            self.calls.append("save_evidence")

    async def read_source_evidence(
        self,
        evidence_hash: str,
    ) -> SourceEvidence:
        raise LookupError(evidence_hash)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[RecordingControl]:
        self.calls.append("begin")
        yield self
        self.calls.append("commit")

    async def save_quality_report(
        self,
        report: QualityReport,
    ) -> None:
        self.calls.append("save_report")
        self.reports[report.report_hash] = report

    async def save_manifest(
        self,
        manifest: DatasetManifest,
    ) -> None:
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
        self,
        event_type: str,
        occurred_at: datetime,
        payload: object,
    ) -> str:
        self.calls.append(f"audit:{event_type}")
        return "f" * 64

    async def read_manifest(
        self,
        manifest_hash: str,
    ) -> DatasetManifest:
        return self.manifests[manifest_hash]

    async def read_quality_report(
        self,
        report_hash: str,
    ) -> QualityReport:
        return self.reports[report_hash]


def setup(
    *,
    complete: bool = True,
    fail_indicators: bool = False,
) -> tuple[
    FundamentalIngestionService,
    FundamentalIngestionRequest,
    list[str],
    RecordingControl,
]:
    calls: list[str] = []
    control = RecordingControl(calls)
    service = FundamentalIngestionService(
        source=RecordingSource(
            calls,
            batch(include_valuation=complete),
        ),
        quality_gate=FundamentalQualityGate(),
        fundamental_repository=RecordingFundamentals(
            calls,
            fail_indicators=fail_indicators,
        ),
        control_repository=control,
        now=lambda: AS_OF,
    )
    request = FundamentalIngestionRequest(
        instruments=(INSTRUMENT,),
        start=DAY,
        end=DAY,
        as_of=AS_OF,
        production_complete_requested=True,
    )
    return service, request, calls, control


def test_complete_fundamental_batch_passes_structural_gate() -> None:
    report = FundamentalQualityGate().evaluate(
        batch=batch(),
        requested_instruments=(INSTRUMENT,),
        start=DAY,
        end=DAY,
        as_of=AS_OF,
    )

    assert report.passed is True
    assert report.production_complete is True
    assert report.issues == ()


def test_missing_valuation_fails_closed() -> None:
    report = FundamentalQualityGate().evaluate(
        batch=batch(include_valuation=False),
        requested_instruments=(INSTRUMENT,),
        start=DAY,
        end=DAY,
        as_of=AS_OF,
    )

    assert {value.code for value in report.issues} == {
        "missing_valuation_history"
    }


@pytest.mark.asyncio
async def test_complete_ingestion_freezes_manifest_and_checkpoints() -> None:
    service, request, calls, control = setup()

    result = await service.run(request)

    assert result.status == "completed"
    assert result.fetched_valuations == 1
    assert result.fetched_indicators == 1
    assert result.manifest_hash is not None
    assert control.manifests[result.manifest_hash].source == (
        "tushare-fundamental"
    )
    assert control.checkpoints == ["daily-basic", "fina-indicator"]
    assert calls == [
        "fetch",
        "save_evidence",
        "append_valuations",
        "append_indicators",
        "begin",
        "save_report",
        "save_manifest",
        "checkpoint:daily-basic",
        "checkpoint:fina-indicator",
        "audit:fundamental_ingestion_completed",
        "commit",
    ]


@pytest.mark.asyncio
async def test_quality_rejection_never_freezes_manifest() -> None:
    service, request, calls, control = setup(complete=False)

    result = await service.run(request)

    assert result.status == "quality_rejected"
    assert result.manifest_hash is None
    assert control.checkpoints == []
    assert "save_manifest" not in calls


@pytest.mark.asyncio
async def test_partial_persistence_never_finalizes_metadata() -> None:
    service, request, calls, control = setup(fail_indicators=True)

    result = await service.run(request)

    assert result.status == "persistence_failed"
    assert result.manifest_hash is None
    assert control.checkpoints == []
    assert "begin" not in calls
