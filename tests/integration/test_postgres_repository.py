from __future__ import annotations

import hashlib
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio

from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.data.models import DatasetManifest, SourceEvidence
from autoquant.data.quality import QualityIssue, QualityReport, QualitySeverity

POSTGRES_DSN = os.environ.get("AQ_POSTGRES_DSN", "").strip()
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not POSTGRES_DSN,
        reason="AQ_POSTGRES_DSN is not configured; PostgreSQL infrastructure unavailable",
    ),
]


@pytest_asyncio.fixture
async def repository() -> AsyncIterator[PostgresControlRepository]:
    schema = f"autoquant_test_{uuid4().hex}"
    repo = PostgresControlRepository.connect(dsn=POSTGRES_DSN, schema=schema)
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/003_revision_checkpoints.sql",
        )
    )
    try:
        await repo.initialize(migration)
        yield repo
    finally:
        try:
            await repo.drop_test_schema()
        finally:
            await repo.close()


def report(*, passing: bool) -> QualityReport:
    instant = datetime(2026, 7, 20, 1, 31, tzinfo=UTC)
    issues = () if passing else (
        QualityIssue(
            severity=QualitySeverity.ERROR,
            code="missing_bar",
            instrument="000001.XSHE",
            event_time=instant,
            message="missing",
        ),
    )
    return QualityReport(
        requested_instruments=("000001.XSHE",),
        start=instant,
        end=instant,
        as_of=instant,
        issues=issues,
        production_complete=passing,
    )


def manifest(quality: QualityReport) -> DatasetManifest:
    instant = datetime(2026, 7, 20, 1, 31, tzinfo=UTC)
    return DatasetManifest(
        source="rqdata",
        instruments=("000001.XSHE",),
        start_time=instant,
        end_time=instant,
        as_of=instant,
        record_hashes=("a" * 64,),
        quality_report_hash=quality.report_hash,
        production_complete=True,
        row_count=1,
    )


@pytest.mark.asyncio
async def test_checkpoint_is_monotonic_and_accepts_same_event_corrections(
    repository: PostgresControlRepository,
) -> None:
    current = datetime(2026, 7, 20, 1, 31, tzinfo=UTC)
    await repository.advance_checkpoint("rqdata", "minute", "000001.XSHE", current, "a" * 64)
    await repository.advance_checkpoint("rqdata", "minute", "000001.XSHE", current, "a" * 64)
    with pytest.raises(ValueError, match="backward"):
        await repository.advance_checkpoint(
            "rqdata", "minute", "000001.XSHE", current.replace(minute=30), "a" * 64
        )
    await repository.advance_checkpoint(
        "rqdata", "minute", "000001.XSHE", current, "b" * 64
    )


@pytest.mark.asyncio
async def test_manifest_is_idempotent_and_rejects_failing_quality(
    repository: PostgresControlRepository,
) -> None:
    passing = report(passing=True)
    item = manifest(passing)
    await repository.save_quality_report(passing)
    await repository.save_manifest(item)
    await repository.save_manifest(item)

    failing = report(passing=False)
    await repository.save_quality_report(failing)
    forged = object.__new__(DatasetManifest)
    for field, value in {
        "source": item.source,
        "instruments": item.instruments,
        "start_time": item.start_time,
        "end_time": item.end_time,
        "as_of": item.as_of,
        "record_hashes": item.record_hashes,
        "quality_report_hash": failing.report_hash,
        "production_complete": True,
        "row_count": item.row_count,
        "manifest_hash": "b" * 64,
    }.items():
        object.__setattr__(forged, field, value)
    with pytest.raises(ValueError, match="quality"):
        await repository.save_manifest(forged)


@pytest.mark.asyncio
async def test_source_evidence_is_content_addressed_and_immutable(
    repository: PostgresControlRepository,
) -> None:
    body = b"date,000001.XSHE\n2026-07-20,False\n"
    evidence = SourceEvidence(
        source="rqdata",
        method="is_suspended",
        requested_at=datetime(2026, 7, 21, 8, tzinfo=UTC),
        response_body=body,
        response_hash=hashlib.sha256(body).hexdigest(),
    )
    await repository.save_source_evidence(evidence)
    await repository.save_source_evidence(evidence)
    assert await repository.read_source_evidence(evidence.response_hash) == evidence
