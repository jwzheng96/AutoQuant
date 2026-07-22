from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio

from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.web.models import DailyIngestionJobRequest, OperatorJobState
from autoquant.web.store import PostgresOperatorRepository

POSTGRES_DSN = os.environ.get("AQ_POSTGRES_DSN", "").strip()
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not POSTGRES_DSN,
        reason="AQ_POSTGRES_DSN is not configured; PostgreSQL infrastructure unavailable",
    ),
]


@pytest_asyncio.fixture
async def repository() -> AsyncIterator[PostgresOperatorRepository]:
    schema = f"autoquant_test_{uuid4().hex}"
    control = PostgresControlRepository.connect(dsn=POSTGRES_DSN, schema=schema)
    operator = PostgresOperatorRepository.connect(dsn=POSTGRES_DSN, schema=schema)
    phase1 = Path("migrations/postgres/001_phase1.sql").read_text(encoding="utf-8")
    console = Path("migrations/postgres/002_operator_console.sql").read_text(encoding="utf-8")
    try:
        await control.initialize(phase1)
        await control.initialize(console)
        yield operator
    finally:
        try:
            await operator.close()
        finally:
            try:
                await control.drop_test_schema()
            finally:
                await control.close()


def _request(*, end: date | None = None) -> DailyIngestionJobRequest:
    return DailyIngestionJobRequest(
        instruments=("000001.XSHE",),
        start=date(2025, 1, 1),
        end=date(2025, 1, 2) if end is None else end,
        idempotency_key="integration-operator-job-0001",
    )


@pytest.mark.asyncio
async def test_job_queue_is_idempotent_and_has_monotonic_states(
    repository: PostgresOperatorRepository,
) -> None:
    now = datetime(2025, 1, 3, tzinfo=UTC)
    created = await repository.create_job(_request(), requested_by="operator", now=now)
    repeated = await repository.create_job(_request(), requested_by="operator", now=now)

    assert repeated.job_id == created.job_id
    assert repeated.state is OperatorJobState.QUEUED
    with pytest.raises(ValueError, match="another request"):
        await repository.create_job(
            _request(end=date(2025, 1, 3)), requested_by="operator", now=now
        )

    claimed = await repository.claim_next_job(now=now)
    assert claimed is not None
    assert claimed.job_id == created.job_id
    assert claimed.state is OperatorJobState.RUNNING

    completed = await repository.complete_job(
        claimed.job_id,
        result={"status": "completed", "persisted_bars": 2},
        now=now,
    )
    assert completed.state is OperatorJobState.COMPLETED
    assert completed.result == {"status": "completed", "persisted_bars": 2}
    assert await repository.claim_next_job(now=now) is None

    jobs = await repository.list_jobs()
    assert jobs == (completed,)
