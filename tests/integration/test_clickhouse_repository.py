from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from clickhouse_connect.driver.asyncclient import AsyncClient

from open_quant.adapters.clickhouse import ClickHouseMinuteBarRepository
from open_quant.data.models import MinuteBarRevision

CLICKHOUSE_DSN = os.environ.get("OQ_CLICKHOUSE_DSN", "").strip()
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not CLICKHOUSE_DSN,
        reason="OQ_CLICKHOUSE_DSN is not configured; ClickHouse infrastructure unavailable",
    ),
]


@pytest_asyncio.fixture
async def repository() -> AsyncIterator[ClickHouseMinuteBarRepository]:
    table = f"oq_test_minute_bar_revisions_{uuid4().hex}"
    repo = await ClickHouseMinuteBarRepository.connect(
        dsn=CLICKHOUSE_DSN,
        source=f"integration-{uuid4().hex}",
        table=table,
    )
    client: AsyncClient = repo.client
    migration = (
        Path("migrations/clickhouse/001_phase1.sql")
        .read_text(encoding="utf-8")
        .replace("minute_bar_revisions", table)
    )
    await client.command(migration)
    try:
        yield repo
    finally:
        await client.command(f"DROP TABLE IF EXISTS {table}")
        await client.close()


def revision(
    *,
    source: str,
    available_at: datetime,
    ingested_at: datetime,
    source_revision: str,
    close_price: str,
) -> MinuteBarRevision:
    return MinuteBarRevision.from_values(
        source=source,
        instrument="000001.XSHE",
        event_time=datetime(2026, 7, 20, 1, 31, tzinfo=UTC),
        published_at=datetime(2026, 7, 20, 1, 31, 2, tzinfo=UTC),
        available_at=available_at,
        ingested_at=ingested_at,
        source_revision=source_revision,
        availability_policy="rqdata-minute-v1",
        open_price="10.000001",
        high_price="10.200001",
        low_price="9.900001",
        close_price=close_price,
        volume=1000,
        turnover="10050.0001",
    )


@pytest.mark.asyncio
async def test_as_of_returns_original_before_correction_and_correction_afterward(
    repository: ClickHouseMinuteBarRepository,
) -> None:
    original = revision(
        source=repository.source,
        available_at=datetime(2026, 7, 20, 1, 31, 5, tzinfo=UTC),
        ingested_at=datetime(2026, 7, 20, 2, 0, tzinfo=UTC),
        source_revision="initial",
        close_price="10.100001",
    )
    correction = revision(
        source=repository.source,
        available_at=datetime(2026, 7, 20, 3, 0, tzinfo=UTC),
        ingested_at=datetime(2026, 7, 20, 3, 1, tzinfo=UTC),
        source_revision="corrected",
        close_price="10.150001",
    )
    assert await repository.append((original, correction)) == 2

    before = await repository.query_as_of(
        (original.instrument,),
        original.event_time,
        original.event_time,
        datetime(2026, 7, 20, 2, 30, tzinfo=UTC),
    )
    after = await repository.query_as_of(
        (original.instrument,),
        original.event_time,
        original.event_time,
        datetime(2026, 7, 20, 3, 30, tzinfo=UTC),
    )

    assert before == (original,)
    assert after == (correction,)
