from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from clickhouse_connect.driver.asyncclient import AsyncClient

from autoquant.adapters.clickhouse import ClickHouseMinuteBarRepository
from autoquant.data.models import MinuteBarRevision

CLICKHOUSE_DSN = os.environ.get("AQ_CLICKHOUSE_DSN", "").strip()
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not CLICKHOUSE_DSN,
        reason="AQ_CLICKHOUSE_DSN is not configured; ClickHouse infrastructure unavailable",
    ),
]


@pytest_asyncio.fixture
async def repository() -> AsyncIterator[ClickHouseMinuteBarRepository]:
    table = f"autoquant_test_minute_bar_revisions_{uuid4().hex}"
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
    try:
        await client.command(migration)
        yield repo
    finally:
        try:
            await client.command(f"DROP TABLE IF EXISTS {table}")
        finally:
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
        event_time=datetime(2026, 7, 20, 1, 31, 0, 123456, tzinfo=UTC),
        published_at=datetime(2026, 7, 20, 1, 31, 0, 234567, tzinfo=UTC),
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
        available_at=datetime(2026, 7, 20, 1, 31, 0, 345678, tzinfo=UTC),
        ingested_at=datetime(2026, 7, 20, 2, 0, 0, 456789, tzinfo=UTC),
        source_revision="initial",
        close_price="10.100001",
    )
    correction = revision(
        source=repository.source,
        available_at=datetime(2026, 7, 20, 1, 31, 0, 345679, tzinfo=UTC),
        ingested_at=datetime(2026, 7, 20, 2, 0, 0, 456790, tzinfo=UTC),
        source_revision="corrected",
        close_price="10.150001",
    )
    assert await repository.append((original, correction)) == 2

    before = await repository.query_as_of(
        (original.instrument,),
        original.event_time,
        original.event_time,
        original.available_at,
    )
    after = await repository.query_as_of(
        (original.instrument,),
        original.event_time,
        original.event_time,
        correction.available_at,
    )

    assert before == (original,)
    assert after == (correction,)
    assert before[0].open_price == Decimal("10.000001")
    assert before[0].high_price == Decimal("10.200001")
    assert before[0].low_price == Decimal("9.900001")
    assert before[0].close_price == Decimal("10.100001")
    assert before[0].turnover == Decimal("10050.0001")


@pytest.mark.asyncio
async def test_equal_visibility_times_use_stable_record_identity_as_tie_breaker(
    repository: ClickHouseMinuteBarRepository,
) -> None:
    available_at = datetime(2026, 7, 20, 1, 31, 0, 345678, tzinfo=UTC)
    ingested_at = datetime(2026, 7, 20, 2, 0, 0, 456789, tzinfo=UTC)
    first = revision(
        source=repository.source,
        available_at=available_at,
        ingested_at=ingested_at,
        source_revision="same-time-a",
        close_price="10.100001",
    )
    second = revision(
        source=repository.source,
        available_at=available_at,
        ingested_at=ingested_at,
        source_revision="same-time-b",
        close_price="10.150001",
    )
    assert await repository.append((second, first)) == 2

    first_result = await repository.query_as_of(
        (first.instrument,), first.event_time, first.event_time, available_at
    )
    second_result = await repository.query_as_of(
        (first.instrument,), first.event_time, first.event_time, available_at
    )

    assert first_result in ((first,), (second,))
    assert second_result == first_result
