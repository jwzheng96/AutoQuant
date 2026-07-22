from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio

from autoquant.adapters.clickhouse_daily import ClickHouseDailyRepository
from autoquant.data.daily_models import AdjustmentFactorRevision, DailyBarRevision

CLICKHOUSE_DSN = os.environ.get("AQ_CLICKHOUSE_DSN", "").strip()
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not CLICKHOUSE_DSN,
        reason="AQ_CLICKHOUSE_DSN is not configured; ClickHouse infrastructure unavailable",
    ),
]


@pytest_asyncio.fixture
async def repository() -> AsyncIterator[ClickHouseDailyRepository]:
    suffix = uuid4().hex
    bar_table = f"autoquant_test_daily_bar_revisions_{suffix}"
    factor_table = f"autoquant_test_adjustment_factor_revisions_{suffix}"
    repo = await ClickHouseDailyRepository.connect(
        dsn=CLICKHOUSE_DSN,
        source=f"integration-{suffix}",
        bar_table=bar_table,
        factor_table=factor_table,
    )
    migration = (
        Path("migrations/clickhouse/002_tushare_daily.sql")
        .read_text(encoding="utf-8")
        .replace("daily_bar_revisions", bar_table)
        .replace("adjustment_factor_revisions", factor_table)
    )
    try:
        await repo.client.command(migration)
        yield repo
    finally:
        try:
            await repo.client.command(f"DROP TABLE IF EXISTS {bar_table}")
            await repo.client.command(f"DROP TABLE IF EXISTS {factor_table}")
        finally:
            await repo.client.close()


def bar(
    source: str, *, available_at: datetime, ingested_at: datetime, close: str
) -> DailyBarRevision:
    return DailyBarRevision.from_values(
        source=source,
        instrument="000001.XSHE",
        session_date=date(2026, 7, 20),
        event_time=datetime(2026, 7, 20, 7, 0, tzinfo=UTC),
        available_at=available_at,
        ingested_at=ingested_at,
        source_revision=f"daily-{close}",
        availability_policy="tushare-daily-v1",
        evidence_hash="a" * 64,
        open_price="10",
        high_price="10.2",
        low_price="9.9",
        close_price=close,
        pre_close="9.95",
        volume=100,
        turnover="1000",
    )


def factor(
    source: str, *, available_at: datetime, ingested_at: datetime, value: str
) -> AdjustmentFactorRevision:
    return AdjustmentFactorRevision.from_values(
        source=source,
        instrument="000001.XSHE",
        session_date=date(2026, 7, 20),
        event_time=datetime(2026, 7, 20, 7, 0, tzinfo=UTC),
        available_at=available_at,
        ingested_at=ingested_at,
        source_revision=f"factor-{value}",
        availability_policy="tushare-daily-v1",
        evidence_hash="b" * 64,
        factor=value,
    )


@pytest.mark.asyncio
async def test_daily_repositories_select_corrections_by_as_of(
    repository: ClickHouseDailyRepository,
) -> None:
    first_visible = datetime(2026, 7, 21, 1, 30, tzinfo=UTC)
    corrected_visible = datetime(2026, 7, 22, 1, 30, tzinfo=UTC)
    original_bar = bar(
        repository.source,
        available_at=first_visible,
        ingested_at=datetime(2026, 7, 21, 2, 0, tzinfo=UTC),
        close="10.1",
    )
    corrected_bar = bar(
        repository.source,
        available_at=corrected_visible,
        ingested_at=datetime(2026, 7, 22, 2, 0, tzinfo=UTC),
        close="10.15",
    )
    original_factor = factor(
        repository.source,
        available_at=first_visible,
        ingested_at=datetime(2026, 7, 21, 2, 0, tzinfo=UTC),
        value="123.4",
    )
    corrected_factor = factor(
        repository.source,
        available_at=corrected_visible,
        ingested_at=datetime(2026, 7, 22, 2, 0, tzinfo=UTC),
        value="123.5",
    )
    assert await repository.append_bars((original_bar, corrected_bar)) == 2
    assert await repository.append_factors((original_factor, corrected_factor)) == 2

    before_bars = await repository.query_bars_as_of(
        (original_bar.instrument,), original_bar.session_date, original_bar.session_date,
        first_visible
    )
    after_bars = await repository.query_bars_as_of(
        (original_bar.instrument,), original_bar.session_date, original_bar.session_date,
        corrected_visible
    )
    after_factors = await repository.query_factors_as_of(
        (original_bar.instrument,), original_bar.session_date, original_bar.session_date,
        corrected_visible
    )

    assert before_bars == (original_bar,)
    assert after_bars == (corrected_bar,)
    assert after_factors == (corrected_factor,)
