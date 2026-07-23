from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio

from autoquant.adapters.clickhouse_fundamental import (
    ClickHouseFundamentalRepository,
)
from autoquant.data.fundamental_models import (
    DailyValuationRevision,
    FinancialIndicatorRevision,
)

CLICKHOUSE_DSN = os.environ.get("AQ_CLICKHOUSE_DSN", "").strip()
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not CLICKHOUSE_DSN,
        reason=(
            "AQ_CLICKHOUSE_DSN is not configured; "
            "ClickHouse infrastructure unavailable"
        ),
    ),
]


@pytest_asyncio.fixture
async def repository() -> AsyncIterator[ClickHouseFundamentalRepository]:
    suffix = uuid4().hex
    valuation_table = f"autoquant_test_daily_valuations_{suffix}"
    indicator_table = f"autoquant_test_financial_indicators_{suffix}"
    repo = await ClickHouseFundamentalRepository.connect(
        dsn=CLICKHOUSE_DSN,
        source=f"integration-{suffix}",
        valuation_table=valuation_table,
        indicator_table=indicator_table,
    )
    migration = (
        Path("migrations/clickhouse/004_fundamental_revisions.sql")
        .read_text(encoding="utf-8")
        .replace("daily_valuation_revisions", valuation_table)
        .replace("financial_indicator_revisions", indicator_table)
    )
    try:
        for statement in migration.split(";"):
            if statement.strip():
                await repo.client.command(statement)
        yield repo
    finally:
        try:
            await repo.client.command(
                f"DROP TABLE IF EXISTS {valuation_table}"
            )
            await repo.client.command(
                f"DROP TABLE IF EXISTS {indicator_table}"
            )
        finally:
            await repo.client.close()


def valuation(
    source: str,
    *,
    available_at: datetime,
    ingested_at: datetime,
    pb: str,
) -> DailyValuationRevision:
    return DailyValuationRevision.from_values(
        source=source,
        instrument="600519.XSHG",
        session_date=date(2026, 7, 20),
        event_time=datetime(2026, 7, 20, 7, tzinfo=UTC),
        available_at=available_at,
        ingested_at=ingested_at,
        source_revision=f"daily-basic-{pb}",
        availability_policy="tushare-daily-v1",
        evidence_hash="a" * 64,
        close_price="1500",
        free_float_turnover_rate_percent="0.2",
        pe_ttm="20",
        pb=pb,
        ps_ttm=None,
        dividend_yield_ttm_percent="2",
        total_market_value_cny="1900000000000",
        circulating_market_value_cny="1900000000000",
    )


def indicator(
    source: str,
    *,
    announced_date: date,
    available_at: datetime,
    ingested_at: datetime,
    roe: str,
    updated: bool,
) -> FinancialIndicatorRevision:
    return FinancialIndicatorRevision.from_values(
        source=source,
        instrument="600519.XSHG",
        report_period=date(2026, 3, 31),
        announced_date=announced_date,
        updated=updated,
        event_time=datetime(
            announced_date.year,
            announced_date.month,
            announced_date.day,
            7,
            tzinfo=UTC,
        ),
        available_at=available_at,
        ingested_at=ingested_at,
        source_revision=f"fina-indicator-{roe}",
        availability_policy="tushare-daily-v1",
        evidence_hash="b" * 64,
        roe_diluted_percent=roe,
        roa_percent="8",
        gross_profit_margin_percent=None,
        debt_to_assets_percent="20",
        operating_cashflow_to_revenue_percent="40",
    )


@pytest.mark.asyncio
async def test_valuation_query_respects_availability_and_ingestion_cutoffs(
    repository: ClickHouseFundamentalRepository,
) -> None:
    first = valuation(
        repository.source,
        available_at=datetime(2026, 7, 21, 1, 30, tzinfo=UTC),
        ingested_at=datetime(2026, 7, 21, 2, tzinfo=UTC),
        pb="8",
    )
    correction = valuation(
        repository.source,
        available_at=datetime(2026, 7, 22, 1, 30, tzinfo=UTC),
        ingested_at=datetime(2026, 7, 22, 2, tzinfo=UTC),
        pb="7.9",
    )

    assert (
        await repository.append_valuations((first, correction))
        == 2
    )
    before_ingestion = await repository.query_valuations_as_of(
        (first.instrument,),
        first.session_date,
        first.session_date,
        datetime(2026, 7, 21, 1, 45, tzinfo=UTC),
    )
    first_visible = await repository.query_valuations_as_of(
        (first.instrument,),
        first.session_date,
        first.session_date,
        datetime(2026, 7, 21, 2, tzinfo=UTC),
    )
    corrected = await repository.query_valuations_as_of(
        (first.instrument,),
        first.session_date,
        first.session_date,
        datetime(2026, 7, 22, 2, tzinfo=UTC),
    )

    assert before_ingestion == ()
    assert first_visible == (first,)
    assert corrected == (correction,)


@pytest.mark.asyncio
async def test_indicator_query_preserves_announcement_revision_stream(
    repository: ClickHouseFundamentalRepository,
) -> None:
    original = indicator(
        repository.source,
        announced_date=date(2026, 4, 25),
        available_at=datetime(2026, 4, 27, 1, 30, tzinfo=UTC),
        ingested_at=datetime(2026, 4, 27, 2, tzinfo=UTC),
        roe="10",
        updated=False,
    )
    update = indicator(
        repository.source,
        announced_date=date(2026, 5, 10),
        available_at=datetime(2026, 5, 11, 1, 30, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 11, 2, tzinfo=UTC),
        roe="9.8",
        updated=True,
    )

    assert await repository.append_indicators((original, update)) == 2
    before_update = await repository.query_indicator_revisions_as_of(
        (original.instrument,),
        date(2026, 1, 1),
        date(2026, 12, 31),
        datetime(2026, 4, 27, 2, tzinfo=UTC),
    )
    after_update = await repository.query_indicator_revisions_as_of(
        (original.instrument,),
        date(2026, 1, 1),
        date(2026, 12, 31),
        datetime(2026, 5, 11, 2, tzinfo=UTC),
    )

    assert before_update == (original,)
    assert after_update == (original, update)
    pressure = await repository.merge_pressure()
    assert pressure.inactive_bytes >= 0
    assert pressure.inactive_parts >= 0
    await repository.purge_allocator(strict=True)


@pytest.mark.asyncio
async def test_as_of_preserves_subsecond_ingestion_precision(
    repository: ClickHouseFundamentalRepository,
) -> None:
    record = indicator(
        repository.source,
        announced_date=date(2026, 4, 25),
        available_at=datetime(
            2026, 4, 27, 1, 30, 0, 100_000, tzinfo=UTC
        ),
        ingested_at=datetime(
            2026, 7, 23, 19, 37, 48, 750_000, tzinfo=UTC
        ),
        roe="10",
        updated=False,
    )
    await repository.append_indicators((record,))

    before = await repository.query_indicator_revisions_as_of(
        (record.instrument,),
        date(2026, 1, 1),
        date(2026, 12, 31),
        datetime(
            2026, 7, 23, 19, 37, 48, 749_999, tzinfo=UTC
        ),
    )
    visible = await repository.query_indicator_revisions_as_of(
        (record.instrument,),
        date(2026, 1, 1),
        date(2026, 12, 31),
        datetime(
            2026, 7, 23, 19, 37, 48, 750_000, tzinfo=UTC
        ),
    )

    assert before == ()
    assert visible == (record,)
