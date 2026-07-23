from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio

from autoquant.adapters.clickhouse_daily import ClickHouseDailyRepository
from autoquant.data.daily_models import (
    AdjustmentFactorRevision,
    DailyBarRevision,
    DailyCoverageEvidence,
    DailyPriceLimit,
    DailySuspensionStatus,
    InstrumentLifecycle,
    TradingSession,
)

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
    session_table = f"autoquant_test_trading_session_revisions_{suffix}"
    lifecycle_table = f"autoquant_test_instrument_lifecycle_revisions_{suffix}"
    suspension_table = f"autoquant_test_daily_suspension_revisions_{suffix}"
    limit_table = f"autoquant_test_daily_price_limit_revisions_{suffix}"
    repo = await ClickHouseDailyRepository.connect(
        dsn=CLICKHOUSE_DSN,
        source=f"integration-{suffix}",
        bar_table=bar_table,
        factor_table=factor_table,
        session_table=session_table,
        lifecycle_table=lifecycle_table,
        suspension_table=suspension_table,
        limit_table=limit_table,
    )
    migration = (
        Path("migrations/clickhouse/002_tushare_daily.sql")
        .read_text(encoding="utf-8")
        .replace("daily_bar_revisions", bar_table)
        .replace("adjustment_factor_revisions", factor_table)
    )
    coverage_migration = (
        Path("migrations/clickhouse/003_daily_coverage.sql")
        .read_text(encoding="utf-8")
        .replace("trading_session_revisions", session_table)
        .replace("instrument_lifecycle_revisions", lifecycle_table)
        .replace("daily_suspension_revisions", suspension_table)
        .replace("daily_price_limit_revisions", limit_table)
    )
    try:
        for statement in migration.split(";"):
            if statement.strip():
                await repo.client.command(statement)
        for statement in coverage_migration.split(";"):
            if statement.strip():
                await repo.client.command(statement)
        yield repo
    finally:
        try:
            await repo.client.command(f"DROP TABLE IF EXISTS {bar_table}")
            await repo.client.command(f"DROP TABLE IF EXISTS {factor_table}")
            await repo.client.command(f"DROP TABLE IF EXISTS {session_table}")
            await repo.client.command(f"DROP TABLE IF EXISTS {lifecycle_table}")
            await repo.client.command(f"DROP TABLE IF EXISTS {suspension_table}")
            await repo.client.command(f"DROP TABLE IF EXISTS {limit_table}")
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
        (original_bar.instrument,),
        original_bar.session_date,
        original_bar.session_date,
        first_visible,
    )
    after_bars = await repository.query_bars_as_of(
        (original_bar.instrument,),
        original_bar.session_date,
        original_bar.session_date,
        corrected_visible,
    )
    after_factors = await repository.query_factors_as_of(
        (original_bar.instrument,),
        original_bar.session_date,
        original_bar.session_date,
        corrected_visible,
    )

    assert before_bars == (original_bar,)
    assert after_bars == (corrected_bar,)
    assert after_factors == (corrected_factor,)


def coverage(
    source: str, *, available_at: datetime, suspended: bool, up_limit: str
) -> DailyCoverageEvidence:
    response_hash = ("c" if not suspended else "d") * 64
    return DailyCoverageEvidence(
        sessions=(TradingSession(source, date(2026, 7, 20), True, available_at, response_hash),),
        lifecycles=(
            InstrumentLifecycle(
                source,
                "000001.XSHE",
                date(1991, 4, 3),
                None,
                available_at,
                response_hash,
            ),
        ),
        suspensions=(
            DailySuspensionStatus(
                source,
                "000001.XSHE",
                date(2026, 7, 20),
                suspended,
                available_at,
                response_hash,
            ),
        ),
        price_limits=(
            DailyPriceLimit(
                source,
                "000001.XSHE",
                date(2026, 7, 20),
                Decimal("10"),
                Decimal(up_limit),
                Decimal("9"),
                available_at,
                response_hash,
            ),
        ),
    )


@pytest.mark.asyncio
async def test_daily_coverage_selects_point_in_time_correction(
    repository: ClickHouseDailyRepository,
) -> None:
    first_visible = datetime(2026, 7, 21, 0, 40, tzinfo=UTC)
    corrected_visible = datetime(2026, 7, 22, 0, 50, tzinfo=UTC)
    original = coverage(
        repository.source,
        available_at=first_visible,
        suspended=False,
        up_limit="11",
    )
    corrected = coverage(
        repository.source,
        available_at=corrected_visible,
        suspended=True,
        up_limit="10.5",
    )

    assert await repository.append_coverage(original) == 4
    assert await repository.append_coverage(corrected) == 4

    before = await repository.query_coverage_as_of(
        ("000001.XSHE",), date(2026, 7, 20), date(2026, 7, 20), first_visible
    )
    after = await repository.query_coverage_as_of(
        ("000001.XSHE",), date(2026, 7, 20), date(2026, 7, 20), corrected_visible
    )
    sessions = await repository.query_sessions_as_of(
        date(2026, 7, 20),
        date(2026, 7, 20),
        corrected_visible,
    )

    assert before == original
    assert after == corrected
    assert sessions == corrected.sessions


@pytest.mark.asyncio
async def test_daily_repository_reads_only_frozen_content_hashes(
    repository: ClickHouseDailyRepository,
) -> None:
    first_visible = datetime(2026, 7, 21, 0, 40, tzinfo=UTC)
    corrected_visible = datetime(2026, 7, 22, 0, 50, tzinfo=UTC)
    original_bar = bar(
        repository.source,
        available_at=first_visible,
        ingested_at=first_visible,
        close="10.1",
    )
    corrected_bar = bar(
        repository.source,
        available_at=corrected_visible,
        ingested_at=corrected_visible,
        close="10.2",
    )
    original_factor = factor(
        repository.source,
        available_at=first_visible,
        ingested_at=first_visible,
        value="123.4",
    )
    corrected_factor = factor(
        repository.source,
        available_at=corrected_visible,
        ingested_at=corrected_visible,
        value="123.5",
    )
    original_coverage = coverage(
        repository.source,
        available_at=first_visible,
        suspended=False,
        up_limit="11",
    )
    corrected_coverage = coverage(
        repository.source,
        available_at=corrected_visible,
        suspended=True,
        up_limit="10.5",
    )
    await repository.append_bars((original_bar, corrected_bar))
    await repository.append_factors((original_factor, corrected_factor))
    await repository.append_coverage(original_coverage)
    await repository.append_coverage(corrected_coverage)
    expected_hashes = (
        original_bar.content_hash,
        original_factor.content_hash,
        original_coverage.sessions[0].content_hash,
        original_coverage.lifecycles[0].content_hash,
        original_coverage.suspensions[0].content_hash,
        original_coverage.price_limits[0].content_hash,
    )

    result = await repository.query_exact_records(
        instruments=("000001.XSHE",),
        start=date(2026, 7, 20),
        end=date(2026, 7, 20),
        record_hash_groups=(expected_hashes,),
    )

    assert result.bars == (original_bar,)
    assert result.factors == (original_factor,)
    assert result.sessions == original_coverage.sessions
    assert result.lifecycles == original_coverage.lifecycles
    assert result.suspensions == original_coverage.suspensions
    assert result.price_limits == original_coverage.price_limits
