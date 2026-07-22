from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock

import pytest
from clickhouse_connect.driver.asyncclient import AsyncClient

from autoquant.adapters.clickhouse_daily import ClickHouseDailyRepository
from autoquant.data.daily_models import AdjustmentFactorRevision, DailyBarRevision
from autoquant.errors import PersistenceUnavailableError

EVENT = datetime(2026, 7, 20, 7, 0, tzinfo=UTC)
AVAILABLE = datetime(2026, 7, 21, 1, 30, tzinfo=UTC)
INGESTED = datetime(2026, 7, 22, 8, 0, tzinfo=UTC)
HASH = "a" * 64


def bar(**updates: object) -> DailyBarRevision:
    values: dict[str, object] = {
        "source": "tushare",
        "instrument": "000001.XSHE",
        "session_date": date(2026, 7, 20),
        "event_time": EVENT,
        "available_at": AVAILABLE,
        "ingested_at": INGESTED,
        "source_revision": "tushare:daily:1",
        "availability_policy": "tushare-daily-v1",
        "evidence_hash": HASH,
        "open_price": "10.123456",
        "high_price": "10.223456",
        "low_price": "10.023456",
        "close_price": "10.173456",
        "pre_close": "10.000000",
        "volume": 12345,
        "turnover": "12567.8901",
    }
    values.update(updates)
    return DailyBarRevision.from_values(**values)  # type: ignore[arg-type]


def factor(**updates: object) -> AdjustmentFactorRevision:
    values: dict[str, object] = {
        "source": "tushare",
        "instrument": "000001.XSHE",
        "session_date": date(2026, 7, 20),
        "event_time": EVENT,
        "available_at": AVAILABLE,
        "ingested_at": INGESTED,
        "source_revision": "tushare:adj:1",
        "availability_policy": "tushare-daily-v1",
        "evidence_hash": HASH,
        "factor": "123.456789",
    }
    values.update(updates)
    return AdjustmentFactorRevision.from_values(**values)  # type: ignore[arg-type]


class RecordingClient:
    def __init__(self) -> None:
        self.insert = AsyncMock(return_value=SimpleNamespace(written_rows=1))
        self.query = AsyncMock(
            return_value=SimpleNamespace(column_names=(), result_rows=())
        )
        self.command = AsyncMock(side_effect=[1, 1, 1, 2])


def repository(client: RecordingClient) -> ClickHouseDailyRepository:
    return ClickHouseDailyRepository(
        client=cast(AsyncClient, client),
        source="tushare",
    )


def test_migration_adds_two_append_only_tables_and_version_two() -> None:
    migration = Path("migrations/clickhouse/002_tushare_daily.sql").read_text()

    assert "daily_bar_revisions" in migration
    assert "adjustment_factor_revisions" in migration
    assert "SELECT 'clickhouse', 2" in migration
    assert migration.count("ENGINE = MergeTree") == 2


@pytest.mark.asyncio
async def test_append_maps_bar_and_factor_to_separate_tables() -> None:
    client = RecordingClient()
    repo = repository(client)
    daily = bar()
    adjustment = factor()

    assert await repo.append_bars((daily,)) == 1
    bar_call = client.insert.await_args
    assert bar_call.kwargs["table"] == "daily_bar_revisions"
    assert bar_call.kwargs["data"][0][1:] == (
        "tushare",
        "000001.XSHE",
        date(2026, 7, 20),
        EVENT,
        AVAILABLE,
        INGESTED,
        "tushare:daily:1",
        "tushare-daily-v1",
        HASH,
        Decimal("10.123456"),
        Decimal("10.223456"),
        Decimal("10.023456"),
        Decimal("10.173456"),
        Decimal("10.000000"),
        12345,
        Decimal("12567.8901"),
        daily.content_hash,
    )

    assert await repo.append_factors((adjustment,)) == 1
    factor_call = client.insert.await_args
    assert factor_call.kwargs["table"] == "adjustment_factor_revisions"
    assert factor_call.kwargs["data"][0][1:] == (
        "tushare",
        "000001.XSHE",
        date(2026, 7, 20),
        EVENT,
        AVAILABLE,
        INGESTED,
        "tushare:adj:1",
        "tushare-daily-v1",
        HASH,
        Decimal("123.456789"),
        adjustment.content_hash,
    )


@pytest.mark.asyncio
async def test_append_ids_are_stable_and_duplicate_batch_is_rejected() -> None:
    client = RecordingClient()
    repo = repository(client)
    revision = bar()

    await repo.append_bars((revision,))
    first = client.insert.await_args.kwargs["data"][0][0]
    await repo.append_bars((revision,))
    second = client.insert.await_args.kwargs["data"][0][0]
    await repo.append_bars((bar(ingested_at=INGESTED + timedelta(microseconds=1)),))
    later = client.insert.await_args.kwargs["data"][0][0]

    assert first == second
    assert first != later
    with pytest.raises(ValueError, match="duplicate"):
        await repo.append_bars((revision, revision))


@pytest.mark.asyncio
async def test_append_rejects_wrong_source_type_and_decimal_overflow() -> None:
    client = RecordingClient()
    repo = repository(client)

    with pytest.raises(ValueError, match="source"):
        await repo.append_bars((bar(source="other"),))
    with pytest.raises(TypeError, match="DailyBarRevision"):
        await repo.append_bars((factor(),))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="open_price"):
        await repo.append_bars((bar(open_price="10.1234567"),))
    with pytest.raises(ValueError, match="factor"):
        await repo.append_factors((factor(factor="123.4567891"),))


@pytest.mark.asyncio
async def test_append_fails_closed_on_driver_error_or_short_write() -> None:
    client = RecordingClient()
    client.insert.side_effect = RuntimeError("driver detail")
    with pytest.raises(PersistenceUnavailableError, match="append failed"):
        await repository(client).append_bars((bar(),))

    client.insert.side_effect = None
    client.insert.return_value = SimpleNamespace(written_rows=0)
    with pytest.raises(PersistenceUnavailableError, match="row count"):
        await repository(client).append_factors((factor(),))


@pytest.mark.asyncio
async def test_query_bars_binds_filters_and_verifies_content_hash() -> None:
    client = RecordingClient()
    revision = bar()
    client.query.return_value = SimpleNamespace(
        column_names=ClickHouseDailyRepository.BAR_RESULT_COLUMNS,
        result_rows=[ClickHouseDailyRepository.bar_result_row(revision)],
    )

    values = await repository(client).query_bars_as_of(
        ("000001.XSHE",), date(2026, 7, 20), date(2026, 7, 20), AVAILABLE
    )

    assert values == (revision,)
    call = client.query.await_args
    assert "argMax" in call.kwargs["query"]
    assert "available_at <=" in call.kwargs["query"]
    assert call.kwargs["parameters"] == {
        "source": "tushare",
        "instruments": ["000001.XSHE"],
        "start_date": date(2026, 7, 20),
        "end_date": date(2026, 7, 20),
        "as_of": AVAILABLE,
    }

    byte_hash_row = list(ClickHouseDailyRepository.bar_result_row(revision))
    byte_hash_row[8] = revision.evidence_hash.encode("ascii")
    byte_hash_row[-1] = revision.content_hash.encode("ascii")
    client.query.return_value = SimpleNamespace(
        column_names=ClickHouseDailyRepository.BAR_RESULT_COLUMNS,
        result_rows=[tuple(byte_hash_row)],
    )
    assert await repository(client).query_bars_as_of(
        ("000001.XSHE",), date(2026, 7, 20), date(2026, 7, 20), AVAILABLE
    ) == (revision,)

    bad_row = list(ClickHouseDailyRepository.bar_result_row(revision))
    bad_row[-1] = "b" * 64
    client.query.return_value = SimpleNamespace(
        column_names=ClickHouseDailyRepository.BAR_RESULT_COLUMNS,
        result_rows=[tuple(bad_row)],
    )
    with pytest.raises(PersistenceUnavailableError, match="malformed"):
        await repository(client).query_bars_as_of(
            ("000001.XSHE",), date(2026, 7, 20), date(2026, 7, 20), AVAILABLE
        )


@pytest.mark.asyncio
async def test_query_factors_maps_exact_revision() -> None:
    client = RecordingClient()
    revision = factor()
    client.query.return_value = SimpleNamespace(
        column_names=ClickHouseDailyRepository.FACTOR_RESULT_COLUMNS,
        result_rows=[ClickHouseDailyRepository.factor_result_row(revision)],
    )

    values = await repository(client).query_factors_as_of(
        ("000001.XSHE",), date(2026, 7, 20), date(2026, 7, 20), AVAILABLE
    )

    assert values == (revision,)


@pytest.mark.asyncio
async def test_connection_check_requires_both_tables_and_schema_version_two() -> None:
    client = RecordingClient()

    await repository(client).check_connection()

    assert client.command.await_count == 4
    client = RecordingClient()
    client.command.side_effect = [1, 1, 0, 2]
    with pytest.raises(PersistenceUnavailableError, match="schema"):
        await repository(client).check_connection()
