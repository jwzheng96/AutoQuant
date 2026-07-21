from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, patch

import pytest
from clickhouse_connect.driver.asyncclient import AsyncClient

from open_quant.adapters.clickhouse import ClickHouseMinuteBarRepository
from open_quant.data.models import MinuteBarRevision
from open_quant.errors import PersistenceUnavailableError

EVENT_TIME = datetime(2026, 7, 20, 1, 31, 2, 123456, tzinfo=UTC)
PUBLISHED_AT = datetime(2026, 7, 20, 1, 31, 3, 234567, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 7, 20, 1, 31, 4, 345678, tzinfo=UTC)
INGESTED_AT = datetime(2026, 7, 20, 2, 0, 5, 456789, tzinfo=UTC)


def make_revision(**updates: object) -> MinuteBarRevision:
    values: dict[str, object] = {
        "source": "rqdata",
        "instrument": "000001.XSHE",
        "event_time": EVENT_TIME,
        "published_at": PUBLISHED_AT,
        "available_at": AVAILABLE_AT,
        "ingested_at": INGESTED_AT,
        "source_revision": "vendor-revision-7",
        "availability_policy": "rqdata-minute-v1",
        "open_price": "10.123456",
        "high_price": "10.223456",
        "low_price": "10.023456",
        "close_price": "10.173456",
        "volume": 1234,
        "turnover": "12567.8901",
    }
    values.update(updates)
    return MinuteBarRevision.from_values(**values)  # type: ignore[arg-type]


class RecordingClient:
    def __init__(self) -> None:
        self.insert = AsyncMock(return_value=SimpleNamespace(written_rows=1))
        self.query = AsyncMock(
            return_value=SimpleNamespace(column_names=(), result_rows=())
        )


def repository(client: RecordingClient) -> ClickHouseMinuteBarRepository:
    return ClickHouseMinuteBarRepository(
        client=cast(AsyncClient, client),
        source="rqdata",
    )


@pytest.mark.asyncio
async def test_append_maps_every_immutable_field_without_value_interpolation() -> None:
    client = RecordingClient()
    revision = make_revision()

    inserted = await repository(client).append((revision,))

    assert inserted == 1
    client.insert.assert_awaited_once()
    call = client.insert.await_args
    assert call.kwargs["table"] == "minute_bar_revisions"
    assert call.kwargs["column_names"] == (
        "record_id",
        "source",
        "instrument",
        "event_time",
        "published_at",
        "available_at",
        "ingested_at",
        "source_revision",
        "availability_policy",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "turnover",
        "content_hash",
    )
    row = call.kwargs["data"][0]
    assert row[1:] == (
        "rqdata",
        "000001.XSHE",
        EVENT_TIME,
        PUBLISHED_AT,
        AVAILABLE_AT,
        INGESTED_AT,
        "vendor-revision-7",
        "rqdata-minute-v1",
        Decimal("10.123456"),
        Decimal("10.223456"),
        Decimal("10.023456"),
        Decimal("10.173456"),
        1234,
        Decimal("12567.8901"),
        revision.content_hash,
    )


@pytest.mark.asyncio
async def test_record_id_is_stable_for_idempotent_retries_and_tracks_ingestion() -> None:
    client = RecordingClient()
    repo = repository(client)
    revision = make_revision()

    await repo.append((revision,))
    first_id = client.insert.await_args.kwargs["data"][0][0]
    await repo.append((revision,))
    second_id = client.insert.await_args.kwargs["data"][0][0]
    later = make_revision(ingested_at=INGESTED_AT + timedelta(microseconds=1))
    await repo.append((later,))
    later_id = client.insert.await_args.kwargs["data"][0][0]

    assert first_id == second_id
    assert first_id != later_id


@pytest.mark.asyncio
async def test_empty_append_is_a_no_op_without_contacting_clickhouse() -> None:
    client = RecordingClient()

    assert await repository(client).append(()) == 0

    client.insert.assert_not_awaited()


@pytest.mark.asyncio
async def test_append_rejects_duplicate_rows_in_one_batch_without_contacting_clickhouse() -> None:
    client = RecordingClient()
    revision = make_revision()

    with pytest.raises(ValueError, match="duplicate"):
        await repository(client).append((revision, revision))

    client.insert.assert_not_awaited()


@pytest.mark.asyncio
async def test_append_fails_closed_on_driver_error_or_short_write() -> None:
    client = RecordingClient()
    client.insert.side_effect = RuntimeError("driver detail")
    with pytest.raises(PersistenceUnavailableError, match="append failed") as caught:
        await repository(client).append((make_revision(),))
    assert caught.value.__cause__ is None

    client.insert.side_effect = None
    client.insert.return_value = SimpleNamespace(written_rows=0)
    with pytest.raises(PersistenceUnavailableError, match="row count"):
        await repository(client).append((make_revision(),))


@pytest.mark.asyncio
async def test_append_fails_closed_on_malformed_driver_summary() -> None:
    client = RecordingClient()
    client.insert.return_value = SimpleNamespace()

    with pytest.raises(PersistenceUnavailableError, match="row count"):
        await repository(client).append((make_revision(),))


@pytest.mark.asyncio
async def test_query_binds_all_temporal_and_identity_filters_and_maps_exact_row() -> None:
    client = RecordingClient()
    revision = make_revision()
    columns = (
        "source",
        "instrument",
        "event_time",
        "published_at",
        "available_at",
        "ingested_at",
        "source_revision",
        "availability_policy",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "turnover",
        "content_hash",
    )
    client.query.return_value = SimpleNamespace(
        column_names=columns,
        result_rows=(
            (
                revision.source,
                revision.instrument,
                revision.event_time,
                revision.published_at,
                revision.available_at,
                revision.ingested_at,
                revision.source_revision,
                revision.availability_policy,
                revision.open_price,
                revision.high_price,
                revision.low_price,
                revision.close_price,
                revision.volume,
                revision.turnover,
                revision.content_hash,
            ),
        ),
    )
    start = EVENT_TIME - timedelta(minutes=1)
    end = EVENT_TIME + timedelta(minutes=1)
    as_of = AVAILABLE_AT + timedelta(seconds=1)

    result = await repository(client).query_as_of(
        ("000001.XSHE", "600000.XSHG"), start, end, as_of
    )

    assert result == (revision,)
    call = client.query.await_args
    sql = call.kwargs["query"]
    assert "available_at <= %(as_of)s" in sql
    assert "source = %(source)s" in sql
    assert "instrument IN %(instruments)s" in sql
    assert "event_time >= %(start)s" in sql
    assert "event_time <= %(end)s" in sql
    assert "argMax" in sql
    assert "tuple(available_at, ingested_at" in sql
    assert call.kwargs["parameters"] == {
        "source": "rqdata",
        "instruments": ("000001.XSHE", "600000.XSHG"),
        "start": start,
        "end": end,
        "as_of": as_of,
    }
    assert call.kwargs["tz_mode"] == "aware"


@pytest.mark.asyncio
async def test_query_empty_instruments_is_a_no_op() -> None:
    client = RecordingClient()

    result = await repository(client).query_as_of(
        (), EVENT_TIME, EVENT_TIME, AVAILABLE_AT
    )

    assert result == ()
    client.query.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["start", "end", "as_of"])
async def test_query_rejects_naive_temporal_filters(field: str) -> None:
    client = RecordingClient()
    values = {"start": EVENT_TIME, "end": EVENT_TIME, "as_of": AVAILABLE_AT}
    values[field] = values[field].replace(tzinfo=None)

    with pytest.raises(ValueError, match="timezone-aware"):
        await repository(client).query_as_of(("000001.XSHE",), **values)  # type: ignore[arg-type]

    client.query.assert_not_awaited()


@pytest.mark.asyncio
async def test_query_fails_closed_on_wrong_columns_malformed_row_or_driver_error() -> None:
    client = RecordingClient()
    client.query.return_value = SimpleNamespace(
        column_names=("source",), result_rows=(("rqdata",),)
    )
    with pytest.raises(PersistenceUnavailableError, match="malformed"):
        await repository(client).query_as_of(
            ("000001.XSHE",), EVENT_TIME, EVENT_TIME, AVAILABLE_AT
        )

    client.query.side_effect = RuntimeError("query detail")
    with pytest.raises(PersistenceUnavailableError, match="query failed") as caught:
        await repository(client).query_as_of(
            ("000001.XSHE",), EVENT_TIME, EVENT_TIME, AVAILABLE_AT
        )
    assert caught.value.__cause__ is None


@pytest.mark.asyncio
async def test_query_fails_closed_on_malformed_driver_result_container() -> None:
    client = RecordingClient()
    client.query.return_value = SimpleNamespace()

    with pytest.raises(PersistenceUnavailableError, match="malformed"):
        await repository(client).query_as_of(
            ("000001.XSHE",), EVENT_TIME, EVENT_TIME, AVAILABLE_AT
        )


@pytest.mark.asyncio
async def test_query_rejects_content_hash_that_does_not_match_the_returned_row() -> None:
    client = RecordingClient()
    revision = make_revision()
    client.query.return_value = SimpleNamespace(
        column_names=(
            "source",
            "instrument",
            "event_time",
            "published_at",
            "available_at",
            "ingested_at",
            "source_revision",
            "availability_policy",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "turnover",
            "content_hash",
        ),
        result_rows=(
            (
                revision.source,
                revision.instrument,
                revision.event_time,
                revision.published_at,
                revision.available_at,
                revision.ingested_at,
                revision.source_revision,
                revision.availability_policy,
                revision.open_price,
                revision.high_price,
                revision.low_price,
                revision.close_price,
                revision.volume,
                revision.turnover,
                revision.content_hash[:-1] + "0",
            ),
        ),
    )

    with pytest.raises(PersistenceUnavailableError, match="malformed"):
        await repository(client).query_as_of(
            (revision.instrument,), EVENT_TIME, EVENT_TIME, AVAILABLE_AT
        )


@pytest.mark.asyncio
async def test_connect_sanitizes_dsn_and_driver_details() -> None:
    dsn = "clickhouse://named-user:super-secret@example.invalid/database"
    with patch(
        "open_quant.adapters.clickhouse.clickhouse_connect.get_async_client",
        new=AsyncMock(side_effect=RuntimeError(dsn)),
    ):
        with pytest.raises(PersistenceUnavailableError, match="connection failed") as caught:
            await ClickHouseMinuteBarRepository.connect(dsn=dsn, source="rqdata")

    assert caught.value.__cause__ is None
    assert "super-secret" not in str(caught.value)
    assert "super-secret" not in repr(caught.value)


def test_repository_rejects_unsafe_table_identifier() -> None:
    client = RecordingClient()
    with pytest.raises(ValueError, match="table"):
        ClickHouseMinuteBarRepository(
            client=cast(AsyncClient, client),
            source="rqdata",
            table="minute_bar_revisions; DROP TABLE minute_bar_revisions",
        )
