from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid5

import clickhouse_connect  # type: ignore[import-untyped]
from clickhouse_connect.driver.asyncclient import (  # type: ignore[import-untyped]
    AsyncClient,
)

from open_quant.clock import to_utc
from open_quant.data.models import MinuteBarRevision
from open_quant.errors import PersistenceUnavailableError

_DEFAULT_TABLE = "minute_bar_revisions"
_TABLE_IDENTIFIER = re.compile(
    r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?\Z"
)
_RECORD_NAMESPACE = UUID("4c23b4fd-1ad3-4a95-a2c4-7380ea89d658")
_INSERT_COLUMNS = (
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
_RESULT_COLUMNS = _INSERT_COLUMNS[1:]


class ClickHouseMinuteBarRepository:
    """Append-only ClickHouse storage for point-in-time minute-bar revisions."""

    def __init__(
        self,
        *,
        client: AsyncClient,
        source: str,
        table: str = _DEFAULT_TABLE,
    ) -> None:
        if not isinstance(source, str) or not source.strip():
            raise ValueError("source cannot be empty")
        if not isinstance(table, str) or _TABLE_IDENTIFIER.fullmatch(table) is None:
            raise ValueError("table must be a safe ClickHouse identifier")
        self._client = client
        self._source = source
        self._table = table

    @classmethod
    async def connect(
        cls,
        *,
        dsn: str,
        source: str,
        table: str = _DEFAULT_TABLE,
    ) -> ClickHouseMinuteBarRepository:
        """Create a repository without retaining or exposing the connection DSN."""
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            client = await clickhouse_connect.get_async_client(dsn=dsn, tz_mode="aware")
        except Exception:
            raise PersistenceUnavailableError("ClickHouse connection failed") from None
        return cls(client=client, source=source, table=table)

    @property
    def client(self) -> AsyncClient:
        return self._client

    @property
    def source(self) -> str:
        return self._source

    async def append(self, records: tuple[MinuteBarRevision, ...]) -> int:
        if not records:
            return 0

        rows: list[tuple[Any, ...]] = []
        identities: set[UUID] = set()
        for record in records:
            if not isinstance(record, MinuteBarRevision):
                raise TypeError("records must contain MinuteBarRevision values")
            if record.source != self._source:
                raise ValueError("record source does not match repository source")
            record_id = self._record_id(record)
            if record_id in identities:
                raise ValueError("duplicate minute-bar revision in append batch")
            identities.add(record_id)
            rows.append(self._insert_row(record_id, record))

        try:
            summary = await self._client.insert(
                table=self._table,
                data=rows,
                column_names=_INSERT_COLUMNS,
            )
        except Exception:
            raise PersistenceUnavailableError("ClickHouse append failed") from None
        try:
            written_rows = summary.written_rows
        except Exception:
            raise PersistenceUnavailableError(
                "ClickHouse append row count unavailable"
            ) from None
        if (
            not isinstance(written_rows, int)
            or isinstance(written_rows, bool)
            or written_rows != len(rows)
        ):
            raise PersistenceUnavailableError("ClickHouse append row count mismatch")
        return len(rows)

    async def query_as_of(
        self,
        instruments: tuple[str, ...],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> tuple[MinuteBarRevision, ...]:
        start_utc = to_utc(start, name="start")
        end_utc = to_utc(end, name="end")
        as_of_utc = to_utc(as_of, name="as_of")
        if start_utc > end_utc:
            raise ValueError("start cannot be after end")
        if not instruments:
            return ()
        if any(not isinstance(value, str) or not value.strip() for value in instruments):
            raise ValueError("instruments must contain nonempty strings")
        if len(set(instruments)) != len(instruments):
            raise ValueError("instruments must be unique")

        parameters: dict[str, Any] = {
            "source": self._source,
            "instruments": instruments,
            "start": start_utc,
            "end": end_utc,
            "as_of": as_of_utc,
        }
        try:
            result = await self._client.query(
                query=self._as_of_sql(),
                parameters=parameters,
                tz_mode="aware",
            )
        except Exception:
            raise PersistenceUnavailableError("ClickHouse query failed") from None

        try:
            column_names = tuple(result.column_names)
            result_rows = result.result_rows
        except Exception:
            raise PersistenceUnavailableError(
                "ClickHouse returned malformed minute-bar rows"
            ) from None
        if column_names != _RESULT_COLUMNS:
            raise PersistenceUnavailableError("ClickHouse returned malformed minute-bar rows")
        try:
            return tuple(self._map_result_row(row) for row in result_rows)
        except (IndexError, TypeError, ValueError):
            raise PersistenceUnavailableError(
                "ClickHouse returned malformed minute-bar rows"
            ) from None

    @staticmethod
    def _record_id(record: MinuteBarRevision) -> UUID:
        ingestion = record.ingested_at.isoformat(timespec="microseconds")
        return uuid5(_RECORD_NAMESPACE, f"{record.content_hash}:{ingestion}")

    @staticmethod
    def _insert_row(record_id: UUID, record: MinuteBarRevision) -> tuple[Any, ...]:
        return (
            record_id,
            record.source,
            record.instrument,
            record.event_time,
            record.published_at,
            record.available_at,
            record.ingested_at,
            record.source_revision,
            record.availability_policy,
            record.open_price,
            record.high_price,
            record.low_price,
            record.close_price,
            record.volume,
            record.turnover,
            record.content_hash,
        )

    def _as_of_sql(self) -> str:
        return f"""
SELECT
    source,
    instrument,
    event_time,
    tupleElement(latest, 1) AS published_at,
    tupleElement(latest, 2) AS available_at,
    tupleElement(latest, 3) AS ingested_at,
    tupleElement(latest, 4) AS source_revision,
    tupleElement(latest, 5) AS availability_policy,
    tupleElement(latest, 6) AS open_price,
    tupleElement(latest, 7) AS high_price,
    tupleElement(latest, 8) AS low_price,
    tupleElement(latest, 9) AS close_price,
    tupleElement(latest, 10) AS volume,
    tupleElement(latest, 11) AS turnover,
    tupleElement(latest, 12) AS content_hash
FROM
(
    SELECT
        source,
        instrument,
        event_time,
        argMax(
            tuple(
                published_at,
                available_at,
                ingested_at,
                source_revision,
                availability_policy,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                turnover,
                content_hash
            ),
            tuple(available_at, ingested_at, record_id)
        ) AS latest
    FROM {self._table}
    WHERE source = %(source)s
      AND instrument IN %(instruments)s
      AND event_time >= %(start)s
      AND event_time <= %(end)s
      AND available_at <= %(as_of)s
    GROUP BY source, instrument, event_time
)
ORDER BY instrument, event_time, source
""".strip()

    @staticmethod
    def _map_result_row(row: tuple[Any, ...]) -> MinuteBarRevision:
        if not isinstance(row, (tuple, list)) or len(row) != len(_RESULT_COLUMNS):
            raise ValueError("unexpected result row shape")
        (
            source,
            instrument,
            event_time,
            published_at,
            available_at,
            ingested_at,
            source_revision,
            availability_policy,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            turnover,
            content_hash,
        ) = row
        if not all(
            isinstance(value, str)
            for value in (
                source,
                instrument,
                source_revision,
                availability_policy,
                content_hash,
            )
        ):
            raise TypeError("unexpected string field type")
        if not all(
            isinstance(value, datetime)
            for value in (event_time, available_at, ingested_at)
        ):
            raise TypeError("unexpected datetime field type")
        if published_at is not None and not isinstance(published_at, datetime):
            raise TypeError("unexpected published_at field type")
        if not all(
            isinstance(value, Decimal)
            for value in (open_price, high_price, low_price, close_price, turnover)
        ):
            raise TypeError("unexpected Decimal field type")
        if not isinstance(volume, int) or isinstance(volume, bool):
            raise TypeError("unexpected volume field type")

        revision = MinuteBarRevision.from_values(
            source=source,
            instrument=instrument,
            event_time=event_time,
            published_at=published_at,
            available_at=available_at,
            ingested_at=ingested_at,
            source_revision=source_revision,
            availability_policy=availability_policy,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            volume=volume,
            turnover=turnover,
        )
        if revision.content_hash != content_hash:
            raise ValueError("content hash does not match result row")
        return revision
