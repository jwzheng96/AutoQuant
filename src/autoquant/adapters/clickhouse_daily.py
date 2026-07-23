from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid5

import clickhouse_connect  # type: ignore[import-untyped]
from clickhouse_connect.driver.asyncclient import (  # type: ignore[import-untyped]
    AsyncClient,
)

from autoquant.clock import to_utc
from autoquant.data.daily_models import (
    AdjustmentFactorRevision,
    DailyBarRevision,
    DailyCoverageEvidence,
    DailyPriceLimit,
    DailySuspensionStatus,
    InstrumentLifecycle,
    TradingSession,
)
from autoquant.errors import PersistenceUnavailableError

_BAR_TABLE = "daily_bar_revisions"
_FACTOR_TABLE = "adjustment_factor_revisions"
_SESSION_TABLE = "trading_session_revisions"
_LIFECYCLE_TABLE = "instrument_lifecycle_revisions"
_SUSPENSION_TABLE = "daily_suspension_revisions"
_LIMIT_TABLE = "daily_price_limit_revisions"
_HISTORICAL_QUERY_SETTINGS: dict[str, int] = {
    "max_block_size": 1_024,
    "max_bytes_before_external_group_by": 32 * 1024 * 1024,
    "max_read_buffer_size": 64 * 1024,
    "max_read_buffer_size_local_fs": 32 * 1024,
    "max_threads": 1,
}
_TABLE_IDENTIFIER = re.compile(
    r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?\Z"
)


def _year_intervals(
    start: date,
    end: date,
) -> tuple[tuple[date, date], ...]:
    if start > end:
        raise ValueError("historical interval is invalid")
    intervals: list[tuple[date, date]] = []
    current = start
    while current <= end:
        chunk_end = min(end, date(current.year, 12, 31))
        intervals.append((current, chunk_end))
        current = date(current.year + 1, 1, 1)
    return tuple(intervals)


_BAR_NAMESPACE = UUID("59a27670-8f4f-4d69-a823-d3cc2406d8a5")
_FACTOR_NAMESPACE = UUID("7fcb33c7-1470-4e80-a702-afcf797cc651")
_SESSION_NAMESPACE = UUID("3773b145-b18c-468b-bc64-8092a0421687")
_LIFECYCLE_NAMESPACE = UUID("39fdbe3d-9ed1-48f5-8e94-a88bf946f45d")
_SUSPENSION_NAMESPACE = UUID("90957a0d-3a32-4b5d-92ee-b69db836572c")
_LIMIT_NAMESPACE = UUID("07283e54-eb0f-4f98-95bd-c872e140aadb")

_BAR_INSERT_COLUMNS = (
    "record_id",
    "source",
    "instrument",
    "session_date",
    "event_time",
    "available_at",
    "ingested_at",
    "source_revision",
    "availability_policy",
    "evidence_hash",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "pre_close",
    "volume",
    "turnover",
    "content_hash",
)
_FACTOR_INSERT_COLUMNS = (
    "record_id",
    "source",
    "instrument",
    "session_date",
    "event_time",
    "available_at",
    "ingested_at",
    "source_revision",
    "availability_policy",
    "evidence_hash",
    "factor",
    "content_hash",
)
_SESSION_INSERT_COLUMNS = (
    "record_id", "source", "session_date", "is_open", "available_at",
    "response_hash", "content_hash",
)
_LIFECYCLE_INSERT_COLUMNS = (
    "record_id", "source", "instrument", "list_date", "delist_date",
    "available_at", "response_hash", "content_hash",
)
_SUSPENSION_INSERT_COLUMNS = (
    "record_id", "source", "instrument", "session_date", "suspended",
    "available_at", "response_hash", "content_hash",
)
_LIMIT_INSERT_COLUMNS = (
    "record_id", "source", "instrument", "session_date", "pre_close", "up_limit",
    "down_limit", "available_at", "response_hash", "content_hash",
)


@dataclass(frozen=True, slots=True)
class ClickHouseMergePressure:
    inactive_bytes: int
    inactive_parts: int

    def __post_init__(self) -> None:
        if self.inactive_bytes < 0 or self.inactive_parts < 0:
            raise ValueError("ClickHouse merge pressure cannot be negative")


class ClickHouseDailyRepository:
    BAR_RESULT_COLUMNS = _BAR_INSERT_COLUMNS[1:]
    FACTOR_RESULT_COLUMNS = _FACTOR_INSERT_COLUMNS[1:]

    def __init__(
        self,
        *,
        client: AsyncClient,
        source: str,
        bar_table: str = _BAR_TABLE,
        factor_table: str = _FACTOR_TABLE,
        session_table: str = _SESSION_TABLE,
        lifecycle_table: str = _LIFECYCLE_TABLE,
        suspension_table: str = _SUSPENSION_TABLE,
        limit_table: str = _LIMIT_TABLE,
    ) -> None:
        self._validate_identity(
            source, bar_table, factor_table, session_table, lifecycle_table,
            suspension_table, limit_table,
        )
        self._client = client
        self._source = source
        self._bar_table = bar_table
        self._factor_table = factor_table
        self._session_table = session_table
        self._lifecycle_table = lifecycle_table
        self._suspension_table = suspension_table
        self._limit_table = limit_table

    @classmethod
    async def connect(
        cls,
        *,
        dsn: str,
        source: str,
        bar_table: str = _BAR_TABLE,
        factor_table: str = _FACTOR_TABLE,
        session_table: str = _SESSION_TABLE,
        lifecycle_table: str = _LIFECYCLE_TABLE,
        suspension_table: str = _SUSPENSION_TABLE,
        limit_table: str = _LIMIT_TABLE,
    ) -> ClickHouseDailyRepository:
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("dsn cannot be empty")
        cls._validate_identity(
            source, bar_table, factor_table, session_table, lifecycle_table,
            suspension_table, limit_table,
        )
        try:
            client = await clickhouse_connect.get_async_client(dsn=dsn, tz_mode="aware")
        except Exception:
            raise PersistenceUnavailableError("ClickHouse connection failed") from None
        return cls(
            client=client,
            source=source,
            bar_table=bar_table,
            factor_table=factor_table,
            session_table=session_table,
            lifecycle_table=lifecycle_table,
            suspension_table=suspension_table,
            limit_table=limit_table,
        )

    @property
    def client(self) -> AsyncClient:
        return self._client

    @property
    def source(self) -> str:
        return self._source

    async def check_connection(self) -> None:
        try:
            await self._client.command("SELECT 1")
            bar_exists = await self._client.command(f"EXISTS TABLE {self._bar_table}")
            factor_exists = await self._client.command(
                f"EXISTS TABLE {self._factor_table}"
            )
            coverage_exists = [
                await self._client.command(f"EXISTS TABLE {table}")
                for table in (
                    self._session_table,
                    self._lifecycle_table,
                    self._suspension_table,
                    self._limit_table,
                )
            ]
            version = await self._client.command(
                "SELECT max(version) FROM schema_versions WHERE component = 'clickhouse'"
            )
        except Exception:
            raise PersistenceUnavailableError("ClickHouse connection check failed") from None
        if (
            bar_exists not in (1, "1", True)
            or factor_exists not in (1, "1", True)
            or any(value not in (1, "1", True) for value in coverage_exists)
        ):
            raise PersistenceUnavailableError("ClickHouse daily schema is unavailable")
        if not isinstance(version, int) or version < 3:
            raise PersistenceUnavailableError("ClickHouse daily schema version is unavailable")

    async def merge_pressure(self) -> ClickHouseMergePressure:
        """Measure obsolete business-table parts without changing merge settings."""

        tables = (
            self._bar_table,
            self._factor_table,
            self._session_table,
            self._lifecycle_table,
            self._suspension_table,
            self._limit_table,
        )
        sql = """
SELECT
    toUInt64(coalesce(sum(bytes_on_disk), 0)) AS inactive_bytes,
    toUInt64(count()) AS inactive_parts
FROM system.parts
WHERE database = currentDatabase()
  AND table IN {tables:Array(String)}
  AND active = 0
""".strip()
        try:
            result = await self._client.query(
                query=sql,
                parameters={"tables": list(tables)},
            )
            rows = tuple(tuple(row) for row in result.result_rows)
            if (
                tuple(result.column_names)
                != ("inactive_bytes", "inactive_parts")
                or len(rows) != 1
                or len(rows[0]) != 2
            ):
                raise ValueError("unexpected merge pressure result")
            inactive_bytes = int(rows[0][0])
            inactive_parts = int(rows[0][1])
            return ClickHouseMergePressure(
                inactive_bytes=inactive_bytes,
                inactive_parts=inactive_parts,
            )
        except Exception:
            raise PersistenceUnavailableError(
                "ClickHouse merge pressure check failed"
            ) from None

    async def append_bars(self, records: tuple[DailyBarRevision, ...]) -> int:
        rows: list[tuple[Any, ...]] = []
        identities: set[UUID] = set()
        for record in records:
            if not isinstance(record, DailyBarRevision):
                raise TypeError("records must contain DailyBarRevision values")
            self._validate_source(record.source)
            self._validate_bar_decimals(record)
            record_id = self._record_id(_BAR_NAMESPACE, record.content_hash, record.ingested_at)
            if record_id in identities:
                raise ValueError("duplicate daily-bar revision in append batch")
            identities.add(record_id)
            rows.append((record_id, *self.bar_result_row(record)))
        return await self._append(self._bar_table, _BAR_INSERT_COLUMNS, rows)

    async def append_factors(
        self, records: tuple[AdjustmentFactorRevision, ...]
    ) -> int:
        rows: list[tuple[Any, ...]] = []
        identities: set[UUID] = set()
        for record in records:
            if not isinstance(record, AdjustmentFactorRevision):
                raise TypeError("records must contain AdjustmentFactorRevision values")
            self._validate_source(record.source)
            self._require_exact_decimal(
                record.factor, name="factor", precision=24, scale=6
            )
            record_id = self._record_id(
                _FACTOR_NAMESPACE, record.content_hash, record.ingested_at
            )
            if record_id in identities:
                raise ValueError("duplicate adjustment-factor revision in append batch")
            identities.add(record_id)
            rows.append((record_id, *self.factor_result_row(record)))
        return await self._append(self._factor_table, _FACTOR_INSERT_COLUMNS, rows)

    async def append_coverage(self, coverage: DailyCoverageEvidence) -> int:
        if not isinstance(coverage, DailyCoverageEvidence):
            raise TypeError("coverage must be DailyCoverageEvidence")
        session_rows = [
            (
                uuid5(_SESSION_NAMESPACE, value.content_hash),
                value.source,
                value.session_date,
                value.is_open,
                value.available_at,
                value.response_hash,
                value.content_hash,
            )
            for value in coverage.sessions
        ]
        lifecycle_rows = [
            (
                uuid5(_LIFECYCLE_NAMESPACE, value.content_hash),
                value.source,
                value.instrument,
                value.list_date,
                value.delist_date,
                value.available_at,
                value.response_hash,
                value.content_hash,
            )
            for value in coverage.lifecycles
        ]
        suspension_rows = [
            (
                uuid5(_SUSPENSION_NAMESPACE, value.content_hash),
                value.source,
                value.instrument,
                value.session_date,
                value.suspended,
                value.available_at,
                value.response_hash,
                value.content_hash,
            )
            for value in coverage.suspensions
        ]
        limit_rows = [
            (
                uuid5(_LIMIT_NAMESPACE, value.content_hash),
                value.source,
                value.instrument,
                value.session_date,
                value.pre_close,
                value.up_limit,
                value.down_limit,
                value.available_at,
                value.response_hash,
                value.content_hash,
            )
            for value in coverage.price_limits
        ]
        for values in (
            coverage.sessions,
            coverage.lifecycles,
            coverage.suspensions,
            coverage.price_limits,
        ):
            if any(value.source != self._source for value in values):
                raise ValueError("coverage source does not match repository source")
        written = 0
        for table, columns, rows in (
            (self._session_table, _SESSION_INSERT_COLUMNS, session_rows),
            (self._lifecycle_table, _LIFECYCLE_INSERT_COLUMNS, lifecycle_rows),
            (self._suspension_table, _SUSPENSION_INSERT_COLUMNS, suspension_rows),
            (self._limit_table, _LIMIT_INSERT_COLUMNS, limit_rows),
        ):
            written += await self._append(table, columns, rows)
        return written

    async def query_bars_as_of(
        self,
        instruments: tuple[str, ...],
        start: date,
        end: date,
        as_of: datetime,
    ) -> tuple[DailyBarRevision, ...]:
        rows = await self._query(
            table=self._bar_table,
            result_columns=self.BAR_RESULT_COLUMNS,
            tuple_columns=self.BAR_RESULT_COLUMNS[3:],
            instruments=instruments,
            start=start,
            end=end,
            as_of=as_of,
        )
        try:
            return tuple(self._map_bar(row) for row in rows)
        except (IndexError, TypeError, ValueError):
            raise PersistenceUnavailableError(
                "ClickHouse returned malformed daily-bar rows"
            ) from None

    async def query_factors_as_of(
        self,
        instruments: tuple[str, ...],
        start: date,
        end: date,
        as_of: datetime,
    ) -> tuple[AdjustmentFactorRevision, ...]:
        rows = await self._query(
            table=self._factor_table,
            result_columns=self.FACTOR_RESULT_COLUMNS,
            tuple_columns=self.FACTOR_RESULT_COLUMNS[3:],
            instruments=instruments,
            start=start,
            end=end,
            as_of=as_of,
        )
        try:
            return tuple(self._map_factor(row) for row in rows)
        except (IndexError, TypeError, ValueError):
            raise PersistenceUnavailableError(
                "ClickHouse returned malformed adjustment-factor rows"
            ) from None

    async def query_coverage_as_of(
        self,
        instruments: tuple[str, ...],
        start: date,
        end: date,
        as_of: datetime,
    ) -> DailyCoverageEvidence:
        self._validate_query(instruments, start, end)
        cutoff = to_utc(as_of, name="as_of")
        parameters: dict[str, object] = {
            "source": self._source,
            "instruments": list(instruments),
            "start_date": start,
            "end_date": end,
            "as_of_64": cutoff,
        }
        sessions = await self._coverage_query(
            f"""
SELECT source, session_date,
       tupleElement(latest, 1) AS is_open,
       tupleElement(latest, 2) AS available_at,
       tupleElement(latest, 3) AS response_hash,
       tupleElement(latest, 4) AS content_hash
FROM
(
    SELECT source, session_date,
           argMax(tuple(is_open, available_at, response_hash, content_hash),
                  tuple(available_at, record_id)) AS latest
    FROM {self._session_table}
    WHERE source = {{source:String}}
      AND session_date BETWEEN {{start_date:Date}} AND {{end_date:Date}}
      AND available_at <= {{as_of:DateTime64(6, 'UTC')}}
    GROUP BY source, session_date
)
ORDER BY session_date
""".strip(),
            ("source", "session_date", "is_open", "available_at", "response_hash", "content_hash"),
            parameters,
        )
        lifecycles = await self._coverage_query(
            f"""
SELECT source, instrument,
       tupleElement(latest, 1) AS list_date,
       tupleElement(latest, 2) AS delist_date,
       tupleElement(latest, 3) AS available_at,
       tupleElement(latest, 4) AS response_hash,
       tupleElement(latest, 5) AS content_hash
FROM
(
    SELECT source, instrument,
           argMax(tuple(list_date, delist_date, available_at, response_hash, content_hash),
                  tuple(available_at, record_id)) AS latest
    FROM {self._lifecycle_table}
    WHERE source = {{source:String}}
      AND instrument IN {{instruments:Array(String)}}
      AND available_at <= {{as_of:DateTime64(6, 'UTC')}}
    GROUP BY source, instrument
)
ORDER BY instrument
""".strip(),
            (
                "source",
                "instrument",
                "list_date",
                "delist_date",
                "available_at",
                "response_hash",
                "content_hash",
            ),
            parameters,
        )
        suspensions = await self._instrument_coverage_query(
            self._suspension_table,
            ("suspended", "available_at", "response_hash", "content_hash"),
            parameters,
        )
        limits = await self._instrument_coverage_query(
            self._limit_table,
            (
                "pre_close", "up_limit", "down_limit", "available_at",
                "response_hash", "content_hash",
            ),
            parameters,
        )
        try:
            return DailyCoverageEvidence(
                sessions=tuple(self._map_session(row) for row in sessions),
                lifecycles=tuple(self._map_lifecycle(row) for row in lifecycles),
                suspensions=tuple(self._map_suspension(row) for row in suspensions),
                price_limits=tuple(self._map_limit(row) for row in limits),
            )
        except (IndexError, TypeError, ValueError):
            raise PersistenceUnavailableError(
                "ClickHouse returned malformed daily coverage rows"
            ) from None

    async def _instrument_coverage_query(
        self,
        table: str,
        value_columns: tuple[str, ...],
        parameters: dict[str, object],
    ) -> tuple[tuple[Any, ...], ...]:
        result_columns = ("source", "instrument", "session_date", *value_columns)
        projected = ", ".join(
            f"tupleElement(latest, {index}) AS {column}"
            for index, column in enumerate(value_columns, start=1)
        )
        sql = f"""
SELECT source, instrument, session_date, {projected}
FROM
(
    SELECT source, instrument, session_date,
           argMax(tuple({', '.join(value_columns)}),
                  tuple(available_at, record_id)) AS latest
    FROM {table}
    WHERE source = {{source:String}}
      AND instrument IN {{instruments:Array(String)}}
      AND session_date BETWEEN {{start_date:Date}} AND {{end_date:Date}}
      AND available_at <= {{as_of:DateTime64(6, 'UTC')}}
    GROUP BY source, instrument, session_date
)
ORDER BY instrument, session_date
""".strip()
        start = parameters.get("start_date")
        end = parameters.get("end_date")
        if not isinstance(start, date) or not isinstance(end, date):
            raise ValueError(
                "coverage query requires date boundaries"
            )
        rows: list[tuple[Any, ...]] = []
        for chunk_start, chunk_end in _year_intervals(start, end):
            chunk_parameters = {
                **parameters,
                "start_date": chunk_start,
                "end_date": chunk_end,
            }
            rows.extend(
                await self._coverage_query(
                    sql,
                    result_columns,
                    chunk_parameters,
                )
            )
        return tuple(rows)

    async def _coverage_query(
        self,
        sql: str,
        columns: tuple[str, ...],
        parameters: dict[str, object],
    ) -> tuple[tuple[Any, ...], ...]:
        try:
            result = await self._client.query(
                query=sql,
                parameters=parameters,
                settings=_HISTORICAL_QUERY_SETTINGS,
                tz_mode="aware",
            )
            rows = tuple(tuple(row) for row in result.result_rows)
            result_columns = tuple(result.column_names)
            if not rows and not result_columns:
                return ()
            if result_columns != columns:
                raise ValueError("unexpected columns")
            return rows
        except Exception as error:
            raise PersistenceUnavailableError(
                "ClickHouse daily coverage query failed"
            ) from error

    async def _append(
        self,
        table: str,
        columns: tuple[str, ...],
        rows: list[tuple[Any, ...]],
    ) -> int:
        if not rows:
            return 0
        try:
            summary = await self._client.insert(
                table=table, data=rows, column_names=columns
            )
            written_rows = summary.written_rows
        except Exception:
            raise PersistenceUnavailableError("ClickHouse daily append failed") from None
        if (
            not isinstance(written_rows, int)
            or isinstance(written_rows, bool)
            or written_rows != len(rows)
        ):
            raise PersistenceUnavailableError("ClickHouse daily append row count mismatch")
        return written_rows

    async def _query(
        self,
        *,
        table: str,
        result_columns: tuple[str, ...],
        tuple_columns: tuple[str, ...],
        instruments: tuple[str, ...],
        start: date,
        end: date,
        as_of: datetime,
    ) -> tuple[tuple[Any, ...], ...]:
        self._validate_query(instruments, start, end)
        if not instruments:
            return ()
        cutoff = to_utc(as_of, name="as_of")
        base_parameters: dict[str, object] = {
            "source": self._source,
            "instruments": list(instruments),
            "as_of_64": cutoff,
        }
        sql = self._as_of_sql(table, result_columns, tuple_columns)
        rows: list[tuple[Any, ...]] = []
        for chunk_start, chunk_end in _year_intervals(start, end):
            parameters = {
                **base_parameters,
                "start_date": chunk_start,
                "end_date": chunk_end,
            }
            try:
                result = await self._client.query(
                    query=sql,
                    parameters=parameters,
                    settings=_HISTORICAL_QUERY_SETTINGS,
                    tz_mode="aware",
                )
                chunk_rows = tuple(
                    tuple(row) for row in result.result_rows
                )
                actual_columns = tuple(result.column_names)
                if not chunk_rows and not actual_columns:
                    continue
                if actual_columns != result_columns:
                    raise ValueError("unexpected columns")
                rows.extend(chunk_rows)
            except Exception as error:
                raise PersistenceUnavailableError(
                    "ClickHouse returned malformed daily rows"
                ) from error
        return tuple(rows)

    @staticmethod
    def _as_of_sql(
        table: str,
        result_columns: tuple[str, ...],
        tuple_columns: tuple[str, ...],
    ) -> str:
        outer = ["source", "instrument", "session_date"]
        outer.extend(
            f"tupleElement(latest, {index}) AS {column}"
            for index, column in enumerate(tuple_columns, start=1)
        )
        return f"""
SELECT
    {', '.join(outer)}
FROM
(
    SELECT
        source,
        instrument,
        session_date,
        argMax(
            tuple({', '.join(tuple_columns)}),
            tuple(available_at, ingested_at, record_id)
        ) AS latest
    FROM {table}
    WHERE source = {{source:String}}
      AND instrument IN {{instruments:Array(String)}}
      AND session_date >= {{start_date:Date}}
      AND session_date <= {{end_date:Date}}
      AND available_at <= {{as_of:DateTime64(6, 'UTC')}}
    GROUP BY source, instrument, session_date
)
ORDER BY instrument, session_date, source
""".strip()

    @classmethod
    def _map_bar(cls, row: tuple[Any, ...]) -> DailyBarRevision:
        if len(row) != len(cls.BAR_RESULT_COLUMNS):
            raise ValueError("unexpected row shape")
        revision = DailyBarRevision.from_values(
            source=cls._string(row[0]),
            instrument=cls._string(row[1]),
            session_date=cls._date(row[2]),
            event_time=cls._datetime(row[3]),
            available_at=cls._datetime(row[4]),
            ingested_at=cls._datetime(row[5]),
            source_revision=cls._string(row[6]),
            availability_policy=cls._string(row[7]),
            evidence_hash=cls._string(row[8]),
            open_price=cls._decimal(row[9]),
            high_price=cls._decimal(row[10]),
            low_price=cls._decimal(row[11]),
            close_price=cls._decimal(row[12]),
            pre_close=cls._decimal(row[13]),
            volume=cls._integer(row[14]),
            turnover=cls._decimal(row[15]),
        )
        if revision.content_hash != cls._string(row[16]):
            raise ValueError("content hash mismatch")
        return revision

    @classmethod
    def _map_factor(cls, row: tuple[Any, ...]) -> AdjustmentFactorRevision:
        if len(row) != len(cls.FACTOR_RESULT_COLUMNS):
            raise ValueError("unexpected row shape")
        revision = AdjustmentFactorRevision.from_values(
            source=cls._string(row[0]),
            instrument=cls._string(row[1]),
            session_date=cls._date(row[2]),
            event_time=cls._datetime(row[3]),
            available_at=cls._datetime(row[4]),
            ingested_at=cls._datetime(row[5]),
            source_revision=cls._string(row[6]),
            availability_policy=cls._string(row[7]),
            evidence_hash=cls._string(row[8]),
            factor=cls._decimal(row[9]),
        )
        if revision.content_hash != cls._string(row[10]):
            raise ValueError("content hash mismatch")
        return revision

    @classmethod
    def _map_session(cls, row: tuple[Any, ...]) -> TradingSession:
        revision = TradingSession(
            source=cls._string(row[0]),
            session_date=cls._date(row[1]),
            is_open=cls._boolean(row[2]),
            available_at=cls._datetime(row[3]),
            response_hash=cls._string(row[4]),
        )
        if revision.content_hash != cls._string(row[5]):
            raise ValueError("content hash mismatch")
        return revision

    @classmethod
    def _map_lifecycle(cls, row: tuple[Any, ...]) -> InstrumentLifecycle:
        raw_delist = row[3]
        revision = InstrumentLifecycle(
            source=cls._string(row[0]),
            instrument=cls._string(row[1]),
            list_date=cls._date(row[2]),
            delist_date=None if raw_delist is None else cls._date(raw_delist),
            available_at=cls._datetime(row[4]),
            response_hash=cls._string(row[5]),
        )
        if revision.content_hash != cls._string(row[6]):
            raise ValueError("content hash mismatch")
        return revision

    @classmethod
    def _map_suspension(cls, row: tuple[Any, ...]) -> DailySuspensionStatus:
        revision = DailySuspensionStatus(
            source=cls._string(row[0]),
            instrument=cls._string(row[1]),
            session_date=cls._date(row[2]),
            suspended=cls._boolean(row[3]),
            available_at=cls._datetime(row[4]),
            response_hash=cls._string(row[5]),
        )
        if revision.content_hash != cls._string(row[6]):
            raise ValueError("content hash mismatch")
        return revision

    @classmethod
    def _map_limit(cls, row: tuple[Any, ...]) -> DailyPriceLimit:
        revision = DailyPriceLimit(
            source=cls._string(row[0]),
            instrument=cls._string(row[1]),
            session_date=cls._date(row[2]),
            pre_close=cls._decimal(row[3]),
            up_limit=cls._decimal(row[4]),
            down_limit=cls._decimal(row[5]),
            available_at=cls._datetime(row[6]),
            response_hash=cls._string(row[7]),
        )
        if revision.content_hash != cls._string(row[8]):
            raise ValueError("content hash mismatch")
        return revision

    @staticmethod
    def bar_result_row(record: DailyBarRevision) -> tuple[Any, ...]:
        return (
            record.source,
            record.instrument,
            record.session_date,
            record.event_time,
            record.available_at,
            record.ingested_at,
            record.source_revision,
            record.availability_policy,
            record.evidence_hash,
            record.open_price,
            record.high_price,
            record.low_price,
            record.close_price,
            record.pre_close,
            record.volume,
            record.turnover,
            record.content_hash,
        )

    @staticmethod
    def factor_result_row(record: AdjustmentFactorRevision) -> tuple[Any, ...]:
        return (
            record.source,
            record.instrument,
            record.session_date,
            record.event_time,
            record.available_at,
            record.ingested_at,
            record.source_revision,
            record.availability_policy,
            record.evidence_hash,
            record.factor,
            record.content_hash,
        )

    def _validate_source(self, source: str) -> None:
        if source != self._source:
            raise ValueError("record source does not match repository source")

    @staticmethod
    def _record_id(namespace: UUID, content_hash: str, ingested_at: datetime) -> UUID:
        return uuid5(
            namespace,
            f"{content_hash}:{ingested_at.isoformat(timespec='microseconds')}",
        )

    @classmethod
    def _validate_bar_decimals(cls, record: DailyBarRevision) -> None:
        for field in (
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "pre_close",
        ):
            cls._require_exact_decimal(
                getattr(record, field), name=field, precision=20, scale=6
            )
        cls._require_exact_decimal(
            record.turnover, name="turnover", precision=24, scale=4
        )

    @staticmethod
    def _require_exact_decimal(
        value: Decimal, *, name: str, precision: int, scale: int
    ) -> None:
        components = value.as_tuple()
        exponent = components.exponent
        if not isinstance(exponent, int):
            raise ValueError(f"{name} must be finite")
        digits = list(components.digits)
        while digits and digits[-1] == 0:
            digits.pop()
            exponent += 1
        while digits and digits[0] == 0:
            digits.pop(0)
        if not digits:
            return
        if exponent < -scale or len(digits) + exponent + scale > precision:
            raise ValueError(
                f"{name} is not exactly representable as Decimal({precision}, {scale})"
            )

    @staticmethod
    def _validate_identity(source: str, *tables: str) -> None:
        if not isinstance(source, str) or not source.strip():
            raise ValueError("source cannot be empty")
        if any(
            not isinstance(table, str) or _TABLE_IDENTIFIER.fullmatch(table) is None
            for table in tables
        ):
            raise ValueError("table must be a safe ClickHouse identifier")

    @staticmethod
    def _validate_query(instruments: tuple[str, ...], start: date, end: date) -> None:
        if start > end:
            raise ValueError("start cannot follow end")
        if any(not isinstance(value, str) or not value.strip() for value in instruments):
            raise ValueError("instruments must contain nonempty strings")
        if len(set(instruments)) != len(instruments):
            raise ValueError("instruments must be unique")

    @staticmethod
    def _string(value: object) -> str:
        if isinstance(value, bytes):
            try:
                return value.decode("ascii")
            except UnicodeDecodeError:
                raise TypeError("expected ASCII string") from None
        if not isinstance(value, str):
            raise TypeError("expected string")
        return value

    @staticmethod
    def _date(value: object) -> date:
        if not isinstance(value, date) or isinstance(value, datetime):
            raise TypeError("expected date")
        return value

    @staticmethod
    def _datetime(value: object) -> datetime:
        if not isinstance(value, datetime):
            raise TypeError("expected datetime")
        return value

    @staticmethod
    def _decimal(value: object) -> Decimal:
        if not isinstance(value, Decimal):
            raise TypeError("expected Decimal")
        return value

    @staticmethod
    def _integer(value: object) -> int:
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError("expected integer")
        return value

    @staticmethod
    def _boolean(value: object) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and value in (0, 1):
            return bool(value)
        raise TypeError("expected boolean")
