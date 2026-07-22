from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid5

import clickhouse_connect  # type: ignore[import-untyped]
from clickhouse_connect.driver.asyncclient import (  # type: ignore[import-untyped]
    AsyncClient,
)

from autoquant.clock import to_utc
from autoquant.data.daily_models import AdjustmentFactorRevision, DailyBarRevision
from autoquant.errors import PersistenceUnavailableError

_BAR_TABLE = "daily_bar_revisions"
_FACTOR_TABLE = "adjustment_factor_revisions"
_TABLE_IDENTIFIER = re.compile(
    r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?\Z"
)
_BAR_NAMESPACE = UUID("59a27670-8f4f-4d69-a823-d3cc2406d8a5")
_FACTOR_NAMESPACE = UUID("7fcb33c7-1470-4e80-a702-afcf797cc651")

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
    ) -> None:
        self._validate_identity(source, bar_table, factor_table)
        self._client = client
        self._source = source
        self._bar_table = bar_table
        self._factor_table = factor_table

    @classmethod
    async def connect(
        cls,
        *,
        dsn: str,
        source: str,
        bar_table: str = _BAR_TABLE,
        factor_table: str = _FACTOR_TABLE,
    ) -> ClickHouseDailyRepository:
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("dsn cannot be empty")
        cls._validate_identity(source, bar_table, factor_table)
        try:
            client = await clickhouse_connect.get_async_client(dsn=dsn, tz_mode="aware")
        except Exception:
            raise PersistenceUnavailableError("ClickHouse connection failed") from None
        return cls(
            client=client,
            source=source,
            bar_table=bar_table,
            factor_table=factor_table,
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
            version = await self._client.command(
                "SELECT max(version) FROM schema_versions WHERE component = 'clickhouse'"
            )
        except Exception:
            raise PersistenceUnavailableError("ClickHouse connection check failed") from None
        if bar_exists not in (1, "1", True) or factor_exists not in (1, "1", True):
            raise PersistenceUnavailableError("ClickHouse daily schema is unavailable")
        if version != 2:
            raise PersistenceUnavailableError("ClickHouse daily schema version is unavailable")

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
        parameters: dict[str, object] = {
            "source": self._source,
            "instruments": list(instruments),
            "start_date": start,
            "end_date": end,
            "as_of": cutoff,
        }
        sql = self._as_of_sql(table, result_columns, tuple_columns)
        try:
            result = await self._client.query(
                query=sql, parameters=parameters, tz_mode="aware"
            )
            if tuple(result.column_names) != result_columns:
                raise ValueError("unexpected columns")
            return tuple(tuple(row) for row in result.result_rows)
        except Exception:
            raise PersistenceUnavailableError(
                "ClickHouse returned malformed daily rows"
            ) from None

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
