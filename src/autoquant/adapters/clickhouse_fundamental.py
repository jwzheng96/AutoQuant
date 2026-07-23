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
from autoquant.data.fundamental_models import (
    DailyValuationRevision,
    FinancialIndicatorRevision,
)
from autoquant.errors import PersistenceUnavailableError

_VALUATION_TABLE = "daily_valuation_revisions"
_INDICATOR_TABLE = "financial_indicator_revisions"
_TABLE_IDENTIFIER = re.compile(
    r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?\Z"
)
_QUERY_SETTINGS: dict[str, int] = {
    "max_block_size": 1_024,
    "max_bytes_before_external_group_by": 32 * 1024 * 1024,
    "max_threads": 1,
}
_VALUATION_NAMESPACE = UUID("86d71ad9-ee26-433a-ac34-f2e20cf566f7")
_INDICATOR_NAMESPACE = UUID("aeb42b50-2df1-453c-8ec2-b48d7b5a1180")

_VALUATION_COLUMNS = (
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
    "close_price",
    "free_float_turnover_rate_percent",
    "pe_ttm",
    "pb",
    "ps_ttm",
    "dividend_yield_ttm_percent",
    "total_market_value_cny",
    "circulating_market_value_cny",
    "content_hash",
)
_INDICATOR_COLUMNS = (
    "record_id",
    "source",
    "instrument",
    "report_period",
    "announced_date",
    "updated",
    "event_time",
    "available_at",
    "ingested_at",
    "source_revision",
    "availability_policy",
    "evidence_hash",
    "roe_diluted_percent",
    "roa_percent",
    "gross_profit_margin_percent",
    "debt_to_assets_percent",
    "operating_cashflow_to_revenue_percent",
    "content_hash",
)


def _year_intervals(
    start: date,
    end: date,
) -> tuple[tuple[date, date], ...]:
    if start > end:
        raise ValueError("historical interval is invalid")
    values: list[tuple[date, date]] = []
    current = start
    while current <= end:
        interval_end = min(end, date(current.year, 12, 31))
        values.append((current, interval_end))
        current = date(current.year + 1, 1, 1)
    return tuple(values)


class ClickHouseFundamentalRepository:
    VALUATION_RESULT_COLUMNS = _VALUATION_COLUMNS[1:]
    INDICATOR_RESULT_COLUMNS = _INDICATOR_COLUMNS[1:]

    def __init__(
        self,
        *,
        client: AsyncClient,
        source: str,
        valuation_table: str = _VALUATION_TABLE,
        indicator_table: str = _INDICATOR_TABLE,
    ) -> None:
        self._validate_identity(
            source,
            valuation_table,
            indicator_table,
        )
        self._client = client
        self._source = source
        self._valuation_table = valuation_table
        self._indicator_table = indicator_table

    @classmethod
    async def connect(
        cls,
        *,
        dsn: str,
        source: str,
        valuation_table: str = _VALUATION_TABLE,
        indicator_table: str = _INDICATOR_TABLE,
    ) -> ClickHouseFundamentalRepository:
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("dsn cannot be empty")
        cls._validate_identity(
            source,
            valuation_table,
            indicator_table,
        )
        try:
            client = await clickhouse_connect.get_async_client(
                dsn=dsn,
                tz_mode="aware",
            )
        except Exception:
            raise PersistenceUnavailableError(
                "ClickHouse fundamental connection failed"
            ) from None
        return cls(
            client=client,
            source=source,
            valuation_table=valuation_table,
            indicator_table=indicator_table,
        )

    @property
    def client(self) -> AsyncClient:
        return self._client

    @property
    def source(self) -> str:
        return self._source

    async def check_connection(self) -> None:
        try:
            valuation_exists = await self._client.command(
                f"EXISTS TABLE {self._valuation_table}"
            )
            indicator_exists = await self._client.command(
                f"EXISTS TABLE {self._indicator_table}"
            )
            version = await self._client.command(
                "SELECT max(version) FROM schema_versions "
                "WHERE component = 'clickhouse'"
            )
        except Exception:
            raise PersistenceUnavailableError(
                "ClickHouse fundamental connection check failed"
            ) from None
        if (
            valuation_exists not in (1, "1", True)
            or indicator_exists not in (1, "1", True)
            or not isinstance(version, int)
            or isinstance(version, bool)
            or version < 4
        ):
            raise PersistenceUnavailableError(
                "ClickHouse fundamental schema is unavailable"
            )

    async def append_valuations(
        self,
        records: tuple[DailyValuationRevision, ...],
    ) -> int:
        rows: list[tuple[Any, ...]] = []
        identities: set[UUID] = set()
        for record in records:
            if not isinstance(record, DailyValuationRevision):
                raise TypeError(
                    "records must contain DailyValuationRevision values"
                )
            self._validate_source(record.source)
            self._validate_valuation_decimals(record)
            record_id = self._record_id(
                _VALUATION_NAMESPACE,
                record.content_hash,
                record.ingested_at,
            )
            if record_id in identities:
                raise ValueError(
                    "duplicate daily valuation revision in append batch"
                )
            identities.add(record_id)
            rows.append(
                (
                    record_id,
                    record.source,
                    record.instrument,
                    record.session_date,
                    record.event_time,
                    record.available_at,
                    record.ingested_at,
                    record.source_revision,
                    record.availability_policy,
                    record.evidence_hash,
                    record.close_price,
                    record.free_float_turnover_rate_percent,
                    record.pe_ttm,
                    record.pb,
                    record.ps_ttm,
                    record.dividend_yield_ttm_percent,
                    record.total_market_value_cny,
                    record.circulating_market_value_cny,
                    record.content_hash,
                )
            )
        return await self._append(
            self._valuation_table,
            _VALUATION_COLUMNS,
            rows,
        )

    async def append_indicators(
        self,
        records: tuple[FinancialIndicatorRevision, ...],
    ) -> int:
        rows: list[tuple[Any, ...]] = []
        identities: set[UUID] = set()
        for record in records:
            if not isinstance(record, FinancialIndicatorRevision):
                raise TypeError(
                    "records must contain FinancialIndicatorRevision values"
                )
            self._validate_source(record.source)
            self._validate_indicator_decimals(record)
            record_id = self._record_id(
                _INDICATOR_NAMESPACE,
                record.content_hash,
                record.ingested_at,
            )
            if record_id in identities:
                raise ValueError(
                    "duplicate financial indicator revision in append batch"
                )
            identities.add(record_id)
            rows.append(
                (
                    record_id,
                    record.source,
                    record.instrument,
                    record.report_period,
                    record.announced_date,
                    record.updated,
                    record.event_time,
                    record.available_at,
                    record.ingested_at,
                    record.source_revision,
                    record.availability_policy,
                    record.evidence_hash,
                    record.roe_diluted_percent,
                    record.roa_percent,
                    record.gross_profit_margin_percent,
                    record.debt_to_assets_percent,
                    record.operating_cashflow_to_revenue_percent,
                    record.content_hash,
                )
            )
        return await self._append(
            self._indicator_table,
            _INDICATOR_COLUMNS,
            rows,
        )

    async def query_valuations_as_of(
        self,
        instruments: tuple[str, ...],
        start: date,
        end: date,
        as_of: datetime,
    ) -> tuple[DailyValuationRevision, ...]:
        self._validate_query(instruments, start, end)
        if not instruments:
            return ()
        tuple_columns = self.VALUATION_RESULT_COLUMNS[3:]
        outer = ["source", "instrument", "session_date"]
        outer.extend(
            f"tupleElement(latest, {index}) AS {column}"
            for index, column in enumerate(tuple_columns, start=1)
        )
        sql = f"""
SELECT {', '.join(outer)}
FROM
(
    SELECT source, instrument, session_date,
           argMax(
               tuple({', '.join(tuple_columns)}),
               tuple(available_at, ingested_at, record_id)
           ) AS latest
    FROM {self._valuation_table}
    WHERE source = {{source:String}}
      AND instrument IN {{instruments:Array(String)}}
      AND session_date BETWEEN {{start_date:Date}} AND {{end_date:Date}}
      AND available_at <= {{as_of:DateTime64(6, 'UTC')}}
      AND ingested_at <= {{as_of:DateTime64(6, 'UTC')}}
    GROUP BY source, instrument, session_date
)
ORDER BY instrument, session_date, source
""".strip()
        rows = await self._query_years(
            sql=sql,
            columns=self.VALUATION_RESULT_COLUMNS,
            instruments=instruments,
            start=start,
            end=end,
            as_of=as_of,
        )
        try:
            return tuple(self._map_valuation(row) for row in rows)
        except (IndexError, TypeError, ValueError):
            raise PersistenceUnavailableError(
                "ClickHouse returned malformed valuation rows"
            ) from None

    async def query_indicator_revisions_as_of(
        self,
        instruments: tuple[str, ...],
        announced_start: date,
        announced_end: date,
        as_of: datetime,
    ) -> tuple[FinancialIndicatorRevision, ...]:
        self._validate_query(
            instruments,
            announced_start,
            announced_end,
        )
        if not instruments:
            return ()
        tuple_columns = self.INDICATOR_RESULT_COLUMNS[5:]
        outer = [
            "source",
            "instrument",
            "report_period",
            "announced_date",
            "updated",
        ]
        outer.extend(
            f"tupleElement(latest, {index}) AS {column}"
            for index, column in enumerate(tuple_columns, start=1)
        )
        sql = f"""
SELECT {', '.join(outer)}
FROM
(
    SELECT source, instrument, report_period, announced_date, updated,
           argMax(
               tuple({', '.join(tuple_columns)}),
               tuple(available_at, ingested_at, record_id)
           ) AS latest
    FROM {self._indicator_table}
    WHERE source = {{source:String}}
      AND instrument IN {{instruments:Array(String)}}
      AND announced_date BETWEEN {{start_date:Date}} AND {{end_date:Date}}
      AND available_at <= {{as_of:DateTime64(6, 'UTC')}}
      AND ingested_at <= {{as_of:DateTime64(6, 'UTC')}}
    GROUP BY
        source, instrument, report_period, announced_date, updated
)
ORDER BY
    instrument, announced_date, report_period, updated, source
""".strip()
        rows = await self._query_years(
            sql=sql,
            columns=self.INDICATOR_RESULT_COLUMNS,
            instruments=instruments,
            start=announced_start,
            end=announced_end,
            as_of=as_of,
        )
        try:
            return tuple(self._map_indicator(row) for row in rows)
        except (IndexError, TypeError, ValueError):
            raise PersistenceUnavailableError(
                "ClickHouse returned malformed financial indicator rows"
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
                table=table,
                data=rows,
                column_names=columns,
            )
            written = summary.written_rows
        except Exception:
            raise PersistenceUnavailableError(
                "ClickHouse fundamental append failed"
            ) from None
        if (
            not isinstance(written, int)
            or isinstance(written, bool)
            or written != len(rows)
        ):
            raise PersistenceUnavailableError(
                "ClickHouse fundamental append row count mismatch"
            )
        return written

    async def _query_years(
        self,
        *,
        sql: str,
        columns: tuple[str, ...],
        instruments: tuple[str, ...],
        start: date,
        end: date,
        as_of: datetime,
    ) -> tuple[tuple[Any, ...], ...]:
        cutoff = to_utc(as_of, name="as_of")
        rows: list[tuple[Any, ...]] = []
        for interval_start, interval_end in _year_intervals(start, end):
            try:
                result = await self._client.query(
                    query=sql,
                    parameters={
                        "source": self._source,
                        "instruments": list(instruments),
                        "start_date": interval_start,
                        "end_date": interval_end,
                        "as_of": cutoff,
                    },
                    settings=_QUERY_SETTINGS,
                    tz_mode="aware",
                )
                chunk = tuple(tuple(row) for row in result.result_rows)
                actual_columns = tuple(result.column_names)
                if not chunk and not actual_columns:
                    continue
                if actual_columns != columns:
                    raise ValueError("unexpected columns")
                rows.extend(chunk)
            except Exception as error:
                raise PersistenceUnavailableError(
                    "ClickHouse fundamental query failed"
                ) from error
        return tuple(rows)

    @classmethod
    def _map_valuation(
        cls,
        row: tuple[Any, ...],
    ) -> DailyValuationRevision:
        if len(row) != len(cls.VALUATION_RESULT_COLUMNS):
            raise ValueError("unexpected valuation row shape")
        revision = DailyValuationRevision(
            source=cls._string(row[0]),
            instrument=cls._string(row[1]),
            session_date=cls._date(row[2]),
            event_time=cls._datetime(row[3]),
            available_at=cls._datetime(row[4]),
            ingested_at=cls._datetime(row[5]),
            source_revision=cls._string(row[6]),
            availability_policy=cls._string(row[7]),
            evidence_hash=cls._string(row[8]),
            close_price=cls._decimal(row[9]),
            free_float_turnover_rate_percent=cls._optional_decimal(
                row[10]
            ),
            pe_ttm=cls._optional_decimal(row[11]),
            pb=cls._optional_decimal(row[12]),
            ps_ttm=cls._optional_decimal(row[13]),
            dividend_yield_ttm_percent=cls._optional_decimal(row[14]),
            total_market_value_cny=cls._decimal(row[15]),
            circulating_market_value_cny=cls._decimal(row[16]),
        )
        if revision.content_hash != cls._string(row[17]):
            raise ValueError("valuation content hash mismatch")
        return revision

    @classmethod
    def _map_indicator(
        cls,
        row: tuple[Any, ...],
    ) -> FinancialIndicatorRevision:
        if len(row) != len(cls.INDICATOR_RESULT_COLUMNS):
            raise ValueError("unexpected indicator row shape")
        revision = FinancialIndicatorRevision(
            source=cls._string(row[0]),
            instrument=cls._string(row[1]),
            report_period=cls._date(row[2]),
            announced_date=cls._date(row[3]),
            updated=cls._boolean(row[4]),
            event_time=cls._datetime(row[5]),
            available_at=cls._datetime(row[6]),
            ingested_at=cls._datetime(row[7]),
            source_revision=cls._string(row[8]),
            availability_policy=cls._string(row[9]),
            evidence_hash=cls._string(row[10]),
            roe_diluted_percent=cls._optional_decimal(row[11]),
            roa_percent=cls._optional_decimal(row[12]),
            gross_profit_margin_percent=cls._optional_decimal(row[13]),
            debt_to_assets_percent=cls._optional_decimal(row[14]),
            operating_cashflow_to_revenue_percent=(
                cls._optional_decimal(row[15])
            ),
        )
        if revision.content_hash != cls._string(row[16]):
            raise ValueError("indicator content hash mismatch")
        return revision

    def _validate_source(self, source: str) -> None:
        if source != self._source:
            raise ValueError(
                "record source does not match repository source"
            )

    @staticmethod
    def _record_id(
        namespace: UUID,
        content_hash: str,
        ingested_at: datetime,
    ) -> UUID:
        return uuid5(
            namespace,
            f"{content_hash}:{ingested_at.isoformat(timespec='microseconds')}",
        )

    @classmethod
    def _validate_valuation_decimals(
        cls,
        record: DailyValuationRevision,
    ) -> None:
        cls._require_exact_decimal(
            record.close_price,
            name="close_price",
            precision=20,
            scale=6,
        )
        for name in (
            "free_float_turnover_rate_percent",
            "pe_ttm",
            "pb",
            "ps_ttm",
            "dividend_yield_ttm_percent",
        ):
            value = getattr(record, name)
            if value is not None:
                cls._require_exact_decimal(
                    value,
                    name=name,
                    precision=24,
                    scale=8,
                )
        for name in (
            "total_market_value_cny",
            "circulating_market_value_cny",
        ):
            cls._require_exact_decimal(
                getattr(record, name),
                name=name,
                precision=28,
                scale=4,
            )

    @classmethod
    def _validate_indicator_decimals(
        cls,
        record: FinancialIndicatorRevision,
    ) -> None:
        for name in (
            "roe_diluted_percent",
            "roa_percent",
            "gross_profit_margin_percent",
            "debt_to_assets_percent",
            "operating_cashflow_to_revenue_percent",
        ):
            value = getattr(record, name)
            if value is not None:
                cls._require_exact_decimal(
                    value,
                    name=name,
                    precision=24,
                    scale=8,
                )

    @staticmethod
    def _require_exact_decimal(
        value: Decimal,
        *,
        name: str,
        precision: int,
        scale: int,
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
                f"{name} is not exactly representable as "
                f"Decimal({precision}, {scale})"
            )

    @staticmethod
    def _validate_identity(source: str, *tables: str) -> None:
        if not isinstance(source, str) or not source.strip():
            raise ValueError("source cannot be empty")
        if any(
            not isinstance(table, str)
            or _TABLE_IDENTIFIER.fullmatch(table) is None
            for table in tables
        ):
            raise ValueError(
                "table must be a safe ClickHouse identifier"
            )

    @staticmethod
    def _validate_query(
        instruments: tuple[str, ...],
        start: date,
        end: date,
    ) -> None:
        if start > end:
            raise ValueError("start cannot follow end")
        if any(
            not isinstance(value, str) or not value.strip()
            for value in instruments
        ):
            raise ValueError(
                "instruments must contain nonempty strings"
            )
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

    @classmethod
    def _optional_decimal(
        cls,
        value: object,
    ) -> Decimal | None:
        return None if value is None else cls._decimal(value)

    @staticmethod
    def _boolean(value: object) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and value in (0, 1):
            return bool(value)
        raise TypeError("expected boolean")
