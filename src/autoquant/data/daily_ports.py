from datetime import date, datetime
from typing import Protocol

from autoquant.data.daily_models import (
    AdjustmentFactorRevision,
    DailyBarRevision,
    DailyCoverageEvidence,
    DailyDatasetBatch,
    SessionReferenceBatch,
    TradingCalendarBatch,
)


class DailyDataSource(Protocol):
    async def fetch_daily_dataset(
        self, instruments: tuple[str, ...], start: date, end: date
    ) -> DailyDatasetBatch: ...


class TradingCalendarSource(Protocol):
    async def fetch_trading_calendar(
        self,
        start: date,
        end: date,
    ) -> TradingCalendarBatch: ...


class SessionReferenceSource(Protocol):
    async def fetch_session_reference(
        self,
        instruments: tuple[str, ...],
        session_date: date,
    ) -> SessionReferenceBatch: ...


class DailyMarketRepository(Protocol):
    async def append_bars(self, records: tuple[DailyBarRevision, ...]) -> int: ...

    async def append_factors(
        self, records: tuple[AdjustmentFactorRevision, ...]
    ) -> int: ...

    async def append_coverage(self, coverage: DailyCoverageEvidence) -> int: ...

    async def query_bars_as_of(
        self,
        instruments: tuple[str, ...],
        start: date,
        end: date,
        as_of: datetime,
    ) -> tuple[DailyBarRevision, ...]: ...

    async def query_factors_as_of(
        self,
        instruments: tuple[str, ...],
        start: date,
        end: date,
        as_of: datetime,
    ) -> tuple[AdjustmentFactorRevision, ...]: ...

    async def query_coverage_as_of(
        self,
        instruments: tuple[str, ...],
        start: date,
        end: date,
        as_of: datetime,
    ) -> DailyCoverageEvidence: ...
