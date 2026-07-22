from datetime import date, datetime
from typing import Protocol

from autoquant.data.daily_models import (
    AdjustmentFactorRevision,
    DailyBarRevision,
    DailyDatasetBatch,
)


class DailyDataSource(Protocol):
    async def fetch_daily_dataset(
        self, instruments: tuple[str, ...], start: date, end: date
    ) -> DailyDatasetBatch: ...


class DailyMarketRepository(Protocol):
    async def append_bars(self, records: tuple[DailyBarRevision, ...]) -> int: ...

    async def append_factors(
        self, records: tuple[AdjustmentFactorRevision, ...]
    ) -> int: ...

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
