from datetime import date, datetime
from typing import Protocol

from autoquant.data.fundamental_models import (
    DailyValuationRevision,
    FinancialIndicatorRevision,
    FundamentalDatasetBatch,
)


class FundamentalDataSource(Protocol):
    async def fetch_fundamental_dataset(
        self,
        instruments: tuple[str, ...],
        start: date,
        end: date,
    ) -> FundamentalDatasetBatch: ...


class FundamentalRepository(Protocol):
    async def append_valuations(
        self,
        records: tuple[DailyValuationRevision, ...],
    ) -> int: ...

    async def append_indicators(
        self,
        records: tuple[FinancialIndicatorRevision, ...],
    ) -> int: ...

    async def query_valuations_as_of(
        self,
        instruments: tuple[str, ...],
        start: date,
        end: date,
        as_of: datetime,
    ) -> tuple[DailyValuationRevision, ...]: ...

    async def query_indicator_revisions_as_of(
        self,
        instruments: tuple[str, ...],
        announced_start: date,
        announced_end: date,
        as_of: datetime,
    ) -> tuple[FinancialIndicatorRevision, ...]: ...
