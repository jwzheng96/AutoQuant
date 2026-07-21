from datetime import datetime
from typing import Protocol

from open_quant.data.models import (
    CoverageBatch,
    DatasetManifest,
    MinuteBarBatch,
    MinuteBarRevision,
)


class MarketDataSource(Protocol):
    async def fetch_minute_bars(
        self, instruments: tuple[str, ...], start: datetime, end: datetime
    ) -> MinuteBarBatch: ...

    async def fetch_coverage_evidence(
        self, instruments: tuple[str, ...], start: datetime, end: datetime
    ) -> CoverageBatch: ...


class MinuteBarRepository(Protocol):
    async def append(self, records: tuple[MinuteBarRevision, ...]) -> int: ...

    async def query_as_of(
        self, instruments: tuple[str, ...], start: datetime, end: datetime, as_of: datetime
    ) -> tuple[MinuteBarRevision, ...]: ...


class ManifestRepository(Protocol):
    async def save_manifest(self, manifest: DatasetManifest) -> None: ...
