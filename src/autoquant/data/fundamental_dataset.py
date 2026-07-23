from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Protocol

from autoquant.clock import to_shanghai
from autoquant.data.fundamental_models import (
    DailyValuationRevision,
    FinancialIndicatorRevision,
)
from autoquant.data.models import (
    DatasetManifest,
    _canonical_hash,
    _require_lowercase_sha256,
)
from autoquant.errors import (
    ManifestIntegrityError,
    PersistenceUnavailableError,
)

FUNDAMENTAL_DATASET_MANIFEST_VERSION = (
    "fundamental-research-dataset-manifest-v1"
)


@dataclass(frozen=True, slots=True)
class FundamentalDatasetShard:
    sequence: int
    instrument: str
    manifest_hash: str

    def __post_init__(self) -> None:
        if self.sequence < 1:
            raise ValueError(
                "fundamental shard sequence must be positive"
            )
        if (
            len(self.instrument) != 11
            or self.instrument[6:] not in {".XSHG", ".XSHE"}
            or not self.instrument[:6].isdigit()
        ):
            raise ValueError(
                "fundamental shard instrument is invalid"
            )
        _require_lowercase_sha256(
            self.manifest_hash,
            name="fundamental shard manifest hash",
        )

    def payload(self) -> dict[str, object]:
        return {
            "instrument": self.instrument,
            "manifest_hash": self.manifest_hash,
            "sequence": self.sequence,
        }


@dataclass(frozen=True, slots=True)
class FundamentalResearchDatasetManifest:
    spec_hash: str
    start_date: date
    end_date: date
    shards: tuple[FundamentalDatasetShard, ...]
    source: str = "tushare-fundamental"
    version: str = FUNDAMENTAL_DATASET_MANIFEST_VERSION
    manifest_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.spec_hash,
            name="fundamental research spec hash",
        )
        shards = tuple(self.shards)
        if self.start_date > self.end_date:
            raise ValueError(
                "fundamental dataset start cannot follow end"
            )
        if (
            not shards
            or len(shards) > 1000
            or tuple(value.sequence for value in shards)
            != tuple(range(1, len(shards) + 1))
            or len({value.instrument for value in shards})
            != len(shards)
            or len({value.manifest_hash for value in shards})
            != len(shards)
            or tuple(value.instrument for value in shards)
            != tuple(
                sorted(value.instrument for value in shards)
            )
        ):
            raise ValueError(
                "fundamental dataset shards are invalid"
            )
        if (
            self.source != "tushare-fundamental"
            or self.version
            != FUNDAMENTAL_DATASET_MANIFEST_VERSION
        ):
            raise ValueError(
                "fundamental dataset version is unsupported"
            )
        object.__setattr__(self, "shards", shards)
        object.__setattr__(
            self,
            "manifest_hash",
            _canonical_hash(self.payload()),
        )

    @property
    def instruments(self) -> tuple[str, ...]:
        return tuple(value.instrument for value in self.shards)

    def payload(self) -> dict[str, object]:
        return {
            "end_date": self.end_date.isoformat(),
            "shards": [value.payload() for value in self.shards],
            "source": self.source,
            "spec_hash": self.spec_hash,
            "start_date": self.start_date.isoformat(),
            "version": self.version,
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> FundamentalResearchDatasetManifest:
        raw_shards = payload["shards"]
        if not isinstance(raw_shards, list):
            raise TypeError(
                "fundamental dataset shards must be a list"
            )
        shards: list[FundamentalDatasetShard] = []
        for raw in raw_shards:
            if not isinstance(raw, dict):
                raise TypeError(
                    "fundamental dataset shard is invalid"
                )
            shards.append(
                FundamentalDatasetShard(
                    sequence=int(str(raw["sequence"])),
                    instrument=str(raw["instrument"]),
                    manifest_hash=str(raw["manifest_hash"]),
                )
            )
        value = cls(
            spec_hash=str(payload["spec_hash"]),
            start_date=date.fromisoformat(
                str(payload["start_date"])
            ),
            end_date=date.fromisoformat(str(payload["end_date"])),
            shards=tuple(shards),
            source=str(payload["source"]),
            version=str(payload["version"]),
        )
        if value.payload() != payload:
            raise ValueError(
                "fundamental dataset payload is not canonical"
            )
        return value


class FundamentalManifestReader(Protocol):
    async def read_manifest(
        self,
        manifest_hash: str,
    ) -> DatasetManifest: ...


class FundamentalAsOfReader(Protocol):
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


@dataclass(frozen=True, slots=True)
class ValidatedFundamentalShard:
    instrument: str
    manifest: DatasetManifest
    valuations: tuple[DailyValuationRevision, ...]
    indicators: tuple[FinancialIndicatorRevision, ...]


class ValidatedFundamentalDatasetReader:
    """Stream immutable fundamental shards with exact row-hash checks."""

    def __init__(
        self,
        *,
        aggregate: FundamentalResearchDatasetManifest,
        manifest_reader: FundamentalManifestReader,
        data_reader: FundamentalAsOfReader,
        max_read_attempts: int = 3,
        retry_delay_seconds: float = 0.25,
    ) -> None:
        if (
            not 1 <= max_read_attempts <= 5
            or retry_delay_seconds < 0
            or retry_delay_seconds > 5
        ):
            raise ValueError(
                "fundamental shard retry policy is invalid"
            )
        self._aggregate = aggregate
        self._manifests = manifest_reader
        self._data = data_reader
        self._max_read_attempts = max_read_attempts
        self._retry_delay_seconds = retry_delay_seconds

    async def query_instrument(
        self,
        instrument: str,
    ) -> ValidatedFundamentalShard:
        shard = next(
            (
                value
                for value in self._aggregate.shards
                if value.instrument == instrument
            ),
            None,
        )
        if shard is None:
            raise LookupError(
                "instrument is not in the fundamental dataset"
            )
        manifest = await self._manifests.read_manifest(
            shard.manifest_hash
        )
        if (
            manifest.manifest_hash != shard.manifest_hash
            or manifest.source != "tushare-fundamental"
            or not manifest.production_complete
            or manifest.instruments != (instrument,)
            or to_shanghai(manifest.start_time).date()
            != self._aggregate.start_date
            or to_shanghai(manifest.end_time).date()
            != self._aggregate.end_date
        ):
            raise ManifestIntegrityError(
                "fundamental shard does not match its aggregate manifest"
            )
        for attempt in range(1, self._max_read_attempts + 1):
            try:
                valuations = (
                    await self._data.query_valuations_as_of(
                        (instrument,),
                        self._aggregate.start_date,
                        self._aggregate.end_date,
                        manifest.as_of,
                    )
                )
                indicators = (
                    await self._data.query_indicator_revisions_as_of(
                        (instrument,),
                        self._aggregate.start_date,
                        self._aggregate.end_date,
                        manifest.as_of,
                    )
                )
                break
            except PersistenceUnavailableError as error:
                if attempt == self._max_read_attempts:
                    raise PersistenceUnavailableError(
                        f"fundamental shard {instrument} remained "
                        f"unavailable after {attempt} attempts"
                    ) from error
                if self._retry_delay_seconds:
                    await asyncio.sleep(self._retry_delay_seconds)
        actual = tuple(
            value.content_hash for value in valuations
        ) + tuple(value.content_hash for value in indicators)
        if (
            len(actual) != len(manifest.record_hashes)
            or set(actual) != set(manifest.record_hashes)
        ):
            raise ManifestIntegrityError(
                f"fundamental shard {instrument} failed row-hash "
                "verification"
            )
        return ValidatedFundamentalShard(
            instrument=instrument,
            manifest=manifest,
            valuations=valuations,
            indicators=indicators,
        )

    async def iter_all(
        self,
    ) -> AsyncIterator[ValidatedFundamentalShard]:
        for shard in self._aggregate.shards:
            yield await self.query_instrument(shard.instrument)
