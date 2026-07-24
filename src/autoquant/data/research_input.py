from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Protocol

from autoquant.clock import to_shanghai
from autoquant.data.daily_ingestion import (
    ExactDailyRecordBatch,
    ValidatedDailyDataset,
    validate_daily_manifest_dataset,
)
from autoquant.data.daily_models import DailyCoverageEvidence
from autoquant.data.models import (
    DatasetManifest,
    _canonical_hash,
    _require_lowercase_sha256,
)
from autoquant.data.quality import QualityReport
from autoquant.data.research_data_campaign import (
    ResearchDatasetManifest,
    ResearchDatasetShard,
)
from autoquant.errors import (
    ManifestIntegrityError,
    PersistenceUnavailableError,
)

RESEARCH_INPUT_PLAN_VERSION = "point-in-time-monthly-strict-after-v1"
RESEARCH_UNIVERSE_ACTIVATION_RULE = "session_date>snapshot.reference_date"


@dataclass(frozen=True, slots=True)
class ResearchUniverseBinding:
    sequence: int
    snapshot_hash: str
    policy_hash: str
    reference_date: date
    knowledge_as_of: datetime
    members: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.sequence < 1:
            raise ValueError("research universe sequence must be positive")
        _require_lowercase_sha256(
            self.snapshot_hash,
            name="research universe snapshot hash",
        )
        _require_lowercase_sha256(
            self.policy_hash,
            name="research universe policy hash",
        )
        if self.knowledge_as_of.tzinfo is None or self.knowledge_as_of.utcoffset() is None:
            raise ValueError("research universe knowledge_as_of must be aware")
        members = tuple(self.members)
        if not members or members != tuple(sorted(members)) or len(set(members)) != len(members):
            raise ValueError("research universe members must be nonempty, sorted and unique")
        object.__setattr__(
            self,
            "knowledge_as_of",
            self.knowledge_as_of.astimezone(UTC),
        )
        object.__setattr__(self, "members", members)

    def payload(self) -> dict[str, object]:
        return {
            "knowledge_as_of": self.knowledge_as_of.isoformat(),
            "members": list(self.members),
            "policy_hash": self.policy_hash,
            "reference_date": self.reference_date.isoformat(),
            "sequence": self.sequence,
            "snapshot_hash": self.snapshot_hash,
        }


@dataclass(frozen=True, slots=True)
class ResearchInputPlan:
    dataset_manifest_hash: str
    campaign_hash: str
    policy_hash: str
    start_date: date
    end_date: date
    shards: tuple[ResearchDatasetShard, ...]
    universes: tuple[ResearchUniverseBinding, ...]
    version: str = RESEARCH_INPUT_PLAN_VERSION
    activation_rule: str = RESEARCH_UNIVERSE_ACTIVATION_RULE
    plan_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.dataset_manifest_hash,
            name="research dataset manifest hash",
        )
        _require_lowercase_sha256(
            self.campaign_hash,
            name="research data campaign hash",
        )
        _require_lowercase_sha256(
            self.policy_hash,
            name="research universe policy hash",
        )
        shards = tuple(self.shards)
        universes = tuple(self.universes)
        if self.start_date > self.end_date:
            raise ValueError("research input start cannot follow end")
        if not shards:
            raise ValueError("research input requires dataset shards")
        if not universes:
            raise ValueError("research input requires universe snapshots")
        if tuple(value.sequence for value in universes) != tuple(range(1, len(universes) + 1)):
            raise ValueError("research universe sequence must be contiguous")
        if len({value.snapshot_hash for value in universes}) != len(universes):
            raise ValueError("research universe snapshots must be unique")
        if any(value.policy_hash != self.policy_hash for value in universes):
            raise ValueError("research universes must use the manifest policy")
        references = tuple(value.reference_date for value in universes)
        if references != tuple(sorted(references)) or len(set(references)) != len(references):
            raise ValueError("research universe reference dates must increase")
        if any(value < self.start_date or value > self.end_date for value in references):
            raise ValueError("research universe reference date is out of bounds")
        actual_months = tuple((value.year, value.month) for value in references)
        if actual_months != _calendar_months(self.start_date, self.end_date):
            raise ValueError("research input requires exactly one snapshot per calendar month")
        shard_instruments = {value.instrument for value in shards}
        universe_instruments = {
            instrument for universe in universes for instrument in universe.members
        }
        if universe_instruments != shard_instruments:
            raise ValueError("research universe union must exactly match dataset shards")
        if self.version != RESEARCH_INPUT_PLAN_VERSION:
            raise ValueError("research input plan version is unsupported")
        if self.activation_rule != RESEARCH_UNIVERSE_ACTIVATION_RULE:
            raise ValueError("research universe activation rule is unsupported")
        object.__setattr__(self, "shards", shards)
        object.__setattr__(self, "universes", universes)
        object.__setattr__(self, "plan_hash", _canonical_hash(self.payload()))

    def payload(self) -> dict[str, object]:
        return {
            "activation_rule": self.activation_rule,
            "campaign_hash": self.campaign_hash,
            "dataset_manifest_hash": self.dataset_manifest_hash,
            "end_date": self.end_date.isoformat(),
            "policy_hash": self.policy_hash,
            "shards": [value.payload() for value in self.shards],
            "start_date": self.start_date.isoformat(),
            "universes": [value.payload() for value in self.universes],
            "version": self.version,
        }

    @property
    def instruments(self) -> tuple[str, ...]:
        return tuple(value.instrument for value in self.shards)

    def members_for(self, session_date: date) -> tuple[str, ...]:
        active = self.universe_for(session_date)
        return () if active is None else active.members

    def universe_for(
        self,
        session_date: date,
    ) -> ResearchUniverseBinding | None:
        if session_date < self.start_date or session_date > self.end_date:
            raise ValueError("research session date is outside plan bounds")
        active: ResearchUniverseBinding | None = None
        for universe in self.universes:
            if universe.reference_date >= session_date:
                break
            active = universe
        return active

    def shard_manifest_for(self, instrument: str) -> str:
        for shard in self.shards:
            if shard.instrument == instrument:
                return shard.manifest_hash
        raise LookupError("instrument is not bound to a research dataset shard")


class DailyManifestReader(Protocol):
    async def read_manifest(
        self,
        manifest_hash: str,
    ) -> DatasetManifest: ...


class ValidatedDailyReader(Protocol):
    async def query(
        self,
        manifest_hash: str,
        as_of: datetime,
    ) -> ValidatedDailyDataset: ...


class ExactDailyRecordReader(Protocol):
    async def query_exact_records(
        self,
        *,
        instruments: tuple[str, ...],
        start: date,
        end: date,
        record_hash_groups: tuple[tuple[str, ...], ...],
    ) -> ExactDailyRecordBatch: ...


class ExactResearchShardPlan(Protocol):
    @property
    def start_date(self) -> date: ...

    @property
    def end_date(self) -> date: ...

    @property
    def shards(self) -> tuple[ResearchDatasetShard, ...]: ...


class DailyManifestEvidenceReader(DailyManifestReader, Protocol):
    async def read_quality_report(
        self,
        report_hash: str,
    ) -> QualityReport: ...


@dataclass(frozen=True, slots=True)
class ValidatedResearchShard:
    instrument: str
    manifest: DatasetManifest
    dataset: ValidatedDailyDataset


class ValidatedResearchDatasetReader:
    """Read immutable daily shards lazily without loading the 493-name union."""

    def __init__(
        self,
        *,
        plan: ResearchInputPlan,
        manifest_reader: DailyManifestReader,
        dataset_reader: ValidatedDailyReader,
        max_read_attempts: int = 3,
        retry_delay_seconds: float = 0.25,
    ) -> None:
        if not 1 <= max_read_attempts <= 5 or retry_delay_seconds < 0 or retry_delay_seconds > 5:
            raise ValueError("research shard retry policy is invalid")
        self._plan = plan
        self._manifests = manifest_reader
        self._datasets = dataset_reader
        self._max_read_attempts = max_read_attempts
        self._retry_delay_seconds = retry_delay_seconds

    async def query_instrument(
        self,
        instrument: str,
    ) -> ValidatedResearchShard:
        manifest_hash = self._plan.shard_manifest_for(instrument)
        manifest = await self._manifests.read_manifest(manifest_hash)
        if (
            manifest.manifest_hash != manifest_hash
            or manifest.source != "tushare"
            or not manifest.production_complete
            or manifest.instruments != (instrument,)
            or to_shanghai(manifest.start_time).date() != self._plan.start_date
            or to_shanghai(manifest.end_time).date() != self._plan.end_date
        ):
            raise ValueError("daily shard does not match the research input plan")
        for attempt in range(1, self._max_read_attempts + 1):
            try:
                dataset = await self._datasets.query(
                    manifest.manifest_hash,
                    manifest.as_of,
                )
                break
            except ManifestIntegrityError as error:
                raise ManifestIntegrityError(
                    f"research shard {instrument} failed row-hash verification"
                ) from error
            except PersistenceUnavailableError as error:
                if attempt == self._max_read_attempts:
                    raise PersistenceUnavailableError(
                        f"research shard {instrument} remained unavailable after {attempt} attempts"
                    ) from error
                if self._retry_delay_seconds:
                    await asyncio.sleep(self._retry_delay_seconds)
        return ValidatedResearchShard(
            instrument=instrument,
            manifest=manifest,
            dataset=dataset,
        )

    async def iter_all(self) -> AsyncIterator[ValidatedResearchShard]:
        for shard in self._plan.shards:
            yield await self.query_instrument(shard.instrument)

    async def iter_members(
        self,
        session_date: date,
    ) -> AsyncIterator[ValidatedResearchShard]:
        for instrument in self._plan.members_for(session_date):
            yield await self.query_instrument(instrument)


class ExactManifestResearchDatasetReader:
    """Read frozen daily rows in bounded hash-addressed batches."""

    def __init__(
        self,
        *,
        plan: ExactResearchShardPlan,
        control_reader: DailyManifestEvidenceReader,
        record_reader: ExactDailyRecordReader,
        batch_size: int = 4,
        max_read_attempts: int = 3,
        retry_delay_seconds: float = 0.25,
    ) -> None:
        if (
            not 1 <= batch_size <= 25
            or not 1 <= max_read_attempts <= 5
            or retry_delay_seconds < 0
            or retry_delay_seconds > 5
        ):
            raise ValueError("exact research reader resource policy is invalid")
        self._plan = plan
        self._control = control_reader
        self._records = record_reader
        self._batch_size = batch_size
        self._max_read_attempts = max_read_attempts
        self._retry_delay_seconds = retry_delay_seconds

    async def iter_all(
        self,
    ) -> AsyncIterator[ValidatedResearchShard]:
        shards = self._plan.shards
        for offset in range(0, len(shards), self._batch_size):
            batch = shards[offset : offset + self._batch_size]
            manifests = tuple(
                [
                    await self._read_manifest(
                        instrument=value.instrument,
                        manifest_hash=value.manifest_hash,
                    )
                    for value in batch
                ]
            )
            dataset = await self._read_batch(manifests)
            for manifest in manifests:
                yield ValidatedResearchShard(
                    instrument=manifest.instruments[0],
                    manifest=manifest,
                    dataset=self._validated_shard(
                        manifest=manifest,
                        batch=dataset,
                    ),
                )

    async def _read_manifest(
        self,
        *,
        instrument: str,
        manifest_hash: str,
    ) -> DatasetManifest:
        manifest = await self._control.read_manifest(manifest_hash)
        if (
            manifest.manifest_hash != manifest_hash
            or manifest.source != "tushare"
            or not manifest.production_complete
            or manifest.instruments != (instrument,)
            or to_shanghai(manifest.start_time).date() != self._plan.start_date
            or to_shanghai(manifest.end_time).date() != self._plan.end_date
        ):
            raise ValueError("daily shard does not match the research input plan")
        report = await self._control.read_quality_report(manifest.quality_report_hash)
        if not report.passed or not report.production_complete:
            raise ValueError("exact research requires passing production evidence")
        return manifest

    async def _read_batch(
        self,
        manifests: tuple[DatasetManifest, ...],
    ) -> ExactDailyRecordBatch:
        instruments = tuple(value.instruments[0] for value in manifests)
        for attempt in range(1, self._max_read_attempts + 1):
            try:
                return await self._records.query_exact_records(
                    instruments=instruments,
                    start=self._plan.start_date,
                    end=self._plan.end_date,
                    record_hash_groups=tuple(value.record_hashes for value in manifests),
                )
            except PersistenceUnavailableError as error:
                if attempt == self._max_read_attempts:
                    raise PersistenceUnavailableError(
                        f"exact daily batch remained unavailable after {attempt} attempts"
                    ) from error
                if self._retry_delay_seconds:
                    await asyncio.sleep(self._retry_delay_seconds)
        raise AssertionError("exact daily retry loop did not return")

    @staticmethod
    def _validated_shard(
        *,
        manifest: DatasetManifest,
        batch: ExactDailyRecordBatch,
    ) -> ValidatedDailyDataset:
        instrument = manifest.instruments[0]
        expected = set(manifest.record_hashes)
        bars = tuple(
            value
            for value in batch.bars
            if value.instrument == instrument and value.content_hash in expected
        )
        bar_keys = {(value.instrument, value.session_date) for value in bars}
        factors = tuple(
            value
            for value in batch.factors
            if value.instrument == instrument
            and value.content_hash in expected
            and (value.instrument, value.session_date) in bar_keys
        )
        coverage = DailyCoverageEvidence(
            sessions=tuple(value for value in batch.sessions if value.content_hash in expected),
            lifecycles=tuple(
                value
                for value in batch.lifecycles
                if value.instrument == instrument and value.content_hash in expected
            ),
            suspensions=tuple(
                value
                for value in batch.suspensions
                if value.instrument == instrument and value.content_hash in expected
            ),
            price_limits=tuple(
                value
                for value in batch.price_limits
                if value.instrument == instrument and value.content_hash in expected
            ),
        )
        dataset = ValidatedDailyDataset(
            bars=bars,
            factors=factors,
            coverage=coverage,
        )
        if any(
            value.available_at > manifest.as_of
            for values in (
                dataset.bars,
                dataset.factors,
                dataset.coverage.sessions,
                dataset.coverage.lifecycles,
                dataset.coverage.suspensions,
                dataset.coverage.price_limits,
            )
            for value in values
        ):
            raise ManifestIntegrityError("exact daily row exceeds the manifest cutoff")
        validate_daily_manifest_dataset(
            manifest=manifest,
            dataset=dataset,
        )
        return dataset


def compile_research_input_plan(
    *,
    manifest: ResearchDatasetManifest,
    universes: tuple[ResearchUniverseBinding, ...],
) -> ResearchInputPlan:
    if tuple(value.snapshot_hash for value in universes) != manifest.snapshot_hashes:
        raise ValueError("research universe bindings do not match the dataset manifest")
    return ResearchInputPlan(
        dataset_manifest_hash=manifest.manifest_hash,
        campaign_hash=manifest.campaign_hash,
        policy_hash=manifest.policy_hash,
        start_date=manifest.start_date,
        end_date=manifest.end_date,
        shards=manifest.shards,
        universes=universes,
    )


def _calendar_months(start: date, end: date) -> tuple[tuple[int, int], ...]:
    values: list[tuple[int, int]] = []
    current = start.replace(day=1)
    final = end.replace(day=1)
    while current <= final:
        values.append((current.year, current.month))
        current = (
            date(current.year + 1, 1, 1)
            if current.month == 12
            else date(current.year, current.month + 1, 1)
        )
    return tuple(values)
