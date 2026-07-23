from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime

from autoquant.data.models import _canonical_hash, _require_lowercase_sha256
from autoquant.data.research_data_campaign import (
    ResearchDatasetManifest,
    ResearchDatasetShard,
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

    def members_for(self, session_date: date) -> tuple[str, ...]:
        if session_date < self.start_date or session_date > self.end_date:
            raise ValueError("research session date is outside plan bounds")
        active: ResearchUniverseBinding | None = None
        for universe in self.universes:
            if universe.reference_date >= session_date:
                break
            active = universe
        return () if active is None else active.members

    def shard_manifest_for(self, instrument: str) -> str:
        for shard in self.shards:
            if shard.instrument == instrument:
                return shard.manifest_hash
        raise LookupError("instrument is not bound to a research dataset shard")


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
