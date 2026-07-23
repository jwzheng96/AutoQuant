from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date

from autoquant.data.models import (
    _canonical_hash,
    _require_lowercase_sha256,
    _require_nonblank,
)

_CAMPAIGN_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{15,127}\Z")
_INSTRUMENT = re.compile(r"[0-9]{6}\.(?:XSHG|XSHE)\Z")
RESEARCH_DATA_CAMPAIGN_VERSION = "research-data-campaign-v1"
RESEARCH_DATA_MANIFEST_VERSION = "research-dataset-manifest-v1"


@dataclass(frozen=True, slots=True)
class ResearchDataCampaignSpec:
    campaign_key: str
    policy_hash: str
    snapshot_hashes: tuple[str, ...]
    instruments: tuple[str, ...]
    start_date: date
    end_date: date
    requested_by: str
    max_attempts: int = 3
    version: str = RESEARCH_DATA_CAMPAIGN_VERSION
    campaign_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if _CAMPAIGN_KEY.fullmatch(self.campaign_key) is None:
            raise ValueError("campaign_key must contain 16-128 safe characters")
        _require_lowercase_sha256(self.policy_hash, name="research universe policy hash")
        snapshots = tuple(self.snapshot_hashes)
        instruments = tuple(sorted(self.instruments))
        if not snapshots or len(set(snapshots)) != len(snapshots):
            raise ValueError("snapshot_hashes must be nonempty and unique")
        for value in snapshots:
            _require_lowercase_sha256(value, name="research universe snapshot hash")
        if (
            not instruments
            or len(instruments) > 1000
            or len(set(instruments)) != len(instruments)
            or any(_INSTRUMENT.fullmatch(value) is None for value in instruments)
        ):
            raise ValueError("instruments must contain 1-1000 unique canonical symbols")
        if self.start_date > self.end_date:
            raise ValueError("research data campaign start cannot follow end")
        if self.max_attempts < 1 or self.max_attempts > 10:
            raise ValueError("max_attempts must be between 1 and 10")
        _require_nonblank(self.requested_by, name="research data requested_by")
        if len(self.requested_by) > 128:
            raise ValueError("research data requested_by cannot exceed 128 characters")
        if self.version != RESEARCH_DATA_CAMPAIGN_VERSION:
            raise ValueError("research data campaign version is unsupported")
        object.__setattr__(self, "snapshot_hashes", snapshots)
        object.__setattr__(self, "instruments", instruments)
        object.__setattr__(self, "campaign_hash", _canonical_hash(self.payload()))

    def payload(self) -> dict[str, object]:
        return {
            "campaign_key": self.campaign_key,
            "end_date": self.end_date.isoformat(),
            "instruments": list(self.instruments),
            "max_attempts": self.max_attempts,
            "policy_hash": self.policy_hash,
            "requested_by": self.requested_by,
            "snapshot_hashes": list(self.snapshot_hashes),
            "start_date": self.start_date.isoformat(),
            "version": self.version,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, object]) -> ResearchDataCampaignSpec:
        raw_snapshots = payload.get("snapshot_hashes")
        raw_instruments = payload.get("instruments")
        if not isinstance(raw_snapshots, list) or not isinstance(raw_instruments, list):
            raise TypeError("research data campaign arrays are invalid")
        value = cls(
            campaign_key=str(payload["campaign_key"]),
            policy_hash=str(payload["policy_hash"]),
            snapshot_hashes=tuple(str(item) for item in raw_snapshots),
            instruments=tuple(str(item) for item in raw_instruments),
            start_date=date.fromisoformat(str(payload["start_date"])),
            end_date=date.fromisoformat(str(payload["end_date"])),
            requested_by=str(payload["requested_by"]),
            max_attempts=int(str(payload["max_attempts"])),
            version=str(payload["version"]),
        )
        if value.payload() != payload:
            raise ValueError("research data campaign payload is not canonical")
        return value


@dataclass(frozen=True, slots=True)
class ResearchDatasetShard:
    sequence: int
    instrument: str
    manifest_hash: str

    def __post_init__(self) -> None:
        if self.sequence < 1:
            raise ValueError("research dataset shard sequence must be positive")
        if _INSTRUMENT.fullmatch(self.instrument) is None:
            raise ValueError("research dataset shard instrument is invalid")
        _require_lowercase_sha256(
            self.manifest_hash,
            name="research dataset shard manifest hash",
        )

    def payload(self) -> dict[str, object]:
        return {
            "instrument": self.instrument,
            "manifest_hash": self.manifest_hash,
            "sequence": self.sequence,
        }


@dataclass(frozen=True, slots=True)
class ResearchDatasetManifest:
    campaign_hash: str
    policy_hash: str
    snapshot_hashes: tuple[str, ...]
    start_date: date
    end_date: date
    shards: tuple[ResearchDatasetShard, ...]
    source: str = "tushare"
    version: str = RESEARCH_DATA_MANIFEST_VERSION
    manifest_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(self.campaign_hash, name="research data campaign hash")
        _require_lowercase_sha256(self.policy_hash, name="research universe policy hash")
        snapshots = tuple(self.snapshot_hashes)
        shards = tuple(self.shards)
        if not snapshots or len(set(snapshots)) != len(snapshots):
            raise ValueError("research dataset snapshots must be nonempty and unique")
        for value in snapshots:
            _require_lowercase_sha256(value, name="research universe snapshot hash")
        if not shards or len(shards) > 1000:
            raise ValueError("research dataset must contain 1-1000 shards")
        if tuple(value.sequence for value in shards) != tuple(
            range(1, len(shards) + 1)
        ):
            raise ValueError("research dataset shard sequence must be contiguous")
        if len({value.instrument for value in shards}) != len(shards):
            raise ValueError("research dataset shard instruments must be unique")
        if len({value.manifest_hash for value in shards}) != len(shards):
            raise ValueError("research dataset shard manifests must be unique")
        if tuple(value.instrument for value in shards) != tuple(
            sorted(value.instrument for value in shards)
        ):
            raise ValueError("research dataset shards must be instrument-sorted")
        if self.start_date > self.end_date:
            raise ValueError("research dataset start cannot follow end")
        if self.source != "tushare":
            raise ValueError("research dataset source is unsupported")
        if self.version != RESEARCH_DATA_MANIFEST_VERSION:
            raise ValueError("research dataset manifest version is unsupported")
        object.__setattr__(self, "snapshot_hashes", snapshots)
        object.__setattr__(self, "shards", shards)
        object.__setattr__(self, "manifest_hash", _canonical_hash(self.payload()))

    @property
    def instruments(self) -> tuple[str, ...]:
        return tuple(value.instrument for value in self.shards)

    def payload(self) -> dict[str, object]:
        return {
            "campaign_hash": self.campaign_hash,
            "end_date": self.end_date.isoformat(),
            "policy_hash": self.policy_hash,
            "shards": [value.payload() for value in self.shards],
            "snapshot_hashes": list(self.snapshot_hashes),
            "source": self.source,
            "start_date": self.start_date.isoformat(),
            "version": self.version,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, object]) -> ResearchDatasetManifest:
        raw_snapshots = payload.get("snapshot_hashes")
        raw_shards = payload.get("shards")
        if not isinstance(raw_snapshots, list) or not isinstance(raw_shards, list):
            raise TypeError("research dataset manifest arrays are invalid")
        shards: list[ResearchDatasetShard] = []
        for raw in raw_shards:
            if not isinstance(raw, dict):
                raise TypeError("research dataset shard payload is invalid")
            shards.append(
                ResearchDatasetShard(
                    sequence=int(str(raw["sequence"])),
                    instrument=str(raw["instrument"]),
                    manifest_hash=str(raw["manifest_hash"]),
                )
            )
        value = cls(
            campaign_hash=str(payload["campaign_hash"]),
            policy_hash=str(payload["policy_hash"]),
            snapshot_hashes=tuple(str(item) for item in raw_snapshots),
            start_date=date.fromisoformat(str(payload["start_date"])),
            end_date=date.fromisoformat(str(payload["end_date"])),
            shards=tuple(shards),
            source=str(payload["source"]),
            version=str(payload["version"]),
        )
        if value.payload() != payload:
            raise ValueError("research dataset manifest payload is not canonical")
        return value
