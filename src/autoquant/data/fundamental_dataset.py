from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from autoquant.data.models import (
    _canonical_hash,
    _require_lowercase_sha256,
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
