from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal

from autoquant.data.models import SourceEvidence

_INDEX_CODE = re.compile(r"[0-9]{6}\.(?:SH|SZ)\Z")
_INSTRUMENT = re.compile(r"[0-9]{6}\.(?:XSHG|XSHE)\Z")


@dataclass(frozen=True, slots=True)
class IndexUniverseBatch:
    constituents: tuple[IndexConstituent, ...]
    liquidity: tuple[DailyLiquidityMetric, ...]
    source_evidence: tuple[SourceEvidence, ...]

    def __post_init__(self) -> None:
        if (
            not self.constituents
            or not self.liquidity
            or len(self.source_evidence) != 2
            or len(
                {
                    value.response_hash
                    for value in self.source_evidence
                }
            )
            != 2
        ):
            raise ValueError("index universe batch is incomplete")


@dataclass(frozen=True, slots=True)
class IndexConstituent:
    source: str
    index_code: str
    instrument: str
    trade_date: date
    weight: Decimal
    available_at: datetime
    response_hash: str

    def __post_init__(self) -> None:
        _identity(self)
        if self.weight <= 0 or self.weight > Decimal("100"):
            raise ValueError("index constituent weight is invalid")


@dataclass(frozen=True, slots=True)
class DailyLiquidityMetric:
    source: str
    instrument: str
    trade_date: date
    turnover_rate_f: Decimal
    volume_ratio: Decimal | None
    circulating_market_value: Decimal
    available_at: datetime
    response_hash: str

    def __post_init__(self) -> None:
        if (
            not self.source.strip()
            or _INSTRUMENT.fullmatch(self.instrument) is None
            or self.turnover_rate_f < 0
            or self.circulating_market_value <= 0
            or (
                self.volume_ratio is not None
                and self.volume_ratio < 0
            )
        ):
            raise ValueError("daily liquidity metric is invalid")
        _aware(self.available_at)
        _sha256(self.response_hash)


@dataclass(frozen=True, slots=True)
class PointInTimeUniversePolicy:
    index_code: str
    minimum_members: int = 100
    maximum_members: int = 500
    minimum_turnover_rate_f: Decimal = Decimal("0")
    minimum_circulating_market_value: Decimal = Decimal("0")
    version: str = "index-liquidity-universe-v1"
    policy_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            _INDEX_CODE.fullmatch(self.index_code) is None
            or not 20 <= self.minimum_members <= 500
            or not self.minimum_members
            <= self.maximum_members
            <= 1000
            or self.minimum_turnover_rate_f < 0
            or self.minimum_circulating_market_value < 0
            or not self.version.strip()
        ):
            raise ValueError("point-in-time universe policy is invalid")
        object.__setattr__(
            self,
            "policy_hash",
            _hash(self.payload(include_hash=False)),
        )

    def payload(
        self,
        *,
        include_hash: bool = True,
    ) -> dict[str, object]:
        value: dict[str, object] = {
            "index_code": self.index_code,
            "maximum_members": self.maximum_members,
            "minimum_circulating_market_value": _decimal(
                self.minimum_circulating_market_value
            ),
            "minimum_members": self.minimum_members,
            "minimum_turnover_rate_f": _decimal(
                self.minimum_turnover_rate_f
            ),
            "version": self.version,
        }
        if include_hash:
            value["policy_hash"] = self.policy_hash
        return value


@dataclass(frozen=True, slots=True)
class UniverseMember:
    instrument: str
    index_weight: Decimal
    turnover_rate_f: Decimal
    volume_ratio: Decimal | None
    circulating_market_value: Decimal

    def __post_init__(self) -> None:
        if (
            _INSTRUMENT.fullmatch(self.instrument) is None
            or self.index_weight <= 0
            or self.turnover_rate_f < 0
            or self.circulating_market_value <= 0
            or (
                self.volume_ratio is not None
                and self.volume_ratio < 0
            )
        ):
            raise ValueError("universe member is invalid")

    def payload(self) -> dict[str, object]:
        return {
            "circulating_market_value": _decimal(
                self.circulating_market_value
            ),
            "index_weight": _decimal(self.index_weight),
            "instrument": self.instrument,
            "turnover_rate_f": _decimal(self.turnover_rate_f),
            "volume_ratio": (
                None
                if self.volume_ratio is None
                else _decimal(self.volume_ratio)
            ),
        }


@dataclass(frozen=True, slots=True)
class PointInTimeUniverseSnapshot:
    policy: PointInTimeUniversePolicy
    reference_date: date
    index_constituent_date: date
    liquidity_date: date
    knowledge_as_of: datetime
    index_response_hash: str
    liquidity_response_hash: str
    members: tuple[UniverseMember, ...]
    snapshot_hash: str = field(init=False)

    def __post_init__(self) -> None:
        knowledge = _aware(self.knowledge_as_of)
        if (
            self.index_constituent_date > self.reference_date
            or self.liquidity_date > self.reference_date
            or self.liquidity_date < self.index_constituent_date
        ):
            raise ValueError("universe snapshot dates are invalid")
        _sha256(self.index_response_hash)
        _sha256(self.liquidity_response_hash)
        members = tuple(
            sorted(self.members, key=lambda value: value.instrument)
        )
        if (
            not self.policy.minimum_members
            <= len(members)
            <= self.policy.maximum_members
            or len({value.instrument for value in members})
            != len(members)
        ):
            raise ValueError("universe snapshot membership is invalid")
        object.__setattr__(self, "knowledge_as_of", knowledge)
        object.__setattr__(self, "members", members)
        object.__setattr__(
            self,
            "snapshot_hash",
            _hash(self.payload(include_hash=False)),
        )

    def payload(
        self,
        *,
        include_hash: bool = True,
    ) -> dict[str, object]:
        value: dict[str, object] = {
            "index_constituent_date": (
                self.index_constituent_date.isoformat()
            ),
            "index_response_hash": self.index_response_hash,
            "knowledge_as_of": self.knowledge_as_of.isoformat(),
            "liquidity_date": self.liquidity_date.isoformat(),
            "liquidity_response_hash": self.liquidity_response_hash,
            "members": [member.payload() for member in self.members],
            "policy": self.policy.payload(),
            "reference_date": self.reference_date.isoformat(),
        }
        if include_hash:
            value["snapshot_hash"] = self.snapshot_hash
        return value


def build_point_in_time_universe(
    *,
    policy: PointInTimeUniversePolicy,
    reference_date: date,
    constituents: tuple[IndexConstituent, ...],
    liquidity: tuple[DailyLiquidityMetric, ...],
) -> PointInTimeUniverseSnapshot:
    if not constituents or not liquidity:
        raise ValueError("universe inputs cannot be empty")
    eligible_constituents = tuple(
        value
        for value in constituents
        if value.index_code == policy.index_code
        and value.trade_date <= reference_date
    )
    if not eligible_constituents:
        raise ValueError("no index constituents exist at cutoff")
    constituent_date = max(
        value.trade_date for value in eligible_constituents
    )
    selected_constituents = tuple(
        value
        for value in eligible_constituents
        if value.trade_date == constituent_date
    )
    liquidity_dates = tuple(
        value.trade_date
        for value in liquidity
        if constituent_date <= value.trade_date <= reference_date
    )
    if not liquidity_dates:
        raise ValueError("no liquidity cross-section exists at cutoff")
    liquidity_date = max(liquidity_dates)
    selected_liquidity = {
        value.instrument: value
        for value in liquidity
        if value.trade_date == liquidity_date
    }
    if len(selected_liquidity) != sum(
        value.trade_date == liquidity_date for value in liquidity
    ):
        raise ValueError("liquidity cross-section contains duplicates")
    members = tuple(
        UniverseMember(
            instrument=value.instrument,
            index_weight=value.weight,
            turnover_rate_f=metric.turnover_rate_f,
            volume_ratio=metric.volume_ratio,
            circulating_market_value=(
                metric.circulating_market_value
            ),
        )
        for value in selected_constituents
        if (metric := selected_liquidity.get(value.instrument))
        is not None
        and metric.turnover_rate_f
        >= policy.minimum_turnover_rate_f
        and metric.circulating_market_value
        >= policy.minimum_circulating_market_value
    )
    return PointInTimeUniverseSnapshot(
        policy=policy,
        reference_date=reference_date,
        index_constituent_date=constituent_date,
        liquidity_date=liquidity_date,
        knowledge_as_of=max(
            (
                *(value.available_at for value in selected_constituents),
                *(
                    value.available_at
                    for value in selected_liquidity.values()
                ),
            )
        ),
        index_response_hash=_one_hash(
            tuple(
                value.response_hash
                for value in selected_constituents
            )
        ),
        liquidity_response_hash=_one_hash(
            tuple(
                value.response_hash
                for value in selected_liquidity.values()
            )
        ),
        members=members,
    )


def _identity(value: IndexConstituent) -> None:
    if (
        not value.source.strip()
        or _INDEX_CODE.fullmatch(value.index_code) is None
        or _INSTRUMENT.fullmatch(value.instrument) is None
    ):
        raise ValueError("index constituent identity is invalid")
    _aware(value.available_at)
    _sha256(value.response_hash)


def _one_hash(values: tuple[str, ...]) -> str:
    unique = set(values)
    if len(unique) != 1:
        raise ValueError("universe cross-section has mixed evidence")
    return next(iter(unique))


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("universe timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _sha256(value: str) -> None:
    if len(value) != 64 or any(
        character not in "0123456789abcdef"
        for character in value
    ):
        raise ValueError("universe evidence hash must be SHA-256")


def _decimal(value: Decimal) -> str:
    normalized = value.normalize()
    return "0" if normalized == 0 else format(normalized, "f")


def _hash(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()
