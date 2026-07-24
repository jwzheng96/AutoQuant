from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal

from autoquant.backtest.low_volatility_strategy import (
    LowVolatilityObservation,
)
from autoquant.clock import SHANGHAI, to_utc
from autoquant.data.models import (
    _canonical_hash,
    _datetime_text,
    _decimal_text,
    _require_lowercase_sha256,
    _require_nonblank,
)

LOW_VOLATILITY_PAPER_DAILY_SIGNAL_VERSION = "low-volatility-paper-daily-signal-v1"
LOW_VOLATILITY_PAPER_DECISION_POLICY = "prior-close-preopen-observation-only-v1"
LOW_VOLATILITY_PAPER_SIGNAL_GENESIS_HASH = "0" * 64
_INSTRUMENT = re.compile(r"[0-9]{6}\.(?:XSHG|XSHE)\Z")


@dataclass(frozen=True, slots=True)
class LowVolatilityPaperValuation:
    instrument: str
    price_date: date
    adjusted_close: Decimal
    prior_volume: int
    adjusted_bar_hash: str
    valuation_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            _INSTRUMENT.fullmatch(self.instrument) is None
            or not isinstance(self.adjusted_close, Decimal)
            or not self.adjusted_close.is_finite()
            or self.adjusted_close <= 0
            or not isinstance(self.prior_volume, int)
            or isinstance(self.prior_volume, bool)
            or self.prior_volume < 0
        ):
            raise ValueError("low-volatility paper valuation is invalid")
        _require_lowercase_sha256(
            self.adjusted_bar_hash,
            name="low-volatility paper adjusted bar hash",
        )
        object.__setattr__(
            self,
            "valuation_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "adjusted_bar_hash": self.adjusted_bar_hash,
            "adjusted_close": _decimal_text(self.adjusted_close),
            "instrument": self.instrument,
            "price_date": self.price_date.isoformat(),
            "prior_volume": self.prior_volume,
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> LowVolatilityPaperValuation:
        value = cls(
            instrument=str(payload["instrument"]),
            price_date=date.fromisoformat(str(payload["price_date"])),
            adjusted_close=Decimal(str(payload["adjusted_close"])),
            prior_volume=int(str(payload["prior_volume"])),
            adjusted_bar_hash=str(payload["adjusted_bar_hash"]),
        )
        if value.payload() != payload:
            raise ValueError("low-volatility paper valuation payload is not canonical")
        return value


@dataclass(frozen=True, slots=True)
class LowVolatilityPaperDailySignal:
    candidate_approval_hash: str
    account_id: str
    strategy_id: str
    source_spec_hash: str
    risk_policy_hash: str
    session_sequence: int
    session_date: date
    signal_date: date
    window_start_date: date
    previous_signal_hash: str
    snapshot_hash: str
    dataset_manifest_hash: str
    rule_set_hash: str
    universe_members: tuple[str, ...]
    evidence_instruments: tuple[str, ...]
    observations: tuple[LowVolatilityObservation, ...]
    valuations: tuple[LowVolatilityPaperValuation, ...]
    selected_instruments: tuple[str, ...]
    rebalance_due: bool
    prepared_by: str
    prepared_at: datetime
    execution_timing_compatible: bool = False
    runtime_activation_allowed: bool = False
    live_trading_locked: bool = True
    decision_policy: str = LOW_VOLATILITY_PAPER_DECISION_POLICY
    version: str = LOW_VOLATILITY_PAPER_DAILY_SIGNAL_VERSION
    signal_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for name, value in (
            ("account_id", self.account_id),
            ("strategy_id", self.strategy_id),
            ("prepared_by", self.prepared_by),
        ):
            _require_nonblank(value, name=name)
            if value != value.strip() or len(value) > 128:
                raise ValueError(f"{name} must contain 1-128 trimmed characters")
        for name, value in (
            (
                "candidate_approval_hash",
                self.candidate_approval_hash,
            ),
            ("source_spec_hash", self.source_spec_hash),
            ("risk_policy_hash", self.risk_policy_hash),
            ("previous_signal_hash", self.previous_signal_hash),
            ("snapshot_hash", self.snapshot_hash),
            ("dataset_manifest_hash", self.dataset_manifest_hash),
            ("rule_set_hash", self.rule_set_hash),
        ):
            _require_lowercase_sha256(value, name=name)
        prepared_at = to_utc(
            self.prepared_at,
            name="low-volatility paper signal prepared_at",
        )
        session_open = datetime.combine(
            self.session_date,
            time(9, 30),
            tzinfo=SHANGHAI,
        ).astimezone(prepared_at.tzinfo)
        members = tuple(self.universe_members)
        evidence = tuple(self.evidence_instruments)
        observations = tuple(
            sorted(
                self.observations,
                key=lambda value: value.instrument,
            )
        )
        valuations = tuple(
            sorted(
                self.valuations,
                key=lambda value: value.instrument,
            )
        )
        selected = tuple(self.selected_instruments)
        observation_instruments = tuple(value.instrument for value in observations)
        valuation_instruments = tuple(value.instrument for value in valuations)
        expected_rebalance = (
            (self.session_sequence - 1) % 21 == 0 if self.session_sequence >= 1 else False
        )
        if (
            self.session_sequence < 1
            or self.window_start_date > self.signal_date
            or self.signal_date >= self.session_date
            or prepared_at >= session_open
            or not members
            or members != tuple(sorted(members))
            or len(set(members)) != len(members)
            or not evidence
            or evidence != tuple(sorted(evidence))
            or len(set(evidence)) != len(evidence)
            or not set(members) <= set(evidence)
            or observation_instruments != tuple(sorted(observation_instruments))
            or len(set(observation_instruments)) != len(observation_instruments)
            or not set(observation_instruments) <= set(members)
            or any(
                value.signal_date != self.signal_date or value.execution_date != self.session_date
                for value in observations
            )
            or valuation_instruments != evidence
            or any(value.price_date > self.signal_date for value in valuations)
            or selected != tuple(sorted(selected))
            or len(set(selected)) != len(selected)
            or not set(selected) <= set(evidence)
            or len(selected) not in (0, 20)
            or (self.rebalance_due and len(observations) >= 60 and len(selected) != 20)
            or (self.rebalance_due and len(observations) < 60 and selected)
            or self.rebalance_due != expected_rebalance
            or (
                self.session_sequence == 1
                and self.previous_signal_hash != LOW_VOLATILITY_PAPER_SIGNAL_GENESIS_HASH
            )
            or self.execution_timing_compatible
            or self.runtime_activation_allowed
            or not self.live_trading_locked
            or self.decision_policy != LOW_VOLATILITY_PAPER_DECISION_POLICY
            or self.version != LOW_VOLATILITY_PAPER_DAILY_SIGNAL_VERSION
        ):
            raise ValueError("low-volatility paper daily signal is invalid")
        for value in (*members, *evidence, *selected):
            if _INSTRUMENT.fullmatch(value) is None:
                raise ValueError("low-volatility paper signal instrument is invalid")
        object.__setattr__(self, "prepared_at", prepared_at)
        object.__setattr__(self, "universe_members", members)
        object.__setattr__(
            self,
            "evidence_instruments",
            evidence,
        )
        object.__setattr__(self, "observations", observations)
        object.__setattr__(self, "valuations", valuations)
        object.__setattr__(
            self,
            "selected_instruments",
            selected,
        )
        object.__setattr__(
            self,
            "signal_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "account_id": self.account_id,
            "candidate_approval_hash": (self.candidate_approval_hash),
            "dataset_manifest_hash": self.dataset_manifest_hash,
            "decision_policy": self.decision_policy,
            "evidence_instruments": list(self.evidence_instruments),
            "execution_timing_compatible": (self.execution_timing_compatible),
            "live_trading_locked": self.live_trading_locked,
            "observations": [value.payload() for value in self.observations],
            "prepared_at": _datetime_text(self.prepared_at),
            "prepared_by": self.prepared_by,
            "previous_signal_hash": self.previous_signal_hash,
            "rebalance_due": self.rebalance_due,
            "risk_policy_hash": self.risk_policy_hash,
            "rule_set_hash": self.rule_set_hash,
            "runtime_activation_allowed": (self.runtime_activation_allowed),
            "selected_instruments": list(self.selected_instruments),
            "session_date": self.session_date.isoformat(),
            "session_sequence": self.session_sequence,
            "signal_date": self.signal_date.isoformat(),
            "snapshot_hash": self.snapshot_hash,
            "source_spec_hash": self.source_spec_hash,
            "strategy_id": self.strategy_id,
            "universe_members": list(self.universe_members),
            "valuations": [value.payload() for value in self.valuations],
            "version": self.version,
            "window_start_date": (self.window_start_date.isoformat()),
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> LowVolatilityPaperDailySignal:
        members = _string_list(
            payload["universe_members"],
            name="universe_members",
        )
        evidence = _string_list(
            payload["evidence_instruments"],
            name="evidence_instruments",
        )
        selected = _string_list(
            payload["selected_instruments"],
            name="selected_instruments",
        )
        raw_observations = _object_list(
            payload["observations"],
            name="observations",
        )
        raw_valuations = _object_list(
            payload["valuations"],
            name="valuations",
        )
        value = cls(
            candidate_approval_hash=str(payload["candidate_approval_hash"]),
            account_id=str(payload["account_id"]),
            strategy_id=str(payload["strategy_id"]),
            source_spec_hash=str(payload["source_spec_hash"]),
            risk_policy_hash=str(payload["risk_policy_hash"]),
            session_sequence=int(str(payload["session_sequence"])),
            session_date=date.fromisoformat(str(payload["session_date"])),
            signal_date=date.fromisoformat(str(payload["signal_date"])),
            window_start_date=date.fromisoformat(str(payload["window_start_date"])),
            previous_signal_hash=str(payload["previous_signal_hash"]),
            snapshot_hash=str(payload["snapshot_hash"]),
            dataset_manifest_hash=str(payload["dataset_manifest_hash"]),
            rule_set_hash=str(payload["rule_set_hash"]),
            universe_members=members,
            evidence_instruments=evidence,
            observations=tuple(
                LowVolatilityObservation(
                    instrument=str(item["instrument"]),
                    signal_date=date.fromisoformat(str(item["signal_date"])),
                    execution_date=date.fromisoformat(str(item["execution_date"])),
                    volatility=Decimal(str(item["volatility"])),
                    window_hash=str(item["window_hash"]),
                )
                for item in raw_observations
            ),
            valuations=tuple(
                LowVolatilityPaperValuation.from_payload(item) for item in raw_valuations
            ),
            selected_instruments=selected,
            rebalance_due=_boolean(payload["rebalance_due"]),
            prepared_by=str(payload["prepared_by"]),
            prepared_at=datetime.fromisoformat(str(payload["prepared_at"])),
            execution_timing_compatible=_boolean(payload["execution_timing_compatible"]),
            runtime_activation_allowed=_boolean(payload["runtime_activation_allowed"]),
            live_trading_locked=_boolean(payload["live_trading_locked"]),
            decision_policy=str(payload["decision_policy"]),
            version=str(payload["version"]),
        )
        if value.payload() != payload:
            raise ValueError("low-volatility paper daily signal payload is not canonical")
        return value


def _string_list(value: object, *, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise TypeError(f"low-volatility paper {name} is invalid")
    return tuple(value)


def _object_list(
    value: object,
    *,
    name: str,
) -> tuple[dict[str, object], ...]:
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise TypeError(f"low-volatility paper {name} is invalid")
    return tuple(
        {str(key): item for key, item in raw.items()} for raw in value if isinstance(raw, dict)
    )


def _boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise TypeError("low-volatility paper signal boolean is invalid")
    return value
