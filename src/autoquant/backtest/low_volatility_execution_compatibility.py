from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from autoquant.backtest.low_volatility_strategy import (
    LOW_VOLATILITY_DECISION_TIME_ORDER_POLICY_VERSION,
)
from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _datetime_text,
    _require_lowercase_sha256,
    _require_nonblank,
)

LOW_VOLATILITY_EXECUTION_COMPATIBILITY_VERSION = "low-volatility-execution-compatibility-spec-v1"
LOW_VOLATILITY_RESEARCH_EXECUTION_VERSION = "daily-open-conservative-v1"
DECISION_TIME_INPUTS = (
    "preopen_instrument_rules",
    "preopen_suspension_status",
    "prior_adjusted_close",
    "prior_session_volume",
)
FORBIDDEN_INTENT_INPUTS = (
    "execution_close",
    "execution_final_volume",
    "execution_high",
    "execution_low",
    "execution_open",
)


@dataclass(frozen=True, slots=True)
class LowVolatilityExecutionCompatibilitySpec:
    source_spec_hash: str
    forward_spec_hash: str
    observed_forward_session_count: int
    frozen_by: str
    frozen_at: datetime
    minimum_forward_sessions: int = 126
    decision_time_inputs: tuple[str, ...] = DECISION_TIME_INPUTS
    forbidden_intent_inputs: tuple[str, ...] = FORBIDDEN_INTENT_INPUTS
    decision_order_policy_version: str = LOW_VOLATILITY_DECISION_TIME_ORDER_POLICY_VERSION
    research_execution_version: str = LOW_VOLATILITY_RESEARCH_EXECUTION_VERSION
    order_intent_invariance_required: bool = True
    same_forward_window_required: bool = True
    compatibility_can_only_disqualify: bool = True
    terminal_outcome_observed_before_freeze: bool = False
    historical_reclassification_allowed: bool = False
    paper_activation_allowed: bool = False
    live_trading_locked: bool = True
    version: str = LOW_VOLATILITY_EXECUTION_COMPATIBILITY_VERSION
    spec_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.source_spec_hash,
            name="low-volatility compatibility source spec hash",
        )
        _require_lowercase_sha256(
            self.forward_spec_hash,
            name="low-volatility compatibility forward spec hash",
        )
        _require_nonblank(self.frozen_by, name="frozen_by")
        frozen_at = to_utc(
            self.frozen_at,
            name="low-volatility compatibility freeze time",
        )
        if (
            self.frozen_by != self.frozen_by.strip()
            or len(self.frozen_by) > 128
            or not 0 <= self.observed_forward_session_count < self.minimum_forward_sessions
            or self.minimum_forward_sessions != 126
            or self.decision_time_inputs != DECISION_TIME_INPUTS
            or self.forbidden_intent_inputs != FORBIDDEN_INTENT_INPUTS
            or self.decision_order_policy_version
            != LOW_VOLATILITY_DECISION_TIME_ORDER_POLICY_VERSION
            or self.research_execution_version != LOW_VOLATILITY_RESEARCH_EXECUTION_VERSION
            or not self.order_intent_invariance_required
            or not self.same_forward_window_required
            or not self.compatibility_can_only_disqualify
            or self.terminal_outcome_observed_before_freeze
            or self.historical_reclassification_allowed
            or self.paper_activation_allowed
            or not self.live_trading_locked
            or self.version != LOW_VOLATILITY_EXECUTION_COMPATIBILITY_VERSION
        ):
            raise ValueError("low-volatility execution compatibility spec is invalid")
        object.__setattr__(self, "frozen_at", frozen_at)
        object.__setattr__(
            self,
            "spec_hash",
            _canonical_hash(self.payload()),
        )

    @property
    def partial_outcome_observed_before_freeze(self) -> bool:
        return self.observed_forward_session_count > 0

    def payload(self) -> dict[str, object]:
        return {
            "compatibility_can_only_disqualify": (self.compatibility_can_only_disqualify),
            "decision_order_policy_version": (self.decision_order_policy_version),
            "decision_time_inputs": list(self.decision_time_inputs),
            "forbidden_intent_inputs": list(self.forbidden_intent_inputs),
            "forward_spec_hash": self.forward_spec_hash,
            "frozen_at": _datetime_text(self.frozen_at),
            "frozen_by": self.frozen_by,
            "historical_reclassification_allowed": (self.historical_reclassification_allowed),
            "live_trading_locked": self.live_trading_locked,
            "minimum_forward_sessions": (self.minimum_forward_sessions),
            "observed_forward_session_count": (self.observed_forward_session_count),
            "order_intent_invariance_required": (self.order_intent_invariance_required),
            "paper_activation_allowed": (self.paper_activation_allowed),
            "partial_outcome_observed_before_freeze": (self.partial_outcome_observed_before_freeze),
            "research_execution_version": (self.research_execution_version),
            "same_forward_window_required": (self.same_forward_window_required),
            "source_spec_hash": self.source_spec_hash,
            "terminal_outcome_observed_before_freeze": (
                self.terminal_outcome_observed_before_freeze
            ),
            "version": self.version,
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> LowVolatilityExecutionCompatibilitySpec:
        value = cls(
            source_spec_hash=str(payload["source_spec_hash"]),
            forward_spec_hash=str(payload["forward_spec_hash"]),
            observed_forward_session_count=int(str(payload["observed_forward_session_count"])),
            frozen_by=str(payload["frozen_by"]),
            frozen_at=datetime.fromisoformat(str(payload["frozen_at"])),
            minimum_forward_sessions=int(str(payload["minimum_forward_sessions"])),
            decision_time_inputs=_strings(payload["decision_time_inputs"]),
            forbidden_intent_inputs=_strings(payload["forbidden_intent_inputs"]),
            decision_order_policy_version=str(payload["decision_order_policy_version"]),
            research_execution_version=str(payload["research_execution_version"]),
            order_intent_invariance_required=_boolean(payload["order_intent_invariance_required"]),
            same_forward_window_required=_boolean(payload["same_forward_window_required"]),
            compatibility_can_only_disqualify=_boolean(
                payload["compatibility_can_only_disqualify"]
            ),
            terminal_outcome_observed_before_freeze=_boolean(
                payload["terminal_outcome_observed_before_freeze"]
            ),
            historical_reclassification_allowed=_boolean(
                payload["historical_reclassification_allowed"]
            ),
            paper_activation_allowed=_boolean(payload["paper_activation_allowed"]),
            live_trading_locked=_boolean(payload["live_trading_locked"]),
            version=str(payload["version"]),
        )
        if (
            value.partial_outcome_observed_before_freeze
            is not _boolean(payload["partial_outcome_observed_before_freeze"])
            or value.payload() != payload
        ):
            raise ValueError("low-volatility compatibility payload is not canonical")
        return value


def _strings(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise TypeError("low-volatility compatibility string array is invalid")
    return tuple(value)


def _boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise TypeError("low-volatility compatibility boolean is invalid")
    return value
