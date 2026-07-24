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

LOW_VOLATILITY_PAPER_DEPLOYMENT_CONTRACT_VERSION = "low-volatility-paper-deployment-contract-v1"
LOW_VOLATILITY_DEPLOYABLE_SIGNAL_POLICY_VERSION = "low-volatility-decision-time-paper-signal-v2"


@dataclass(frozen=True, slots=True)
class LowVolatilityPaperDeploymentContract:
    source_spec_hash: str
    forward_spec_hash: str
    compatibility_spec_hash: str
    observed_forward_session_count: int
    frozen_by: str
    frozen_at: datetime
    minimum_forward_sessions: int = 126
    minimum_paper_sessions: int = 60
    required_forward_evidence_status: str = "paper_candidate"
    required_compatibility_status: str = "compatible"
    required_order_policy_version: str = LOW_VOLATILITY_DECISION_TIME_ORDER_POLICY_VERSION
    required_daily_signal_policy_version: str = LOW_VOLATILITY_DEPLOYABLE_SIGNAL_POLICY_VERSION
    candidate_approval_after_compatibility_required: bool = True
    exact_session_signal_required: bool = True
    decision_time_signal_required: bool = True
    preopen_signal_required: bool = True
    point_in_time_universe_required: bool = True
    held_position_valuation_coverage_required: bool = True
    exact_risk_policy_required: bool = True
    kill_switch_active_at_authorization_required: bool = True
    exclusive_paper_deployment_required: bool = True
    fresh_runtime_unlock_evidence_required: bool = True
    runtime_authorization_separate: bool = True
    terminal_outcome_observed_before_freeze: bool = False
    historical_reclassification_allowed: bool = False
    paper_activation_authority_granted: bool = False
    runtime_activation_allowed: bool = False
    live_trading_locked: bool = True
    version: str = LOW_VOLATILITY_PAPER_DEPLOYMENT_CONTRACT_VERSION
    contract_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for name, value in (
            ("source_spec_hash", self.source_spec_hash),
            ("forward_spec_hash", self.forward_spec_hash),
            (
                "compatibility_spec_hash",
                self.compatibility_spec_hash,
            ),
        ):
            _require_lowercase_sha256(value, name=name)
        _require_nonblank(self.frozen_by, name="frozen_by")
        frozen_at = to_utc(
            self.frozen_at,
            name="paper deployment contract freeze time",
        )
        required_gates = (
            self.candidate_approval_after_compatibility_required,
            self.exact_session_signal_required,
            self.decision_time_signal_required,
            self.preopen_signal_required,
            self.point_in_time_universe_required,
            self.held_position_valuation_coverage_required,
            self.exact_risk_policy_required,
            self.kill_switch_active_at_authorization_required,
            self.exclusive_paper_deployment_required,
            self.fresh_runtime_unlock_evidence_required,
            self.runtime_authorization_separate,
        )
        if (
            self.frozen_by != self.frozen_by.strip()
            or len(self.frozen_by) > 128
            or not 0 <= self.observed_forward_session_count < self.minimum_forward_sessions
            or self.minimum_forward_sessions != 126
            or self.minimum_paper_sessions != 60
            or self.required_forward_evidence_status != "paper_candidate"
            or self.required_compatibility_status != "compatible"
            or self.required_order_policy_version
            != LOW_VOLATILITY_DECISION_TIME_ORDER_POLICY_VERSION
            or self.required_daily_signal_policy_version
            != LOW_VOLATILITY_DEPLOYABLE_SIGNAL_POLICY_VERSION
            or not all(required_gates)
            or self.terminal_outcome_observed_before_freeze
            or self.historical_reclassification_allowed
            or self.paper_activation_authority_granted
            or self.runtime_activation_allowed
            or not self.live_trading_locked
            or self.version != LOW_VOLATILITY_PAPER_DEPLOYMENT_CONTRACT_VERSION
        ):
            raise ValueError("low-volatility paper deployment contract is invalid")
        object.__setattr__(self, "frozen_at", frozen_at)
        object.__setattr__(
            self,
            "contract_hash",
            _canonical_hash(self.payload()),
        )

    @property
    def partial_outcome_observed_before_freeze(self) -> bool:
        return self.observed_forward_session_count > 0

    def payload(self) -> dict[str, object]:
        return {
            "candidate_approval_after_compatibility_required": (
                self.candidate_approval_after_compatibility_required
            ),
            "compatibility_spec_hash": (self.compatibility_spec_hash),
            "decision_time_signal_required": (self.decision_time_signal_required),
            "exact_risk_policy_required": (self.exact_risk_policy_required),
            "exact_session_signal_required": (self.exact_session_signal_required),
            "exclusive_paper_deployment_required": (self.exclusive_paper_deployment_required),
            "forward_spec_hash": self.forward_spec_hash,
            "fresh_runtime_unlock_evidence_required": (self.fresh_runtime_unlock_evidence_required),
            "frozen_at": _datetime_text(self.frozen_at),
            "frozen_by": self.frozen_by,
            "held_position_valuation_coverage_required": (
                self.held_position_valuation_coverage_required
            ),
            "historical_reclassification_allowed": (self.historical_reclassification_allowed),
            "kill_switch_active_at_authorization_required": (
                self.kill_switch_active_at_authorization_required
            ),
            "live_trading_locked": self.live_trading_locked,
            "minimum_forward_sessions": (self.minimum_forward_sessions),
            "minimum_paper_sessions": self.minimum_paper_sessions,
            "observed_forward_session_count": (self.observed_forward_session_count),
            "paper_activation_authority_granted": (self.paper_activation_authority_granted),
            "partial_outcome_observed_before_freeze": (self.partial_outcome_observed_before_freeze),
            "point_in_time_universe_required": (self.point_in_time_universe_required),
            "preopen_signal_required": (self.preopen_signal_required),
            "required_compatibility_status": (self.required_compatibility_status),
            "required_daily_signal_policy_version": (self.required_daily_signal_policy_version),
            "required_forward_evidence_status": (self.required_forward_evidence_status),
            "required_order_policy_version": (self.required_order_policy_version),
            "runtime_activation_allowed": (self.runtime_activation_allowed),
            "runtime_authorization_separate": (self.runtime_authorization_separate),
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
    ) -> LowVolatilityPaperDeploymentContract:
        value = cls(
            source_spec_hash=str(payload["source_spec_hash"]),
            forward_spec_hash=str(payload["forward_spec_hash"]),
            compatibility_spec_hash=str(payload["compatibility_spec_hash"]),
            observed_forward_session_count=int(str(payload["observed_forward_session_count"])),
            frozen_by=str(payload["frozen_by"]),
            frozen_at=datetime.fromisoformat(str(payload["frozen_at"])),
            minimum_forward_sessions=int(str(payload["minimum_forward_sessions"])),
            minimum_paper_sessions=int(str(payload["minimum_paper_sessions"])),
            required_forward_evidence_status=str(payload["required_forward_evidence_status"]),
            required_compatibility_status=str(payload["required_compatibility_status"]),
            required_order_policy_version=str(payload["required_order_policy_version"]),
            required_daily_signal_policy_version=str(
                payload["required_daily_signal_policy_version"]
            ),
            candidate_approval_after_compatibility_required=(
                _boolean(payload["candidate_approval_after_compatibility_required"])
            ),
            exact_session_signal_required=_boolean(payload["exact_session_signal_required"]),
            decision_time_signal_required=_boolean(payload["decision_time_signal_required"]),
            preopen_signal_required=_boolean(payload["preopen_signal_required"]),
            point_in_time_universe_required=_boolean(payload["point_in_time_universe_required"]),
            held_position_valuation_coverage_required=(
                _boolean(payload["held_position_valuation_coverage_required"])
            ),
            exact_risk_policy_required=_boolean(payload["exact_risk_policy_required"]),
            kill_switch_active_at_authorization_required=(
                _boolean(payload["kill_switch_active_at_authorization_required"])
            ),
            exclusive_paper_deployment_required=_boolean(
                payload["exclusive_paper_deployment_required"]
            ),
            fresh_runtime_unlock_evidence_required=_boolean(
                payload["fresh_runtime_unlock_evidence_required"]
            ),
            runtime_authorization_separate=_boolean(payload["runtime_authorization_separate"]),
            terminal_outcome_observed_before_freeze=_boolean(
                payload["terminal_outcome_observed_before_freeze"]
            ),
            historical_reclassification_allowed=_boolean(
                payload["historical_reclassification_allowed"]
            ),
            paper_activation_authority_granted=_boolean(
                payload["paper_activation_authority_granted"]
            ),
            runtime_activation_allowed=_boolean(payload["runtime_activation_allowed"]),
            live_trading_locked=_boolean(payload["live_trading_locked"]),
            version=str(payload["version"]),
        )
        if (
            value.partial_outcome_observed_before_freeze
            is not _boolean(payload["partial_outcome_observed_before_freeze"])
            or value.payload() != payload
        ):
            raise ValueError("paper deployment contract payload is not canonical")
        return value


def _boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise TypeError("paper deployment contract boolean is invalid")
    return value
