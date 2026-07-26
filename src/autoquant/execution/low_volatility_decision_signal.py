from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta

from autoquant.backtest.low_volatility_strategy import (
    LOW_VOLATILITY_DECISION_TIME_ORDER_POLICY_VERSION,
)
from autoquant.clock import SHANGHAI, to_utc
from autoquant.data.models import (
    _canonical_hash,
    _datetime_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.execution.low_volatility_paper_deployment_contract import (
    LOW_VOLATILITY_DEPLOYABLE_SIGNAL_POLICY_VERSION,
)

LOW_VOLATILITY_DECISION_TIME_SIGNAL_VERSION = (
    LOW_VOLATILITY_DEPLOYABLE_SIGNAL_POLICY_VERSION
)
LOW_VOLATILITY_DECISION_TIME_ACCOUNT_EVIDENCE_MAX_AGE = timedelta(minutes=5)
_INSTRUMENT = re.compile(r"[0-9]{6}\.(?:XSHG|XSHE)\Z")


@dataclass(frozen=True, slots=True)
class LowVolatilityDecisionTimePaperSignal:
    deployment_contract_hash: str
    candidate_approval_hash: str
    compatibility_run_hash: str
    observation_signal_hash: str
    reconciliation_report_hash: str
    internal_account_snapshot_hash: str
    broker_account_snapshot_hash: str
    kill_switch_event_hash: str
    account_id: str
    strategy_id: str
    source_spec_hash: str
    forward_spec_hash: str
    compatibility_spec_hash: str
    risk_policy_hash: str
    snapshot_hash: str
    dataset_manifest_hash: str
    rule_set_hash: str
    session_sequence: int
    session_date: date
    signal_date: date
    account_evidence_at: datetime
    kill_switch_changed_at: datetime
    selected_instruments: tuple[str, ...]
    held_instruments: tuple[str, ...]
    valuation_instruments: tuple[str, ...]
    prepared_by: str
    prepared_at: datetime
    point_in_time_universe_verified: bool = True
    held_position_valuation_coverage_verified: bool = True
    exact_risk_policy_verified: bool = True
    exact_session_rules_verified: bool = True
    decision_time_inputs_verified: bool = True
    account_reconciled: bool = True
    no_open_orders_verified: bool = True
    kill_switch_active: bool = True
    execution_timing_compatible: bool = True
    paper_activation_authority_granted: bool = False
    runtime_activation_allowed: bool = False
    live_trading_locked: bool = True
    decision_order_policy_version: str = (
        LOW_VOLATILITY_DECISION_TIME_ORDER_POLICY_VERSION
    )
    version: str = LOW_VOLATILITY_DECISION_TIME_SIGNAL_VERSION
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
            ("deployment_contract_hash", self.deployment_contract_hash),
            ("candidate_approval_hash", self.candidate_approval_hash),
            ("compatibility_run_hash", self.compatibility_run_hash),
            ("observation_signal_hash", self.observation_signal_hash),
            ("reconciliation_report_hash", self.reconciliation_report_hash),
            (
                "internal_account_snapshot_hash",
                self.internal_account_snapshot_hash,
            ),
            ("broker_account_snapshot_hash", self.broker_account_snapshot_hash),
            ("kill_switch_event_hash", self.kill_switch_event_hash),
            ("source_spec_hash", self.source_spec_hash),
            ("forward_spec_hash", self.forward_spec_hash),
            ("compatibility_spec_hash", self.compatibility_spec_hash),
            ("risk_policy_hash", self.risk_policy_hash),
            ("snapshot_hash", self.snapshot_hash),
            ("dataset_manifest_hash", self.dataset_manifest_hash),
            ("rule_set_hash", self.rule_set_hash),
        ):
            _require_lowercase_sha256(value, name=name)
        prepared_at = to_utc(
            self.prepared_at,
            name="decision-time paper signal prepared_at",
        )
        account_evidence_at = to_utc(
            self.account_evidence_at,
            name="decision-time account evidence time",
        )
        kill_switch_changed_at = to_utc(
            self.kill_switch_changed_at,
            name="decision-time kill switch time",
        )
        session_open = datetime.combine(
            self.session_date,
            time(9, 30),
            tzinfo=SHANGHAI,
        ).astimezone(prepared_at.tzinfo)
        selected = _instruments(
            self.selected_instruments,
            name="selected instruments",
            allow_empty=True,
        )
        held = _instruments(
            self.held_instruments,
            name="held instruments",
            allow_empty=True,
        )
        valuations = _instruments(
            self.valuation_instruments,
            name="valuation instruments",
        )
        required_gates = (
            self.point_in_time_universe_verified,
            self.held_position_valuation_coverage_verified,
            self.exact_risk_policy_verified,
            self.exact_session_rules_verified,
            self.decision_time_inputs_verified,
            self.account_reconciled,
            self.no_open_orders_verified,
            self.kill_switch_active,
            self.execution_timing_compatible,
        )
        if (
            self.session_sequence < 1
            or self.signal_date >= self.session_date
            or account_evidence_at > prepared_at
            or prepared_at - account_evidence_at
            > LOW_VOLATILITY_DECISION_TIME_ACCOUNT_EVIDENCE_MAX_AGE
            or kill_switch_changed_at > prepared_at
            or prepared_at >= session_open
            or len(selected) not in (0, 20)
            or not set(selected) <= set(valuations)
            or not set(held) <= set(valuations)
            or not all(required_gates)
            or self.paper_activation_authority_granted
            or self.runtime_activation_allowed
            or not self.live_trading_locked
            or self.decision_order_policy_version
            != LOW_VOLATILITY_DECISION_TIME_ORDER_POLICY_VERSION
            or self.version != LOW_VOLATILITY_DECISION_TIME_SIGNAL_VERSION
        ):
            raise ValueError("low-volatility decision-time paper signal is invalid")
        object.__setattr__(self, "account_evidence_at", account_evidence_at)
        object.__setattr__(
            self,
            "kill_switch_changed_at",
            kill_switch_changed_at,
        )
        object.__setattr__(self, "prepared_at", prepared_at)
        object.__setattr__(self, "selected_instruments", selected)
        object.__setattr__(self, "held_instruments", held)
        object.__setattr__(self, "valuation_instruments", valuations)
        object.__setattr__(
            self,
            "signal_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "account_evidence_at": _datetime_text(self.account_evidence_at),
            "account_id": self.account_id,
            "account_reconciled": self.account_reconciled,
            "broker_account_snapshot_hash": self.broker_account_snapshot_hash,
            "candidate_approval_hash": self.candidate_approval_hash,
            "compatibility_run_hash": self.compatibility_run_hash,
            "compatibility_spec_hash": self.compatibility_spec_hash,
            "dataset_manifest_hash": self.dataset_manifest_hash,
            "decision_order_policy_version": self.decision_order_policy_version,
            "decision_time_inputs_verified": self.decision_time_inputs_verified,
            "deployment_contract_hash": self.deployment_contract_hash,
            "exact_risk_policy_verified": self.exact_risk_policy_verified,
            "exact_session_rules_verified": self.exact_session_rules_verified,
            "execution_timing_compatible": self.execution_timing_compatible,
            "forward_spec_hash": self.forward_spec_hash,
            "held_instruments": list(self.held_instruments),
            "held_position_valuation_coverage_verified": (
                self.held_position_valuation_coverage_verified
            ),
            "internal_account_snapshot_hash": (
                self.internal_account_snapshot_hash
            ),
            "kill_switch_active": self.kill_switch_active,
            "kill_switch_changed_at": _datetime_text(
                self.kill_switch_changed_at
            ),
            "kill_switch_event_hash": self.kill_switch_event_hash,
            "live_trading_locked": self.live_trading_locked,
            "no_open_orders_verified": self.no_open_orders_verified,
            "observation_signal_hash": self.observation_signal_hash,
            "paper_activation_authority_granted": (
                self.paper_activation_authority_granted
            ),
            "point_in_time_universe_verified": (
                self.point_in_time_universe_verified
            ),
            "prepared_at": _datetime_text(self.prepared_at),
            "prepared_by": self.prepared_by,
            "reconciliation_report_hash": self.reconciliation_report_hash,
            "risk_policy_hash": self.risk_policy_hash,
            "rule_set_hash": self.rule_set_hash,
            "runtime_activation_allowed": self.runtime_activation_allowed,
            "selected_instruments": list(self.selected_instruments),
            "session_date": self.session_date.isoformat(),
            "session_sequence": self.session_sequence,
            "signal_date": self.signal_date.isoformat(),
            "snapshot_hash": self.snapshot_hash,
            "source_spec_hash": self.source_spec_hash,
            "strategy_id": self.strategy_id,
            "valuation_instruments": list(self.valuation_instruments),
            "version": self.version,
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> LowVolatilityDecisionTimePaperSignal:
        value = cls(
            deployment_contract_hash=str(payload["deployment_contract_hash"]),
            candidate_approval_hash=str(payload["candidate_approval_hash"]),
            compatibility_run_hash=str(payload["compatibility_run_hash"]),
            observation_signal_hash=str(payload["observation_signal_hash"]),
            reconciliation_report_hash=str(
                payload["reconciliation_report_hash"]
            ),
            internal_account_snapshot_hash=str(
                payload["internal_account_snapshot_hash"]
            ),
            broker_account_snapshot_hash=str(
                payload["broker_account_snapshot_hash"]
            ),
            kill_switch_event_hash=str(payload["kill_switch_event_hash"]),
            account_id=str(payload["account_id"]),
            strategy_id=str(payload["strategy_id"]),
            source_spec_hash=str(payload["source_spec_hash"]),
            forward_spec_hash=str(payload["forward_spec_hash"]),
            compatibility_spec_hash=str(payload["compatibility_spec_hash"]),
            risk_policy_hash=str(payload["risk_policy_hash"]),
            snapshot_hash=str(payload["snapshot_hash"]),
            dataset_manifest_hash=str(payload["dataset_manifest_hash"]),
            rule_set_hash=str(payload["rule_set_hash"]),
            session_sequence=int(str(payload["session_sequence"])),
            session_date=date.fromisoformat(str(payload["session_date"])),
            signal_date=date.fromisoformat(str(payload["signal_date"])),
            account_evidence_at=datetime.fromisoformat(
                str(payload["account_evidence_at"])
            ),
            kill_switch_changed_at=datetime.fromisoformat(
                str(payload["kill_switch_changed_at"])
            ),
            selected_instruments=_string_list(
                payload["selected_instruments"],
                name="selected_instruments",
            ),
            held_instruments=_string_list(
                payload["held_instruments"],
                name="held_instruments",
            ),
            valuation_instruments=_string_list(
                payload["valuation_instruments"],
                name="valuation_instruments",
            ),
            prepared_by=str(payload["prepared_by"]),
            prepared_at=datetime.fromisoformat(str(payload["prepared_at"])),
            point_in_time_universe_verified=_boolean(
                payload["point_in_time_universe_verified"]
            ),
            held_position_valuation_coverage_verified=_boolean(
                payload["held_position_valuation_coverage_verified"]
            ),
            exact_risk_policy_verified=_boolean(
                payload["exact_risk_policy_verified"]
            ),
            exact_session_rules_verified=_boolean(
                payload["exact_session_rules_verified"]
            ),
            decision_time_inputs_verified=_boolean(
                payload["decision_time_inputs_verified"]
            ),
            account_reconciled=_boolean(payload["account_reconciled"]),
            no_open_orders_verified=_boolean(
                payload["no_open_orders_verified"]
            ),
            kill_switch_active=_boolean(payload["kill_switch_active"]),
            execution_timing_compatible=_boolean(
                payload["execution_timing_compatible"]
            ),
            paper_activation_authority_granted=_boolean(
                payload["paper_activation_authority_granted"]
            ),
            runtime_activation_allowed=_boolean(
                payload["runtime_activation_allowed"]
            ),
            live_trading_locked=_boolean(payload["live_trading_locked"]),
            decision_order_policy_version=str(
                payload["decision_order_policy_version"]
            ),
            version=str(payload["version"]),
        )
        if value.payload() != payload:
            raise ValueError(
                "low-volatility decision-time signal payload is not canonical"
            )
        return value


def _instruments(
    values: tuple[str, ...],
    *,
    name: str,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    normalized = tuple(values)
    if (
        (not allow_empty and not normalized)
        or normalized != tuple(sorted(normalized))
        or len(set(normalized)) != len(normalized)
        or len(normalized) > 1000
        or any(_INSTRUMENT.fullmatch(value) is None for value in normalized)
    ):
        raise ValueError(f"low-volatility decision-time {name} are invalid")
    return normalized


def _string_list(value: object, *, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) for item in value
    ):
        raise TypeError(f"low-volatility decision-time {name} are invalid")
    return tuple(value)


def _boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise TypeError("low-volatility decision-time boolean is invalid")
    return value
