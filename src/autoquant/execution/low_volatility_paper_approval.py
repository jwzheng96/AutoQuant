from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _datetime_text,
    _require_lowercase_sha256,
    _require_nonblank,
)

LOW_VOLATILITY_PAPER_APPROVAL_VERSION = "low-volatility-paper-candidate-approval-v1"
LOW_VOLATILITY_PAPER_REVOCATION_VERSION = "low-volatility-paper-candidate-revocation-v1"
_INSTRUMENT = re.compile(r"[0-9]{6}\.(?:XSHG|XSHE)\Z")


class LowVolatilityPaperRevocationReason(StrEnum):
    EVIDENCE_INVALIDATED = "evidence_invalidated"
    RISK_CHANGED = "risk_changed"
    RUNTIME_DESIGN_CHANGED = "runtime_design_changed"
    OPERATOR_SAFETY_ACTION = "operator_safety_action"


@dataclass(frozen=True, slots=True)
class LowVolatilityPaperCandidateApproval:
    account_id: str
    strategy_id: str
    forward_spec_hash: str
    evaluation_result_hash: str
    evaluation_assessment_hash: str
    evaluation_dataset_manifest_hash: str
    source_spec_hash: str
    risk_policy_hash: str
    instruments: tuple[str, ...]
    approved_by: str
    approved_at: datetime
    evidence_status: str = "paper_candidate"
    minimum_paper_sessions: int = 60
    execution_mode: str = "paper"
    daily_signal_evidence_required: bool = True
    runtime_activation_allowed: bool = False
    live_trading_locked: bool = True
    version: str = LOW_VOLATILITY_PAPER_APPROVAL_VERSION
    approval_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for name, value in (
            ("account_id", self.account_id),
            ("strategy_id", self.strategy_id),
            ("approved_by", self.approved_by),
        ):
            _require_nonblank(value, name=name)
            if value != value.strip() or len(value) > 128:
                raise ValueError(f"{name} must contain 1-128 trimmed characters")
        for name, value in (
            ("forward_spec_hash", self.forward_spec_hash),
            (
                "evaluation_result_hash",
                self.evaluation_result_hash,
            ),
            (
                "evaluation_assessment_hash",
                self.evaluation_assessment_hash,
            ),
            (
                "evaluation_dataset_manifest_hash",
                self.evaluation_dataset_manifest_hash,
            ),
            ("source_spec_hash", self.source_spec_hash),
            ("risk_policy_hash", self.risk_policy_hash),
        ):
            _require_lowercase_sha256(value, name=name)
        instruments = tuple(self.instruments)
        if (
            not instruments
            or instruments != tuple(sorted(instruments))
            or len(set(instruments)) != len(instruments)
            or len(instruments) > 1000
            or any(_INSTRUMENT.fullmatch(value) is None for value in instruments)
            or self.evidence_status != "paper_candidate"
            or self.minimum_paper_sessions != 60
            or self.execution_mode != "paper"
            or not self.daily_signal_evidence_required
            or self.runtime_activation_allowed
            or not self.live_trading_locked
            or self.version != LOW_VOLATILITY_PAPER_APPROVAL_VERSION
        ):
            raise ValueError("low-volatility paper candidate approval is invalid")
        object.__setattr__(self, "instruments", instruments)
        object.__setattr__(
            self,
            "approved_at",
            to_utc(
                self.approved_at,
                name="low-volatility paper approval time",
            ),
        )
        object.__setattr__(
            self,
            "approval_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "account_id": self.account_id,
            "approved_at": _datetime_text(self.approved_at),
            "approved_by": self.approved_by,
            "daily_signal_evidence_required": (self.daily_signal_evidence_required),
            "evaluation_assessment_hash": (self.evaluation_assessment_hash),
            "evaluation_dataset_manifest_hash": (self.evaluation_dataset_manifest_hash),
            "evaluation_result_hash": self.evaluation_result_hash,
            "evidence_status": self.evidence_status,
            "execution_mode": self.execution_mode,
            "forward_spec_hash": self.forward_spec_hash,
            "instruments": list(self.instruments),
            "live_trading_locked": self.live_trading_locked,
            "minimum_paper_sessions": self.minimum_paper_sessions,
            "risk_policy_hash": self.risk_policy_hash,
            "runtime_activation_allowed": (self.runtime_activation_allowed),
            "source_spec_hash": self.source_spec_hash,
            "strategy_id": self.strategy_id,
            "version": self.version,
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> LowVolatilityPaperCandidateApproval:
        raw_instruments = payload["instruments"]
        if not isinstance(raw_instruments, list) or any(
            not isinstance(value, str) for value in raw_instruments
        ):
            raise TypeError("low-volatility paper approval instruments are invalid")
        value = cls(
            account_id=str(payload["account_id"]),
            strategy_id=str(payload["strategy_id"]),
            forward_spec_hash=str(payload["forward_spec_hash"]),
            evaluation_result_hash=str(payload["evaluation_result_hash"]),
            evaluation_assessment_hash=str(payload["evaluation_assessment_hash"]),
            evaluation_dataset_manifest_hash=str(payload["evaluation_dataset_manifest_hash"]),
            source_spec_hash=str(payload["source_spec_hash"]),
            risk_policy_hash=str(payload["risk_policy_hash"]),
            instruments=tuple(raw_instruments),
            approved_by=str(payload["approved_by"]),
            approved_at=datetime.fromisoformat(str(payload["approved_at"])),
            evidence_status=str(payload["evidence_status"]),
            minimum_paper_sessions=int(str(payload["minimum_paper_sessions"])),
            execution_mode=str(payload["execution_mode"]),
            daily_signal_evidence_required=_boolean(payload["daily_signal_evidence_required"]),
            runtime_activation_allowed=_boolean(payload["runtime_activation_allowed"]),
            live_trading_locked=_boolean(payload["live_trading_locked"]),
            version=str(payload["version"]),
        )
        if value.payload() != payload:
            raise ValueError("low-volatility paper approval payload is not canonical")
        return value


@dataclass(frozen=True, slots=True)
class LowVolatilityPaperCandidateRevocation:
    approval_hash: str
    revoked_by: str
    revoked_at: datetime
    reason: LowVolatilityPaperRevocationReason
    live_trading_locked: bool = True
    version: str = LOW_VOLATILITY_PAPER_REVOCATION_VERSION
    revocation_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.approval_hash,
            name="low-volatility approval hash",
        )
        _require_nonblank(self.revoked_by, name="revoked_by")
        if (
            self.revoked_by != self.revoked_by.strip()
            or len(self.revoked_by) > 128
            or not isinstance(
                self.reason,
                LowVolatilityPaperRevocationReason,
            )
            or not self.live_trading_locked
            or self.version != LOW_VOLATILITY_PAPER_REVOCATION_VERSION
        ):
            raise ValueError("low-volatility paper revocation is invalid")
        object.__setattr__(
            self,
            "revoked_at",
            to_utc(
                self.revoked_at,
                name="low-volatility paper revocation time",
            ),
        )
        object.__setattr__(
            self,
            "revocation_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "approval_hash": self.approval_hash,
            "live_trading_locked": self.live_trading_locked,
            "reason": self.reason.value,
            "revoked_at": _datetime_text(self.revoked_at),
            "revoked_by": self.revoked_by,
            "version": self.version,
        }


def _boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise TypeError("low-volatility paper approval boolean is invalid")
    return value
