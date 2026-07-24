from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Final

from autoquant.clock import SHANGHAI, to_utc
from autoquant.data.models import (
    _canonical_hash,
    _datetime_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.errors import BrokerStateUnknownError
from autoquant.execution.qmt_canary_contract import QmtCanaryOrderStage
from autoquant.execution.qmt_readonly import QmtReadOnlyBaseline

QMT_CANARY_REMARK_RECOVERY_VERSION: Final = "qmt-canary-remark-recovery-v1"


@dataclass(frozen=True, slots=True)
class QmtCanaryRemarkRecovery:
    stage_hash: str
    candidate_hash: str
    account_id: str
    broker_session_date: date
    broker_order_id: str
    client_order_id: str
    baseline_hash: str
    observed_at: datetime
    version: str = QMT_CANARY_REMARK_RECOVERY_VERSION
    recovery_hash: str = field(init=False)

    @property
    def broker_mutation_allowed(self) -> bool:
        return False

    def __post_init__(self) -> None:
        for value, name in (
            (self.stage_hash, "QMT stage hash"),
            (self.candidate_hash, "QMT candidate hash"),
            (self.baseline_hash, "QMT baseline hash"),
        ):
            _require_lowercase_sha256(value, name=name)
        for value, name in (
            (self.account_id, "QMT recovery account_id"),
            (self.client_order_id, "QMT recovery client_order_id"),
            (self.broker_order_id, "QMT recovery broker_order_id"),
        ):
            _require_nonblank(value, name=name)
        for value, name in (
            (self.account_id, "QMT recovery account_id"),
            (self.client_order_id, "QMT recovery client_order_id"),
        ):
            if value != value.strip() or len(value) > 128:
                raise ValueError(f"{name} must be trimmed and at most 128 characters")
        if (
            not self.broker_order_id.isascii()
            or not self.broker_order_id.isdigit()
            or int(self.broker_order_id) < 1
        ):
            raise ValueError("QMT recovery broker_order_id must be a positive integer string")
        if not isinstance(self.broker_session_date, date):
            raise TypeError("broker_session_date must be a date")
        observed_at = to_utc(self.observed_at, name="QMT recovery observation time")
        if (
            observed_at.astimezone(SHANGHAI).date() != self.broker_session_date
            or self.version != QMT_CANARY_REMARK_RECOVERY_VERSION
        ):
            raise ValueError("QMT recovery date or version is unsupported")
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "recovery_hash", _canonical_hash(self.payload()))

    def payload(self) -> dict[str, object]:
        return {
            "account_id": self.account_id,
            "baseline_hash": self.baseline_hash,
            "broker_mutation_allowed": False,
            "broker_order_id": self.broker_order_id,
            "broker_session_date": self.broker_session_date.isoformat(),
            "candidate_hash": self.candidate_hash,
            "client_order_id": self.client_order_id,
            "observed_at": _datetime_text(self.observed_at),
            "stage_hash": self.stage_hash,
            "version": self.version,
        }


def match_qmt_canary_stage(
    stage: QmtCanaryOrderStage,
    baseline: QmtReadOnlyBaseline,
) -> QmtCanaryRemarkRecovery | None:
    """Match one unresolved stage against one coherent same-day QMT query."""

    if not isinstance(stage, QmtCanaryOrderStage):
        raise TypeError("stage must be QmtCanaryOrderStage")
    if not isinstance(baseline, QmtReadOnlyBaseline):
        raise TypeError("baseline must be QmtReadOnlyBaseline")
    observed_at = baseline.query_completed_at
    if (
        baseline.logical_account_id != stage.account_id
        or observed_at.astimezone(SHANGHAI).date() != stage.broker_session_date
    ):
        raise BrokerStateUnknownError(
            "QMT recovery baseline does not match the staged account and session date"
        )
    matches = tuple(
        order
        for order in baseline.orders
        if order.order_remark == stage.broker_order_remark
    )
    if not matches:
        return None
    if len(matches) != 1:
        raise BrokerStateUnknownError(
            "QMT recovery remark matched more than one broker order"
        )
    order = matches[0]
    if (
        order.instrument != stage.instrument
        or order.side is not stage.side
        or order.order_volume != stage.quantity
        or order.order_price != stage.limit_price
        or (
            not order.client_order_id.startswith("qmt-unmapped-")
            and order.client_order_id != stage.client_order_id
        )
    ):
        raise BrokerStateUnknownError(
            "QMT recovery remark matched conflicting broker order facts"
        )
    return QmtCanaryRemarkRecovery(
        stage_hash=stage.stage_hash,
        candidate_hash=stage.candidate_hash,
        account_id=stage.account_id,
        broker_session_date=stage.broker_session_date,
        broker_order_id=order.broker_order_id,
        client_order_id=stage.client_order_id,
        baseline_hash=baseline.evidence_hash,
        observed_at=observed_at,
    )
