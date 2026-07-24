from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Final

from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _datetime_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.execution.qmt_callback_reducer import QmtOrderConvergence
from autoquant.execution.qmt_callback_reducer_store import (
    QmtCallbackReductionResult,
)
from autoquant.execution.qmt_readonly import QmtReadOnlyBaseline
from autoquant.execution.qmt_readonly_store import (
    QmtReadOnlyAcceptanceEvidence,
)

QMT_CALLBACK_RECONCILIATION_VERSION: Final = "qmt-callback-reconciliation-v1"
_AUTOQUANT_REMARK = re.compile(r"AQ[0-9a-f]{22}\Z")
_HOLDER_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")


class QmtCallbackReconciliationState(StrEnum):
    PASSED = "passed"
    REJECTED = "rejected"


class QmtCallbackReconciliationIssue(StrEnum):
    ACCEPTANCE_BASELINE_MISMATCH = "acceptance_baseline_mismatch"
    BASELINE_BEFORE_CALLBACK = "baseline_before_callback"
    BROKER_STATE_UNKNOWN = "broker_state_unknown"
    CALLBACK_CURSOR_MISMATCH = "callback_cursor_mismatch"
    CALLBACK_ORDER_CONFLICT = "callback_order_conflict"
    CALLBACK_ORDER_MISSING = "callback_order_missing"
    CALLBACK_PROJECTION_PENDING = "callback_projection_pending"
    CALLBACK_TRADE_CONFLICT = "callback_trade_conflict"
    CALLBACK_TRADE_MISSING = "callback_trade_missing"
    LEASE_SCOPE_MISMATCH = "lease_scope_mismatch"
    UNTRACKED_AUTOQUANT_ORDER = "untracked_autoquant_order"
    UNTRACKED_AUTOQUANT_TRADE = "untracked_autoquant_trade"


@dataclass(frozen=True, slots=True)
class QmtCallbackReconciliationReport:
    logical_account_id: str
    gateway_holder_id: str
    qmt_session_id: int
    qmt_lease_generation: int
    acceptance_evidence_hash: str
    baseline_evidence_hash: str
    callback_processing_hash: str
    callback_cursor: int
    projection_hashes: tuple[str, ...]
    trade_fact_hashes: tuple[str, ...]
    matched_broker_order_ids: tuple[str, ...]
    matched_trade_ids: tuple[str, ...]
    issues: tuple[QmtCallbackReconciliationIssue, ...]
    observed_at: datetime
    version: str = QMT_CALLBACK_RECONCILIATION_VERSION
    report_hash: str = field(init=False)

    @property
    def state(self) -> QmtCallbackReconciliationState:
        if self.issues:
            return QmtCallbackReconciliationState.REJECTED
        return QmtCallbackReconciliationState.PASSED

    @property
    def broker_mutation_allowed(self) -> bool:
        return False

    def __post_init__(self) -> None:
        _require_nonblank(
            self.logical_account_id,
            name="QMT reconciliation logical_account_id",
        )
        if (
            not isinstance(self.gateway_holder_id, str)
            or _HOLDER_ID.fullmatch(self.gateway_holder_id) is None
        ):
            raise ValueError("QMT reconciliation holder is invalid")
        for integer, integer_name in (
            (self.qmt_session_id, "qmt_session_id"),
            (self.qmt_lease_generation, "qmt_lease_generation"),
        ):
            if not isinstance(integer, int) or isinstance(integer, bool) or integer < 1:
                raise ValueError(f"QMT reconciliation {integer_name} must be positive")
        for hash_value, hash_name in (
            (self.acceptance_evidence_hash, "acceptance_evidence_hash"),
            (self.baseline_evidence_hash, "baseline_evidence_hash"),
            (self.callback_processing_hash, "callback_processing_hash"),
        ):
            _require_lowercase_sha256(
                hash_value,
                name=f"QMT reconciliation {hash_name}",
            )
        if (
            not isinstance(self.callback_cursor, int)
            or isinstance(self.callback_cursor, bool)
            or self.callback_cursor < 0
        ):
            raise ValueError("QMT reconciliation callback_cursor must be nonnegative")
        projection_hashes = _sorted_hashes(
            self.projection_hashes,
            name="projection_hash",
        )
        trade_fact_hashes = _sorted_hashes(
            self.trade_fact_hashes,
            name="trade_fact_hash",
        )
        order_ids = _sorted_nonblank(
            self.matched_broker_order_ids,
            name="broker_order_id",
        )
        trade_ids = _sorted_nonblank(
            self.matched_trade_ids,
            name="trade_id",
        )
        issues = tuple(sorted(set(self.issues), key=lambda item: item.value))
        if any(not isinstance(issue, QmtCallbackReconciliationIssue) for issue in issues):
            raise TypeError("QMT reconciliation issues are invalid")
        observed_at = to_utc(
            self.observed_at,
            name="QMT reconciliation observed_at",
        )
        if self.version != QMT_CALLBACK_RECONCILIATION_VERSION:
            raise ValueError("unsupported QMT callback reconciliation version")
        object.__setattr__(self, "projection_hashes", projection_hashes)
        object.__setattr__(self, "trade_fact_hashes", trade_fact_hashes)
        object.__setattr__(self, "matched_broker_order_ids", order_ids)
        object.__setattr__(self, "matched_trade_ids", trade_ids)
        object.__setattr__(self, "issues", issues)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "report_hash", _canonical_hash(self.payload()))

    def payload(self) -> dict[str, object]:
        return {
            "acceptance_evidence_hash": self.acceptance_evidence_hash,
            "baseline_evidence_hash": self.baseline_evidence_hash,
            "broker_mutation_allowed": False,
            "callback_cursor": self.callback_cursor,
            "callback_processing_hash": self.callback_processing_hash,
            "gateway_holder_id": self.gateway_holder_id,
            "issues": [issue.value for issue in self.issues],
            "logical_account_id": self.logical_account_id,
            "matched_broker_order_ids": list(self.matched_broker_order_ids),
            "matched_trade_ids": list(self.matched_trade_ids),
            "observed_at": _datetime_text(self.observed_at),
            "projection_hashes": list(self.projection_hashes),
            "qmt_lease_generation": self.qmt_lease_generation,
            "qmt_session_id": self.qmt_session_id,
            "state": self.state.value,
            "trade_fact_hashes": list(self.trade_fact_hashes),
            "version": self.version,
        }


def reconcile_qmt_callback_state(
    *,
    baseline: QmtReadOnlyBaseline,
    acceptance: QmtReadOnlyAcceptanceEvidence,
    reduction: QmtCallbackReductionResult,
) -> QmtCallbackReconciliationReport:
    if not isinstance(baseline, QmtReadOnlyBaseline):
        raise TypeError("baseline must be QmtReadOnlyBaseline")
    if not isinstance(acceptance, QmtReadOnlyAcceptanceEvidence):
        raise TypeError("acceptance must be QmtReadOnlyAcceptanceEvidence")
    if not isinstance(reduction, QmtCallbackReductionResult):
        raise TypeError("reduction must be QmtCallbackReductionResult")
    issues: set[QmtCallbackReconciliationIssue] = set()
    if (
        acceptance.logical_account_id != baseline.logical_account_id
        or acceptance.baseline_evidence_hash != baseline.evidence_hash
        or acceptance.account_snapshot_hash != baseline.account_snapshot.snapshot_hash
        or acceptance.observed_at != baseline.query_completed_at
        or acceptance.position_count != len(baseline.positions)
        or acceptance.order_count != len(baseline.orders)
        or acceptance.trade_count != len(baseline.trades)
        or acceptance.callback_cursor != baseline.callback_cursor
    ):
        issues.add(QmtCallbackReconciliationIssue.ACCEPTANCE_BASELINE_MISMATCH)
    if (
        reduction.account_id != baseline.logical_account_id
        or reduction.gateway_holder_id != acceptance.lease_holder_id
        or reduction.qmt_session_id != acceptance.lease_session_id
        or reduction.qmt_lease_generation != acceptance.lease_generation
    ):
        issues.add(QmtCallbackReconciliationIssue.LEASE_SCOPE_MISMATCH)
    if reduction.last_local_sequence != baseline.callback_cursor:
        issues.add(QmtCallbackReconciliationIssue.CALLBACK_CURSOR_MISMATCH)
    if not reduction.broker_state_known or reduction.fatal_reason is not None:
        issues.add(QmtCallbackReconciliationIssue.BROKER_STATE_UNKNOWN)

    baseline_orders = {order.broker_order_id: order for order in baseline.orders}
    callback_orders = {
        projection.broker_order_id: projection for projection in reduction.projections
    }
    matched_order_ids: list[str] = []
    for projection in reduction.projections:
        if projection.convergence is not QmtOrderConvergence.CONVERGED:
            issues.add(QmtCallbackReconciliationIssue.CALLBACK_PROJECTION_PENDING)
        if projection.updated_at > baseline.query_started_at:
            issues.add(QmtCallbackReconciliationIssue.BASELINE_BEFORE_CALLBACK)
        order = baseline_orders.get(projection.broker_order_id)
        if order is None:
            issues.add(QmtCallbackReconciliationIssue.CALLBACK_ORDER_MISSING)
            continue
        if (
            order.instrument != projection.instrument
            or order.side is not projection.side
            or order.order_volume != projection.quantity
            or order.order_price != projection.limit_price
            or order.order_remark != projection.order_remark
            or order.traded_volume != projection.reported_traded_volume
            or order.average_traded_price != projection.reported_average_price
            or order.raw_status != projection.raw_order_status
            or order.state is not projection.order_state
        ):
            issues.add(QmtCallbackReconciliationIssue.CALLBACK_ORDER_CONFLICT)
            continue
        matched_order_ids.append(order.broker_order_id)
    for order in baseline.orders:
        if (
            _AUTOQUANT_REMARK.fullmatch(order.order_remark) is not None
            and order.broker_order_id not in callback_orders
        ):
            issues.add(QmtCallbackReconciliationIssue.UNTRACKED_AUTOQUANT_ORDER)

    baseline_trades = {trade.trade_id: trade for trade in baseline.trades}
    callback_trades = {fact.trade_id: fact for fact in reduction.trade_facts}
    matched_trade_ids: list[str] = []
    for fact in reduction.trade_facts:
        if fact.observed_at > baseline.query_started_at:
            issues.add(QmtCallbackReconciliationIssue.BASELINE_BEFORE_CALLBACK)
        trade = baseline_trades.get(fact.trade_id)
        if trade is None:
            issues.add(QmtCallbackReconciliationIssue.CALLBACK_TRADE_MISSING)
            continue
        if (
            trade.broker_order_id != fact.broker_order_id
            or trade.instrument != fact.instrument
            or trade.side is not fact.side
            or trade.volume != fact.volume
            or trade.price != fact.price
            or trade.amount != fact.amount
            or trade.order_remark != fact.order_remark
        ):
            issues.add(QmtCallbackReconciliationIssue.CALLBACK_TRADE_CONFLICT)
            continue
        matched_trade_ids.append(trade.trade_id)
    for trade in baseline.trades:
        order = baseline_orders[trade.broker_order_id]
        if (
            _AUTOQUANT_REMARK.fullmatch(order.order_remark) is not None
            and trade.trade_id not in callback_trades
        ):
            issues.add(QmtCallbackReconciliationIssue.UNTRACKED_AUTOQUANT_TRADE)

    return QmtCallbackReconciliationReport(
        logical_account_id=baseline.logical_account_id,
        gateway_holder_id=acceptance.lease_holder_id,
        qmt_session_id=acceptance.lease_session_id,
        qmt_lease_generation=acceptance.lease_generation,
        acceptance_evidence_hash=acceptance.evidence_hash,
        baseline_evidence_hash=baseline.evidence_hash,
        callback_processing_hash=reduction.last_processing_hash,
        callback_cursor=baseline.callback_cursor,
        projection_hashes=tuple(item.projection_hash for item in reduction.projections),
        trade_fact_hashes=tuple(item.fact_hash for item in reduction.trade_facts),
        matched_broker_order_ids=tuple(matched_order_ids),
        matched_trade_ids=tuple(matched_trade_ids),
        issues=tuple(issues),
        observed_at=baseline.query_completed_at,
    )


def _sorted_hashes(values: tuple[str, ...], *, name: str) -> tuple[str, ...]:
    result = tuple(sorted(values))
    if len(result) != len(set(result)):
        raise ValueError(f"QMT reconciliation {name} values must be unique")
    for value in result:
        _require_lowercase_sha256(value, name=f"QMT reconciliation {name}")
    return result


def _sorted_nonblank(values: tuple[str, ...], *, name: str) -> tuple[str, ...]:
    result = tuple(sorted(values))
    if len(result) != len(set(result)):
        raise ValueError(f"QMT reconciliation {name} values must be unique")
    for value in result:
        _require_nonblank(value, name=f"QMT reconciliation {name}")
    return result


__all__ = [
    "QMT_CALLBACK_RECONCILIATION_VERSION",
    "QmtCallbackReconciliationIssue",
    "QmtCallbackReconciliationReport",
    "QmtCallbackReconciliationState",
    "reconcile_qmt_callback_state",
]
