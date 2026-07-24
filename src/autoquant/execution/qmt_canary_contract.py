from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Final

from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _datetime_text,
    _decimal_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.errors import BrokerStateUnknownError, LiveTradingLockedError
from autoquant.execution.qmt_models import to_qmt_instrument
from autoquant.risk.models import ExecutionMode, RiskDecision, RiskDecisionState

QMT_CANARY_CANDIDATE_VERSION: Final = "qmt-canary-order-candidate-v1"
MAXIMUM_CANDIDATE_LIFETIME: Final = timedelta(seconds=30)
MAXIMUM_DECISION_AGE: Final = timedelta(seconds=5)


@dataclass(frozen=True, slots=True)
class QmtCanaryOrderCandidate:
    """Exact short-lived evidence for one future order, never an authorization."""

    account_id: str
    strategy_id: str
    gateway_holder_id: str
    qmt_session_id: int
    qmt_lease_generation: int
    decision: RiskDecision
    promotion_report_hash: str
    compliance_approval_hash: str
    qmt_acceptance_hash: str
    reconciliation_report_hash: str
    maximum_order_notional: Decimal
    created_at: datetime
    valid_until: datetime
    version: str = QMT_CANARY_CANDIDATE_VERSION
    candidate_hash: str = field(init=False)

    @property
    def broker_mutation_allowed(self) -> bool:
        return False

    def __post_init__(self) -> None:
        for value, name in (
            (self.account_id, "canary account_id"),
            (self.strategy_id, "canary strategy_id"),
            (self.gateway_holder_id, "canary gateway_holder_id"),
        ):
            _require_nonblank(value, name=name)
            if value != value.strip() or len(value) > 128:
                raise ValueError(f"{name} must be trimmed and at most 128 characters")
        if (
            not isinstance(self.qmt_session_id, int)
            or isinstance(self.qmt_session_id, bool)
            or self.qmt_session_id < 1
        ):
            raise ValueError("qmt_session_id must be a positive integer")
        if (
            not isinstance(self.qmt_lease_generation, int)
            or isinstance(self.qmt_lease_generation, bool)
            or self.qmt_lease_generation < 1
        ):
            raise ValueError("qmt_lease_generation must be a positive integer")
        if not isinstance(self.decision, RiskDecision):
            raise TypeError("decision must be RiskDecision")
        if (
            self.decision.account_id != self.account_id
            or self.decision.mode is not ExecutionMode.LIVE
            or self.decision.state is not RiskDecisionState.ACCEPTED
        ):
            raise ValueError("candidate requires a matching accepted live risk decision")
        to_qmt_instrument(self.decision.order.instrument)
        for value, name in (
            (self.promotion_report_hash, "promotion report hash"),
            (self.compliance_approval_hash, "compliance approval hash"),
            (self.qmt_acceptance_hash, "QMT acceptance hash"),
            (self.reconciliation_report_hash, "reconciliation report hash"),
        ):
            _require_lowercase_sha256(value, name=name)
        if (
            not isinstance(self.maximum_order_notional, Decimal)
            or not self.maximum_order_notional.is_finite()
            or self.maximum_order_notional <= 0
            or self.decision.order_notional <= 0
            or self.decision.order_notional > self.maximum_order_notional
        ):
            raise ValueError("candidate order notional exceeds its positive finite cap")
        created_at = to_utc(self.created_at, name="canary candidate creation time")
        valid_until = to_utc(self.valid_until, name="canary candidate expiry")
        decision_age = created_at - self.decision.evaluated_at
        if (
            decision_age < timedelta(0)
            or decision_age > MAXIMUM_DECISION_AGE
            or valid_until <= created_at
            or valid_until - created_at > MAXIMUM_CANDIDATE_LIFETIME
            or self.version != QMT_CANARY_CANDIDATE_VERSION
        ):
            raise ValueError("canary candidate timing or version is unsupported")
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "valid_until", valid_until)
        object.__setattr__(self, "candidate_hash", _canonical_hash(self.payload()))

    def payload(self) -> dict[str, object]:
        order = self.decision.order
        return {
            "account_id": self.account_id,
            "broker_mutation_allowed": False,
            "compliance_approval_hash": self.compliance_approval_hash,
            "created_at": _datetime_text(self.created_at),
            "gateway_holder_id": self.gateway_holder_id,
            "maximum_order_notional": _decimal_text(self.maximum_order_notional),
            "order": {
                "client_order_id": order.client_order_id,
                "instrument": order.instrument,
                "limit_price": (
                    None if order.limit_price is None else _decimal_text(order.limit_price)
                ),
                "quantity": order.quantity,
                "side": order.side.value,
                "submitted_at": _datetime_text(order.submitted_at),
            },
            "order_count_limit": 1,
            "promotion_report_hash": self.promotion_report_hash,
            "qmt_acceptance_hash": self.qmt_acceptance_hash,
            "qmt_lease_generation": self.qmt_lease_generation,
            "qmt_session_id": self.qmt_session_id,
            "reconciliation_report_hash": self.reconciliation_report_hash,
            "risk_decision_hash": self.decision.decision_hash,
            "risk_policy_hash": self.decision.policy_hash,
            "strategy_id": self.strategy_id,
            "valid_until": _datetime_text(self.valid_until),
            "version": self.version,
        }

    def require_current(self, *, now: datetime) -> None:
        instant = to_utc(now, name="canary candidate validation time")
        if instant < self.created_at or instant >= self.valid_until:
            raise ValueError("canary order candidate is not currently valid")

    def require_broker_mutation(self, *, now: datetime) -> None:
        self.require_current(now=now)
        raise LiveTradingLockedError(
            "QMT canary candidate is evidence only; broker mutation is hard-locked"
        )


@dataclass(frozen=True, slots=True)
class QmtOrderCorrelation:
    candidate_hash: str
    client_order_id: str
    async_request_id: int
    reserved_at: datetime
    broker_order_id: str | None = None
    bound_at: datetime | None = None
    correlation_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(self.candidate_hash, name="QMT candidate hash")
        _require_nonblank(self.client_order_id, name="QMT client_order_id")
        if (
            not isinstance(self.async_request_id, int)
            or isinstance(self.async_request_id, bool)
            or self.async_request_id < 1
        ):
            raise ValueError("QMT async_request_id must be positive")
        reserved_at = to_utc(self.reserved_at, name="QMT correlation reservation time")
        object.__setattr__(self, "reserved_at", reserved_at)
        if (self.broker_order_id is None) != (self.bound_at is None):
            raise ValueError("QMT broker order identity and binding time must appear together")
        if self.broker_order_id is not None:
            _require_nonblank(self.broker_order_id, name="QMT broker_order_id")
            if (
                not self.broker_order_id.isascii()
                or not self.broker_order_id.isdigit()
                or int(self.broker_order_id) < 1
            ):
                raise ValueError("QMT broker_order_id must be a positive integer string")
            assert self.bound_at is not None
            bound_at = to_utc(self.bound_at, name="QMT correlation binding time")
            if bound_at < reserved_at:
                raise ValueError("QMT correlation binding cannot precede reservation")
            object.__setattr__(self, "bound_at", bound_at)
        object.__setattr__(
            self,
            "correlation_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "async_request_id": self.async_request_id,
            "bound_at": (None if self.bound_at is None else _datetime_text(self.bound_at)),
            "broker_order_id": self.broker_order_id,
            "candidate_hash": self.candidate_hash,
            "client_order_id": self.client_order_id,
            "reserved_at": _datetime_text(self.reserved_at),
            "version": "qmt-order-correlation-v1",
        }


class QmtOrderCorrelationBook:
    """Fail-closed one-to-one mapping for a future asynchronous QMT gateway."""

    def __init__(self) -> None:
        self._by_candidate: dict[str, QmtOrderCorrelation] = {}
        self._by_request: dict[int, str] = {}
        self._by_broker_order: dict[str, str] = {}

    @classmethod
    def restore(
        cls,
        correlations: tuple[QmtOrderCorrelation, ...],
    ) -> QmtOrderCorrelationBook:
        book = cls()
        for correlation in sorted(
            correlations,
            key=lambda item: (item.reserved_at, item.async_request_id),
        ):
            if not isinstance(correlation, QmtOrderCorrelation):
                raise TypeError("correlations must contain QmtOrderCorrelation values")
            if (
                correlation.candidate_hash in book._by_candidate
                or correlation.async_request_id in book._by_request
                or any(
                    item.client_order_id == correlation.client_order_id
                    for item in book._by_candidate.values()
                )
            ):
                raise BrokerStateUnknownError(
                    "persisted QMT reservations contain duplicate identities"
                )
            book._by_candidate[correlation.candidate_hash] = correlation
            book._by_request[correlation.async_request_id] = correlation.candidate_hash
            if correlation.broker_order_id is not None:
                if correlation.broker_order_id in book._by_broker_order:
                    raise BrokerStateUnknownError(
                        "persisted QMT bindings contain duplicate broker orders"
                    )
                book._by_broker_order[correlation.broker_order_id] = correlation.candidate_hash
        return book

    def reserve(
        self,
        candidate: QmtCanaryOrderCandidate,
        *,
        async_request_id: int,
        reserved_at: datetime,
    ) -> QmtOrderCorrelation:
        if not isinstance(candidate, QmtCanaryOrderCandidate):
            raise TypeError("candidate must be QmtCanaryOrderCandidate")
        candidate.require_current(now=reserved_at)
        proposed = QmtOrderCorrelation(
            candidate_hash=candidate.candidate_hash,
            client_order_id=candidate.decision.order.client_order_id,
            async_request_id=async_request_id,
            reserved_at=reserved_at,
        )
        existing = self._by_candidate.get(candidate.candidate_hash)
        if existing is not None:
            if existing != proposed:
                raise ValueError("QMT candidate already has another correlation")
            return existing
        if async_request_id in self._by_request:
            raise ValueError("QMT async request identifier is already reserved")
        if any(
            item.client_order_id == proposed.client_order_id for item in self._by_candidate.values()
        ):
            raise ValueError("QMT client_order_id is already reserved")
        self._by_candidate[candidate.candidate_hash] = proposed
        self._by_request[async_request_id] = candidate.candidate_hash
        return proposed

    def bind(
        self,
        *,
        async_request_id: int,
        broker_order_id: str,
        bound_at: datetime,
    ) -> QmtOrderCorrelation:
        candidate_hash = self._by_request.get(async_request_id)
        if candidate_hash is None:
            raise BrokerStateUnknownError("QMT async response has no reserved candidate")
        current = self._by_candidate[candidate_hash]
        proposed = QmtOrderCorrelation(
            candidate_hash=current.candidate_hash,
            client_order_id=current.client_order_id,
            async_request_id=current.async_request_id,
            reserved_at=current.reserved_at,
            broker_order_id=broker_order_id,
            bound_at=bound_at,
        )
        if current.broker_order_id is not None:
            if current != proposed:
                raise BrokerStateUnknownError(
                    "QMT async response conflicts with an existing order binding"
                )
            return current
        owner = self._by_broker_order.get(broker_order_id)
        if owner is not None and owner != candidate_hash:
            raise BrokerStateUnknownError(
                "QMT broker order identifier belongs to another candidate"
            )
        self._by_candidate[candidate_hash] = proposed
        self._by_broker_order[broker_order_id] = candidate_hash
        return proposed

    def client_order_id(self, *, broker_order_id: str) -> str:
        candidate_hash = self._by_broker_order.get(broker_order_id)
        if candidate_hash is None:
            raise BrokerStateUnknownError("QMT broker order is not correlated")
        return self._by_candidate[candidate_hash].client_order_id

    def broker_mapping(self) -> dict[int, str]:
        return {
            int(broker_order_id): self._by_candidate[candidate_hash].client_order_id
            for broker_order_id, candidate_hash in self._by_broker_order.items()
        }
