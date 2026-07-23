from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol
from uuid import uuid4
from zoneinfo import ZoneInfo

from autoquant.backtest.models import InstrumentRules
from autoquant.clock import to_utc
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.account_projection import PaperAccountProjector
from autoquant.execution.control import KillSwitchControl, KillSwitchReason
from autoquant.execution.control_store import PostgresExecutionControlRepository
from autoquant.execution.models import (
    ApprovedPaperOrder,
    BrokerOrderUpdate,
    PaperOrderHistory,
    PaperOrderProjection,
    PaperOrderState,
)
from autoquant.execution.reconciliation import (
    AccountReconciler,
    ExecutionAccountSnapshot,
    ReconciliationReport,
)
from autoquant.execution.session_risk import (
    PaperSessionRiskState,
    SessionRiskObservation,
    derive_session_turnover,
)
from autoquant.execution.session_risk_store import (
    PostgresPaperSessionRiskRepository,
)
from autoquant.execution.simulated_broker import (
    PersistentSimulatedBroker,
    SimulatedBrokerControlError,
)
from autoquant.execution.store import PostgresPaperExecutionRepository
from autoquant.risk.engine import PreTradeRiskEngine
from autoquant.risk.models import (
    ExecutionMode,
    MarketQuote,
    ProposedOrder,
    RiskAccountState,
    RiskDecision,
    RiskDecisionState,
    RiskEvaluationInput,
    RiskPolicy,
    RiskPosition,
)

_SHANGHAI = ZoneInfo("Asia/Shanghai")


class StoredRiskDecision(Protocol):
    decision_hash: str
    state: str
    evaluated_at: datetime
    policy_hash: str
    quote_hash: str


class RiskDecisionRepository(Protocol):
    async def append(self, decision: RiskDecision) -> object: ...

    async def get(self, *, account_id: str, client_order_id: str) -> StoredRiskDecision: ...


@dataclass(frozen=True, slots=True)
class PaperSubmissionRequest:
    order: ProposedOrder
    quote: MarketQuote
    rules: InstrumentRules
    policy: RiskPolicy
    marks: dict[str, Decimal]
    now: datetime

    def __post_init__(self) -> None:
        if (
            self.quote.instrument != self.order.instrument
            or self.rules.instrument != self.order.instrument
        ):
            raise ValueError("order, quote, and rules instruments must match")
        marks = dict(self.marks)
        object.__setattr__(self, "marks", marks)
        if marks.get(self.order.instrument) != self.quote.last_price:
            raise ValueError("order instrument mark must equal the submitted quote last price")
        object.__setattr__(self, "now", to_utc(self.now, name="coordination time"))


class PaperCoordinationStatus(StrEnum):
    RISK_REJECTED = "risk_rejected"
    SUBMITTED = "submitted"
    FILLED = "filled"
    RECOVERED = "recovered"
    BLOCKED = "blocked"
    RECONCILIATION_FAILED = "reconciliation_failed"


@dataclass(frozen=True, slots=True)
class PaperCoordinationResult:
    status: PaperCoordinationStatus
    control: KillSwitchControl
    decision: RiskDecision | None
    projection: PaperOrderProjection | None
    updates: tuple[BrokerOrderUpdate, ...]
    pre_reconciliation: ReconciliationReport | None
    post_reconciliation: ReconciliationReport | None


@dataclass(frozen=True, slots=True)
class _AccountEvidence:
    snapshot: ExecutionAccountSnapshot
    histories: tuple[PaperOrderHistory, ...]
    seen_client_order_ids: tuple[str, ...]


class PaperOrderCoordinator:
    """Fail-closed paper order orchestration with durable recovery boundaries."""

    def __init__(
        self,
        *,
        account_id: str,
        initial_cash: Decimal,
        risks: RiskDecisionRepository,
        executions: PostgresPaperExecutionRepository,
        controls: PostgresExecutionControlRepository,
        broker: PersistentSimulatedBroker,
        sessions: PostgresPaperSessionRiskRepository,
        projector: PaperAccountProjector | None = None,
        reconciler: AccountReconciler | None = None,
        risk_engine: PreTradeRiskEngine | None = None,
    ) -> None:
        if not account_id.strip():
            raise ValueError("account_id cannot be empty")
        if (
            not isinstance(initial_cash, Decimal)
            or not initial_cash.is_finite()
            or initial_cash <= 0
        ):
            raise ValueError("initial_cash must be a positive finite Decimal")
        self._account_id = account_id
        self._initial_cash = initial_cash
        self._risks = risks
        self._executions = executions
        self._controls = controls
        self._broker = broker
        self._sessions = sessions
        self._projector = projector or PaperAccountProjector()
        self._reconciler = reconciler or AccountReconciler()
        self._risk_engine = risk_engine or PreTradeRiskEngine()

    async def submit(self, request: PaperSubmissionRequest) -> PaperCoordinationResult:
        if not isinstance(request, PaperSubmissionRequest):
            raise TypeError("request must be PaperSubmissionRequest")
        try:
            async with self._controls.coordination_lock(account_id=self._account_id):
                return await self._submit(request)
        except PersistenceUnavailableError:
            try:
                await self._controls.activate(
                    account_id=self._account_id,
                    command_id=f"coord-dependency-{uuid4()}",
                    reason=KillSwitchReason.DEPENDENCY_UNAVAILABLE,
                    actor="paper-order-coordinator",
                    now=request.now,
                )
            except Exception:
                pass
            raise

    async def _submit(self, request: PaperSubmissionRequest) -> PaperCoordinationResult:
        if request.order.client_order_id.strip() == "":
            raise ValueError("client_order_id cannot be empty")
        await self._controls.ensure_fail_closed(
            account_id=self._account_id,
            now=request.now,
        )
        try:
            existing = await self._executions.load_order(
                account_id=self._account_id,
                client_order_id=request.order.client_order_id,
            )
        except LookupError:
            existing = None
        if existing is not None:
            return await self._resume(existing=existing, request=request)

        pre_report, evidence = await self._reconcile(
            marks=request.marks,
            now=request.now,
        )
        control = await self._guard_reconciliation(pre_report, now=request.now)
        session = await self._observe_session(evidence=evidence, now=request.now)
        account = self._risk_account(
            request=request,
            evidence=evidence,
            session=session,
            reconciled=pre_report.reconciled,
            control=control,
        )
        decision = self._risk_engine.evaluate(
            RiskEvaluationInput(
                mode=ExecutionMode.PAPER,
                policy=request.policy,
                account=account,
                order=request.order,
                quote=request.quote,
                rules=request.rules,
                now=request.now,
            )
        )
        await self._risks.append(decision)
        if decision.state is RiskDecisionState.REJECTED:
            return PaperCoordinationResult(
                status=PaperCoordinationStatus.RISK_REJECTED,
                control=control,
                decision=decision,
                projection=None,
                updates=(),
                pre_reconciliation=pre_report,
                post_reconciliation=None,
            )

        order = ApprovedPaperOrder.from_risk_decision(decision)
        projection = await self._executions.create_order(order)
        dispatch_control = await self._controls.get(account_id=self._account_id)
        if dispatch_control.active or dispatch_control.state_hash != control.state_hash:
            return await self._block_approved_intent(
                request=request,
                projection=projection,
                control=dispatch_control,
                decision=decision,
                pre_report=pre_report,
            )
        try:
            updates = await self._broker.submit(
                order=order,
                quote=request.quote,
                now=request.now,
                control_fence=dispatch_control,
            )
        except SimulatedBrokerControlError:
            return await self._block_approved_intent(
                request=request,
                projection=projection,
                control=await self._controls.get(account_id=self._account_id),
                decision=decision,
                pre_report=pre_report,
            )
        projection = await self._consume_updates(
            order=order,
            updates=updates,
            now=request.now,
        )
        post_report, post_evidence = await self._reconcile(marks=request.marks, now=request.now)
        await self._observe_session(evidence=post_evidence, now=request.now)
        control = await self._guard_reconciliation(post_report, now=request.now)
        control = await self._guard_unknown_state(
            projection=projection,
            control=control,
            now=request.now,
        )
        return PaperCoordinationResult(
            status=(
                PaperCoordinationStatus.BLOCKED
                if projection.state is PaperOrderState.UNKNOWN
                else PaperCoordinationStatus.RECONCILIATION_FAILED
                if not post_report.reconciled
                else PaperCoordinationStatus.FILLED
                if projection.state is PaperOrderState.FILLED
                else PaperCoordinationStatus.SUBMITTED
            ),
            control=control,
            decision=decision,
            projection=projection,
            updates=updates,
            pre_reconciliation=pre_report,
            post_reconciliation=post_report,
        )

    async def _resume(
        self,
        *,
        existing: PaperOrderProjection,
        request: PaperSubmissionRequest,
    ) -> PaperCoordinationResult:
        order = existing.order
        self._require_same_intent(order=order, request=request)
        control = await self._controls.get(account_id=self._account_id)
        if request.now < control.changed_at:
            raise ValueError("coordination time precedes current kill switch state")
        try:
            broker_history = await self._broker.order_history(order_hash=order.order_hash)
        except LookupError:
            broker_history = None

        if broker_history is None:
            stored_risk = await self._risks.get(
                account_id=self._account_id,
                client_order_id=order.client_order_id,
            )
            dispatch_safe = self._dispatch_still_safe(
                order=order,
                request=request,
                stored=stored_risk,
                control=control,
            )
            if not dispatch_safe:
                return await self._block_approved_intent(
                    request=request,
                    projection=existing,
                    control=control,
                    decision=None,
                    pre_report=None,
                )
            try:
                updates = await self._broker.submit(
                    order=order,
                    quote=request.quote,
                    now=request.now,
                    control_fence=control,
                )
            except SimulatedBrokerControlError:
                return await self._block_approved_intent(
                    request=request,
                    projection=existing,
                    control=await self._controls.get(account_id=self._account_id),
                    decision=None,
                    pre_report=None,
                )
        else:
            updates = broker_history.updates

        projection = await self._consume_updates(
            order=order,
            updates=updates,
            now=request.now,
        )
        post_report, post_evidence = await self._reconcile(marks=request.marks, now=request.now)
        await self._observe_session(evidence=post_evidence, now=request.now)
        control = await self._guard_reconciliation(post_report, now=request.now)
        control = await self._guard_unknown_state(
            projection=projection,
            control=control,
            now=request.now,
        )
        return PaperCoordinationResult(
            status=(
                PaperCoordinationStatus.BLOCKED
                if projection.state is PaperOrderState.UNKNOWN
                else PaperCoordinationStatus.RECOVERED
                if post_report.reconciled
                else PaperCoordinationStatus.RECONCILIATION_FAILED
            ),
            control=control,
            decision=None,
            projection=projection,
            updates=updates,
            pre_reconciliation=None,
            post_reconciliation=post_report,
        )

    async def _block_approved_intent(
        self,
        *,
        request: PaperSubmissionRequest,
        projection: PaperOrderProjection,
        control: KillSwitchControl,
        decision: RiskDecision | None,
        pre_report: ReconciliationReport | None,
    ) -> PaperCoordinationResult:
        report, evidence = await self._reconcile(
            marks=request.marks,
            now=request.now,
        )
        await self._observe_session(evidence=evidence, now=request.now)
        if not control.active:
            control = await self._controls.activate(
                account_id=self._account_id,
                command_id=f"coord-unknown-{uuid4()}",
                reason=KillSwitchReason.ORDER_STATE_UNKNOWN,
                actor="paper-order-coordinator",
                now=max(request.now, control.changed_at),
                evidence_hash=report.report_hash,
            )
        return PaperCoordinationResult(
            status=PaperCoordinationStatus.BLOCKED,
            control=control,
            decision=decision,
            projection=projection,
            updates=(),
            pre_reconciliation=pre_report,
            post_reconciliation=report,
        )

    async def _consume_updates(
        self,
        *,
        order: ApprovedPaperOrder,
        updates: tuple[BrokerOrderUpdate, ...],
        now: datetime,
    ) -> PaperOrderProjection:
        try:
            for update in updates:
                await self._executions.apply_update(
                    account_id=order.account_id,
                    client_order_id=order.client_order_id,
                    update=update,
                )
        except ValueError:
            current = await self._controls.get(account_id=self._account_id)
            occurred_at = max(
                now,
                current.changed_at,
                max(
                    (item.occurred_at for item in updates),
                    default=order.approved_at,
                ),
            )
            await self._controls.activate(
                account_id=self._account_id,
                command_id=f"coord-update-conflict-{uuid4()}",
                reason=KillSwitchReason.ORDER_STATE_UNKNOWN,
                actor="paper-order-coordinator",
                now=occurred_at,
            )
            raise
        return await self._executions.replay_order(
            account_id=order.account_id,
            client_order_id=order.client_order_id,
        )

    async def _reconcile(
        self, *, marks: dict[str, Decimal], now: datetime
    ) -> tuple[ReconciliationReport, _AccountEvidence]:
        internal_histories = await self._executions.account_histories(account_id=self._account_id)
        broker_histories = await self._broker.account_histories(account_id=self._account_id)
        internal = self._projector.project(
            account_id=self._account_id,
            initial_cash=self._initial_cash,
            histories=internal_histories,
            marks=marks,
            as_of=now,
        )
        broker = self._projector.project(
            account_id=self._account_id,
            initial_cash=self._initial_cash,
            histories=broker_histories,
            marks=marks,
            as_of=now,
        )
        report = self._reconciler.reconcile(
            internal=internal,
            broker=broker,
            now=now,
        )
        await self._executions.save_reconciliation(
            internal=internal,
            broker=broker,
            report=report,
        )
        return report, _AccountEvidence(
            snapshot=internal,
            histories=internal_histories,
            seen_client_order_ids=tuple(
                history.order.client_order_id for history in internal_histories
            ),
        )

    async def _guard_reconciliation(
        self, report: ReconciliationReport, *, now: datetime
    ) -> KillSwitchControl:
        current = await self._controls.get(account_id=self._account_id)
        if now < current.changed_at:
            raise ValueError("coordination time precedes current kill switch state")
        if report.reconciled:
            return current
        return await self._controls.activate(
            account_id=self._account_id,
            command_id=f"coord-mismatch:{report.report_hash}",
            reason=KillSwitchReason.RECONCILIATION_FAILED,
            actor="paper-order-coordinator",
            now=now,
            evidence_hash=report.report_hash,
        )

    async def _guard_unknown_state(
        self,
        *,
        projection: PaperOrderProjection,
        control: KillSwitchControl,
        now: datetime,
    ) -> KillSwitchControl:
        if projection.state is not PaperOrderState.UNKNOWN:
            return control
        return await self._controls.activate(
            account_id=self._account_id,
            command_id=f"coord-order-unknown-{uuid4()}",
            reason=KillSwitchReason.ORDER_STATE_UNKNOWN,
            actor="paper-order-coordinator",
            now=max(now, control.changed_at),
            evidence_hash=projection.projection_hash,
        )

    def _risk_account(
        self,
        *,
        request: PaperSubmissionRequest,
        evidence: _AccountEvidence,
        session: PaperSessionRiskState,
        reconciled: bool,
        control: KillSwitchControl,
    ) -> RiskAccountState:
        snapshot = evidence.snapshot
        positions = tuple(
            RiskPosition(
                instrument=item.instrument,
                total_quantity=item.total_quantity,
                sellable_quantity=item.sellable_quantity,
                market_value=item.market_value,
            )
            for item in snapshot.positions
        )
        gross_exposure = sum((position.market_value for position in positions), Decimal("0"))
        return RiskAccountState(
            account_id=self._account_id,
            as_of=snapshot.as_of,
            cash=snapshot.cash,
            equity=snapshot.equity,
            day_start_equity=session.day_start_equity,
            peak_equity=session.peak_equity,
            gross_exposure=gross_exposure,
            daily_turnover=session.cumulative_turnover,
            open_order_count=len(snapshot.open_client_order_ids),
            reconciled=reconciled,
            kill_switch=control.active,
            positions=positions,
            seen_client_order_ids=evidence.seen_client_order_ids,
        )

    async def _observe_session(
        self, *, evidence: _AccountEvidence, now: datetime
    ) -> PaperSessionRiskState:
        session_date = now.astimezone(_SHANGHAI).date()
        turnover = derive_session_turnover(
            account_id=self._account_id,
            session_date=session_date,
            histories=evidence.histories,
        )
        observation = SessionRiskObservation(
            account_id=self._account_id,
            session_date=session_date,
            as_of=evidence.snapshot.as_of,
            equity=evidence.snapshot.equity,
            cumulative_turnover=turnover.cumulative_turnover,
            snapshot_hash=evidence.snapshot.snapshot_hash,
            turnover_evidence_hash=turnover.evidence_hash,
        )
        try:
            return await self._sessions.observe(observation)
        except LookupError:
            raise PersistenceUnavailableError(
                "Paper session risk state is not initialized"
            ) from None

    def _dispatch_still_safe(
        self,
        *,
        order: ApprovedPaperOrder,
        request: PaperSubmissionRequest,
        stored: StoredRiskDecision,
        control: KillSwitchControl,
    ) -> bool:
        return (
            not control.active
            and request.now >= control.changed_at
            and stored.state == RiskDecisionState.ACCEPTED.value
            and stored.decision_hash == order.risk_decision_hash
            and stored.policy_hash == request.policy.policy_hash
            and stored.quote_hash == request.quote.quote_hash
            and request.quote.market_open
            and request.now >= request.quote.as_of
            and request.now - request.quote.as_of <= request.policy.max_quote_age
            and request.now >= stored.evaluated_at
            and request.now - stored.evaluated_at <= request.policy.max_quote_age
            and existing_is_approved(order, request)
        )

    def _require_same_intent(
        self, *, order: ApprovedPaperOrder, request: PaperSubmissionRequest
    ) -> None:
        if order.account_id != self._account_id or not existing_is_approved(order, request):
            raise ValueError("client_order_id belongs to another paper order intent")


def existing_is_approved(order: ApprovedPaperOrder, request: PaperSubmissionRequest) -> bool:
    proposed = request.order
    return (
        order.client_order_id == proposed.client_order_id
        and order.instrument == proposed.instrument
        and order.side is proposed.side
        and order.quantity == proposed.quantity
        and order.limit_price == proposed.limit_price
    )
