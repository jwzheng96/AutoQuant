from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from uuid import uuid4
from zoneinfo import ZoneInfo

from autoquant.clock import to_utc
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.account_projection import PaperAccountProjector
from autoquant.execution.control import KillSwitchControl, KillSwitchReason
from autoquant.execution.control_store import PostgresExecutionControlRepository
from autoquant.execution.reconciliation import AccountReconciler, ReconciliationReport
from autoquant.execution.session_risk import (
    PaperSessionRiskState,
    SessionRiskObservation,
    derive_session_turnover,
)
from autoquant.execution.session_risk_store import (
    PostgresPaperSessionRiskRepository,
)
from autoquant.execution.simulated_broker import PersistentSimulatedBroker
from autoquant.execution.store import PostgresPaperExecutionRepository

_SHANGHAI = ZoneInfo("Asia/Shanghai")


class MarketPhase(StrEnum):
    PRE_OPEN = "pre_open"
    OPEN = "open"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class PaperSessionInitializationRequest:
    session_date: date
    as_of: datetime
    market_phase: MarketPhase
    marks: dict[str, Decimal]

    def __post_init__(self) -> None:
        as_of = to_utc(self.as_of, name="session initialization as_of")
        object.__setattr__(self, "as_of", as_of)
        if as_of.astimezone(_SHANGHAI).date() != self.session_date:
            raise ValueError("session initialization time must belong to session_date")
        if not isinstance(self.market_phase, MarketPhase):
            raise TypeError("market_phase must be MarketPhase")
        object.__setattr__(self, "marks", dict(self.marks))


@dataclass(frozen=True, slots=True)
class PaperSessionInitializationResult:
    state: PaperSessionRiskState
    reconciliation: ReconciliationReport
    control: KillSwitchControl


class PaperSessionInitializer:
    """Freeze one daily opening state only from reconciled pre-open evidence."""

    def __init__(
        self,
        *,
        account_id: str,
        initial_cash: Decimal,
        executions: PostgresPaperExecutionRepository,
        controls: PostgresExecutionControlRepository,
        broker: PersistentSimulatedBroker,
        sessions: PostgresPaperSessionRiskRepository,
        projector: PaperAccountProjector | None = None,
        reconciler: AccountReconciler | None = None,
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
        self._executions = executions
        self._controls = controls
        self._broker = broker
        self._sessions = sessions
        self._projector = projector or PaperAccountProjector()
        self._reconciler = reconciler or AccountReconciler()

    async def initialize(
        self, request: PaperSessionInitializationRequest
    ) -> PaperSessionInitializationResult:
        if not isinstance(request, PaperSessionInitializationRequest):
            raise TypeError("request must be PaperSessionInitializationRequest")
        if request.market_phase is not MarketPhase.PRE_OPEN:
            raise ValueError("paper session can only be initialized pre-open")
        try:
            async with self._controls.coordination_lock(account_id=self._account_id):
                return await self._initialize(request)
        except PersistenceUnavailableError:
            try:
                await self._controls.activate(
                    account_id=self._account_id,
                    command_id=f"session-init-dependency-{uuid4()}",
                    reason=KillSwitchReason.DEPENDENCY_UNAVAILABLE,
                    actor="paper-session-initializer",
                    now=request.as_of,
                )
            except Exception:
                pass
            raise

    async def _initialize(
        self, request: PaperSessionInitializationRequest
    ) -> PaperSessionInitializationResult:
        control = await self._controls.ensure_fail_closed(
            account_id=self._account_id,
            now=request.as_of,
        )
        internal_histories = await self._executions.account_histories(account_id=self._account_id)
        broker_histories = await self._broker.account_histories(account_id=self._account_id)
        internal = self._projector.project(
            account_id=self._account_id,
            initial_cash=self._initial_cash,
            histories=internal_histories,
            marks=request.marks,
            as_of=request.as_of,
        )
        broker = self._projector.project(
            account_id=self._account_id,
            initial_cash=self._initial_cash,
            histories=broker_histories,
            marks=request.marks,
            as_of=request.as_of,
        )
        report = self._reconciler.reconcile(
            internal=internal,
            broker=broker,
            now=request.as_of,
        )
        await self._executions.save_reconciliation(
            internal=internal,
            broker=broker,
            report=report,
        )
        if not report.reconciled:
            await self._controls.activate(
                account_id=self._account_id,
                command_id=f"session-init-mismatch:{report.report_hash}",
                reason=KillSwitchReason.RECONCILIATION_FAILED,
                actor="paper-session-initializer",
                now=request.as_of,
                evidence_hash=report.report_hash,
            )
            raise PersistenceUnavailableError("Paper session opening snapshots do not reconcile")
        turnover = derive_session_turnover(
            account_id=self._account_id,
            session_date=request.session_date,
            histories=internal_histories,
        )
        if turnover.cumulative_turnover != 0:
            await self._controls.activate(
                account_id=self._account_id,
                command_id=f"session-init-late-{uuid4()}",
                reason=KillSwitchReason.ORDER_STATE_UNKNOWN,
                actor="paper-session-initializer",
                now=request.as_of,
                evidence_hash=turnover.evidence_hash,
            )
            raise PersistenceUnavailableError(
                "Paper session cannot initialize after its first fill"
            )
        observation = SessionRiskObservation(
            account_id=self._account_id,
            session_date=request.session_date,
            as_of=internal.as_of,
            equity=internal.equity,
            cumulative_turnover=turnover.cumulative_turnover,
            snapshot_hash=internal.snapshot_hash,
            turnover_evidence_hash=turnover.evidence_hash,
        )
        try:
            state = await self._sessions.initialize(observation)
        except ValueError:
            await self._controls.activate(
                account_id=self._account_id,
                command_id=f"session-init-conflict-{uuid4()}",
                reason=KillSwitchReason.ORDER_STATE_UNKNOWN,
                actor="paper-session-initializer",
                now=request.as_of,
                evidence_hash=observation.observation_hash,
            )
            raise PersistenceUnavailableError(
                "Paper session opening state conflicts with existing evidence"
            ) from None
        return PaperSessionInitializationResult(
            state=state,
            reconciliation=report,
            control=control,
        )
