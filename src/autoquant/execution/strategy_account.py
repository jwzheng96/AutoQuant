from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from uuid import uuid4

from autoquant.clock import to_shanghai, to_utc
from autoquant.data.models import (
    _canonical_hash,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.account_projection import PaperAccountProjector
from autoquant.execution.control import KillSwitchReason
from autoquant.execution.control_store import PostgresExecutionControlRepository
from autoquant.execution.models import PaperOrderState
from autoquant.execution.reconciliation import (
    AccountReconciler,
    ReconciliationReport,
)
from autoquant.execution.session_risk import (
    SessionRiskObservation,
    derive_session_turnover,
)
from autoquant.execution.session_risk_store import (
    PostgresPaperSessionRiskRepository,
)
from autoquant.execution.simulated_broker import PersistentSimulatedBroker
from autoquant.execution.store import PostgresPaperExecutionRepository
from autoquant.risk.models import RiskAccountState, RiskPosition

_TERMINAL = {
    PaperOrderState.FILLED,
    PaperOrderState.CANCELLED,
    PaperOrderState.REJECTED,
}


@dataclass(frozen=True, slots=True)
class PaperStrategyAccountEvidence:
    session_date: date
    account: RiskAccountState
    reconciliation_hash: str
    internal_snapshot_hash: str
    broker_snapshot_hash: str
    session_state_hash: str
    evidence_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if to_shanghai(self.account.as_of).date() != self.session_date:
            raise ValueError("strategy account evidence is from after its session")
        if not self.account.reconciled or self.account.kill_switch:
            raise ValueError("strategy account evidence must be reconciled and unlocked")
        for name, value in (
            ("reconciliation_hash", self.reconciliation_hash),
            ("internal_snapshot_hash", self.internal_snapshot_hash),
            ("broker_snapshot_hash", self.broker_snapshot_hash),
            ("session_state_hash", self.session_state_hash),
        ):
            _require_lowercase_sha256(value, name=name)
        object.__setattr__(
            self,
            "evidence_hash",
            _canonical_hash(
                {
                    "account_state_hash": self.account.state_hash,
                    "broker_snapshot_hash": self.broker_snapshot_hash,
                    "internal_snapshot_hash": self.internal_snapshot_hash,
                    "reconciliation_hash": self.reconciliation_hash,
                    "session_date": self.session_date.isoformat(),
                    "session_state_hash": self.session_state_hash,
                }
            ),
        )


class PaperStrategyAccountReader:
    """Build a reconciled strategy view from independent durable fact streams."""

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
        _require_nonblank(account_id, name="strategy account_id")
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

    async def __call__(
        self,
        account_id: str,
        session_date: date,
        marks: dict[str, Decimal],
        now: datetime,
    ) -> PaperStrategyAccountEvidence:
        if account_id != self._account_id:
            raise ValueError("strategy account reader received another account")
        instant = to_utc(now, name="strategy account read time")
        try:
            async with self._controls.coordination_lock(account_id=self._account_id):
                return await self._read(
                    session_date=session_date,
                    marks=marks,
                    now=instant,
                )
        except PersistenceUnavailableError:
            raise
        except (TypeError, ValueError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "strategy account evidence read failed"
            ) from None

    async def _read(
        self,
        *,
        session_date: date,
        marks: dict[str, Decimal],
        now: datetime,
    ) -> PaperStrategyAccountEvidence:
        control = await self._controls.get(account_id=self._account_id)
        if control.active:
            raise PersistenceUnavailableError(
                "strategy account evidence cannot be read while kill switch is active"
            )
        if now < control.changed_at:
            raise ValueError("strategy account read precedes the current control state")

        internal_histories = await self._executions.account_histories(
            account_id=self._account_id
        )
        broker_histories = await self._broker.account_histories(
            account_id=self._account_id
        )
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
        if not report.reconciled:
            await self._activate_mismatch(report=report, now=now)
            raise PersistenceUnavailableError(
                "strategy account snapshots do not reconcile"
            )

        turnover = derive_session_turnover(
            account_id=self._account_id,
            session_date=session_date,
            histories=internal_histories,
        )
        session = await self._sessions.observe(
            SessionRiskObservation(
                account_id=self._account_id,
                session_date=session_date,
                as_of=internal.as_of,
                equity=internal.equity,
                cumulative_turnover=turnover.cumulative_turnover,
                snapshot_hash=internal.snapshot_hash,
                turnover_evidence_hash=turnover.evidence_hash,
            )
        )
        latest_control = await self._controls.get(account_id=self._account_id)
        if latest_control.active or latest_control.state_hash != control.state_hash:
            raise PersistenceUnavailableError(
                "strategy account control fence changed during evidence read"
            )
        positions = tuple(
            RiskPosition(
                instrument=value.instrument,
                total_quantity=value.total_quantity,
                sellable_quantity=value.sellable_quantity,
                market_value=value.market_value,
            )
            for value in internal.positions
        )
        gross_exposure = sum(
            (value.market_value for value in positions),
            Decimal("0"),
        )
        return PaperStrategyAccountEvidence(
            session_date=session_date,
            account=RiskAccountState(
                account_id=self._account_id,
                as_of=internal.as_of,
                cash=internal.cash,
                equity=internal.equity,
                day_start_equity=session.day_start_equity,
                peak_equity=session.peak_equity,
                gross_exposure=gross_exposure,
                daily_turnover=session.cumulative_turnover,
                open_order_count=sum(
                    history.state not in _TERMINAL
                    for history in internal_histories
                ),
                reconciled=True,
                kill_switch=False,
                positions=positions,
                seen_client_order_ids=tuple(
                    history.order.client_order_id
                    for history in internal_histories
                ),
            ),
            reconciliation_hash=report.report_hash,
            internal_snapshot_hash=internal.snapshot_hash,
            broker_snapshot_hash=broker.snapshot_hash,
            session_state_hash=session.state_hash,
        )

    async def _activate_mismatch(
        self,
        *,
        report: ReconciliationReport,
        now: datetime,
    ) -> None:
        await self._controls.activate(
            account_id=self._account_id,
            command_id=f"strategy-account-mismatch-{uuid4()}",
            reason=KillSwitchReason.RECONCILIATION_FAILED,
            actor="paper-strategy-account-reader",
            now=now,
            evidence_hash=report.report_hash,
        )
