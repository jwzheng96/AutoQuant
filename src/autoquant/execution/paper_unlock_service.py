from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol
from uuid import uuid4

from pydantic import SecretStr

from autoquant.clock import to_shanghai, to_utc
from autoquant.data.daily_models import TradingSession
from autoquant.errors import MissingCapabilityError, PersistenceUnavailableError
from autoquant.execution.account_projection import PaperAccountProjector
from autoquant.execution.control import KillSwitchControl
from autoquant.execution.control_store import PostgresExecutionControlRepository
from autoquant.execution.market_clock import AShareMarketClock
from autoquant.execution.paper_runtime import RuntimeCalendarReader
from autoquant.execution.paper_scheduler_lease_store import (
    PostgresPaperSchedulerLeaseRepository,
    scheduler_lease_token_hash,
)
from autoquant.execution.paper_unlock import (
    PaperRuntimeUnlockEvidence,
    PostgresPaperRuntimeUnlockRepository,
)
from autoquant.execution.quote_book import QuoteBookSnapshot
from autoquant.execution.reconciliation import AccountReconciler
from autoquant.execution.session_risk import (
    SessionRiskObservation,
    derive_session_turnover,
)
from autoquant.execution.session_risk_store import (
    PostgresPaperSessionRiskRepository,
)
from autoquant.execution.simulated_broker import PersistentSimulatedBroker
from autoquant.execution.store import PostgresPaperExecutionRepository
from autoquant.execution.strategy_registry_store import (
    PostgresPaperStrategyRegistry,
)


class PaperUnlockQuoteReader(Protocol):
    async def __call__(
        self,
        *,
        instruments: tuple[str, ...],
        session: TradingSession,
        now: datetime,
    ) -> QuoteBookSnapshot: ...


@dataclass(frozen=True, slots=True)
class PaperRuntimeUnlockResult:
    control: KillSwitchControl
    evidence: PaperRuntimeUnlockEvidence

    def __post_init__(self) -> None:
        if self.control.active:
            raise ValueError("paper runtime unlock result must be inactive")
        if self.control.account_id != self.evidence.account_id:
            raise ValueError("paper runtime unlock result identity mismatch")


class PaperRuntimeUnlockService:
    """Reset only from fresh QMT quotes and atomically rechecked paper evidence."""

    def __init__(
        self,
        *,
        account_id: str,
        strategy_id: str,
        initial_cash: Decimal,
        holder_id: str,
        lease_token: SecretStr,
        controls: PostgresExecutionControlRepository,
        executions: PostgresPaperExecutionRepository,
        broker: PersistentSimulatedBroker,
        sessions: PostgresPaperSessionRiskRepository,
        strategies: PostgresPaperStrategyRegistry,
        leases: PostgresPaperSchedulerLeaseRepository,
        unlocks: PostgresPaperRuntimeUnlockRepository,
        calendar: RuntimeCalendarReader,
        quotes: PaperUnlockQuoteReader,
        clock: AShareMarketClock | None = None,
        now: Callable[[], datetime] | None = None,
        max_quote_age: timedelta = timedelta(seconds=2),
        max_unlock_age: timedelta = timedelta(seconds=3),
        projector: PaperAccountProjector | None = None,
        reconciler: AccountReconciler | None = None,
    ) -> None:
        if not account_id.strip() or not strategy_id.strip():
            raise ValueError("paper unlock identifiers cannot be empty")
        if (
            not isinstance(initial_cash, Decimal)
            or not initial_cash.is_finite()
            or initial_cash <= 0
        ):
            raise ValueError("initial_cash must be positive and finite")
        if not holder_id.strip():
            raise ValueError("paper unlock holder_id cannot be empty")
        if max_quote_age <= timedelta(0) or max_unlock_age <= timedelta(0):
            raise ValueError("paper unlock evidence ages must be positive")
        self._account_id = account_id
        self._strategy_id = strategy_id
        self._initial_cash = initial_cash
        self._holder_id = holder_id
        self._lease_token = lease_token
        self._controls = controls
        self._executions = executions
        self._broker = broker
        self._sessions = sessions
        self._strategies = strategies
        self._leases = leases
        self._unlocks = unlocks
        self._calendar = calendar
        self._quotes = quotes
        self._clock = clock or AShareMarketClock()
        self._now = now or (lambda: datetime.now(UTC))
        self._max_quote_age = max_quote_age
        self._max_unlock_age = max_unlock_age
        self._projector = projector or PaperAccountProjector()
        self._reconciler = reconciler or AccountReconciler()

    async def unlock(self, *, actor: str) -> PaperRuntimeUnlockResult:
        if not actor.strip() or len(actor) > 128:
            raise ValueError("paper unlock actor must contain 1-128 characters")
        started_at = to_utc(self._now(), name="paper unlock start time")
        async with self._controls.coordination_lock(
            account_id=self._account_id
        ):
            return await self._unlock(actor=actor, started_at=started_at)

    async def _unlock(
        self,
        *,
        actor: str,
        started_at: datetime,
    ) -> PaperRuntimeUnlockResult:
        control = await self._controls.replay(account_id=self._account_id)
        if not control.active:
            raise MissingCapabilityError("paper kill switch is already inactive")
        registration = await self._strategies.active(
            account_id=self._account_id,
            strategy_id=self._strategy_id,
        )
        if registration is None:
            raise MissingCapabilityError(
                "paper unlock requires an active approved strategy"
            )
        if (
            registration.account_id != self._account_id
            or registration.strategy_id != self._strategy_id
            or registration.execution_mode != "paper"
        ):
            raise PersistenceUnavailableError(
                "paper unlock strategy registration is inconsistent"
            )
        session_date = to_shanghai(started_at).date()
        calendar = await self._calendar(session_date, started_at)
        if not self._clock.phase(
            now=started_at,
            session=calendar,
        ).accepts_strategy_orders:
            raise MissingCapabilityError(
                "paper unlock is allowed only during continuous trading"
            )
        lease = await self._leases.verify_owner(
            account_id=self._account_id,
            strategy_id=self._strategy_id,
            holder_id=self._holder_id,
            token=self._lease_token,
            now=started_at,
        )
        session = await self._sessions.replay(
            account_id=self._account_id,
            session_date=session_date,
        )
        quote_snapshot = await self._quotes(
            instruments=(registration.instrument,),
            session=calendar,
            now=started_at,
        )
        evaluated_at = to_utc(
            self._now(),
            name="paper unlock evaluation time",
        )
        if (
            quote_snapshot.source != "qmt"
            or quote_snapshot.as_of > evaluated_at
            or evaluated_at - quote_snapshot.as_of > self._max_quote_age
            or set(quote_snapshot.quotes) != {registration.instrument}
            or any(not quote.market_open for quote in quote_snapshot.quotes.values())
        ):
            raise PersistenceUnavailableError(
                "paper unlock quote evidence is stale, incomplete, or closed"
            )
        internal_histories = await self._executions.account_histories(
            account_id=self._account_id
        )
        broker_histories = await self._broker.account_histories(
            account_id=self._account_id
        )
        if internal_histories != broker_histories:
            raise PersistenceUnavailableError(
                "paper unlock execution histories do not converge"
            )
        internal = self._projector.project(
            account_id=self._account_id,
            initial_cash=self._initial_cash,
            histories=internal_histories,
            marks=quote_snapshot.marks,
            as_of=evaluated_at,
        )
        broker = self._projector.project(
            account_id=self._account_id,
            initial_cash=self._initial_cash,
            histories=broker_histories,
            marks=quote_snapshot.marks,
            as_of=evaluated_at,
        )
        report = self._reconciler.reconcile(
            internal=internal,
            broker=broker,
            now=evaluated_at,
        )
        await self._executions.save_reconciliation(
            internal=internal,
            broker=broker,
            report=report,
        )
        if not report.reconciled:
            raise PersistenceUnavailableError(
                "paper unlock account reconciliation failed"
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
                as_of=evaluated_at,
                equity=internal.equity,
                cumulative_turnover=turnover.cumulative_turnover,
                snapshot_hash=internal.snapshot_hash,
                turnover_evidence_hash=turnover.evidence_hash,
            )
        )
        latest_control = await self._controls.get(account_id=self._account_id)
        if (
            not latest_control.active
            or latest_control.state_hash != control.state_hash
        ):
            raise PersistenceUnavailableError(
                "paper kill switch changed during unlock evidence collection"
            )
        lease = await self._leases.verify_owner(
            account_id=self._account_id,
            strategy_id=self._strategy_id,
            holder_id=self._holder_id,
            token=self._lease_token,
            now=evaluated_at,
        )
        evidence = PaperRuntimeUnlockEvidence(
            account_id=self._account_id,
            strategy_id=self._strategy_id,
            session_date=session_date,
            evaluated_at=evaluated_at,
            registration_hash=registration.registration_hash,
            calendar_hash=calendar.content_hash,
            session_state_hash=session.state_hash,
            quote_evidence_hash=quote_snapshot.evidence_hash,
            reconciliation_report_hash=report.report_hash,
            lease_holder_id=lease.holder_id,
            lease_token_hash=scheduler_lease_token_hash(
                self._lease_token
            ),
            lease_generation=lease.generation,
        )
        await self._unlocks.append(evidence)
        reset_at = to_utc(self._now(), name="paper unlock reset time")
        control = await self._controls.reset_paper_runtime(
            evidence=evidence,
            lease_token=self._lease_token,
            command_id=f"paper-runtime-unlock-{uuid4()}",
            actor=actor,
            now=reset_at,
            expected_version=latest_control.version,
            max_evidence_age=self._max_unlock_age,
        )
        return PaperRuntimeUnlockResult(
            control=control,
            evidence=evidence,
        )
