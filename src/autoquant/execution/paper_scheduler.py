from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Protocol
from uuid import uuid4
from zoneinfo import ZoneInfo

from pydantic import SecretStr

from autoquant.backtest.models import InstrumentRules
from autoquant.clock import to_utc
from autoquant.data.daily_models import TradingSession
from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.errors import (
    MarketCalendarUnavailableError,
    PaperSchedulerLeaseLostError,
    PersistenceUnavailableError,
    QuoteStreamUnavailableError,
)
from autoquant.execution.control import KillSwitchControl, KillSwitchReason
from autoquant.execution.control_store import PostgresExecutionControlRepository
from autoquant.execution.coordinator import (
    PaperCoordinationResult,
    PaperOrderCoordinator,
    PaperSubmissionRequest,
)
from autoquant.execution.market_clock import AShareMarketClock, AShareTradingPhase
from autoquant.execution.paper_scheduler_lease_store import (
    PostgresPaperSchedulerLeaseRepository,
)
from autoquant.execution.quote_book import ContinuousQuoteBook, QuoteBookSnapshot
from autoquant.execution.session_initializer import (
    MarketPhase,
    PaperSessionInitializationRequest,
    PaperSessionInitializationResult,
    PaperSessionInitializer,
)
from autoquant.execution.session_risk_store import PostgresPaperSessionRiskRepository
from autoquant.risk.models import MarketQuote, ProposedOrder, RiskPolicy

SHANGHAI = ZoneInfo("Asia/Shanghai")
CalendarReader = Callable[[date, datetime], Awaitable[TradingSession]]
PreOpenMarkReader = Callable[[date, tuple[str, ...], datetime], Awaitable["PreOpenMarks"]]
CycleSink = Callable[["PaperSchedulerCycle"], Awaitable[None]]


class PaperIntentSource(Protocol):
    async def generate(self, context: PaperStrategyContext) -> tuple[PaperStrategyIntent, ...]: ...


@dataclass(frozen=True, slots=True)
class PreOpenMarks:
    session_date: date
    as_of: datetime
    marks: dict[str, Decimal]
    source_evidence_hash: str
    marks_hash: str = field(init=False)

    def __post_init__(self) -> None:
        as_of = to_utc(self.as_of, name="pre-open marks as_of")
        if as_of.astimezone(SHANGHAI).date() > self.session_date:
            raise ValueError("pre-open marks cannot come from after the session date")
        marks = dict(self.marks)
        if not marks or any(not instrument.strip() for instrument in marks):
            raise ValueError("pre-open marks must contain nonblank instruments")
        for value in marks.values():
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise ValueError("pre-open marks must be positive finite Decimals")
        _require_lowercase_sha256(self.source_evidence_hash, name="source_evidence_hash")
        object.__setattr__(self, "as_of", as_of)
        object.__setattr__(self, "marks", marks)
        object.__setattr__(
            self,
            "marks_hash",
            _canonical_hash(
                {
                    "as_of": as_of.isoformat(timespec="microseconds"),
                    "marks": {
                        instrument: _decimal_text(value)
                        for instrument, value in sorted(marks.items())
                    },
                    "session_date": self.session_date.isoformat(),
                    "source_evidence_hash": self.source_evidence_hash,
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class PaperStrategyIntent:
    order: ProposedOrder
    rules: InstrumentRules
    policy: RiskPolicy

    def __post_init__(self) -> None:
        if (
            self.order.instrument != self.rules.instrument
            or self.order.instrument not in self.policy.allowed_instruments
        ):
            raise ValueError("strategy intent instrument is not covered by rules and policy")


@dataclass(frozen=True, slots=True)
class PaperStrategyContext:
    account_id: str
    session_date: date
    now: datetime
    phase: AShareTradingPhase
    quote_snapshot: QuoteBookSnapshot

    def __post_init__(self) -> None:
        _require_nonblank(self.account_id, name="account_id")
        now = to_utc(self.now, name="strategy context time")
        if now.astimezone(SHANGHAI).date() != self.session_date:
            raise ValueError("strategy context date does not match Shanghai date")
        if not self.phase.accepts_strategy_orders:
            raise ValueError("strategy context requires a continuous-auction phase")
        if self.quote_snapshot.as_of != now:
            raise ValueError("strategy context and quote snapshot times must match")
        object.__setattr__(self, "now", now)

    @property
    def quotes(self) -> dict[str, MarketQuote]:
        return self.quote_snapshot.quotes


class PaperSchedulerStatus(StrEnum):
    IDLE = "idle"
    LOCKED = "locked"
    SESSION_INITIALIZED = "session_initialized"
    SESSION_READY = "session_ready"
    NO_INTENTS = "no_intents"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class PaperSchedulerCycle:
    account_id: str
    strategy_id: str
    session_date: date
    evaluated_at: datetime
    phase: AShareTradingPhase
    status: PaperSchedulerStatus
    control: KillSwitchControl
    clock_rule_version: str
    calendar_hash: str | None = None
    mark_evidence_hash: str | None = None
    quote_evidence_hash: str | None = None
    initialization: PaperSessionInitializationResult | None = None
    coordination_results: tuple[PaperCoordinationResult, ...] = ()
    error_code: str | None = None
    cycle_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(self.account_id, name="scheduler account_id")
        _require_nonblank(self.strategy_id, name="scheduler strategy_id")
        _require_nonblank(self.clock_rule_version, name="clock_rule_version")
        evaluated_at = to_utc(self.evaluated_at, name="scheduler evaluated_at")
        results = tuple(self.coordination_results)
        object.__setattr__(self, "evaluated_at", evaluated_at)
        object.__setattr__(self, "coordination_results", results)
        if evaluated_at.astimezone(SHANGHAI).date() != self.session_date:
            raise ValueError("scheduler cycle date does not match Shanghai date")
        if self.control.account_id != self.account_id:
            raise ValueError("scheduler control belongs to another account")
        for name, value in (
            ("calendar_hash", self.calendar_hash),
            ("mark_evidence_hash", self.mark_evidence_hash),
            ("quote_evidence_hash", self.quote_evidence_hash),
        ):
            if value is not None:
                _require_lowercase_sha256(value, name=name)
        if self.status is PaperSchedulerStatus.FAILED:
            _require_nonblank(self.error_code or "", name="scheduler error_code")
            if not self.control.active:
                raise ValueError("failed scheduler cycle must be fail-closed")
        elif self.error_code is not None:
            raise ValueError("only failed scheduler cycles can have an error_code")
        if self.status is not PaperSchedulerStatus.FAILED and self.calendar_hash is None:
            raise ValueError("successful scheduler phase requires calendar evidence")
        if (self.status is PaperSchedulerStatus.SESSION_INITIALIZED) != (
            self.initialization is not None
        ):
            raise ValueError("session initialization status and evidence must match")
        if self.mark_evidence_hash is not None and (
            self.status is not PaperSchedulerStatus.SESSION_INITIALIZED
        ):
            raise ValueError("mark evidence is only valid for session initialization")
        if self.status is PaperSchedulerStatus.COMPLETED:
            if not results or self.quote_evidence_hash is None:
                raise ValueError("completed scheduler cycle requires quotes and results")
        elif results:
            raise ValueError("only completed scheduler cycles can contain order results")
        if self.status is PaperSchedulerStatus.NO_INTENTS:
            if self.quote_evidence_hash is None:
                raise ValueError("no-intent scheduler cycle requires quote evidence")
        elif (
            self.quote_evidence_hash is not None
            and self.status is not PaperSchedulerStatus.COMPLETED
        ):
            raise ValueError("quote evidence is only valid for evaluated strategy cycles")
        if self.status is PaperSchedulerStatus.LOCKED and not self.control.active:
            raise ValueError("locked scheduler cycle requires an active control")
        object.__setattr__(
            self,
            "cycle_hash",
            _canonical_hash(scheduler_cycle_payload(self)),
        )


class PaperTradingScheduler:
    """Fail-closed paper cycle runner; never resets controls or enables live trading."""

    def __init__(
        self,
        *,
        account_id: str,
        strategy_id: str,
        instruments: tuple[str, ...],
        calendar_reader: CalendarReader,
        pre_open_mark_reader: PreOpenMarkReader,
        quotes: ContinuousQuoteBook,
        initializer: PaperSessionInitializer,
        coordinator: PaperOrderCoordinator,
        controls: PostgresExecutionControlRepository,
        sessions: PostgresPaperSessionRiskRepository,
        intent_source: PaperIntentSource,
        max_quote_age: timedelta = timedelta(seconds=3),
        max_pre_open_mark_age: timedelta = timedelta(days=4),
        max_orders_per_cycle: int = 20,
        clock: AShareMarketClock | None = None,
    ) -> None:
        _require_nonblank(account_id, name="account_id")
        _require_nonblank(strategy_id, name="strategy_id")
        normalized = tuple(sorted(instruments))
        if not normalized or len(set(normalized)) != len(normalized):
            raise ValueError("scheduler instruments must be nonempty and unique")
        if any(not instrument.strip() for instrument in normalized):
            raise ValueError("scheduler instruments cannot be blank")
        if max_quote_age <= timedelta(0) or max_pre_open_mark_age <= timedelta(0):
            raise ValueError("scheduler evidence ages must be positive")
        if (
            not isinstance(max_orders_per_cycle, int)
            or isinstance(max_orders_per_cycle, bool)
            or max_orders_per_cycle < 1
        ):
            raise ValueError("max_orders_per_cycle must be a positive integer")
        self._account_id = account_id
        self._strategy_id = strategy_id
        self._instruments = normalized
        self._calendar_reader = calendar_reader
        self._pre_open_mark_reader = pre_open_mark_reader
        self._quotes = quotes
        self._initializer = initializer
        self._coordinator = coordinator
        self._controls = controls
        self._sessions = sessions
        self._intent_source = intent_source
        self._max_quote_age = max_quote_age
        self._max_pre_open_mark_age = max_pre_open_mark_age
        self._max_orders_per_cycle = max_orders_per_cycle
        self._clock = clock or AShareMarketClock()
        self._cycle_lock = asyncio.Lock()

    @property
    def account_id(self) -> str:
        return self._account_id

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    async def tick(self, *, now: datetime) -> PaperSchedulerCycle:
        instant = to_utc(now, name="scheduler tick time")
        if self._cycle_lock.locked():
            current = await self._controls.get(account_id=self._account_id)
            await self._activate_dependency_failure(current=current, now=instant)
            raise PersistenceUnavailableError("paper scheduler cycle overlap detected")
        async with self._cycle_lock:
            return await self._tick(now=instant)

    async def _tick(self, *, now: datetime) -> PaperSchedulerCycle:
        session_date = now.astimezone(SHANGHAI).date()
        control = await self._controls.ensure_fail_closed(
            account_id=self._account_id,
            now=now,
        )
        phase = AShareTradingPhase.CLOSED
        calendar_hash: str | None = None
        try:
            session = await self._calendar_reader(session_date, now)
            calendar_hash = session.content_hash
            phase = self._clock.phase(now=now, session=session)
            if phase is AShareTradingPhase.PRE_OPEN:
                try:
                    await self._sessions.replay(
                        account_id=self._account_id,
                        session_date=session_date,
                    )
                except LookupError:
                    pass
                else:
                    return PaperSchedulerCycle(
                        account_id=self._account_id,
                        strategy_id=self._strategy_id,
                        session_date=session_date,
                        evaluated_at=now,
                        phase=phase,
                        status=PaperSchedulerStatus.SESSION_READY,
                        control=control,
                        clock_rule_version=self._clock.rule_version,
                        calendar_hash=calendar_hash,
                    )
                marks = await self._pre_open_mark_reader(session_date, self._instruments, now)
                self._validate_pre_open_marks(marks=marks, now=now)
                initialization = await self._initializer.initialize(
                    PaperSessionInitializationRequest(
                        session_date=session_date,
                        as_of=now,
                        market_phase=MarketPhase.PRE_OPEN,
                        marks=marks.marks,
                    )
                )
                return PaperSchedulerCycle(
                    account_id=self._account_id,
                    strategy_id=self._strategy_id,
                    session_date=session_date,
                    evaluated_at=now,
                    phase=phase,
                    status=PaperSchedulerStatus.SESSION_INITIALIZED,
                    control=initialization.control,
                    clock_rule_version=self._clock.rule_version,
                    calendar_hash=calendar_hash,
                    mark_evidence_hash=marks.marks_hash,
                    initialization=initialization,
                )
            if not phase.accepts_strategy_orders:
                return PaperSchedulerCycle(
                    account_id=self._account_id,
                    strategy_id=self._strategy_id,
                    session_date=session_date,
                    evaluated_at=now,
                    phase=phase,
                    status=PaperSchedulerStatus.IDLE,
                    control=control,
                    clock_rule_version=self._clock.rule_version,
                    calendar_hash=calendar_hash,
                )
            if control.active:
                return PaperSchedulerCycle(
                    account_id=self._account_id,
                    strategy_id=self._strategy_id,
                    session_date=session_date,
                    evaluated_at=now,
                    phase=phase,
                    status=PaperSchedulerStatus.LOCKED,
                    control=control,
                    clock_rule_version=self._clock.rule_version,
                    calendar_hash=calendar_hash,
                )
            await self._sessions.replay(
                account_id=self._account_id,
                session_date=session_date,
            )
            snapshot = self._quotes.snapshot(
                instruments=self._instruments,
                now=now,
                max_age=self._max_quote_age,
                require_market_open=True,
            )
            context = PaperStrategyContext(
                account_id=self._account_id,
                session_date=session_date,
                now=now,
                phase=phase,
                quote_snapshot=snapshot,
            )
            intents = tuple(await self._intent_source.generate(context))
            self._validate_intents(intents=intents, context=context)
            if not intents:
                return PaperSchedulerCycle(
                    account_id=self._account_id,
                    strategy_id=self._strategy_id,
                    session_date=session_date,
                    evaluated_at=now,
                    phase=phase,
                    status=PaperSchedulerStatus.NO_INTENTS,
                    control=control,
                    clock_rule_version=self._clock.rule_version,
                    calendar_hash=calendar_hash,
                    quote_evidence_hash=snapshot.evidence_hash,
                )
            results: list[PaperCoordinationResult] = []
            for intent in intents:
                results.append(
                    await self._coordinator.submit(
                        PaperSubmissionRequest(
                            order=intent.order,
                            quote=context.quotes[intent.order.instrument],
                            rules=intent.rules,
                            policy=intent.policy,
                            marks=snapshot.marks,
                            now=now,
                        )
                    )
                )
            latest_control = await self._controls.get(account_id=self._account_id)
            return PaperSchedulerCycle(
                account_id=self._account_id,
                strategy_id=self._strategy_id,
                session_date=session_date,
                evaluated_at=now,
                phase=phase,
                status=PaperSchedulerStatus.COMPLETED,
                control=latest_control,
                clock_rule_version=self._clock.rule_version,
                calendar_hash=calendar_hash,
                quote_evidence_hash=snapshot.evidence_hash,
                coordination_results=tuple(results),
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            failed_control = await self._activate_dependency_failure(
                current=control,
                now=now,
            )
            return PaperSchedulerCycle(
                account_id=self._account_id,
                strategy_id=self._strategy_id,
                session_date=session_date,
                evaluated_at=now,
                phase=phase,
                status=PaperSchedulerStatus.FAILED,
                control=failed_control,
                clock_rule_version=self._clock.rule_version,
                calendar_hash=calendar_hash,
                error_code=self._error_code(error),
            )

    async def run(
        self,
        *,
        stop: asyncio.Event,
        poll_interval: timedelta,
        now: Callable[[], datetime],
        sink: CycleSink,
    ) -> None:
        if poll_interval <= timedelta(0):
            raise ValueError("poll_interval must be positive")
        while not stop.is_set():
            cycle = await self.tick(now=now())
            try:
                await sink(cycle)
            except asyncio.CancelledError:
                raise
            except Exception:
                await self._activate_dependency_failure(
                    current=cycle.control,
                    now=cycle.evaluated_at,
                )
                raise PersistenceUnavailableError(
                    "paper scheduler cycle evidence persistence failed"
                ) from None
            try:
                await asyncio.wait_for(stop.wait(), timeout=poll_interval.total_seconds())
            except TimeoutError:
                pass

    async def fail_closed(self, *, now: datetime) -> KillSwitchControl:
        """Activate the dependency kill switch for an external runtime failure."""

        instant = to_utc(now, name="scheduler failure time")
        current = await self._controls.ensure_fail_closed(
            account_id=self._account_id,
            now=instant,
        )
        return await self._activate_dependency_failure(current=current, now=instant)

    def _validate_pre_open_marks(self, *, marks: PreOpenMarks, now: datetime) -> None:
        if not isinstance(marks, PreOpenMarks):
            raise TypeError("pre-open mark reader must return PreOpenMarks")
        if marks.session_date != now.astimezone(SHANGHAI).date():
            raise ValueError("pre-open marks belong to another session")
        if set(marks.marks) != set(self._instruments):
            raise ValueError("pre-open marks do not cover the configured universe")
        if marks.as_of > now or now - marks.as_of > self._max_pre_open_mark_age:
            raise ValueError("pre-open marks are stale or from the future")

    def _validate_intents(
        self,
        *,
        intents: tuple[PaperStrategyIntent, ...],
        context: PaperStrategyContext,
    ) -> None:
        if len(intents) > self._max_orders_per_cycle:
            raise ValueError("strategy exceeded the per-cycle order limit")
        if any(not isinstance(intent, PaperStrategyIntent) for intent in intents):
            raise TypeError("intent source returned an invalid item")
        order_ids = tuple(intent.order.client_order_id for intent in intents)
        if len(set(order_ids)) != len(order_ids):
            raise ValueError("strategy returned duplicate client_order_id values")
        for intent in intents:
            if intent.order.instrument not in self._instruments:
                raise ValueError("strategy intent is outside the scheduler universe")
            if intent.order.submitted_at != context.now:
                raise ValueError("strategy order timestamp must equal the scheduler tick")

    async def _activate_dependency_failure(
        self, *, current: KillSwitchControl, now: datetime
    ) -> KillSwitchControl:
        if current.active:
            return current
        return await self._controls.activate(
            account_id=self._account_id,
            command_id=f"paper-scheduler-dependency-{uuid4()}",
            reason=KillSwitchReason.DEPENDENCY_UNAVAILABLE,
            actor="paper-trading-scheduler",
            now=max(now, current.changed_at),
        )

    @staticmethod
    def _error_code(error: Exception) -> str:
        if isinstance(error, PersistenceUnavailableError):
            return "persistence_unavailable"
        if isinstance(error, MarketCalendarUnavailableError):
            return "market_calendar_unavailable"
        if isinstance(error, QuoteStreamUnavailableError):
            return "quote_stream_unavailable"
        if isinstance(error, LookupError):
            return "session_not_initialized"
        if isinstance(error, (TypeError, ValueError)):
            return "invalid_scheduler_input"
        return "scheduler_dependency_failed"


class LeasedPaperSchedulerRunner:
    """Run a scheduler only while this process owns a renewable durable lease."""

    def __init__(
        self,
        *,
        scheduler: PaperTradingScheduler,
        leases: PostgresPaperSchedulerLeaseRepository,
        holder_id: str,
        token: SecretStr,
        ttl: timedelta = timedelta(seconds=30),
        renewal_interval: timedelta = timedelta(seconds=10),
    ) -> None:
        if not timedelta(0) < renewal_interval < ttl:
            raise ValueError("renewal_interval must be positive and smaller than ttl")
        self._scheduler = scheduler
        self._leases = leases
        self._holder_id = holder_id
        self._token = token
        self._ttl = ttl
        self._renewal_interval = renewal_interval

    async def run(
        self,
        *,
        stop: asyncio.Event,
        poll_interval: timedelta,
        now: Callable[[], datetime],
        sink: CycleSink,
    ) -> None:
        lease = await self._leases.acquire(
            account_id=self._scheduler.account_id,
            strategy_id=self._scheduler.strategy_id,
            holder_id=self._holder_id,
            token=self._token,
            now=now(),
            ttl=self._ttl,
        )
        internal_stop = asyncio.Event()
        if stop.is_set():
            internal_stop.set()
        heartbeat_failed = False

        async def heartbeat() -> None:
            nonlocal lease
            while not internal_stop.is_set():
                try:
                    await asyncio.wait_for(
                        internal_stop.wait(),
                        timeout=self._renewal_interval.total_seconds(),
                    )
                except TimeoutError:
                    lease = await self._leases.renew(
                        account_id=self._scheduler.account_id,
                        strategy_id=self._scheduler.strategy_id,
                        holder_id=self._holder_id,
                        token=self._token,
                        now=max(to_utc(now(), name="scheduler heartbeat time"), lease.heartbeat_at),
                        ttl=self._ttl,
                    )

        async def relay_stop() -> None:
            await stop.wait()
            internal_stop.set()

        scheduler_task = asyncio.create_task(
            self._scheduler.run(
                stop=internal_stop,
                poll_interval=poll_interval,
                now=now,
                sink=sink,
            )
        )
        heartbeat_task = asyncio.create_task(heartbeat())
        relay_task = asyncio.create_task(relay_stop())
        run_error: BaseException | None = None
        try:
            done, _ = await asyncio.wait(
                {scheduler_task, heartbeat_task, relay_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if heartbeat_task in done:
                run_error = heartbeat_task.exception()
                heartbeat_failed = run_error is not None
                if run_error is None and not internal_stop.is_set():
                    run_error = PaperSchedulerLeaseLostError(
                        "paper scheduler heartbeat stopped unexpectedly"
                    )
                    heartbeat_failed = True
            if scheduler_task in done and run_error is None:
                run_error = scheduler_task.exception()
                if run_error is None and not internal_stop.is_set():
                    run_error = PersistenceUnavailableError(
                        "paper scheduler stopped unexpectedly"
                    )
            internal_stop.set()
            if not scheduler_task.done():
                await scheduler_task
        except asyncio.CancelledError:
            internal_stop.set()
            scheduler_task.cancel()
            heartbeat_task.cancel()
            relay_task.cancel()
            await asyncio.gather(
                scheduler_task,
                heartbeat_task,
                relay_task,
                return_exceptions=True,
            )
            try:
                await asyncio.shield(self._scheduler.fail_closed(now=now()))
            except Exception:
                pass
            if not heartbeat_failed:
                try:
                    await asyncio.shield(
                        self._leases.release(
                            account_id=self._scheduler.account_id,
                            strategy_id=self._scheduler.strategy_id,
                            holder_id=self._holder_id,
                            token=self._token,
                            now=max(
                                to_utc(now(), name="scheduler release time"),
                                lease.heartbeat_at,
                            ),
                        )
                    )
                except Exception:
                    pass
            raise
        finally:
            for task in (heartbeat_task, relay_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(heartbeat_task, relay_task, return_exceptions=True)

        if run_error is not None:
            await self._scheduler.fail_closed(now=now())
        if not heartbeat_failed:
            try:
                await self._leases.release(
                    account_id=self._scheduler.account_id,
                    strategy_id=self._scheduler.strategy_id,
                    holder_id=self._holder_id,
                    token=self._token,
                    now=max(to_utc(now(), name="scheduler release time"), lease.heartbeat_at),
                )
            except Exception:
                await self._scheduler.fail_closed(now=now())
                raise PersistenceUnavailableError(
                    "paper scheduler lease release failed"
                ) from None
        if run_error is not None:
            raise PersistenceUnavailableError(
                "paper scheduler lost durable runtime ownership"
            ) from None


def scheduler_cycle_payload(cycle: PaperSchedulerCycle) -> dict[str, object]:
    initialization = cycle.initialization
    initialization_payload = (
        None
        if initialization is None
        else {
            "control_state_hash": initialization.control.state_hash,
            "reconciliation_hash": initialization.reconciliation.report_hash,
            "session_state_hash": initialization.state.state_hash,
        }
    )
    results = [
        {
            "control_state_hash": result.control.state_hash,
            "decision_hash": (None if result.decision is None else result.decision.decision_hash),
            "post_reconciliation_hash": (
                None
                if result.post_reconciliation is None
                else result.post_reconciliation.report_hash
            ),
            "pre_reconciliation_hash": (
                None if result.pre_reconciliation is None else result.pre_reconciliation.report_hash
            ),
            "projection_hash": (
                None if result.projection is None else result.projection.projection_hash
            ),
            "status": result.status.value,
            "update_hashes": [update.update_hash for update in result.updates],
        }
        for result in cycle.coordination_results
    ]
    return {
        "account_id": cycle.account_id,
        "calendar_hash": cycle.calendar_hash,
        "clock_rule_version": cycle.clock_rule_version,
        "control_state_hash": cycle.control.state_hash,
        "coordination_results": results,
        "error_code": cycle.error_code,
        "evaluated_at": cycle.evaluated_at.isoformat(timespec="microseconds"),
        "initialization": initialization_payload,
        "mark_evidence_hash": cycle.mark_evidence_hash,
        "phase": cycle.phase.value,
        "quote_evidence_hash": cycle.quote_evidence_hash,
        "session_date": cycle.session_date.isoformat(),
        "status": cycle.status.value,
        "strategy_id": cycle.strategy_id,
    }
