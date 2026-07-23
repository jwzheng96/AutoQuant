from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Protocol

from autoquant.clock import to_shanghai, to_utc
from autoquant.data.daily_models import TradingSession
from autoquant.data.daily_ports import DailyMarketRepository
from autoquant.data.ingestion import ControlRepository
from autoquant.errors import (
    MarketCalendarUnavailableError,
    MissingCapabilityError,
    PersistenceUnavailableError,
)
from autoquant.execution.control import KillSwitchControl
from autoquant.execution.models import PaperOrderHistory
from autoquant.execution.paper_scheduler import (
    CycleSink,
    LeasedPaperSchedulerRunner,
    PaperTradingScheduler,
)
from autoquant.execution.paper_scheduler_store import PaperSchedulerRecovery
from autoquant.execution.simulated_broker import SimulatedBrokerSummary
from autoquant.execution.store import ExecutionStoreSummary
from autoquant.execution.validated_sma import ValidatedSmaRegistration


class ExecutionControlReader(Protocol):
    async def ensure_fail_closed(
        self,
        *,
        account_id: str,
        now: datetime,
    ) -> KillSwitchControl: ...


class ActiveStrategyReader(Protocol):
    async def active(
        self,
        *,
        account_id: str,
        strategy_id: str,
    ) -> ValidatedSmaRegistration | None: ...


class ExecutionRecoveryReader(Protocol):
    async def verify_recovery(
        self,
        *,
        max_orders: int = 10_000,
    ) -> ExecutionStoreSummary: ...

    async def account_histories(
        self,
        *,
        account_id: str,
        max_orders: int = 10_000,
    ) -> tuple[PaperOrderHistory, ...]: ...


class SimulatedBrokerRecoveryReader(Protocol):
    async def verify_recovery(
        self,
        *,
        max_orders: int = 10_000,
    ) -> SimulatedBrokerSummary: ...

    async def account_histories(
        self,
        *,
        account_id: str,
        max_orders: int = 10_000,
    ) -> tuple[PaperOrderHistory, ...]: ...


class SchedulerRecoveryReader(Protocol):
    async def replay(self, *, account_id: str) -> PaperSchedulerRecovery: ...


class RuntimeCalendarReader(Protocol):
    async def __call__(
        self,
        session_date: date,
        as_of: datetime,
    ) -> TradingSession: ...


class PaperQuoteRuntime(Protocol):
    """Own one quote connection and publish only normalized, baselined observations."""

    async def open(self) -> None: ...

    async def pump(self, *, stop: asyncio.Event) -> None: ...

    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class PaperRuntimeReadiness:
    account_id: str
    strategy_id: str
    registration_hash: str
    instrument: str
    kill_switch_state_hash: str
    calendar_hash: str
    execution_order_count: int
    broker_order_count: int
    scheduler_event_count: int
    checked_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "checked_at",
            to_utc(self.checked_at, name="paper runtime readiness time"),
        )


class ExactTradingCalendarReader:
    """Read one current exchange session only from source-backed point-in-time facts."""

    def __init__(
        self,
        *,
        instruments: tuple[str, ...],
        market_repository: DailyMarketRepository,
        control_repository: ControlRepository,
        source: str = "tushare",
    ) -> None:
        normalized = tuple(sorted(instruments))
        if (
            not normalized
            or len(set(normalized)) != len(normalized)
            or any(not value.strip() for value in normalized)
        ):
            raise ValueError("calendar instruments must be nonempty and unique")
        if not source.strip():
            raise ValueError("calendar source cannot be empty")
        self._instruments = normalized
        self._market = market_repository
        self._control = control_repository
        self._source = source

    async def __call__(
        self,
        session_date: date,
        as_of: datetime,
    ) -> TradingSession:
        cutoff = to_utc(as_of, name="trading calendar read time")
        coverage = await self._market.query_coverage_as_of(
            self._instruments,
            session_date,
            session_date,
            cutoff,
        )
        sessions = tuple(
            value
            for value in coverage.sessions
            if value.source == self._source
            and value.session_date == session_date
        )
        if len(sessions) != 1:
            raise MarketCalendarUnavailableError(
                "exact trading calendar session is unavailable"
            )
        session = sessions[0]
        if session.available_at > cutoff:
            raise MarketCalendarUnavailableError(
                "trading calendar session is not yet visible"
            )
        try:
            evidence = await self._control.read_source_evidence(
                session.response_hash
            )
        except Exception:
            raise MarketCalendarUnavailableError(
                "trading calendar source evidence is unavailable"
            ) from None
        if (
            evidence.source != self._source
            or evidence.method != "trade_cal"
            or evidence.requested_at > cutoff
            or evidence.response_hash != session.response_hash
        ):
            raise MarketCalendarUnavailableError(
                "trading calendar source evidence does not match"
            )
        return session


class PaperRuntimeReadinessGate:
    """Replay every durable paper boundary before a quote connection may open."""

    def __init__(
        self,
        *,
        account_id: str,
        strategy_id: str,
        controls: ExecutionControlReader,
        strategies: ActiveStrategyReader,
        executions: ExecutionRecoveryReader,
        broker: SimulatedBrokerRecoveryReader,
        scheduler_events: SchedulerRecoveryReader,
        calendar: RuntimeCalendarReader,
        max_orders: int = 10_000,
    ) -> None:
        if not account_id.strip() or not strategy_id.strip():
            raise ValueError("paper runtime identifiers cannot be empty")
        if max_orders < 1:
            raise ValueError("max_orders must be positive")
        self._account_id = account_id
        self._strategy_id = strategy_id
        self._controls = controls
        self._strategies = strategies
        self._executions = executions
        self._broker = broker
        self._scheduler_events = scheduler_events
        self._calendar = calendar
        self._max_orders = max_orders

    async def verify(self, *, now: datetime) -> PaperRuntimeReadiness:
        instant = to_utc(now, name="paper runtime readiness time")
        control = await self._controls.ensure_fail_closed(
            account_id=self._account_id,
            now=instant,
        )
        if not control.active:
            raise MissingCapabilityError(
                "paper runtime cold start requires an active kill switch"
            )
        registration = await self._strategies.active(
            account_id=self._account_id,
            strategy_id=self._strategy_id,
        )
        if registration is None:
            raise MissingCapabilityError(
                "paper runtime requires an active approved strategy"
            )
        if (
            registration.account_id != self._account_id
            or registration.strategy_id != self._strategy_id
            or registration.execution_mode != "paper"
        ):
            raise PersistenceUnavailableError(
                "paper strategy registration identity is inconsistent"
            )
        execution = await self._executions.verify_recovery(
            max_orders=self._max_orders
        )
        broker = await self._broker.verify_recovery(
            max_orders=self._max_orders
        )
        execution_histories = await self._executions.account_histories(
            account_id=self._account_id,
            max_orders=self._max_orders,
        )
        broker_histories = await self._broker.account_histories(
            account_id=self._account_id,
            max_orders=self._max_orders,
        )
        scheduler = await self._scheduler_events.replay(
            account_id=self._account_id
        )
        session = await self._calendar(
            to_shanghai(instant).date(),
            instant,
        )
        if session.available_at > instant:
            raise PersistenceUnavailableError(
                "paper runtime calendar evidence is from the future"
            )
        if (
            not execution.recovery_verified
            or not broker.recovery_verified
            or not scheduler.recovery_verified
            or execution.order_count != broker.order_count
            or execution.open_order_count != broker.open_order_count
            or execution_histories != broker_histories
        ):
            raise PersistenceUnavailableError(
                "paper execution and simulated broker recovery do not converge"
            )
        return PaperRuntimeReadiness(
            account_id=self._account_id,
            strategy_id=self._strategy_id,
            registration_hash=registration.registration_hash,
            instrument=registration.instrument,
            kill_switch_state_hash=control.state_hash,
            calendar_hash=session.content_hash,
            execution_order_count=len(execution_histories),
            broker_order_count=len(broker_histories),
            scheduler_event_count=scheduler.event_count,
            checked_at=instant,
        )


class ResidentPaperRuntime:
    """Supervise quotes and the leased scheduler as one fail-closed process."""

    def __init__(
        self,
        *,
        readiness: PaperRuntimeReadinessGate,
        scheduler: PaperTradingScheduler,
        runner: LeasedPaperSchedulerRunner,
        quotes: PaperQuoteRuntime,
        sink: CycleSink,
        poll_interval: timedelta,
        now: Callable[[], datetime],
    ) -> None:
        if poll_interval <= timedelta(0):
            raise ValueError("paper runtime poll interval must be positive")
        self._readiness = readiness
        self._scheduler = scheduler
        self._runner = runner
        self._quotes = quotes
        self._sink = sink
        self._poll_interval = poll_interval
        self._now = now
        self._run_lock = asyncio.Lock()

    async def run(self, *, stop: asyncio.Event) -> PaperRuntimeReadiness | None:
        if stop.is_set():
            return None
        if self._run_lock.locked():
            raise RuntimeError("paper runtime is already running")
        async with self._run_lock:
            return await self._run(stop=stop)

    async def _run(
        self,
        *,
        stop: asyncio.Event,
    ) -> PaperRuntimeReadiness:
        readiness: PaperRuntimeReadiness | None = None
        quote_open_attempted = False
        internal_stop = asyncio.Event()
        runner_task: asyncio.Task[None] | None = None
        quote_task: asyncio.Task[None] | None = None
        relay_task: asyncio.Task[None] | None = None
        failure: BaseException | None = None
        try:
            readiness = await self._readiness.verify(now=self._now())
            quote_open_attempted = True
            await self._quotes.open()

            async def relay_stop() -> None:
                await stop.wait()
                internal_stop.set()

            runner_task = asyncio.create_task(
                self._runner.run(
                    stop=internal_stop,
                    poll_interval=self._poll_interval,
                    now=self._now,
                    sink=self._sink,
                )
            )
            quote_task = asyncio.create_task(
                self._quotes.pump(stop=internal_stop)
            )
            relay_task = asyncio.create_task(relay_stop())
            done, _ = await asyncio.wait(
                {runner_task, quote_task, relay_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if relay_task not in done:
                completed = runner_task if runner_task in done else quote_task
                failure = completed.exception()
                if failure is None:
                    failure = PersistenceUnavailableError(
                        "paper runtime component stopped unexpectedly"
                    )
            internal_stop.set()
            component_results = await asyncio.gather(
                runner_task,
                quote_task,
                return_exceptions=True,
            )
            if failure is None:
                failure = next(
                    (
                        result
                        for result in component_results
                        if isinstance(result, BaseException)
                    ),
                    None,
                )
        except asyncio.CancelledError:
            internal_stop.set()
            for task in (runner_task, quote_task, relay_task):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(
                *(
                    task
                    for task in (runner_task, quote_task, relay_task)
                    if task is not None
                ),
                return_exceptions=True,
            )
            await self._fail_closed()
            raise
        except BaseException as error:
            failure = error
            internal_stop.set()
        finally:
            if relay_task is not None and not relay_task.done():
                relay_task.cancel()
            if relay_task is not None:
                await asyncio.gather(relay_task, return_exceptions=True)
            if quote_open_attempted:
                try:
                    await self._quotes.close()
                except BaseException as error:
                    if failure is None:
                        failure = error
            if failure is not None:
                await self._fail_closed()
        if failure is not None:
            raise PersistenceUnavailableError(
                "resident paper runtime failed closed"
            ) from failure
        if readiness is None:
            raise PersistenceUnavailableError(
                "paper runtime readiness was not established"
            )
        return readiness

    async def _fail_closed(self) -> None:
        try:
            await self._scheduler.fail_closed(now=self._now())
        except Exception:
            pass


async def close_paper_runtime_resources(
    *closers: Callable[[], Awaitable[None]],
) -> None:
    """Close independently owned stores without abandoning later resources."""

    results = await asyncio.gather(
        *(closer() for closer in reversed(closers)),
        return_exceptions=True,
    )
    if any(isinstance(result, BaseException) for result in results):
        raise PersistenceUnavailableError(
            "one or more paper runtime resources failed to close"
        )
