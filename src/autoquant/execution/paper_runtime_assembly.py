from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Self
from uuid import uuid4

from pydantic import SecretStr

from autoquant.adapters.clickhouse_daily import ClickHouseDailyRepository
from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.config import AppSettings, RuntimeEnvironment
from autoquant.data.daily_ingestion import ValidatedDailyDatasetReader
from autoquant.errors import MissingCapabilityError, PersistenceUnavailableError
from autoquant.execution.control import KillSwitchReason
from autoquant.execution.control_store import PostgresExecutionControlRepository
from autoquant.execution.coordinator import PaperOrderCoordinator
from autoquant.execution.market_clock import AShareMarketClock
from autoquant.execution.paper_policy import default_paper_policy
from autoquant.execution.paper_runtime import (
    ExactTradingCalendarReader,
    PaperQuoteRuntime,
    PaperRuntimeReadinessGate,
    ResidentPaperRuntime,
    close_paper_runtime_resources,
)
from autoquant.execution.paper_scheduler import (
    LeasedPaperSchedulerRunner,
    PaperSchedulerCycle,
    PaperTradingScheduler,
)
from autoquant.execution.paper_scheduler_lease_store import (
    PostgresPaperSchedulerLeaseRepository,
)
from autoquant.execution.paper_scheduler_store import (
    PostgresPaperSchedulerRepository,
)
from autoquant.execution.pre_open_marks import DailyClosePreOpenMarkReader
from autoquant.execution.qmt_quote_adapter import QmtWholeQuoteBridge
from autoquant.execution.quote_book import ContinuousQuoteBook
from autoquant.execution.session_initializer import PaperSessionInitializer
from autoquant.execution.session_risk_store import (
    PostgresPaperSessionRiskRepository,
)
from autoquant.execution.session_rules import ExactSessionRuleReader
from autoquant.execution.simulated_broker import PersistentSimulatedBroker
from autoquant.execution.store import PostgresPaperExecutionRepository
from autoquant.execution.strategy_account import PaperStrategyAccountReader
from autoquant.execution.strategy_registry_store import (
    PostgresPaperStrategyRegistry,
)
from autoquant.execution.target_strategy import TargetPositionPaperIntentSource
from autoquant.execution.validated_sma import ValidatedSmaTargetProvider
from autoquant.web.risk_store import PostgresRiskDecisionRepository

PaperQuoteRuntimeFactory = Callable[
    [
        QmtWholeQuoteBridge,
        tuple[str, ...],
        ExactTradingCalendarReader,
        AShareMarketClock,
    ],
    PaperQuoteRuntime,
]


def _configured_dsn(
    value: SecretStr | None,
    *,
    capability: str,
) -> str:
    if value is None or not value.get_secret_value().strip():
        raise MissingCapabilityError(f"{capability} is not configured")
    return value.get_secret_value()


class AssembledPaperRuntime:
    """Own all stores used by one resident scheduler assembly."""

    def __init__(
        self,
        *,
        runtime: ResidentPaperRuntime,
        quote_bridge: QmtWholeQuoteBridge,
        closers: tuple[Callable[[], Awaitable[None]], ...],
    ) -> None:
        self.runtime = runtime
        self.quote_bridge = quote_bridge
        self._closers = closers
        self._close_lock = asyncio.Lock()
        self._closed = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        _exception_type: object,
        _exception: object,
        _traceback: object,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            await close_paper_runtime_resources(*self._closers)
            self._closed = True


async def assemble_paper_runtime(
    settings: AppSettings,
    *,
    quote_runtime_factory: PaperQuoteRuntimeFactory,
    now: Callable[[], datetime] | None = None,
) -> AssembledPaperRuntime:
    """Wire the paper-only scheduler; the injected quote runtime owns no broker mutation."""

    if settings.environment is not RuntimeEnvironment.PAPER:
        raise MissingCapabilityError("paper environment is not configured")
    if settings.live_trading_enabled:
        raise MissingCapabilityError("live trading must remain hard-locked")
    lease = settings.require_paper_runtime()
    clock_now = now or (lambda: datetime.now(UTC))
    postgres_dsn = _configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    clickhouse_dsn = _configured_dsn(
        settings.clickhouse_dsn,
        capability="ClickHouse",
    )
    closers: list[Callable[[], Awaitable[None]]] = []
    try:
        clickhouse = await ClickHouseDailyRepository.connect(
            dsn=clickhouse_dsn,
            source="tushare",
        )
        closers.append(clickhouse.client.close)
        evidence = PostgresControlRepository.connect(dsn=postgres_dsn)
        closers.append(evidence.close)
        controls = PostgresExecutionControlRepository.connect(dsn=postgres_dsn)
        closers.append(controls.close)
        executions = PostgresPaperExecutionRepository.connect(dsn=postgres_dsn)
        closers.append(executions.close)
        broker = PersistentSimulatedBroker.connect(dsn=postgres_dsn)
        closers.append(broker.close)
        sessions = PostgresPaperSessionRiskRepository.connect(dsn=postgres_dsn)
        closers.append(sessions.close)
        risks = PostgresRiskDecisionRepository.connect(dsn=postgres_dsn)
        closers.append(risks.close)
        scheduler_events = PostgresPaperSchedulerRepository.connect(
            dsn=postgres_dsn
        )
        closers.append(scheduler_events.close)
        scheduler_leases = PostgresPaperSchedulerLeaseRepository.connect(
            dsn=postgres_dsn
        )
        closers.append(scheduler_leases.close)
        strategies = PostgresPaperStrategyRegistry.connect(dsn=postgres_dsn)
        closers.append(strategies.close)

        cold_start_control = await controls.ensure_fail_closed(
            account_id=settings.paper_account_id,
            now=clock_now(),
        )
        if not cold_start_control.active:
            await controls.activate(
                account_id=settings.paper_account_id,
                command_id=f"paper-runtime-cold-start-{uuid4()}",
                reason=KillSwitchReason.DEPENDENCY_UNAVAILABLE,
                actor="paper-runtime-assembly",
                now=max(clock_now(), cold_start_control.changed_at),
            )
            raise MissingCapabilityError(
                "paper runtime cold start re-armed the inactive kill switch"
            )
        registration = await strategies.active(
            account_id=settings.paper_account_id,
            strategy_id=settings.paper_strategy_id,
        )
        if registration is None:
            raise MissingCapabilityError(
                "paper runtime requires an active approved strategy"
            )
        instruments = (registration.instrument,)
        policy = default_paper_policy(instruments)
        calendar = ExactTradingCalendarReader(
            instruments=instruments,
            market_repository=clickhouse,
            control_repository=evidence,
        )
        session_rules = ExactSessionRuleReader(
            market_repository=clickhouse,
            control_repository=evidence,
        )
        target_provider = ValidatedSmaTargetProvider(
            strategy_id=settings.paper_strategy_id,
            registry=strategies,
            control_repository=evidence,
            dataset_reader=ValidatedDailyDatasetReader(
                control_repository=evidence,
                market_repository=clickhouse,
            ),
            session_rule_reader=session_rules,
            policy=policy,
        )
        intent_source = TargetPositionPaperIntentSource(
            strategy_id=settings.paper_strategy_id,
            provider=target_provider,
        )
        quote_book = ContinuousQuoteBook(source="qmt")
        quote_bridge = QmtWholeQuoteBridge(
            quote_book=quote_book,
            instruments=instruments,
        )
        market_clock = AShareMarketClock()
        initializer = PaperSessionInitializer(
            account_id=settings.paper_account_id,
            initial_cash=settings.paper_initial_cash,
            executions=executions,
            controls=controls,
            broker=broker,
            sessions=sessions,
        )
        coordinator = PaperOrderCoordinator(
            account_id=settings.paper_account_id,
            initial_cash=settings.paper_initial_cash,
            risks=risks,
            executions=executions,
            controls=controls,
            broker=broker,
            sessions=sessions,
        )
        account_reader = PaperStrategyAccountReader(
            account_id=settings.paper_account_id,
            initial_cash=settings.paper_initial_cash,
            executions=executions,
            controls=controls,
            broker=broker,
            sessions=sessions,
        )
        scheduler = PaperTradingScheduler(
            account_id=settings.paper_account_id,
            strategy_id=settings.paper_strategy_id,
            instruments=instruments,
            calendar_reader=calendar,
            pre_open_mark_reader=DailyClosePreOpenMarkReader(
                repository=clickhouse,
                evidence_repository=evidence,
                manifest_hash=registration.signal_manifest_hash,
                source="tushare",
            ),
            quotes=quote_book,
            initializer=initializer,
            coordinator=coordinator,
            controls=controls,
            sessions=sessions,
            intent_source=intent_source,
            strategy_account_reader=account_reader,
            clock=market_clock,
        )
        readiness = PaperRuntimeReadinessGate(
            account_id=settings.paper_account_id,
            strategy_id=settings.paper_strategy_id,
            controls=controls,
            strategies=strategies,
            executions=executions,
            broker=broker,
            scheduler_events=scheduler_events,
            calendar=calendar,
        )
        runner = LeasedPaperSchedulerRunner(
            scheduler=scheduler,
            leases=scheduler_leases,
            holder_id=lease.holder_id,
            token=lease.lease_token,
            ttl=timedelta(
                seconds=settings.paper_scheduler_lease_ttl_seconds
            ),
            renewal_interval=timedelta(
                seconds=settings.paper_scheduler_renewal_seconds
            ),
        )
        quote_runtime = quote_runtime_factory(
            quote_bridge,
            instruments,
            calendar,
            market_clock,
        )

        async def persist_cycle(cycle: PaperSchedulerCycle) -> None:
            await scheduler_events.append(cycle)

        runtime = ResidentPaperRuntime(
            readiness=readiness,
            scheduler=scheduler,
            runner=runner,
            quotes=quote_runtime,
            sink=persist_cycle,
            poll_interval=timedelta(
                seconds=float(settings.paper_poll_interval_seconds)
            ),
            now=clock_now,
        )
        return AssembledPaperRuntime(
            runtime=runtime,
            quote_bridge=quote_bridge,
            closers=tuple(closers),
        )
    except BaseException:
        try:
            await close_paper_runtime_resources(*closers)
        except PersistenceUnavailableError:
            pass
        raise
