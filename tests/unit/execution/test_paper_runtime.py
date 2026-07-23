from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from autoquant.data.daily_models import DailyCoverageEvidence, TradingSession
from autoquant.errors import (
    MarketCalendarUnavailableError,
    MissingCapabilityError,
    PersistenceUnavailableError,
)
from autoquant.execution.control import KillSwitchControl, KillSwitchReason
from autoquant.execution.paper_runtime import (
    ExactTradingCalendarReader,
    PaperRuntimeReadinessGate,
    ResidentPaperRuntime,
)
from autoquant.execution.paper_runtime_assembly import AssembledPaperRuntime
from autoquant.execution.paper_scheduler_store import PaperSchedulerRecovery
from autoquant.execution.simulated_broker import SimulatedBrokerSummary
from autoquant.execution.store import ExecutionStoreSummary

NOW = datetime(2026, 7, 23, 1, 30, tzinfo=UTC)
SESSION_DATE = date(2026, 7, 23)
ZERO_HASH = "0" * 64


def _control(*, active: bool = True) -> KillSwitchControl:
    return KillSwitchControl(
        account_id="paper-main",
        active=active,
        version=1,
        reason=(
            KillSwitchReason.INITIALIZING
            if active
            else KillSwitchReason.RESET_APPROVED
        ),
        changed_at=NOW - timedelta(hours=1),
        changed_by="test",
        last_event_hash=ZERO_HASH,
    )


def _execution_summary(
    *,
    orders: int = 0,
    open_orders: int = 0,
) -> ExecutionStoreSummary:
    return ExecutionStoreSummary(
        order_count=orders,
        event_count=orders,
        reconciliation_count=0,
        open_order_count=open_orders,
        latest_reconciliation_at=None,
        latest_reconciled=None,
        recovery_verified=True,
    )


def _broker_summary(
    *,
    orders: int = 0,
    open_orders: int = 0,
) -> SimulatedBrokerSummary:
    return SimulatedBrokerSummary(
        order_count=orders,
        fact_count=orders,
        open_order_count=open_orders,
        recovery_verified=True,
    )


def _readiness_gate(
    *,
    control: KillSwitchControl | None = None,
    registration: object | None = SimpleNamespace(
        account_id="paper-main",
        strategy_id="validated-sma-paper",
        execution_mode="paper",
        registration_hash="a" * 64,
        instrument="600000.XSHG",
    ),
    execution: ExecutionStoreSummary | None = None,
    broker: SimulatedBrokerSummary | None = None,
) -> tuple[PaperRuntimeReadinessGate, dict[str, AsyncMock]]:
    dependencies = {
        "controls": AsyncMock(),
        "strategies": AsyncMock(),
        "executions": AsyncMock(),
        "broker": AsyncMock(),
        "scheduler": AsyncMock(),
        "calendar": AsyncMock(),
    }
    dependencies["controls"].ensure_fail_closed.return_value = control or _control()
    dependencies["strategies"].active.return_value = registration
    dependencies["executions"].verify_recovery.return_value = (
        execution or _execution_summary()
    )
    dependencies["broker"].verify_recovery.return_value = (
        broker or _broker_summary()
    )
    dependencies["executions"].account_histories.return_value = ()
    dependencies["broker"].account_histories.return_value = ()
    dependencies["scheduler"].replay.return_value = PaperSchedulerRecovery(
        account_id="paper-main",
        event_count=3,
        latest_evaluated_at=NOW,
        latest_cycle_hash="b" * 64,
        recovery_verified=True,
    )
    dependencies["calendar"].return_value = TradingSession(
        source="tushare",
        session_date=SESSION_DATE,
        is_open=True,
        available_at=NOW - timedelta(days=1),
        response_hash="d" * 64,
    )
    return (
        PaperRuntimeReadinessGate(
            account_id="paper-main",
            strategy_id="validated-sma-paper",
            controls=dependencies["controls"],
            strategies=dependencies["strategies"],
            executions=dependencies["executions"],
            broker=dependencies["broker"],
            scheduler_events=dependencies["scheduler"],
            calendar=dependencies["calendar"],
        ),
        dependencies,
    )


@pytest.mark.asyncio
async def test_exact_calendar_reader_requires_matching_source_evidence() -> None:
    session = TradingSession(
        source="tushare",
        session_date=SESSION_DATE,
        is_open=True,
        available_at=NOW - timedelta(days=1),
        response_hash="c" * 64,
    )
    market = AsyncMock()
    market.query_coverage_as_of.return_value = DailyCoverageEvidence(
        sessions=(session,),
        lifecycles=(),
        suspensions=(),
    )
    control = AsyncMock()
    control.read_source_evidence.return_value = SimpleNamespace(
        source="tushare",
        method="trade_cal",
        requested_at=NOW - timedelta(days=1),
        response_hash=session.response_hash,
    )
    reader = ExactTradingCalendarReader(
        instruments=("600000.XSHG",),
        market_repository=market,
        control_repository=control,
    )

    assert await reader(SESSION_DATE, NOW) == session
    market.query_coverage_as_of.assert_awaited_once_with(
        ("600000.XSHG",),
        SESSION_DATE,
        SESSION_DATE,
        NOW,
    )

    control.read_source_evidence.return_value = SimpleNamespace(
        source="other",
        method="trade_cal",
        requested_at=NOW - timedelta(days=1),
        response_hash=session.response_hash,
    )
    with pytest.raises(MarketCalendarUnavailableError, match="does not match"):
        await reader(SESSION_DATE, NOW)


@pytest.mark.asyncio
async def test_readiness_replays_all_durable_boundaries() -> None:
    gate, dependencies = _readiness_gate()

    report = await gate.verify(now=NOW)

    assert report.instrument == "600000.XSHG"
    assert report.scheduler_event_count == 3
    assert report.execution_order_count == report.broker_order_count == 0
    assert report.calendar_hash
    dependencies["executions"].verify_recovery.assert_awaited_once_with(
        max_orders=10_000
    )
    dependencies["broker"].verify_recovery.assert_awaited_once_with(
        max_orders=10_000
    )
    dependencies["executions"].account_histories.assert_awaited_once_with(
        account_id="paper-main",
        max_orders=10_000,
    )
    dependencies["broker"].account_histories.assert_awaited_once_with(
        account_id="paper-main",
        max_orders=10_000,
    )


@pytest.mark.asyncio
async def test_readiness_refuses_missing_strategy_before_quote_start() -> None:
    gate, _ = _readiness_gate(registration=None)

    with pytest.raises(MissingCapabilityError, match="approved strategy"):
        await gate.verify(now=NOW)


@pytest.mark.asyncio
async def test_readiness_refuses_cold_start_with_inactive_kill_switch() -> None:
    gate, dependencies = _readiness_gate(control=_control(active=False))

    with pytest.raises(MissingCapabilityError, match="re-armed"):
        await gate.verify(now=NOW)

    dependencies["controls"].activate.assert_awaited_once()


@pytest.mark.asyncio
async def test_readiness_refuses_execution_broker_divergence() -> None:
    gate, _ = _readiness_gate(
        execution=_execution_summary(orders=1),
        broker=_broker_summary(orders=0),
    )

    with pytest.raises(PersistenceUnavailableError, match="do not converge"):
        await gate.verify(now=NOW)


@pytest.mark.asyncio
async def test_readiness_refuses_account_history_divergence() -> None:
    gate, dependencies = _readiness_gate()
    dependencies["executions"].account_histories.return_value = ("internal",)
    dependencies["broker"].account_histories.return_value = ()

    with pytest.raises(PersistenceUnavailableError, match="do not converge"):
        await gate.verify(now=NOW)


@pytest.mark.asyncio
async def test_resident_runtime_stops_both_components_and_closes_quotes() -> None:
    gate, _ = _readiness_gate()
    scheduler = AsyncMock()
    runner = AsyncMock()
    quotes = AsyncMock()
    started = asyncio.Event()

    async def run_component(*, stop: asyncio.Event, **_kwargs: object) -> None:
        started.set()
        await stop.wait()

    runner.run.side_effect = run_component
    quotes.pump.side_effect = run_component
    runtime = ResidentPaperRuntime(
        readiness=gate,
        scheduler=scheduler,
        runner=runner,
        quotes=quotes,
        sink=AsyncMock(),
        poll_interval=timedelta(seconds=1),
        now=lambda: NOW,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(runtime.run(stop=stop))
    await started.wait()
    stop.set()

    report = await task

    assert report is not None
    assert report.registration_hash == "a" * 64
    quotes.open.assert_awaited_once_with()
    quotes.close.assert_awaited_once_with()
    scheduler.fail_closed.assert_not_awaited()


@pytest.mark.asyncio
async def test_quote_failure_stops_scheduler_and_fails_closed() -> None:
    gate, _ = _readiness_gate()
    scheduler = AsyncMock()
    runner = AsyncMock()
    quotes = AsyncMock()

    async def run_scheduler(*, stop: asyncio.Event, **_kwargs: object) -> None:
        await stop.wait()

    runner.run.side_effect = run_scheduler
    quotes.pump.side_effect = RuntimeError("quote connection lost")
    runtime = ResidentPaperRuntime(
        readiness=gate,
        scheduler=scheduler,
        runner=runner,
        quotes=quotes,
        sink=AsyncMock(),
        poll_interval=timedelta(seconds=1),
        now=lambda: NOW,
    )

    with pytest.raises(PersistenceUnavailableError, match="failed closed"):
        await runtime.run(stop=asyncio.Event())

    scheduler.fail_closed.assert_awaited_once_with(now=NOW)
    quotes.close.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_missing_strategy_never_opens_quote_connection_and_fails_closed() -> None:
    gate, _ = _readiness_gate(registration=None)
    scheduler = AsyncMock()
    quotes = AsyncMock()
    runtime = ResidentPaperRuntime(
        readiness=gate,
        scheduler=scheduler,
        runner=AsyncMock(),
        quotes=quotes,
        sink=AsyncMock(),
        poll_interval=timedelta(seconds=1),
        now=lambda: NOW,
    )

    with pytest.raises(PersistenceUnavailableError, match="failed closed"):
        await runtime.run(stop=asyncio.Event())

    quotes.open.assert_not_awaited()
    scheduler.fail_closed.assert_awaited_once_with(now=NOW)


@pytest.mark.asyncio
async def test_partial_quote_open_failure_is_closed_and_fails_closed() -> None:
    gate, _ = _readiness_gate()
    scheduler = AsyncMock()
    quotes = AsyncMock()
    quotes.open.side_effect = RuntimeError("baseline failed")
    runtime = ResidentPaperRuntime(
        readiness=gate,
        scheduler=scheduler,
        runner=AsyncMock(),
        quotes=quotes,
        sink=AsyncMock(),
        poll_interval=timedelta(seconds=1),
        now=lambda: NOW,
    )

    with pytest.raises(PersistenceUnavailableError, match="failed closed"):
        await runtime.run(stop=asyncio.Event())

    quotes.close.assert_awaited_once_with()
    scheduler.fail_closed.assert_awaited_once_with(now=NOW)


@pytest.mark.asyncio
async def test_assembled_runtime_closes_owned_resources_once_in_reverse() -> None:
    closed: list[str] = []

    async def close_first() -> None:
        closed.append("first")

    async def close_second() -> None:
        closed.append("second")

    assembled = AssembledPaperRuntime(
        runtime=AsyncMock(),
        quote_bridge=AsyncMock(),
        closers=(close_first, close_second),
    )

    await assembled.close()
    await assembled.close()

    assert closed == ["second", "first"]


@pytest.mark.asyncio
async def test_assembled_runtime_attempts_every_close_after_one_failure() -> None:
    closed: list[str] = []

    async def close_first() -> None:
        closed.append("first")

    async def close_second() -> None:
        closed.append("second")
        raise RuntimeError("close failed")

    assembled = AssembledPaperRuntime(
        runtime=AsyncMock(),
        quote_bridge=AsyncMock(),
        closers=(close_first, close_second),
    )

    with pytest.raises(PersistenceUnavailableError, match="failed to close"):
        await assembled.close()

    assert closed == ["second", "first"]
