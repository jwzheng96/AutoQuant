from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.execution.control import KillSwitchControl, KillSwitchReason
from autoquant.execution.market_clock import AShareTradingPhase
from autoquant.execution.paper_scheduler import (
    PaperSchedulerCycle,
    PaperSchedulerStatus,
)
from autoquant.execution.paper_scheduler_store import (
    PostgresPaperSchedulerRepository,
)

POSTGRES_DSN = os.environ.get("AQ_POSTGRES_DSN", "").strip()
NOW = datetime(2026, 7, 23, 1, 20, tzinfo=UTC)
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not POSTGRES_DSN,
        reason="AQ_POSTGRES_DSN is not configured; PostgreSQL infrastructure unavailable",
    ),
]


@pytest_asyncio.fixture
async def scheduler_store() -> AsyncIterator[
    tuple[PostgresPaperSchedulerRepository, AsyncEngine, str]
]:
    schema = f"autoquant_test_{uuid4().hex}"
    engine = create_async_engine(POSTGRES_DSN, pool_pre_ping=True)
    control_schema = PostgresControlRepository(engine=engine, schema=schema)
    repository = PostgresPaperSchedulerRepository(engine=engine, schema=schema)
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/013_paper_scheduler_events.sql",
        )
    )
    try:
        await control_schema.initialize(migration)
        yield repository, engine, schema
    finally:
        try:
            await control_schema.drop_test_schema()
        finally:
            await engine.dispose()


def _control(*, active: bool) -> KillSwitchControl:
    return KillSwitchControl(
        account_id="paper-main",
        active=active,
        version=1,
        reason=(KillSwitchReason.INITIALIZING if active else KillSwitchReason.RESET_APPROVED),
        changed_at=datetime(2026, 7, 23, tzinfo=UTC),
        changed_by="integration-test",
        last_event_hash="0" * 64,
    )


def _cycle(
    *,
    seconds: int,
    status: PaperSchedulerStatus,
    phase: AShareTradingPhase = AShareTradingPhase.OPENING_AUCTION,
) -> PaperSchedulerCycle:
    return PaperSchedulerCycle(
        account_id="paper-main",
        strategy_id="integration-strategy-v1",
        session_date=date(2026, 7, 23),
        evaluated_at=NOW + timedelta(seconds=seconds),
        phase=phase,
        status=status,
        control=_control(
            active=status in {PaperSchedulerStatus.LOCKED, PaperSchedulerStatus.FAILED}
        ),
        clock_rule_version="cn-equity-auction-hours-2026-v1",
        calendar_hash="a" * 64,
        error_code=(
            "scheduler_dependency_failed" if status is PaperSchedulerStatus.FAILED else None
        ),
    )


@pytest.mark.asyncio
async def test_scheduler_cycles_are_idempotent_hash_chained_and_replayable(
    scheduler_store: tuple[PostgresPaperSchedulerRepository, AsyncEngine, str],
) -> None:
    repository, _, _ = scheduler_store
    first_cycle = _cycle(seconds=0, status=PaperSchedulerStatus.IDLE)
    second_cycle = _cycle(seconds=1, status=PaperSchedulerStatus.FAILED)

    first = await repository.append(first_cycle)
    second = await repository.append(second_cycle)
    duplicate = await repository.append(first_cycle)
    recovery = await repository.replay(account_id="paper-main")

    assert first.sequence == 1
    assert second.sequence == 2
    assert second.previous_hash == first.event_hash
    assert duplicate == first
    assert recovery.recovery_verified is True
    assert recovery.event_count == 2
    assert recovery.latest_cycle_hash == second_cycle.cycle_hash


@pytest.mark.asyncio
async def test_scheduler_cycles_serialize_concurrent_appends(
    scheduler_store: tuple[PostgresPaperSchedulerRepository, AsyncEngine, str],
) -> None:
    repository, _, _ = scheduler_store
    cycles = (
        _cycle(seconds=0, status=PaperSchedulerStatus.IDLE),
        _cycle(
            seconds=0,
            status=PaperSchedulerStatus.LOCKED,
            phase=AShareTradingPhase.MORNING_CONTINUOUS,
        ),
    )

    events = await asyncio.gather(*(repository.append(cycle) for cycle in cycles))
    recovery = await repository.replay(account_id="paper-main")

    assert {event.sequence for event in events} == {1, 2}
    assert recovery.event_count == 2


@pytest.mark.asyncio
async def test_scheduler_rejects_time_regression(
    scheduler_store: tuple[PostgresPaperSchedulerRepository, AsyncEngine, str],
) -> None:
    repository, _, _ = scheduler_store
    await repository.append(_cycle(seconds=1, status=PaperSchedulerStatus.IDLE))

    with pytest.raises(ValueError, match="backwards"):
        await repository.append(_cycle(seconds=0, status=PaperSchedulerStatus.LOCKED))

    assert (await repository.replay(account_id="paper-main")).event_count == 1


@pytest.mark.asyncio
async def test_scheduler_events_and_materialized_sequence_are_database_guarded(
    scheduler_store: tuple[PostgresPaperSchedulerRepository, AsyncEngine, str],
) -> None:
    repository, engine, schema = scheduler_store
    event = await repository.append(_cycle(seconds=0, status=PaperSchedulerStatus.IDLE))

    with pytest.raises(DBAPIError):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"UPDATE {schema}.paper_scheduler_events "
                    "SET status='failed' WHERE event_hash=:event_hash"
                ),
                {"event_hash": event.event_hash},
            )
    with pytest.raises(DBAPIError):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"UPDATE {schema}.paper_scheduler_state "
                    "SET last_sequence=last_sequence + 2 WHERE account_id='paper-main'"
                )
            )
