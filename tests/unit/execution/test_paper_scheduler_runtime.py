from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import SecretStr

from autoquant.errors import PaperSchedulerLeaseLostError, PersistenceUnavailableError
from autoquant.execution.paper_scheduler import LeasedPaperSchedulerRunner
from autoquant.execution.paper_scheduler_lease_store import PaperSchedulerLease

NOW = datetime(2026, 7, 23, 1, 30, tzinfo=UTC)
TOKEN = SecretStr("scheduler-runtime-token-at-least-32-chars")


def _lease(*, heartbeat_at: datetime = NOW) -> PaperSchedulerLease:
    return PaperSchedulerLease(
        account_id="paper-main",
        strategy_id="strategy-v1",
        holder_id="runtime-a",
        token_hash="a" * 64,
        acquired_at=NOW,
        heartbeat_at=heartbeat_at,
        expires_at=heartbeat_at + timedelta(seconds=1),
        released_at=None,
        generation=1,
        version=1,
        event_sequence=1,
        last_event_hash="b" * 64,
    )


def _scheduler() -> MagicMock:
    scheduler = MagicMock()
    scheduler.account_id = "paper-main"
    scheduler.strategy_id = "strategy-v1"

    async def run(*, stop, poll_interval, now, sink):  # type: ignore[no-untyped-def]
        del poll_interval, now, sink
        await stop.wait()

    scheduler.run = AsyncMock(side_effect=run)
    scheduler.fail_closed = AsyncMock()
    return scheduler


@pytest.mark.asyncio
async def test_runtime_acquires_owns_and_releases_around_scheduler() -> None:
    scheduler = _scheduler()
    leases = MagicMock()
    leases.acquire = AsyncMock(return_value=_lease())
    leases.renew = AsyncMock(return_value=_lease())
    leases.release = AsyncMock(return_value=MagicMock())
    runner = LeasedPaperSchedulerRunner(
        scheduler=scheduler,
        leases=leases,
        holder_id="runtime-a",
        token=TOKEN,
        ttl=timedelta(seconds=1),
        renewal_interval=timedelta(milliseconds=10),
    )
    stop = asyncio.Event()
    stop.set()

    await runner.run(
        stop=stop,
        poll_interval=timedelta(seconds=1),
        now=lambda: NOW,
        sink=AsyncMock(),
    )

    leases.acquire.assert_awaited_once()
    leases.release.assert_awaited_once()
    scheduler.fail_closed.assert_not_awaited()


@pytest.mark.asyncio
async def test_runtime_heartbeat_loss_stops_scheduler_and_fails_closed() -> None:
    scheduler = _scheduler()
    leases = MagicMock()
    leases.acquire = AsyncMock(return_value=_lease())
    leases.renew = AsyncMock(side_effect=PaperSchedulerLeaseLostError("lost"))
    leases.release = AsyncMock()
    runner = LeasedPaperSchedulerRunner(
        scheduler=scheduler,
        leases=leases,
        holder_id="runtime-a",
        token=TOKEN,
        ttl=timedelta(seconds=1),
        renewal_interval=timedelta(milliseconds=1),
    )

    with pytest.raises(PersistenceUnavailableError, match="lost durable"):
        await runner.run(
            stop=asyncio.Event(),
            poll_interval=timedelta(seconds=1),
            now=lambda: NOW,
            sink=AsyncMock(),
        )

    leases.renew.assert_awaited_once()
    leases.release.assert_not_awaited()
    scheduler.fail_closed.assert_awaited_once_with(now=NOW)


@pytest.mark.asyncio
async def test_runtime_release_failure_fails_closed() -> None:
    scheduler = _scheduler()
    leases = MagicMock()
    leases.acquire = AsyncMock(return_value=_lease())
    leases.renew = AsyncMock()
    leases.release = AsyncMock(side_effect=RuntimeError("db unavailable"))
    runner = LeasedPaperSchedulerRunner(
        scheduler=scheduler,
        leases=leases,
        holder_id="runtime-a",
        token=TOKEN,
        ttl=timedelta(seconds=1),
        renewal_interval=timedelta(milliseconds=10),
    )
    stop = asyncio.Event()
    stop.set()

    with pytest.raises(PersistenceUnavailableError, match="release failed"):
        await runner.run(
            stop=stop,
            poll_interval=timedelta(seconds=1),
            now=lambda: NOW,
            sink=AsyncMock(),
        )

    scheduler.fail_closed.assert_awaited_once_with(now=NOW)


@pytest.mark.asyncio
async def test_runtime_cancellation_attempts_fail_closed_release_before_reraising() -> None:
    scheduler = _scheduler()
    leases = MagicMock()
    leases.acquire = AsyncMock(return_value=_lease())
    leases.renew = AsyncMock()
    leases.release = AsyncMock(return_value=MagicMock())
    runner = LeasedPaperSchedulerRunner(
        scheduler=scheduler,
        leases=leases,
        holder_id="runtime-a",
        token=TOKEN,
        ttl=timedelta(seconds=1),
        renewal_interval=timedelta(milliseconds=10),
    )
    task = asyncio.create_task(
        runner.run(
            stop=asyncio.Event(),
            poll_interval=timedelta(seconds=1),
            now=lambda: NOW,
            sink=AsyncMock(),
        )
    )
    await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    scheduler.fail_closed.assert_awaited_once_with(now=NOW)
    leases.release.assert_awaited_once()
