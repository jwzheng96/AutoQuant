from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from threading import Event
from unittest.mock import AsyncMock

import pytest
from pydantic import SecretStr

from autoquant.errors import (
    PersistenceUnavailableError,
    QmtSessionLeaseLostError,
)
from autoquant.execution.models import ZERO_HASH
from autoquant.execution.qmt_lease_guard import (
    QmtSessionLeaseGuard,
    run_fenced_blocking,
)
from autoquant.execution.qmt_session_store import QmtSessionLease

NOW = datetime(2026, 7, 23, 1, tzinfo=UTC)
TOKEN = SecretStr("qmt-readonly-lease-guard-token-value")


def _lease(*, heartbeat_at: datetime = NOW) -> QmtSessionLease:
    return QmtSessionLease(
        session_id=20260723,
        holder_id="windows-qmt-readonly-01",
        token_hash="a" * 64,
        acquired_at=NOW,
        heartbeat_at=heartbeat_at,
        expires_at=heartbeat_at + timedelta(seconds=30),
        released_at=None,
        generation=3,
        version=4,
        event_sequence=3,
        last_event_hash=ZERO_HASH,
    )


def _guard(repository: AsyncMock) -> QmtSessionLeaseGuard:
    return QmtSessionLeaseGuard(
        repository=repository,
        session_id=20260723,
        holder_id="windows-qmt-readonly-01",
        token=TOKEN,
        ttl=timedelta(seconds=30),
        renewal_interval=timedelta(milliseconds=1),
        now=lambda: NOW,
    )


async def _wait_for_renewal(repository: AsyncMock) -> None:
    for _ in range(100):
        if repository.renew.await_count:
            return
        await asyncio.sleep(0.001)
    raise AssertionError("QMT lease renewal did not run")


@pytest.mark.asyncio
async def test_qmt_lease_guard_renews_verifies_and_releases_slow_work() -> None:
    repository = AsyncMock()
    repository.acquire.return_value = _lease()
    repository.renew.return_value = _lease(heartbeat_at=NOW + timedelta(seconds=1))
    repository.verify_owner.return_value = repository.renew.return_value
    repository.release.return_value = repository.renew.return_value
    guard = _guard(repository)

    acquired = await guard.start()
    await _wait_for_renewal(repository)
    verified = await guard.verify()
    released = await guard.close()

    assert acquired.generation == verified.generation == 3
    assert released == verified
    repository.acquire.assert_awaited_once()
    repository.renew.assert_awaited()
    repository.verify_owner.assert_awaited_once()
    repository.release.assert_awaited_once()
    assert await guard.close() is None


@pytest.mark.asyncio
async def test_qmt_lease_guard_reports_renewal_loss_after_attempting_release() -> None:
    repository = AsyncMock()
    repository.acquire.return_value = _lease()
    repository.renew.side_effect = PersistenceUnavailableError("database lost")
    repository.release.return_value = _lease()
    guard = _guard(repository)
    await guard.start()
    await _wait_for_renewal(repository)

    with pytest.raises(QmtSessionLeaseLostError, match="renewal failed"):
        await guard.verify()
    with pytest.raises(QmtSessionLeaseLostError, match="renewal failed"):
        await guard.close()

    repository.verify_owner.assert_not_awaited()
    repository.release.assert_awaited_once()


@pytest.mark.asyncio
async def test_qmt_lease_guard_does_not_release_after_failed_acquire() -> None:
    repository = AsyncMock()
    repository.acquire.side_effect = PersistenceUnavailableError("acquire failed")
    guard = _guard(repository)

    with pytest.raises(PersistenceUnavailableError, match="acquire"):
        await guard.start()

    assert await guard.close() is None
    repository.release.assert_not_awaited()


def test_qmt_lease_guard_requires_renewal_before_expiry() -> None:
    with pytest.raises(ValueError, match="within"):
        QmtSessionLeaseGuard(
            repository=AsyncMock(),
            session_id=1,
            holder_id="holder",
            token=TOKEN,
            ttl=timedelta(seconds=5),
            renewal_interval=timedelta(seconds=5),
            now=lambda: NOW,
        )


@pytest.mark.asyncio
async def test_fenced_blocking_delays_cancellation_until_worker_closes() -> None:
    started = Event()
    allow_close = Event()
    closed = Event()

    def blocking_session() -> None:
        started.set()
        allow_close.wait(timeout=5)
        closed.set()

    task = asyncio.create_task(run_fenced_blocking(blocking_session))
    for _ in range(100):
        if started.is_set():
            break
        await asyncio.sleep(0.001)
    assert started.is_set()
    task.cancel()
    await asyncio.sleep(0.01)

    assert not task.done()
    assert not closed.is_set()
    allow_close.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()
