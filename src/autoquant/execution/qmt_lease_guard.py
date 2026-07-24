from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Protocol, TypeVar

from pydantic import SecretStr

from autoquant.errors import QmtSessionLeaseLostError
from autoquant.execution.qmt_session_store import QmtSessionLease

T = TypeVar("T")


async def run_fenced_blocking(operation: Callable[[], T]) -> T:
    """Delay cancellation until a broker worker has returned and closed its session."""

    task = asyncio.create_task(asyncio.to_thread(operation))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await asyncio.gather(task, return_exceptions=True)
        raise


class QmtLeaseRepository(Protocol):
    async def acquire(
        self,
        *,
        session_id: int,
        holder_id: str,
        token: SecretStr,
        now: datetime,
        ttl: timedelta,
    ) -> QmtSessionLease: ...

    async def renew(
        self,
        *,
        session_id: int,
        holder_id: str,
        token: SecretStr,
        now: datetime,
        ttl: timedelta,
    ) -> QmtSessionLease: ...

    async def verify_owner(
        self,
        *,
        session_id: int,
        holder_id: str,
        token: SecretStr,
        now: datetime,
    ) -> QmtSessionLease: ...

    async def release(
        self,
        *,
        session_id: int,
        holder_id: str,
        token: SecretStr,
        now: datetime,
    ) -> QmtSessionLease: ...


class QmtSessionLeaseGuard:
    """Keep one XtTrader session fenced throughout blocking read-only work."""

    def __init__(
        self,
        *,
        repository: QmtLeaseRepository,
        session_id: int,
        holder_id: str,
        token: SecretStr,
        ttl: timedelta,
        now: Callable[[], datetime],
        renewal_interval: timedelta | None = None,
    ) -> None:
        interval = ttl / 3 if renewal_interval is None else renewal_interval
        if ttl <= timedelta(0):
            raise ValueError("QMT lease guard ttl must be positive")
        if interval <= timedelta(0) or interval >= ttl:
            raise ValueError("QMT lease renewal interval must be within its ttl")
        self._repository = repository
        self._session_id = session_id
        self._holder_id = holder_id
        self._token = token
        self._ttl = ttl
        self._now = now
        self._renewal_interval = interval
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._lease: QmtSessionLease | None = None
        self._renewal_failure: BaseException | None = None

    async def start(self) -> QmtSessionLease:
        if self._task is not None or self._lease is not None:
            raise RuntimeError("QMT session lease guard is already started")
        self._stop = asyncio.Event()
        self._renewal_failure = None
        lease = await self._repository.acquire(
            session_id=self._session_id,
            holder_id=self._holder_id,
            token=self._token,
            now=self._now(),
            ttl=self._ttl,
        )
        self._lease = lease
        self._task = asyncio.create_task(self._renew())
        return lease

    async def verify(self) -> QmtSessionLease:
        if self._task is None or self._lease is None:
            raise RuntimeError("QMT session lease guard is not started")
        self._raise_renewal_failure()
        lease = await self._repository.verify_owner(
            session_id=self._session_id,
            holder_id=self._holder_id,
            token=self._token,
            now=self._now(),
        )
        self._lease = lease
        self._raise_renewal_failure()
        return lease

    async def close(self) -> QmtSessionLease | None:
        task = self._task
        if task is None:
            return None
        self._task = None
        self._stop.set()
        task_result = await asyncio.gather(task, return_exceptions=True)
        failure = self._renewal_failure
        if failure is None and task_result and isinstance(task_result[0], BaseException):
            failure = task_result[0]
        released: QmtSessionLease | None = None
        try:
            released = await self._repository.release(
                session_id=self._session_id,
                holder_id=self._holder_id,
                token=self._token,
                now=self._now(),
            )
        finally:
            self._lease = None
        if failure is not None:
            raise QmtSessionLeaseLostError(
                "QMT session lease renewal failed during read-only work"
            ) from failure
        return released

    async def _renew(self) -> None:
        while True:
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self._renewal_interval.total_seconds(),
                )
                return
            except TimeoutError:
                pass
            try:
                self._lease = await self._repository.renew(
                    session_id=self._session_id,
                    holder_id=self._holder_id,
                    token=self._token,
                    now=self._now(),
                    ttl=self._ttl,
                )
            except BaseException as error:
                self._renewal_failure = error
                return

    def _raise_renewal_failure(self) -> None:
        if self._renewal_failure is not None:
            raise QmtSessionLeaseLostError(
                "QMT session lease renewal failed during read-only work"
            ) from self._renewal_failure
