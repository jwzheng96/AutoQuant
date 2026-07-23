from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.errors import (
    PaperSchedulerLeaseConflictError,
    PaperSchedulerLeaseLostError,
)
from autoquant.execution.paper_scheduler_lease_store import (
    PostgresPaperSchedulerLeaseRepository,
)

POSTGRES_DSN = os.environ.get("AQ_POSTGRES_DSN", "").strip()
NOW = datetime(2026, 7, 23, 5, tzinfo=UTC)
TOKEN_ONE = SecretStr("first-scheduler-lease-token-32-characters")
TOKEN_TWO = SecretStr("second-scheduler-lease-token-32-characters")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not POSTGRES_DSN,
        reason="AQ_POSTGRES_DSN is not configured; PostgreSQL infrastructure unavailable",
    ),
]


@pytest_asyncio.fixture
async def leases() -> AsyncIterator[
    tuple[PostgresPaperSchedulerLeaseRepository, AsyncEngine, str]
]:
    schema = f"autoquant_test_{uuid4().hex}"
    engine = create_async_engine(POSTGRES_DSN, pool_pre_ping=True)
    control_schema = PostgresControlRepository(engine=engine, schema=schema)
    repository = PostgresPaperSchedulerLeaseRepository(engine=engine, schema=schema)
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/014_paper_scheduler_leases.sql",
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


@pytest.mark.asyncio
async def test_scheduler_lease_is_fenced_renewable_releasable_and_reacquirable(
    leases: tuple[PostgresPaperSchedulerLeaseRepository, AsyncEngine, str],
) -> None:
    repository, engine, schema = leases
    acquired = await repository.acquire(
        account_id="paper-main",
        strategy_id="strategy-v1",
        holder_id="scheduler-a",
        token=TOKEN_ONE,
        now=NOW,
        ttl=timedelta(seconds=30),
    )
    assert acquired.generation == 1
    assert acquired.event_sequence == 1
    assert acquired.token_hash != TOKEN_ONE.get_secret_value()

    renewed = await repository.renew(
        account_id="paper-main",
        strategy_id="strategy-v1",
        holder_id="scheduler-a",
        token=TOKEN_ONE,
        now=NOW + timedelta(seconds=5),
        ttl=timedelta(seconds=30),
    )
    assert renewed.version == 2
    assert renewed.event_sequence == 1
    assert (
        await repository.verify_owner(
            account_id="paper-main",
            strategy_id="strategy-v1",
            holder_id="scheduler-a",
            token=TOKEN_ONE,
            now=NOW + timedelta(seconds=6),
        )
        == renewed
    )
    with pytest.raises(PaperSchedulerLeaseLostError):
        await repository.verify_owner(
            account_id="paper-main",
            strategy_id="strategy-v1",
            holder_id="scheduler-a",
            token=TOKEN_TWO,
            now=NOW + timedelta(seconds=6),
        )

    with pytest.raises(PaperSchedulerLeaseConflictError):
        await repository.acquire(
            account_id="paper-main",
            strategy_id="strategy-v2",
            holder_id="scheduler-b",
            token=TOKEN_TWO,
            now=NOW + timedelta(seconds=6),
            ttl=timedelta(seconds=30),
        )
    with pytest.raises(PaperSchedulerLeaseLostError):
        await repository.renew(
            account_id="paper-main",
            strategy_id="strategy-v1",
            holder_id="scheduler-a",
            token=TOKEN_TWO,
            now=NOW + timedelta(seconds=7),
            ttl=timedelta(seconds=30),
        )

    released = await repository.release(
        account_id="paper-main",
        strategy_id="strategy-v1",
        holder_id="scheduler-a",
        token=TOKEN_ONE,
        now=NOW + timedelta(seconds=10),
    )
    assert released.event_sequence == 2
    reacquired = await repository.acquire(
        account_id="paper-main",
        strategy_id="strategy-v2",
        holder_id="scheduler-b",
        token=TOKEN_TWO,
        now=NOW + timedelta(seconds=11),
        ttl=timedelta(seconds=30),
    )
    assert reacquired.generation == 2
    assert reacquired.event_sequence == 3

    async with engine.connect() as connection:
        payloads = (
            await connection.scalars(
                text(
                    f"SELECT event_payload::text FROM {schema}."
                    "paper_scheduler_lease_events ORDER BY sequence"
                )
            )
        ).all()
    assert len(payloads) == 3
    assert all(TOKEN_ONE.get_secret_value() not in str(payload) for payload in payloads)
    assert all(TOKEN_TWO.get_secret_value() not in str(payload) for payload in payloads)


@pytest.mark.asyncio
async def test_concurrent_scheduler_acquisition_has_exactly_one_winner(
    leases: tuple[PostgresPaperSchedulerLeaseRepository, AsyncEngine, str],
) -> None:
    repository, _, _ = leases
    results = await asyncio.gather(
        repository.acquire(
            account_id="paper-race",
            strategy_id="strategy-v1",
            holder_id="scheduler-a",
            token=TOKEN_ONE,
            now=NOW,
            ttl=timedelta(seconds=30),
        ),
        repository.acquire(
            account_id="paper-race",
            strategy_id="strategy-v1",
            holder_id="scheduler-b",
            token=TOKEN_TWO,
            now=NOW,
            ttl=timedelta(seconds=30),
        ),
        return_exceptions=True,
    )
    assert sum(not isinstance(result, BaseException) for result in results) == 1
    assert (
        sum(isinstance(result, PaperSchedulerLeaseConflictError) for result in results)
        == 1
    )


@pytest.mark.asyncio
async def test_expired_scheduler_lease_cannot_be_renewed_but_can_be_reacquired(
    leases: tuple[PostgresPaperSchedulerLeaseRepository, AsyncEngine, str],
) -> None:
    repository, _, _ = leases
    await repository.acquire(
        account_id="paper-expired",
        strategy_id="strategy-v1",
        holder_id="scheduler-a",
        token=TOKEN_ONE,
        now=NOW,
        ttl=timedelta(seconds=5),
    )
    with pytest.raises(PaperSchedulerLeaseLostError):
        await repository.renew(
            account_id="paper-expired",
            strategy_id="strategy-v1",
            holder_id="scheduler-a",
            token=TOKEN_ONE,
            now=NOW + timedelta(seconds=5),
            ttl=timedelta(seconds=5),
        )
    reacquired = await repository.acquire(
        account_id="paper-expired",
        strategy_id="strategy-v1",
        holder_id="scheduler-b",
        token=TOKEN_TWO,
        now=NOW + timedelta(seconds=5),
        ttl=timedelta(seconds=5),
    )
    assert reacquired.generation == 2


@pytest.mark.asyncio
async def test_scheduler_lease_event_history_is_immutable(
    leases: tuple[PostgresPaperSchedulerLeaseRepository, AsyncEngine, str],
) -> None:
    repository, engine, schema = leases
    acquired = await repository.acquire(
        account_id="paper-immutable",
        strategy_id="strategy-v1",
        holder_id="scheduler-a",
        token=TOKEN_ONE,
        now=NOW,
        ttl=timedelta(seconds=30),
    )
    with pytest.raises(DBAPIError):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"UPDATE {schema}.paper_scheduler_lease_events "
                    "SET holder_id='tampered' WHERE event_hash=:event_hash"
                ),
                {"event_hash": acquired.last_event_hash},
            )
