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
from autoquant.errors import QmtSessionConflictError, QmtSessionLeaseLostError
from autoquant.execution.qmt_session_store import PostgresQmtSessionLeaseRepository

POSTGRES_DSN = os.environ.get("AQ_POSTGRES_DSN", "").strip()
NOW = datetime(2026, 7, 23, 5, tzinfo=UTC)
TOKEN_ONE = SecretStr("first-qmt-lease-token-with-32-characters")
TOKEN_TWO = SecretStr("second-qmt-lease-token-with-32-characters")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not POSTGRES_DSN,
        reason="AQ_POSTGRES_DSN is not configured; PostgreSQL infrastructure unavailable",
    ),
]


@pytest_asyncio.fixture
async def leases() -> AsyncIterator[tuple[PostgresQmtSessionLeaseRepository, AsyncEngine, str]]:
    schema = f"autoquant_test_{uuid4().hex}"
    engine = create_async_engine(POSTGRES_DSN, pool_pre_ping=True)
    control_schema = PostgresControlRepository(engine=engine, schema=schema)
    repository = PostgresQmtSessionLeaseRepository(engine=engine, schema=schema)
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/012_qmt_session_leases.sql",
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
async def test_session_lease_is_fenced_renewable_releasable_and_reacquirable(
    leases: tuple[PostgresQmtSessionLeaseRepository, AsyncEngine, str],
) -> None:
    repository, engine, schema = leases
    acquired = await repository.acquire(
        session_id=731001,
        holder_id="gateway-a",
        token=TOKEN_ONE,
        now=NOW,
        ttl=timedelta(seconds=30),
    )

    assert acquired.generation == 1
    assert acquired.version == 1
    assert acquired.event_sequence == 1
    assert acquired.active_at(NOW)
    assert acquired.token_hash != TOKEN_ONE.get_secret_value()
    assert await repository.active_session_ids(now=NOW) == (731001,)
    assert (
        await repository.verify_owner(
            session_id=731001,
            holder_id="gateway-a",
            token=TOKEN_ONE,
            now=NOW + timedelta(seconds=1),
        )
    ) == acquired
    with pytest.raises(QmtSessionLeaseLostError):
        await repository.verify_owner(
            session_id=731001,
            holder_id="gateway-a",
            token=TOKEN_TWO,
            now=NOW + timedelta(seconds=1),
        )

    renewed = await repository.acquire(
        session_id=731001,
        holder_id="gateway-a",
        token=TOKEN_ONE,
        now=NOW + timedelta(seconds=5),
        ttl=timedelta(seconds=30),
    )
    assert renewed.generation == 1
    assert renewed.version == 2
    assert renewed.event_sequence == 1

    with pytest.raises(QmtSessionConflictError):
        await repository.acquire(
            session_id=731001,
            holder_id="gateway-b",
            token=TOKEN_TWO,
            now=NOW + timedelta(seconds=6),
            ttl=timedelta(seconds=30),
        )
    with pytest.raises(QmtSessionLeaseLostError):
        await repository.renew(
            session_id=731001,
            holder_id="gateway-a",
            token=TOKEN_TWO,
            now=NOW + timedelta(seconds=7),
            ttl=timedelta(seconds=30),
        )

    released = await repository.release(
        session_id=731001,
        holder_id="gateway-a",
        token=TOKEN_ONE,
        now=NOW + timedelta(seconds=10),
    )
    assert released.released_at == NOW + timedelta(seconds=10)
    assert released.event_sequence == 2
    assert await repository.active_session_ids(now=NOW + timedelta(seconds=10)) == ()

    reacquired = await repository.acquire(
        session_id=731001,
        holder_id="gateway-b",
        token=TOKEN_TWO,
        now=NOW + timedelta(seconds=11),
        ttl=timedelta(seconds=30),
    )
    assert reacquired.generation == 2
    assert reacquired.event_sequence == 3
    assert reacquired.holder_id == "gateway-b"

    async with engine.connect() as connection:
        payloads = (
            await connection.scalars(
                text(
                    f"SELECT event_payload::text FROM {schema}.qmt_session_lease_events "
                    "ORDER BY sequence"
                )
            )
        ).all()
    assert len(payloads) == 3
    assert all(TOKEN_ONE.get_secret_value() not in str(payload) for payload in payloads)
    assert all(TOKEN_TWO.get_secret_value() not in str(payload) for payload in payloads)


@pytest.mark.asyncio
async def test_concurrent_session_acquisition_has_exactly_one_winner(
    leases: tuple[PostgresQmtSessionLeaseRepository, AsyncEngine, str],
) -> None:
    repository, _, _ = leases

    results = await asyncio.gather(
        repository.acquire(
            session_id=731002,
            holder_id="gateway-a",
            token=TOKEN_ONE,
            now=NOW,
            ttl=timedelta(seconds=30),
        ),
        repository.acquire(
            session_id=731002,
            holder_id="gateway-b",
            token=TOKEN_TWO,
            now=NOW,
            ttl=timedelta(seconds=30),
        ),
        return_exceptions=True,
    )

    assert sum(not isinstance(result, BaseException) for result in results) == 1
    assert sum(isinstance(result, QmtSessionConflictError) for result in results) == 1


@pytest.mark.asyncio
async def test_expired_lease_can_be_reacquired_but_cannot_be_renewed(
    leases: tuple[PostgresQmtSessionLeaseRepository, AsyncEngine, str],
) -> None:
    repository, _, _ = leases
    await repository.acquire(
        session_id=731003,
        holder_id="gateway-a",
        token=TOKEN_ONE,
        now=NOW,
        ttl=timedelta(seconds=5),
    )

    with pytest.raises(QmtSessionLeaseLostError):
        await repository.renew(
            session_id=731003,
            holder_id="gateway-a",
            token=TOKEN_ONE,
            now=NOW + timedelta(seconds=5),
            ttl=timedelta(seconds=5),
        )

    reacquired = await repository.acquire(
        session_id=731003,
        holder_id="gateway-b",
        token=TOKEN_TWO,
        now=NOW + timedelta(seconds=5),
        ttl=timedelta(seconds=5),
    )
    assert reacquired.generation == 2
    assert reacquired.holder_id == "gateway-b"


@pytest.mark.asyncio
async def test_session_lease_event_history_is_immutable(
    leases: tuple[PostgresQmtSessionLeaseRepository, AsyncEngine, str],
) -> None:
    repository, engine, schema = leases
    acquired = await repository.acquire(
        session_id=731004,
        holder_id="gateway-a",
        token=TOKEN_ONE,
        now=NOW,
        ttl=timedelta(seconds=30),
    )

    with pytest.raises(DBAPIError):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"UPDATE {schema}.qmt_session_lease_events "
                    "SET holder_id = 'tampered' WHERE event_hash = :event_hash"
                ),
                {"event_hash": acquired.last_event_hash},
            )
