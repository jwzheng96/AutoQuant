from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.errors import BrokerStateUnknownError, QmtSessionLeaseLostError
from autoquant.execution.qmt_callback_coordinator import (
    QmtCallbackPersistenceCoordinator,
)
from autoquant.execution.qmt_callback_inbox import (
    QmtSanitizedCallback,
    sanitize_qmt_callback,
)
from autoquant.execution.qmt_callback_store import PostgresQmtCallbackInbox
from autoquant.execution.qmt_gateway import QmtCallbackBuffer, QmtCallbackKind
from autoquant.execution.qmt_session_store import (
    PostgresQmtSessionLeaseRepository,
)

POSTGRES_DSN = os.environ.get("AQ_POSTGRES_DSN", "").strip()
BROKER_ACCOUNT = "sensitive-broker-account"
LOGICAL_ACCOUNT = "paper-main"
HOLDER_ID = "windows-qmt-canary-01"
SESSION_ID = 20260724
LEASE_TOKEN = SecretStr("qmt-callback-integration-token-value")
WRONG_TOKEN = SecretStr("wrong-qmt-callback-token-value-000")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not POSTGRES_DSN,
        reason=("AQ_POSTGRES_DSN is not configured; PostgreSQL infrastructure unavailable"),
    ),
]


@pytest_asyncio.fixture
async def callback_fixture() -> AsyncIterator[
    tuple[
        PostgresQmtCallbackInbox,
        PostgresQmtSessionLeaseRepository,
        AsyncEngine,
        str,
        int,
    ]
]:
    schema = f"autoquant_test_{uuid4().hex}"
    engine = create_async_engine(POSTGRES_DSN, pool_pre_ping=True)
    control = PostgresControlRepository(engine=engine, schema=schema)
    inbox = PostgresQmtCallbackInbox(engine=engine, schema=schema)
    leases = PostgresQmtSessionLeaseRepository(engine=engine, schema=schema)
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/012_qmt_session_leases.sql",
            "migrations/postgres/037_qmt_canary_order_ledger.sql",
            "migrations/postgres/038_qmt_canary_order_staging.sql",
            "migrations/postgres/039_qmt_canary_remark_recovery.sql",
            "migrations/postgres/040_qmt_callback_inbox.sql",
            "migrations/postgres/041_qmt_callback_persistence_receipts.sql",
        )
    )
    try:
        await control.initialize(migration)
        lease = await leases.acquire(
            session_id=SESSION_ID,
            holder_id=HOLDER_ID,
            token=LEASE_TOKEN,
            now=datetime.now(UTC) - timedelta(seconds=10),
            ttl=timedelta(minutes=2),
        )
        yield inbox, leases, engine, schema, lease.generation
    finally:
        try:
            await control.drop_test_schema()
        finally:
            await engine.dispose()


def _capture_callbacks() -> tuple[QmtSanitizedCallback, QmtSanitizedCallback]:
    buffer = QmtCallbackBuffer()
    first = buffer.capture(
        QmtCallbackKind.ORDER,
        {
            "account_id": BROKER_ACCOUNT,
            "order_id": 88001,
            "order_remark": "AQ1234567890abcdef123456",
            "order_status": 50,
            "order_volume": 100,
            "price": 10.5,
            "side": "buy",
            "status_msg": "sensitive broker rejection details",
            "stock_code": "600000.SH",
            "traded_price": 0.0,
            "traded_volume": 0,
        },
        received_at=datetime.now(UTC),
    )
    second = buffer.capture(
        QmtCallbackKind.ACCOUNT_STATUS,
        {"account_id": BROKER_ACCOUNT, "status": 0},
        received_at=datetime.now(UTC),
    )
    return (
        sanitize_qmt_callback(
            first,
            expected_broker_account_id=BROKER_ACCOUNT,
            logical_account_id=LOGICAL_ACCOUNT,
        ),
        sanitize_qmt_callback(
            second,
            expected_broker_account_id=BROKER_ACCOUNT,
            logical_account_id=LOGICAL_ACCOUNT,
        ),
    )


def _coordinator(
    *,
    buffer: QmtCallbackBuffer,
    inbox: PostgresQmtCallbackInbox,
    generation: int,
) -> QmtCallbackPersistenceCoordinator:
    return QmtCallbackPersistenceCoordinator(
        buffer=buffer,
        inbox=inbox,
        expected_broker_account_id=BROKER_ACCOUNT,
        logical_account_id=LOGICAL_ACCOUNT,
        gateway_holder_id=HOLDER_ID,
        qmt_session_id=SESSION_ID,
        qmt_lease_generation=generation,
    )


@pytest.mark.asyncio
async def test_callback_coordinator_retries_same_batch_after_lease_failure(
    callback_fixture: tuple[
        PostgresQmtCallbackInbox,
        PostgresQmtSessionLeaseRepository,
        AsyncEngine,
        str,
        int,
    ],
) -> None:
    inbox, _, _, _, generation = callback_fixture
    buffer = QmtCallbackBuffer()
    envelope = buffer.capture(
        QmtCallbackKind.ACCOUNT_STATUS,
        {"account_id": BROKER_ACCOUNT, "status": 0},
        received_at=datetime.now(UTC),
    )
    coordinator = _coordinator(
        buffer=buffer,
        inbox=inbox,
        generation=generation,
    )

    with pytest.raises(QmtSessionLeaseLostError):
        await coordinator.persist_and_process(lease_token=WRONG_TOKEN)
    result = await coordinator.persist_and_process(lease_token=LEASE_TOKEN)
    assert len(result.persisted_events) == 1
    assert result.persisted_events[0].callback.local_sequence == (
        envelope.local_sequence
    )
    assert result.async_bindings == ()
    assert result.broker_mutation_allowed is False
    restarted_buffer = QmtCallbackBuffer()
    restarted = _coordinator(
        buffer=restarted_buffer,
        inbox=inbox,
        generation=generation,
    )
    assert await restarted.restore_before_capture(
        lease_token=LEASE_TOKEN,
    ) == result
    assert restarted_buffer.cursor == envelope.local_sequence
    resumed = restarted_buffer.capture(
        QmtCallbackKind.ACCOUNT_STATUS,
        {"account_id": BROKER_ACCOUNT, "status": 0},
        received_at=datetime.now(UTC),
    )
    assert resumed.local_sequence == 2
    resumed_result = await restarted.persist_and_process(
        lease_token=LEASE_TOKEN,
    )
    assert [
        event.callback.local_sequence
        for event in resumed_result.persisted_events
    ] == [2]
    empty = await restarted.persist_and_process(lease_token=LEASE_TOKEN)
    assert empty.persisted_events == ()
    assert empty.async_bindings == ()


@pytest.mark.asyncio
async def test_callback_coordinator_retains_partially_persisted_batch_on_failure(
    callback_fixture: tuple[
        PostgresQmtCallbackInbox,
        PostgresQmtSessionLeaseRepository,
        AsyncEngine,
        str,
        int,
    ],
) -> None:
    inbox, _, _, _, generation = callback_fixture
    buffer = QmtCallbackBuffer()
    first = buffer.capture(
        QmtCallbackKind.ACCOUNT_STATUS,
        {"account_id": BROKER_ACCOUNT, "status": 0},
        received_at=datetime.now(UTC),
    )
    second = buffer.capture(
        QmtCallbackKind.DISCONNECTED,
        {"reason": "xttrader_disconnected"},
        received_at=datetime.now(UTC) - timedelta(seconds=6),
    )
    coordinator = _coordinator(
        buffer=buffer,
        inbox=inbox,
        generation=generation,
    )

    with pytest.raises(BrokerStateUnknownError, match="within five seconds"):
        await coordinator.persist_and_process(lease_token=LEASE_TOKEN)
    durable = await inbox.replay_current(
        account_id=LOGICAL_ACCOUNT,
        gateway_holder_id=HOLDER_ID,
        qmt_session_id=SESSION_ID,
        qmt_lease_generation=generation,
        lease_token=LEASE_TOKEN,
    )
    assert len(durable) == 1
    assert durable[0].callback.local_sequence == first.local_sequence

    retried = buffer.reserve_durable()
    assert retried is not None
    assert retried.events == (first, second)
    buffer.release_durable(reservation_id=retried.reservation_id)


@pytest.mark.asyncio
async def test_callback_coordinator_serializes_concurrent_consumers(
    callback_fixture: tuple[
        PostgresQmtCallbackInbox,
        PostgresQmtSessionLeaseRepository,
        AsyncEngine,
        str,
        int,
    ],
) -> None:
    inbox, _, _, _, generation = callback_fixture
    buffer = QmtCallbackBuffer()
    for status in (0, 1):
        buffer.capture(
            QmtCallbackKind.ACCOUNT_STATUS,
            {"account_id": BROKER_ACCOUNT, "status": status},
            received_at=datetime.now(UTC),
        )
    coordinator = _coordinator(
        buffer=buffer,
        inbox=inbox,
        generation=generation,
    )

    results = await asyncio.gather(
        coordinator.persist_and_process(
            lease_token=LEASE_TOKEN,
            limit=1,
        ),
        coordinator.persist_and_process(
            lease_token=LEASE_TOKEN,
            limit=1,
        ),
    )
    assert [
        event.callback.local_sequence
        for result in results
        for event in result.persisted_events
    ] == [1, 2]
    replayed = await coordinator.replay_persisted(
        lease_token=LEASE_TOKEN,
    )
    assert [
        event.callback.local_sequence for event in replayed.persisted_events
    ] == [1, 2]


@pytest.mark.asyncio
async def test_callback_inbox_is_redacted_idempotent_replayable_and_immutable(
    callback_fixture: tuple[
        PostgresQmtCallbackInbox,
        PostgresQmtSessionLeaseRepository,
        AsyncEngine,
        str,
        int,
    ],
) -> None:
    inbox, _, engine, schema, generation = callback_fixture
    first, second = _capture_callbacks()

    await inbox.check_connection()
    stored_first = await inbox.append(
        first,
        gateway_holder_id=HOLDER_ID,
        qmt_session_id=SESSION_ID,
        qmt_lease_generation=generation,
        lease_token=LEASE_TOKEN,
    )
    assert (
        await inbox.append(
            first,
            gateway_holder_id=HOLDER_ID,
            qmt_session_id=SESSION_ID,
            qmt_lease_generation=generation,
            lease_token=LEASE_TOKEN,
        )
        == stored_first
    )
    stored_second = await inbox.append(
        second,
        gateway_holder_id=HOLDER_ID,
        qmt_session_id=SESSION_ID,
        qmt_lease_generation=generation,
        lease_token=LEASE_TOKEN,
    )
    assert await inbox.replay_current(
        account_id=LOGICAL_ACCOUNT,
        gateway_holder_id=HOLDER_ID,
        qmt_session_id=SESSION_ID,
        qmt_lease_generation=generation,
        lease_token=LEASE_TOKEN,
    ) == (stored_first, stored_second)
    receipt = await inbox.persistence_receipt(
        stored_first,
        lease_token=LEASE_TOKEN,
    )
    assert receipt.event == stored_first
    assert (
        receipt.persisted_at - receipt.event.callback.received_at
        <= timedelta(seconds=5)
    )

    async with engine.connect() as connection:
        serialized = await connection.scalar(
            text(
                f"""
                SELECT string_agg(
                    e.event_payload::text ||
                    e.redacted_payload::text ||
                    r.receipt_payload::text,
                    ''
                )
                FROM {schema}.qmt_callback_inbox_events e
                JOIN {schema}.qmt_callback_persistence_receipts r
                  ON r.event_hash = e.event_hash
                """
            )
        )
    assert isinstance(serialized, str)
    assert BROKER_ACCOUNT not in serialized
    assert "sensitive broker rejection details" not in serialized
    assert "status_msg" not in serialized

    with pytest.raises(SQLAlchemyError):
        async with engine.begin() as connection:
            await connection.execute(text(f"DELETE FROM {schema}.qmt_callback_inbox_events"))
    with pytest.raises(SQLAlchemyError):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"DELETE FROM {schema}.qmt_callback_persistence_receipts"
                )
            )


@pytest.mark.asyncio
async def test_callback_inbox_fails_closed_on_conflict_gap_or_lost_lease(
    callback_fixture: tuple[
        PostgresQmtCallbackInbox,
        PostgresQmtSessionLeaseRepository,
        AsyncEngine,
        str,
        int,
    ],
) -> None:
    inbox, leases, _, _, generation = callback_fixture
    first, second = _capture_callbacks()
    await inbox.append(
        first,
        gateway_holder_id=HOLDER_ID,
        qmt_session_id=SESSION_ID,
        qmt_lease_generation=generation,
        lease_token=LEASE_TOKEN,
    )

    with pytest.raises(BrokerStateUnknownError, match="conflicts"):
        await inbox.append(
            replace(
                first,
                redacted_payload={
                    **dict(first.redacted_payload),
                    "order_status": 54,
                },
            ),
            gateway_holder_id=HOLDER_ID,
            qmt_session_id=SESSION_ID,
            qmt_lease_generation=generation,
            lease_token=LEASE_TOKEN,
        )
    with pytest.raises(BrokerStateUnknownError, match="gap"):
        await inbox.append(
            replace(
                second,
                local_sequence=3,
                received_at=datetime.now(UTC),
            ),
            gateway_holder_id=HOLDER_ID,
            qmt_session_id=SESSION_ID,
            qmt_lease_generation=generation,
            lease_token=LEASE_TOKEN,
        )
    with pytest.raises(QmtSessionLeaseLostError):
        await inbox.replay_current(
            account_id=LOGICAL_ACCOUNT,
            gateway_holder_id=HOLDER_ID,
            qmt_session_id=SESSION_ID,
            qmt_lease_generation=generation,
            lease_token=WRONG_TOKEN,
        )

    await leases.release(
        session_id=SESSION_ID,
        holder_id=HOLDER_ID,
        token=LEASE_TOKEN,
        now=datetime.now(UTC),
    )
    with pytest.raises(QmtSessionLeaseLostError):
        await inbox.append(
            second,
            gateway_holder_id=HOLDER_ID,
            qmt_session_id=SESSION_ID,
            qmt_lease_generation=generation,
            lease_token=LEASE_TOKEN,
        )
