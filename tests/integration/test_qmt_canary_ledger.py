from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.backtest.models import OrderSide
from autoquant.errors import BrokerStateUnknownError, QmtSessionLeaseLostError
from autoquant.execution.qmt_canary_contract import QmtCanaryOrderCandidate
from autoquant.execution.qmt_canary_store import PostgresQmtCanaryOrderLedger
from autoquant.execution.qmt_session_store import (
    PostgresQmtSessionLeaseRepository,
)
from autoquant.risk.models import (
    ExecutionMode,
    ProposedOrder,
    RiskDecision,
    RiskDecisionState,
)

POSTGRES_DSN = os.environ.get("AQ_POSTGRES_DSN", "").strip()
NOW = datetime(2026, 7, 23, 1, tzinfo=UTC)
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not POSTGRES_DSN,
        reason="AQ_POSTGRES_DSN is not configured; PostgreSQL infrastructure unavailable",
    ),
]


def _candidate(
    *,
    client_order_id: str = "canary-order-0001",
    qmt_lease_generation: int = 1,
    created_at: datetime = NOW,
) -> QmtCanaryOrderCandidate:
    order = ProposedOrder(
        client_order_id=client_order_id,
        instrument="600000.XSHG",
        side=OrderSide.BUY,
        quantity=100,
        submitted_at=created_at - timedelta(seconds=1),
        limit_price=Decimal("10"),
    )
    decision = RiskDecision(
        account_id="canary-account",
        mode=ExecutionMode.LIVE,
        order=order,
        evaluated_at=created_at - timedelta(seconds=1),
        state=RiskDecisionState.ACCEPTED,
        violations=(),
        policy_hash="b" * 64,
        account_state_hash="c" * 64,
        quote_hash="d" * 64,
        rules_version="canary-risk-v1",
        estimated_price=Decimal("10"),
        order_notional=Decimal("1000"),
        projected_cash=Decimal("99000"),
        projected_gross_exposure=Decimal("0.01"),
        projected_position_weight=Decimal("0.01"),
        projected_daily_turnover=Decimal("0.01"),
    )
    return QmtCanaryOrderCandidate(
        account_id="canary-account",
        strategy_id="low-volatility-v5",
        gateway_holder_id="windows-qmt-canary-01",
        qmt_session_id=20260723,
        qmt_lease_generation=qmt_lease_generation,
        decision=decision,
        promotion_report_hash="a" * 64,
        compliance_approval_hash="e" * 64,
        qmt_acceptance_hash="f" * 64,
        reconciliation_report_hash="1" * 64,
        maximum_order_notional=Decimal("2000"),
        created_at=created_at,
        valid_until=created_at + timedelta(seconds=30),
    )


@pytest_asyncio.fixture
async def ledger_fixture() -> AsyncIterator[
    tuple[
        PostgresQmtCanaryOrderLedger,
        PostgresQmtSessionLeaseRepository,
        SecretStr,
        AsyncEngine,
        str,
    ]
]:
    schema = f"autoquant_test_{uuid4().hex}"
    control = PostgresControlRepository.connect(
        dsn=POSTGRES_DSN,
        schema=schema,
    )
    ledger = PostgresQmtCanaryOrderLedger.connect(
        dsn=POSTGRES_DSN,
        schema=schema,
    )
    leases = PostgresQmtSessionLeaseRepository.connect(
        dsn=POSTGRES_DSN,
        schema=schema,
    )
    lease_token = SecretStr("qmt-canary-integration-token-value")
    engine = create_async_engine(POSTGRES_DSN, pool_pre_ping=True)
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/012_qmt_session_leases.sql",
            "migrations/postgres/037_qmt_canary_order_ledger.sql",
        )
    )
    try:
        await control.initialize(migration)
        await leases.acquire(
            session_id=20260723,
            holder_id="windows-qmt-canary-01",
            token=lease_token,
            now=NOW - timedelta(seconds=2),
            ttl=timedelta(minutes=2),
        )
        yield ledger, leases, lease_token, engine, schema
    finally:
        try:
            await ledger.close()
        finally:
            try:
                await leases.close()
            finally:
                try:
                    await engine.dispose()
                finally:
                    try:
                        await control.drop_test_schema()
                    finally:
                        await control.close()


@pytest.mark.asyncio
async def test_qmt_canary_ledger_is_idempotent_and_restart_recoverable(
    ledger_fixture: tuple[
        PostgresQmtCanaryOrderLedger,
        PostgresQmtSessionLeaseRepository,
        SecretStr,
        AsyncEngine,
        str,
    ],
) -> None:
    ledger, leases, lease_token, _, schema = ledger_fixture
    candidate = _candidate()
    reserved = await ledger.reserve(
        candidate,
        async_request_id=17,
        reserved_at=NOW + timedelta(seconds=1),
    )
    repeated = await ledger.reserve(
        candidate,
        async_request_id=17,
        reserved_at=NOW + timedelta(seconds=1),
    )
    bound = await ledger.bind(
        gateway_holder_id=candidate.gateway_holder_id,
        qmt_session_id=candidate.qmt_session_id,
        qmt_lease_generation=candidate.qmt_lease_generation,
        async_request_id=17,
        broker_order_id="88001",
        bound_at=NOW + timedelta(seconds=2),
    )
    repeated_binding = await ledger.bind(
        gateway_holder_id=candidate.gateway_holder_id,
        qmt_session_id=candidate.qmt_session_id,
        qmt_lease_generation=candidate.qmt_lease_generation,
        async_request_id=17,
        broker_order_id="88001",
        bound_at=NOW + timedelta(seconds=2),
    )
    await leases.release(
        session_id=candidate.qmt_session_id,
        holder_id=candidate.gateway_holder_id,
        token=lease_token,
        now=NOW + timedelta(seconds=3),
    )
    next_lease = await leases.acquire(
        session_id=candidate.qmt_session_id,
        holder_id=candidate.gateway_holder_id,
        token=lease_token,
        now=NOW + timedelta(seconds=4),
        ttl=timedelta(minutes=2),
    )
    next_generation = _candidate(
        client_order_id="canary-order-0002",
        qmt_lease_generation=next_lease.generation,
        created_at=NOW + timedelta(seconds=4),
    )
    await ledger.reserve(
        next_generation,
        async_request_id=17,
        reserved_at=NOW + timedelta(seconds=5),
    )
    await ledger.bind(
        gateway_holder_id=next_generation.gateway_holder_id,
        qmt_session_id=next_generation.qmt_session_id,
        qmt_lease_generation=next_generation.qmt_lease_generation,
        async_request_id=17,
        broker_order_id="88001",
        bound_at=NOW + timedelta(seconds=6),
    )
    restarted = PostgresQmtCanaryOrderLedger.connect(
        dsn=POSTGRES_DSN,
        schema=schema,
    )
    try:
        restored = await restarted.restore_book(
            account_id=candidate.account_id,
            gateway_holder_id=candidate.gateway_holder_id,
            qmt_session_id=candidate.qmt_session_id,
            qmt_lease_generation=candidate.qmt_lease_generation,
        )
        await restarted.check_connection()
        next_restored = await restarted.restore_book(
            account_id=next_generation.account_id,
            gateway_holder_id=next_generation.gateway_holder_id,
            qmt_session_id=next_generation.qmt_session_id,
            qmt_lease_generation=next_generation.qmt_lease_generation,
        )
    finally:
        await restarted.close()

    assert reserved == repeated
    assert bound == repeated_binding
    assert restored.broker_mapping() == {88001: candidate.decision.order.client_order_id}
    assert next_restored.broker_mapping() == {88001: next_generation.decision.order.client_order_id}


@pytest.mark.asyncio
async def test_qmt_canary_ledger_rejects_conflicts_and_mutation(
    ledger_fixture: tuple[
        PostgresQmtCanaryOrderLedger,
        PostgresQmtSessionLeaseRepository,
        SecretStr,
        AsyncEngine,
        str,
    ],
) -> None:
    ledger, _, _, engine, schema = ledger_fixture
    candidate = _candidate()
    await ledger.reserve(
        candidate,
        async_request_id=17,
        reserved_at=NOW + timedelta(seconds=1),
    )

    with pytest.raises(ValueError, match="identity conflicts"):
        await ledger.reserve(
            _candidate(client_order_id="canary-order-0002"),
            async_request_id=17,
            reserved_at=NOW + timedelta(seconds=1),
        )
    with pytest.raises(QmtSessionLeaseLostError, match="matching active"):
        await ledger.reserve(
            _candidate(
                client_order_id="canary-order-wrong-generation",
                qmt_lease_generation=99,
            ),
            async_request_id=18,
            reserved_at=NOW + timedelta(seconds=1),
        )
    with pytest.raises(BrokerStateUnknownError, match="no durable"):
        await ledger.bind(
            gateway_holder_id=candidate.gateway_holder_id,
            qmt_session_id=candidate.qmt_session_id,
            qmt_lease_generation=candidate.qmt_lease_generation,
            async_request_id=99,
            broker_order_id="88099",
            bound_at=NOW + timedelta(seconds=2),
        )
    await ledger.bind(
        gateway_holder_id=candidate.gateway_holder_id,
        qmt_session_id=candidate.qmt_session_id,
        qmt_lease_generation=candidate.qmt_lease_generation,
        async_request_id=17,
        broker_order_id="88001",
        bound_at=NOW + timedelta(seconds=2),
    )
    with pytest.raises(BrokerStateUnknownError, match="conflicts"):
        await ledger.bind(
            gateway_holder_id=candidate.gateway_holder_id,
            qmt_session_id=candidate.qmt_session_id,
            qmt_lease_generation=candidate.qmt_lease_generation,
            async_request_id=17,
            broker_order_id="88002",
            bound_at=NOW + timedelta(seconds=2),
        )

    for table in (
        "qmt_canary_order_candidates",
        "qmt_order_correlation_reservations",
        "qmt_order_correlation_bindings",
    ):
        with pytest.raises(SQLAlchemyError):
            async with engine.begin() as connection:
                await connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
                await connection.execute(text(f"DELETE FROM {table}"))
