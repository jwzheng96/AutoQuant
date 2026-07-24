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
from autoquant.execution.qmt_canary_recovery import QmtCanaryRemarkRecovery
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
    created_at: datetime | None = None,
) -> QmtCanaryOrderCandidate:
    instant = datetime.now(UTC) if created_at is None else created_at
    order = ProposedOrder(
        client_order_id=client_order_id,
        instrument="600000.XSHG",
        side=OrderSide.BUY,
        quantity=100,
        submitted_at=instant - timedelta(seconds=1),
        limit_price=Decimal("10"),
    )
    decision = RiskDecision(
        account_id="canary-account",
        mode=ExecutionMode.LIVE,
        order=order,
        evaluated_at=instant - timedelta(seconds=1),
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
        created_at=instant,
        valid_until=instant + timedelta(seconds=30),
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
            "migrations/postgres/038_qmt_canary_order_staging.sql",
            "migrations/postgres/039_qmt_canary_remark_recovery.sql",
        )
    )
    try:
        await control.initialize(migration)
        await leases.acquire(
            session_id=20260723,
            holder_id="windows-qmt-canary-01",
            token=lease_token,
            now=datetime.now(UTC) - timedelta(seconds=2),
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
    now = datetime.now(UTC)
    candidate = _candidate(created_at=now)
    staged = await ledger.stage(
        candidate,
        lease_token=lease_token,
        staged_at=now,
    )
    repeated_stage = await ledger.stage(
        candidate,
        lease_token=lease_token,
        staged_at=now,
    )
    assert await ledger.unresolved_stages(
        account_id=candidate.account_id,
        gateway_holder_id=candidate.gateway_holder_id,
        qmt_session_id=candidate.qmt_session_id,
        qmt_lease_generation=candidate.qmt_lease_generation,
        lease_token=lease_token,
    ) == (staged,)
    reserved_at = datetime.now(UTC)
    reserved = await ledger.reserve(
        candidate,
        lease_token=lease_token,
        async_request_id=17,
        reserved_at=reserved_at,
    )
    repeated = await ledger.reserve(
        candidate,
        lease_token=lease_token,
        async_request_id=17,
        reserved_at=reserved_at,
    )
    bound_at = datetime.now(UTC)
    bound = await ledger.bind(
        gateway_holder_id=candidate.gateway_holder_id,
        qmt_session_id=candidate.qmt_session_id,
        qmt_lease_generation=candidate.qmt_lease_generation,
        lease_token=lease_token,
        async_request_id=17,
        broker_order_id="88001",
        bound_at=bound_at,
    )
    repeated_binding = await ledger.bind(
        gateway_holder_id=candidate.gateway_holder_id,
        qmt_session_id=candidate.qmt_session_id,
        qmt_lease_generation=candidate.qmt_lease_generation,
        lease_token=lease_token,
        async_request_id=17,
        broker_order_id="88001",
        bound_at=bound_at,
    )
    restarted = PostgresQmtCanaryOrderLedger.connect(
        dsn=POSTGRES_DSN,
        schema=schema,
    )
    restored = await restarted.restore_book(
        account_id=candidate.account_id,
        gateway_holder_id=candidate.gateway_holder_id,
        qmt_session_id=candidate.qmt_session_id,
        qmt_lease_generation=candidate.qmt_lease_generation,
        lease_token=lease_token,
    )
    released_at = datetime.now(UTC)
    await leases.release(
        session_id=candidate.qmt_session_id,
        holder_id=candidate.gateway_holder_id,
        token=lease_token,
        now=released_at,
    )
    reacquired_at = datetime.now(UTC)
    next_lease = await leases.acquire(
        session_id=candidate.qmt_session_id,
        holder_id=candidate.gateway_holder_id,
        token=lease_token,
        now=reacquired_at,
        ttl=timedelta(minutes=2),
    )
    next_created_at = datetime.now(UTC)
    next_generation = _candidate(
        client_order_id="canary-order-0002",
        qmt_lease_generation=next_lease.generation,
        created_at=next_created_at,
    )
    await ledger.stage(
        next_generation,
        lease_token=lease_token,
        staged_at=next_created_at,
    )
    await ledger.reserve(
        next_generation,
        lease_token=lease_token,
        async_request_id=17,
        reserved_at=next_created_at,
    )
    await ledger.bind(
        gateway_holder_id=next_generation.gateway_holder_id,
        qmt_session_id=next_generation.qmt_session_id,
        qmt_lease_generation=next_generation.qmt_lease_generation,
        lease_token=lease_token,
        async_request_id=17,
        broker_order_id="88001",
        bound_at=datetime.now(UTC),
    )
    try:
        with pytest.raises(QmtSessionLeaseLostError, match="active bearer"):
            await restarted.restore_book(
                account_id=candidate.account_id,
                gateway_holder_id=candidate.gateway_holder_id,
                qmt_session_id=candidate.qmt_session_id,
                qmt_lease_generation=candidate.qmt_lease_generation,
                lease_token=lease_token,
            )
        await restarted.check_connection()
        next_restored = await restarted.restore_book(
            account_id=next_generation.account_id,
            gateway_holder_id=next_generation.gateway_holder_id,
            qmt_session_id=next_generation.qmt_session_id,
            qmt_lease_generation=next_generation.qmt_lease_generation,
            lease_token=lease_token,
        )
    finally:
        await restarted.close()

    assert reserved == repeated
    assert staged == repeated_stage
    assert len(staged.broker_order_remark) == 24
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
    ledger, _, lease_token, engine, schema = ledger_fixture
    now = datetime.now(UTC)
    candidate = _candidate(created_at=now)
    await ledger.stage(
        candidate,
        lease_token=lease_token,
        staged_at=now,
    )
    await ledger.reserve(
        candidate,
        lease_token=lease_token,
        async_request_id=17,
        reserved_at=datetime.now(UTC),
    )

    unstaged = _candidate(client_order_id="canary-order-unstaged")
    with pytest.raises(BrokerStateUnknownError, match="durably staged"):
        await ledger.reserve(
            unstaged,
            lease_token=lease_token,
            async_request_id=18,
            reserved_at=datetime.now(UTC),
        )

    conflicting = _candidate(client_order_id="canary-order-0002")
    await ledger.stage(
        conflicting,
        lease_token=lease_token,
        staged_at=conflicting.created_at,
    )
    with pytest.raises(ValueError, match="identity conflicts"):
        await ledger.reserve(
            conflicting,
            lease_token=lease_token,
            async_request_id=17,
            reserved_at=datetime.now(UTC),
        )
    with pytest.raises(QmtSessionLeaseLostError, match="matching active"):
        await ledger.reserve(
            _candidate(
                client_order_id="canary-order-wrong-generation",
                qmt_lease_generation=99,
            ),
            lease_token=lease_token,
            async_request_id=18,
            reserved_at=datetime.now(UTC),
        )
    with pytest.raises(BrokerStateUnknownError, match="no durable"):
        await ledger.bind(
            gateway_holder_id=candidate.gateway_holder_id,
            qmt_session_id=candidate.qmt_session_id,
            qmt_lease_generation=candidate.qmt_lease_generation,
            lease_token=lease_token,
            async_request_id=99,
            broker_order_id="88099",
            bound_at=datetime.now(UTC),
        )
    await ledger.bind(
        gateway_holder_id=candidate.gateway_holder_id,
        qmt_session_id=candidate.qmt_session_id,
        qmt_lease_generation=candidate.qmt_lease_generation,
        lease_token=lease_token,
        async_request_id=17,
        broker_order_id="88001",
        bound_at=datetime.now(UTC),
    )
    with pytest.raises(BrokerStateUnknownError, match="conflicts"):
        await ledger.bind(
            gateway_holder_id=candidate.gateway_holder_id,
            qmt_session_id=candidate.qmt_session_id,
            qmt_lease_generation=candidate.qmt_lease_generation,
            lease_token=lease_token,
            async_request_id=17,
            broker_order_id="88002",
            bound_at=datetime.now(UTC),
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


@pytest.mark.asyncio
async def test_qmt_canary_ledger_requires_active_bearer_lease_for_every_operation(
    ledger_fixture: tuple[
        PostgresQmtCanaryOrderLedger,
        PostgresQmtSessionLeaseRepository,
        SecretStr,
        AsyncEngine,
        str,
    ],
) -> None:
    ledger, leases, lease_token, _, _ = ledger_fixture
    now = datetime.now(UTC)
    candidate = _candidate(
        client_order_id="canary-order-bearer-fence",
        created_at=now,
    )
    wrong_token = SecretStr("wrong-qmt-canary-integration-token")

    with pytest.raises(QmtSessionLeaseLostError, match="active bearer"):
        await ledger.stage(
            candidate,
            lease_token=wrong_token,
            staged_at=now,
        )

    stage = await ledger.stage(
        candidate,
        lease_token=lease_token,
        staged_at=now,
    )
    unresolved = await ledger.unresolved_stages(
        account_id=candidate.account_id,
        gateway_holder_id=candidate.gateway_holder_id,
        qmt_session_id=candidate.qmt_session_id,
        qmt_lease_generation=candidate.qmt_lease_generation,
        lease_token=lease_token,
    )
    assert unresolved[0].candidate_hash == candidate.candidate_hash
    assert unresolved[0].broker_order_remark.startswith("AQ")
    with pytest.raises(BrokerStateUnknownError, match="fresh post-stage"):
        await ledger.reserve(
            candidate,
            lease_token=lease_token,
            async_request_id=31,
            reserved_at=datetime.now(UTC) + timedelta(seconds=10),
        )
    await ledger.reserve(
        candidate,
        lease_token=lease_token,
        async_request_id=31,
        reserved_at=datetime.now(UTC),
    )
    assert await ledger.unresolved_stages(
        account_id=candidate.account_id,
        gateway_holder_id=candidate.gateway_holder_id,
        qmt_session_id=candidate.qmt_session_id,
        qmt_lease_generation=candidate.qmt_lease_generation,
        lease_token=lease_token,
    ) == ()
    with pytest.raises(BrokerStateUnknownError, match="fresh same-session callback"):
        await ledger.bind(
            gateway_holder_id=candidate.gateway_holder_id,
            qmt_session_id=candidate.qmt_session_id,
            qmt_lease_generation=candidate.qmt_lease_generation,
            lease_token=lease_token,
            async_request_id=31,
            broker_order_id="88031",
            bound_at=datetime.now(UTC) + timedelta(seconds=10),
        )
    recovery_for_reserved_candidate = QmtCanaryRemarkRecovery(
        stage_hash=stage.stage_hash,
        candidate_hash=stage.candidate_hash,
        account_id=stage.account_id,
        broker_session_date=stage.broker_session_date,
        broker_order_id="88031",
        client_order_id=stage.client_order_id,
        baseline_hash="8" * 64,
        observed_at=datetime.now(UTC),
    )
    with pytest.raises(BrokerStateUnknownError, match="staged candidate state"):
        await ledger.bind_recovered_order(
            recovery_for_reserved_candidate,
            gateway_holder_id=candidate.gateway_holder_id,
            qmt_session_id=candidate.qmt_session_id,
            qmt_lease_generation=candidate.qmt_lease_generation,
            lease_token=lease_token,
        )
    with pytest.raises(QmtSessionLeaseLostError, match="active bearer"):
        await ledger.bind(
            gateway_holder_id=candidate.gateway_holder_id,
            qmt_session_id=candidate.qmt_session_id,
            qmt_lease_generation=candidate.qmt_lease_generation,
            lease_token=wrong_token,
            async_request_id=31,
            broker_order_id="88031",
            bound_at=datetime.now(UTC),
        )
    with pytest.raises(QmtSessionLeaseLostError, match="active bearer"):
        await ledger.restore_book(
            account_id=candidate.account_id,
            gateway_holder_id=candidate.gateway_holder_id,
            qmt_session_id=candidate.qmt_session_id,
            qmt_lease_generation=candidate.qmt_lease_generation,
            lease_token=wrong_token,
        )
    with pytest.raises(QmtSessionLeaseLostError, match="active bearer"):
        await ledger.unresolved_stages(
            account_id=candidate.account_id,
            gateway_holder_id=candidate.gateway_holder_id,
            qmt_session_id=candidate.qmt_session_id,
            qmt_lease_generation=candidate.qmt_lease_generation,
            lease_token=wrong_token,
        )

    await leases.release(
        session_id=candidate.qmt_session_id,
        holder_id=candidate.gateway_holder_id,
        token=lease_token,
        now=datetime.now(UTC),
    )
    with pytest.raises(QmtSessionLeaseLostError, match="active bearer"):
        await ledger.bind(
            gateway_holder_id=candidate.gateway_holder_id,
            qmt_session_id=candidate.qmt_session_id,
            qmt_lease_generation=candidate.qmt_lease_generation,
            lease_token=lease_token,
            async_request_id=31,
            broker_order_id="88031",
            bound_at=datetime.now(UTC),
        )
    with pytest.raises(QmtSessionLeaseLostError, match="active bearer"):
        await ledger.restore_book(
            account_id=candidate.account_id,
            gateway_holder_id=candidate.gateway_holder_id,
            qmt_session_id=candidate.qmt_session_id,
            qmt_lease_generation=candidate.qmt_lease_generation,
            lease_token=lease_token,
        )
    with pytest.raises(QmtSessionLeaseLostError, match="active bearer"):
        await ledger.unresolved_stages(
            account_id=candidate.account_id,
            gateway_holder_id=candidate.gateway_holder_id,
            qmt_session_id=candidate.qmt_session_id,
            qmt_lease_generation=candidate.qmt_lease_generation,
            lease_token=lease_token,
        )


@pytest.mark.asyncio
async def test_new_lease_generation_inventories_prior_ambiguous_submit_stage(
    ledger_fixture: tuple[
        PostgresQmtCanaryOrderLedger,
        PostgresQmtSessionLeaseRepository,
        SecretStr,
        AsyncEngine,
        str,
    ],
) -> None:
    ledger, leases, lease_token, engine, schema = ledger_fixture
    staged_at = datetime.now(UTC)
    candidate = _candidate(
        client_order_id="canary-order-prior-ambiguous",
        created_at=staged_at,
    )
    stage = await ledger.stage(
        candidate,
        lease_token=lease_token,
        staged_at=staged_at,
    )
    await leases.release(
        session_id=candidate.qmt_session_id,
        holder_id=candidate.gateway_holder_id,
        token=lease_token,
        now=datetime.now(UTC),
    )
    current = await leases.acquire(
        session_id=candidate.qmt_session_id,
        holder_id=candidate.gateway_holder_id,
        token=lease_token,
        now=datetime.now(UTC),
        ttl=timedelta(minutes=2),
    )

    unresolved = await ledger.unresolved_stages(
        account_id=candidate.account_id,
        gateway_holder_id=candidate.gateway_holder_id,
        qmt_session_id=candidate.qmt_session_id,
        qmt_lease_generation=current.generation,
        lease_token=lease_token,
    )

    assert current.generation == candidate.qmt_lease_generation + 1
    assert unresolved == (stage,)

    observed_at = datetime.now(UTC)
    stale_recovery = QmtCanaryRemarkRecovery(
        stage_hash=stage.stage_hash,
        candidate_hash=stage.candidate_hash,
        account_id=stage.account_id,
        broker_session_date=stage.broker_session_date,
        broker_order_id="88991",
        client_order_id=stage.client_order_id,
        baseline_hash="9" * 64,
        observed_at=observed_at - timedelta(seconds=10),
    )
    with pytest.raises(BrokerStateUnknownError, match="fresh same-lease"):
        await ledger.bind_recovered_order(
            stale_recovery,
            gateway_holder_id=candidate.gateway_holder_id,
            qmt_session_id=candidate.qmt_session_id,
            qmt_lease_generation=current.generation,
            lease_token=lease_token,
        )
    recovery = QmtCanaryRemarkRecovery(
        stage_hash=stage.stage_hash,
        candidate_hash=stage.candidate_hash,
        account_id=stage.account_id,
        broker_session_date=stage.broker_session_date,
        broker_order_id="88991",
        client_order_id=stage.client_order_id,
        baseline_hash="9" * 64,
        observed_at=observed_at,
    )
    stored = await ledger.bind_recovered_order(
        recovery,
        gateway_holder_id=candidate.gateway_holder_id,
        qmt_session_id=candidate.qmt_session_id,
        qmt_lease_generation=current.generation,
        lease_token=lease_token,
    )
    repeated = await ledger.bind_recovered_order(
        recovery,
        gateway_holder_id=candidate.gateway_holder_id,
        qmt_session_id=candidate.qmt_session_id,
        qmt_lease_generation=current.generation,
        lease_token=lease_token,
    )
    mapping = await ledger.recovered_broker_mapping(
        account_id=candidate.account_id,
        broker_session_date=stage.broker_session_date,
        gateway_holder_id=candidate.gateway_holder_id,
        qmt_session_id=candidate.qmt_session_id,
        qmt_lease_generation=current.generation,
        lease_token=lease_token,
    )

    assert stored == repeated == recovery
    assert mapping == {88991: candidate.decision.order.client_order_id}
    assert await ledger.unresolved_stages(
        account_id=candidate.account_id,
        gateway_holder_id=candidate.gateway_holder_id,
        qmt_session_id=candidate.qmt_session_id,
        qmt_lease_generation=current.generation,
        lease_token=lease_token,
    ) == ()
    with pytest.raises(SQLAlchemyError):
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
            await connection.execute(
                text(
                    "DELETE FROM qmt_order_remark_recovery_bindings "
                    "WHERE recovery_hash = :recovery_hash"
                ),
                {"recovery_hash": recovery.recovery_hash},
            )
