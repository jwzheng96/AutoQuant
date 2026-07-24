from __future__ import annotations

import os
from collections.abc import AsyncIterator
from dataclasses import replace
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
from autoquant.errors import BrokerStateUnknownError
from autoquant.execution.control_store import (
    PostgresExecutionControlRepository,
)
from autoquant.execution.qmt_callback_coordinator import (
    QmtCallbackPersistenceCoordinator,
)
from autoquant.execution.qmt_callback_reconciliation import (
    QmtCallbackReconciliationState,
    reconcile_qmt_callback_state,
)
from autoquant.execution.qmt_callback_reconciliation_store import (
    PostgresQmtCallbackReconciliationRepository,
)
from autoquant.execution.qmt_callback_reducer import (
    QmtCallbackDisposition,
    QmtOrderConvergence,
)
from autoquant.execution.qmt_callback_reducer_store import (
    PostgresQmtCallbackStateReducer,
)
from autoquant.execution.qmt_callback_store import PostgresQmtCallbackInbox
from autoquant.execution.qmt_canary_contract import (
    QmtCanaryOrderCandidate,
    qmt_canary_order_remark,
)
from autoquant.execution.qmt_canary_store import PostgresQmtCanaryOrderLedger
from autoquant.execution.qmt_gateway import QmtCallbackBuffer, QmtCallbackKind
from autoquant.execution.qmt_models import QmtOrderStatus
from autoquant.execution.qmt_readonly import (
    QmtReadOnlyBaseline,
    build_qmt_readonly_baseline,
    normalize_qmt_asset,
    normalize_qmt_order,
    normalize_qmt_trade,
)
from autoquant.execution.qmt_readonly_store import (
    PostgresQmtReadOnlyAcceptanceRepository,
    QmtReadOnlyAcceptanceEvidence,
)
from autoquant.execution.qmt_session_store import (
    PostgresQmtSessionLeaseRepository,
    QmtSessionLease,
)
from autoquant.risk.models import (
    ExecutionMode,
    ProposedOrder,
    RiskDecision,
    RiskDecisionState,
)

POSTGRES_DSN = os.environ.get("AQ_POSTGRES_DSN", "").strip()
ACCOUNT_ID = "canary-account"
BROKER_ACCOUNT = "integration-broker-account"
HOLDER_ID = "windows-qmt-canary-01"
SESSION_ID = 20260724
LEASE_TOKEN = SecretStr("qmt-reducer-integration-token-value")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not POSTGRES_DSN,
        reason="AQ_POSTGRES_DSN is not configured; PostgreSQL unavailable",
    ),
]


def _candidate(*, generation: int, now: datetime) -> QmtCanaryOrderCandidate:
    order = ProposedOrder(
        client_order_id="canary-order-reducer-0001",
        instrument="600000.XSHG",
        side=OrderSide.BUY,
        quantity=100,
        submitted_at=now - timedelta(seconds=1),
        limit_price=Decimal("10"),
    )
    decision = RiskDecision(
        account_id=ACCOUNT_ID,
        mode=ExecutionMode.LIVE,
        order=order,
        evaluated_at=now - timedelta(seconds=1),
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
        account_id=ACCOUNT_ID,
        strategy_id="low-volatility-v5",
        gateway_holder_id=HOLDER_ID,
        qmt_session_id=SESSION_ID,
        qmt_lease_generation=generation,
        decision=decision,
        promotion_report_hash="a" * 64,
        compliance_approval_hash="e" * 64,
        qmt_acceptance_hash="f" * 64,
        reconciliation_report_hash="1" * 64,
        maximum_order_notional=Decimal("2000"),
        created_at=now,
        valid_until=now + timedelta(seconds=30),
    )


@pytest_asyncio.fixture
async def reducer_fixture() -> AsyncIterator[
    tuple[
        PostgresQmtCallbackInbox,
        PostgresQmtCallbackStateReducer,
        AsyncEngine,
        str,
        int,
        QmtCanaryOrderCandidate,
        QmtSessionLease,
    ]
]:
    schema = f"autoquant_test_{uuid4().hex}"
    engine = create_async_engine(POSTGRES_DSN, pool_pre_ping=True)
    control = PostgresControlRepository(engine=engine, schema=schema)
    leases = PostgresQmtSessionLeaseRepository(engine=engine, schema=schema)
    ledger = PostgresQmtCanaryOrderLedger(engine=engine, schema=schema)
    inbox = PostgresQmtCallbackInbox(engine=engine, schema=schema)
    reducer = PostgresQmtCallbackStateReducer(engine=engine, schema=schema)
    controls = PostgresExecutionControlRepository(
        engine=engine,
        schema=schema,
    )
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/009_execution_controls.sql",
            "migrations/postgres/012_qmt_session_leases.sql",
            "migrations/postgres/017_qmt_readonly_acceptance.sql",
            "migrations/postgres/037_qmt_canary_order_ledger.sql",
            "migrations/postgres/038_qmt_canary_order_staging.sql",
            "migrations/postgres/039_qmt_canary_remark_recovery.sql",
            "migrations/postgres/040_qmt_callback_inbox.sql",
            "migrations/postgres/041_qmt_callback_persistence_receipts.sql",
            "migrations/postgres/042_qmt_callback_state_reduction.sql",
            "migrations/postgres/043_qmt_callback_reconciliation.sql",
        )
    )
    try:
        await control.initialize(migration)
        lease = await leases.acquire(
            session_id=SESSION_ID,
            holder_id=HOLDER_ID,
            token=LEASE_TOKEN,
            now=datetime.now(UTC) - timedelta(seconds=2),
            ttl=timedelta(minutes=2),
        )
        await controls.ensure_fail_closed(
            account_id=ACCOUNT_ID,
            now=datetime.now(UTC),
        )
        now = datetime.now(UTC)
        candidate = _candidate(generation=lease.generation, now=now)
        await ledger.stage(candidate, lease_token=LEASE_TOKEN, staged_at=now)
        await ledger.reserve(
            candidate,
            lease_token=LEASE_TOKEN,
            async_request_id=42,
            reserved_at=datetime.now(UTC),
        )
        await ledger.bind(
            account_id=ACCOUNT_ID,
            gateway_holder_id=HOLDER_ID,
            qmt_session_id=SESSION_ID,
            qmt_lease_generation=lease.generation,
            lease_token=LEASE_TOKEN,
            async_request_id=42,
            broker_order_id="88001",
            broker_order_remark=qmt_canary_order_remark(candidate.candidate_hash),
            bound_at=datetime.now(UTC),
        )
        yield (
            inbox,
            reducer,
            engine,
            schema,
            lease.generation,
            candidate,
            lease,
        )
    finally:
        try:
            await control.drop_test_schema()
        finally:
            await engine.dispose()


def _order_payload(
    candidate: QmtCanaryOrderCandidate,
    *,
    traded_volume: int,
    status: int,
) -> dict[str, object]:
    return {
        "account_id": BROKER_ACCOUNT,
        "order_id": 88001,
        "order_remark": qmt_canary_order_remark(candidate.candidate_hash),
        "order_status": status,
        "order_volume": 100,
        "price": 10.0,
        "side": "buy",
        "status_msg": "",
        "stock_code": "600000.SH",
        "traded_price": 0.0 if traded_volume == 0 else 10.0,
        "traded_volume": traded_volume,
    }


def _trade_payload(
    candidate: QmtCanaryOrderCandidate,
    *,
    volume: int,
    trade_id: str = "TRADE-001",
) -> dict[str, object]:
    return {
        "account_id": BROKER_ACCOUNT,
        "order_id": 88001,
        "order_remark": qmt_canary_order_remark(candidate.candidate_hash),
        "side": "buy",
        "stock_code": "600000.SH",
        "traded_amount": 10.0 * volume,
        "traded_id": trade_id,
        "traded_price": 10.0,
        "traded_volume": volume,
    }


def _readonly_baseline(
    candidate: QmtCanaryOrderCandidate,
    *,
    callback_cursor: int,
) -> QmtReadOnlyBaseline:
    observed_at = datetime.now(UTC)
    remark = qmt_canary_order_remark(candidate.candidate_hash)
    order = normalize_qmt_order(
        _order_payload(candidate, traded_volume=40, status=55),
        expected_account_id=BROKER_ACCOUNT,
        observed_at=observed_at,
        client_order_ids={88001: candidate.decision.order.client_order_id},
    )
    trade = normalize_qmt_trade(
        _trade_payload(candidate, volume=40),
        expected_account_id=BROKER_ACCOUNT,
        observed_at=observed_at,
    )
    assert order.raw_status == QmtOrderStatus.PARTIALLY_FILLED
    assert trade.order_remark == remark
    return build_qmt_readonly_baseline(
        baseline_id=f"callback-reconcile-{callback_cursor}",
        generation=1,
        logical_account_id=ACCOUNT_ID,
        query_started_at=observed_at,
        query_completed_at=observed_at,
        callback_cursor_before=callback_cursor,
        callback_cursor_after=callback_cursor,
        callback_stream_healthy=True,
        asset=normalize_qmt_asset(
            {
                "account_id": BROKER_ACCOUNT,
                "cash": 1000,
                "frozen_cash": 0,
                "market_value": 0,
                "total_asset": 1000,
            },
            expected_account_id=BROKER_ACCOUNT,
            observed_at=observed_at,
        ),
        positions=(),
        orders=(order,),
        trades=(trade,),
    )


@pytest.mark.asyncio
async def test_qmt_reducer_converges_restart_replays_and_fences_trade_conflict(
    reducer_fixture: tuple[
        PostgresQmtCallbackInbox,
        PostgresQmtCallbackStateReducer,
        AsyncEngine,
        str,
        int,
        QmtCanaryOrderCandidate,
        QmtSessionLease,
    ],
) -> None:
    inbox, reducer, engine, schema, generation, candidate, lease = reducer_fixture
    buffer = QmtCallbackBuffer()
    coordinator = QmtCallbackPersistenceCoordinator(
        buffer=buffer,
        inbox=inbox,
        expected_broker_account_id=BROKER_ACCOUNT,
        logical_account_id=ACCOUNT_ID,
        gateway_holder_id=HOLDER_ID,
        qmt_session_id=SESSION_ID,
        qmt_lease_generation=generation,
        state_reducer=reducer,
    )
    buffer.capture(
        QmtCallbackKind.ORDER,
        _order_payload(candidate, traded_volume=40, status=55),
        received_at=datetime.now(UTC),
    )
    coordinated = await coordinator.persist_and_process(
        lease_token=LEASE_TOKEN,
    )
    assert coordinated.state_reduction is not None
    pending = coordinated.state_reduction
    assert pending.records[-1].disposition is (QmtCallbackDisposition.PENDING_RECONCILIATION)
    assert pending.projections[0].convergence is QmtOrderConvergence.PENDING
    assert pending.broker_state_known is False

    buffer.capture(
        QmtCallbackKind.TRADE,
        _trade_payload(candidate, volume=40),
        received_at=datetime.now(UTC),
    )
    coordinated = await coordinator.persist_and_process(
        lease_token=LEASE_TOKEN,
    )
    assert coordinated.state_reduction is not None
    converged = coordinated.state_reduction
    assert converged.records[-1].disposition is QmtCallbackDisposition.TRADE_APPLIED
    assert converged.projections[0].convergence is QmtOrderConvergence.CONVERGED
    assert converged.broker_state_known is True

    restarted = PostgresQmtCallbackStateReducer(engine=engine, schema=schema)
    replayed = await restarted.process_current(
        inbox=inbox,
        account_id=ACCOUNT_ID,
        gateway_holder_id=HOLDER_ID,
        qmt_session_id=SESSION_ID,
        qmt_lease_generation=generation,
        lease_token=LEASE_TOKEN,
    )
    assert replayed.records == ()
    assert replayed.projections == converged.projections
    assert replayed.broker_state_known is True

    acceptance_store = PostgresQmtReadOnlyAcceptanceRepository(
        engine=engine,
        schema=schema,
    )
    reconciliation_store = PostgresQmtCallbackReconciliationRepository(
        engine=engine,
        schema=schema,
    )
    baseline = _readonly_baseline(candidate, callback_cursor=2)
    acceptance = QmtReadOnlyAcceptanceEvidence.from_baseline(
        baseline=baseline,
        package_manifest_hash="9" * 64,
        lease=lease,
    )
    await acceptance_store.append(
        acceptance,
        now=datetime.now(UTC),
    )
    passed_report = reconcile_qmt_callback_state(
        baseline=baseline,
        acceptance=acceptance,
        reduction=replayed,
    )
    assert passed_report.state is QmtCallbackReconciliationState.PASSED
    assert (
        await reconciliation_store.append(
            passed_report,
            lease_token=LEASE_TOKEN,
        )
        == passed_report
    )

    buffer.capture(
        QmtCallbackKind.TRADE,
        _trade_payload(candidate, volume=41),
        received_at=datetime.now(UTC),
    )
    conflicted_result = await coordinator.persist_and_process(
        lease_token=LEASE_TOKEN,
    )
    assert conflicted_result.state_reduction is not None
    conflicted = conflicted_result.state_reduction
    assert conflicted.records[-1].disposition is (QmtCallbackDisposition.BROKER_STATE_UNKNOWN)
    assert conflicted.fatal_reason == "trade_id_conflict"
    assert conflicted.broker_state_known is False
    assert conflicted.projections[0].convergence is QmtOrderConvergence.UNKNOWN

    rejected_baseline = _readonly_baseline(candidate, callback_cursor=3)
    rejected_acceptance = QmtReadOnlyAcceptanceEvidence.from_baseline(
        baseline=rejected_baseline,
        package_manifest_hash="9" * 64,
        lease=lease,
    )
    await acceptance_store.append(
        rejected_acceptance,
        now=datetime.now(UTC),
    )
    rejected_report = reconcile_qmt_callback_state(
        baseline=rejected_baseline,
        acceptance=rejected_acceptance,
        reduction=conflicted,
    )
    assert rejected_report.state is QmtCallbackReconciliationState.REJECTED
    with pytest.raises(
        BrokerStateUnknownError,
        match="passed QMT reconciliation",
    ):
        await reconciliation_store.append(
            replace(rejected_report, issues=()),
            lease_token=LEASE_TOKEN,
        )
    await reconciliation_store.append(
        rejected_report,
        lease_token=LEASE_TOKEN,
    )
    assert await reconciliation_store.latest(logical_account_id=ACCOUNT_ID) == rejected_report

    async with engine.begin() as connection:
        with pytest.raises(SQLAlchemyError):
            await connection.execute(
                text(
                    f"""
                    UPDATE {schema}.qmt_callback_processing_events
                    SET reason = 'tampered'
                    """
                )
            )


def test_qmt_callback_reducer_migration_is_fenced_and_non_executable() -> None:
    sql = Path("migrations/postgres/042_qmt_callback_state_reduction.sql").read_text(
        encoding="utf-8"
    )

    assert "qmt_callback_processing_events" in sql
    assert "qmt_callback_processing_cursors" in sql
    assert "qmt_broker_trade_facts_immutable" in sql
    assert "qmt_callback_processing_events_immutable" in sql
    assert "NOT broker_mutation_allowed" in sql
    assert "VALUES ('postgres', 42)" in sql


def test_qmt_callback_reconciliation_migration_is_immutable() -> None:
    sql = Path("migrations/postgres/043_qmt_callback_reconciliation.sql").read_text(
        encoding="utf-8"
    )

    assert "qmt_callback_reconciliation_reports" in sql
    assert "qmt_callback_reconciliation_reports_immutable" in sql
    assert "NOT broker_mutation_allowed" in sql
    assert "VALUES ('postgres', 43)" in sql
