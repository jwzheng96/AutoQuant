from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.backtest.models import OrderSide
from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.account_projection import PaperAccountProjector
from autoquant.execution.control import KillSwitchReason
from autoquant.execution.control_store import PostgresExecutionControlRepository
from autoquant.execution.coordinator import (
    PaperCoordinationStatus,
    PaperOrderCoordinator,
    PaperSubmissionRequest,
)
from autoquant.execution.models import ApprovedPaperOrder, PaperOrderState
from autoquant.execution.reconciliation import (
    AccountReconciler,
    ReconciliationCode,
)
from autoquant.execution.session_risk import (
    SessionRiskObservation,
    derive_session_turnover,
)
from autoquant.execution.session_risk_store import (
    PostgresPaperSessionRiskRepository,
)
from autoquant.execution.simulated_broker import (
    PersistentSimulatedBroker,
    SimulatedBrokerControlError,
)
from autoquant.execution.store import PostgresPaperExecutionRepository
from autoquant.risk.engine import PreTradeRiskEngine
from autoquant.risk.models import (
    ExecutionMode,
    MarketQuote,
    ProposedOrder,
    RiskAccountState,
    RiskEvaluationInput,
    RiskPolicy,
)
from autoquant.web.risk_store import PostgresRiskDecisionRepository

POSTGRES_DSN = os.environ.get("AQ_POSTGRES_DSN", "").strip()
NOW = datetime(2026, 7, 22, 5, tzinfo=UTC)
INSTRUMENT = "000001.XSHE"
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not POSTGRES_DSN,
        reason="AQ_POSTGRES_DSN is not configured; PostgreSQL infrastructure unavailable",
    ),
]

Repositories = tuple[
    PostgresRiskDecisionRepository,
    PostgresPaperExecutionRepository,
    PostgresExecutionControlRepository,
    PersistentSimulatedBroker,
    PostgresPaperSessionRiskRepository,
    AsyncEngine,
    str,
]


@pytest_asyncio.fixture
async def repositories() -> AsyncIterator[Repositories]:
    schema = f"autoquant_test_{uuid4().hex}"
    engine = create_async_engine(POSTGRES_DSN, pool_pre_ping=True)
    control = PostgresControlRepository(engine=engine, schema=schema)
    risks = PostgresRiskDecisionRepository(engine=engine, schema=schema)
    executions = PostgresPaperExecutionRepository(engine=engine, schema=schema)
    controls = PostgresExecutionControlRepository(engine=engine, schema=schema)
    broker = PersistentSimulatedBroker(engine=engine, schema=schema)
    sessions = PostgresPaperSessionRiskRepository(engine=engine, schema=schema)
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/007_risk_decisions.sql",
            "migrations/postgres/008_paper_execution.sql",
            "migrations/postgres/009_execution_controls.sql",
            "migrations/postgres/010_simulated_broker.sql",
            "migrations/postgres/011_paper_session_risk.sql",
        )
    )
    try:
        await control.initialize(migration)
        yield risks, executions, controls, broker, sessions, engine, schema
    finally:
        try:
            await control.drop_test_schema()
        finally:
            await engine.dispose()


def _quote(*, market_open: bool = True) -> MarketQuote:
    return MarketQuote(
        instrument=INSTRUMENT,
        as_of=NOW,
        last_price=Decimal("10"),
        bid_price=Decimal("9.99"),
        ask_price=Decimal("10.01"),
        market_open=market_open,
    )


def _decision(*, order_id: str, limit_price: str | None = None):  # type: ignore[no-untyped-def]
    account = RiskAccountState(
        account_id="paper-main",
        as_of=NOW,
        cash=Decimal("100000"),
        equity=Decimal("100000"),
        day_start_equity=Decimal("100000"),
        peak_equity=Decimal("100000"),
        gross_exposure=Decimal("0"),
        daily_turnover=Decimal("0"),
        open_order_count=0,
        reconciled=True,
        kill_switch=False,
    )
    order = ProposedOrder(
        client_order_id=order_id,
        instrument=INSTRUMENT,
        side=OrderSide.BUY,
        quantity=100,
        submitted_at=NOW,
        limit_price=None if limit_price is None else Decimal(limit_price),
    )
    rules = AshareRuleBook().resolve(
        INSTRUMENT,
        date(2026, 7, 22),
        SecurityStatus(risk_warning=False, listing_session_number=1000),
    )
    return PreTradeRiskEngine().evaluate(
        RiskEvaluationInput(
            mode=ExecutionMode.PAPER,
            policy=RiskPolicy(allowed_instruments=(INSTRUMENT,)),
            account=account,
            order=order,
            quote=_quote(),
            rules=rules,
            now=NOW,
        )
    )


async def _persist_order(
    risks: PostgresRiskDecisionRepository,
    executions: PostgresPaperExecutionRepository,
    *,
    order_id: str,
    limit_price: str | None = None,
) -> ApprovedPaperOrder:
    decision = _decision(order_id=order_id, limit_price=limit_price)
    await risks.append(decision)
    order = ApprovedPaperOrder.from_risk_decision(decision)
    await executions.create_order(order)
    return order


def _submission_request(
    *,
    order_id: str,
    now: datetime = NOW,
    quantity: int = 100,
    policy: RiskPolicy | None = None,
) -> PaperSubmissionRequest:
    return PaperSubmissionRequest(
        order=ProposedOrder(
            client_order_id=order_id,
            instrument=INSTRUMENT,
            side=OrderSide.BUY,
            quantity=quantity,
            submitted_at=NOW,
        ),
        quote=_quote(),
        rules=AshareRuleBook().resolve(
            INSTRUMENT,
            date(2026, 7, 22),
            SecurityStatus(risk_warning=False, listing_session_number=1000),
        ),
        policy=policy or RiskPolicy(allowed_instruments=(INSTRUMENT,)),
        marks={INSTRUMENT: Decimal("10")},
        now=now,
    )


async def _initialize_session(
    *,
    executions: PostgresPaperExecutionRepository,
    sessions: PostgresPaperSessionRiskRepository,
) -> str:
    snapshot = PaperAccountProjector().project(
        account_id="paper-main",
        initial_cash=Decimal("100000"),
        histories=(),
        marks={},
        as_of=NOW,
    )
    report = AccountReconciler().reconcile(
        internal=snapshot,
        broker=snapshot,
        now=NOW,
    )
    await executions.save_reconciliation(
        internal=snapshot,
        broker=snapshot,
        report=report,
    )
    turnover = derive_session_turnover(
        account_id="paper-main",
        session_date=date(2026, 7, 22),
        histories=(),
    )
    await sessions.initialize(
        SessionRiskObservation(
            account_id="paper-main",
            session_date=date(2026, 7, 22),
            as_of=NOW,
            equity=snapshot.equity,
            cumulative_turnover=turnover.cumulative_turnover,
            snapshot_hash=snapshot.snapshot_hash,
            turnover_evidence_hash=turnover.evidence_hash,
        )
    )
    return report.report_hash


async def _reset_kill_switch(
    *,
    executions: PostgresPaperExecutionRepository,
    controls: PostgresExecutionControlRepository,
    sessions: PostgresPaperSessionRiskRepository,
) -> None:
    report_hash = await _initialize_session(
        executions=executions,
        sessions=sessions,
    )
    active = await controls.ensure_fail_closed(account_id="paper-main", now=NOW)
    await controls.reset(
        account_id="paper-main",
        command_id="integration-reset-paper-control",
        actor="integration-test",
        now=NOW,
        expected_version=active.version,
        reconciliation_report_hash=report_hash,
        recovery_verified=True,
    )


@pytest.mark.asyncio
async def test_simulated_broker_is_persistent_idempotent_and_independently_replayable(
    repositories: Repositories,
) -> None:
    risks, executions, _, broker, _, engine, schema = repositories
    order = await _persist_order(
        risks,
        executions,
        order_id="simulated-market-order-0001",
    )

    updates = await broker.submit(order=order, quote=_quote(), now=NOW)
    repeated = await broker.submit(order=order, quote=_quote(), now=NOW)
    replayed = await broker.replay(order_hash=order.order_hash)

    assert tuple(update.state for update in updates) == (
        PaperOrderState.SUBMITTED,
        PaperOrderState.FILLED,
    )
    assert updates[1].average_fill_price == Decimal("10.01")
    assert repeated == updates
    assert replayed.state is PaperOrderState.FILLED
    assert replayed.cumulative_filled_quantity == order.quantity

    internal = await executions.load_order(
        account_id=order.account_id,
        client_order_id=order.client_order_id,
    )
    assert internal.state is PaperOrderState.APPROVED
    for update in updates:
        await executions.apply_update(
            account_id=order.account_id,
            client_order_id=order.client_order_id,
            update=update,
        )
    assert (
        await executions.replay_order(
            account_id=order.account_id,
            client_order_id=order.client_order_id,
        )
    ).state is PaperOrderState.FILLED

    resting_order = await _persist_order(
        risks,
        executions,
        order_id="simulated-resting-order-0001",
        limit_price="10.00",
    )
    resting_updates = await broker.submit(
        order=resting_order,
        quote=_quote(),
        now=NOW,
    )
    assert tuple(update.state for update in resting_updates) == (PaperOrderState.SUBMITTED,)

    with pytest.raises(ValueError, match="market is closed"):
        unsubmitted = await _persist_order(
            risks,
            executions,
            order_id="simulated-closed-order-0001",
        )
        await broker.submit(order=unsubmitted, quote=_quote(market_open=False), now=NOW)

    async with engine.connect() as connection:
        fact_count = await connection.scalar(
            text(f"SELECT count(*) FROM {schema}.simulated_broker_facts")
        )
    assert fact_count == 3

    with pytest.raises(DBAPIError):
        async with engine.begin() as connection:
            await connection.execute(
                text(f"UPDATE {schema}.simulated_broker_facts SET update_payload = '{{}}'::jsonb")
            )


@pytest.mark.asyncio
async def test_independent_account_projections_detect_and_close_callback_gap(
    repositories: Repositories,
) -> None:
    risks, executions, _, broker, _, _, _ = repositories
    order = await _persist_order(
        risks,
        executions,
        order_id="simulated-reconciliation-order-0001",
    )
    updates = await broker.submit(order=order, quote=_quote(), now=NOW)
    projector = PaperAccountProjector()
    projection_input = {
        "account_id": order.account_id,
        "initial_cash": Decimal("100000"),
        "marks": {INSTRUMENT: Decimal("10.01")},
        "as_of": NOW,
    }

    internal_before = projector.project(
        **projection_input,
        histories=await executions.account_histories(account_id=order.account_id),
    )
    broker_snapshot = projector.project(
        **projection_input,
        histories=await broker.account_histories(account_id=order.account_id),
    )
    before = AccountReconciler().reconcile(
        internal=internal_before,
        broker=broker_snapshot,
        now=NOW,
    )

    assert before.reconciled is False
    assert before.issues == (
        ReconciliationCode.CASH_MISMATCH,
        ReconciliationCode.EQUITY_MISMATCH,
        ReconciliationCode.POSITION_MISMATCH,
        ReconciliationCode.OPEN_ORDER_MISMATCH,
    )
    assert internal_before.evidence_hash != broker_snapshot.evidence_hash
    assert broker_snapshot.cash == Decimal("98993.99")
    assert broker_snapshot.equity == Decimal("99994.99")
    assert broker_snapshot.positions[0].total_quantity == 100
    assert broker_snapshot.positions[0].sellable_quantity == 0

    for update in updates:
        await executions.apply_update(
            account_id=order.account_id,
            client_order_id=order.client_order_id,
            update=update,
        )
    internal_after = projector.project(
        **projection_input,
        histories=await executions.account_histories(account_id=order.account_id),
    )
    after = AccountReconciler().reconcile(
        internal=internal_after,
        broker=broker_snapshot,
        now=NOW,
    )
    stored = await executions.save_reconciliation(
        internal=internal_after,
        broker=broker_snapshot,
        report=after,
    )

    assert internal_after == broker_snapshot
    assert internal_after.evidence_hash == broker_snapshot.evidence_hash
    assert after.reconciled is True
    assert stored == after
    assert (await executions.verify_recovery()).latest_reconciled is True


@pytest.mark.asyncio
async def test_simulated_broker_requires_an_unchanged_inactive_control_fence(
    repositories: Repositories,
) -> None:
    risks, executions, controls, broker, _, _, _ = repositories
    order = await _persist_order(
        risks,
        executions,
        order_id="simulated-control-fence-order-0001",
    )
    active = await controls.ensure_fail_closed(account_id="paper-main", now=NOW)

    with pytest.raises(SimulatedBrokerControlError, match="control fence"):
        await broker.submit(
            order=order,
            quote=_quote(),
            now=NOW,
            control_fence=active,
        )

    assert (await broker.verify_recovery()).order_count == 0


@pytest.mark.asyncio
async def test_coordinator_runs_risk_submission_callback_and_reconciliation_idempotently(
    repositories: Repositories,
) -> None:
    risks, executions, controls, broker, sessions, engine, schema = repositories
    await _reset_kill_switch(
        executions=executions,
        controls=controls,
        sessions=sessions,
    )
    coordinator = PaperOrderCoordinator(
        account_id="paper-main",
        initial_cash=Decimal("100000"),
        risks=risks,
        executions=executions,
        controls=controls,
        broker=broker,
        sessions=sessions,
    )
    request = _submission_request(order_id="coordinated-market-order-0001")

    first = await coordinator.submit(request)
    repeated = await coordinator.submit(request)

    assert first.status is PaperCoordinationStatus.FILLED
    assert first.decision is not None
    assert first.decision.state.value == "accepted"
    assert first.projection is not None
    assert first.projection.state is PaperOrderState.FILLED
    assert first.pre_reconciliation is not None
    assert first.pre_reconciliation.reconciled is True
    assert first.post_reconciliation is not None
    assert first.post_reconciliation.reconciled is True
    assert first.control.active is False
    assert repeated.status is PaperCoordinationStatus.RECOVERED
    assert repeated.decision is None
    assert repeated.projection == first.projection
    assert repeated.post_reconciliation is not None
    assert repeated.post_reconciliation.reconciled is True
    session = await sessions.replay(
        account_id="paper-main",
        session_date=date(2026, 7, 22),
    )
    assert session.day_start_equity == Decimal("100000")
    assert session.peak_equity == Decimal("100000")
    assert session.cumulative_turnover == Decimal("1001.00")
    with pytest.raises(DBAPIError):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"UPDATE {schema}.paper_session_risk_events "
                    "SET observation_payload = '{}'::jsonb"
                )
            )
    async with engine.connect() as connection:
        counts = (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT
                          (SELECT count(*) FROM {schema}.risk_decisions) AS risks,
                          (SELECT count(*) FROM {schema}.paper_orders) AS orders,
                          (SELECT count(*) FROM {schema}.paper_order_events) AS events,
                          (SELECT count(*) FROM {schema}.simulated_broker_facts) AS facts
                        """
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(counts) == {"risks": 1, "orders": 1, "events": 2, "facts": 2}


@pytest.mark.asyncio
async def test_account_lock_serializes_concurrent_risk_cycles(
    repositories: Repositories,
) -> None:
    risks, executions, controls, broker, sessions, _, _ = repositories
    await _reset_kill_switch(
        executions=executions,
        controls=controls,
        sessions=sessions,
    )
    coordinator = PaperOrderCoordinator(
        account_id="paper-main",
        initial_cash=Decimal("100000"),
        risks=risks,
        executions=executions,
        controls=controls,
        broker=broker,
        sessions=sessions,
    )
    policy = RiskPolicy(
        allowed_instruments=(INSTRUMENT,),
        max_position_weight=Decimal("1"),
        max_gross_exposure=Decimal("1"),
        max_daily_turnover=Decimal("2"),
    )

    results = await asyncio.gather(
        coordinator.submit(
            _submission_request(
                order_id="concurrent-paper-order-0001",
                quantity=6000,
                policy=policy,
            )
        ),
        coordinator.submit(
            _submission_request(
                order_id="concurrent-paper-order-0002",
                quantity=6000,
                policy=policy,
            )
        ),
    )

    assert {result.status for result in results} == {
        PaperCoordinationStatus.FILLED,
        PaperCoordinationStatus.RISK_REJECTED,
    }
    rejected = next(
        result for result in results if result.status is PaperCoordinationStatus.RISK_REJECTED
    )
    assert rejected.decision is not None
    assert "insufficient_cash" in {violation.value for violation in rejected.decision.violations}
    assert (await risks.count()) == 2
    assert (await executions.verify_recovery()).order_count == 1
    assert (await broker.verify_recovery()).order_count == 1
    assert (
        await sessions.replay(
            account_id="paper-main",
            session_date=date(2026, 7, 22),
        )
    ).cumulative_turnover == Decimal("60060.00")


@pytest.mark.asyncio
async def test_coordinator_persists_rejection_while_kill_switch_is_active(
    repositories: Repositories,
) -> None:
    risks, executions, controls, broker, sessions, _, _ = repositories
    await _initialize_session(executions=executions, sessions=sessions)
    active = await controls.ensure_fail_closed(account_id="paper-main", now=NOW)
    coordinator = PaperOrderCoordinator(
        account_id="paper-main",
        initial_cash=Decimal("100000"),
        risks=risks,
        executions=executions,
        controls=controls,
        broker=broker,
        sessions=sessions,
    )

    result = await coordinator.submit(
        _submission_request(order_id="coordinated-rejected-order-0001")
    )

    assert result.status is PaperCoordinationStatus.RISK_REJECTED
    assert result.control == active
    assert result.decision is not None
    assert tuple(code.value for code in result.decision.violations) == ("kill_switch_active",)
    assert (await risks.count()) == 1
    assert (await executions.verify_recovery()).order_count == 0
    assert (await broker.verify_recovery()).order_count == 0


@pytest.mark.asyncio
async def test_coordinator_fails_closed_without_preinitialized_session_risk(
    repositories: Repositories,
) -> None:
    risks, executions, controls, broker, sessions, _, _ = repositories
    coordinator = PaperOrderCoordinator(
        account_id="paper-main",
        initial_cash=Decimal("100000"),
        risks=risks,
        executions=executions,
        controls=controls,
        broker=broker,
        sessions=sessions,
    )

    with pytest.raises(PersistenceUnavailableError, match="not initialized"):
        await coordinator.submit(
            _submission_request(order_id="missing-session-risk-order-0001")
        )

    control = await controls.get(account_id="paper-main")
    assert control.active is True
    assert control.reason is KillSwitchReason.DEPENDENCY_UNAVAILABLE
    assert await risks.count() == 0
    assert (await broker.verify_recovery()).order_count == 0


@pytest.mark.asyncio
async def test_coordinator_blocks_stale_unsubmitted_intent_and_activates_kill_switch(
    repositories: Repositories,
) -> None:
    risks, executions, controls, broker, sessions, _, _ = repositories
    await _reset_kill_switch(
        executions=executions,
        controls=controls,
        sessions=sessions,
    )
    order = await _persist_order(
        risks,
        executions,
        order_id="coordinated-stale-dispatch-0001",
    )
    coordinator = PaperOrderCoordinator(
        account_id="paper-main",
        initial_cash=Decimal("100000"),
        risks=risks,
        executions=executions,
        controls=controls,
        broker=broker,
        sessions=sessions,
    )

    result = await coordinator.submit(
        _submission_request(
            order_id=order.client_order_id,
            now=NOW + timedelta(seconds=4),
        )
    )

    assert result.status is PaperCoordinationStatus.BLOCKED
    assert result.control.active is True
    assert result.control.reason is KillSwitchReason.ORDER_STATE_UNKNOWN
    assert result.projection is not None
    assert result.projection.state is PaperOrderState.APPROVED
    assert result.post_reconciliation is not None
    assert result.post_reconciliation.reconciled is False
    assert (await broker.verify_recovery()).order_count == 0
