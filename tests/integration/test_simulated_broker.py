from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime
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
from autoquant.execution.account_projection import PaperAccountProjector
from autoquant.execution.models import ApprovedPaperOrder, PaperOrderState
from autoquant.execution.reconciliation import (
    AccountReconciler,
    ReconciliationCode,
)
from autoquant.execution.simulated_broker import PersistentSimulatedBroker
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


@pytest_asyncio.fixture
async def repositories() -> AsyncIterator[
    tuple[
        PostgresRiskDecisionRepository,
        PostgresPaperExecutionRepository,
        PersistentSimulatedBroker,
        AsyncEngine,
        str,
    ]
]:
    schema = f"autoquant_test_{uuid4().hex}"
    engine = create_async_engine(POSTGRES_DSN, pool_pre_ping=True)
    control = PostgresControlRepository(engine=engine, schema=schema)
    risks = PostgresRiskDecisionRepository(engine=engine, schema=schema)
    executions = PostgresPaperExecutionRepository(engine=engine, schema=schema)
    broker = PersistentSimulatedBroker(engine=engine, schema=schema)
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/007_risk_decisions.sql",
            "migrations/postgres/008_paper_execution.sql",
            "migrations/postgres/010_simulated_broker.sql",
        )
    )
    try:
        await control.initialize(migration)
        yield risks, executions, broker, engine, schema
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


@pytest.mark.asyncio
async def test_simulated_broker_is_persistent_idempotent_and_independently_replayable(
    repositories: tuple[
        PostgresRiskDecisionRepository,
        PostgresPaperExecutionRepository,
        PersistentSimulatedBroker,
        AsyncEngine,
        str,
    ],
) -> None:
    risks, executions, broker, engine, schema = repositories
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
    assert tuple(update.state for update in resting_updates) == (
        PaperOrderState.SUBMITTED,
    )

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
                text(
                    f"UPDATE {schema}.simulated_broker_facts "
                    "SET update_payload = '{}'::jsonb"
                )
            )


@pytest.mark.asyncio
async def test_independent_account_projections_detect_and_close_callback_gap(
    repositories: tuple[
        PostgresRiskDecisionRepository,
        PostgresPaperExecutionRepository,
        PersistentSimulatedBroker,
        AsyncEngine,
        str,
    ],
) -> None:
    risks, executions, broker, _, _ = repositories
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
