from __future__ import annotations

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
from autoquant.execution.models import (
    ApprovedPaperOrder,
    BrokerOrderUpdate,
    PaperOrderState,
)
from autoquant.execution.reconciliation import (
    AccountPosition,
    AccountReconciler,
    ExecutionAccountSnapshot,
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
NOW = datetime(2026, 7, 22, 3, tzinfo=UTC)
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
    tuple[PostgresRiskDecisionRepository, PostgresPaperExecutionRepository, AsyncEngine, str]
]:
    schema = f"autoquant_test_{uuid4().hex}"
    engine = create_async_engine(POSTGRES_DSN, pool_pre_ping=True)
    control = PostgresControlRepository(engine=engine, schema=schema)
    risks = PostgresRiskDecisionRepository(engine=engine, schema=schema)
    executions = PostgresPaperExecutionRepository(engine=engine, schema=schema)
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/007_risk_decisions.sql",
            "migrations/postgres/008_paper_execution.sql",
        )
    )
    try:
        await control.initialize(migration)
        yield risks, executions, engine, schema
    finally:
        try:
            await control.drop_test_schema()
        finally:
            await engine.dispose()


def _risk_decision():  # type: ignore[no-untyped-def]
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
    proposed = ProposedOrder(
        client_order_id="paper-integration-order-0001",
        instrument=INSTRUMENT,
        side=OrderSide.BUY,
        quantity=100,
        submitted_at=NOW,
    )
    quote = MarketQuote(
        instrument=INSTRUMENT,
        as_of=NOW,
        last_price=Decimal("10"),
        bid_price=Decimal("9.99"),
        ask_price=Decimal("10.01"),
        market_open=True,
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
            order=proposed,
            quote=quote,
            rules=rules,
            now=NOW,
        )
    )


def _update(
    sequence: int,
    state: PaperOrderState,
    *,
    filled: int = 0,
    price: str | None = None,
) -> BrokerOrderUpdate:
    return BrokerOrderUpdate(
        account_id="paper-main",
        client_order_id="paper-integration-order-0001",
        broker_order_id="simulated-broker-order-0001",
        broker_sequence=sequence,
        state=state,
        cumulative_filled_quantity=filled,
        average_fill_price=None if price is None else Decimal(price),
        occurred_at=NOW + timedelta(seconds=sequence),
    )


def _snapshot() -> ExecutionAccountSnapshot:
    return ExecutionAccountSnapshot(
        account_id="paper-main",
        as_of=NOW,
        cash=Decimal("99000"),
        equity=Decimal("100000"),
        positions=(
            AccountPosition(
                instrument=INSTRUMENT,
                total_quantity=100,
                sellable_quantity=0,
                market_value=Decimal("1000"),
            ),
        ),
    )


@pytest.mark.asyncio
async def test_paper_order_and_reconciliation_survive_verified_replay(
    repositories: tuple[
        PostgresRiskDecisionRepository,
        PostgresPaperExecutionRepository,
        AsyncEngine,
        str,
    ],
) -> None:
    risks, executions, engine, schema = repositories
    decision = _risk_decision()
    await risks.append(decision)
    order = ApprovedPaperOrder.from_risk_decision(decision)

    created = await executions.create_order(order)
    repeated = await executions.create_order(order)
    submitted_update = _update(10, PaperOrderState.SUBMITTED)
    submitted = await executions.apply_update(
        account_id=order.account_id,
        client_order_id=order.client_order_id,
        update=submitted_update,
    )
    partial = await executions.apply_update(
        account_id=order.account_id,
        client_order_id=order.client_order_id,
        update=_update(
            20,
            PaperOrderState.PARTIALLY_FILLED,
            filled=40,
            price="10.02",
        ),
    )
    historical_duplicate = await executions.apply_update(
        account_id=order.account_id,
        client_order_id=order.client_order_id,
        update=submitted_update,
    )

    assert created == repeated
    assert submitted.applied is True
    assert partial.projection.version == 2
    assert historical_duplicate.applied is False
    assert historical_duplicate.projection == partial.projection
    assert await executions.load_order(
        account_id=order.account_id,
        client_order_id=order.client_order_id,
    ) == partial.projection
    assert await executions.replay_order(
        account_id=order.account_id,
        client_order_id=order.client_order_id,
    ) == partial.projection

    with pytest.raises(ValueError, match="sequence already belongs"):
        await executions.apply_update(
            account_id=order.account_id,
            client_order_id=order.client_order_id,
            update=_update(10, PaperOrderState.UNKNOWN),
        )

    snapshot = _snapshot()
    report = AccountReconciler().reconcile(
        internal=snapshot,
        broker=snapshot,
        now=NOW,
    )
    assert await executions.save_reconciliation(
        internal=snapshot,
        broker=snapshot,
        report=report,
    ) == report
    assert await executions.save_reconciliation(
        internal=snapshot,
        broker=snapshot,
        report=report,
    ) == report

    async with engine.connect() as connection:
        event_count = await connection.scalar(
            text(f"SELECT count(*) FROM {schema}.paper_order_events")
        )
        snapshot_count = await connection.scalar(
            text(f"SELECT count(*) FROM {schema}.execution_account_snapshots")
        )
        report_count = await connection.scalar(
            text(f"SELECT count(*) FROM {schema}.execution_reconciliation_reports")
        )
    assert event_count == 2
    assert snapshot_count == 1
    assert report_count == 1

    with pytest.raises(DBAPIError):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"UPDATE {schema}.paper_order_events "
                    "SET resulting_state = 'unknown' WHERE order_hash = :order_hash"
                ),
                {"order_hash": order.order_hash},
            )

    with pytest.raises(DBAPIError):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"UPDATE {schema}.paper_orders "
                    "SET order_payload = '{}'::jsonb WHERE order_hash = :order_hash"
                ),
                {"order_hash": order.order_hash},
            )
