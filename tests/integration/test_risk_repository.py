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
NOW = datetime(2026, 7, 22, 2, tzinfo=UTC)
INSTRUMENT = "000001.XSHE"
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not POSTGRES_DSN,
        reason="AQ_POSTGRES_DSN is not configured; PostgreSQL infrastructure unavailable",
    ),
]


@pytest_asyncio.fixture
async def repository() -> AsyncIterator[tuple[PostgresRiskDecisionRepository, AsyncEngine, str]]:
    schema = f"autoquant_test_{uuid4().hex}"
    engine = create_async_engine(POSTGRES_DSN, pool_pre_ping=True)
    control = PostgresControlRepository(engine=engine, schema=schema)
    risks = PostgresRiskDecisionRepository(engine=engine, schema=schema)
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/007_risk_decisions.sql",
        )
    )
    try:
        await control.initialize(migration)
        yield risks, engine, schema
    finally:
        try:
            await control.drop_test_schema()
        finally:
            await engine.dispose()


def _decision(*, quantity: int = 100):  # type: ignore[no-untyped-def]
    policy = RiskPolicy(allowed_instruments=(INSTRUMENT,))
    account = RiskAccountState(
        account_id="paper-main",
        as_of=NOW,
        cash=Decimal("1000000"),
        equity=Decimal("1000000"),
        day_start_equity=Decimal("1000000"),
        peak_equity=Decimal("1000000"),
        gross_exposure=Decimal("0"),
        daily_turnover=Decimal("0"),
        open_order_count=0,
        reconciled=True,
        kill_switch=False,
    )
    order = ProposedOrder(
        client_order_id="integration-paper-order-0001",
        instrument=INSTRUMENT,
        side=OrderSide.BUY,
        quantity=quantity,
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
            policy=policy,
            account=account,
            order=order,
            quote=quote,
            rules=rules,
            now=NOW,
        )
    )


@pytest.mark.asyncio
async def test_risk_decision_is_idempotent_integrity_checked_and_append_only(
    repository: tuple[PostgresRiskDecisionRepository, AsyncEngine, str],
) -> None:
    risks, engine, schema = repository
    decision = _decision()

    created = await risks.append(decision)
    repeated = await risks.append(decision)

    assert repeated.decision_hash == created.decision_hash
    assert await risks.count() == 1
    assert (await risks.list_recent())[0].violations == ()
    loaded = await risks.get(
        account_id=decision.account_id,
        client_order_id=decision.order.client_order_id,
    )
    assert loaded == created
    with pytest.raises(LookupError, match="not found"):
        await risks.get(
            account_id=decision.account_id,
            client_order_id="missing-risk-decision",
        )

    with pytest.raises(ValueError, match="another risk decision"):
        await risks.append(_decision(quantity=200))
    assert await risks.count() == 1

    with pytest.raises(DBAPIError):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"UPDATE {schema}.risk_decisions "
                    "SET state = 'rejected' WHERE decision_hash = :decision_hash"
                ),
                {"decision_hash": decision.decision_hash},
            )
