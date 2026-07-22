from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.execution.control import KillSwitchReason
from autoquant.execution.control_store import PostgresExecutionControlRepository
from autoquant.execution.reconciliation import (
    AccountReconciler,
    ExecutionAccountSnapshot,
)
from autoquant.execution.store import PostgresPaperExecutionRepository

POSTGRES_DSN = os.environ.get("AQ_POSTGRES_DSN", "").strip()
NOW = datetime(2026, 7, 22, 4, tzinfo=UTC)
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
        PostgresExecutionControlRepository,
        PostgresPaperExecutionRepository,
        AsyncEngine,
        str,
    ]
]:
    schema = f"autoquant_test_{uuid4().hex}"
    engine = create_async_engine(POSTGRES_DSN, pool_pre_ping=True)
    control_schema = PostgresControlRepository(engine=engine, schema=schema)
    controls = PostgresExecutionControlRepository(engine=engine, schema=schema)
    executions = PostgresPaperExecutionRepository(engine=engine, schema=schema)
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/007_risk_decisions.sql",
            "migrations/postgres/008_paper_execution.sql",
            "migrations/postgres/009_execution_controls.sql",
        )
    )
    try:
        await control_schema.initialize(migration)
        yield controls, executions, engine, schema
    finally:
        try:
            await control_schema.drop_test_schema()
        finally:
            await engine.dispose()


def _snapshot(*, as_of: datetime) -> ExecutionAccountSnapshot:
    return ExecutionAccountSnapshot(
        account_id="paper-main",
        as_of=as_of,
        cash=Decimal("100000"),
        equity=Decimal("100000"),
    )


@pytest.mark.asyncio
async def test_kill_switch_is_fail_closed_replayable_and_evidence_gated(
    repositories: tuple[
        PostgresExecutionControlRepository,
        PostgresPaperExecutionRepository,
        AsyncEngine,
        str,
    ],
) -> None:
    controls, executions, engine, schema = repositories
    initialized = await controls.ensure_fail_closed(account_id="paper-main", now=NOW)
    repeated = await controls.ensure_fail_closed(account_id="paper-main", now=NOW)

    assert initialized == repeated
    assert initialized.active is True
    assert initialized.reason is KillSwitchReason.INITIALIZING

    manual = await controls.activate(
        account_id="paper-main",
        command_id="manual-activation-0001",
        reason=KillSwitchReason.MANUAL,
        actor="operator",
        now=NOW + timedelta(seconds=1),
    )
    repeated_activation = await controls.activate(
        account_id="paper-main",
        command_id="manual-activation-noop-0001",
        reason=KillSwitchReason.MANUAL,
        actor="operator",
        now=NOW + timedelta(seconds=1),
    )
    assert manual.version == 2
    assert repeated_activation.version == 3

    snapshot = _snapshot(as_of=NOW + timedelta(seconds=2))
    report = AccountReconciler().reconcile(
        internal=snapshot,
        broker=snapshot,
        now=NOW + timedelta(seconds=2),
    )
    await executions.save_reconciliation(
        internal=snapshot,
        broker=snapshot,
        report=report,
    )
    reset = await controls.reset(
        account_id="paper-main",
        command_id="approved-reset-command-0001",
        actor="operator",
        now=NOW + timedelta(seconds=3),
        expected_version=repeated_activation.version,
        reconciliation_report_hash=report.report_hash,
        recovery_verified=True,
    )

    assert reset.active is False
    assert reset.reason is KillSwitchReason.RESET_APPROVED
    assert await controls.replay(account_id="paper-main") == reset

    drill_time = NOW + timedelta(seconds=4)
    drill = await controls.activate(
        account_id="paper-main",
        command_id="kill-switch-drill-0001",
        reason=KillSwitchReason.DRILL,
        actor="operator",
        now=drill_time,
    )
    duplicate = await controls.activate(
        account_id="paper-main",
        command_id="kill-switch-drill-0001",
        reason=KillSwitchReason.DRILL,
        actor="operator",
        now=drill_time,
    )
    assert duplicate == drill
    assert await controls.replay(account_id="paper-main") == drill

    with pytest.raises(ValueError, match="another command"):
        await controls.activate(
            account_id="paper-main",
            command_id="kill-switch-drill-0001",
            reason=KillSwitchReason.MANUAL,
            actor="operator",
            now=drill_time,
        )
    with pytest.raises(ValueError, match="stale"):
        await controls.reset(
            account_id="paper-main",
            command_id="stale-reset-command-0001",
            actor="operator",
            now=NOW + timedelta(seconds=30),
            expected_version=drill.version,
            reconciliation_report_hash=report.report_hash,
            recovery_verified=True,
        )

    async with engine.connect() as connection:
        event_count = await connection.scalar(
            text(f"SELECT count(*) FROM {schema}.execution_control_events")
        )
    assert event_count == 5

    with pytest.raises(DBAPIError):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"UPDATE {schema}.execution_control_events "
                    "SET reason = 'manual' WHERE account_id = 'paper-main'"
                )
            )
    with pytest.raises(DBAPIError):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"UPDATE {schema}.execution_control_state "
                    "SET account_id = 'paper-other' WHERE account_id = 'paper-main'"
                )
            )
