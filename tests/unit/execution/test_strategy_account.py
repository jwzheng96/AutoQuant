from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.control import KillSwitchControl, KillSwitchReason
from autoquant.execution.reconciliation import (
    ReconciliationCode,
    ReconciliationReport,
)
from autoquant.execution.session_risk import PaperSessionRiskState
from autoquant.execution.strategy_account import PaperStrategyAccountReader

NOW = datetime(2026, 7, 23, 1, 30, tzinfo=UTC)
SESSION_DATE = date(2026, 7, 23)
ZERO_HASH = "0" * 64


def _control(*, active: bool = False) -> KillSwitchControl:
    return KillSwitchControl(
        account_id="paper-main",
        active=active,
        version=1,
        reason=(
            KillSwitchReason.DEPENDENCY_UNAVAILABLE
            if active
            else KillSwitchReason.RESET_APPROVED
        ),
        changed_at=datetime(2026, 7, 23, 1, tzinfo=UTC),
        changed_by="test",
        last_event_hash=ZERO_HASH,
    )


def _dependencies() -> tuple[MagicMock, MagicMock, MagicMock, MagicMock]:
    executions = MagicMock()
    executions.account_histories = AsyncMock(return_value=())
    executions.save_reconciliation = AsyncMock()
    broker = MagicMock()
    broker.account_histories = AsyncMock(return_value=())
    controls = MagicMock()
    controls.get = AsyncMock(return_value=_control())
    controls.activate = AsyncMock(return_value=_control(active=True))

    @asynccontextmanager
    async def lock(*, account_id: str):  # type: ignore[no-untyped-def]
        assert account_id == "paper-main"
        yield

    controls.coordination_lock = lock
    sessions = MagicMock()

    async def observe(observation):  # type: ignore[no-untyped-def]
        return PaperSessionRiskState(
            account_id=observation.account_id,
            session_date=observation.session_date,
            day_start_equity=observation.equity,
            peak_equity=observation.equity,
            cumulative_turnover=observation.cumulative_turnover,
            as_of=observation.as_of,
            latest_observation_hash=observation.observation_hash,
            latest_snapshot_hash=observation.snapshot_hash,
            turnover_evidence_hash=observation.turnover_evidence_hash,
            version=1,
            last_event_hash="e" * 64,
        )

    sessions.observe = AsyncMock(side_effect=observe)
    return executions, broker, controls, sessions


@pytest.mark.asyncio
async def test_strategy_account_reader_reconciles_and_commits_session_state() -> None:
    executions, broker, controls, sessions = _dependencies()
    reader = PaperStrategyAccountReader(
        account_id="paper-main",
        initial_cash=Decimal("1000000"),
        executions=executions,
        controls=controls,
        broker=broker,
        sessions=sessions,
    )

    evidence = await reader(
        "paper-main",
        SESSION_DATE,
        {"600000.XSHG": Decimal("10")},
        NOW,
    )

    assert evidence.account.cash == Decimal("1000000")
    assert evidence.account.equity == Decimal("1000000")
    assert evidence.account.reconciled is True
    assert evidence.account.kill_switch is False
    assert len(evidence.evidence_hash) == 64
    executions.save_reconciliation.assert_awaited_once()
    sessions.observe.assert_awaited_once()
    controls.activate.assert_not_awaited()


@pytest.mark.asyncio
async def test_strategy_account_reader_activates_kill_switch_on_mismatch() -> None:
    executions, broker, controls, sessions = _dependencies()
    reconciler = MagicMock()
    reconciler.reconcile = MagicMock(
        return_value=ReconciliationReport(
            account_id="paper-main",
            evaluated_at=NOW,
            internal_snapshot_hash="a" * 64,
            broker_snapshot_hash="b" * 64,
            issues=(ReconciliationCode.CASH_MISMATCH,),
        )
    )
    reader = PaperStrategyAccountReader(
        account_id="paper-main",
        initial_cash=Decimal("1000000"),
        executions=executions,
        controls=controls,
        broker=broker,
        sessions=sessions,
        reconciler=reconciler,
    )

    with pytest.raises(PersistenceUnavailableError, match="do not reconcile"):
        await reader(
            "paper-main",
            SESSION_DATE,
            {"600000.XSHG": Decimal("10")},
            NOW,
        )

    controls.activate.assert_awaited_once()
    assert controls.activate.await_args.kwargs["reason"] is (
        KillSwitchReason.RECONCILIATION_FAILED
    )
    sessions.observe.assert_not_awaited()
