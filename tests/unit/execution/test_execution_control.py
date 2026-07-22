from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from autoquant.execution.control import (
    KillSwitchAction,
    KillSwitchCommand,
    KillSwitchReason,
    apply_kill_switch_command,
)
from autoquant.execution.reconciliation import ExecutionAccountSnapshot
from autoquant.execution.supervisor import ReconciliationSupervisor

NOW = datetime(2026, 7, 22, 4, tzinfo=UTC)


def test_kill_switch_initializes_active_and_requires_evidence_for_reset() -> None:
    initialize = KillSwitchCommand(
        command_id="initialize-control-0001",
        account_id="paper-main",
        action=KillSwitchAction.INITIALIZE,
        reason=KillSwitchReason.INITIALIZING,
        actor="system",
        occurred_at=NOW,
    )
    state, event = apply_kill_switch_command(None, initialize)

    assert state.active is True
    assert state.version == 1
    assert state.last_event_hash == event.event_hash
    with pytest.raises(ValueError, match="evidence"):
        KillSwitchCommand(
            command_id="reset-control-000001",
            account_id="paper-main",
            action=KillSwitchAction.RESET,
            reason=KillSwitchReason.RESET_APPROVED,
            actor="operator",
            occurred_at=NOW,
        )


def _snapshot(*, cash: str = "100000") -> ExecutionAccountSnapshot:
    return ExecutionAccountSnapshot(
        account_id="paper-main",
        as_of=NOW,
        cash=Decimal(cash),
        equity=Decimal("100000"),
    )


@pytest.mark.asyncio
async def test_supervisor_keeps_switch_active_after_passing_reconciliation() -> None:
    snapshot = _snapshot()
    controls = MagicMock()
    active = MagicMock()
    active.active = True
    controls.get = AsyncMock(return_value=active)
    controls.activate = AsyncMock()
    executions = MagicMock()
    executions.save_reconciliation = AsyncMock()
    supervisor = ReconciliationSupervisor(
        account_id="paper-main",
        internal_reader=AsyncMock(return_value=snapshot),
        broker_reader=AsyncMock(return_value=snapshot),
        execution_repository=executions,
        control_repository=controls,
    )

    result = await supervisor.run_once(now=NOW)

    assert result.status == "reconciled"
    assert result.report is not None and result.report.reconciled
    controls.activate.assert_not_awaited()


@pytest.mark.asyncio
async def test_supervisor_activates_switch_for_mismatch_and_dependency_failure() -> None:
    internal = _snapshot()
    mismatched = _snapshot(cash="90000")
    controls = MagicMock()
    activated = MagicMock()
    activated.active = True
    controls.activate = AsyncMock(return_value=activated)
    executions = MagicMock()
    executions.save_reconciliation = AsyncMock()
    mismatch_supervisor = ReconciliationSupervisor(
        account_id="paper-main",
        internal_reader=AsyncMock(return_value=internal),
        broker_reader=AsyncMock(return_value=mismatched),
        execution_repository=executions,
        control_repository=controls,
    )

    mismatch = await mismatch_supervisor.run_once(now=NOW)

    assert mismatch.error_code == "reconciliation_mismatch"
    assert controls.activate.await_args.kwargs["reason"] is (
        KillSwitchReason.RECONCILIATION_FAILED
    )

    controls.activate.reset_mock()
    dependency_supervisor = ReconciliationSupervisor(
        account_id="paper-main",
        internal_reader=AsyncMock(side_effect=RuntimeError("secret dependency detail")),
        broker_reader=AsyncMock(),
        execution_repository=executions,
        control_repository=controls,
    )
    dependency = await dependency_supervisor.run_once(now=NOW)

    assert dependency.error_code == "reconciliation_dependency_failed"
    assert "secret dependency detail" not in str(dependency)
    assert controls.activate.await_args.kwargs["reason"] is (
        KillSwitchReason.DEPENDENCY_UNAVAILABLE
    )
