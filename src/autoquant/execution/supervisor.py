from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

from autoquant.execution.control import KillSwitchControl, KillSwitchReason
from autoquant.execution.control_store import PostgresExecutionControlRepository
from autoquant.execution.reconciliation import (
    AccountReconciler,
    ExecutionAccountSnapshot,
    ReconciliationReport,
)
from autoquant.execution.store import PostgresPaperExecutionRepository

SnapshotReader = Callable[[str, datetime], Awaitable[ExecutionAccountSnapshot]]


@dataclass(frozen=True, slots=True)
class ReconciliationCycleResult:
    status: str
    control: KillSwitchControl
    report: ReconciliationReport | None
    error_code: str | None


class ReconciliationSupervisor:
    """One fail-closed cycle; scheduling and concrete adapters stay outside."""

    def __init__(
        self,
        *,
        account_id: str,
        internal_reader: SnapshotReader,
        broker_reader: SnapshotReader,
        execution_repository: PostgresPaperExecutionRepository,
        control_repository: PostgresExecutionControlRepository,
        reconciler: AccountReconciler | None = None,
    ) -> None:
        if not account_id.strip():
            raise ValueError("account_id cannot be empty")
        self._account_id = account_id
        self._internal_reader = internal_reader
        self._broker_reader = broker_reader
        self._executions = execution_repository
        self._controls = control_repository
        self._reconciler = reconciler or AccountReconciler()

    async def run_once(self, *, now: datetime) -> ReconciliationCycleResult:
        try:
            internal = await self._internal_reader(self._account_id, now)
            broker = await self._broker_reader(self._account_id, now)
            report = self._reconciler.reconcile(
                internal=internal,
                broker=broker,
                now=now,
            )
            await self._executions.save_reconciliation(
                internal=internal,
                broker=broker,
                report=report,
            )
        except Exception:
            control = await self._controls.activate(
                account_id=self._account_id,
                command_id=f"reconcile-dependency-{uuid4()}",
                reason=KillSwitchReason.DEPENDENCY_UNAVAILABLE,
                actor="reconciliation-supervisor",
                now=now,
            )
            return ReconciliationCycleResult(
                status="failed",
                control=control,
                report=None,
                error_code="reconciliation_dependency_failed",
            )
        if not report.reconciled:
            control = await self._controls.activate(
                account_id=self._account_id,
                command_id=f"reconcile-mismatch-{uuid4()}",
                reason=KillSwitchReason.RECONCILIATION_FAILED,
                actor="reconciliation-supervisor",
                now=now,
                evidence_hash=report.report_hash,
            )
            return ReconciliationCycleResult(
                status="failed",
                control=control,
                report=report,
                error_code="reconciliation_mismatch",
            )
        control = await self._controls.get(account_id=self._account_id)
        return ReconciliationCycleResult(
            status="reconciled",
            control=control,
            report=report,
            error_code=None,
        )
