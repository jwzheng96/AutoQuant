from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Protocol

from pydantic import SecretStr

from autoquant.errors import BrokerStateUnknownError
from autoquant.execution.qmt_callback_coordinator import (
    QmtCallbackCoordinationResult,
)
from autoquant.execution.qmt_callback_reconciliation import (
    QmtCallbackReconciliationReport,
    reconcile_qmt_callback_state,
)
from autoquant.execution.qmt_lease_guard import run_fenced_blocking
from autoquant.execution.qmt_readonly_store import (
    QmtReadOnlyAcceptanceEvidence,
)
from autoquant.execution.qmt_session_store import QmtSessionLease
from autoquant.execution.qmt_windows_readonly import (
    QmtReadOnlyAcceptance,
)


class QmtDurableReadOnlySession(Protocol):
    def query_preserving_callbacks(
        self,
        *,
        expected_callback_cursor: int,
    ) -> QmtReadOnlyAcceptance: ...


class QmtDurableCallbackCoordinator(Protocol):
    async def persist_and_process(
        self,
        *,
        lease_token: SecretStr,
        limit: int = 1000,
    ) -> QmtCallbackCoordinationResult: ...


class QmtLeaseVerifier(Protocol):
    async def verify(self) -> QmtSessionLease: ...


class QmtAcceptanceWriter(Protocol):
    async def append(
        self,
        evidence: QmtReadOnlyAcceptanceEvidence,
        *,
        now: datetime,
    ) -> QmtReadOnlyAcceptanceEvidence: ...


class QmtReconciliationWriter(Protocol):
    async def append(
        self,
        report: QmtCallbackReconciliationReport,
        *,
        lease_token: SecretStr,
    ) -> QmtCallbackReconciliationReport: ...


@dataclass(frozen=True, slots=True)
class QmtObservationResult:
    acceptance: QmtReadOnlyAcceptanceEvidence
    report: QmtCallbackReconciliationReport
    callback_cursor: int
    attempts: int

    @property
    def live_trading_locked(self) -> bool:
        return True

    @property
    def broker_mutation_allowed(self) -> bool:
        return False


class QmtReadOnlyObserver:
    """Join durable callbacks to one coherent broker query without broker writes."""

    def __init__(
        self,
        *,
        session: QmtDurableReadOnlySession,
        callbacks: QmtDurableCallbackCoordinator,
        lease: QmtLeaseVerifier,
        acceptances: QmtAcceptanceWriter,
        reconciliations: QmtReconciliationWriter,
        lease_token: SecretStr,
        max_query_attempts: int = 3,
    ) -> None:
        if (
            not isinstance(max_query_attempts, int)
            or isinstance(max_query_attempts, bool)
            or max_query_attempts < 1
        ):
            raise ValueError("QMT observer max_query_attempts must be positive")
        if len(lease_token.get_secret_value()) < 32:
            raise ValueError("QMT observer lease token must contain at least 32 characters")
        self._session = session
        self._callbacks = callbacks
        self._lease = lease
        self._acceptances = acceptances
        self._reconciliations = reconciliations
        self._lease_token = lease_token
        self._max_query_attempts = max_query_attempts

    async def observe(self) -> QmtObservationResult:
        last_error: BrokerStateUnknownError | None = None
        for attempt in range(1, self._max_query_attempts + 1):
            before = await self._callbacks.persist_and_process(
                lease_token=self._lease_token,
            )
            before_reduction = before.state_reduction
            if before_reduction is None:
                raise BrokerStateUnknownError("QMT observer requires the durable callback reducer")
            expected_cursor = before_reduction.last_local_sequence
            try:
                query = await run_fenced_blocking(
                    partial(
                        self._session.query_preserving_callbacks,
                        expected_callback_cursor=expected_cursor,
                    )
                )
            except BrokerStateUnknownError as error:
                last_error = error
                await self._callbacks.persist_and_process(
                    lease_token=self._lease_token,
                )
                continue
            after = await self._callbacks.persist_and_process(
                lease_token=self._lease_token,
            )
            reduction = after.state_reduction
            if reduction is None:
                raise BrokerStateUnknownError("QMT observer requires the durable callback reducer")
            baseline = query.baseline
            if baseline.callback_cursor != reduction.last_local_sequence:
                last_error = BrokerStateUnknownError(
                    "QMT callbacks advanced after the coherent broker query"
                )
                continue
            active_lease = await self._lease.verify()
            acceptance = QmtReadOnlyAcceptanceEvidence.from_baseline(
                baseline=baseline,
                package_manifest_hash=query.package_manifest_hash,
                lease=active_lease,
            )
            acceptance = await self._acceptances.append(
                acceptance,
                now=baseline.query_completed_at,
            )
            report = reconcile_qmt_callback_state(
                baseline=baseline,
                acceptance=acceptance,
                reduction=reduction,
            )
            report = await self._reconciliations.append(
                report,
                lease_token=self._lease_token,
            )
            return QmtObservationResult(
                acceptance=acceptance,
                report=report,
                callback_cursor=reduction.last_local_sequence,
                attempts=attempt,
            )
        raise BrokerStateUnknownError(
            "QMT observer could not obtain a callback-stable broker query"
        ) from last_error


__all__ = [
    "QmtObservationResult",
    "QmtReadOnlyObserver",
]
