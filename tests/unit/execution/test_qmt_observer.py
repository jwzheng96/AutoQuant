from __future__ import annotations

import inspect
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import SecretStr

from autoquant.errors import BrokerStateUnknownError
from autoquant.execution.models import ZERO_HASH
from autoquant.execution.qmt_callback_coordinator import (
    QmtCallbackCoordinationResult,
)
from autoquant.execution.qmt_callback_reconciliation import (
    QmtCallbackReconciliationReport,
    QmtCallbackReconciliationState,
)
from autoquant.execution.qmt_callback_reducer_store import (
    QmtCallbackReductionResult,
)
from autoquant.execution.qmt_observer import QmtReadOnlyObserver
from autoquant.execution.qmt_readonly import (
    build_qmt_readonly_baseline,
    normalize_qmt_asset,
)
from autoquant.execution.qmt_readonly_store import (
    QmtReadOnlyAcceptanceEvidence,
)
from autoquant.execution.qmt_session_store import QmtSessionLease
from autoquant.execution.qmt_windows_readonly import QmtReadOnlyAcceptance
from autoquant.operations import run_qmt_observer

ACCOUNT = "paper-main"
BROKER_ACCOUNT = "broker-account"
HOLDER = "windows-observer-01"
SESSION = 20260724
TOKEN = SecretStr("observer-lease-token-value-0000000001")
NOW = datetime(2026, 7, 24, 2, tzinfo=UTC)


def _reduction(cursor: int) -> QmtCallbackReductionResult:
    return QmtCallbackReductionResult(
        account_id=ACCOUNT,
        gateway_holder_id=HOLDER,
        qmt_session_id=SESSION,
        qmt_lease_generation=1,
        records=(),
        projections=(),
        trade_facts=(),
        last_local_sequence=cursor,
        last_processing_hash=ZERO_HASH if cursor == 0 else "1" * 64,
        broker_state_known=True,
        fatal_reason=None,
    )


def _query(cursor: int) -> QmtReadOnlyAcceptance:
    baseline = build_qmt_readonly_baseline(
        baseline_id=f"observer-baseline-{cursor}",
        generation=1,
        logical_account_id=ACCOUNT,
        query_started_at=NOW,
        query_completed_at=NOW + timedelta(milliseconds=10),
        callback_cursor_before=cursor,
        callback_cursor_after=cursor,
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
            observed_at=NOW + timedelta(milliseconds=10),
        ),
        positions=(),
        orders=(),
        trades=(),
    )
    return QmtReadOnlyAcceptance(
        baseline=baseline,
        package_manifest_hash="2" * 64,
    )


def _lease() -> QmtSessionLease:
    return QmtSessionLease(
        session_id=SESSION,
        holder_id=HOLDER,
        token_hash="3" * 64,
        acquired_at=NOW - timedelta(seconds=1),
        heartbeat_at=NOW,
        expires_at=NOW + timedelta(minutes=1),
        released_at=None,
        generation=1,
        version=1,
        event_sequence=1,
        last_event_hash="4" * 64,
    )


class FakeSession:
    def __init__(self, *, fail_once: bool = False) -> None:
        self.fail_once = fail_once
        self.queries: list[int] = []

    def query_preserving_callbacks(
        self,
        *,
        expected_callback_cursor: int,
    ) -> QmtReadOnlyAcceptance:
        self.queries.append(expected_callback_cursor)
        if self.fail_once:
            self.fail_once = False
            raise BrokerStateUnknownError("callback changed")
        return _query(expected_callback_cursor)


class AlwaysChangingSession:
    def query_preserving_callbacks(
        self,
        *,
        expected_callback_cursor: int,
    ) -> QmtReadOnlyAcceptance:
        raise BrokerStateUnknownError(f"callback changed after {expected_callback_cursor}")


class FakeCallbacks:
    def __init__(self, cursors: tuple[int, ...]) -> None:
        self._cursors = iter(cursors)
        self.calls = 0

    async def persist_and_process(
        self,
        *,
        lease_token: SecretStr,
        limit: int = 1000,
    ) -> QmtCallbackCoordinationResult:
        assert lease_token is TOKEN
        assert limit == 1000
        self.calls += 1
        return QmtCallbackCoordinationResult(
            persisted_events=(),
            async_bindings=(),
            state_reduction=_reduction(next(self._cursors)),
        )


class FakeLease:
    async def verify(self) -> QmtSessionLease:
        return _lease()


class FakeAcceptances:
    def __init__(self) -> None:
        self.items: list[QmtReadOnlyAcceptanceEvidence] = []

    async def append(
        self,
        evidence: QmtReadOnlyAcceptanceEvidence,
        *,
        now: datetime,
    ) -> QmtReadOnlyAcceptanceEvidence:
        assert now == evidence.observed_at
        self.items.append(evidence)
        return evidence


class FakeReconciliations:
    def __init__(self) -> None:
        self.items: list[QmtCallbackReconciliationReport] = []

    async def append(
        self,
        report: QmtCallbackReconciliationReport,
        *,
        lease_token: SecretStr,
    ) -> QmtCallbackReconciliationReport:
        assert lease_token is TOKEN
        self.items.append(report)
        return report


@pytest.mark.asyncio
async def test_observer_persists_current_query_and_passed_reconciliation() -> None:
    session = FakeSession()
    callbacks = FakeCallbacks((0, 0))
    acceptances = FakeAcceptances()
    reconciliations = FakeReconciliations()
    observer = QmtReadOnlyObserver(
        session=session,
        callbacks=callbacks,
        lease=FakeLease(),
        acceptances=acceptances,
        reconciliations=reconciliations,
        lease_token=TOKEN,
    )

    result = await observer.observe()

    assert result.attempts == 1
    assert result.callback_cursor == 0
    assert result.report.state is QmtCallbackReconciliationState.PASSED
    assert result.live_trading_locked is True
    assert result.broker_mutation_allowed is False
    assert len(acceptances.items) == 1
    assert reconciliations.items == [result.report]


@pytest.mark.asyncio
async def test_observer_persists_racing_callback_before_retrying_query() -> None:
    session = FakeSession(fail_once=True)
    callbacks = FakeCallbacks((0, 1, 1, 1))
    observer = QmtReadOnlyObserver(
        session=session,
        callbacks=callbacks,
        lease=FakeLease(),
        acceptances=FakeAcceptances(),
        reconciliations=FakeReconciliations(),
        lease_token=TOKEN,
    )

    result = await observer.observe()

    assert result.attempts == 2
    assert result.callback_cursor == 1
    assert session.queries == [0, 1]
    assert callbacks.calls == 4
    assert result.report.state is QmtCallbackReconciliationState.PASSED


def test_resident_observer_assembly_has_no_broker_mutation_path() -> None:
    source = inspect.getsource(run_qmt_observer)

    assert "LockedQmtGateway" in source
    for forbidden in (
        "order_stock",
        "submit_order",
        "cancel_order",
        "order_stock_async",
    ):
        assert forbidden not in source


@pytest.mark.asyncio
async def test_observer_fails_closed_after_bounded_query_races() -> None:
    acceptances = FakeAcceptances()
    reconciliations = FakeReconciliations()
    observer = QmtReadOnlyObserver(
        session=AlwaysChangingSession(),
        callbacks=FakeCallbacks((0, 1, 1, 2, 2, 3)),
        lease=FakeLease(),
        acceptances=acceptances,
        reconciliations=reconciliations,
        lease_token=TOKEN,
        max_query_attempts=3,
    )

    with pytest.raises(
        BrokerStateUnknownError,
        match="could not obtain",
    ):
        await observer.observe()

    assert acceptances.items == []
    assert reconciliations.items == []
