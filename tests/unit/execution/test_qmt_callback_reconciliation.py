from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from autoquant.backtest.models import OrderSide
from autoquant.execution.models import PaperOrderState
from autoquant.execution.qmt_callback_reconciliation import (
    QmtCallbackReconciliationIssue,
    QmtCallbackReconciliationState,
    reconcile_qmt_callback_state,
)
from autoquant.execution.qmt_callback_reducer import (
    QmtBrokerOrderProjection,
    QmtBrokerTradeFact,
    QmtOrderConvergence,
)
from autoquant.execution.qmt_callback_reducer_store import (
    QmtCallbackReductionResult,
)
from autoquant.execution.qmt_models import QmtOrderStatus
from autoquant.execution.qmt_readonly import (
    build_qmt_readonly_baseline,
    normalize_qmt_asset,
    normalize_qmt_order,
    normalize_qmt_trade,
)
from autoquant.execution.qmt_readonly_store import (
    QmtReadOnlyAcceptanceEvidence,
)
from autoquant.execution.qmt_session_store import QmtSessionLease

ACCOUNT = "broker-account"
LOGICAL_ACCOUNT = "canary-account"
HOLDER_ID = "windows-qmt-canary-01"
STARTED = datetime(2026, 7, 24, 1, tzinfo=UTC)
COMPLETED = STARTED + timedelta(milliseconds=50)
REMARK = "AQ1234567890abcdef123456"


def _baseline():
    order = normalize_qmt_order(
        {
            "account_id": ACCOUNT,
            "order_id": 88001,
            "order_remark": REMARK,
            "order_status": QmtOrderStatus.FILLED,
            "order_volume": 100,
            "price": 10,
            "side": "buy",
            "status_msg": "",
            "stock_code": "600000.SH",
            "traded_price": 10,
            "traded_volume": 100,
        },
        expected_account_id=ACCOUNT,
        observed_at=COMPLETED,
        client_order_ids={88001: "canary-order-1"},
    )
    trade = normalize_qmt_trade(
        {
            "account_id": ACCOUNT,
            "order_id": 88001,
            "order_remark": REMARK,
            "side": "buy",
            "stock_code": "600000.SH",
            "traded_amount": 1000,
            "traded_id": "TRADE-001",
            "traded_price": 10,
            "traded_volume": 100,
        },
        expected_account_id=ACCOUNT,
        observed_at=COMPLETED,
    )
    return build_qmt_readonly_baseline(
        baseline_id="baseline-reconciliation-1",
        generation=1,
        logical_account_id=LOGICAL_ACCOUNT,
        query_started_at=STARTED,
        query_completed_at=COMPLETED,
        callback_cursor_before=2,
        callback_cursor_after=2,
        callback_stream_healthy=True,
        asset=normalize_qmt_asset(
            {
                "account_id": ACCOUNT,
                "cash": 1000,
                "frozen_cash": 0,
                "market_value": 0,
                "total_asset": 1000,
            },
            expected_account_id=ACCOUNT,
            observed_at=COMPLETED,
        ),
        positions=(),
        orders=(order,),
        trades=(trade,),
    )


def _acceptance(baseline) -> QmtReadOnlyAcceptanceEvidence:
    lease = QmtSessionLease(
        session_id=20260724,
        holder_id=HOLDER_ID,
        token_hash="a" * 64,
        acquired_at=STARTED - timedelta(minutes=1),
        heartbeat_at=STARTED - timedelta(seconds=1),
        expires_at=COMPLETED + timedelta(minutes=1),
        released_at=None,
        generation=3,
        version=1,
        event_sequence=1,
        last_event_hash="b" * 64,
    )
    return QmtReadOnlyAcceptanceEvidence.from_baseline(
        baseline=baseline,
        package_manifest_hash="c" * 64,
        lease=lease,
    )


def _reduction() -> QmtCallbackReductionResult:
    projection = QmtBrokerOrderProjection(
        account_id=LOGICAL_ACCOUNT,
        candidate_hash="d" * 64,
        client_order_id="canary-order-1",
        broker_order_id="88001",
        instrument="600000.XSHG",
        side=OrderSide.BUY,
        quantity=100,
        limit_price=Decimal("10"),
        order_remark=REMARK,
        reported_traded_volume=100,
        reported_average_price=Decimal("10"),
        raw_order_status=QmtOrderStatus.FILLED,
        order_state=PaperOrderState.FILLED,
        trade_volume=100,
        trade_amount=Decimal("1000"),
        convergence=QmtOrderConvergence.CONVERGED,
        last_callback_sequence=2,
        last_callback_event_hash="e" * 64,
        updated_at=STARTED - timedelta(milliseconds=10),
    )
    fact = QmtBrokerTradeFact(
        account_id=LOGICAL_ACCOUNT,
        candidate_hash="d" * 64,
        client_order_id="canary-order-1",
        broker_order_id="88001",
        trade_id="TRADE-001",
        instrument="600000.XSHG",
        side=OrderSide.BUY,
        volume=100,
        price=Decimal("10"),
        amount=Decimal("1000"),
        order_remark=REMARK,
        callback_event_hash="f" * 64,
        observed_at=STARTED - timedelta(milliseconds=10),
    )
    return QmtCallbackReductionResult(
        account_id=LOGICAL_ACCOUNT,
        gateway_holder_id=HOLDER_ID,
        qmt_session_id=20260724,
        qmt_lease_generation=3,
        records=(),
        projections=(projection,),
        trade_facts=(fact,),
        last_local_sequence=2,
        last_processing_hash="1" * 64,
        broker_state_known=True,
        fatal_reason=None,
    )


def test_query_baseline_independently_proves_callback_state() -> None:
    baseline = _baseline()
    report = reconcile_qmt_callback_state(
        baseline=baseline,
        acceptance=_acceptance(baseline),
        reduction=_reduction(),
    )

    assert report.state is QmtCallbackReconciliationState.PASSED
    assert report.issues == ()
    assert report.matched_broker_order_ids == ("88001",)
    assert report.matched_trade_ids == ("TRADE-001",)
    assert report.broker_mutation_allowed is False


def test_cursor_scope_or_trade_mismatch_rejects_reconciliation() -> None:
    baseline = _baseline()
    reduction = _reduction()
    report = reconcile_qmt_callback_state(
        baseline=baseline,
        acceptance=_acceptance(baseline),
        reduction=replace(
            reduction,
            gateway_holder_id="another-holder",
            last_local_sequence=1,
            trade_facts=(),
        ),
    )

    assert report.state is QmtCallbackReconciliationState.REJECTED
    assert set(report.issues) == {
        QmtCallbackReconciliationIssue.CALLBACK_CURSOR_MISMATCH,
        QmtCallbackReconciliationIssue.LEASE_SCOPE_MISMATCH,
        QmtCallbackReconciliationIssue.UNTRACKED_AUTOQUANT_TRADE,
    }


def test_pending_or_late_callback_cannot_be_reconciled() -> None:
    baseline = _baseline()
    reduction = _reduction()
    projection = replace(
        reduction.projections[0],
        convergence=QmtOrderConvergence.PENDING,
        updated_at=STARTED + timedelta(milliseconds=1),
    )
    report = reconcile_qmt_callback_state(
        baseline=baseline,
        acceptance=_acceptance(baseline),
        reduction=replace(
            reduction,
            projections=(projection,),
            broker_state_known=False,
        ),
    )

    assert {
        QmtCallbackReconciliationIssue.BASELINE_BEFORE_CALLBACK,
        QmtCallbackReconciliationIssue.BROKER_STATE_UNKNOWN,
        QmtCallbackReconciliationIssue.CALLBACK_PROJECTION_PENDING,
    }.issubset(report.issues)
