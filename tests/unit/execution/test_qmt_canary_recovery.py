from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from autoquant.backtest.models import OrderSide
from autoquant.errors import BrokerStateUnknownError
from autoquant.execution.qmt_canary_contract import (
    QmtCanaryOrderCandidate,
    QmtCanaryOrderStage,
)
from autoquant.execution.qmt_canary_recovery import match_qmt_canary_stage
from autoquant.execution.qmt_models import (
    QmtAssetSnapshot,
    QmtOrderObservation,
    QmtOrderStatus,
)
from autoquant.execution.qmt_readonly import QmtReadOnlyBaseline
from autoquant.risk.models import (
    ExecutionMode,
    ProposedOrder,
    RiskDecision,
    RiskDecisionState,
)

NOW = datetime(2026, 7, 23, 1, tzinfo=UTC)


def _stage() -> QmtCanaryOrderStage:
    order = ProposedOrder(
        client_order_id="canary-order-recovery-0001",
        instrument="600000.XSHG",
        side=OrderSide.BUY,
        quantity=100,
        submitted_at=NOW - timedelta(seconds=1),
        limit_price=Decimal("10"),
    )
    decision = RiskDecision(
        account_id="canary-account",
        mode=ExecutionMode.LIVE,
        order=order,
        evaluated_at=NOW - timedelta(seconds=1),
        state=RiskDecisionState.ACCEPTED,
        violations=(),
        policy_hash="b" * 64,
        account_state_hash="c" * 64,
        quote_hash="d" * 64,
        rules_version="canary-risk-v1",
        estimated_price=Decimal("10"),
        order_notional=Decimal("1000"),
        projected_cash=Decimal("99000"),
        projected_gross_exposure=Decimal("0.01"),
        projected_position_weight=Decimal("0.01"),
        projected_daily_turnover=Decimal("0.01"),
    )
    candidate = QmtCanaryOrderCandidate(
        account_id="canary-account",
        strategy_id="low-volatility-v5",
        gateway_holder_id="windows-qmt-canary-01",
        qmt_session_id=20260723,
        qmt_lease_generation=1,
        decision=decision,
        promotion_report_hash="a" * 64,
        compliance_approval_hash="e" * 64,
        qmt_acceptance_hash="f" * 64,
        reconciliation_report_hash="1" * 64,
        maximum_order_notional=Decimal("2000"),
        created_at=NOW,
        valid_until=NOW + timedelta(seconds=30),
    )
    return QmtCanaryOrderStage.from_candidate(
        candidate,
        staged_at=NOW + timedelta(seconds=1),
    )


def _order(
    stage: QmtCanaryOrderStage,
    *,
    broker_order_id: str = "88001",
    instrument: str | None = None,
    remark: str | None = None,
) -> QmtOrderObservation:
    return QmtOrderObservation(
        account_id="broker-account",
        client_order_id=f"qmt-unmapped-{broker_order_id}",
        broker_order_id=broker_order_id,
        instrument=stage.instrument if instrument is None else instrument,
        side=stage.side,
        order_volume=stage.quantity,
        traded_volume=0,
        average_traded_price=None,
        order_price=stage.limit_price,
        raw_status=QmtOrderStatus.REPORTED,
        status_message="reported",
        observed_at=NOW + timedelta(seconds=2),
        order_remark=stage.broker_order_remark if remark is None else remark,
    )


def _baseline(
    stage: QmtCanaryOrderStage,
    *,
    orders: tuple[QmtOrderObservation, ...],
    observed_at: datetime = NOW + timedelta(seconds=2),
) -> QmtReadOnlyBaseline:
    return QmtReadOnlyBaseline(
        baseline_id="qmt-recovery-baseline",
        generation=1,
        logical_account_id=stage.account_id,
        query_started_at=observed_at - timedelta(milliseconds=50),
        query_completed_at=observed_at,
        callback_cursor=0,
        asset=QmtAssetSnapshot(
            account_id="broker-account",
            cash=Decimal("100000"),
            frozen_cash=Decimal("0"),
            market_value=Decimal("0"),
            total_asset=Decimal("100000"),
            observed_at=observed_at,
        ),
        positions=(),
        orders=orders,
        trades=(),
    )


def test_exact_same_day_remark_match_builds_non_mutating_recovery() -> None:
    stage = _stage()
    recovery = match_qmt_canary_stage(
        stage,
        _baseline(stage, orders=(_order(stage),)),
    )

    assert recovery is not None
    assert recovery.broker_order_id == "88001"
    assert recovery.client_order_id == stage.client_order_id
    assert recovery.broker_mutation_allowed is False
    assert len(recovery.recovery_hash) == 64


def test_absent_remark_stays_unresolved_instead_of_guessing_no_order() -> None:
    stage = _stage()

    recovery = match_qmt_canary_stage(
        stage,
        _baseline(
            stage,
            orders=(_order(stage, remark="manual-order"),),
        ),
    )

    assert recovery is None


@pytest.mark.parametrize(
    "orders",
    [
        lambda stage: (
            _order(stage, instrument="000001.XSHE"),
        ),
        lambda stage: (
            _order(stage, broker_order_id="88001"),
            _order(stage, broker_order_id="88002"),
        ),
    ],
)
def test_conflicting_or_duplicate_remark_match_fails_closed(
    orders,
) -> None:
    stage = _stage()

    with pytest.raises(BrokerStateUnknownError):
        match_qmt_canary_stage(
            stage,
            _baseline(stage, orders=orders(stage)),
        )


def test_recovery_rejects_a_different_broker_session_date() -> None:
    stage = _stage()
    next_day = NOW + timedelta(days=1)

    with pytest.raises(BrokerStateUnknownError, match="session date"):
        match_qmt_canary_stage(
            stage,
            _baseline(
                stage,
                orders=(),
                observed_at=next_day,
            ),
        )
