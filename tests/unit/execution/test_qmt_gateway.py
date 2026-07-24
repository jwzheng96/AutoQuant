from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from autoquant.backtest.models import OrderSide
from autoquant.errors import BrokerStateUnknownError, LiveTradingLockedError
from autoquant.execution.qmt_canary_contract import (
    QmtCanaryOrderCandidate,
    QmtCanaryOrderStage,
    QmtOrderCorrelation,
    QmtOrderCorrelationBook,
    qmt_canary_order_remark,
)
from autoquant.execution.qmt_gateway import (
    LockedQmtGateway,
    QmtCallbackBuffer,
    QmtCallbackKind,
)
from autoquant.risk.models import (
    ExecutionMode,
    ProposedOrder,
    RiskDecision,
    RiskDecisionState,
)

NOW = datetime(2026, 7, 23, 1, 0, tzinfo=UTC)


def _candidate(
    *,
    client_order_id: str = "canary-order-0001",
    valid_until: datetime = NOW + timedelta(seconds=30),
    maximum_order_notional: Decimal = Decimal("2000"),
) -> QmtCanaryOrderCandidate:
    order = ProposedOrder(
        client_order_id=client_order_id,
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
    return QmtCanaryOrderCandidate(
        account_id="canary-account",
        strategy_id="low-volatility-v5",
        gateway_holder_id="windows-qmt-canary-01",
        qmt_session_id=20260723,
        qmt_lease_generation=7,
        decision=decision,
        promotion_report_hash="a" * 64,
        compliance_approval_hash="e" * 64,
        qmt_acceptance_hash="f" * 64,
        reconciliation_report_hash="1" * 64,
        maximum_order_notional=maximum_order_notional,
        created_at=NOW,
        valid_until=valid_until,
    )


def test_callback_capture_preserves_local_order_and_copies_payload() -> None:
    buffer = QmtCallbackBuffer()
    payload = {"order_id": "first"}
    first = buffer.capture(
        QmtCallbackKind.ORDER,
        payload,
        received_at=datetime(2026, 7, 23, 1, 0, tzinfo=UTC),
    )
    payload["order_id"] = "mutated"
    second = buffer.capture(QmtCallbackKind.TRADE, {"trade_id": "second"})

    assert buffer.queued_count == 2
    drained = buffer.drain()

    assert [event.local_sequence for event in drained] == [1, 2]
    assert buffer.cursor == 2
    assert buffer.queued_count == 0
    assert first.payload["order_id"] == "first"
    assert second.local_sequence == 2
    assert buffer.drain() == ()


def test_callback_buffer_validates_limit() -> None:
    with pytest.raises(ValueError, match="positive"):
        QmtCallbackBuffer().drain(limit=0)


def test_callback_buffer_retries_reserved_batch_until_durable_acknowledgement() -> None:
    buffer = QmtCallbackBuffer()
    first = buffer.capture(QmtCallbackKind.ORDER, {"order_id": 1})
    second = buffer.capture(QmtCallbackKind.TRADE, {"trade_id": 2})

    reserved = buffer.reserve_durable(limit=2)
    assert reserved is not None
    assert reserved.events == (first, second)
    with pytest.raises(RuntimeError, match="already has a durable"):
        buffer.reserve_durable()
    with pytest.raises(RuntimeError, match="durable reservation pending"):
        buffer.drain()

    buffer.release_durable(reservation_id=reserved.reservation_id)
    retried = buffer.reserve_durable(limit=1)
    assert retried is not None
    assert retried.reservation_id != reserved.reservation_id
    assert retried.events == reserved.events
    with pytest.raises(RuntimeError, match="not the active"):
        buffer.acknowledge_durable(reservation_id=reserved.reservation_id)

    buffer.acknowledge_durable(reservation_id=retried.reservation_id)
    assert buffer.reserve_durable() is None
    assert buffer.drain() == ()


def test_callback_buffer_restores_cursor_only_before_new_capture() -> None:
    buffer = QmtCallbackBuffer()

    buffer.restore_cursor(local_sequence=7)
    event = buffer.capture(QmtCallbackKind.ACCOUNT_STATUS, {"status": 0})

    assert event.local_sequence == 8
    with pytest.raises(RuntimeError, match="fresh empty"):
        buffer.restore_cursor(local_sequence=8)
    with pytest.raises(ValueError, match="nonnegative"):
        QmtCallbackBuffer().restore_cursor(local_sequence=-1)


def test_callback_buffer_cannot_acknowledge_after_stream_overflow() -> None:
    buffer = QmtCallbackBuffer(capacity=1)
    buffer.capture(QmtCallbackKind.ORDER, {"order_id": 1})
    reserved = buffer.reserve_durable()
    assert reserved is not None
    buffer.capture(QmtCallbackKind.ORDER, {"order_id": 2})
    with pytest.raises(BrokerStateUnknownError, match="overflow"):
        buffer.capture(QmtCallbackKind.ORDER, {"order_id": 3})

    with pytest.raises(BrokerStateUnknownError, match="overflow"):
        buffer.acknowledge_durable(
            reservation_id=reserved.reservation_id,
        )
    buffer.release_durable(reservation_id=reserved.reservation_id)
    with pytest.raises(BrokerStateUnknownError, match="overflow"):
        buffer.reserve_durable()


def test_callback_buffer_overflow_requires_a_full_reconnect() -> None:
    buffer = QmtCallbackBuffer(capacity=1)
    buffer.capture(QmtCallbackKind.ORDER, {"order_id": 1})

    with pytest.raises(BrokerStateUnknownError, match="overflow"):
        buffer.capture(QmtCallbackKind.ORDER, {"order_id": 2})
    with pytest.raises(BrokerStateUnknownError, match="overflow"):
        buffer.capture(QmtCallbackKind.ORDER, {"order_id": 3})
    assert not buffer.healthy
    with pytest.raises(BrokerStateUnknownError, match="overflow"):
        buffer.drain()


@pytest.mark.parametrize(
    "payload",
    [{"nested": []}, {"price": float("nan")}, {"": "missing-key"}],
)
def test_callback_capture_rejects_mutable_or_nonfinite_payloads(
    payload: dict[str, object],
) -> None:
    with pytest.raises((TypeError, ValueError)):
        QmtCallbackBuffer().capture(QmtCallbackKind.ORDER, payload)  # type: ignore[arg-type]


def test_qmt_gateway_never_submits_or_cancels_live_orders() -> None:
    gateway = LockedQmtGateway()

    assert gateway.gateway_available is False
    with pytest.raises(LiveTradingLockedError, match="hard-locked"):
        gateway.submit_order("anything")
    with pytest.raises(LiveTradingLockedError, match="hard-locked"):
        gateway.cancel_order("anything")


def test_canary_candidate_binds_exact_evidence_but_stays_non_executable() -> None:
    candidate = _candidate()

    assert candidate.payload()["broker_mutation_allowed"] is False
    assert candidate.payload()["order_count_limit"] == 1
    assert len(candidate.candidate_hash) == 64
    with pytest.raises(LiveTradingLockedError, match="evidence only"):
        candidate.require_broker_mutation(now=NOW + timedelta(seconds=1))


def test_canary_stage_freezes_exact_xtquant_recovery_remark_before_mutation() -> None:
    candidate = _candidate()
    stage = QmtCanaryOrderStage.from_candidate(
        candidate,
        staged_at=NOW + timedelta(seconds=1),
    )

    assert stage.broker_order_remark == qmt_canary_order_remark(candidate.candidate_hash)
    assert len(stage.broker_order_remark) == 24
    assert stage.broker_order_remark.isascii()
    assert stage.payload()["broker_mutation_allowed"] is False
    assert len(stage.stage_hash) == 64

    with pytest.raises(ValueError, match="recovery tag"):
        replace(
            stage,
            broker_order_remark="AQwrong",
        )


@pytest.mark.parametrize(
    "overrides",
    [
        {"valid_until": NOW + timedelta(seconds=31)},
        {"maximum_order_notional": Decimal("999")},
    ],
)
def test_canary_candidate_rejects_unsafe_lifetime_or_notional(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        _candidate(**overrides)  # type: ignore[arg-type]


def test_canary_candidate_rejects_cross_session_or_unpriced_order() -> None:
    candidate = _candidate()

    for order in (
        replace(
            candidate.decision.order,
            submitted_at=NOW - timedelta(days=1),
        ),
        replace(
            candidate.decision.order,
            limit_price=None,
        ),
    ):
        with pytest.raises(ValueError, match="same-session limit"):
            replace(
                candidate,
                decision=replace(candidate.decision, order=order),
            )


def test_qmt_order_correlation_is_exact_idempotent_and_one_to_one() -> None:
    candidate = _candidate()
    book = QmtOrderCorrelationBook()
    reserved = book.reserve(
        candidate,
        async_request_id=17,
        reserved_at=NOW + timedelta(seconds=1),
    )

    assert (
        book.reserve(
            candidate,
            async_request_id=17,
            reserved_at=NOW + timedelta(seconds=1),
        )
        == reserved
    )
    bound = book.bind(
        async_request_id=17,
        broker_order_id="88001",
        bound_at=NOW + timedelta(seconds=2),
    )

    assert book.client_order_id(broker_order_id="88001") == "canary-order-0001"
    assert book.broker_mapping() == {88001: "canary-order-0001"}
    assert len(reserved.correlation_hash) == len(bound.correlation_hash) == 64
    assert reserved.correlation_hash != bound.correlation_hash
    assert (
        book.bind(
            async_request_id=17,
            broker_order_id="88001",
            bound_at=NOW + timedelta(seconds=2),
        )
        == bound
    )
    with pytest.raises(BrokerStateUnknownError, match="conflicts"):
        book.bind(
            async_request_id=17,
            broker_order_id="88002",
            bound_at=NOW + timedelta(seconds=2),
        )


def test_qmt_order_correlation_rejects_unknown_or_reused_identities() -> None:
    book = QmtOrderCorrelationBook()
    book.reserve(
        _candidate(),
        async_request_id=17,
        reserved_at=NOW + timedelta(seconds=1),
    )

    with pytest.raises(ValueError, match="already reserved"):
        book.reserve(
            _candidate(client_order_id="canary-order-0002"),
            async_request_id=17,
            reserved_at=NOW + timedelta(seconds=1),
        )
    with pytest.raises(BrokerStateUnknownError, match="no reserved"):
        book.bind(
            async_request_id=99,
            broker_order_id="88001",
            bound_at=NOW + timedelta(seconds=2),
        )
    with pytest.raises(BrokerStateUnknownError, match="not correlated"):
        book.client_order_id(broker_order_id="88001")
    with pytest.raises(ValueError, match="positive integer string"):
        book.bind(
            async_request_id=17,
            broker_order_id="not-an-order",
            bound_at=NOW + timedelta(seconds=2),
        )


def test_qmt_order_correlation_book_restores_bound_and_pending_orders() -> None:
    pending = QmtOrderCorrelation(
        candidate_hash="2" * 64,
        client_order_id="canary-order-pending",
        async_request_id=18,
        reserved_at=NOW,
    )
    bound = QmtOrderCorrelation(
        candidate_hash="3" * 64,
        client_order_id="canary-order-bound",
        async_request_id=19,
        reserved_at=NOW + timedelta(seconds=1),
        broker_order_id="88003",
        bound_at=NOW + timedelta(seconds=2),
    )

    restored = QmtOrderCorrelationBook.restore((bound, pending))

    assert restored.broker_mapping() == {88003: "canary-order-bound"}
    with pytest.raises(BrokerStateUnknownError, match="duplicate"):
        QmtOrderCorrelationBook.restore((pending, pending))


def test_qmt_canary_ledger_migration_is_append_only_and_locked() -> None:
    sql = Path("migrations/postgres/037_qmt_canary_order_ledger.sql").read_text(encoding="utf-8")
    staging = Path("migrations/postgres/038_qmt_canary_order_staging.sql").read_text(
        encoding="utf-8"
    )

    assert sql.count("CREATE TABLE IF NOT EXISTS") == 3
    assert sql.count("autoquant_reject_immutable_change()") == 3
    assert "NOT broker_mutation_allowed" in sql
    assert "VALUES ('postgres', 37)" in sql
    assert "schema v38 requires" in staging
    assert "broker_order_remark ~ '^AQ[0-9a-f]{22}$'" in staging
    assert "stage_payload ? 'broker_mutation_allowed'" in staging
    assert "stage_payload->>'broker_mutation_allowed' = 'false'" in staging
    assert "VALUES ('postgres', 38)" in staging
    recovery = Path("migrations/postgres/039_qmt_canary_remark_recovery.sql").read_text(
        encoding="utf-8"
    )
    assert "qmt_order_remark_recovery_bindings" in recovery
    assert "qmt-canary-order-stage-v2" in recovery
    assert "AT TIME ZONE 'Asia/Shanghai'" in recovery
    assert "NOT broker_mutation_allowed" in recovery
    assert "VALUES ('postgres', 39)" in recovery
