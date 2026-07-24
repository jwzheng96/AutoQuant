from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from autoquant.backtest.models import OrderSide
from autoquant.errors import BrokerStateUnknownError
from autoquant.execution.models import PaperOrderState
from autoquant.execution.qmt_gateway import QmtCallbackEnvelope, QmtCallbackKind
from autoquant.execution.qmt_models import QmtOrderStatus
from autoquant.execution.qmt_readonly import (
    QmtReadOnlyBrokerReader,
    QmtReadOnlyRecoveryState,
    build_qmt_readonly_baseline,
    normalize_qmt_asset,
    normalize_qmt_order,
    normalize_qmt_position,
    normalize_qmt_trade,
)
from autoquant.execution.supervisor import ReconciliationSupervisor

ACCOUNT = "broker-account"
LOGICAL_ACCOUNT = "paper-main"
STARTED = datetime(2026, 7, 23, 1, 30, tzinfo=UTC)
NOW = STARTED + timedelta(milliseconds=50)


def _asset_payload(
    *,
    cash: object = 500,
    frozen_cash: object = 100,
    market_value: object = 1000,
    total_asset: object = 1600,
) -> dict[str, object]:
    return {
        "account_id": ACCOUNT,
        "cash": cash,
        "frozen_cash": frozen_cash,
        "market_value": market_value,
        "total_asset": total_asset,
    }


def _position_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "account_id": ACCOUNT,
        "stock_code": "600000.SH",
        "volume": 100,
        "can_use_volume": 80,
        "frozen_volume": 20,
        "avg_price": 9.5,
        "market_value": 1000,
    }
    payload.update(overrides)
    return payload


def _order_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "account_id": ACCOUNT,
        "stock_code": "600000.SH",
        "order_id": 101,
        "side": "buy",
        "order_volume": 100,
        "traded_volume": 0,
        "traded_price": 0,
        "order_status": QmtOrderStatus.REPORTED,
        "status_msg": "",
    }
    payload.update(overrides)
    return payload


def _trade_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "account_id": ACCOUNT,
        "traded_id": "trade-1",
        "order_id": 101,
        "stock_code": "600000.SH",
        "side": "buy",
        "traded_price": 10,
        "traded_volume": 100,
        "traded_amount": 1000,
    }
    payload.update(overrides)
    return payload


def _normalized(
    *,
    order_payloads: tuple[dict[str, object], ...] = (),
    trade_payloads: tuple[dict[str, object], ...] = (),
    asset_payload: dict[str, object] | None = None,
):
    asset = normalize_qmt_asset(
        _asset_payload() if asset_payload is None else asset_payload,  # type: ignore[arg-type]
        expected_account_id=ACCOUNT,
        observed_at=NOW,
    )
    positions = (
        normalize_qmt_position(
            _position_payload(),  # type: ignore[arg-type]
            expected_account_id=ACCOUNT,
            observed_at=NOW,
        ),
    )
    orders = tuple(
        normalize_qmt_order(
            payload,  # type: ignore[arg-type]
            expected_account_id=ACCOUNT,
            observed_at=NOW,
            client_order_ids={101: "client-101"},
        )
        for payload in order_payloads
    )
    trades = tuple(
        normalize_qmt_trade(
            payload,  # type: ignore[arg-type]
            expected_account_id=ACCOUNT,
            observed_at=NOW,
        )
        for payload in trade_payloads
    )
    return asset, positions, orders, trades


def _baseline(
    *,
    generation: int = 1,
    cursor: int = 0,
    order_payloads: tuple[dict[str, object], ...] = (),
    trade_payloads: tuple[dict[str, object], ...] = (),
):
    asset, positions, orders, trades = _normalized(
        order_payloads=order_payloads,
        trade_payloads=trade_payloads,
    )
    return build_qmt_readonly_baseline(
        baseline_id=f"baseline-{generation}",
        generation=generation,
        logical_account_id=LOGICAL_ACCOUNT,
        query_started_at=STARTED,
        query_completed_at=NOW,
        callback_cursor_before=cursor,
        callback_cursor_after=cursor,
        callback_stream_healthy=True,
        asset=asset,
        positions=positions,
        orders=orders,
        trades=trades,
    )


def test_normalizers_copy_documented_asset_position_order_and_trade_fields() -> None:
    asset, positions, orders, trades = _normalized(
        order_payloads=(
            _order_payload(
                traded_volume=100,
                traded_price=10,
                order_status=QmtOrderStatus.FILLED,
            ),
        ),
        trade_payloads=(_trade_payload(),),
    )

    assert asset.cash == Decimal("500")
    assert positions[0].instrument == "600000.XSHG"
    assert positions[0].available_volume == 80
    assert orders[0].client_order_id == "client-101"
    assert orders[0].state is PaperOrderState.FILLED
    assert trades[0].side is OrderSide.BUY
    assert trades[0].amount == Decimal("1000")


def test_live_order_normalization_requires_a_trusted_correlation() -> None:
    with pytest.raises(BrokerStateUnknownError, match="no trusted"):
        normalize_qmt_order(
            _order_payload(),  # type: ignore[arg-type]
            expected_account_id=ACCOUNT,
            observed_at=NOW,
            client_order_ids={},
            require_client_order_mapping=True,
        )


def test_balanced_query_baseline_builds_hash_committed_account_snapshot() -> None:
    baseline = _baseline(order_payloads=(_order_payload(),))

    assert baseline.broker_account_id == ACCOUNT
    assert baseline.account_snapshot.account_id == LOGICAL_ACCOUNT
    assert baseline.account_snapshot.cash == Decimal("600")
    assert baseline.account_snapshot.equity == Decimal("1600")
    assert baseline.account_snapshot.open_client_order_ids == ("client-101",)
    assert baseline.account_snapshot.positions[0].sellable_quantity == 80
    assert baseline.account_snapshot.evidence_hash == baseline.evidence_hash
    assert len(baseline.evidence_hash) == 64


def test_filled_order_must_converge_with_daily_trade_query() -> None:
    filled = _order_payload(
        traded_volume=100,
        traded_price=10,
        order_status=QmtOrderStatus.FILLED,
    )
    baseline = _baseline(
        order_payloads=(filled,),
        trade_payloads=(_trade_payload(),),
    )
    assert baseline.orders[0].state is PaperOrderState.FILLED
    assert baseline.account_snapshot.open_client_order_ids == ()

    with pytest.raises(BrokerStateUnknownError, match="cumulative volume"):
        _baseline(order_payloads=(filled,), trade_payloads=())


@pytest.mark.parametrize(
    "failure",
    ["asset", "positions", "orders", "trades", "cursor"],
)
def test_none_queries_and_callback_race_never_mean_an_empty_account(
    failure: str,
) -> None:
    asset, positions, orders, trades = _normalized()
    values: dict[str, object] = {
        "asset": asset,
        "positions": positions,
        "orders": orders,
        "trades": trades,
    }
    if failure != "cursor":
        values[failure] = None

    with pytest.raises(BrokerStateUnknownError):
        build_qmt_readonly_baseline(
            baseline_id="raced",
            generation=1,
            logical_account_id=LOGICAL_ACCOUNT,
            query_started_at=STARTED,
            query_completed_at=NOW,
            callback_cursor_before=0,
            callback_cursor_after=1 if failure == "cursor" else 0,
            callback_stream_healthy=True,
            asset=values["asset"],  # type: ignore[arg-type]
            positions=values["positions"],  # type: ignore[arg-type]
            orders=values["orders"],  # type: ignore[arg-type]
            trades=values["trades"],  # type: ignore[arg-type]
        )


def test_unknown_order_or_unbalanced_asset_fails_closed() -> None:
    with pytest.raises(BrokerStateUnknownError, match="unknown"):
        _baseline(
            order_payloads=(
                _order_payload(order_status=QmtOrderStatus.UNKNOWN),
            )
        )

    asset, positions, orders, trades = _normalized(
        asset_payload=_asset_payload(total_asset=1590)
    )
    with pytest.raises(BrokerStateUnknownError, match="total"):
        build_qmt_readonly_baseline(
            baseline_id="unbalanced",
            generation=1,
            logical_account_id=LOGICAL_ACCOUNT,
            query_started_at=STARTED,
            query_completed_at=NOW,
            callback_cursor_before=0,
            callback_cursor_after=0,
            callback_stream_healthy=True,
            asset=asset,
            positions=positions,
            orders=orders,
            trades=trades,
        )


def _event(
    sequence: int,
    kind: QmtCallbackKind,
    payload: dict[str, object],
) -> QmtCallbackEnvelope:
    return QmtCallbackEnvelope(
        local_sequence=sequence,
        kind=kind,
        received_at=NOW,
        payload=payload,  # type: ignore[arg-type]
    )


def test_recovery_accepts_normal_account_heartbeat_but_refreshes_on_order_change() -> None:
    recovery = QmtReadOnlyRecoveryState()
    recovery.install(_baseline())
    recovery.apply(
        (
            _event(
                1,
                QmtCallbackKind.ACCOUNT_STATUS,
                {"account_id": ACCOUNT, "status": 0},
            ),
        )
    )
    assert recovery.trusted

    with pytest.raises(BrokerStateUnknownError, match="full query refresh"):
        recovery.apply(
            (
                _event(
                    2,
                    QmtCallbackKind.ORDER,
                    {"account_id": ACCOUNT, "order_id": 101},
                ),
            )
        )
    assert not recovery.trusted
    with pytest.raises(BrokerStateUnknownError, match="full_refresh_required"):
        recovery.snapshot()

    recovery.install(_baseline(generation=2, cursor=2))
    assert recovery.trusted


@pytest.mark.asyncio
async def test_broker_reader_exposes_only_trusted_logical_account_snapshot() -> None:
    recovery = QmtReadOnlyRecoveryState()
    recovery.install(_baseline())
    reader = QmtReadOnlyBrokerReader(
        recovery=recovery,
        logical_account_id=LOGICAL_ACCOUNT,
    )

    snapshot = await reader(LOGICAL_ACCOUNT, NOW)

    assert snapshot.account_id == LOGICAL_ACCOUNT
    with pytest.raises(BrokerStateUnknownError, match="another logical"):
        await reader("another-account", NOW)
    with pytest.raises(BrokerStateUnknownError, match="time"):
        await reader(LOGICAL_ACCOUNT, NOW - timedelta(seconds=1))


@pytest.mark.asyncio
async def test_qmt_reader_feeds_persisted_supervisor_and_callback_change_fails_closed() -> None:
    recovery = QmtReadOnlyRecoveryState()
    baseline = _baseline()
    recovery.install(baseline)
    broker_reader = QmtReadOnlyBrokerReader(
        recovery=recovery,
        logical_account_id=LOGICAL_ACCOUNT,
    )
    executions = MagicMock()
    executions.save_reconciliation = AsyncMock()
    controls = MagicMock()
    controls.get = AsyncMock(return_value=MagicMock(active=True))
    controls.activate = AsyncMock(return_value=MagicMock(active=True))
    supervisor = ReconciliationSupervisor(
        account_id=LOGICAL_ACCOUNT,
        internal_reader=AsyncMock(return_value=baseline.account_snapshot),
        broker_reader=broker_reader,
        execution_repository=executions,
        control_repository=controls,
    )

    reconciled = await supervisor.run_once(now=NOW)

    assert reconciled.status == "reconciled"
    assert reconciled.report is not None and reconciled.report.reconciled
    executions.save_reconciliation.assert_awaited_once()

    with pytest.raises(BrokerStateUnknownError):
        recovery.apply(
            (
                _event(
                    1,
                    QmtCallbackKind.TRADE,
                    {"account_id": ACCOUNT, "traded_id": "new"},
                ),
            )
        )
    failed = await supervisor.run_once(now=NOW)

    assert failed.status == "failed"
    assert failed.error_code == "reconciliation_dependency_failed"
    controls.activate.assert_awaited_once()


def test_recovery_rejects_disconnect_gap_bad_account_and_generation_regression() -> None:
    recovery = QmtReadOnlyRecoveryState()
    recovery.install(_baseline())
    with pytest.raises(BrokerStateUnknownError, match="gap"):
        recovery.apply(
            (
                _event(
                    2,
                    QmtCallbackKind.ACCOUNT_STATUS,
                    {"account_id": ACCOUNT, "status": 0},
                ),
            )
        )
    with pytest.raises(BrokerStateUnknownError, match="generation"):
        recovery.install(_baseline())

    replacement = QmtReadOnlyRecoveryState()
    replacement.install(_baseline())
    with pytest.raises(BrokerStateUnknownError, match="not in the normal"):
        replacement.apply(
            (
                _event(
                    1,
                    QmtCallbackKind.ACCOUNT_STATUS,
                    {"account_id": ACCOUNT, "status": 3},
                ),
            )
        )

    disconnected = QmtReadOnlyRecoveryState()
    disconnected.install(_baseline())
    with pytest.raises(BrokerStateUnknownError, match="disconnected"):
        disconnected.apply((_event(1, QmtCallbackKind.DISCONNECTED, {}),))
