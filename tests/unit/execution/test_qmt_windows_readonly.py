from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from autoquant.errors import BrokerStateUnknownError
from autoquant.execution.qmt_gateway import QmtCallbackKind
from autoquant.execution.qmt_preflight import QmtClockAttestation
from autoquant.execution.qmt_readonly_store import (
    QmtReadOnlyAcceptanceEvidence,
)
from autoquant.execution.qmt_session_store import QmtSessionLease
from autoquant.execution.qmt_windows_readonly import (
    QmtReadOnlyWindowsSession,
    QmtVendorBindings,
)

ACCOUNT = "broker-account-secret"
LOGICAL_ACCOUNT = "paper-main"
NOW = datetime(2026, 7, 23, 2, tzinfo=UTC)
HASH = "a" * 64


class CallbackBase:
    pass


class FakeTrader:
    def __init__(self) -> None:
        self.callback: object | None = None
        self.started = False
        self.stopped = False
        self.subscribed = False
        self.connect_result = 0
        self.subscribe_result = 0
        self.unsubscribe_result = 0
        self.asset: object | None = SimpleNamespace(
            account_id=ACCOUNT,
            cash=1000.0,
            frozen_cash=0.0,
            market_value=0.0,
            total_asset=1000.0,
        )
        self.positions: list[object] | None = []
        self.orders: list[object] | None = []
        self.trades: list[object] | None = []

    def register_callback(self, callback: object) -> None:
        self.callback = callback

    def start(self) -> None:
        self.started = True

    def connect(self) -> int:
        return self.connect_result

    def subscribe(self, _account: object) -> int:
        self.subscribed = True
        return self.subscribe_result

    def unsubscribe(self, _account: object) -> int:
        self.subscribed = False
        return self.unsubscribe_result

    def stop(self) -> None:
        self.stopped = True

    def query_account_status(self) -> list[object]:
        return [SimpleNamespace(account_id=ACCOUNT, status=0)]

    def query_stock_asset(self, _account: object) -> object | None:
        return self.asset

    def query_stock_positions(
        self,
        _account: object,
    ) -> list[object] | None:
        return self.positions

    def query_stock_orders(
        self,
        _account: object,
        cancelable_only: bool = False,
    ) -> list[object] | None:
        assert cancelable_only is False
        return self.orders

    def query_stock_trades(
        self,
        _account: object,
    ) -> list[object] | None:
        return self.trades


def _session(
    trader: FakeTrader,
    *,
    times: tuple[datetime, ...] = (
        NOW,
        NOW + timedelta(milliseconds=10),
    ),
    attempts: int = 3,
) -> QmtReadOnlyWindowsSession:
    clock = iter(times)
    bindings = QmtVendorBindings(
        trader_factory=lambda _path, _session_id: trader,
        account_factory=lambda account_id: {"account_id": account_id},
        callback_base=CallbackBase,
        stock_buy=23,
        stock_sell=24,
        package_manifest_hash=HASH,
    )
    return QmtReadOnlyWindowsSession(
        userdata_path=Path("/qmt/userdata_mini"),
        session_id=731001,
        broker_account_id=ACCOUNT,
        logical_account_id=LOGICAL_ACCOUNT,
        bindings=bindings,
        now=lambda: next(clock),
        max_query_attempts=attempts,
    )


def test_readonly_session_builds_coherent_redacted_baseline_and_closes() -> None:
    trader = FakeTrader()
    session = _session(trader)

    session.open()
    result = session.query()
    session.close()

    assert result.package_manifest_hash == HASH
    assert result.baseline.logical_account_id == LOGICAL_ACCOUNT
    assert result.baseline.account_snapshot.account_id == LOGICAL_ACCOUNT
    assert result.baseline.positions == ()
    assert result.baseline.orders == ()
    assert result.baseline.trades == ()
    assert trader.started is True
    assert trader.stopped is True
    assert trader.subscribed is False


@pytest.mark.parametrize(
    "query_name",
    ["asset", "positions", "orders", "trades"],
)
def test_readonly_session_rejects_ambiguous_none_queries(
    query_name: str,
) -> None:
    trader = FakeTrader()
    setattr(trader, query_name, None)
    session = _session(trader)
    session.open()

    with pytest.raises(BrokerStateUnknownError, match="returned None"):
        session.query()

    session.close()


def test_readonly_session_rejects_slow_cross_time_queries() -> None:
    trader = FakeTrader()
    session = _session(
        trader,
        times=(NOW, NOW + timedelta(seconds=6)),
    )
    session.open()

    with pytest.raises(
        BrokerStateUnknownError,
        match="coherent snapshot window",
    ):
        session.query()

    session.close()


def test_readonly_session_retries_normal_status_callback_during_query() -> None:
    trader = FakeTrader()
    session = _session(
        trader,
        times=(
            NOW,
            NOW + timedelta(milliseconds=10),
            NOW + timedelta(milliseconds=20),
            NOW + timedelta(milliseconds=30),
        ),
    )
    original = trader.query_stock_asset
    called = False

    def query_with_callback(account: object) -> object | None:
        nonlocal called
        if not called:
            called = True
            callback = trader.callback
            assert callback is not None
            callback.on_account_status(  # type: ignore[attr-defined]
                SimpleNamespace(account_id=ACCOUNT, status=0)
            )
        return original(account)

    trader.query_stock_asset = query_with_callback  # type: ignore[method-assign]
    session.open()
    result = session.query()
    session.close()

    assert result.baseline.generation == 2
    assert result.baseline.callback_cursor == 1


def test_durable_query_preserves_callback_for_persistence_coordinator() -> None:
    trader = FakeTrader()
    session = _session(trader)
    session.open()
    session.gateway.callbacks.capture(
        QmtCallbackKind.ACCOUNT_STATUS,
        {"account_id": ACCOUNT, "status": 0},
    )

    with pytest.raises(
        BrokerStateUnknownError,
        match="persistence must catch up",
    ):
        session.query_preserving_callbacks(expected_callback_cursor=0)

    events = session.gateway.callbacks.drain()
    session.close()
    assert len(events) == 1
    assert events[0].kind is QmtCallbackKind.ACCOUNT_STATUS


def test_durable_query_retains_callback_arriving_during_query() -> None:
    trader = FakeTrader()
    session = _session(trader)
    original = trader.query_stock_asset

    def query_with_callback(account: object) -> object | None:
        session.gateway.callbacks.capture(
            QmtCallbackKind.ACCOUNT_STATUS,
            {"account_id": ACCOUNT, "status": 0},
        )
        return original(account)

    trader.query_stock_asset = query_with_callback  # type: ignore[method-assign]
    session.open()

    with pytest.raises(
        BrokerStateUnknownError,
        match="changed during the durable",
    ):
        session.query_preserving_callbacks(expected_callback_cursor=0)

    events = session.gateway.callbacks.drain()
    session.close()
    assert len(events) == 1
    assert events[0].kind is QmtCallbackKind.ACCOUNT_STATUS


def test_readonly_session_rejects_state_changing_callback() -> None:
    trader = FakeTrader()
    session = _session(
        trader,
        times=(
            NOW,
            NOW + timedelta(milliseconds=10),
            NOW + timedelta(milliseconds=20),
            NOW + timedelta(milliseconds=30),
        ),
    )
    original = trader.query_stock_asset
    called = False

    def query_with_callback(account: object) -> object | None:
        nonlocal called
        if not called:
            called = True
            session.gateway.callbacks.capture(
                QmtCallbackKind.TRADE,
                {"account_id": ACCOUNT},
            )
        return original(account)

    trader.query_stock_asset = query_with_callback  # type: ignore[method-assign]
    session.open()

    with pytest.raises(
        BrokerStateUnknownError,
        match="full reconnect",
    ):
        session.query()

    session.close()


def test_invalid_vendor_callback_is_converted_to_fail_closed_event() -> None:
    trader = FakeTrader()
    session = _session(trader)
    session.open()
    callback = trader.callback
    assert callback is not None

    callback.on_stock_order(SimpleNamespace())  # type: ignore[attr-defined]

    with pytest.raises(
        BrokerStateUnknownError,
        match="full reconnect",
    ):
        session.query()
    session.close()


def test_connection_failure_stops_vendor_runtime() -> None:
    trader = FakeTrader()
    trader.connect_result = -1
    session = _session(trader)

    with pytest.raises(BrokerStateUnknownError, match="connection failed"):
        session.open()

    assert trader.started is True
    assert trader.stopped is True


def test_acceptance_evidence_never_contains_broker_account_identifier() -> None:
    trader = FakeTrader()
    session = _session(trader)
    session.open()
    baseline = session.query().baseline
    session.close()
    lease = QmtSessionLease(
        session_id=731001,
        holder_id="gateway-a",
        token_hash="b" * 64,
        acquired_at=NOW - timedelta(seconds=1),
        heartbeat_at=NOW - timedelta(seconds=1),
        expires_at=NOW + timedelta(seconds=30),
        released_at=None,
        generation=1,
        version=1,
        event_sequence=1,
        last_event_hash="c" * 64,
    )

    evidence = QmtReadOnlyAcceptanceEvidence.from_baseline(
        baseline=baseline,
        package_manifest_hash=HASH,
        lease=lease,
        clock_attestation=QmtClockAttestation(
            request_started_at=NOW + timedelta(milliseconds=20),
            database_observed_at=NOW + timedelta(milliseconds=25),
            request_completed_at=NOW + timedelta(milliseconds=30),
        ),
    )

    assert ACCOUNT not in repr(evidence.payload())
    assert evidence.logical_account_id == LOGICAL_ACCOUNT
    assert evidence.payload()["version"] == "qmt-readonly-acceptance-v2"
    assert evidence.clock_attestation is not None
    assert evidence.clock_attestation.trusted is True
