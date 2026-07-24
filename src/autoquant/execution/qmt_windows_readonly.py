from __future__ import annotations

import hashlib
import importlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol, cast
from uuid import uuid4

from autoquant.errors import (
    BrokerStateUnknownError,
    MissingCapabilityError,
)
from autoquant.execution.qmt_gateway import (
    LockedQmtGateway,
    QmtCallbackKind,
    QmtCallbackValue,
)
from autoquant.execution.qmt_readonly import (
    QmtReadOnlyBaseline,
    build_qmt_readonly_baseline,
    normalize_qmt_asset,
    normalize_qmt_order,
    normalize_qmt_position,
    normalize_qmt_trade,
)

_PACKAGE_SUFFIXES = frozenset(
    {".py", ".pyi", ".pyd", ".dll", ".so", ".json"}
)
_MAX_PACKAGE_FILES = 20_000
_MAX_PACKAGE_BYTES = 512 * 1024 * 1024


class QmtTraderProtocol(Protocol):
    def register_callback(self, callback: object) -> None: ...

    def start(self) -> None: ...

    def connect(self) -> int: ...

    def subscribe(self, account: object) -> int: ...

    def unsubscribe(self, account: object) -> int: ...

    def stop(self) -> None: ...

    def query_account_status(self) -> Sequence[object] | None: ...

    def query_stock_asset(self, account: object) -> object | None: ...

    def query_stock_positions(
        self, account: object
    ) -> Sequence[object] | None: ...

    def query_stock_orders(
        self,
        account: object,
        cancelable_only: bool = False,
    ) -> Sequence[object] | None: ...

    def query_stock_trades(
        self, account: object
    ) -> Sequence[object] | None: ...


@dataclass(frozen=True, slots=True)
class QmtVendorBindings:
    trader_factory: Callable[[str, int], object]
    account_factory: Callable[[str], object]
    callback_base: type[object]
    stock_buy: int
    stock_sell: int
    package_manifest_hash: str

    @classmethod
    def load(cls) -> QmtVendorBindings:
        try:
            package = importlib.import_module("xtquant")
            trader_module = importlib.import_module("xtquant.xttrader")
            type_module = importlib.import_module("xtquant.xttype")
            constants = importlib.import_module("xtquant.xtconstant")
            trader_factory = cast(
                Callable[[str, int], object],
                trader_module.XtQuantTrader,
            )
            account_factory = cast(
                Callable[[str], object],
                type_module.StockAccount,
            )
            callback_base = cast(
                type[object],
                trader_module.XtQuantTraderCallback,
            )
            stock_buy = _strict_int(
                constants.STOCK_BUY,
                name="xtconstant.STOCK_BUY",
            )
            stock_sell = _strict_int(
                constants.STOCK_SELL,
                name="xtconstant.STOCK_SELL",
            )
            package_file = package.__file__
        except (AttributeError, ImportError, TypeError, ValueError):
            raise MissingCapabilityError(
                "XtQuant read-only bindings are unavailable"
            ) from None
        if not isinstance(package_file, str) or not package_file.strip():
            raise MissingCapabilityError(
                "XtQuant package location is unavailable"
            )
        return cls(
            trader_factory=trader_factory,
            account_factory=account_factory,
            callback_base=callback_base,
            stock_buy=stock_buy,
            stock_sell=stock_sell,
            package_manifest_hash=_package_manifest_hash(
                Path(package_file).resolve().parent
            ),
        )

    def side(self, order_type: object) -> str:
        value = _strict_int(order_type, name="QMT order_type")
        if value == self.stock_buy:
            return "buy"
        if value == self.stock_sell:
            return "sell"
        raise BrokerStateUnknownError(
            "QMT returned a non-stock buy/sell order type"
        )


@dataclass(frozen=True, slots=True)
class QmtReadOnlyAcceptance:
    baseline: QmtReadOnlyBaseline
    package_manifest_hash: str


class QmtReadOnlyWindowsSession:
    """One-shot XtTrader query session with no broker mutation methods."""

    def __init__(
        self,
        *,
        userdata_path: Path,
        session_id: int,
        broker_account_id: str,
        logical_account_id: str,
        bindings: QmtVendorBindings,
        gateway: LockedQmtGateway | None = None,
        now: Callable[[], datetime] | None = None,
        max_query_attempts: int = 3,
        max_query_duration: timedelta = timedelta(seconds=5),
    ) -> None:
        if (
            not userdata_path.is_absolute()
            or userdata_path.name.casefold() != "userdata_mini"
        ):
            raise ValueError(
                "QMT userdata path must be an absolute userdata_mini path"
            )
        if (
            not isinstance(session_id, int)
            or isinstance(session_id, bool)
            or not 1 <= session_id <= 2_147_483_647
        ):
            raise ValueError("QMT session_id must be a positive 32-bit integer")
        for name, value in (
            ("broker_account_id", broker_account_id),
            ("logical_account_id", logical_account_id),
        ):
            if not value.strip():
                raise ValueError(f"{name} cannot be empty")
        if (
            not isinstance(max_query_attempts, int)
            or isinstance(max_query_attempts, bool)
            or max_query_attempts < 1
        ):
            raise ValueError("max_query_attempts must be positive")
        if max_query_duration <= timedelta(0):
            raise ValueError("max_query_duration must be positive")
        self._userdata_path = userdata_path
        self._session_id = session_id
        self._broker_account_id = broker_account_id
        self._logical_account_id = logical_account_id
        self._bindings = bindings
        self._gateway = gateway or LockedQmtGateway()
        self._now = now or (lambda: datetime.now(UTC))
        self._max_query_attempts = max_query_attempts
        self._max_query_duration = max_query_duration
        self._trader: QmtTraderProtocol | None = None
        self._account: object | None = None
        self._subscribed = False

    @property
    def gateway(self) -> LockedQmtGateway:
        return self._gateway

    def open(self) -> None:
        if self._trader is not None:
            raise RuntimeError("QMT read-only session is already open")
        account = self._bindings.account_factory(self._broker_account_id)
        trader = cast(
            QmtTraderProtocol,
            self._bindings.trader_factory(
                str(self._userdata_path),
                self._session_id,
            ),
        )
        trader.register_callback(self._callback())
        try:
            trader.start()
            if trader.connect() != 0:
                raise BrokerStateUnknownError(
                    "QMT MiniQMT connection failed"
                )
            if trader.subscribe(account) != 0:
                raise BrokerStateUnknownError(
                    "QMT account subscription failed"
                )
        except Exception:
            try:
                trader.stop()
            except Exception:
                pass
            raise
        self._trader = trader
        self._account = account
        self._subscribed = True

    def close(self) -> None:
        trader = self._trader
        account = self._account
        self._trader = None
        self._account = None
        was_subscribed = self._subscribed
        self._subscribed = False
        if trader is None:
            return
        unsubscribe_failed = False
        try:
            if was_subscribed and account is not None:
                unsubscribe_failed = trader.unsubscribe(account) != 0
        finally:
            trader.stop()
        if unsubscribe_failed:
            raise BrokerStateUnknownError(
                "QMT account unsubscribe failed"
            )

    def query(self) -> QmtReadOnlyAcceptance:
        trader, account = self._require_open()
        self._require_account_normal(trader)
        last_error: BrokerStateUnknownError | None = None
        for generation in range(1, self._max_query_attempts + 1):
            self._drain_before_query()
            callback_cursor_before = self._gateway.callbacks.cursor
            query_started_at = self._utc_now("QMT query start")
            asset_object = trader.query_stock_asset(account)
            position_objects = trader.query_stock_positions(account)
            order_objects = trader.query_stock_orders(
                account,
                cancelable_only=False,
            )
            trade_objects = trader.query_stock_trades(account)
            query_completed_at = self._utc_now("QMT query completion")
            callback_cursor_after = self._gateway.callbacks.cursor
            if (
                query_completed_at < query_started_at
                or query_completed_at - query_started_at
                > self._max_query_duration
            ):
                raise BrokerStateUnknownError(
                    "QMT queries exceeded the coherent snapshot window"
                )
            try:
                asset = (
                    None
                    if asset_object is None
                    else normalize_qmt_asset(
                        _asset_payload(asset_object),
                        expected_account_id=self._broker_account_id,
                        observed_at=query_completed_at,
                    )
                )
                positions = (
                    None
                    if position_objects is None
                    else tuple(
                        normalize_qmt_position(
                            _position_payload(item),
                            expected_account_id=self._broker_account_id,
                            observed_at=query_completed_at,
                        )
                        for item in position_objects
                    )
                )
                orders = (
                    None
                    if order_objects is None
                    else tuple(
                        normalize_qmt_order(
                            _order_payload(item, self._bindings),
                            expected_account_id=self._broker_account_id,
                            observed_at=query_completed_at,
                        )
                        for item in order_objects
                    )
                )
                trades = (
                    None
                    if trade_objects is None
                    else tuple(
                        normalize_qmt_trade(
                            _trade_payload(item, self._bindings),
                            expected_account_id=self._broker_account_id,
                            observed_at=query_completed_at,
                        )
                        for item in trade_objects
                    )
                )
                baseline = build_qmt_readonly_baseline(
                    baseline_id=f"qmt-readonly-{uuid4()}",
                    generation=generation,
                    logical_account_id=self._logical_account_id,
                    query_started_at=query_started_at,
                    query_completed_at=query_completed_at,
                    callback_cursor_before=callback_cursor_before,
                    callback_cursor_after=callback_cursor_after,
                    callback_stream_healthy=self._gateway.callbacks.healthy,
                    asset=asset,
                    positions=positions,
                    orders=orders,
                    trades=trades,
                )
            except BrokerStateUnknownError as error:
                if callback_cursor_before != callback_cursor_after:
                    last_error = error
                    continue
                raise
            return QmtReadOnlyAcceptance(
                baseline=baseline,
                package_manifest_hash=self._bindings.package_manifest_hash,
            )
        raise BrokerStateUnknownError(
            "QMT callbacks prevented a coherent read-only baseline"
        ) from last_error

    def _require_open(self) -> tuple[QmtTraderProtocol, object]:
        if self._trader is None or self._account is None:
            raise RuntimeError("QMT read-only session is not open")
        return self._trader, self._account

    def _require_account_normal(self, trader: QmtTraderProtocol) -> None:
        statuses = trader.query_account_status()
        if statuses is None:
            raise BrokerStateUnknownError(
                "QMT account status query returned None"
            )
        matching = [
            item
            for item in statuses
            if _string_attr(item, "account_id") == self._broker_account_id
        ]
        if len(matching) != 1 or _int_attr(matching[0], "status") != 0:
            raise BrokerStateUnknownError(
                "QMT account is not uniquely present in normal status"
            )

    def _drain_before_query(self) -> None:
        for event in self._gateway.callbacks.drain():
            if (
                event.kind is not QmtCallbackKind.ACCOUNT_STATUS
                or event.payload.get("account_id") != self._broker_account_id
                or event.payload.get("status") != 0
            ):
                raise BrokerStateUnknownError(
                    "QMT callback requires a full reconnect before baseline"
                )

    def _callback(self) -> object:
        gateway = self._gateway
        bindings = self._bindings

        def capture(
            kind: QmtCallbackKind,
            payload: Mapping[str, QmtCallbackValue],
        ) -> None:
            gateway.callbacks.capture(kind, payload)

        def capture_object(
            kind: QmtCallbackKind,
            payload: Callable[[], Mapping[str, QmtCallbackValue]],
            *,
            invalid_reason: str,
        ) -> None:
            try:
                capture(kind, payload())
            except (BrokerStateUnknownError, TypeError, ValueError):
                capture(
                    QmtCallbackKind.ORDER_ERROR,
                    {"reason": invalid_reason},
                )

        def on_disconnected(_callback: object) -> None:
            capture(
                QmtCallbackKind.DISCONNECTED,
                {"reason": "xttrader_disconnected"},
            )

        def on_account_status(
            _callback: object,
            status: object,
        ) -> None:
            capture_object(
                QmtCallbackKind.ACCOUNT_STATUS,
                lambda: {
                    "account_id": _string_attr(status, "account_id"),
                    "status": _int_attr(status, "status"),
                },
                invalid_reason="invalid_account_status_callback",
            )

        def on_stock_order(_callback: object, order: object) -> None:
            capture_object(
                QmtCallbackKind.ORDER,
                lambda: _order_payload(order, bindings),
                invalid_reason="invalid_order_callback",
            )

        def on_stock_trade(_callback: object, trade: object) -> None:
            capture_object(
                QmtCallbackKind.TRADE,
                lambda: _trade_payload(trade, bindings),
                invalid_reason="invalid_trade_callback",
            )

        def on_order_error(_callback: object, error: object) -> None:
            capture_object(
                QmtCallbackKind.ORDER_ERROR,
                lambda: {
                    "account_id": _string_attr(error, "account_id"),
                    "error_id": _int_attr(error, "error_id"),
                    "order_id": _int_attr(error, "order_id"),
                },
                invalid_reason="invalid_order_error_callback",
            )

        def on_cancel_error(_callback: object, error: object) -> None:
            capture_object(
                QmtCallbackKind.CANCEL_ERROR,
                lambda: {
                    "account_id": _string_attr(error, "account_id"),
                    "error_id": _int_attr(error, "error_id"),
                    "order_id": _int_attr(error, "order_id"),
                },
                invalid_reason="invalid_cancel_error_callback",
            )

        callback_type = type(
            "AutoQuantReadOnlyCallback",
            (bindings.callback_base,),
            {
                "on_disconnected": on_disconnected,
                "on_account_status": on_account_status,
                "on_stock_order": on_stock_order,
                "on_stock_trade": on_stock_trade,
                "on_order_error": on_order_error,
                "on_cancel_error": on_cancel_error,
            },
        )
        return callback_type()

    def _utc_now(self, name: str) -> datetime:
        value = self._now()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must be timezone-aware")
        return value.astimezone(UTC)


def _asset_payload(value: object) -> Mapping[str, QmtCallbackValue]:
    return {
        "account_id": _string_attr(value, "account_id"),
        "cash": _float_attr(value, "cash"),
        "frozen_cash": _float_attr(value, "frozen_cash"),
        "market_value": _float_attr(value, "market_value"),
        "total_asset": _float_attr(value, "total_asset"),
    }


def _position_payload(value: object) -> Mapping[str, QmtCallbackValue]:
    return {
        "account_id": _string_attr(value, "account_id"),
        "stock_code": _string_attr(value, "stock_code"),
        "volume": _int_attr(value, "volume"),
        "can_use_volume": _int_attr(value, "can_use_volume"),
        "frozen_volume": _int_attr(value, "frozen_volume"),
        "avg_price": _float_attr(value, "avg_price"),
        "market_value": _float_attr(value, "market_value"),
    }


def _order_payload(
    value: object,
    bindings: QmtVendorBindings,
) -> Mapping[str, QmtCallbackValue]:
    return {
        "account_id": _string_attr(value, "account_id"),
        "stock_code": _string_attr(value, "stock_code"),
        "order_id": _int_attr(value, "order_id"),
        "side": bindings.side(_attr(value, "order_type")),
        "order_volume": _int_attr(value, "order_volume"),
        "traded_volume": _int_attr(value, "traded_volume"),
        "traded_price": _float_attr(value, "traded_price"),
        "order_status": _int_attr(value, "order_status"),
        "order_remark": _string_attr(
            value,
            "order_remark",
            allow_blank=True,
        ),
        "status_msg": _string_attr(
            value,
            "status_msg",
            allow_blank=True,
        ),
    }


def _trade_payload(
    value: object,
    bindings: QmtVendorBindings,
) -> Mapping[str, QmtCallbackValue]:
    return {
        "account_id": _string_attr(value, "account_id"),
        "traded_id": _string_attr(value, "traded_id"),
        "order_id": _int_attr(value, "order_id"),
        "stock_code": _string_attr(value, "stock_code"),
        "side": bindings.side(_attr(value, "order_type")),
        "traded_price": _float_attr(value, "traded_price"),
        "traded_volume": _int_attr(value, "traded_volume"),
        "traded_amount": _float_attr(value, "traded_amount"),
        "order_remark": _string_attr(
            value,
            "order_remark",
            allow_blank=True,
        ),
    }


def _attr(value: object, name: str) -> object:
    try:
        return getattr(value, name)
    except AttributeError:
        raise BrokerStateUnknownError(
            f"QMT object omitted required field: {name}"
        ) from None


def _string_attr(
    value: object,
    name: str,
    *,
    allow_blank: bool = False,
) -> str:
    raw = _attr(value, name)
    if not isinstance(raw, str):
        raise BrokerStateUnknownError(
            f"QMT field {name} is not a string"
        )
    normalized = raw.strip()
    if not allow_blank and not normalized:
        raise BrokerStateUnknownError(f"QMT field {name} is empty")
    return normalized


def _int_attr(value: object, name: str) -> int:
    return _strict_int(_attr(value, name), name=f"QMT field {name}")


def _strict_int(value: object, *, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise BrokerStateUnknownError(f"{name} is not an integer")
    return value


def _float_attr(value: object, name: str) -> float:
    raw = _attr(value, name)
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise BrokerStateUnknownError(
            f"QMT field {name} is not numeric"
        )
    converted = float(raw)
    if not (-float("inf") < converted < float("inf")):
        raise BrokerStateUnknownError(
            f"QMT field {name} is not finite"
        )
    return converted


def _package_manifest_hash(root: Path) -> str:
    if not root.is_absolute() or not root.is_dir():
        raise MissingCapabilityError(
            "XtQuant package directory is unavailable"
        )
    files = tuple(
        sorted(
            (
                path
                for path in root.rglob("*")
                if path.is_file()
                and not path.is_symlink()
                and path.suffix.casefold() in _PACKAGE_SUFFIXES
            ),
            key=lambda path: path.relative_to(root).as_posix(),
        )
    )
    if not files or len(files) > _MAX_PACKAGE_FILES:
        raise MissingCapabilityError(
            "XtQuant package manifest has an unsafe file count"
        )
    total_bytes = sum(path.stat().st_size for path in files)
    if total_bytes > _MAX_PACKAGE_BYTES:
        raise MissingCapabilityError(
            "XtQuant package manifest exceeds the audit size limit"
        )
    manifest = hashlib.sha256()
    for path in files:
        relative = path.relative_to(root).as_posix()
        file_hash = hashlib.sha256()
        try:
            before = path.stat()
            with path.open("rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    file_hash.update(chunk)
            after = path.stat()
        except OSError:
            raise MissingCapabilityError(
                "XtQuant package manifest could not be read"
            ) from None
        if (
            before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns
        ):
            raise MissingCapabilityError(
                "XtQuant package changed while its manifest was computed"
            )
        manifest.update(relative.encode("utf-8"))
        manifest.update(b"\0")
        manifest.update(str(after.st_size).encode("ascii"))
        manifest.update(b"\0")
        manifest.update(file_hash.digest())
    return manifest.hexdigest()
