from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
    _require_nonblank,
)


def _finite(value: Decimal, *, name: str, minimum: Decimal | None = None) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")


class ReconciliationCode(StrEnum):
    STALE_INTERNAL_SNAPSHOT = "stale_internal_snapshot"
    STALE_BROKER_SNAPSHOT = "stale_broker_snapshot"
    INTERNAL_ACCOUNT_UNBALANCED = "internal_account_unbalanced"
    BROKER_ACCOUNT_UNBALANCED = "broker_account_unbalanced"
    CASH_MISMATCH = "cash_mismatch"
    EQUITY_MISMATCH = "equity_mismatch"
    POSITION_MISMATCH = "position_mismatch"
    SELLABLE_MISMATCH = "sellable_mismatch"
    MARKET_VALUE_MISMATCH = "market_value_mismatch"
    OPEN_ORDER_MISMATCH = "open_order_mismatch"


@dataclass(frozen=True, slots=True)
class AccountPosition:
    instrument: str
    total_quantity: int
    sellable_quantity: int
    market_value: Decimal

    def __post_init__(self) -> None:
        _require_nonblank(self.instrument, name="position instrument")
        if self.total_quantity < 0 or self.sellable_quantity < 0:
            raise ValueError("position quantities cannot be negative")
        if self.sellable_quantity > self.total_quantity:
            raise ValueError("sellable quantity cannot exceed total quantity")
        _finite(self.market_value, name="market_value", minimum=Decimal("0"))


@dataclass(frozen=True, slots=True)
class ExecutionAccountSnapshot:
    account_id: str
    as_of: datetime
    cash: Decimal
    equity: Decimal
    positions: tuple[AccountPosition, ...] = ()
    open_client_order_ids: tuple[str, ...] = ()
    snapshot_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(self.account_id, name="account_id")
        as_of = to_utc(self.as_of, name="account snapshot as_of")
        object.__setattr__(self, "as_of", as_of)
        positions = tuple(sorted(self.positions, key=lambda item: item.instrument))
        orders = tuple(sorted(self.open_client_order_ids))
        object.__setattr__(self, "positions", positions)
        object.__setattr__(self, "open_client_order_ids", orders)
        if len({item.instrument for item in positions}) != len(positions):
            raise ValueError("positions must contain unique instruments")
        if len(set(orders)) != len(orders):
            raise ValueError("open_client_order_ids must be unique")
        for order_id in orders:
            _require_nonblank(order_id, name="open client_order_id")
        _finite(self.cash, name="cash", minimum=Decimal("0"))
        _finite(self.equity, name="equity", minimum=Decimal("0"))
        object.__setattr__(
            self,
            "snapshot_hash",
            _canonical_hash(snapshot_payload(self)),
        )


@dataclass(frozen=True, slots=True)
class ReconciliationReport:
    account_id: str
    evaluated_at: datetime
    internal_snapshot_hash: str
    broker_snapshot_hash: str
    issues: tuple[ReconciliationCode, ...]
    report_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(self.account_id, name="account_id")
        _require_lowercase_sha256(
            self.internal_snapshot_hash, name="internal_snapshot_hash"
        )
        _require_lowercase_sha256(
            self.broker_snapshot_hash, name="broker_snapshot_hash"
        )
        evaluated_at = to_utc(self.evaluated_at, name="reconciliation time")
        object.__setattr__(self, "evaluated_at", evaluated_at)
        issues = tuple(self.issues)
        object.__setattr__(self, "issues", issues)
        if len(set(issues)) != len(issues):
            raise ValueError("reconciliation issues must be unique")
        object.__setattr__(
            self,
            "report_hash",
            _canonical_hash(
                {
                    "account_id": self.account_id,
                    "broker_snapshot_hash": self.broker_snapshot_hash,
                    "evaluated_at": evaluated_at.isoformat(timespec="microseconds"),
                    "internal_snapshot_hash": self.internal_snapshot_hash,
                    "issues": [issue.value for issue in issues],
                }
            ),
        )

    @property
    def reconciled(self) -> bool:
        return not self.issues


class AccountReconciler:
    def reconcile(
        self,
        *,
        internal: ExecutionAccountSnapshot,
        broker: ExecutionAccountSnapshot,
        now: datetime,
        max_age: timedelta = timedelta(seconds=5),
        money_tolerance: Decimal = Decimal("0.01"),
    ) -> ReconciliationReport:
        if internal.account_id != broker.account_id:
            raise ValueError("account snapshots must belong to the same account")
        evaluated_at = to_utc(now, name="reconciliation time")
        if max_age <= timedelta(0):
            raise ValueError("max_age must be positive")
        _finite(money_tolerance, name="money_tolerance", minimum=Decimal("0"))
        issues: list[ReconciliationCode] = []
        self._add(
            issues,
            evaluated_at < internal.as_of
            or evaluated_at - internal.as_of > max_age,
            ReconciliationCode.STALE_INTERNAL_SNAPSHOT,
        )
        self._add(
            issues,
            evaluated_at < broker.as_of or evaluated_at - broker.as_of > max_age,
            ReconciliationCode.STALE_BROKER_SNAPSHOT,
        )
        internal_market_value = sum(
            (item.market_value for item in internal.positions), Decimal("0")
        )
        broker_market_value = sum(
            (item.market_value for item in broker.positions), Decimal("0")
        )
        self._add(
            issues,
            (internal.cash + internal_market_value - internal.equity).copy_abs()
            > money_tolerance,
            ReconciliationCode.INTERNAL_ACCOUNT_UNBALANCED,
        )
        self._add(
            issues,
            (broker.cash + broker_market_value - broker.equity).copy_abs()
            > money_tolerance,
            ReconciliationCode.BROKER_ACCOUNT_UNBALANCED,
        )
        self._add(
            issues,
            (internal.cash - broker.cash).copy_abs() > money_tolerance,
            ReconciliationCode.CASH_MISMATCH,
        )
        self._add(
            issues,
            (internal.equity - broker.equity).copy_abs() > money_tolerance,
            ReconciliationCode.EQUITY_MISMATCH,
        )
        internal_positions = {item.instrument: item for item in internal.positions}
        broker_positions = {item.instrument: item for item in broker.positions}
        instruments = set(internal_positions) | set(broker_positions)
        self._add(
            issues,
            any(
                instrument not in internal_positions
                or instrument not in broker_positions
                or internal_positions[instrument].total_quantity
                != broker_positions[instrument].total_quantity
                for instrument in instruments
            ),
            ReconciliationCode.POSITION_MISMATCH,
        )
        self._add(
            issues,
            any(
                instrument in internal_positions
                and instrument in broker_positions
                and internal_positions[instrument].sellable_quantity
                != broker_positions[instrument].sellable_quantity
                for instrument in instruments
            ),
            ReconciliationCode.SELLABLE_MISMATCH,
        )
        self._add(
            issues,
            any(
                instrument in internal_positions
                and instrument in broker_positions
                and (
                    internal_positions[instrument].market_value
                    - broker_positions[instrument].market_value
                ).copy_abs()
                > money_tolerance
                for instrument in instruments
            ),
            ReconciliationCode.MARKET_VALUE_MISMATCH,
        )
        self._add(
            issues,
            internal.open_client_order_ids != broker.open_client_order_ids,
            ReconciliationCode.OPEN_ORDER_MISMATCH,
        )
        return ReconciliationReport(
            account_id=internal.account_id,
            evaluated_at=evaluated_at,
            internal_snapshot_hash=internal.snapshot_hash,
            broker_snapshot_hash=broker.snapshot_hash,
            issues=tuple(issues),
        )

    @staticmethod
    def _add(
        values: list[ReconciliationCode],
        condition: bool,
        code: ReconciliationCode,
    ) -> None:
        if condition:
            values.append(code)


def snapshot_payload(snapshot: ExecutionAccountSnapshot) -> dict[str, object]:
    return {
        "account_id": snapshot.account_id,
        "as_of": snapshot.as_of.isoformat(timespec="microseconds"),
        "cash": _decimal_text(snapshot.cash),
        "equity": _decimal_text(snapshot.equity),
        "open_client_order_ids": list(snapshot.open_client_order_ids),
        "positions": [
            {
                "instrument": item.instrument,
                "market_value": _decimal_text(item.market_value),
                "sellable_quantity": item.sellable_quantity,
                "total_quantity": item.total_quantity,
            }
            for item in snapshot.positions
        ],
    }
