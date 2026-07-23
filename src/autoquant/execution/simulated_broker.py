from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from autoquant.backtest.models import OrderSide
from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.control import KillSwitchControl
from autoquant.execution.models import (
    ZERO_HASH,
    ApprovedPaperOrder,
    BrokerOrderUpdate,
    PaperOrderHistory,
    PaperOrderState,
    order_payload,
    update_payload,
)
from autoquant.risk.models import MarketQuote

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_BROKER_COLUMNS = """
order_hash, account_id, client_order_id, broker_order_id, state,
cumulative_filled_quantity, average_fill_price, last_broker_sequence,
last_fact_hash, state_hash, order_payload, state_payload, updated_at
"""


class SimulatedBrokerControlError(ValueError):
    """The durable execution-control fence no longer authorizes dispatch."""


@dataclass(frozen=True, slots=True)
class SimulatedBrokerOrder:
    order: ApprovedPaperOrder
    broker_order_id: str
    state: PaperOrderState
    cumulative_filled_quantity: int
    average_fill_price: Decimal | None
    last_broker_sequence: int
    last_fact_hash: str
    updated_at: datetime
    state_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(self.broker_order_id, name="broker_order_id")
        if self.state is PaperOrderState.APPROVED:
            raise ValueError("simulated broker cannot own approved state")
        if self.last_broker_sequence < 1:
            raise ValueError("last_broker_sequence must be positive")
        if not 0 <= self.cumulative_filled_quantity <= self.order.quantity:
            raise ValueError("cumulative fill is outside order quantity")
        if self.cumulative_filled_quantity == 0:
            if self.average_fill_price is not None:
                raise ValueError("zero fills cannot have an average price")
        elif self.average_fill_price is None or self.average_fill_price <= 0:
            raise ValueError("fills require a positive average price")
        if self.state is PaperOrderState.SUBMITTED:
            if self.cumulative_filled_quantity != 0:
                raise ValueError("submitted state cannot contain fills")
        elif self.state is PaperOrderState.PARTIALLY_FILLED:
            if not 0 < self.cumulative_filled_quantity < self.order.quantity:
                raise ValueError("partial state requires a partial fill")
        elif self.state is PaperOrderState.FILLED:
            if self.cumulative_filled_quantity != self.order.quantity:
                raise ValueError("filled state must equal order quantity")
        elif self.state is PaperOrderState.REJECTED:
            if self.cumulative_filled_quantity != 0:
                raise ValueError("rejected state cannot contain fills")
        elif self.state is PaperOrderState.CANCELLED:
            if self.cumulative_filled_quantity == self.order.quantity:
                raise ValueError("cancelled state cannot be fully filled")
        _require_lowercase_sha256(self.last_fact_hash, name="last_fact_hash")
        updated_at = to_utc(self.updated_at, name="simulated broker updated_at")
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(
            self,
            "state_hash",
            _canonical_hash(simulated_broker_state_payload(self)),
        )


@dataclass(frozen=True, slots=True)
class SimulatedBrokerSummary:
    order_count: int
    fact_count: int
    open_order_count: int
    recovery_verified: bool


class PersistentSimulatedBroker:
    """A deterministic PostgreSQL simulator with no network or real-money capability."""

    def __init__(self, *, engine: AsyncEngine, schema: str = "public") -> None:
        if _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError("schema must be a safe PostgreSQL identifier")
        self._engine = engine
        self._schema = schema

    @classmethod
    def connect(cls, *, dsn: str, schema: str = "public") -> PersistentSimulatedBroker:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "PostgreSQL simulated broker connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def submit(
        self,
        *,
        order: ApprovedPaperOrder,
        quote: MarketQuote,
        now: datetime,
        control_fence: KillSwitchControl | None = None,
    ) -> tuple[BrokerOrderUpdate, ...]:
        submitted_at = to_utc(now, name="simulated submission time")
        if quote.instrument != order.instrument:
            raise ValueError("quote instrument does not match paper order")
        if not quote.market_open:
            raise ValueError("simulated broker refuses orders while market is closed")
        broker_order_id = f"sim-{order.order_hash[:40]}"
        try:
            async with self._engine.begin() as connection:
                await _lock(connection, order.order_hash)
                existing = await self._select_order(connection, order.order_hash)
                if existing is not None:
                    state = _state_from_row(existing)
                    if state.order != order:
                        raise ValueError("order hash belongs to another simulated order")
                    return await self._updates(connection, order.order_hash)
                if control_fence is not None:
                    await self._verify_control_fence(
                        connection,
                        order=order,
                        expected=control_fence,
                    )
                approved = await connection.scalar(
                    text(
                        f"SELECT count(*) FROM {self._schema}.paper_orders "
                        "WHERE order_hash = :order_hash "
                        "AND account_id = :account_id "
                        "AND client_order_id = :client_order_id"
                    ),
                    {
                        "order_hash": order.order_hash,
                        "account_id": order.account_id,
                        "client_order_id": order.client_order_id,
                    },
                )
                if int(approved or 0) != 1:
                    raise ValueError("simulated broker requires a persisted paper order")
                submitted = BrokerOrderUpdate(
                    account_id=order.account_id,
                    client_order_id=order.client_order_id,
                    broker_order_id=broker_order_id,
                    broker_sequence=1,
                    state=PaperOrderState.SUBMITTED,
                    cumulative_filled_quantity=0,
                    average_fill_price=None,
                    occurred_at=submitted_at,
                )
                state = _initial_state(order, submitted)
                fact_hash = _fact_hash(
                    order_hash=order.order_hash,
                    previous_hash=ZERO_HASH,
                    update=submitted,
                )
                state = _with_fact(state, fact_hash)
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.simulated_broker_orders
                            (order_hash, account_id, client_order_id, broker_order_id,
                             state, cumulative_filled_quantity, average_fill_price,
                             last_broker_sequence, last_fact_hash, state_hash,
                             order_payload, state_payload, updated_at)
                        VALUES
                            (:order_hash, :account_id, :client_order_id, :broker_order_id,
                             :state, :cumulative_filled_quantity, :average_fill_price,
                             :last_broker_sequence, :last_fact_hash, :state_hash,
                             CAST(:order_payload AS jsonb), CAST(:state_payload AS jsonb),
                             :updated_at)
                        """
                    ),
                    _state_parameters(state),
                )
                await _insert_fact(
                    connection,
                    schema=self._schema,
                    order_hash=order.order_hash,
                    previous_hash=ZERO_HASH,
                    fact_hash=fact_hash,
                    update=submitted,
                )
                fill_price = _marketable_price(order, quote)
                if fill_price is not None:
                    filled = BrokerOrderUpdate(
                        account_id=order.account_id,
                        client_order_id=order.client_order_id,
                        broker_order_id=broker_order_id,
                        broker_sequence=2,
                        state=PaperOrderState.FILLED,
                        cumulative_filled_quantity=order.quantity,
                        average_fill_price=fill_price,
                        occurred_at=submitted_at,
                    )
                    state = await self._append_update(connection, state, filled)
                return await self._updates(connection, order.order_hash)
        except (ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError("Simulated broker submission failed") from None

    async def _verify_control_fence(
        self,
        connection: AsyncConnection,
        *,
        order: ApprovedPaperOrder,
        expected: KillSwitchControl,
    ) -> None:
        row = (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT active, state_hash
                        FROM {self._schema}.execution_control_state
                        WHERE account_id = :account_id
                        FOR SHARE
                        """
                    ),
                    {"account_id": order.account_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if (
            expected.account_id != order.account_id
            or expected.active
            or row is None
            or bool(row["active"])
            or str(row["state_hash"]) != expected.state_hash
        ):
            raise SimulatedBrokerControlError("simulated broker control fence rejected submission")

    async def replay(self, *, order_hash: str) -> SimulatedBrokerOrder:
        try:
            async with self._engine.connect() as connection:
                row = await self._select_order(connection, order_hash)
                if row is None:
                    raise LookupError("simulated broker order not found")
                current = _state_from_row(row)
                updates = await self._updates(connection, order_hash)
                fact_rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT fact_hash, previous_hash, update_hash
                                FROM {self._schema}.simulated_broker_facts
                                WHERE order_hash = :order_hash
                                ORDER BY broker_sequence
                                """
                            ),
                            {"order_hash": order_hash},
                        )
                    )
                    .mappings()
                    .all()
                )
        except (LookupError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError("Simulated broker replay read failed") from None
        replayed: SimulatedBrokerOrder | None = None
        previous_hash = ZERO_HASH
        for update, fact_row in zip(updates, fact_rows, strict=True):
            expected_fact = _fact_hash(
                order_hash=current.order.order_hash,
                previous_hash=previous_hash,
                update=update,
            )
            if (
                expected_fact != str(fact_row["fact_hash"])
                or previous_hash != str(fact_row["previous_hash"])
                or update.update_hash != str(fact_row["update_hash"])
            ):
                raise PersistenceUnavailableError(
                    "Simulated broker fact failed integrity verification"
                )
            if replayed is None:
                replayed = _with_fact(_initial_state(current.order, update), expected_fact)
            else:
                replayed = _transition(replayed, update, expected_fact)
            previous_hash = expected_fact
        if replayed is None or replayed != current:
            raise PersistenceUnavailableError("Simulated broker state does not match fact replay")
        return replayed

    async def verify_recovery(self, *, max_orders: int = 10_000) -> SimulatedBrokerSummary:
        if max_orders < 1:
            raise ValueError("max_orders must be positive")
        try:
            async with self._engine.connect() as connection:
                counts = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT
                                  (SELECT count(*)
                                   FROM {self._schema}.simulated_broker_orders)
                                    AS order_count,
                                  (SELECT count(*)
                                   FROM {self._schema}.simulated_broker_facts)
                                    AS fact_count,
                                  (SELECT count(*)
                                   FROM {self._schema}.simulated_broker_orders
                                   WHERE state NOT IN ('filled', 'cancelled', 'rejected'))
                                    AS open_order_count
                                """
                            )
                        )
                    )
                    .mappings()
                    .one()
                )
                order_hashes = tuple(
                    str(row["order_hash"])
                    for row in (
                        (
                            await connection.execute(
                                text(
                                    f"""
                                    SELECT order_hash
                                    FROM {self._schema}.simulated_broker_orders
                                    ORDER BY created_at, order_hash
                                    LIMIT :limit
                                    """
                                ),
                                {"limit": max_orders + 1},
                            )
                        )
                        .mappings()
                        .all()
                    )
                )
        except Exception:
            raise PersistenceUnavailableError(
                "Simulated broker recovery inventory failed"
            ) from None
        if len(order_hashes) > max_orders:
            raise PersistenceUnavailableError(
                "Simulated broker recovery exceeds configured verification bound"
            )
        for order_hash in order_hashes:
            await self.replay(order_hash=order_hash)
        return SimulatedBrokerSummary(
            order_count=int(counts["order_count"]),
            fact_count=int(counts["fact_count"]),
            open_order_count=int(counts["open_order_count"]),
            recovery_verified=True,
        )

    async def order_history(self, *, order_hash: str) -> PaperOrderHistory:
        current = await self.replay(order_hash=order_hash)
        try:
            async with self._engine.connect() as connection:
                updates = await self._updates(connection, order_hash)
        except Exception:
            raise PersistenceUnavailableError(
                "Simulated broker order history read failed"
            ) from None
        return PaperOrderHistory(
            order=current.order,
            state=current.state,
            updates=updates,
        )

    async def account_histories(
        self, *, account_id: str, max_orders: int = 10_000
    ) -> tuple[PaperOrderHistory, ...]:
        if max_orders < 1:
            raise ValueError("max_orders must be positive")
        try:
            async with self._engine.connect() as connection:
                order_hashes = tuple(
                    str(row["order_hash"])
                    for row in (
                        (
                            await connection.execute(
                                text(
                                    f"""
                                    SELECT order_hash
                                    FROM {self._schema}.simulated_broker_orders
                                    WHERE account_id = :account_id
                                    ORDER BY created_at, order_hash
                                    LIMIT :limit
                                    """
                                ),
                                {
                                    "account_id": account_id,
                                    "limit": max_orders + 1,
                                },
                            )
                        )
                        .mappings()
                        .all()
                    )
                )
        except Exception:
            raise PersistenceUnavailableError(
                "Simulated broker account history inventory failed"
            ) from None
        if len(order_hashes) > max_orders:
            raise PersistenceUnavailableError(
                "Simulated broker account history exceeds projection bound"
            )
        return tuple(
            [await self.order_history(order_hash=order_hash) for order_hash in order_hashes]
        )

    async def _append_update(
        self,
        connection: AsyncConnection,
        current: SimulatedBrokerOrder,
        update: BrokerOrderUpdate,
    ) -> SimulatedBrokerOrder:
        fact_hash = _fact_hash(
            order_hash=current.order.order_hash,
            previous_hash=current.last_fact_hash,
            update=update,
        )
        state = _transition(current, update, fact_hash)
        await _insert_fact(
            connection,
            schema=self._schema,
            order_hash=current.order.order_hash,
            previous_hash=current.last_fact_hash,
            fact_hash=fact_hash,
            update=update,
        )
        result = await connection.execute(
            text(
                f"""
                UPDATE {self._schema}.simulated_broker_orders
                SET state = :state,
                    cumulative_filled_quantity = :cumulative_filled_quantity,
                    average_fill_price = :average_fill_price,
                    last_broker_sequence = :last_broker_sequence,
                    last_fact_hash = :last_fact_hash,
                    state_hash = :state_hash,
                    state_payload = CAST(:state_payload AS jsonb),
                    updated_at = :updated_at
                WHERE order_hash = :order_hash
                  AND state_hash = :previous_state_hash
                """
            ),
            {
                **_state_parameters(state),
                "previous_state_hash": current.state_hash,
            },
        )
        if result.rowcount != 1:
            raise PersistenceUnavailableError("Simulated broker optimistic update failed")
        return state

    async def _select_order(
        self, connection: AsyncConnection, order_hash: str
    ) -> RowMapping | None:
        return (
            (
                await connection.execute(
                    text(
                        f"SELECT {_BROKER_COLUMNS} "
                        f"FROM {self._schema}.simulated_broker_orders "
                        "WHERE order_hash = :order_hash"
                    ),
                    {"order_hash": order_hash},
                )
            )
            .mappings()
            .one_or_none()
        )

    async def _updates(
        self, connection: AsyncConnection, order_hash: str
    ) -> tuple[BrokerOrderUpdate, ...]:
        rows = (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT update_hash, update_payload
                        FROM {self._schema}.simulated_broker_facts
                        WHERE order_hash = :order_hash
                        ORDER BY broker_sequence
                        """
                    ),
                    {"order_hash": order_hash},
                )
            )
            .mappings()
            .all()
        )
        return tuple(_update_from_row(row) for row in rows)


def _initial_state(order: ApprovedPaperOrder, update: BrokerOrderUpdate) -> SimulatedBrokerOrder:
    if update.broker_sequence != 1 or update.state is not PaperOrderState.SUBMITTED:
        raise ValueError("first simulated broker fact must be submitted sequence one")
    return SimulatedBrokerOrder(
        order=order,
        broker_order_id=update.broker_order_id,
        state=update.state,
        cumulative_filled_quantity=update.cumulative_filled_quantity,
        average_fill_price=update.average_fill_price,
        last_broker_sequence=update.broker_sequence,
        last_fact_hash=ZERO_HASH,
        updated_at=update.occurred_at,
    )


def _transition(
    current: SimulatedBrokerOrder,
    update: BrokerOrderUpdate,
    fact_hash: str,
) -> SimulatedBrokerOrder:
    if current.state in {
        PaperOrderState.FILLED,
        PaperOrderState.CANCELLED,
        PaperOrderState.REJECTED,
    }:
        raise ValueError("terminal simulated broker order cannot change")
    if update.broker_order_id != current.broker_order_id:
        raise ValueError("simulated broker_order_id cannot change")
    if update.broker_sequence != current.last_broker_sequence + 1:
        raise ValueError("simulated broker sequence must increment by one")
    if update.occurred_at < current.updated_at:
        raise ValueError("simulated broker time cannot move backwards")
    if update.cumulative_filled_quantity < current.cumulative_filled_quantity:
        raise ValueError("simulated broker cumulative fill cannot decrease")
    return SimulatedBrokerOrder(
        order=current.order,
        broker_order_id=current.broker_order_id,
        state=update.state,
        cumulative_filled_quantity=update.cumulative_filled_quantity,
        average_fill_price=update.average_fill_price,
        last_broker_sequence=update.broker_sequence,
        last_fact_hash=fact_hash,
        updated_at=update.occurred_at,
    )


def _with_fact(state: SimulatedBrokerOrder, fact_hash: str) -> SimulatedBrokerOrder:
    return SimulatedBrokerOrder(
        order=state.order,
        broker_order_id=state.broker_order_id,
        state=state.state,
        cumulative_filled_quantity=state.cumulative_filled_quantity,
        average_fill_price=state.average_fill_price,
        last_broker_sequence=state.last_broker_sequence,
        last_fact_hash=fact_hash,
        updated_at=state.updated_at,
    )


def _marketable_price(order: ApprovedPaperOrder, quote: MarketQuote) -> Decimal | None:
    if order.side is OrderSide.BUY:
        if order.limit_price is not None and order.limit_price < quote.ask_price:
            return None
        return quote.ask_price
    if order.limit_price is not None and order.limit_price > quote.bid_price:
        return None
    return quote.bid_price


def _fact_hash(*, order_hash: str, previous_hash: str, update: BrokerOrderUpdate) -> str:
    return _canonical_hash(
        {
            "broker_sequence": update.broker_sequence,
            "order_hash": order_hash,
            "previous_hash": previous_hash,
            "update_hash": update.update_hash,
        }
    )


def simulated_broker_state_payload(
    state: SimulatedBrokerOrder,
) -> dict[str, object]:
    return {
        "average_fill_price": (
            None if state.average_fill_price is None else _decimal_text(state.average_fill_price)
        ),
        "broker_order_id": state.broker_order_id,
        "cumulative_filled_quantity": state.cumulative_filled_quantity,
        "last_broker_sequence": state.last_broker_sequence,
        "last_fact_hash": state.last_fact_hash,
        "order_hash": state.order.order_hash,
        "state": state.state.value,
        "updated_at": state.updated_at.isoformat(timespec="microseconds"),
    }


def _state_parameters(state: SimulatedBrokerOrder) -> dict[str, object]:
    return {
        "order_hash": state.order.order_hash,
        "account_id": state.order.account_id,
        "client_order_id": state.order.client_order_id,
        "broker_order_id": state.broker_order_id,
        "state": state.state.value,
        "cumulative_filled_quantity": state.cumulative_filled_quantity,
        "average_fill_price": state.average_fill_price,
        "last_broker_sequence": state.last_broker_sequence,
        "last_fact_hash": state.last_fact_hash,
        "state_hash": state.state_hash,
        "order_payload": _json(order_payload(state.order)),
        "state_payload": _json(simulated_broker_state_payload(state)),
        "updated_at": state.updated_at,
    }


def _state_from_row(row: RowMapping) -> SimulatedBrokerOrder:
    try:
        order_data = _object(row["order_payload"])
        state_data = _object(row["state_payload"])
        order = _order_from_payload(order_data)
        state = SimulatedBrokerOrder(
            order=order,
            broker_order_id=str(state_data["broker_order_id"]),
            state=PaperOrderState(str(state_data["state"])),
            cumulative_filled_quantity=int(str(state_data["cumulative_filled_quantity"])),
            average_fill_price=(
                None
                if state_data["average_fill_price"] is None
                else Decimal(str(state_data["average_fill_price"]))
            ),
            last_broker_sequence=int(str(state_data["last_broker_sequence"])),
            last_fact_hash=str(state_data["last_fact_hash"]),
            updated_at=_datetime(state_data["updated_at"]),
        )
        if (
            state.order.order_hash != str(row["order_hash"])
            or state.order.account_id != str(row["account_id"])
            or state.order.client_order_id != str(row["client_order_id"])
            or state.broker_order_id != str(row["broker_order_id"])
            or state.state.value != str(row["state"])
            or state.cumulative_filled_quantity != int(row["cumulative_filled_quantity"])
            or state.average_fill_price != row["average_fill_price"]
            or state.last_broker_sequence != int(row["last_broker_sequence"])
            or state.last_fact_hash != str(row["last_fact_hash"])
            or state.state_hash != str(row["state_hash"])
            or state.updated_at != _datetime(row["updated_at"])
        ):
            raise ValueError("simulated broker columns do not match payload")
        return state
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "Stored simulated broker order failed integrity verification"
        ) from None


def _order_from_payload(payload: dict[str, object]) -> ApprovedPaperOrder:
    order = ApprovedPaperOrder(
        account_id=str(payload["account_id"]),
        client_order_id=str(payload["client_order_id"]),
        risk_decision_hash=str(payload["risk_decision_hash"]),
        instrument=str(payload["instrument"]),
        side=OrderSide(str(payload["side"])),
        quantity=int(str(payload["quantity"])),
        limit_price=(
            None if payload["limit_price"] is None else Decimal(str(payload["limit_price"]))
        ),
        approved_at=_datetime(payload["approved_at"]),
    )
    if _canonical_hash(payload) != order.order_hash:
        raise ValueError("paper order payload hash mismatch")
    return order


def _update_from_row(row: RowMapping) -> BrokerOrderUpdate:
    payload = _object(row["update_payload"])
    try:
        update = BrokerOrderUpdate(
            account_id=str(payload["account_id"]),
            client_order_id=str(payload["client_order_id"]),
            broker_order_id=str(payload["broker_order_id"]),
            broker_sequence=int(str(payload["broker_sequence"])),
            state=PaperOrderState(str(payload["state"])),
            cumulative_filled_quantity=int(str(payload["cumulative_filled_quantity"])),
            average_fill_price=(
                None
                if payload["average_fill_price"] is None
                else Decimal(str(payload["average_fill_price"]))
            ),
            occurred_at=_datetime(payload["occurred_at"]),
            rejection_code=(
                None if payload["rejection_code"] is None else str(payload["rejection_code"])
            ),
        )
        if update.update_hash != str(row["update_hash"]):
            raise ValueError("simulated broker update hash mismatch")
        return update
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "Stored simulated broker update failed integrity verification"
        ) from None


async def _insert_fact(
    connection: AsyncConnection,
    *,
    schema: str,
    order_hash: str,
    previous_hash: str,
    fact_hash: str,
    update: BrokerOrderUpdate,
) -> None:
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema}.simulated_broker_facts
                (fact_hash, order_hash, broker_sequence, previous_hash,
                 update_hash, update_payload)
            VALUES
                (:fact_hash, :order_hash, :broker_sequence, :previous_hash,
                 :update_hash, CAST(:update_payload AS jsonb))
            """
        ),
        {
            "fact_hash": fact_hash,
            "order_hash": order_hash,
            "broker_sequence": update.broker_sequence,
            "previous_hash": previous_hash,
            "update_hash": update.update_hash,
            "update_payload": _json(update_payload(update)),
        },
    )


async def _lock(connection: AsyncConnection, order_hash: str) -> None:
    await connection.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
        {"lock_key": f"autoquant:simulated-broker:{order_hash}"},
    )


def _object(raw: object) -> dict[str, object]:
    value = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(value, dict):
        raise TypeError("stored payload must be an object")
    return value


def _datetime(raw: object) -> datetime:
    if isinstance(raw, datetime):
        return to_utc(raw)
    if isinstance(raw, str):
        return to_utc(datetime.fromisoformat(raw))
    raise TypeError("stored timestamp is invalid")


def _json(value: object) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)
