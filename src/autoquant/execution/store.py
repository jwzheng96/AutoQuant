from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.backtest.models import OrderSide
from autoquant.data.models import _canonical_hash, _decimal_text
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.models import (
    ApprovedPaperOrder,
    BrokerOrderUpdate,
    PaperOrderHistory,
    PaperOrderProjection,
    PaperOrderState,
    PaperOrderTransition,
    order_payload,
    projection_payload,
    update_payload,
)
from autoquant.execution.reconciliation import (
    AccountPosition,
    ExecutionAccountSnapshot,
    ReconciliationCode,
    ReconciliationReport,
    snapshot_payload,
)
from autoquant.execution.state_machine import PaperOrderStateMachine

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_ORDER_COLUMNS = """
order_hash, account_id, client_order_id, risk_decision_hash, instrument,
side, quantity, approved_at, state, broker_order_id, version,
projection_hash, order_payload, projection_payload, updated_at
"""


@dataclass(frozen=True, slots=True)
class ExecutionStoreSummary:
    order_count: int
    event_count: int
    reconciliation_count: int
    open_order_count: int
    latest_reconciliation_at: datetime | None
    latest_reconciled: bool | None
    recovery_verified: bool


class PostgresPaperExecutionRepository:
    """Atomic materialized paper orders backed by immutable transition facts."""

    def __init__(self, *, engine: AsyncEngine, schema: str = "public") -> None:
        if _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError("schema must be a safe PostgreSQL identifier")
        self._engine = engine
        self._schema = schema
        self._machine = PaperOrderStateMachine()

    @classmethod
    def connect(
        cls, *, dsn: str, schema: str = "public"
    ) -> PostgresPaperExecutionRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "PostgreSQL paper execution connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def create_order(self, order: ApprovedPaperOrder) -> PaperOrderProjection:
        projection = PaperOrderProjection.create(order)
        parameters = {
            "order_hash": order.order_hash,
            "account_id": order.account_id,
            "client_order_id": order.client_order_id,
            "risk_decision_hash": order.risk_decision_hash,
            "instrument": order.instrument,
            "side": order.side.value,
            "quantity": order.quantity,
            "limit_price": (
                None if order.limit_price is None else _decimal_text(order.limit_price)
            ),
            "approved_at": order.approved_at,
            "state": projection.state.value,
            "version": projection.version,
            "projection_hash": projection.projection_hash,
            "order_payload": _json(order_payload(order)),
            "projection_payload": _json(projection_payload(projection)),
            "updated_at": projection.updated_at,
        }
        sql = text(
            f"""
            INSERT INTO {self._schema}.paper_orders
                (order_hash, account_id, client_order_id, risk_decision_hash,
                 instrument, side, quantity, approved_at, state, version,
                 projection_hash, order_payload, projection_payload, updated_at)
            SELECT
                :order_hash, :account_id, :client_order_id, :risk_decision_hash,
                :instrument, :side, :quantity, :approved_at, :state, :version,
                :projection_hash, CAST(:order_payload AS jsonb),
                CAST(:projection_payload AS jsonb), :updated_at
            FROM {self._schema}.risk_decisions
            WHERE decision_hash = :risk_decision_hash
              AND account_id = :account_id
              AND client_order_id = :client_order_id
              AND mode = 'paper'
              AND state = 'accepted'
              AND payload -> 'order' ->> 'instrument' = :instrument
              AND payload -> 'order' ->> 'side' = :side
              AND (payload -> 'order' ->> 'quantity')::bigint = :quantity
              AND payload -> 'order' ->> 'limit_price'
                  IS NOT DISTINCT FROM :limit_price
              AND evaluated_at = :approved_at
            ON CONFLICT (account_id, client_order_id) DO NOTHING
            RETURNING {_ORDER_COLUMNS}
            """
        )
        try:
            async with self._engine.begin() as connection:
                row = (await connection.execute(sql, parameters)).mappings().one_or_none()
                if row is None:
                    row = (
                        (
                            await connection.execute(
                                text(
                                    f"SELECT {_ORDER_COLUMNS} "
                                    f"FROM {self._schema}.paper_orders "
                                    "WHERE account_id = :account_id "
                                    "AND client_order_id = :client_order_id"
                                ),
                                {
                                    "account_id": order.account_id,
                                    "client_order_id": order.client_order_id,
                                },
                            )
                        )
                        .mappings()
                        .one_or_none()
                    )
        except Exception:
            raise PersistenceUnavailableError("Paper order creation failed") from None
        if row is None:
            raise ValueError("paper order requires its accepted persisted risk decision")
        stored = _projection_from_row(row)
        if stored.order != order:
            raise ValueError("client_order_id already belongs to another paper order")
        return stored

    async def load_order(
        self, *, account_id: str, client_order_id: str
    ) -> PaperOrderProjection:
        try:
            async with self._engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"SELECT {_ORDER_COLUMNS} "
                                f"FROM {self._schema}.paper_orders "
                                "WHERE account_id = :account_id "
                                "AND client_order_id = :client_order_id"
                            ),
                            {
                                "account_id": account_id,
                                "client_order_id": client_order_id,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError("Paper order read failed") from None
        if row is None:
            raise LookupError("paper order not found")
        return _projection_from_row(row)

    async def apply_update(
        self,
        *,
        account_id: str,
        client_order_id: str,
        update: BrokerOrderUpdate,
    ) -> PaperOrderTransition:
        try:
            async with self._engine.begin() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"SELECT {_ORDER_COLUMNS} "
                                f"FROM {self._schema}.paper_orders "
                                "WHERE account_id = :account_id "
                                "AND client_order_id = :client_order_id FOR UPDATE"
                            ),
                            {
                                "account_id": account_id,
                                "client_order_id": client_order_id,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise LookupError("paper order not found")
                projection = _projection_from_row(row)
                prior = (
                    (
                        await connection.execute(
                            text(
                                f"SELECT update_hash FROM {self._schema}.paper_order_events "
                                "WHERE order_hash = :order_hash "
                                "AND broker_sequence = :broker_sequence"
                            ),
                            {
                                "order_hash": projection.order.order_hash,
                                "broker_sequence": update.broker_sequence,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if prior is not None:
                    if str(prior["update_hash"]) != update.update_hash:
                        raise ValueError(
                            "broker sequence already belongs to another update"
                        )
                    return PaperOrderTransition(
                        projection=projection, event=None, applied=False
                    )
                transition = self._machine.apply(projection, update)
                event = transition.event
                if event is None:
                    return transition
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.paper_order_events
                            (event_hash, order_hash, sequence, broker_sequence,
                             previous_hash, update_hash, resulting_state,
                             transition_projection_hash, update_payload)
                        VALUES
                            (:event_hash, :order_hash, :sequence, :broker_sequence,
                             :previous_hash, :update_hash, :resulting_state,
                             :transition_projection_hash, CAST(:update_payload AS jsonb))
                        """
                    ),
                    {
                        "event_hash": event.event_hash,
                        "order_hash": projection.order.order_hash,
                        "sequence": event.sequence,
                        "broker_sequence": update.broker_sequence,
                        "previous_hash": event.previous_hash,
                        "update_hash": event.update_hash,
                        "resulting_state": event.resulting_state.value,
                        "transition_projection_hash": event.projection_hash,
                        "update_payload": _json(update_payload(update)),
                    },
                )
                result = await connection.execute(
                    text(
                        f"""
                        UPDATE {self._schema}.paper_orders
                        SET state = :state,
                            broker_order_id = :broker_order_id,
                            version = :version,
                            projection_hash = :projection_hash,
                            projection_payload = CAST(:projection_payload AS jsonb),
                            updated_at = :updated_at
                        WHERE order_hash = :order_hash
                          AND projection_hash = :previous_projection_hash
                        """
                    ),
                    {
                        "state": transition.projection.state.value,
                        "broker_order_id": transition.projection.broker_order_id,
                        "version": transition.projection.version,
                        "projection_hash": transition.projection.projection_hash,
                        "projection_payload": _json(
                            projection_payload(transition.projection)
                        ),
                        "updated_at": transition.projection.updated_at,
                        "order_hash": projection.order.order_hash,
                        "previous_projection_hash": projection.projection_hash,
                    },
                )
                if result.rowcount != 1:
                    raise PersistenceUnavailableError(
                        "Paper order optimistic update failed"
                    )
                return transition
        except (LookupError, ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError("Paper order update failed") from None

    async def replay_order(
        self, *, account_id: str, client_order_id: str
    ) -> PaperOrderProjection:
        current = await self.load_order(
            account_id=account_id, client_order_id=client_order_id
        )
        try:
            async with self._engine.connect() as connection:
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT event_hash, sequence, broker_sequence,
                                       previous_hash, update_hash, resulting_state,
                                       transition_projection_hash, update_payload
                                FROM {self._schema}.paper_order_events
                                WHERE order_hash = :order_hash
                                ORDER BY sequence
                                """
                            ),
                            {"order_hash": current.order.order_hash},
                        )
                    )
                    .mappings()
                    .all()
                )
        except Exception:
            raise PersistenceUnavailableError("Paper order replay read failed") from None
        replayed = PaperOrderProjection.create(current.order)
        for row in rows:
            update = _update_from_payload(row["update_payload"])
            transition = self._machine.apply(replayed, update)
            if transition.event is None or not transition.applied:
                raise PersistenceUnavailableError("Paper order replay was not monotonic")
            event = transition.event
            if (
                event.event_hash != str(row["event_hash"])
                or event.sequence != int(row["sequence"])
                or event.previous_hash != str(row["previous_hash"])
                or event.update_hash != str(row["update_hash"])
                or event.resulting_state.value != str(row["resulting_state"])
                or event.projection_hash
                != str(row["transition_projection_hash"])
            ):
                raise PersistenceUnavailableError(
                    "Paper order event failed integrity verification"
                )
            replayed = transition.projection
        if replayed != current:
            raise PersistenceUnavailableError(
                "Paper order projection does not match event replay"
            )
        return replayed

    async def order_history(
        self, *, account_id: str, client_order_id: str
    ) -> PaperOrderHistory:
        current = await self.replay_order(
            account_id=account_id,
            client_order_id=client_order_id,
        )
        try:
            async with self._engine.connect() as connection:
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT update_hash, update_payload
                                FROM {self._schema}.paper_order_events
                                WHERE order_hash = :order_hash
                                ORDER BY sequence
                                """
                            ),
                            {"order_hash": current.order.order_hash},
                        )
                    )
                    .mappings()
                    .all()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "Paper order history read failed"
            ) from None
        updates = tuple(_verified_update_from_row(row) for row in rows)
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
                identifiers = tuple(
                    str(row["client_order_id"])
                    for row in (
                        (
                            await connection.execute(
                                text(
                                    f"""
                                    SELECT client_order_id
                                    FROM {self._schema}.paper_orders
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
                "Paper account history inventory failed"
            ) from None
        if len(identifiers) > max_orders:
            raise PersistenceUnavailableError(
                "Paper account history exceeds configured projection bound"
            )
        return tuple(
            [
                await self.order_history(
                    account_id=account_id,
                    client_order_id=client_order_id,
                )
                for client_order_id in identifiers
            ]
        )

    async def save_reconciliation(
        self,
        *,
        internal: ExecutionAccountSnapshot,
        broker: ExecutionAccountSnapshot,
        report: ReconciliationReport,
    ) -> ReconciliationReport:
        if internal.account_id != broker.account_id or report.account_id != internal.account_id:
            raise ValueError("reconciliation account ids must match")
        if (
            report.internal_snapshot_hash != internal.snapshot_hash
            or report.broker_snapshot_hash != broker.snapshot_hash
        ):
            raise ValueError("reconciliation report does not reference supplied snapshots")
        try:
            async with self._engine.begin() as connection:
                for snapshot in (internal, broker):
                    await connection.execute(
                        text(
                            f"""
                            INSERT INTO {self._schema}.execution_account_snapshots
                                (snapshot_hash, account_id, as_of, payload)
                            VALUES
                                (:snapshot_hash, :account_id, :as_of,
                                 CAST(:payload AS jsonb))
                            ON CONFLICT (snapshot_hash) DO NOTHING
                            """
                        ),
                        {
                            "snapshot_hash": snapshot.snapshot_hash,
                            "account_id": snapshot.account_id,
                            "as_of": snapshot.as_of,
                            "payload": _json(snapshot_payload(snapshot)),
                        },
                    )
                snapshot_rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT snapshot_hash, account_id, as_of, payload
                                FROM {self._schema}.execution_account_snapshots
                                WHERE snapshot_hash = ANY(:snapshot_hashes)
                                """
                            ),
                            {
                                "snapshot_hashes": list(
                                    {internal.snapshot_hash, broker.snapshot_hash}
                                )
                            },
                        )
                    )
                    .mappings()
                    .all()
                )
                stored_snapshots = {
                    snapshot.snapshot_hash: snapshot
                    for snapshot in (_snapshot_from_row(row) for row in snapshot_rows)
                }
                if stored_snapshots != {
                    internal.snapshot_hash: internal,
                    broker.snapshot_hash: broker,
                }:
                    raise PersistenceUnavailableError(
                        "Stored execution snapshot failed integrity verification"
                    )
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.execution_reconciliation_reports
                            (report_hash, account_id, evaluated_at,
                             internal_snapshot_hash, broker_snapshot_hash,
                             reconciled, issues)
                        VALUES
                            (:report_hash, :account_id, :evaluated_at,
                             :internal_snapshot_hash, :broker_snapshot_hash,
                             :reconciled, :issues)
                        ON CONFLICT (report_hash) DO NOTHING
                        """
                    ),
                    {
                        "report_hash": report.report_hash,
                        "account_id": report.account_id,
                        "evaluated_at": report.evaluated_at,
                        "internal_snapshot_hash": report.internal_snapshot_hash,
                        "broker_snapshot_hash": report.broker_snapshot_hash,
                        "reconciled": report.reconciled,
                        "issues": [issue.value for issue in report.issues],
                    },
                )
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT account_id, evaluated_at,
                                       internal_snapshot_hash, broker_snapshot_hash,
                                       reconciled, issues
                                FROM {self._schema}.execution_reconciliation_reports
                                WHERE report_hash = :report_hash
                                """
                            ),
                            {"report_hash": report.report_hash},
                        )
                    )
                    .mappings()
                    .one()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "Execution reconciliation persistence failed"
            ) from None
        stored = ReconciliationReport(
            account_id=str(row["account_id"]),
            evaluated_at=_datetime(row["evaluated_at"]),
            internal_snapshot_hash=str(row["internal_snapshot_hash"]),
            broker_snapshot_hash=str(row["broker_snapshot_hash"]),
            issues=tuple(ReconciliationCode(str(value)) for value in row["issues"]),
        )
        if stored != report or bool(row["reconciled"]) != report.reconciled:
            raise PersistenceUnavailableError(
                "Stored reconciliation failed integrity verification"
            )
        return stored

    async def verify_recovery(
        self, *, max_orders: int = 10_000
    ) -> ExecutionStoreSummary:
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
                                  (SELECT count(*) FROM {self._schema}.paper_orders)
                                    AS order_count,
                                  (SELECT count(*) FROM {self._schema}.paper_order_events)
                                    AS event_count,
                                  (SELECT count(*) FROM
                                     {self._schema}.execution_reconciliation_reports)
                                    AS reconciliation_count,
                                  (SELECT count(*) FROM {self._schema}.paper_orders
                                   WHERE state NOT IN ('filled', 'cancelled', 'rejected'))
                                    AS open_order_count
                                """
                            )
                        )
                    )
                    .mappings()
                    .one()
                )
                latest = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT evaluated_at, reconciled
                                FROM {self._schema}.execution_reconciliation_reports
                                ORDER BY evaluated_at DESC, report_hash DESC
                                LIMIT 1
                                """
                            )
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                identifiers = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT account_id, client_order_id
                                FROM {self._schema}.paper_orders
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
        except Exception:
            raise PersistenceUnavailableError(
                "Paper execution recovery inventory failed"
            ) from None
        if len(identifiers) > max_orders:
            raise PersistenceUnavailableError(
                "Paper execution recovery exceeds configured verification bound"
            )
        for identifier in identifiers:
            await self.replay_order(
                account_id=str(identifier["account_id"]),
                client_order_id=str(identifier["client_order_id"]),
            )
        return ExecutionStoreSummary(
            order_count=int(counts["order_count"]),
            event_count=int(counts["event_count"]),
            reconciliation_count=int(counts["reconciliation_count"]),
            open_order_count=int(counts["open_order_count"]),
            latest_reconciliation_at=(
                None if latest is None else _datetime(latest["evaluated_at"])
            ),
            latest_reconciled=(
                None if latest is None else bool(latest["reconciled"])
            ),
            recovery_verified=True,
        )


def _projection_from_row(row: RowMapping) -> PaperOrderProjection:
    try:
        order_data = _object(row["order_payload"])
        projection_data = _object(row["projection_payload"])
        order = ApprovedPaperOrder(
            account_id=str(order_data["account_id"]),
            client_order_id=str(order_data["client_order_id"]),
            risk_decision_hash=str(order_data["risk_decision_hash"]),
            instrument=str(order_data["instrument"]),
            side=OrderSide(str(order_data["side"])),
            quantity=int(str(order_data["quantity"])),
            limit_price=(
                None
                if order_data["limit_price"] is None
                else Decimal(str(order_data["limit_price"]))
            ),
            approved_at=_datetime(order_data["approved_at"]),
        )
        projection = PaperOrderProjection(
            order=order,
            state=PaperOrderState(str(projection_data["state"])),
            broker_order_id=(
                None
                if projection_data["broker_order_id"] is None
                else str(projection_data["broker_order_id"])
            ),
            cumulative_filled_quantity=int(
                str(projection_data["cumulative_filled_quantity"])
            ),
            average_fill_price=(
                None
                if projection_data["average_fill_price"] is None
                else Decimal(str(projection_data["average_fill_price"]))
            ),
            last_broker_sequence=int(str(projection_data["last_broker_sequence"])),
            last_update_hash=str(projection_data["last_update_hash"]),
            last_event_hash=str(projection_data["last_event_hash"]),
            updated_at=_datetime(projection_data["updated_at"]),
            version=int(str(projection_data["version"])),
        )
        if (
            order.order_hash != str(row["order_hash"])
            or projection.projection_hash != str(row["projection_hash"])
            or order.account_id != str(row["account_id"])
            or order.client_order_id != str(row["client_order_id"])
            or order.risk_decision_hash != str(row["risk_decision_hash"])
            or order.instrument != str(row["instrument"])
            or order.side.value != str(row["side"])
            or order.quantity != int(row["quantity"])
            or order.approved_at != _datetime(row["approved_at"])
            or projection.state.value != str(row["state"])
            or projection.broker_order_id != row["broker_order_id"]
            or projection.version != int(row["version"])
            or projection.updated_at != _datetime(row["updated_at"])
        ):
            raise ValueError("paper order columns do not match payload")
        return projection
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "Stored paper order failed integrity verification"
        ) from None


def _update_from_payload(raw: object) -> BrokerOrderUpdate:
    try:
        payload = _object(raw)
        update = BrokerOrderUpdate(
            account_id=str(payload["account_id"]),
            client_order_id=str(payload["client_order_id"]),
            broker_order_id=str(payload["broker_order_id"]),
            broker_sequence=int(str(payload["broker_sequence"])),
            state=PaperOrderState(str(payload["state"])),
            cumulative_filled_quantity=int(
                str(payload["cumulative_filled_quantity"])
            ),
            average_fill_price=(
                None
                if payload["average_fill_price"] is None
                else Decimal(str(payload["average_fill_price"]))
            ),
            occurred_at=_datetime(payload["occurred_at"]),
            rejection_code=(
                None
                if payload["rejection_code"] is None
                else str(payload["rejection_code"])
            ),
        )
        if _canonical_hash(payload) != update.update_hash:
            raise ValueError("update payload hash mismatch")
        return update
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "Stored broker update failed integrity verification"
        ) from None


def _verified_update_from_row(row: RowMapping) -> BrokerOrderUpdate:
    update = _update_from_payload(row["update_payload"])
    if update.update_hash != str(row["update_hash"]):
        raise PersistenceUnavailableError(
            "Stored broker update hash does not match event"
        )
    return update


def _snapshot_from_row(row: RowMapping) -> ExecutionAccountSnapshot:
    try:
        payload = _object(row["payload"])
        positions_raw = payload["positions"]
        orders_raw = payload["open_client_order_ids"]
        if not isinstance(positions_raw, list) or not isinstance(orders_raw, list):
            raise TypeError("snapshot collections are invalid")
        positions: list[AccountPosition] = []
        for raw_position in positions_raw:
            if not isinstance(raw_position, dict):
                raise TypeError("snapshot position is invalid")
            positions.append(
                AccountPosition(
                    instrument=str(raw_position["instrument"]),
                    total_quantity=int(str(raw_position["total_quantity"])),
                    sellable_quantity=int(str(raw_position["sellable_quantity"])),
                    market_value=Decimal(str(raw_position["market_value"])),
                )
            )
        snapshot = ExecutionAccountSnapshot(
            account_id=str(payload["account_id"]),
            as_of=_datetime(payload["as_of"]),
            cash=Decimal(str(payload["cash"])),
            equity=Decimal(str(payload["equity"])),
            positions=tuple(positions),
            open_client_order_ids=tuple(str(value) for value in orders_raw),
            projection_version=str(
                payload.get("projection_version", "unspecified-v1")
            ),
            evidence_hash=str(payload.get("evidence_hash", "0" * 64)),
        )
        if (
            snapshot.snapshot_hash != str(row["snapshot_hash"])
            or snapshot.account_id != str(row["account_id"])
            or snapshot.as_of != _datetime(row["as_of"])
        ):
            raise ValueError("snapshot columns do not match payload")
        return snapshot
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "Stored execution snapshot failed integrity verification"
        ) from None


def _object(raw: object) -> dict[str, object]:
    value = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(value, dict):
        raise TypeError("stored payload must be an object")
    return value


def _datetime(raw: object) -> datetime:
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, str):
        return datetime.fromisoformat(raw)
    raise TypeError("stored timestamp is invalid")


def _json(value: object) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)
