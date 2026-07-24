from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal

from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from autoquant.backtest.models import OrderSide
from autoquant.clock import SHANGHAI
from autoquant.errors import (
    BrokerStateUnknownError,
    PersistenceUnavailableError,
    QmtSessionLeaseLostError,
)
from autoquant.execution.models import ZERO_HASH, PaperOrderState
from autoquant.execution.qmt_callback_inbox import QmtCallbackInboxEvent
from autoquant.execution.qmt_callback_reducer import (
    QmtBrokerOrderProjection,
    QmtBrokerTradeFact,
    QmtCallbackDisposition,
    QmtCallbackProcessingRecord,
    QmtOrderConvergence,
    apply_qmt_order_callback,
    apply_qmt_trade_fact,
    initial_qmt_order_projection,
    qmt_trade_fact_from_callback,
    unknown_qmt_order_projection,
)
from autoquant.execution.qmt_callback_store import (
    PostgresQmtCallbackInbox,
    qmt_callback_receipt_from_row,
)
from autoquant.execution.qmt_gateway import QmtCallbackKind
from autoquant.execution.qmt_session_store import qmt_session_token_hash

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


@dataclass(frozen=True, slots=True)
class QmtCallbackReductionResult:
    records: tuple[QmtCallbackProcessingRecord, ...]
    projections: tuple[QmtBrokerOrderProjection, ...]
    broker_state_known: bool
    fatal_reason: str | None

    @property
    def broker_mutation_allowed(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class _OrderIdentity:
    candidate_hash: str
    client_order_id: str
    broker_order_id: str
    instrument: str
    side: OrderSide
    quantity: int
    limit_price: Decimal
    order_remark: str


class PostgresQmtCallbackStateReducer:
    """Reduce durable QMT callbacks without ever mutating the broker."""

    def __init__(self, *, engine: AsyncEngine, schema: str = "public") -> None:
        if _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError("schema must be a safe PostgreSQL identifier")
        self._engine = engine
        self._schema = schema

    @classmethod
    def connect(
        cls,
        *,
        dsn: str,
        schema: str = "public",
    ) -> PostgresQmtCallbackStateReducer:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError("QMT callback reducer connection failed") from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def check_connection(self) -> None:
        required = {
            "qmt_broker_order_projections",
            "qmt_broker_trade_facts",
            "qmt_callback_processing_cursors",
            "qmt_callback_processing_events",
        }
        try:
            async with self._engine.connect() as connection:
                tables = set(
                    map(
                        str,
                        (
                            await connection.scalars(
                                text(
                                    """
                                    SELECT table_name
                                    FROM information_schema.tables
                                    WHERE table_schema = :schema
                                      AND table_name = ANY(:tables)
                                    """
                                ),
                                {
                                    "schema": self._schema,
                                    "tables": sorted(required),
                                },
                            )
                        ).all(),
                    )
                )
                version = await connection.scalar(
                    text(
                        f"""
                        SELECT version
                        FROM {self._schema}.schema_versions
                        WHERE component = 'postgres'
                        """
                    )
                )
        except Exception:
            raise PersistenceUnavailableError("QMT callback reducer schema check failed") from None
        if tables != required or not isinstance(version, int) or version < 42:
            raise PersistenceUnavailableError("QMT callback reducer schema v42 is unavailable")

    async def process_current(
        self,
        *,
        inbox: PostgresQmtCallbackInbox,
        account_id: str,
        gateway_holder_id: str,
        qmt_session_id: int,
        qmt_lease_generation: int,
        lease_token: SecretStr,
    ) -> QmtCallbackReductionResult:
        if not isinstance(inbox, PostgresQmtCallbackInbox):
            raise TypeError("inbox must be PostgresQmtCallbackInbox")
        events = await inbox.replay_current(
            account_id=account_id,
            gateway_holder_id=gateway_holder_id,
            qmt_session_id=qmt_session_id,
            qmt_lease_generation=qmt_lease_generation,
            lease_token=lease_token,
        )
        return await self.process(
            events,
            account_id=account_id,
            gateway_holder_id=gateway_holder_id,
            qmt_session_id=qmt_session_id,
            qmt_lease_generation=qmt_lease_generation,
            lease_token=lease_token,
        )

    async def process(
        self,
        events: tuple[QmtCallbackInboxEvent, ...],
        *,
        account_id: str,
        gateway_holder_id: str,
        qmt_session_id: int,
        qmt_lease_generation: int,
        lease_token: SecretStr,
    ) -> QmtCallbackReductionResult:
        token_hash = qmt_session_token_hash(lease_token)
        records: list[QmtCallbackProcessingRecord] = []
        try:
            for event in events:
                if (
                    event.callback.account_id != account_id
                    or event.gateway_holder_id != gateway_holder_id
                    or event.qmt_session_id != qmt_session_id
                    or event.qmt_lease_generation != qmt_lease_generation
                ):
                    raise BrokerStateUnknownError(
                        "QMT callback reducer received another lease scope"
                    )
                record = await self._process_one(
                    event,
                    token_hash=token_hash,
                )
                if record is not None:
                    records.append(record)
            return await self._result(
                records=tuple(records),
                account_id=account_id,
                gateway_holder_id=gateway_holder_id,
                qmt_session_id=qmt_session_id,
                qmt_lease_generation=qmt_lease_generation,
            )
        except (
            TypeError,
            ValueError,
            BrokerStateUnknownError,
            QmtSessionLeaseLostError,
        ):
            raise
        except PersistenceUnavailableError:
            raise
        except Exception:
            raise PersistenceUnavailableError("QMT callback state reduction failed") from None

    async def _process_one(
        self,
        event: QmtCallbackInboxEvent,
        *,
        token_hash: str,
    ) -> QmtCallbackProcessingRecord | None:
        callback = event.callback
        scope = _scope_parameters(event)
        async with self._engine.begin() as connection:
            await connection.execute(
                text("SELECT pg_advisory_xact_lock(hashtext(:identity))"),
                {
                    "identity": (
                        f"qmt-callback-reducer:{callback.account_id}:"
                        f"{event.gateway_holder_id}:{event.qmt_session_id}:"
                        f"{event.qmt_lease_generation}"
                    )
                },
            )
            lease_row = (
                (
                    await connection.execute(
                        text(
                            f"""
                            SELECT *, clock_timestamp() AS observed_at
                            FROM {self._schema}.qmt_session_leases
                            WHERE session_id = :qmt_session_id
                            FOR SHARE
                            """
                        ),
                        scope,
                    )
                )
                .mappings()
                .one_or_none()
            )
            if not _active_lease_matches(
                lease_row,
                event=event,
                token_hash=token_hash,
            ):
                raise QmtSessionLeaseLostError(
                    "QMT callback reducer requires its active daily bearer lease"
                )
            cursor = (
                (
                    await connection.execute(
                        text(
                            f"""
                            SELECT *
                            FROM {self._schema}.qmt_callback_processing_cursors
                            WHERE account_id = :account_id
                              AND gateway_holder_id = :gateway_holder_id
                              AND qmt_session_id = :qmt_session_id
                              AND qmt_lease_generation =
                                  :qmt_lease_generation
                            FOR UPDATE
                            """
                        ),
                        scope,
                    )
                )
                .mappings()
                .one_or_none()
            )
            last_sequence = 0 if cursor is None else int(cursor["last_local_sequence"])
            if callback.local_sequence <= last_sequence:
                stored = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.qmt_callback_processing_events
                                WHERE callback_event_hash =
                                    :callback_event_hash
                                """
                            ),
                            {"callback_event_hash": event.event_hash},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if stored is None:
                    raise PersistenceUnavailableError(
                        "QMT callback cursor has no immutable processing event"
                    )
                await _verify_stored_processing(
                    connection,
                    schema=self._schema,
                    row=stored,
                    event=event,
                )
                return None
            if callback.local_sequence != last_sequence + 1:
                raise BrokerStateUnknownError("QMT callback processing cursor has a sequence gap")
            expected_callback_previous = (
                ZERO_HASH if cursor is None else str(cursor["last_callback_event_hash"])
            )
            if event.previous_hash != expected_callback_previous:
                raise BrokerStateUnknownError(
                    "QMT callback processing cursor conflicts with inbox chain"
                )
            receipt_row = (
                (
                    await connection.execute(
                        text(
                            f"""
                            SELECT *
                            FROM {self._schema}.qmt_callback_persistence_receipts
                            WHERE event_hash = :event_hash
                            FOR SHARE
                            """
                        ),
                        {"event_hash": event.event_hash},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if receipt_row is None:
                raise PersistenceUnavailableError(
                    "QMT callback reducer requires its persistence receipt"
                )
            receipt = qmt_callback_receipt_from_row(receipt_row, event=event)
            (
                disposition,
                reason,
                projection,
                identity,
                fatal_reason,
            ) = await self._reduce(
                connection,
                event=event,
            )
            if projection is not None:
                await self._upsert_projection(
                    connection,
                    event=event,
                    projection=projection,
                )
            previous_hash = ZERO_HASH if cursor is None else str(cursor["last_processing_hash"])
            record = QmtCallbackProcessingRecord(
                event=event,
                receipt=receipt,
                disposition=disposition,
                reason=reason,
                previous_hash=previous_hash,
                candidate_hash=(None if identity is None else identity.candidate_hash),
                client_order_id=(None if identity is None else identity.client_order_id),
                broker_order_id=(None if identity is None else identity.broker_order_id),
                projection_hash=(None if projection is None else projection.projection_hash),
            )
            await connection.execute(
                text(
                    f"""
                    INSERT INTO {self._schema}.qmt_callback_processing_events
                        (processing_hash, callback_event_hash,
                         callback_receipt_hash, account_id,
                         gateway_holder_id, qmt_session_id,
                         qmt_lease_generation, local_sequence, kind,
                         disposition, reason, previous_hash,
                         candidate_hash, client_order_id,
                         broker_order_id, projection_hash,
                         broker_mutation_allowed, processing_version,
                         processing_payload)
                    VALUES
                        (:processing_hash, :callback_event_hash,
                         :callback_receipt_hash, :account_id,
                         :gateway_holder_id, :qmt_session_id,
                         :qmt_lease_generation, :local_sequence, :kind,
                         :disposition, :reason, :previous_hash,
                         :candidate_hash, :client_order_id,
                         :broker_order_id, :projection_hash, false,
                         :processing_version,
                         CAST(:processing_payload AS jsonb))
                    """
                ),
                {
                    **scope,
                    "broker_order_id": record.broker_order_id,
                    "callback_event_hash": event.event_hash,
                    "callback_receipt_hash": receipt.receipt_hash,
                    "candidate_hash": record.candidate_hash,
                    "client_order_id": record.client_order_id,
                    "disposition": record.disposition.value,
                    "kind": callback.kind.value,
                    "previous_hash": record.previous_hash,
                    "processing_hash": record.processing_hash,
                    "processing_payload": _json(record.payload()),
                    "processing_version": record.version,
                    "projection_hash": record.projection_hash,
                    "reason": record.reason,
                },
            )
            inherited_fatal = None if cursor is None else cursor["fatal_reason"]
            effective_fatal = str(inherited_fatal) if inherited_fatal is not None else fatal_reason
            pending_count = int(
                await connection.scalar(
                    text(
                        f"""
                        SELECT count(*)
                        FROM {self._schema}.qmt_broker_order_projections
                        WHERE account_id = :account_id
                          AND gateway_holder_id = :gateway_holder_id
                          AND qmt_session_id = :qmt_session_id
                          AND qmt_lease_generation =
                              :qmt_lease_generation
                          AND convergence <> 'converged'
                        """
                    ),
                    scope,
                )
                or 0
            )
            await connection.execute(
                text(
                    f"""
                    INSERT INTO {self._schema}.qmt_callback_processing_cursors
                        (account_id, gateway_holder_id, qmt_session_id,
                         qmt_lease_generation, last_local_sequence,
                         last_callback_event_hash, last_processing_hash,
                         fatal_reason, broker_state_known, updated_at,
                         broker_mutation_allowed, cursor_version)
                    VALUES
                        (:account_id, :gateway_holder_id, :qmt_session_id,
                         :qmt_lease_generation, :local_sequence,
                         :callback_event_hash, :processing_hash,
                         :fatal_reason, :broker_state_known,
                         clock_timestamp(), false,
                         'qmt-callback-processing-cursor-v1')
                    ON CONFLICT (
                        account_id, gateway_holder_id, qmt_session_id,
                        qmt_lease_generation
                    ) DO UPDATE SET
                        last_local_sequence =
                            EXCLUDED.last_local_sequence,
                        last_callback_event_hash =
                            EXCLUDED.last_callback_event_hash,
                        last_processing_hash =
                            EXCLUDED.last_processing_hash,
                        fatal_reason = EXCLUDED.fatal_reason,
                        broker_state_known =
                            EXCLUDED.broker_state_known,
                        updated_at = EXCLUDED.updated_at
                    """
                ),
                {
                    **scope,
                    "broker_state_known": (effective_fatal is None and pending_count == 0),
                    "callback_event_hash": event.event_hash,
                    "fatal_reason": effective_fatal,
                    "processing_hash": record.processing_hash,
                },
            )
            return record

    async def _reduce(
        self,
        connection: AsyncConnection,
        *,
        event: QmtCallbackInboxEvent,
    ) -> tuple[
        QmtCallbackDisposition,
        str,
        QmtBrokerOrderProjection | None,
        _OrderIdentity | None,
        str | None,
    ]:
        kind = event.callback.kind
        payload = event.callback.redacted_payload
        if kind is QmtCallbackKind.DISCONNECTED:
            return (
                QmtCallbackDisposition.BROKER_STATE_UNKNOWN,
                "broker_disconnected",
                None,
                None,
                "broker_disconnected",
            )
        if kind in {QmtCallbackKind.ORDER_ERROR, QmtCallbackKind.CANCEL_ERROR}:
            return (
                QmtCallbackDisposition.BROKER_STATE_UNKNOWN,
                f"{kind.value}_reported",
                None,
                None,
                f"{kind.value}_reported",
            )
        if kind is QmtCallbackKind.ACCOUNT_STATUS:
            if payload["status"] == 0:
                return (
                    QmtCallbackDisposition.OBSERVED,
                    "account_status_normal",
                    None,
                    None,
                    None,
                )
            return (
                QmtCallbackDisposition.BROKER_STATE_UNKNOWN,
                "account_status_abnormal",
                None,
                None,
                "account_status_abnormal",
            )
        if kind is QmtCallbackKind.ASYNC_ORDER_RESPONSE:
            identity = await self._order_identity(
                connection,
                event=event,
            )
            if identity is None or payload["order_remark"] != identity.order_remark:
                return (
                    QmtCallbackDisposition.BROKER_STATE_UNKNOWN,
                    "async_binding_missing",
                    None,
                    None,
                    "async_binding_missing",
                )
            return (
                QmtCallbackDisposition.ASYNC_BOUND,
                "async_binding_verified",
                None,
                identity,
                None,
            )
        if kind not in {QmtCallbackKind.ORDER, QmtCallbackKind.TRADE}:
            return (
                QmtCallbackDisposition.BROKER_STATE_UNKNOWN,
                "unsupported_callback_kind",
                None,
                None,
                "unsupported_callback_kind",
            )
        identity = await self._order_identity(connection, event=event)
        if identity is None:
            return (
                QmtCallbackDisposition.BROKER_STATE_UNKNOWN,
                "order_correlation_missing",
                None,
                None,
                "order_correlation_missing",
            )
        projection = await self._projection(
            connection,
            event=event,
            identity=identity,
        )
        if kind is QmtCallbackKind.ORDER:
            try:
                reduced = apply_qmt_order_callback(projection, event)
            except (TypeError, ValueError):
                reduced = unknown_qmt_order_projection(projection, event)
                return (
                    QmtCallbackDisposition.BROKER_STATE_UNKNOWN,
                    "order_fact_conflict",
                    reduced,
                    identity,
                    "order_fact_conflict",
                )
            if reduced.convergence is QmtOrderConvergence.UNKNOWN:
                return (
                    QmtCallbackDisposition.BROKER_STATE_UNKNOWN,
                    "order_fact_unknown",
                    reduced,
                    identity,
                    "order_fact_unknown",
                )
            if reduced.convergence is QmtOrderConvergence.PENDING:
                return (
                    QmtCallbackDisposition.PENDING_RECONCILIATION,
                    "order_trade_evidence_pending",
                    reduced,
                    identity,
                    None,
                )
            return (
                QmtCallbackDisposition.ORDER_APPLIED,
                "order_trade_evidence_converged",
                reduced,
                identity,
                None,
            )
        fact = qmt_trade_fact_from_callback(
            event,
            candidate_hash=identity.candidate_hash,
            client_order_id=identity.client_order_id,
        )
        existing_trade = await self._trade_row(
            connection,
            event=event,
            trade_id=fact.trade_id,
        )
        if existing_trade is not None:
            if not _same_trade_fact(existing_trade, fact):
                reduced = unknown_qmt_order_projection(projection, event)
                return (
                    QmtCallbackDisposition.BROKER_STATE_UNKNOWN,
                    "trade_id_conflict",
                    reduced,
                    identity,
                    "trade_id_conflict",
                )
            return (
                QmtCallbackDisposition.DUPLICATE_TRADE,
                "duplicate_trade_verified",
                projection,
                identity,
                None,
            )
        try:
            reduced = apply_qmt_trade_fact(projection, fact, event)
        except (TypeError, ValueError):
            reduced = unknown_qmt_order_projection(projection, event)
            return (
                QmtCallbackDisposition.BROKER_STATE_UNKNOWN,
                "trade_fact_conflict",
                reduced,
                identity,
                "trade_fact_conflict",
            )
        # The immutable trade fact is foreign-keyed to its order projection.
        # Persist the pre-trade identity first in this same transaction; the
        # caller replaces it with the reduced projection before commit.
        await self._upsert_projection(
            connection,
            event=event,
            projection=projection,
        )
        await self._insert_trade(
            connection,
            event=event,
            fact=fact,
        )
        if reduced.convergence is QmtOrderConvergence.UNKNOWN:
            return (
                QmtCallbackDisposition.BROKER_STATE_UNKNOWN,
                "trade_order_evidence_unknown",
                reduced,
                identity,
                "trade_order_evidence_unknown",
            )
        if reduced.convergence is QmtOrderConvergence.PENDING:
            return (
                QmtCallbackDisposition.PENDING_RECONCILIATION,
                "trade_order_evidence_pending",
                reduced,
                identity,
                None,
            )
        return (
            QmtCallbackDisposition.TRADE_APPLIED,
            "trade_order_evidence_converged",
            reduced,
            identity,
            None,
        )

    async def _order_identity(
        self,
        connection: AsyncConnection,
        *,
        event: QmtCallbackInboxEvent,
    ) -> _OrderIdentity | None:
        payload = event.callback.redacted_payload
        broker_order_id = str(payload["order_id"])
        row = (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT b.candidate_hash, b.broker_order_id,
                               r.client_order_id, r.async_request_id,
                               c.account_id, c.broker_order_remark,
                               c.payload AS candidate_payload
                        FROM
                            {self._schema}.qmt_order_correlation_bindings b
                        JOIN
                            {self._schema}.qmt_order_correlation_reservations r
                          ON r.candidate_hash = b.candidate_hash
                        JOIN
                            {self._schema}.qmt_canary_order_candidates c
                          ON c.candidate_hash = b.candidate_hash
                        WHERE c.account_id = :account_id
                          AND b.gateway_holder_id =
                              :gateway_holder_id
                          AND b.qmt_session_id = :qmt_session_id
                          AND b.qmt_lease_generation =
                              :qmt_lease_generation
                          AND b.broker_order_id = :broker_order_id
                        """
                    ),
                    {**_scope_parameters(event), "broker_order_id": broker_order_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        candidate_payload = dict(row["candidate_payload"])
        order_payload = candidate_payload.get("order")
        if (
            not isinstance(order_payload, dict)
            or candidate_payload.get("account_id") != event.callback.account_id
            or candidate_payload.get("qmt_session_id") != event.qmt_session_id
            or candidate_payload.get("qmt_lease_generation") != event.qmt_lease_generation
        ):
            raise PersistenceUnavailableError("QMT candidate payload failed identity verification")
        identity = _OrderIdentity(
            candidate_hash=str(row["candidate_hash"]),
            client_order_id=str(row["client_order_id"]),
            broker_order_id=broker_order_id,
            instrument=str(order_payload["instrument"]),
            side=OrderSide(str(order_payload["side"])),
            quantity=int(order_payload["quantity"]),
            limit_price=Decimal(str(order_payload["limit_price"])),
            order_remark=str(row["broker_order_remark"]),
        )
        if (
            str(row["broker_order_id"]) != identity.broker_order_id
            or str(order_payload["client_order_id"]) != identity.client_order_id
            or (
                event.callback.kind is QmtCallbackKind.ASYNC_ORDER_RESPONSE
                and row["async_request_id"] != event.callback.redacted_payload["seq"]
            )
        ):
            raise PersistenceUnavailableError("QMT durable binding failed identity verification")
        return identity

    async def _projection(
        self,
        connection: AsyncConnection,
        *,
        event: QmtCallbackInboxEvent,
        identity: _OrderIdentity,
    ) -> QmtBrokerOrderProjection:
        row = (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT *
                        FROM {self._schema}.qmt_broker_order_projections
                        WHERE account_id = :account_id
                          AND gateway_holder_id = :gateway_holder_id
                          AND qmt_session_id = :qmt_session_id
                          AND qmt_lease_generation =
                              :qmt_lease_generation
                          AND broker_order_id = :broker_order_id
                        FOR UPDATE
                        """
                    ),
                    {
                        **_scope_parameters(event),
                        "broker_order_id": identity.broker_order_id,
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return initial_qmt_order_projection(
                event=event,
                candidate_hash=identity.candidate_hash,
                client_order_id=identity.client_order_id,
                broker_order_id=identity.broker_order_id,
                instrument=identity.instrument,
                side=identity.side,
                quantity=identity.quantity,
                limit_price=identity.limit_price,
                order_remark=identity.order_remark,
            )
        projection = _projection_from_row(row)
        if (
            projection.candidate_hash != identity.candidate_hash
            or projection.client_order_id != identity.client_order_id
            or projection.instrument != identity.instrument
            or projection.side is not identity.side
            or projection.quantity != identity.quantity
            or projection.limit_price != identity.limit_price
            or projection.order_remark != identity.order_remark
        ):
            raise PersistenceUnavailableError(
                "QMT order projection conflicts with durable candidate"
            )
        return projection

    async def _upsert_projection(
        self,
        connection: AsyncConnection,
        *,
        event: QmtCallbackInboxEvent,
        projection: QmtBrokerOrderProjection,
    ) -> None:
        await connection.execute(
            text(
                f"""
                INSERT INTO {self._schema}.qmt_broker_order_projections
                    (projection_hash, account_id, gateway_holder_id,
                     qmt_session_id, qmt_lease_generation,
                     candidate_hash, client_order_id, broker_order_id,
                     instrument, side, quantity, limit_price,
                     order_remark, reported_traded_volume,
                     reported_average_price, raw_order_status,
                     order_state, trade_volume, trade_amount,
                     convergence, last_callback_sequence,
                     last_callback_event_hash, updated_at,
                     broker_mutation_allowed, projection_version,
                     projection_payload)
                VALUES
                    (:projection_hash, :account_id, :gateway_holder_id,
                     :qmt_session_id, :qmt_lease_generation,
                     :candidate_hash, :client_order_id, :broker_order_id,
                     :instrument, :side, :quantity, :limit_price,
                     :order_remark, :reported_traded_volume,
                     :reported_average_price, :raw_order_status,
                     :order_state, :trade_volume, :trade_amount,
                     :convergence, :last_callback_sequence,
                     :last_callback_event_hash, :updated_at, false,
                     :projection_version,
                     CAST(:projection_payload AS jsonb))
                ON CONFLICT (
                    account_id, gateway_holder_id, qmt_session_id,
                    qmt_lease_generation, broker_order_id
                ) DO UPDATE SET
                    projection_hash = EXCLUDED.projection_hash,
                    reported_traded_volume =
                        EXCLUDED.reported_traded_volume,
                    reported_average_price =
                        EXCLUDED.reported_average_price,
                    raw_order_status = EXCLUDED.raw_order_status,
                    order_state = EXCLUDED.order_state,
                    trade_volume = EXCLUDED.trade_volume,
                    trade_amount = EXCLUDED.trade_amount,
                    convergence = EXCLUDED.convergence,
                    last_callback_sequence =
                        EXCLUDED.last_callback_sequence,
                    last_callback_event_hash =
                        EXCLUDED.last_callback_event_hash,
                    updated_at = EXCLUDED.updated_at,
                    projection_payload = EXCLUDED.projection_payload
                """
            ),
            {
                **_scope_parameters(event),
                **_projection_parameters(projection),
            },
        )

    async def _trade_row(
        self,
        connection: AsyncConnection,
        *,
        event: QmtCallbackInboxEvent,
        trade_id: str,
    ) -> RowMapping | None:
        return (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT *
                        FROM {self._schema}.qmt_broker_trade_facts
                        WHERE account_id = :account_id
                          AND gateway_holder_id = :gateway_holder_id
                          AND qmt_session_id = :qmt_session_id
                          AND qmt_lease_generation =
                              :qmt_lease_generation
                          AND trade_id = :trade_id
                        """
                    ),
                    {**_scope_parameters(event), "trade_id": trade_id},
                )
            )
            .mappings()
            .one_or_none()
        )

    async def _insert_trade(
        self,
        connection: AsyncConnection,
        *,
        event: QmtCallbackInboxEvent,
        fact: QmtBrokerTradeFact,
    ) -> None:
        await connection.execute(
            text(
                f"""
                INSERT INTO {self._schema}.qmt_broker_trade_facts
                    (fact_hash, account_id, gateway_holder_id,
                     qmt_session_id, qmt_lease_generation,
                     candidate_hash, client_order_id, broker_order_id,
                     trade_id, instrument, side, volume, price, amount,
                     order_remark, callback_event_hash, observed_at,
                     broker_mutation_allowed, fact_version,
                     fact_payload)
                VALUES
                    (:fact_hash, :account_id, :gateway_holder_id,
                     :qmt_session_id, :qmt_lease_generation,
                     :candidate_hash, :client_order_id, :broker_order_id,
                     :trade_id, :instrument, :side, :volume, :price,
                     :amount, :order_remark, :callback_event_hash,
                     :observed_at, false, :fact_version,
                     CAST(:fact_payload AS jsonb))
                """
            ),
            {
                **_scope_parameters(event),
                "amount": fact.amount,
                "broker_order_id": fact.broker_order_id,
                "callback_event_hash": fact.callback_event_hash,
                "candidate_hash": fact.candidate_hash,
                "client_order_id": fact.client_order_id,
                "fact_hash": fact.fact_hash,
                "fact_payload": _json(fact.payload()),
                "fact_version": fact.version,
                "instrument": fact.instrument,
                "observed_at": fact.observed_at,
                "order_remark": fact.order_remark,
                "price": fact.price,
                "side": fact.side.value,
                "trade_id": fact.trade_id,
                "volume": fact.volume,
            },
        )

    async def _result(
        self,
        *,
        records: tuple[QmtCallbackProcessingRecord, ...],
        account_id: str,
        gateway_holder_id: str,
        qmt_session_id: int,
        qmt_lease_generation: int,
    ) -> QmtCallbackReductionResult:
        parameters = {
            "account_id": account_id,
            "gateway_holder_id": gateway_holder_id,
            "qmt_lease_generation": qmt_lease_generation,
            "qmt_session_id": qmt_session_id,
        }
        async with self._engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        text(
                            f"""
                            SELECT *
                            FROM {self._schema}.qmt_broker_order_projections
                            WHERE account_id = :account_id
                              AND gateway_holder_id =
                                  :gateway_holder_id
                              AND qmt_session_id = :qmt_session_id
                              AND qmt_lease_generation =
                                  :qmt_lease_generation
                            ORDER BY broker_order_id
                            """
                        ),
                        parameters,
                    )
                )
                .mappings()
                .all()
            )
            cursor = (
                (
                    await connection.execute(
                        text(
                            f"""
                            SELECT *
                            FROM {self._schema}.qmt_callback_processing_cursors
                            WHERE account_id = :account_id
                              AND gateway_holder_id =
                                  :gateway_holder_id
                              AND qmt_session_id = :qmt_session_id
                              AND qmt_lease_generation =
                                  :qmt_lease_generation
                            """
                        ),
                        parameters,
                    )
                )
                .mappings()
                .one_or_none()
            )
        projections = tuple(_projection_from_row(row) for row in rows)
        return QmtCallbackReductionResult(
            records=records,
            projections=projections,
            broker_state_known=(True if cursor is None else bool(cursor["broker_state_known"])),
            fatal_reason=(
                None
                if cursor is None or cursor["fatal_reason"] is None
                else str(cursor["fatal_reason"])
            ),
        )


def _scope_parameters(event: QmtCallbackInboxEvent) -> dict[str, object]:
    return {
        "account_id": event.callback.account_id,
        "gateway_holder_id": event.gateway_holder_id,
        "local_sequence": event.callback.local_sequence,
        "qmt_lease_generation": event.qmt_lease_generation,
        "qmt_session_id": event.qmt_session_id,
    }


def _projection_parameters(
    projection: QmtBrokerOrderProjection,
) -> dict[str, object]:
    return {
        "broker_order_id": projection.broker_order_id,
        "candidate_hash": projection.candidate_hash,
        "client_order_id": projection.client_order_id,
        "convergence": projection.convergence.value,
        "instrument": projection.instrument,
        "last_callback_event_hash": projection.last_callback_event_hash,
        "last_callback_sequence": projection.last_callback_sequence,
        "limit_price": projection.limit_price,
        "order_remark": projection.order_remark,
        "order_state": projection.order_state.value,
        "projection_hash": projection.projection_hash,
        "projection_payload": _json(projection.payload()),
        "projection_version": projection.version,
        "quantity": projection.quantity,
        "raw_order_status": projection.raw_order_status,
        "reported_average_price": projection.reported_average_price,
        "reported_traded_volume": projection.reported_traded_volume,
        "side": projection.side.value,
        "trade_amount": projection.trade_amount,
        "trade_volume": projection.trade_volume,
        "updated_at": projection.updated_at,
    }


def _projection_from_row(row: RowMapping) -> QmtBrokerOrderProjection:
    projection = QmtBrokerOrderProjection(
        account_id=str(row["account_id"]),
        candidate_hash=str(row["candidate_hash"]),
        client_order_id=str(row["client_order_id"]),
        broker_order_id=str(row["broker_order_id"]),
        instrument=str(row["instrument"]),
        side=OrderSide(str(row["side"])),
        quantity=int(row["quantity"]),
        limit_price=Decimal(str(row["limit_price"])),
        order_remark=str(row["order_remark"]),
        reported_traded_volume=(
            None if row["reported_traded_volume"] is None else int(row["reported_traded_volume"])
        ),
        reported_average_price=(
            None
            if row["reported_average_price"] is None
            else Decimal(str(row["reported_average_price"]))
        ),
        raw_order_status=(
            None if row["raw_order_status"] is None else int(row["raw_order_status"])
        ),
        order_state=PaperOrderState(str(row["order_state"])),
        trade_volume=int(row["trade_volume"]),
        trade_amount=Decimal(str(row["trade_amount"])),
        convergence=QmtOrderConvergence(str(row["convergence"])),
        last_callback_sequence=int(row["last_callback_sequence"]),
        last_callback_event_hash=str(row["last_callback_event_hash"]),
        updated_at=row["updated_at"],
        version=str(row["projection_version"]),
    )
    if (
        str(row["projection_hash"]) != projection.projection_hash
        or row["broker_mutation_allowed"] is not False
        or dict(row["projection_payload"]) != projection.payload()
    ):
        raise PersistenceUnavailableError(
            "QMT broker order projection failed integrity verification"
        )
    return projection


def _same_trade_fact(row: RowMapping, fact: QmtBrokerTradeFact) -> bool:
    stored = _trade_fact_from_row(row)
    return bool(
        stored.account_id == fact.account_id
        and stored.candidate_hash == fact.candidate_hash
        and stored.client_order_id == fact.client_order_id
        and stored.broker_order_id == fact.broker_order_id
        and stored.trade_id == fact.trade_id
        and stored.instrument == fact.instrument
        and stored.side is fact.side
        and stored.volume == fact.volume
        and stored.price == fact.price
        and stored.amount == fact.amount
        and stored.order_remark == fact.order_remark
    )


def _trade_fact_from_row(row: RowMapping) -> QmtBrokerTradeFact:
    fact = QmtBrokerTradeFact(
        account_id=str(row["account_id"]),
        candidate_hash=str(row["candidate_hash"]),
        client_order_id=str(row["client_order_id"]),
        broker_order_id=str(row["broker_order_id"]),
        trade_id=str(row["trade_id"]),
        instrument=str(row["instrument"]),
        side=OrderSide(str(row["side"])),
        volume=int(row["volume"]),
        price=Decimal(str(row["price"])),
        amount=Decimal(str(row["amount"])),
        order_remark=str(row["order_remark"]),
        callback_event_hash=str(row["callback_event_hash"]),
        observed_at=row["observed_at"],
        version=str(row["fact_version"]),
    )
    if (
        str(row["fact_hash"]) != fact.fact_hash
        or row["broker_mutation_allowed"] is not False
        or dict(row["fact_payload"]) != fact.payload()
    ):
        raise PersistenceUnavailableError("QMT broker trade fact failed integrity verification")
    return fact


async def _verify_stored_processing(
    connection: AsyncConnection,
    *,
    schema: str,
    row: RowMapping,
    event: QmtCallbackInboxEvent,
) -> None:
    receipt_row = (
        (
            await connection.execute(
                text(
                    f"""
                    SELECT *
                    FROM {schema}.qmt_callback_persistence_receipts
                    WHERE event_hash = :event_hash
                    """
                ),
                {"event_hash": event.event_hash},
            )
        )
        .mappings()
        .one()
    )
    receipt = qmt_callback_receipt_from_row(receipt_row, event=event)
    record = QmtCallbackProcessingRecord(
        event=event,
        receipt=receipt,
        disposition=QmtCallbackDisposition(str(row["disposition"])),
        reason=str(row["reason"]),
        previous_hash=str(row["previous_hash"]),
        candidate_hash=(None if row["candidate_hash"] is None else str(row["candidate_hash"])),
        client_order_id=(None if row["client_order_id"] is None else str(row["client_order_id"])),
        broker_order_id=(None if row["broker_order_id"] is None else str(row["broker_order_id"])),
        projection_hash=(None if row["projection_hash"] is None else str(row["projection_hash"])),
        version=str(row["processing_version"]),
    )
    if (
        str(row["processing_hash"]) != record.processing_hash
        or str(row["callback_event_hash"]) != event.event_hash
        or str(row["callback_receipt_hash"]) != receipt.receipt_hash
        or str(row["account_id"]) != event.callback.account_id
        or str(row["gateway_holder_id"]) != event.gateway_holder_id
        or int(row["qmt_session_id"]) != event.qmt_session_id
        or int(row["qmt_lease_generation"]) != event.qmt_lease_generation
        or int(row["local_sequence"]) != event.callback.local_sequence
        or str(row["kind"]) != event.callback.kind.value
        or row["broker_mutation_allowed"] is not False
        or dict(row["processing_payload"]) != record.payload()
    ):
        raise PersistenceUnavailableError(
            "QMT callback processing event failed integrity verification"
        )


def _active_lease_matches(
    row: RowMapping | None,
    *,
    event: QmtCallbackInboxEvent,
    token_hash: str,
) -> bool:
    return bool(
        row is not None
        and str(row["holder_id"]) == event.gateway_holder_id
        and str(row["token_hash"]) == token_hash
        and int(row["generation"]) == event.qmt_lease_generation
        and row["released_at"] is None
        and row["expires_at"] > row["observed_at"]
        and row["acquired_at"].astimezone(SHANGHAI).date()
        == row["observed_at"].astimezone(SHANGHAI).date()
        and event.callback.received_at >= row["acquired_at"]
        and event.callback.broker_session_date == row["observed_at"].astimezone(SHANGHAI).date()
    )


def _json(payload: dict[str, object]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


__all__ = [
    "PostgresQmtCallbackStateReducer",
    "QmtCallbackReductionResult",
]
