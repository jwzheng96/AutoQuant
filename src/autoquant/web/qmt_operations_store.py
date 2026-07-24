from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.models import ZERO_HASH
from autoquant.execution.qmt_callback_inbox import (
    QmtCallbackInboxEvent,
    replay_qmt_callback_inbox,
)
from autoquant.execution.qmt_callback_reconciliation import (
    QmtCallbackReconciliationReport,
)
from autoquant.execution.qmt_callback_reconciliation_store import (
    qmt_callback_reconciliation_report_from_row,
)
from autoquant.execution.qmt_callback_reducer import (
    QmtBrokerOrderProjection,
    QmtBrokerTradeFact,
    QmtCallbackDisposition,
    QmtCallbackProcessingRecord,
)
from autoquant.execution.qmt_callback_reducer_store import (
    qmt_broker_order_projection_from_row,
    qmt_broker_trade_fact_from_row,
)
from autoquant.execution.qmt_callback_store import (
    qmt_callback_event_from_row,
    qmt_callback_receipt_from_row,
)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


@dataclass(frozen=True, slots=True)
class QmtOperationsSnapshot:
    account_id: str
    gateway_holder_id: str | None
    qmt_session_id: int | None
    qmt_lease_generation: int | None
    lease_active: bool
    lease_expires_at: datetime | None
    last_local_sequence: int
    last_processing_hash: str
    broker_state_known: bool
    fatal_reason: str | None
    processing_event_count: int
    projections: tuple[QmtBrokerOrderProjection, ...]
    trade_facts: tuple[QmtBrokerTradeFact, ...]
    latest_reconciliation: QmtCallbackReconciliationReport | None
    reconciliation_current: bool
    integrity_verified: bool

    @property
    def live_trading_locked(self) -> bool:
        return True

    @property
    def broker_mutation_allowed(self) -> bool:
        return False


class PostgresQmtOperationsRepository:
    """Read and re-verify redacted QMT operational evidence for the console."""

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
    ) -> PostgresQmtOperationsRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError("QMT operations connection failed") from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def check_connection(self) -> None:
        required = {
            "qmt_broker_order_projections",
            "qmt_broker_trade_facts",
            "qmt_callback_inbox_events",
            "qmt_callback_persistence_receipts",
            "qmt_callback_processing_cursors",
            "qmt_callback_processing_events",
            "qmt_callback_reconciliation_reports",
            "qmt_session_leases",
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
            raise PersistenceUnavailableError("QMT operations schema check failed") from None
        if tables != required or not isinstance(version, int) or version < 43:
            raise PersistenceUnavailableError("QMT operations schema v43 is unavailable")

    async def snapshot(self, *, account_id: str) -> QmtOperationsSnapshot:
        if (
            not isinstance(account_id, str)
            or not account_id
            or account_id != account_id.strip()
            or len(account_id) > 128
        ):
            raise ValueError("QMT operations account_id is invalid")
        try:
            async with self._engine.connect() as connection:
                cursor = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.qmt_callback_processing_cursors
                                WHERE account_id = :account_id
                                ORDER BY updated_at DESC,
                                         qmt_lease_generation DESC
                                LIMIT 1
                                """
                            ),
                            {"account_id": account_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                report_row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.qmt_callback_reconciliation_reports
                                WHERE logical_account_id = :account_id
                                ORDER BY observed_at DESC,
                                         created_at DESC,
                                         report_hash DESC
                                LIMIT 1
                                """
                            ),
                            {"account_id": account_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                report = (
                    None
                    if report_row is None
                    else qmt_callback_reconciliation_report_from_row(report_row)
                )
                if cursor is None:
                    return QmtOperationsSnapshot(
                        account_id=account_id,
                        gateway_holder_id=None,
                        qmt_session_id=None,
                        qmt_lease_generation=None,
                        lease_active=False,
                        lease_expires_at=None,
                        last_local_sequence=0,
                        last_processing_hash=ZERO_HASH,
                        broker_state_known=False,
                        fatal_reason=None,
                        processing_event_count=0,
                        projections=(),
                        trade_facts=(),
                        latest_reconciliation=report,
                        reconciliation_current=False,
                        integrity_verified=True,
                    )
                gateway_holder_id = str(cursor["gateway_holder_id"])
                qmt_session_id = int(cursor["qmt_session_id"])
                qmt_lease_generation = int(cursor["qmt_lease_generation"])
                scope = {
                    "account_id": account_id,
                    "gateway_holder_id": gateway_holder_id,
                    "qmt_session_id": qmt_session_id,
                    "qmt_lease_generation": qmt_lease_generation,
                }
                lease = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *, clock_timestamp() AS database_now
                                FROM {self._schema}.qmt_session_leases
                                WHERE session_id = :qmt_session_id
                                """
                            ),
                            scope,
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                evidence_rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT e.*, r.receipt_hash,
                                       r.persisted_at,
                                       r.receipt_version,
                                       r.receipt_payload
                                FROM
                                    {self._schema}.qmt_callback_inbox_events e
                                JOIN
                                    {self._schema}.qmt_callback_persistence_receipts r
                                  ON r.event_hash = e.event_hash
                                WHERE e.account_id = :account_id
                                  AND e.gateway_holder_id =
                                      :gateway_holder_id
                                  AND e.qmt_session_id = :qmt_session_id
                                  AND e.qmt_lease_generation =
                                      :qmt_lease_generation
                                ORDER BY e.local_sequence
                                """
                            ),
                            scope,
                        )
                    )
                    .mappings()
                    .all()
                )
                processing_rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.qmt_callback_processing_events
                                WHERE account_id = :account_id
                                  AND gateway_holder_id =
                                      :gateway_holder_id
                                  AND qmt_session_id = :qmt_session_id
                                  AND qmt_lease_generation =
                                      :qmt_lease_generation
                                ORDER BY local_sequence
                                """
                            ),
                            scope,
                        )
                    )
                    .mappings()
                    .all()
                )
                projection_rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.qmt_broker_order_projections
                                WHERE account_id = :account_id
                                  AND gateway_holder_id =
                                      :gateway_holder_id
                                  AND qmt_session_id = :qmt_session_id
                                  AND qmt_lease_generation =
                                      :qmt_lease_generation
                                ORDER BY broker_order_id
                                """
                            ),
                            scope,
                        )
                    )
                    .mappings()
                    .all()
                )
                trade_rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.qmt_broker_trade_facts
                                WHERE account_id = :account_id
                                  AND gateway_holder_id =
                                      :gateway_holder_id
                                  AND qmt_session_id = :qmt_session_id
                                  AND qmt_lease_generation =
                                      :qmt_lease_generation
                                ORDER BY trade_id
                                """
                            ),
                            scope,
                        )
                    )
                    .mappings()
                    .all()
                )
            events = tuple(qmt_callback_event_from_row(row) for row in evidence_rows)
            replay_qmt_callback_inbox(events)
            records = _processing_records(
                events=events,
                evidence_rows=evidence_rows,
                processing_rows=processing_rows,
            )
            last_sequence = int(cursor["last_local_sequence"])
            last_processing_hash = str(cursor["last_processing_hash"])
            if (
                len(records) != last_sequence
                or (last_sequence > 0 and records[-1].processing_hash != last_processing_hash)
                or (last_sequence == 0 and last_processing_hash != ZERO_HASH)
            ):
                raise PersistenceUnavailableError("QMT operations cursor failed chain verification")
            projections = tuple(
                qmt_broker_order_projection_from_row(row) for row in projection_rows
            )
            trade_facts = tuple(qmt_broker_trade_fact_from_row(row) for row in trade_rows)
            lease_active = bool(
                lease is not None
                and str(lease["holder_id"]) == scope["gateway_holder_id"]
                and int(lease["generation"]) == scope["qmt_lease_generation"]
                and lease["released_at"] is None
                and lease["expires_at"] > lease["database_now"]
            )
            reconciliation_current = bool(
                report is not None
                and report.logical_account_id == account_id
                and report.gateway_holder_id == scope["gateway_holder_id"]
                and report.qmt_session_id == scope["qmt_session_id"]
                and report.qmt_lease_generation == scope["qmt_lease_generation"]
                and report.callback_cursor == last_sequence
                and report.callback_processing_hash == last_processing_hash
                and report.projection_hashes
                == tuple(sorted(item.projection_hash for item in projections))
                and report.trade_fact_hashes
                == tuple(sorted(item.fact_hash for item in trade_facts))
            )
            return QmtOperationsSnapshot(
                account_id=account_id,
                gateway_holder_id=gateway_holder_id,
                qmt_session_id=qmt_session_id,
                qmt_lease_generation=qmt_lease_generation,
                lease_active=lease_active,
                lease_expires_at=(None if lease is None else lease["expires_at"]),
                last_local_sequence=last_sequence,
                last_processing_hash=last_processing_hash,
                broker_state_known=bool(cursor["broker_state_known"]),
                fatal_reason=(
                    None if cursor["fatal_reason"] is None else str(cursor["fatal_reason"])
                ),
                processing_event_count=len(records),
                projections=projections,
                trade_facts=trade_facts,
                latest_reconciliation=report,
                reconciliation_current=reconciliation_current,
                integrity_verified=True,
            )
        except (TypeError, ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError("QMT operations evidence read failed") from None


def _processing_records(
    *,
    events: tuple[QmtCallbackInboxEvent, ...],
    evidence_rows: Sequence[RowMapping],
    processing_rows: Sequence[RowMapping],
) -> tuple[QmtCallbackProcessingRecord, ...]:
    if len(events) != len(evidence_rows) or len(events) != len(processing_rows):
        raise PersistenceUnavailableError("QMT operations evidence chain is incomplete")
    records: list[QmtCallbackProcessingRecord] = []
    previous_hash = ZERO_HASH
    for event, evidence_row, processing_row in zip(
        events,
        evidence_rows,
        processing_rows,
        strict=True,
    ):
        receipt = qmt_callback_receipt_from_row(
            evidence_row,
            event=event,
        )
        row = processing_row
        record = QmtCallbackProcessingRecord(
            event=event,
            receipt=receipt,
            disposition=QmtCallbackDisposition(str(row["disposition"])),
            reason=str(row["reason"]),
            previous_hash=str(row["previous_hash"]),
            candidate_hash=(None if row["candidate_hash"] is None else str(row["candidate_hash"])),
            client_order_id=(
                None if row["client_order_id"] is None else str(row["client_order_id"])
            ),
            broker_order_id=(
                None if row["broker_order_id"] is None else str(row["broker_order_id"])
            ),
            projection_hash=(
                None if row["projection_hash"] is None else str(row["projection_hash"])
            ),
            version=str(row["processing_version"]),
        )
        if (
            record.previous_hash != previous_hash
            or str(row["processing_hash"]) != record.processing_hash
            or str(row["account_id"]) != event.callback.account_id
            or str(row["gateway_holder_id"]) != event.gateway_holder_id
            or int(row["qmt_session_id"]) != event.qmt_session_id
            or int(row["qmt_lease_generation"]) != event.qmt_lease_generation
            or int(row["local_sequence"]) != event.callback.local_sequence
            or str(row["kind"]) != event.callback.kind.value
            or str(row["callback_event_hash"]) != event.event_hash
            or str(row["callback_receipt_hash"]) != receipt.receipt_hash
            or row["broker_mutation_allowed"] is not False
            or dict(row["processing_payload"]) != record.payload()
        ):
            raise PersistenceUnavailableError("QMT processing chain failed integrity verification")
        records.append(record)
        previous_hash = record.processing_hash
    return tuple(records)


__all__ = [
    "PostgresQmtOperationsRepository",
    "QmtOperationsSnapshot",
]
