from __future__ import annotations

import json
import re

from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.clock import SHANGHAI
from autoquant.data.models import _require_nonblank
from autoquant.errors import (
    BrokerStateUnknownError,
    PersistenceUnavailableError,
    QmtSessionLeaseLostError,
)
from autoquant.execution.models import ZERO_HASH
from autoquant.execution.qmt_callback_reconciliation import (
    QmtCallbackReconciliationIssue,
    QmtCallbackReconciliationReport,
    QmtCallbackReconciliationState,
)
from autoquant.execution.qmt_callback_reducer import QmtOrderConvergence
from autoquant.execution.qmt_callback_reducer_store import (
    qmt_broker_order_projection_from_row,
    qmt_broker_trade_fact_from_row,
)
from autoquant.execution.qmt_readonly_store import (
    qmt_readonly_acceptance_from_row,
)
from autoquant.execution.qmt_session_store import qmt_session_token_hash

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class PostgresQmtCallbackReconciliationRepository:
    """Persist callback/query convergence evidence under the active lease."""

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
    ) -> PostgresQmtCallbackReconciliationRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "QMT callback reconciliation connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def check_connection(self) -> None:
        try:
            async with self._engine.connect() as connection:
                table = await connection.scalar(
                    text("SELECT to_regclass(:table_name)"),
                    {"table_name": (f"{self._schema}.qmt_callback_reconciliation_reports")},
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
            raise PersistenceUnavailableError(
                "QMT callback reconciliation schema check failed"
            ) from None
        if table is None or not isinstance(version, int) or version < 43:
            raise PersistenceUnavailableError(
                "QMT callback reconciliation schema v43 is unavailable"
            )

    async def append(
        self,
        report: QmtCallbackReconciliationReport,
        *,
        lease_token: SecretStr,
    ) -> QmtCallbackReconciliationReport:
        if not isinstance(report, QmtCallbackReconciliationReport):
            raise TypeError("report must be QmtCallbackReconciliationReport")
        token_hash = qmt_session_token_hash(lease_token)
        parameters = _report_parameters(report)
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:identity))"),
                    {
                        "identity": (
                            f"qmt-callback-reconcile:"
                            f"{report.logical_account_id}:"
                            f"{report.gateway_holder_id}:"
                            f"{report.qmt_session_id}:"
                            f"{report.qmt_lease_generation}"
                        )
                    },
                )
                lease = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *, clock_timestamp() AS database_now
                                FROM {self._schema}.qmt_session_leases
                                WHERE session_id = :qmt_session_id
                                FOR SHARE
                                """
                            ),
                            parameters,
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if not _lease_matches(
                    lease,
                    report=report,
                    token_hash=token_hash,
                ):
                    raise QmtSessionLeaseLostError(
                        "QMT callback reconciliation requires its active daily lease"
                    )
                acceptance = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.qmt_readonly_acceptance_evidence
                                WHERE evidence_hash =
                                    :acceptance_evidence_hash
                                FOR SHARE
                                """
                            ),
                            parameters,
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if not _acceptance_matches(
                    acceptance,
                    report=report,
                    token_hash=token_hash,
                ):
                    raise BrokerStateUnknownError(
                        "QMT reconciliation acceptance evidence conflicts"
                    )
                cursor = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.qmt_callback_processing_cursors
                                WHERE account_id = :logical_account_id
                                  AND gateway_holder_id =
                                      :gateway_holder_id
                                  AND qmt_session_id = :qmt_session_id
                                  AND qmt_lease_generation =
                                      :qmt_lease_generation
                                FOR SHARE
                                """
                            ),
                            parameters,
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if not _cursor_matches(cursor, report=report):
                    raise BrokerStateUnknownError("QMT reconciliation callback cursor conflicts")
                projection_rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.qmt_broker_order_projections
                                WHERE account_id = :logical_account_id
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
                    .all()
                )
                projections = tuple(
                    qmt_broker_order_projection_from_row(row) for row in projection_rows
                )
                projection_hashes = tuple(
                    sorted(projection.projection_hash for projection in projections)
                )
                trade_rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.qmt_broker_trade_facts
                                WHERE account_id = :logical_account_id
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
                    .all()
                )
                trade_facts = tuple(qmt_broker_trade_fact_from_row(row) for row in trade_rows)
                trade_fact_hashes = tuple(sorted(fact.fact_hash for fact in trade_facts))
                if (
                    projection_hashes != report.projection_hashes
                    or trade_fact_hashes != report.trade_fact_hashes
                ):
                    raise BrokerStateUnknownError(
                        "QMT reconciliation report uses stale broker facts"
                    )
                if report.state is QmtCallbackReconciliationState.PASSED and (
                    (
                        cursor is not None
                        and (
                            cursor["broker_state_known"] is not True
                            or cursor["fatal_reason"] is not None
                        )
                    )
                    or any(
                        item.convergence is not QmtOrderConvergence.CONVERGED
                        for item in projections
                    )
                    or report.matched_broker_order_ids
                    != tuple(sorted(item.broker_order_id for item in projections))
                    or report.matched_trade_ids
                    != tuple(sorted(item.trade_id for item in trade_facts))
                ):
                    raise BrokerStateUnknownError(
                        "passed QMT reconciliation conflicts with current state"
                    )
                conflict_rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.qmt_callback_reconciliation_reports
                                WHERE report_hash = :report_hash
                                   OR (
                                       acceptance_evidence_hash =
                                           :acceptance_evidence_hash
                                       AND callback_processing_hash =
                                           :callback_processing_hash
                                   )
                                """
                            ),
                            parameters,
                        )
                    )
                    .mappings()
                    .all()
                )
                if any(
                    qmt_callback_reconciliation_report_from_row(row) != report
                    for row in conflict_rows
                ):
                    raise BrokerStateUnknownError(
                        "QMT reconciliation identity already has another report"
                    )
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO
                            {self._schema}.qmt_callback_reconciliation_reports
                            (report_hash, logical_account_id,
                             gateway_holder_id, qmt_session_id,
                             qmt_lease_generation,
                             acceptance_evidence_hash,
                             baseline_evidence_hash,
                             callback_processing_hash, callback_cursor,
                             state, projection_hashes,
                             trade_fact_hashes,
                             matched_broker_order_ids,
                             matched_trade_ids, issues, observed_at,
                             broker_mutation_allowed, report_version,
                             report_payload)
                        VALUES
                            (:report_hash, :logical_account_id,
                             :gateway_holder_id, :qmt_session_id,
                             :qmt_lease_generation,
                             :acceptance_evidence_hash,
                             :baseline_evidence_hash,
                             :callback_processing_hash, :callback_cursor,
                             :state, CAST(:projection_hashes AS jsonb),
                             CAST(:trade_fact_hashes AS jsonb),
                             CAST(:matched_broker_order_ids AS jsonb),
                             CAST(:matched_trade_ids AS jsonb),
                             CAST(:issues AS jsonb), :observed_at, false,
                             :report_version,
                             CAST(:report_payload AS jsonb))
                        ON CONFLICT (report_hash) DO NOTHING
                        """
                    ),
                    parameters,
                )
                stored_row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.qmt_callback_reconciliation_reports
                                WHERE report_hash = :report_hash
                                """
                            ),
                            parameters,
                        )
                    )
                    .mappings()
                    .one()
                )
            stored = qmt_callback_reconciliation_report_from_row(stored_row)
            if stored != report:
                raise PersistenceUnavailableError(
                    "stored QMT callback reconciliation failed verification"
                )
            return stored
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
            raise PersistenceUnavailableError(
                "QMT callback reconciliation persistence failed"
            ) from None

    async def latest(
        self,
        *,
        logical_account_id: str,
    ) -> QmtCallbackReconciliationReport | None:
        _require_nonblank(
            logical_account_id,
            name="QMT reconciliation logical_account_id",
        )
        try:
            async with self._engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.qmt_callback_reconciliation_reports
                                WHERE logical_account_id =
                                    :logical_account_id
                                ORDER BY observed_at DESC,
                                         created_at DESC,
                                         report_hash DESC
                                LIMIT 1
                                """
                            ),
                            {"logical_account_id": logical_account_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError("QMT callback reconciliation read failed") from None
        return None if row is None else qmt_callback_reconciliation_report_from_row(row)


def _report_parameters(
    report: QmtCallbackReconciliationReport,
) -> dict[str, object]:
    return {
        "acceptance_evidence_hash": report.acceptance_evidence_hash,
        "baseline_evidence_hash": report.baseline_evidence_hash,
        "callback_cursor": report.callback_cursor,
        "callback_processing_hash": report.callback_processing_hash,
        "gateway_holder_id": report.gateway_holder_id,
        "issues": _json([item.value for item in report.issues]),
        "logical_account_id": report.logical_account_id,
        "matched_broker_order_ids": _json(list(report.matched_broker_order_ids)),
        "matched_trade_ids": _json(list(report.matched_trade_ids)),
        "observed_at": report.observed_at,
        "projection_hashes": _json(list(report.projection_hashes)),
        "qmt_lease_generation": report.qmt_lease_generation,
        "qmt_session_id": report.qmt_session_id,
        "report_hash": report.report_hash,
        "report_payload": _json(report.payload()),
        "report_version": report.version,
        "state": report.state.value,
        "trade_fact_hashes": _json(list(report.trade_fact_hashes)),
    }


def qmt_callback_reconciliation_report_from_row(
    row: RowMapping,
) -> QmtCallbackReconciliationReport:
    report = QmtCallbackReconciliationReport(
        logical_account_id=str(row["logical_account_id"]),
        gateway_holder_id=str(row["gateway_holder_id"]),
        qmt_session_id=int(row["qmt_session_id"]),
        qmt_lease_generation=int(row["qmt_lease_generation"]),
        acceptance_evidence_hash=str(row["acceptance_evidence_hash"]),
        baseline_evidence_hash=str(row["baseline_evidence_hash"]),
        callback_processing_hash=str(row["callback_processing_hash"]),
        callback_cursor=int(row["callback_cursor"]),
        projection_hashes=tuple(map(str, row["projection_hashes"])),
        trade_fact_hashes=tuple(map(str, row["trade_fact_hashes"])),
        matched_broker_order_ids=tuple(map(str, row["matched_broker_order_ids"])),
        matched_trade_ids=tuple(map(str, row["matched_trade_ids"])),
        issues=tuple(QmtCallbackReconciliationIssue(str(value)) for value in row["issues"]),
        observed_at=row["observed_at"],
        version=str(row["report_version"]),
    )
    if (
        str(row["report_hash"]) != report.report_hash
        or str(row["state"]) != report.state.value
        or row["broker_mutation_allowed"] is not False
        or dict(row["report_payload"]) != report.payload()
    ):
        raise PersistenceUnavailableError(
            "QMT callback reconciliation row failed integrity verification"
        )
    return report


def _lease_matches(
    row: RowMapping | None,
    *,
    report: QmtCallbackReconciliationReport,
    token_hash: str,
) -> bool:
    return bool(
        row is not None
        and str(row["holder_id"]) == report.gateway_holder_id
        and str(row["token_hash"]) == token_hash
        and int(row["generation"]) == report.qmt_lease_generation
        and row["released_at"] is None
        and row["expires_at"] > row["database_now"]
        and row["acquired_at"] <= report.observed_at
        and row["acquired_at"].astimezone(SHANGHAI).date()
        == row["database_now"].astimezone(SHANGHAI).date()
        == report.observed_at.astimezone(SHANGHAI).date()
    )


def _acceptance_matches(
    row: RowMapping | None,
    *,
    report: QmtCallbackReconciliationReport,
    token_hash: str,
) -> bool:
    if row is None:
        return False
    acceptance = qmt_readonly_acceptance_from_row(row)
    return bool(
        acceptance.logical_account_id == report.logical_account_id
        and acceptance.baseline_evidence_hash == report.baseline_evidence_hash
        and acceptance.callback_cursor == report.callback_cursor
        and acceptance.lease_session_id == report.qmt_session_id
        and acceptance.lease_holder_id == report.gateway_holder_id
        and acceptance.lease_generation == report.qmt_lease_generation
        and acceptance.lease_token_hash == token_hash
        and acceptance.observed_at == report.observed_at
        and acceptance.evidence_hash == report.acceptance_evidence_hash
    )


def _cursor_matches(
    row: RowMapping | None,
    *,
    report: QmtCallbackReconciliationReport,
) -> bool:
    if report.callback_cursor == 0:
        return row is None and report.callback_processing_hash == ZERO_HASH
    return bool(
        row is not None
        and int(row["last_local_sequence"]) == report.callback_cursor
        and str(row["last_processing_hash"]) == report.callback_processing_hash
    )


def _json(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


__all__ = [
    "PostgresQmtCallbackReconciliationRepository",
    "qmt_callback_reconciliation_report_from_row",
]
