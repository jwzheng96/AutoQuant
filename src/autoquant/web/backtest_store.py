from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.backtest.models import (
    AccountSnapshot,
    BacktestResult,
    ExecutionReport,
    ExecutionState,
    FeeBreakdown,
    LedgerEvent,
    OrderSide,
    PositionSnapshot,
    RejectionCode,
    backtest_artifact_hash,
)
from autoquant.errors import PersistenceUnavailableError
from autoquant.web.models import (
    BacktestEventView,
    BacktestExecutionView,
    BacktestMetrics,
    BacktestRun,
    BacktestRunDetail,
    BacktestRunRequest,
    BacktestSnapshotView,
    OperatorJobState,
    ResearchManifest,
)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_RUN_COLUMNS = """
run_id, state, strategy_id, request_payload, requested_by, created_at,
started_at, completed_at, as_of, result_hash, ledger_hash, metrics_payload,
error_code
"""
_QUALIFIED_RUN_COLUMNS = """
runs.run_id, runs.state, runs.strategy_id, runs.request_payload,
runs.requested_by, runs.created_at, runs.started_at, runs.completed_at,
runs.as_of, runs.result_hash, runs.ledger_hash, runs.metrics_payload,
runs.error_code
"""


class PostgresBacktestRepository:
    """Persistent backtest queue and atomic, append-only result store."""

    def __init__(self, *, engine: AsyncEngine, schema: str = "public") -> None:
        if _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError("schema must be a safe PostgreSQL identifier")
        self._engine = engine
        self._schema = schema

    @classmethod
    def connect(cls, *, dsn: str, schema: str = "public") -> PostgresBacktestRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError("PostgreSQL backtest connection failed") from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def create_run(
        self,
        request: BacktestRunRequest,
        *,
        requested_by: str,
        now: datetime,
    ) -> BacktestRun:
        run_id = uuid4()
        parameters: dict[str, object] = {
            "run_id": run_id,
            "idempotency_key": request.idempotency_key,
            "state": OperatorJobState.QUEUED.value,
            "strategy_id": "manifest_buy_hold_v1",
            "manifest_hash": request.manifest_hash,
            "request_payload": _json(request.model_dump(mode="json")),
            "requested_by": requested_by,
            "created_at": _aware_utc(now),
        }
        sql = text(
            f"""
            INSERT INTO {self._schema}.backtest_runs
                (run_id, idempotency_key, state, strategy_id, manifest_hash,
                 request_payload, requested_by, created_at)
            VALUES
                (:run_id, :idempotency_key, :state, :strategy_id, :manifest_hash,
                 CAST(:request_payload AS jsonb), :requested_by, :created_at)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING {_RUN_COLUMNS}
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
                                    f"SELECT {_RUN_COLUMNS} FROM {self._schema}.backtest_runs "
                                    "WHERE idempotency_key = :idempotency_key"
                                ),
                                {"idempotency_key": request.idempotency_key},
                            )
                        )
                        .mappings()
                        .one()
                    )
        except Exception:
            raise PersistenceUnavailableError("Backtest run creation failed") from None
        run = self._run_from_row(row)
        if run.request != request or run.requested_by != requested_by:
            raise ValueError("idempotency key already belongs to another request")
        return run

    async def list_runs(self, *, limit: int = 50) -> tuple[BacktestRun, ...]:
        if limit < 1 or limit > 200:
            raise ValueError("limit must be between 1 and 200")
        try:
            async with self._engine.connect() as connection:
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"SELECT {_RUN_COLUMNS} FROM {self._schema}.backtest_runs "
                                "ORDER BY created_at DESC, run_id DESC LIMIT :limit"
                            ),
                            {"limit": limit},
                        )
                    )
                    .mappings()
                    .all()
                )
        except Exception:
            raise PersistenceUnavailableError("Backtest run listing failed") from None
        return tuple(self._run_from_row(row) for row in rows)

    async def list_manifests(self, *, limit: int = 100) -> tuple[ResearchManifest, ...]:
        if limit < 1 or limit > 200:
            raise ValueError("limit must be between 1 and 200")
        try:
            async with self._engine.connect() as connection:
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT manifest_hash, start_time, end_time, as_of,
                                       row_count, payload
                                FROM {self._schema}.dataset_manifests
                                WHERE production_complete = true
                                ORDER BY created_at DESC, manifest_hash DESC
                                LIMIT :limit
                                """
                            ),
                            {"limit": limit},
                        )
                    )
                    .mappings()
                    .all()
                )
        except Exception:
            raise PersistenceUnavailableError("Research manifest listing failed") from None
        manifests: list[ResearchManifest] = []
        try:
            for row in rows:
                payload = _object(row["payload"])
                raw_instruments = payload["instruments"]
                if not isinstance(raw_instruments, list):
                    raise TypeError("instruments must be a list")
                manifests.append(
                    ResearchManifest(
                        manifest_hash=str(row["manifest_hash"]),
                        instruments=tuple(str(item) for item in raw_instruments),
                        start_time=row["start_time"],
                        end_time=row["end_time"],
                        as_of=row["as_of"],
                        row_count=int(row["row_count"]),
                    )
                )
        except (KeyError, TypeError, ValueError):
            raise PersistenceUnavailableError("Stored research manifest is malformed") from None
        return tuple(manifests)

    async def claim_next_run(self, *, now: datetime) -> BacktestRun | None:
        sql = text(
            f"""
            WITH next_run AS (
                SELECT run_id FROM {self._schema}.backtest_runs
                WHERE state = 'queued'
                ORDER BY created_at, run_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE {self._schema}.backtest_runs AS runs
            SET state = 'running', started_at = :started_at
            FROM next_run
            WHERE runs.run_id = next_run.run_id
            RETURNING {_QUALIFIED_RUN_COLUMNS}
            """
        )
        try:
            async with self._engine.begin() as connection:
                row = (
                    (await connection.execute(sql, {"started_at": _aware_utc(now)}))
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError("Backtest run claim failed") from None
        return None if row is None else self._run_from_row(row)

    async def complete_run(
        self,
        run_id: UUID,
        *,
        result: BacktestResult,
        now: datetime,
    ) -> BacktestRun:
        metrics = _metrics(result)
        executions = [_execution_parameters(run_id, item) for item in result.reports]
        snapshots = [_snapshot_parameters(run_id, item) for item in result.snapshots]
        events = [_event_parameters(run_id, item) for item in result.events]
        try:
            async with self._engine.begin() as connection:
                if executions:
                    await connection.execute(
                        text(
                            f"""
                            INSERT INTO {self._schema}.backtest_executions
                                (run_id, client_order_id, instrument, side,
                                 requested_quantity, state, session_date,
                                 filled_quantity, fill_price, gross_amount,
                                 commission, stamp_duty, transfer_fee,
                                 rejection_code, ledger_hash)
                            VALUES
                                (:run_id, :client_order_id, :instrument, :side,
                                 :requested_quantity, :state, :session_date,
                                 :filled_quantity, :fill_price, :gross_amount,
                                 :commission, :stamp_duty, :transfer_fee,
                                 :rejection_code, :ledger_hash)
                            """
                        ),
                        executions,
                    )
                if snapshots:
                    await connection.execute(
                        text(
                            f"""
                            INSERT INTO {self._schema}.backtest_snapshots
                                (run_id, session_date, cash, market_value, equity,
                                 positions_payload, ledger_hash)
                            VALUES
                                (:run_id, :session_date, :cash, :market_value,
                                 :equity, CAST(:positions_payload AS jsonb), :ledger_hash)
                            """
                        ),
                        snapshots,
                    )
                if events:
                    await connection.execute(
                        text(
                            f"""
                            INSERT INTO {self._schema}.backtest_events
                                (run_id, sequence, event_type, session_date,
                                 client_order_id, payload, previous_hash, event_hash)
                            VALUES
                                (:run_id, :sequence, :event_type, :session_date,
                                 :client_order_id, CAST(:payload AS jsonb),
                                 :previous_hash, :event_hash)
                            """
                        ),
                        events,
                    )
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                UPDATE {self._schema}.backtest_runs
                                SET state = 'completed', completed_at = :completed_at,
                                    as_of = :as_of, result_hash = :result_hash,
                                    ledger_hash = :ledger_hash,
                                    metrics_payload = CAST(:metrics_payload AS jsonb),
                                    error_code = NULL
                                WHERE run_id = :run_id AND state = 'running'
                                  AND manifest_hash = :manifest_hash
                                  AND strategy_id = :strategy_id
                                RETURNING {_RUN_COLUMNS}
                                """
                            ),
                            {
                                "run_id": run_id,
                                "completed_at": _aware_utc(now),
                                "as_of": result.as_of,
                                "result_hash": result.result_hash,
                                "ledger_hash": result.ledger_hash,
                                "metrics_payload": _json(metrics.model_dump(mode="json")),
                                "manifest_hash": result.manifest_hash,
                                "strategy_id": result.strategy_id,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise RuntimeError("backtest run is not claimable")
        except Exception:
            raise PersistenceUnavailableError("Backtest result persistence failed") from None
        return self._run_from_row(row)

    async def fail_run(
        self,
        run_id: UUID,
        *,
        error_code: str,
        now: datetime,
        queued: bool = False,
    ) -> BacktestRun:
        if not error_code or len(error_code) > 80:
            raise ValueError("error_code must be 1-80 characters")
        expected = "queued" if queued else "running"
        try:
            async with self._engine.begin() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                UPDATE {self._schema}.backtest_runs
                                SET state = 'failed', completed_at = :completed_at,
                                    error_code = :error_code
                                WHERE run_id = :run_id AND state = :expected
                                RETURNING {_RUN_COLUMNS}
                                """
                            ),
                            {
                                "run_id": run_id,
                                "completed_at": _aware_utc(now),
                                "error_code": error_code,
                                "expected": expected,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError("Backtest run failure persistence failed") from None
        if row is None:
            raise PersistenceUnavailableError("Backtest run is not in the expected state")
        return self._run_from_row(row)

    async def interrupt_running_runs(self, *, now: datetime) -> int:
        try:
            async with self._engine.begin() as connection:
                result = await connection.execute(
                    text(
                        f"""
                        UPDATE {self._schema}.backtest_runs
                        SET state = 'interrupted', completed_at = :completed_at,
                            error_code = 'worker_restarted'
                        WHERE state = 'running'
                        """
                    ),
                    {"completed_at": _aware_utc(now)},
                )
        except Exception:
            raise PersistenceUnavailableError("Backtest run recovery failed") from None
        return int(result.rowcount or 0)

    async def detail(self, run_id: UUID) -> BacktestRunDetail:
        try:
            async with self._engine.connect() as connection:
                run_row = (
                    (
                        await connection.execute(
                            text(
                                f"SELECT {_RUN_COLUMNS} FROM {self._schema}.backtest_runs "
                                "WHERE run_id = :run_id"
                            ),
                            {"run_id": run_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if run_row is None:
                    raise LookupError("backtest run not found")
                execution_rows = (
                    (
                        await connection.execute(
                            text(
                                f"SELECT * FROM {self._schema}.backtest_executions "
                                "WHERE run_id = :run_id ORDER BY session_date, client_order_id"
                            ),
                            {"run_id": run_id},
                        )
                    )
                    .mappings()
                    .all()
                )
                snapshot_rows = (
                    (
                        await connection.execute(
                            text(
                                f"SELECT * FROM {self._schema}.backtest_snapshots "
                                "WHERE run_id = :run_id ORDER BY session_date"
                            ),
                            {"run_id": run_id},
                        )
                    )
                    .mappings()
                    .all()
                )
                event_rows = (
                    (
                        await connection.execute(
                            text(
                                f"SELECT * FROM {self._schema}.backtest_events "
                                "WHERE run_id = :run_id ORDER BY sequence"
                            ),
                            {"run_id": run_id},
                        )
                    )
                    .mappings()
                    .all()
                )
        except LookupError:
            raise
        except Exception:
            raise PersistenceUnavailableError("Backtest detail query failed") from None
        detail = BacktestRunDetail(
            run=self._run_from_row(run_row),
            executions=tuple(_execution_from_row(row) for row in execution_rows),
            snapshots=tuple(_snapshot_from_row(row) for row in snapshot_rows),
            events=tuple(_event_from_row(row) for row in event_rows),
        )
        try:
            _verify_completed_detail(detail)
        except (KeyError, TypeError, ValueError):
            raise PersistenceUnavailableError(
                "Stored backtest result failed integrity verification"
            ) from None
        return detail

    @staticmethod
    def _run_from_row(row: RowMapping) -> BacktestRun:
        try:
            raw_metrics = row["metrics_payload"]
            return BacktestRun(
                run_id=row["run_id"],
                state=OperatorJobState(str(row["state"])),
                strategy_id=str(row["strategy_id"]),
                request=BacktestRunRequest.model_validate(_object(row["request_payload"])),
                requested_by=str(row["requested_by"]),
                created_at=row["created_at"],
                started_at=row["started_at"],
                completed_at=row["completed_at"],
                as_of=row["as_of"],
                result_hash=None if row["result_hash"] is None else str(row["result_hash"]),
                ledger_hash=None if row["ledger_hash"] is None else str(row["ledger_hash"]),
                metrics=(
                    None
                    if raw_metrics is None
                    else BacktestMetrics.model_validate(_object(raw_metrics))
                ),
                error_code=None if row["error_code"] is None else str(row["error_code"]),
            )
        except (KeyError, TypeError, ValueError):
            raise PersistenceUnavailableError("Stored backtest run is malformed") from None


def _metrics(result: BacktestResult) -> BacktestMetrics:
    return BacktestMetrics(
        initial_cash=result.initial_cash,
        ending_equity=result.ending_equity,
        total_return=result.total_return,
        max_drawdown=result.max_drawdown,
        turnover=result.turnover,
        total_fees=result.total_fees,
        rule_versions=result.rule_versions,
        fee_version=result.fee_version,
        execution_version=result.execution_version,
        artifact_hash=backtest_artifact_hash(result),
    )


def _execution_parameters(run_id: UUID, report: Any) -> dict[str, object]:
    return {
        "run_id": run_id,
        "client_order_id": report.client_order_id,
        "instrument": report.instrument,
        "side": report.side.value,
        "requested_quantity": report.requested_quantity,
        "state": report.state.value,
        "session_date": report.session_date,
        "filled_quantity": report.filled_quantity,
        "fill_price": report.fill_price,
        "gross_amount": report.gross_amount,
        "commission": report.fees.commission,
        "stamp_duty": report.fees.stamp_duty,
        "transfer_fee": report.fees.transfer_fee,
        "rejection_code": (
            None if report.rejection_code is None else report.rejection_code.value
        ),
        "ledger_hash": report.ledger_hash,
    }


def _snapshot_parameters(run_id: UUID, snapshot: Any) -> dict[str, object]:
    positions = [
        {
            "instrument": item.instrument,
            "total_quantity": item.total_quantity,
            "sellable_quantity": item.sellable_quantity,
            "average_cost": str(item.average_cost),
            "market_price": str(item.market_price),
            "market_value": str(item.market_value),
            "unrealized_pnl": str(item.unrealized_pnl),
        }
        for item in snapshot.positions
    ]
    return {
        "run_id": run_id,
        "session_date": snapshot.session_date,
        "cash": snapshot.cash,
        "market_value": snapshot.market_value,
        "equity": snapshot.equity,
        "positions_payload": _json(positions),
        "ledger_hash": snapshot.ledger_hash,
    }


def _event_parameters(run_id: UUID, event: Any) -> dict[str, object]:
    return {
        "run_id": run_id,
        "sequence": event.sequence,
        "event_type": event.event_type,
        "session_date": event.session_date,
        "client_order_id": event.client_order_id,
        "payload": _json(dict(event.payload)),
        "previous_hash": event.previous_hash,
        "event_hash": event.event_hash,
    }


def _execution_from_row(row: RowMapping) -> BacktestExecutionView:
    return BacktestExecutionView(
        client_order_id=str(row["client_order_id"]),
        instrument=str(row["instrument"]),
        side=str(row["side"]),
        requested_quantity=int(row["requested_quantity"]),
        state=str(row["state"]),
        session_date=row["session_date"],
        filled_quantity=int(row["filled_quantity"]),
        fill_price=row["fill_price"],
        gross_amount=row["gross_amount"],
        commission=row["commission"],
        stamp_duty=row["stamp_duty"],
        transfer_fee=row["transfer_fee"],
        rejection_code=(
            None if row["rejection_code"] is None else str(row["rejection_code"])
        ),
        ledger_hash=str(row["ledger_hash"]),
    )


def _snapshot_from_row(row: RowMapping) -> BacktestSnapshotView:
    raw_positions = row["positions_payload"]
    if not isinstance(raw_positions, list) or any(
        not isinstance(item, dict) for item in raw_positions
    ):
        raise PersistenceUnavailableError("Stored backtest positions are malformed")
    return BacktestSnapshotView(
        session_date=row["session_date"],
        cash=row["cash"],
        market_value=row["market_value"],
        equity=row["equity"],
        positions=tuple(dict(item) for item in raw_positions),
        ledger_hash=str(row["ledger_hash"]),
    )


def _event_from_row(row: RowMapping) -> BacktestEventView:
    payload = _object(row["payload"])
    return BacktestEventView(
        sequence=int(row["sequence"]),
        event_type=str(row["event_type"]),
        session_date=row["session_date"],
        client_order_id=str(row["client_order_id"]),
        payload={str(key): str(value) for key, value in payload.items()},
        previous_hash=str(row["previous_hash"]),
        event_hash=str(row["event_hash"]),
    )


def _verify_completed_detail(detail: BacktestRunDetail) -> None:
    run = detail.run
    if run.state is not OperatorJobState.COMPLETED:
        return
    if (
        run.metrics is None
        or run.as_of is None
        or run.result_hash is None
        or run.ledger_hash is None
    ):
        raise ValueError("completed run is missing integrity fields")
    events: list[LedgerEvent] = []
    for event_view in detail.events:
        event = LedgerEvent(
            sequence=event_view.sequence,
            event_type=event_view.event_type,
            session_date=event_view.session_date,
            client_order_id=event_view.client_order_id,
            payload=tuple(sorted(event_view.payload.items())),
            previous_hash=event_view.previous_hash,
        )
        if event.event_hash != event_view.event_hash:
            raise ValueError("event hash mismatch")
        if not events and event.previous_hash != "0" * 64:
            raise ValueError("event chain must start at genesis")
        if events and event.previous_hash != events[-1].event_hash:
            raise ValueError("event chain mismatch")
        events.append(event)
    reports = tuple(
        ExecutionReport(
            client_order_id=execution_view.client_order_id,
            instrument=execution_view.instrument,
            side=OrderSide(execution_view.side),
            requested_quantity=execution_view.requested_quantity,
            state=ExecutionState(execution_view.state),
            session_date=execution_view.session_date,
            filled_quantity=execution_view.filled_quantity,
            fill_price=execution_view.fill_price,
            gross_amount=execution_view.gross_amount,
            fees=FeeBreakdown(
                commission=execution_view.commission,
                stamp_duty=execution_view.stamp_duty,
                transfer_fee=execution_view.transfer_fee,
            ),
            rejection_code=(
                None
                if execution_view.rejection_code is None
                else RejectionCode(execution_view.rejection_code)
            ),
            ledger_hash=execution_view.ledger_hash,
        )
        for execution_view in detail.executions
    )
    snapshots: list[AccountSnapshot] = []
    for snapshot_view in detail.snapshots:
        positions = tuple(
            PositionSnapshot(
                instrument=str(position["instrument"]),
                total_quantity=int(str(position["total_quantity"])),
                sellable_quantity=int(str(position["sellable_quantity"])),
                average_cost=Decimal(str(position["average_cost"])),
                market_price=Decimal(str(position["market_price"])),
                market_value=Decimal(str(position["market_value"])),
                unrealized_pnl=Decimal(str(position["unrealized_pnl"])),
            )
            for position in snapshot_view.positions
        )
        snapshots.append(
            AccountSnapshot(
                session_date=snapshot_view.session_date,
                cash=snapshot_view.cash,
                market_value=snapshot_view.market_value,
                equity=snapshot_view.equity,
                positions=positions,
                ledger_hash=snapshot_view.ledger_hash,
            )
        )
    metrics = run.metrics
    if metrics.initial_cash != run.request.initial_cash:
        raise ValueError("initial cash mismatch")
    reconstructed = BacktestResult(
        strategy_id=run.strategy_id,
        manifest_hash=run.request.manifest_hash,
        as_of=run.as_of,
        initial_cash=metrics.initial_cash,
        ending_equity=metrics.ending_equity,
        total_return=metrics.total_return,
        max_drawdown=metrics.max_drawdown,
        turnover=metrics.turnover,
        total_fees=metrics.total_fees,
        reports=reports,
        snapshots=tuple(snapshots),
        events=tuple(events),
        rule_versions=metrics.rule_versions,
        fee_version=metrics.fee_version,
        execution_version=metrics.execution_version,
        ledger_hash=run.ledger_hash,
    )
    if reconstructed.result_hash != run.result_hash:
        raise ValueError("result hash mismatch")
    if (
        metrics.artifact_hash is not None
        and backtest_artifact_hash(reconstructed) != metrics.artifact_hash
    ):
        raise ValueError("artifact hash mismatch")


def _object(value: object) -> dict[str, object]:
    if isinstance(value, str):
        decoded = json.loads(value)
    else:
        decoded = value
    if not isinstance(decoded, dict):
        raise TypeError("stored JSON value must be an object")
    return dict(decoded)


def _json(value: object) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)
