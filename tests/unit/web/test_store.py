from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast
from uuid import uuid4

from sqlalchemy.engine import RowMapping

from autoquant.web.backtest_store import PostgresBacktestRepository
from autoquant.web.models import BacktestRunRequest, OperatorJobState
from autoquant.web.store import PostgresOperatorRepository


def test_operator_job_row_mapping_does_not_expose_credentials() -> None:
    job_id = uuid4()
    row = cast(
        RowMapping,
        {
            "job_id": job_id,
            "job_type": "daily_ingestion",
            "state": "completed",
            "request_payload": {
                "instruments": ["000001.XSHE"],
                "start": "2025-01-01",
                "end": "2025-01-02",
                "idempotency_key": "operator-row-mapping-0001",
            },
            "requested_by": "operator",
            "created_at": datetime(2025, 1, 1, tzinfo=UTC),
            "started_at": datetime(2025, 1, 1, tzinfo=UTC),
            "completed_at": datetime(2025, 1, 1, tzinfo=UTC),
            "result_payload": {"status": "completed", "persisted_bars": 2},
            "error_code": None,
        },
    )

    job = PostgresOperatorRepository._job_from_row(row)

    assert job.job_id == job_id
    assert job.state is OperatorJobState.COMPLETED
    assert job.request.start == date(2025, 1, 1)
    assert job.result == {"status": "completed", "persisted_bars": 2}


def test_operator_console_migration_has_persistent_queue_constraints() -> None:
    migration = Path("migrations/postgres/002_operator_console.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS operator_jobs" in migration
    assert "idempotency_key text NOT NULL UNIQUE" in migration
    assert "FOR UPDATE" not in migration
    assert "VALUES ('postgres', 2)" in migration


def test_backtest_run_row_mapping_keeps_decimal_request_values() -> None:
    run_id = uuid4()
    row = cast(
        RowMapping,
        {
            "run_id": run_id,
            "state": "queued",
            "strategy_id": "manifest_buy_hold_v1",
            "request_payload": {
                "manifest_hash": "a" * 64,
                "instrument": "000001.XSHE",
                "initial_cash": "1000000",
                "allocation": "0.95",
                "slippage_bps": "5",
                "liquidate_at_end": True,
                "idempotency_key": "backtest-row-mapping-0001",
            },
            "requested_by": "operator",
            "created_at": datetime(2025, 1, 1, tzinfo=UTC),
            "started_at": None,
            "completed_at": None,
            "as_of": None,
            "result_hash": None,
            "ledger_hash": None,
            "metrics_payload": None,
            "error_code": None,
        },
    )

    run = PostgresBacktestRepository._run_from_row(row)

    assert run.run_id == run_id
    assert run.request == BacktestRunRequest.model_validate(row["request_payload"])
    assert run.state is OperatorJobState.QUEUED


def test_backtest_migration_has_atomic_result_tables_and_schema_version() -> None:
    migration = Path("migrations/postgres/004_backtest_runs.sql").read_text(
        encoding="utf-8"
    )

    assert "CREATE TABLE IF NOT EXISTS backtest_runs" in migration
    assert "CREATE TABLE IF NOT EXISTS backtest_executions" in migration
    assert "CREATE TABLE IF NOT EXISTS backtest_snapshots" in migration
    assert "CREATE TABLE IF NOT EXISTS backtest_events" in migration
    assert "idempotency_key text NOT NULL UNIQUE" in migration
    assert "VALUES ('postgres', 4)" in migration
