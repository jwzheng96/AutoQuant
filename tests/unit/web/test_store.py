from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast
from uuid import uuid4

from sqlalchemy.engine import RowMapping

from autoquant.web.backtest_store import PostgresBacktestRepository
from autoquant.web.models import BacktestRunRequest, OperatorJobState
from autoquant.web.portfolio_validation_store import (
    PostgresPortfolioValidationRepository,
)
from autoquant.web.store import PostgresOperatorRepository
from autoquant.web.validation_store import PostgresValidationRepository


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


def test_validation_experiment_row_mapping_is_typed_and_bounded() -> None:
    experiment_id = uuid4()
    row = cast(
        RowMapping,
        {
            "experiment_id": experiment_id,
            "state": "queued",
            "validator_id": "sma_cross_walk_forward_v1",
            "request_payload": {
                "manifest_hash": "a" * 64,
                "instrument": "000001.XSHE",
                "initial_cash": "1000000",
                "allocation": "0.95",
                "slippage_bps": "5",
                "train_sessions": 120,
                "test_sessions": 40,
                "embargo_sessions": 1,
                "candidates": [
                    {"fast_sessions": 5, "slow_sessions": 20},
                ],
                "idempotency_key": "validation-row-mapping-0001",
            },
            "requested_by": "operator",
            "created_at": datetime(2025, 1, 1, tzinfo=UTC),
            "started_at": None,
            "completed_at": None,
            "as_of": None,
            "result_hash": None,
            "summary_payload": None,
            "error_code": None,
        },
    )

    experiment = PostgresValidationRepository._experiment_from_row(row)

    assert experiment.experiment_id == experiment_id
    assert experiment.state is OperatorJobState.QUEUED
    assert experiment.request.candidates[0].slow_sessions == 20


def test_validation_migration_persists_full_selected_fold_artifacts() -> None:
    migration = Path("migrations/postgres/005_walk_forward_validation.sql").read_text(
        encoding="utf-8"
    )

    assert "CREATE TABLE IF NOT EXISTS validation_experiments" in migration
    assert "CREATE TABLE IF NOT EXISTS validation_folds" in migration
    assert "training_payload jsonb NOT NULL" in migration
    assert "test_payload jsonb NOT NULL" in migration
    assert "VALUES ('postgres', 5)" in migration
    benchmark = Path("migrations/postgres/006_validation_benchmark.sql").read_text(
        encoding="utf-8"
    )
    assert "benchmark_payload jsonb" in benchmark
    assert "VALUES ('postgres', 6)" in benchmark


def test_portfolio_validation_row_mapping_is_typed_and_locked() -> None:
    experiment_id = uuid4()
    row = cast(
        RowMapping,
        {
            "experiment_id": experiment_id,
            "state": "queued",
            "validator_id": (
                "cross_sectional_momentum_walk_forward_v1"
            ),
            "request_payload": {
                "manifest_hash": "a" * 64,
                "idempotency_key": (
                    "portfolio-row-mapping-0001"
                ),
            },
            "requested_by": "operator",
            "created_at": datetime(2025, 1, 1, tzinfo=UTC),
            "started_at": None,
            "completed_at": None,
            "as_of": None,
            "result_hash": None,
            "summary_payload": None,
            "error_code": None,
        },
    )

    experiment = (
        PostgresPortfolioValidationRepository
        ._experiment_from_row(row)
    )

    assert experiment.experiment_id == experiment_id
    assert experiment.state is OperatorJobState.QUEUED
    assert experiment.request.gross_allocation == Decimal("0.29")
    assert experiment.live_trading_locked is True


def test_portfolio_validation_migration_is_immutable_and_versioned() -> None:
    migration = Path(
        "migrations/postgres/022_portfolio_validation.sql"
    ).read_text(encoding="utf-8")

    assert (
        "CREATE TABLE IF NOT EXISTS "
        "portfolio_validation_experiments"
    ) in migration
    assert (
        "CREATE TABLE IF NOT EXISTS portfolio_validation_folds"
        in migration
    )
    assert "benchmark_payload jsonb NOT NULL" in migration
    assert "portfolio_validation_experiment_guard" in migration
    assert "terminal portfolio validation experiments are immutable" in migration
    assert "portfolio_validation_folds_immutable" in migration
    assert "VALUES ('postgres', 22)" in migration


def test_research_universe_migration_binds_source_evidence() -> None:
    migration = Path(
        "migrations/postgres/023_research_universes.sql"
    ).read_text(encoding="utf-8")

    assert (
        "CREATE TABLE IF NOT EXISTS research_universe_snapshots"
        in migration
    )
    assert (
        "REFERENCES source_evidence(evidence_hash)"
        in migration
    )
    assert "research_universe_snapshots_identity_idx" in migration
    assert "research_universe_snapshots_immutable" in migration
    assert "VALUES ('postgres', 23)" in migration


def test_research_data_campaign_migration_is_restart_safe_and_versioned() -> None:
    migration = Path(
        "migrations/postgres/024_research_data_campaigns.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS research_data_campaigns" in migration
    assert "CREATE TABLE IF NOT EXISTS research_data_campaign_items" in migration
    assert "CREATE TABLE IF NOT EXISTS research_dataset_manifests" in migration
    assert "REFERENCES dataset_manifests(manifest_hash)" in migration
    assert "research_data_campaign_items_queue_idx" in migration
    assert "research_dataset_manifests_immutable" in migration
    assert "VALUES ('postgres', 24)" in migration


def test_dynamic_research_spec_migration_prevents_post_result_tuning() -> None:
    migration = Path(
        "migrations/postgres/025_dynamic_research_specs.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS dynamic_research_specs" in migration
    assert "REFERENCES research_dataset_manifests(manifest_hash)" in migration
    assert "UNIQUE (dataset_manifest_hash, strategy_id)" in migration
    assert "live_trading_locked" in migration
    assert "dynamic_research_specs_immutable" in migration
    assert "VALUES ('postgres', 25)" in migration


def test_fundamental_research_migration_is_immutable_and_versioned() -> None:
    migration = Path(
        "migrations/postgres/028_fundamental_research.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS fundamental_research_specs" in migration
    assert "REFERENCES dynamic_validation_runs(result_hash)" in migration
    assert "fundamental_research_specs_immutable" in migration
    assert "dynamic-universe-quality-value-v3" in migration
    assert "live_trading_locked" in migration
    assert "VALUES ('postgres', 28)" in migration
