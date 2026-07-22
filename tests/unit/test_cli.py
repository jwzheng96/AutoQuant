from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from autoquant.cli import app

runner = CliRunner()


def test_phase1_migrations_record_explicit_schema_versions() -> None:
    postgres = Path("migrations/postgres/001_phase1.sql").read_text(encoding="utf-8")
    clickhouse = Path("migrations/clickhouse/001_phase1.sql").read_text(encoding="utf-8")

    assert "schema_versions" in postgres
    assert "('postgres', 1)" in postgres
    assert "schema_versions" in clickhouse
    assert "SELECT 'clickhouse', 1" in clickhouse


def test_config_check_reports_missing_capabilities_without_secret() -> None:
    result = runner.invoke(app, ["config-check"], env={})

    assert result.exit_code == 2
    payload = json.loads(result.stdout)
    assert payload == {
        "clickhouse": "missing",
        "environment": "backtest",
        "live_trading_enabled": False,
        "postgres": "missing",
        "rqdata": "missing",
    }
    assert "password" not in result.stdout.lower()
    assert "dsn" not in result.stdout.lower()


def test_config_check_reports_only_capability_presence() -> None:
    result = runner.invoke(
        app,
        ["config-check"],
        env={
            "AQ_RQDATA_USERNAME": "configured-user",
            "AQ_RQDATA_PASSWORD": "configured-password",
            "AQ_POSTGRES_DSN": "postgresql+asyncpg://configured-secret",
            "AQ_CLICKHOUSE_DSN": "https://configured-secret",
        },
    )

    assert result.exit_code == 0
    assert set(json.loads(result.stdout).values()) >= {"configured"}
    assert "configured-user" not in result.stdout
    assert "configured-password" not in result.stdout
    assert "configured-secret" not in result.stdout


def test_ingestion_refuses_live_environment_flag_before_capability_checks() -> None:
    result = runner.invoke(
        app,
        [
            "ingest-minute",
            "--instrument",
            "000001.XSHE",
            "--start",
            "2026-07-20T09:30:00+08:00",
            "--end",
            "2026-07-20T09:31:00+08:00",
        ],
        env={"AQ_ENVIRONMENT": "live", "AQ_LIVE_TRADING_ENABLED": "true"},
    )

    assert result.exit_code != 0
    assert "phase-1 ingestion does not enable trading" in result.stdout
    assert "password" not in result.stdout.lower()


def test_ingestion_requires_timezone_aware_bounds() -> None:
    result = runner.invoke(
        app,
        [
            "ingest-minute",
            "--instrument",
            "000001.XSHE",
            "--start",
            "2026-07-20T09:30:00",
            "--end",
            "2026-07-20T09:31:00",
        ],
        env={},
    )

    assert result.exit_code == 2
    assert "timezone-aware" in result.stdout
