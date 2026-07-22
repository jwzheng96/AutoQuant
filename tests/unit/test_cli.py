from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

from typer.testing import CliRunner

from autoquant.cli import app

runner = CliRunner()
MISSING_ENV = {
    "AQ_RQDATA_USERNAME": "",
    "AQ_RQDATA_PASSWORD": "",
    "AQ_TUSHARE_TOKEN": "",
    "AQ_POSTGRES_DSN": "",
    "AQ_CLICKHOUSE_DSN": "",
    "AQ_WEB_PASSWORD": "",
}


def test_phase1_migrations_record_explicit_schema_versions() -> None:
    postgres = Path("migrations/postgres/001_phase1.sql").read_text(encoding="utf-8")
    clickhouse = Path("migrations/clickhouse/001_phase1.sql").read_text(encoding="utf-8")

    assert "schema_versions" in postgres
    assert "('postgres', 1)" in postgres
    assert "schema_versions" in clickhouse
    assert "SELECT 'clickhouse', 1" in clickhouse


def test_config_check_reports_missing_capabilities_without_secret() -> None:
    result = runner.invoke(app, ["config-check"], env=MISSING_ENV)

    assert result.exit_code == 2
    payload = json.loads(result.stdout)
    assert payload == {
        "clickhouse": "missing",
        "environment": "backtest",
        "live_trading_enabled": False,
        "postgres": "missing",
        "rqdata": "missing",
        "tushare": "missing",
        "web": "missing",
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
            "AQ_TUSHARE_TOKEN": "configured-token",
            "AQ_POSTGRES_DSN": "postgresql+asyncpg://configured-secret",
            "AQ_CLICKHOUSE_DSN": "https://configured-secret",
            "AQ_WEB_PASSWORD": "configured-web-password",
        },
    )

    assert result.exit_code == 0
    assert set(json.loads(result.stdout).values()) >= {"configured"}
    assert "configured-user" not in result.stdout
    assert "configured-password" not in result.stdout
    assert "configured-token" not in result.stdout
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
    assert '"error":"configuration is invalid"' in result.stdout
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


def test_tushare_check_requires_token_without_leaking_configuration() -> None:
    result = runner.invoke(app, ["tushare-check"], env={"AQ_TUSHARE_TOKEN": ""})

    assert result.exit_code == 2
    assert "Tushare capability check failed" in result.stdout
    assert "token" not in result.stdout.lower()


def test_tushare_check_reports_sorted_capability_matrix_and_incomplete_exit() -> None:
    statuses = {
        "trade_cal": "available",
        "daily": "available",
        "stock_basic": "error",
        "adj_factor": "permission_denied",
        "suspend_d": "available",
    }
    with patch(
        "autoquant.cli._tushare_capabilities",
        new=AsyncMock(return_value=statuses),
    ):
        result = runner.invoke(
            app,
            ["tushare-check", "--instrument", "000001.XSHE", "--date", "2026-07-20"],
            env={"AQ_TUSHARE_TOKEN": "configured-test-token"},
        )

    assert result.exit_code == 2
    assert json.loads(result.stdout) == {
        "capabilities": dict(sorted(statuses.items())),
        "status": "incomplete",
    }
    assert "configured-test-token" not in result.stdout


def test_tushare_check_succeeds_only_when_all_required_endpoints_are_available() -> None:
    statuses = {
        method: "available"
        for method in (
            "daily",
            "adj_factor",
            "trade_cal",
            "stock_basic",
            "suspend_d",
            "stk_limit",
        )
    }
    with patch(
        "autoquant.cli._tushare_capabilities",
        new=AsyncMock(return_value=statuses),
    ):
        result = runner.invoke(
            app,
            ["tushare-check"],
            env={"AQ_TUSHARE_TOKEN": "configured-test-token"},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout)["status"] == "ok"


def test_daily_ingestion_rejects_bad_dates_before_capability_checks() -> None:
    result = runner.invoke(
        app,
        [
            "ingest-daily",
            "--instrument",
            "000001.XSHE",
            "--start",
            "2026-07-20T00:00:00",
            "--end",
            "2026-07-20",
        ],
        env={},
    )

    assert result.exit_code == 2
    assert "invalid date" in result.stdout


def test_daily_ingestion_refuses_trading_enablement() -> None:
    result = runner.invoke(
        app,
        [
            "ingest-daily",
            "--instrument",
            "000001.XSHE",
            "--start",
            "2026-07-20",
            "--end",
            "2026-07-20",
        ],
        env={"AQ_ENVIRONMENT": "live", "AQ_LIVE_TRADING_ENABLED": "true"},
    )

    assert result.exit_code == 2
    assert '"error":"configuration is invalid"' in result.stdout


def test_daily_ingestion_emits_completed_result_from_async_wiring() -> None:
    payload = {
        "fetched_bars": 1,
        "fetched_factors": 1,
        "manifest_hash": "a" * 64,
        "persisted_bars": 1,
        "persisted_factors": 1,
        "quality_hash": "b" * 64,
        "status": "completed",
    }
    ingestion = AsyncMock(return_value=payload)
    with patch("autoquant.cli._ingest_daily", new=ingestion):
        result = runner.invoke(
            app,
            [
                "ingest-daily",
                "--instrument",
                "000001.XSHE",
                "--start",
                "2026-07-20",
                "--end",
                "2026-07-20",
            ],
            env={},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    ingestion.assert_awaited_once()


def test_web_console_requires_credentials_without_leaking_configuration() -> None:
    result = runner.invoke(app, ["serve-web"], env={"AQ_WEB_PASSWORD": ""})

    assert result.exit_code == 2
    assert "operator console configuration failed" in result.stdout
    assert "password" not in result.stdout.casefold()


def test_web_console_starts_only_on_configured_loopback() -> None:
    with patch("uvicorn.run") as run:
        result = runner.invoke(
            app,
            ["serve-web"],
            env={
                "AQ_WEB_USERNAME": "operator",
                "AQ_WEB_PASSWORD": "local-console-password",
                "AQ_WEB_HOST": "127.0.0.1",
                "AQ_WEB_PORT": "8765",
            },
        )

    assert result.exit_code == 0
    assert run.call_args.kwargs["host"] == "127.0.0.1"
    assert run.call_args.kwargs["port"] == 8765
