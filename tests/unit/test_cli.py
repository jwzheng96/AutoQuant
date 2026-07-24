from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

from typer.testing import CliRunner

from autoquant.cli import app
from autoquant.errors import MissingCapabilityError

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


def test_paper_runtime_check_emits_only_readiness_evidence() -> None:
    payload = {
        "account_id": "paper-main",
        "kill_switch_active": True,
        "live_trading_locked": True,
        "status": "ready_for_quote_connection",
    }
    with patch(
        "autoquant.cli.inspect_paper_runtime_readiness",
        new=AsyncMock(return_value=payload),
    ):
        result = runner.invoke(app, ["paper-runtime-check"])

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload


def test_paper_runtime_check_reports_stable_capability_blocker() -> None:
    with patch(
        "autoquant.cli.inspect_paper_runtime_readiness",
        new=AsyncMock(
            side_effect=MissingCapabilityError("paper runtime requires an active approved strategy")
        ),
    ):
        result = runner.invoke(app, ["paper-runtime-check"])

    assert result.exit_code == 2
    assert json.loads(result.stdout) == {
        "error": "paper runtime requires an active approved strategy",
        "status": "failed",
    }


def test_promotion_check_emits_redacted_blockers_and_exits_nonzero() -> None:
    payload = {
        "blockers": ["paper_session_count", "compliance_approval"],
        "evaluated_at": "2026-07-23T08:00:00+00:00",
        "evidence_gates_passed": False,
        "fact_hash": "a" * 64,
        "gates": {
            "paper_session_count": {
                "actual": "0",
                "required": ">=60",
                "status": "blocked",
            }
        },
        "live_trading_ready": False,
        "policy_hash": "b" * 64,
        "report_hash": "c" * 64,
        "status": "blocked",
    }
    with patch(
        "autoquant.cli.inspect_paper_promotion",
        new=AsyncMock(return_value=payload),
    ):
        result = runner.invoke(app, ["promotion-check"])

    assert result.exit_code == 2
    assert json.loads(result.stdout) == payload
    assert "account_id" not in result.stdout
    assert "token" not in result.stdout.lower()


def test_compliance_approval_requires_confirmation_and_is_redacted() -> None:
    command = [
        "compliance-approve",
        "--external-artifact-hash",
        "a" * 64,
        "--approval-reference",
        "GRC/AQ/2026-0001",
        "--approved-by",
        "independent-compliance",
        "--valid-until",
        "2026-08-01T00:00:00Z",
    ]

    denied = runner.invoke(app, command)

    assert denied.exit_code == 2
    approval = AsyncMock(
        return_value={
            "approval_hash": "b" * 64,
            "live_trading_locked": True,
            "status": ("approved_for_promotion_audit_only"),
        }
    )
    with patch(
        "autoquant.cli.create_compliance_approval",
        new=approval,
    ):
        accepted = runner.invoke(
            app,
            [
                *command,
                "--confirm-independent-compliance",
            ],
            env={"AQ_POSTGRES_DSN": ("postgresql+asyncpg://sensitive")},
        )

    assert accepted.exit_code == 0
    assert json.loads(accepted.stdout)["live_trading_locked"] is True
    assert "sensitive" not in accepted.stdout
    approval.assert_awaited_once()


def test_compliance_revocation_requires_confirmation() -> None:
    command = [
        "compliance-revoke",
        "--approval-hash",
        "a" * 64,
        "--revoked-by",
        "risk-operator",
        "--reason",
        "operator_safety_action",
    ]

    denied = runner.invoke(app, command)

    assert denied.exit_code == 2
    revocation = AsyncMock(
        return_value={
            "approval_hash": "a" * 64,
            "live_trading_locked": True,
            "revocation_hash": "b" * 64,
            "status": "revoked",
        }
    )
    with patch(
        "autoquant.cli.revoke_compliance_approval",
        new=revocation,
    ):
        accepted = runner.invoke(
            app,
            [*command, "--confirm-revocation"],
        )

    assert accepted.exit_code == 0
    assert json.loads(accepted.stdout)["status"] == "revoked"
    revocation.assert_awaited_once()


def test_run_paper_refuses_non_windows_before_database_or_quote_connection() -> None:
    secret = "paper-runtime-lease-secret-value-0001"
    result = runner.invoke(
        app,
        ["run-paper"],
        env={
            "AQ_ENVIRONMENT": "paper",
            "AQ_POSTGRES_DSN": "postgresql+asyncpg://unused",
            "AQ_CLICKHOUSE_DSN": "https://unused",
            "AQ_PAPER_SCHEDULER_HOLDER_ID": "paper-node-01",
            "AQ_PAPER_SCHEDULER_LEASE_TOKEN": secret,
        },
    )

    assert result.exit_code == 2
    assert "resident paper runtime failed closed" in result.stdout
    assert secret not in result.stdout


def test_unlock_paper_requires_explicit_confirmation() -> None:
    result = runner.invoke(
        app,
        ["unlock-paper", "--actor", "operator"],
    )

    assert result.exit_code == 2
    assert "confirmation is required" in result.stdout


def test_unlock_paper_emits_only_fenced_result() -> None:
    payload = {
        "account_id": "paper-main",
        "control_version": 4,
        "evidence_hash": "a" * 64,
        "live_trading_locked": True,
        "status": "paper_unlocked",
        "strategy_id": "validated-sma-paper",
    }
    with patch(
        "autoquant.cli.unlock_paper_runtime",
        new=AsyncMock(return_value=payload),
    ):
        result = runner.invoke(
            app,
            [
                "unlock-paper",
                "--actor",
                "operator",
                "--confirm-paper-unlock",
            ],
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload


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


def test_qmt_check_is_read_only_blocked_and_does_not_emit_configuration() -> None:
    with patch(
        "autoquant.cli._qmt_preflight_db_state",
        new=AsyncMock(return_value=(True, ())),
    ):
        result = runner.invoke(
            app,
            ["qmt-check"],
            env={
                "AQ_QMT_USERDATA_PATH": "/sensitive/userdata_mini",
                "AQ_QMT_ACCOUNT_ID": "sensitive-broker-account",
                "AQ_QMT_SESSION_ID": "123456",
            },
        )

    assert result.exit_code == 2
    payload = json.loads(result.stdout)
    assert payload["status"] == "blocked"
    assert payload["live_trading_ready"] is False
    assert "sensitive" not in result.stdout
    assert "123456" not in result.stdout


def test_qmt_readonly_accept_requires_explicit_confirmation() -> None:
    result = runner.invoke(
        app,
        ["qmt-readonly-accept", "--actor", "operator"],
    )

    assert result.exit_code == 2
    assert "explicit confirmation" in result.stdout


def test_qmt_readonly_accept_emits_only_redacted_evidence() -> None:
    payload = {
        "account_snapshot_hash": "a" * 64,
        "evidence_hash": "b" * 64,
        "live_trading_locked": True,
        "order_count": 0,
        "position_count": 1,
        "status": "qmt_readonly_accepted",
        "trade_count": 0,
    }
    acceptance = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.run_qmt_readonly_acceptance",
        new=acceptance,
    ):
        result = runner.invoke(
            app,
            [
                "qmt-readonly-accept",
                "--actor",
                "operator",
                "--confirm-read-only",
            ],
            env={
                "AQ_QMT_ACCOUNT_ID": "sensitive-broker-account",
                "AQ_QMT_LEASE_TOKEN": ("sensitive-qmt-lease-token-with-32-characters"),
            },
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    acceptance.assert_awaited_once()


def test_qmt_observer_requires_explicit_read_only_confirmation() -> None:
    result = runner.invoke(app, ["run-qmt-observer"])

    assert result.exit_code == 2
    assert "explicit read-only confirmation" in result.stdout


def test_qmt_observer_starts_without_emitting_configuration() -> None:
    observer = AsyncMock(return_value=None)
    with patch(
        "autoquant.cli.run_qmt_observer",
        new=observer,
    ):
        result = runner.invoke(
            app,
            ["run-qmt-observer", "--confirm-read-only"],
            env={
                "AQ_QMT_ACCOUNT_ID": "sensitive-broker-account",
                "AQ_QMT_LEASE_TOKEN": ("sensitive-qmt-lease-token-with-32-characters"),
            },
        )

    assert result.exit_code == 0
    assert "sensitive" not in result.stdout
    observer.assert_awaited_once()


def test_qmt_recovery_drill_requires_explicit_confirmation() -> None:
    start = runner.invoke(
        app,
        [
            "qmt-drill-start",
            "--kind",
            "disconnect_recovery",
            "--actor",
            "operator",
        ],
    )
    complete = runner.invoke(
        app,
        [
            "qmt-drill-complete",
            "--drill-id",
            "5d6bf55d-adf4-41b0-a688-bc65e10f44d0",
            "--actor",
            "operator",
        ],
    )

    assert start.exit_code == 2
    assert complete.exit_code == 2
    assert "confirmation is required" in start.stdout
    assert "confirmation is required" in complete.stdout


def test_qmt_recovery_drill_outputs_only_evidence_hashes() -> None:
    payload = {
        "baseline_qmt_evidence_hash": "a" * 64,
        "drill_id": "5d6bf55d-adf4-41b0-a688-bc65e10f44d0",
        "event_hash": "b" * 64,
        "expires_at": "2026-07-23T08:30:00+00:00",
        "kind": "disconnect_recovery",
        "live_trading_locked": True,
        "status": "drill_started",
    }
    start = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.start_qmt_recovery_drill",
        new=start,
    ):
        result = runner.invoke(
            app,
            [
                "qmt-drill-start",
                "--kind",
                "disconnect_recovery",
                "--actor",
                "operator",
                "--confirm-controlled-drill",
            ],
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "account" not in result.stdout.lower()
    assert "token" not in result.stdout.lower()
    start.assert_awaited_once()


def test_paper_preopen_check_emits_hashes_but_no_marks_or_configuration() -> None:
    payload = {
        "instrument_count": 2,
        "kill_switch_active": True,
        "marks_hash": "a" * 64,
        "session_date": "2026-07-23",
        "source_evidence_hash": "b" * 64,
        "status": "ok",
        "valuation_session_date": "2026-07-22",
    }
    inspection = AsyncMock(return_value=payload)
    with patch("autoquant.cli.inspect_paper_pre_open", new=inspection):
        result = runner.invoke(
            app,
            [
                "paper-preopen-check",
                "--instrument",
                "000001.XSHE",
                "--instrument",
                "600000.XSHG",
                "--manifest-hash",
                "c" * 64,
                "--as-of",
                "2026-07-23T09:00:00+08:00",
            ],
            env={
                "AQ_ENVIRONMENT": "paper",
                "AQ_POSTGRES_DSN": "postgresql+asyncpg://sensitive-postgres",
                "AQ_CLICKHOUSE_DSN": "http://sensitive-clickhouse",
            },
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    assert '"marks":' not in result.stdout
    inspection.assert_awaited_once()


def test_calendar_refresh_emits_only_audit_metadata() -> None:
    payload = {
        "audit_event_hash": "a" * 64,
        "session_count": 1,
        "source_evidence_hash": "b" * 64,
        "status": "completed",
    }
    refresh = AsyncMock(return_value=payload)
    with patch("autoquant.cli.run_trading_calendar_refresh", new=refresh):
        result = runner.invoke(
            app,
            [
                "refresh-trading-calendar",
                "--start",
                "2026-07-23",
                "--end",
                "2026-07-23",
            ],
            env={"AQ_TUSHARE_TOKEN": "sensitive-token"},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    refresh.assert_awaited_once()


def test_session_reference_refresh_emits_only_audit_metadata() -> None:
    payload = {
        "audit_event_hash": "a" * 64,
        "instrument_count": 1,
        "reference_hash": "b" * 64,
        "session_date": "2026-07-23",
        "status": "completed",
    }
    refresh = AsyncMock(return_value=payload)
    with patch("autoquant.cli.run_session_reference_refresh", new=refresh):
        result = runner.invoke(
            app,
            [
                "refresh-session-reference",
                "--instrument",
                "600000.XSHG",
                "--date",
                "2026-07-23",
            ],
            env={"AQ_TUSHARE_TOKEN": "sensitive-token"},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    refresh.assert_awaited_once()


def test_paper_sma_approval_requires_explicit_paper_only_confirmation() -> None:
    approval = AsyncMock()
    with patch("autoquant.cli.approve_paper_sma_strategy", new=approval):
        denied = runner.invoke(
            app,
            [
                "approve-paper-sma",
                "--experiment-id",
                "11111111-1111-1111-1111-111111111111",
                "--signal-manifest-hash",
                "a" * 64,
                "--reference-date",
                "2026-07-23",
                "--approved-by",
                "operator",
            ],
        )

    assert denied.exit_code == 2
    assert approval.await_count == 0


def test_paper_sma_approval_emits_only_paper_artifact_metadata() -> None:
    payload = {
        "account_id": "paper-main",
        "execution_mode": "paper",
        "experiment_id": "11111111-1111-1111-1111-111111111111",
        "fast_sessions": 5,
        "instrument": "600000.XSHG",
        "live_trading_locked": True,
        "registration_hash": "b" * 64,
        "signal_manifest_hash": "a" * 64,
        "slow_sessions": 20,
        "status": "approved",
        "strategy_id": "validated-sma-paper",
        "strategy_version": "sma-paper-v1:test:5-20",
    }
    approval = AsyncMock(return_value=payload)
    with patch("autoquant.cli.approve_paper_sma_strategy", new=approval):
        result = runner.invoke(
            app,
            [
                "approve-paper-sma",
                "--experiment-id",
                str(payload["experiment_id"]),
                "--signal-manifest-hash",
                "a" * 64,
                "--reference-date",
                "2026-07-23",
                "--approved-by",
                "operator",
                "--confirm-paper-only",
            ],
            env={
                "AQ_ENVIRONMENT": "paper",
                "AQ_POSTGRES_DSN": "postgresql+asyncpg://sensitive",
            },
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    approval.assert_awaited_once()


def test_paper_portfolio_approval_requires_matched_components() -> None:
    approval = AsyncMock()
    with patch(
        "autoquant.cli.approve_paper_sma_portfolio_strategy",
        new=approval,
    ):
        denied = runner.invoke(
            app,
            [
                "approve-paper-portfolio",
                "--experiment-id",
                "11111111-1111-1111-1111-111111111111",
                "--signal-manifest-hash",
                "a" * 64,
                "--valuation-manifest-hash",
                "b" * 64,
                "--reference-date",
                "2026-07-23",
                "--approved-by",
                "operator",
                "--confirm-paper-only",
            ],
        )

    assert denied.exit_code == 2
    assert approval.await_count == 0


def test_validation_campaign_queues_only_redacted_research_metadata() -> None:
    payload = {
        "campaign_hash": "a" * 64,
        "campaign_key": "portfolio-research-20260723-0001",
        "components": [],
        "created_at": "2026-07-23T08:00:00+00:00",
        "instrument_count": 3,
        "live_trading_locked": True,
        "manifest_hash": "b" * 64,
        "status": "queued",
    }
    creation = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.create_validation_campaign",
        new=creation,
    ):
        result = runner.invoke(
            app,
            [
                "validation-campaign-create",
                "--campaign-key",
                "portfolio-research-20260723-0001",
                "--manifest-hash",
                "b" * 64,
                "--instrument",
                "000001.XSHE",
                "--instrument",
                "600000.XSHG",
                "--instrument",
                "600519.XSHG",
                "--candidate",
                "5:20",
                "--candidate",
                "10:30",
                "--candidate",
                "20:60",
                "--requested-by",
                "operator",
            ],
            env={"AQ_POSTGRES_DSN": "postgresql+asyncpg://sensitive"},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    creation.assert_awaited_once()


def test_validation_campaign_rejects_invalid_candidate_syntax() -> None:
    creation = AsyncMock()
    with patch(
        "autoquant.cli.create_validation_campaign",
        new=creation,
    ):
        result = runner.invoke(
            app,
            [
                "validation-campaign-create",
                "--campaign-key",
                "portfolio-research-20260723-0001",
                "--manifest-hash",
                "b" * 64,
                "--instrument",
                "000001.XSHE",
                "--candidate",
                "invalid",
                "--requested-by",
                "operator",
            ],
        )

    assert result.exit_code == 2
    assert creation.await_count == 0


def test_low_volatility_spec_freeze_emits_only_redacted_metadata() -> None:
    payload = {
        "created_at": "2026-07-23T12:00:00+00:00",
        "live_trading_locked": True,
        "minimum_history_sessions": 253,
        "predecessor_result_hash": "a" * 64,
        "rebalance_sessions": 21,
        "selection_count": 20,
        "spec_hash": "b" * 64,
        "status": "frozen",
        "strategy_id": "dynamic-universe-low-volatility-v4",
        "version": "low-volatility-research-spec-v4",
        "volatility_lookback_sessions": 252,
    }
    freeze = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.freeze_low_volatility_research_spec",
        new=freeze,
    ):
        result = runner.invoke(
            app,
            [
                "low-volatility-spec-freeze",
                "--predecessor-result-hash",
                "a" * 64,
                "--requested-by",
                "operator",
            ],
            env={"AQ_POSTGRES_DSN": ("postgresql+asyncpg://sensitive")},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    freeze.assert_awaited_once()


def test_low_volatility_validation_emits_only_redacted_evidence() -> None:
    payload = {
        "assessment_hash": "a" * 64,
        "benchmark_compounded_oos_return": "0.01",
        "benchmark_rejected_order_count": 0,
        "benchmark_unresolved_position_count": 2,
        "compounded_oos_return": "0.02",
        "evidence_status": "research_candidate",
        "excess_oos_return": "0.01",
        "fold_count": 12,
        "gate_failures": [],
        "live_trading_locked": True,
        "market_panel_hash": "b" * 64,
        "oos_sessions": 756,
        "panel_hash": "c" * 64,
        "profitable_fold_rate": "0.75",
        "requested_by": "operator",
        "result_hash": "d" * 64,
        "spec_hash": "e" * 64,
        "status": "completed",
        "strategy_rejected_order_count": 0,
        "strategy_unresolved_position_count": 0,
        "train_test_gap": "0.01",
        "version": "low-volatility-fixed-walk-forward-v1",
        "worst_oos_drawdown": "0.08",
    }
    validation = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.run_low_volatility_validation",
        new=validation,
    ):
        result = runner.invoke(
            app,
            [
                "low-volatility-validation-run",
                "--spec-hash",
                "e" * 64,
                "--requested-by",
                "operator",
            ],
            env={"AQ_POSTGRES_DSN": ("postgresql+asyncpg://sensitive")},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    validation.assert_awaited_once()


def test_low_volatility_forward_spec_discloses_outcome_observation() -> None:
    payload = {
        "created_at": "2026-07-24T12:00:00+00:00",
        "formal_hypothesis_count": 4,
        "forward_start_date": "2026-07-23",
        "historical_result_eligible_for_promotion": False,
        "live_trading_locked": True,
        "minimum_forward_sessions": 126,
        "minimum_paper_sessions": 60,
        "outcome_observed_at_design": True,
        "predecessor_result_hash": "a" * 64,
        "retrospective_reclassification_allowed": False,
        "spec_hash": "b" * 64,
        "stability_method_version": ("annualized-geometric-return-gap-v1"),
        "status": "frozen_awaiting_forward_data",
        "strategy_parameters_unchanged": True,
        "version": "low-volatility-forward-evidence-spec-v1",
    }
    freeze = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.freeze_low_volatility_forward_evidence_spec",
        new=freeze,
    ):
        result = runner.invoke(
            app,
            [
                "low-volatility-forward-spec-freeze",
                "--predecessor-result-hash",
                "a" * 64,
                "--requested-by",
                "operator",
            ],
            env={"AQ_POSTGRES_DSN": ("postgresql+asyncpg://sensitive")},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    freeze.assert_awaited_once()


def test_low_volatility_forward_session_create_is_redacted() -> None:
    payload = {
        "campaign_hash": "a" * 64,
        "completed_items": 0,
        "forward_spec_hash": "b" * 64,
        "instrument_count": 300,
        "live_trading_locked": True,
        "session_date": "2026-07-23",
        "snapshot_hash": "c" * 64,
        "snapshot_reference_date": "2026-07-22",
        "status": "queued",
    }
    creation = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.create_low_volatility_forward_session_campaign",
        new=creation,
    ):
        result = runner.invoke(
            app,
            [
                "low-volatility-forward-session-create",
                "--forward-spec-hash",
                "b" * 64,
                "--session",
                "2026-07-23",
                "--requested-by",
                "operator",
            ],
            env={"AQ_POSTGRES_DSN": ("postgresql+asyncpg://sensitive")},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    creation.assert_awaited_once()


def test_low_volatility_forward_session_finalize_is_redacted() -> None:
    payload = {
        "binding_hash": "a" * 64,
        "calendar_as_of": "2026-07-24T01:00:00+00:00",
        "completed_at": "2026-07-24T02:00:00+00:00",
        "dataset_manifest_hash": "b" * 64,
        "forward_spec_hash": "c" * 64,
        "instrument_count": 300,
        "live_trading_locked": True,
        "session_date": "2026-07-23",
        "snapshot_hash": "d" * 64,
        "snapshot_reference_date": "2026-07-22",
        "status": "frozen",
        "version": "low-volatility-forward-session-binding-v1",
    }
    finalization = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.finalize_low_volatility_forward_session",
        new=finalization,
    ):
        result = runner.invoke(
            app,
            [
                "low-volatility-forward-session-finalize",
                "--forward-spec-hash",
                "c" * 64,
                "--dataset-manifest-hash",
                "b" * 64,
                "--requested-by",
                "operator",
            ],
            env={"AQ_POSTGRES_DSN": ("postgresql+asyncpg://sensitive")},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    finalization.assert_awaited_once()


def test_low_volatility_forward_cycle_is_bounded_and_redacted() -> None:
    payload = {
        "completed_required_sessions": 1,
        "forward_spec_hash": "a" * 64,
        "live_trading_locked": True,
        "minimum_forward_sessions": 126,
        "remaining_required_sessions": 125,
        "safe_cutoff_date": "2026-07-23",
        "status": "waiting_for_completed_session",
    }
    cycle = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.run_low_volatility_forward_cycle",
        new=cycle,
    ):
        result = runner.invoke(
            app,
            [
                "low-volatility-forward-cycle-run",
                "--forward-spec-hash",
                "a" * 64,
                "--requested-by",
                "operator",
                "--max-items",
                "10",
                "--pause-seconds",
                "1.25",
            ],
            env={"AQ_POSTGRES_DSN": ("postgresql+asyncpg://sensitive")},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    cycle.assert_awaited_once()
    assert cycle.await_args.kwargs["max_items"] == 10
    assert cycle.await_args.kwargs["pause_seconds"] == Decimal("1.25")


def test_low_volatility_forward_window_is_bounded_and_redacted() -> None:
    payload = {
        "binding_hash": "b" * 64,
        "completed_required_sessions": 2,
        "cycle_statuses": ["batch_progress", "session_frozen"],
        "forward_spec_hash": "a" * 64,
        "live_trading_locked": True,
        "remaining_required_sessions": 124,
        "status": "session_frozen",
        "window_cycles": 2,
        "window_exhausted": False,
    }
    window = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.run_low_volatility_forward_window",
        new=window,
    ):
        result = runner.invoke(
            app,
            [
                "low-volatility-forward-window-run",
                "--forward-spec-hash",
                "a" * 64,
                "--requested-by",
                "scheduler",
                "--max-cycles",
                "20",
                "--max-items",
                "25",
                "--pause-seconds",
                "1.25",
                "--interval-seconds",
                "5",
            ],
            env={"AQ_POSTGRES_DSN": ("postgresql+asyncpg://sensitive")},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    window.assert_awaited_once()
    assert window.await_args.kwargs["max_cycles"] == 20
    assert window.await_args.kwargs["max_items"] == 25
    assert window.await_args.kwargs["pause_seconds"] == Decimal("1.25")
    assert window.await_args.kwargs["interval_seconds"] == Decimal("5")


def test_low_volatility_forward_evaluation_is_redacted_and_does_not_deploy() -> None:
    payload = {
        "assessment_hash": "b" * 64,
        "block_count": 6,
        "evidence_status": "paper_candidate",
        "evaluation_dataset_manifest_hash": "d" * 64,
        "forward_spec_hash": "a" * 64,
        "live_trading_locked": True,
        "paper_deployment_allowed": False,
        "paper_trading_eligible": True,
        "result_hash": "c" * 64,
        "session_count": 126,
        "status": "completed",
    }
    evaluation = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.run_low_volatility_forward_evaluation",
        new=evaluation,
    ):
        result = runner.invoke(
            app,
            [
                "low-volatility-forward-evaluate",
                "--forward-spec-hash",
                "a" * 64,
                "--evaluation-dataset-manifest-hash",
                "d" * 64,
                "--requested-by",
                "research-operator",
            ],
            env={
                "AQ_POSTGRES_DSN": (
                    "postgresql+asyncpg://sensitive"
                )
            },
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    assert payload["paper_deployment_allowed"] is False
    assert payload["live_trading_locked"] is True
    evaluation.assert_awaited_once()
    assert (
        evaluation.await_args.kwargs[
            "evaluation_dataset_manifest_hash"
        ]
        == "d" * 64
    )


def test_forward_evaluation_data_campaign_is_bounded_and_locked() -> None:
    payload = {
        "campaign_hash": "b" * 64,
        "completed_items": 0,
        "forward_spec_hash": "a" * 64,
        "instrument_count": 340,
        "live_trading_locked": True,
        "paper_deployment_allowed": False,
        "session_count": 126,
        "snapshot_count": 7,
        "status": "queued",
    }
    creation = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli."
        "create_low_volatility_forward_evaluation_campaign",
        new=creation,
    ):
        result = runner.invoke(
            app,
            [
                "low-volatility-forward-evaluation-data-create",
                "--forward-spec-hash",
                "a" * 64,
                "--requested-by",
                "research-operator",
            ],
            env={
                "AQ_POSTGRES_DSN": (
                    "postgresql+asyncpg://sensitive"
                )
            },
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    assert payload["paper_deployment_allowed"] is False
    assert payload["live_trading_locked"] is True
    creation.assert_awaited_once()


def test_research_input_plan_compilation_stays_live_locked_and_redacted() -> None:
    payload = {
        "activation_rule": "session_date>snapshot.reference_date",
        "campaign_hash": "a" * 64,
        "instrument_count": 493,
        "live_trading_locked": True,
        "manifest_hash": "b" * 64,
        "plan_hash": "c" * 64,
        "snapshot_count": 79,
        "status": "compiled",
    }
    compilation = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.compile_research_input",
        new=compilation,
    ):
        result = runner.invoke(
            app,
            [
                "research-input-plan-compile",
                "--manifest-hash",
                "b" * 64,
                "--requested-by",
                "operator",
            ],
            env={"AQ_POSTGRES_DSN": ("postgresql+asyncpg://sensitive")},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    compilation.assert_awaited_once()


def test_dynamic_research_freeze_requires_explicit_pre_registration() -> None:
    freeze = AsyncMock()
    with patch(
        "autoquant.cli.freeze_dynamic_research_spec",
        new=freeze,
    ):
        denied = runner.invoke(
            app,
            [
                "dynamic-research-spec-freeze",
                "--manifest-hash",
                "a" * 64,
                "--requested-by",
                "operator",
            ],
        )

    assert denied.exit_code == 2
    assert freeze.await_count == 0


def test_dynamic_research_freeze_is_live_locked_and_redacted() -> None:
    payload = {
        "dataset_manifest_hash": "a" * 64,
        "live_trading_locked": True,
        "plan_hash": "b" * 64,
        "spec_hash": "c" * 64,
        "status": "frozen",
    }
    freeze = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.freeze_dynamic_research_spec",
        new=freeze,
    ):
        result = runner.invoke(
            app,
            [
                "dynamic-research-spec-freeze",
                "--manifest-hash",
                "a" * 64,
                "--requested-by",
                "operator",
                "--confirm-pre-registration",
            ],
            env={"AQ_POSTGRES_DSN": ("postgresql+asyncpg://sensitive")},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    freeze.assert_awaited_once()


def test_dynamic_market_panel_compilation_is_live_locked() -> None:
    payload = {
        "history_count": 493,
        "live_trading_locked": True,
        "panel_hash": "a" * 64,
        "session_count": 1580,
        "spec_hash": "b" * 64,
        "status": "compiled",
    }
    compilation = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.compile_dynamic_market_panel",
        new=compilation,
    ):
        result = runner.invoke(
            app,
            [
                "dynamic-market-panel-compile",
                "--spec-hash",
                "b" * 64,
                "--requested-by",
                "operator",
            ],
            env={"AQ_POSTGRES_DSN": ("postgresql+asyncpg://sensitive")},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    compilation.assert_awaited_once()


def test_dynamic_regime_spec_freeze_is_live_locked() -> None:
    payload = {
        "live_trading_locked": True,
        "predecessor_result_hash": "a" * 64,
        "spec_hash": "b" * 64,
        "status": "frozen",
    }
    freeze = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.freeze_dynamic_regime_research_spec",
        new=freeze,
    ):
        result = runner.invoke(
            app,
            [
                "dynamic-regime-spec-freeze",
                "--predecessor-result-hash",
                "a" * 64,
                "--requested-by",
                "operator",
            ],
            env={"AQ_POSTGRES_DSN": ("postgresql+asyncpg://sensitive")},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    freeze.assert_awaited_once()


def test_dynamic_validation_run_is_live_locked_and_redacted() -> None:
    payload = {
        "evidence_status": "rejected",
        "fold_count": 16,
        "live_trading_locked": True,
        "result_hash": "a" * 64,
        "spec_hash": "b" * 64,
        "status": "completed",
    }
    validation = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.run_dynamic_validation",
        new=validation,
    ):
        result = runner.invoke(
            app,
            [
                "dynamic-validation-run",
                "--spec-hash",
                "b" * 64,
                "--requested-by",
                "operator",
            ],
            env={"AQ_POSTGRES_DSN": ("postgresql+asyncpg://sensitive")},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    validation.assert_awaited_once()


def test_research_input_shard_check_is_redacted() -> None:
    payload = {
        "instrument": "000001.XSHE",
        "live_trading_locked": True,
        "manifest_hash": "a" * 64,
        "plan_hash": "b" * 64,
        "status": "verified",
    }
    inspection = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.inspect_research_input_shard",
        new=inspection,
    ):
        result = runner.invoke(
            app,
            [
                "research-input-shard-check",
                "--manifest-hash",
                "a" * 64,
                "--instrument",
                "000001.XSHE",
                "--requested-by",
                "operator",
            ],
            env={"AQ_POSTGRES_DSN": ("postgresql+asyncpg://sensitive")},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    inspection.assert_awaited_once()


def test_portfolio_validation_cli_queues_live_locked_request() -> None:
    payload = {
        "assessment": None,
        "completed_at": None,
        "error_code": None,
        "experiment_id": ("11111111-1111-1111-1111-111111111111"),
        "fold_count": 0,
        "live_trading_locked": True,
        "manifest_hash": "a" * 64,
        "result_hash": None,
        "state": "queued",
        "validator_id": ("cross_sectional_momentum_walk_forward_v1"),
    }
    creation = AsyncMock(return_value=payload)
    with patch(
        "autoquant.cli.create_portfolio_validation",
        new=creation,
    ):
        result = runner.invoke(
            app,
            [
                "portfolio-validation-create",
                "--manifest-hash",
                "a" * 64,
                "--idempotency-key",
                "portfolio-cli-request-0001",
                "--requested-by",
                "operator",
                "--candidate",
                "20:5:3",
                "--candidate",
                "60:10:3",
                "--candidate",
                "120:20:3",
            ],
            env={"AQ_POSTGRES_DSN": ("postgresql+asyncpg://sensitive")},
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    request = creation.await_args.kwargs["request"]
    assert request.gross_allocation == Decimal("0.29")
    assert request.candidates[0].lookback_sessions == 20


def test_portfolio_validation_status_fails_for_rejected_evidence() -> None:
    inspection = AsyncMock(
        return_value={
            "assessment": {
                "evidence_status": "rejected",
                "gate_failures": ["nonpositive_excess_return"],
            },
            "experiment_id": ("11111111-1111-1111-1111-111111111111"),
            "live_trading_locked": True,
            "state": "completed",
        }
    )
    with patch(
        "autoquant.cli.inspect_portfolio_validation",
        new=inspection,
    ):
        result = runner.invoke(
            app,
            [
                "portfolio-validation-status",
                "--experiment-id",
                "11111111-1111-1111-1111-111111111111",
            ],
            env={"AQ_POSTGRES_DSN": ("postgresql+asyncpg://sensitive")},
        )

    assert result.exit_code == 2
    assert json.loads(result.stdout)["live_trading_locked"] is True


def test_paper_portfolio_approval_emits_redacted_metadata() -> None:
    experiment_ids = (
        "11111111-1111-1111-1111-111111111111",
        "22222222-2222-2222-2222-222222222222",
        "33333333-3333-3333-3333-333333333333",
    )
    payload = {
        "account_id": "paper-main",
        "components": [],
        "execution_mode": "paper",
        "instruments": [
            "000001.XSHE",
            "600000.XSHG",
            "600519.XSHG",
        ],
        "live_trading_locked": True,
        "registration_hash": "c" * 64,
        "status": "approved",
        "strategy_id": "validated-sma-paper",
        "strategy_version": "sma-portfolio-paper-v1:test",
        "valuation_manifest_hash": "b" * 64,
    }
    approval = AsyncMock(return_value=payload)
    arguments = ["approve-paper-portfolio"]
    for experiment_id, suffix in zip(
        experiment_ids,
        ("d", "e", "f"),
        strict=True,
    ):
        arguments.extend(
            [
                "--experiment-id",
                experiment_id,
                "--signal-manifest-hash",
                suffix * 64,
            ]
        )
    arguments.extend(
        [
            "--valuation-manifest-hash",
            "b" * 64,
            "--reference-date",
            "2026-07-23",
            "--approved-by",
            "operator",
            "--confirm-paper-only",
        ]
    )
    with patch(
        "autoquant.cli.approve_paper_sma_portfolio_strategy",
        new=approval,
    ):
        result = runner.invoke(
            app,
            arguments,
            env={
                "AQ_ENVIRONMENT": "paper",
                "AQ_POSTGRES_DSN": "postgresql+asyncpg://sensitive",
            },
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert "sensitive" not in result.stdout
    approval.assert_awaited_once()


def test_paper_strategy_revocation_requires_explicit_confirmation() -> None:
    revocation = AsyncMock()
    with patch("autoquant.cli.revoke_paper_strategy", new=revocation):
        result = runner.invoke(
            app,
            [
                "revoke-paper-strategy",
                "--revoked-by",
                "operator",
                "--reason",
                "scheduled_research_refresh",
            ],
        )

    assert result.exit_code == 2
    assert revocation.await_count == 0
