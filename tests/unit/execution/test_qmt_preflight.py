from datetime import UTC, datetime, timedelta
from pathlib import Path

from autoquant.config import AppSettings
from autoquant.execution.qmt_preflight import (
    QmtClockAttestation,
    QmtReadinessCode,
    inspect_qmt_readiness,
    qmt_permission_sentinel,
)

NOW = datetime(2026, 7, 26, 2, tzinfo=UTC)


def _trusted_clock() -> QmtClockAttestation:
    return QmtClockAttestation(
        request_started_at=NOW,
        database_observed_at=NOW + timedelta(milliseconds=10),
        request_completed_at=NOW + timedelta(milliseconds=20),
    )


def test_unconfigured_non_windows_host_reports_all_material_blockers() -> None:
    report = inspect_qmt_readiness(
        AppSettings(_env_file=None),
        kill_switch_active=None,
        system_name="Darwin",
        pointer_bits=64,
        xtquant_module_available=False,
    )

    assert report.read_only_ready is False
    assert report.order_drill_ready is False
    assert report.live_trading_ready is False
    assert set(report.blockers) == {
        QmtReadinessCode.KILL_SWITCH,
        QmtReadinessCode.WINDOWS_RUNTIME,
        QmtReadinessCode.USERDATA_PATH,
        QmtReadinessCode.ACCOUNT_ID,
        QmtReadinessCode.SESSION_ID,
        QmtReadinessCode.SESSION_ID_UNIQUE,
        QmtReadinessCode.TRUSTED_CLOCK,
        QmtReadinessCode.XTQUANT_MODULE,
        QmtReadinessCode.ORDER_PERMISSION,
    }


def test_fully_configured_host_can_only_pass_read_only_and_order_drill_preflight(
    tmp_path: Path,
) -> None:
    userdata = tmp_path / "userdata_mini"
    userdata.mkdir()
    qmt_permission_sentinel(userdata).touch()
    settings = AppSettings(
        _env_file=None,
        qmt_userdata_path=userdata,
        qmt_account_id="secret-broker-account",
        qmt_session_id=456789,
    )

    report = inspect_qmt_readiness(
        settings,
        kill_switch_active=True,
        active_session_ids=set(),
        clock_attestation=_trusted_clock(),
        system_name="Windows",
        pointer_bits=64,
        xtquant_module_available=True,
    )

    assert report.blockers == ()
    assert report.read_only_ready is True
    assert report.order_drill_ready is True
    assert report.live_trading_ready is False
    assert "secret-broker-account" not in repr(report)


def test_duplicate_session_id_is_rejected_by_preflight(tmp_path: Path) -> None:
    userdata = tmp_path / "userdata_mini"
    userdata.mkdir()
    qmt_permission_sentinel(userdata).touch()
    settings = AppSettings(
        _env_file=None,
        qmt_userdata_path=userdata,
        qmt_account_id="secret-broker-account",
        qmt_session_id=123,
    )

    report = inspect_qmt_readiness(
        settings,
        kill_switch_active=True,
        active_session_ids={123},
        clock_attestation=_trusted_clock(),
        system_name="Windows",
        pointer_bits=64,
        xtquant_module_available=True,
    )

    assert report.blockers == (QmtReadinessCode.SESSION_ID_UNIQUE,)


def test_slow_or_skewed_database_clock_attestation_blocks_qmt(tmp_path: Path) -> None:
    userdata = tmp_path / "userdata_mini"
    userdata.mkdir()
    qmt_permission_sentinel(userdata).touch()
    settings = AppSettings(
        _env_file=None,
        qmt_userdata_path=userdata,
        qmt_account_id="secret-broker-account",
        qmt_session_id=123,
    )

    skewed = inspect_qmt_readiness(
        settings,
        kill_switch_active=True,
        active_session_ids=set(),
        clock_attestation=QmtClockAttestation(
            request_started_at=NOW,
            database_observed_at=NOW + timedelta(seconds=5),
            request_completed_at=NOW + timedelta(milliseconds=20),
        ),
        system_name="Windows",
        pointer_bits=64,
        xtquant_module_available=True,
    )
    slow = inspect_qmt_readiness(
        settings,
        kill_switch_active=True,
        active_session_ids=set(),
        clock_attestation=QmtClockAttestation(
            request_started_at=NOW,
            database_observed_at=NOW + timedelta(seconds=1),
            request_completed_at=NOW + timedelta(seconds=3),
        ),
        system_name="Windows",
        pointer_bits=64,
        xtquant_module_available=True,
    )

    assert skewed.blockers == (QmtReadinessCode.TRUSTED_CLOCK,)
    assert slow.blockers == (QmtReadinessCode.TRUSTED_CLOCK,)


def test_qmt_clock_migration_binds_v2_acceptance_payload() -> None:
    sql = Path(
        "migrations/postgres/052_qmt_clock_attestations.sql"
    ).read_text(encoding="utf-8")

    assert "clock_attestation_hash" in sql
    assert "clock_attestation_payload" in sql
    assert "qmt-readonly-acceptance-v2" in sql
    assert "qmt-clock-attestation-v1" in sql
    assert "VALUES ('postgres', 52)" in sql
