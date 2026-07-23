from pathlib import Path

from autoquant.config import AppSettings
from autoquant.execution.qmt_preflight import (
    QmtReadinessCode,
    inspect_qmt_readiness,
    qmt_permission_sentinel,
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
        system_name="Windows",
        pointer_bits=64,
        xtquant_module_available=True,
    )

    assert report.blockers == (QmtReadinessCode.SESSION_ID_UNIQUE,)
