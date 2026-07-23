from decimal import Decimal

import pytest
from pydantic import SecretStr

from autoquant.config import (
    AppSettings,
    RuntimeEnvironment,
    TushareCredentials,
    WebCredentials,
)
from autoquant.errors import MissingCapabilityError


def test_defaults_are_non_live_and_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "AQ_RQDATA_USERNAME",
        "AQ_RQDATA_PASSWORD",
        "AQ_TUSHARE_TOKEN",
        "AQ_WEB_PASSWORD",
        "AQ_QMT_USERDATA_PATH",
        "AQ_QMT_ACCOUNT_ID",
        "AQ_QMT_SESSION_ID",
    ):
        monkeypatch.delenv(name, raising=False)
    settings = AppSettings(_env_file=None)
    assert settings.environment is RuntimeEnvironment.BACKTEST
    assert settings.live_trading_enabled is False
    assert settings.paper_initial_cash == Decimal("1000000")
    assert settings.qmt_userdata_path is None
    assert settings.qmt_account_id is None
    assert settings.qmt_session_id is None
    with pytest.raises(MissingCapabilityError, match="RQData credentials"):
        settings.require_rqdata()
    with pytest.raises(MissingCapabilityError, match="Tushare token"):
        settings.require_tushare()
    with pytest.raises(MissingCapabilityError, match="Web credentials"):
        settings.require_web()


def test_rqdata_credentials_are_secret_values() -> None:
    settings = AppSettings(
        _env_file=None,
        rqdata_username="user",
        rqdata_password="password",
    )
    credentials = settings.require_rqdata()
    assert isinstance(credentials.password, SecretStr)
    assert "password" not in repr(credentials)


@pytest.mark.parametrize(
    ("username", "password"),
    [
        ("", "password"),
        ("   ", "password"),
        ("user", ""),
        ("user", " \t "),
    ],
)
def test_empty_rqdata_credentials_fail_closed(username: str, password: str) -> None:
    settings = AppSettings(
        _env_file=None,
        rqdata_username=username,
        rqdata_password=password,
    )

    with pytest.raises(MissingCapabilityError, match="RQData credentials"):
        settings.require_rqdata()


def test_tushare_token_is_a_secret_value() -> None:
    settings = AppSettings(_env_file=None, tushare_token="new-local-token")

    credentials = settings.require_tushare()

    assert isinstance(credentials, TushareCredentials)
    assert isinstance(credentials.token, SecretStr)
    assert "new-local-token" not in repr(credentials)
    assert "new-local-token" not in repr(settings)


@pytest.mark.parametrize("token", ["", " ", "\t\n"])
def test_empty_tushare_token_fails_closed(token: str) -> None:
    settings = AppSettings(_env_file=None, tushare_token=token)

    with pytest.raises(MissingCapabilityError, match="Tushare token"):
        settings.require_tushare()


@pytest.mark.parametrize(
    "url",
    [
        "http://api.tushare.pro",
        "ftp://api.tushare.pro",
        "api.tushare.pro",
    ],
)
def test_tushare_api_url_requires_https(url: str) -> None:
    with pytest.raises(ValueError, match="HTTPS"):
        AppSettings(_env_file=None, tushare_api_url=url)


def test_tushare_api_url_defaults_to_official_https_endpoint() -> None:
    settings = AppSettings(_env_file=None)

    assert settings.tushare_api_url == "https://api.tushare.pro"


def test_web_credentials_are_secret_and_loopback_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AQ_WEB_HOST", raising=False)
    monkeypatch.delenv("AQ_WEB_PORT", raising=False)
    settings = AppSettings(
        _env_file=None,
        web_username="operator",
        web_password="a-long-local-password",
    )

    credentials = settings.require_web()

    assert isinstance(credentials, WebCredentials)
    assert isinstance(credentials.password, SecretStr)
    assert settings.web_host == "127.0.0.1"
    assert settings.web_port == 8000
    assert "a-long-local-password" not in repr(settings)
    assert "a-long-local-password" not in repr(credentials)


@pytest.mark.parametrize("password", ["", "short", " " * 20])
def test_web_password_fails_closed(password: str) -> None:
    settings = AppSettings(_env_file=None, web_password=password)

    with pytest.raises(MissingCapabilityError, match="Web credentials"):
        settings.require_web()


@pytest.mark.parametrize("host", ["0.0.0.0", "192.168.1.20", "example.com"])
def test_web_host_rejects_non_loopback_binding(host: str) -> None:
    with pytest.raises(ValueError, match="loopback"):
        AppSettings(_env_file=None, web_host=host)


def test_live_flag_is_rejected_outside_live_environment() -> None:
    with pytest.raises(ValueError, match="hard-locked"):
        AppSettings(_env_file=None, live_trading_enabled=True)


def test_live_flag_is_still_rejected_in_live_environment() -> None:
    with pytest.raises(ValueError, match="hard-locked"):
        AppSettings(
            _env_file=None,
            environment=RuntimeEnvironment.LIVE,
            live_trading_enabled=True,
        )


@pytest.mark.parametrize("account_id", ["", " paper account ", "../paper", "x" * 65])
def test_paper_account_id_must_be_safe(account_id: str) -> None:
    with pytest.raises(ValueError, match="paper_account_id"):
        AppSettings(_env_file=None, paper_account_id=account_id)


@pytest.mark.parametrize("strategy_id", ["", " strategy id ", "../strategy", "x" * 65])
def test_paper_strategy_id_must_be_safe(strategy_id: str) -> None:
    with pytest.raises(ValueError, match="paper_strategy_id"):
        AppSettings(_env_file=None, paper_strategy_id=strategy_id)


@pytest.mark.parametrize("initial_cash", ["9999.99", "1000000000.01", "NaN"])
def test_paper_initial_cash_is_bounded(initial_cash: str) -> None:
    with pytest.raises(ValueError):
        AppSettings(_env_file=None, paper_initial_cash=initial_cash)


def test_qmt_account_is_secret_and_empty_path_is_unconfigured() -> None:
    settings = AppSettings(
        _env_file=None,
        qmt_userdata_path="  ",
        qmt_account_id="sensitive-account-id",
        qmt_session_id=123456,
    )

    assert settings.qmt_userdata_path is None
    assert isinstance(settings.qmt_account_id, SecretStr)
    assert "sensitive-account-id" not in repr(settings)
    assert settings.qmt_session_id == 123456


@pytest.mark.parametrize("session_id", [0, -1, 2_147_483_648])
def test_qmt_session_id_is_positive_and_bounded(session_id: int) -> None:
    with pytest.raises(ValueError):
        AppSettings(_env_file=None, qmt_session_id=session_id)
