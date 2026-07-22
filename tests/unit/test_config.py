import pytest
from pydantic import SecretStr

from autoquant.config import AppSettings, RuntimeEnvironment, TushareCredentials
from autoquant.errors import MissingCapabilityError


def test_defaults_are_non_live_and_fail_closed() -> None:
    settings = AppSettings(_env_file=None)
    assert settings.environment is RuntimeEnvironment.BACKTEST
    assert settings.live_trading_enabled is False
    with pytest.raises(MissingCapabilityError, match="RQData credentials"):
        settings.require_rqdata()
    with pytest.raises(MissingCapabilityError, match="Tushare token"):
        settings.require_tushare()


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


def test_live_flag_is_rejected_outside_live_environment() -> None:
    with pytest.raises(ValueError, match="live environment"):
        AppSettings(_env_file=None, live_trading_enabled=True)
