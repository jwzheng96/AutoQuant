import pytest
from pydantic import SecretStr

from autoquant.config import AppSettings, RuntimeEnvironment
from autoquant.errors import MissingCapabilityError


def test_defaults_are_non_live_and_fail_closed() -> None:
    settings = AppSettings(_env_file=None)
    assert settings.environment is RuntimeEnvironment.BACKTEST
    assert settings.live_trading_enabled is False
    with pytest.raises(MissingCapabilityError, match="RQData credentials"):
        settings.require_rqdata()


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


def test_live_flag_is_rejected_outside_live_environment() -> None:
    with pytest.raises(ValueError, match="live environment"):
        AppSettings(_env_file=None, live_trading_enabled=True)
