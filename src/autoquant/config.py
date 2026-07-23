from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from autoquant.errors import MissingCapabilityError


class RuntimeEnvironment(StrEnum):
    BACKTEST = "backtest"
    PAPER = "paper"
    CANARY = "canary"
    LIVE = "live"


class RqdataCredentials(BaseModel):
    username: str
    password: SecretStr = Field(repr=False)


class TushareCredentials(BaseModel):
    token: SecretStr = Field(repr=False)


class WebCredentials(BaseModel):
    username: str
    password: SecretStr = Field(repr=False)


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AQ_", env_file=".env", extra="forbid")

    environment: RuntimeEnvironment = RuntimeEnvironment.BACKTEST
    live_trading_enabled: bool = False
    rqdata_username: str | None = None
    rqdata_password: SecretStr | None = None
    rqdata_auth_url: str = "https://rqdata.ricequant.com/auth"
    rqdata_api_url: str = "https://rqdata.ricequant.com/api"
    tushare_token: SecretStr | None = None
    tushare_api_url: str = "https://api.tushare.pro"
    postgres_dsn: SecretStr | None = None
    clickhouse_dsn: SecretStr | None = None
    web_host: str = "127.0.0.1"
    web_port: int = Field(default=8000, ge=1, le=65535)
    web_username: str = "operator"
    web_password: SecretStr | None = None
    paper_account_id: str = "paper-main"
    paper_initial_cash: Decimal = Field(
        default=Decimal("1000000"), ge=Decimal("10000"), le=Decimal("1000000000")
    )
    qmt_userdata_path: Path | None = None
    qmt_account_id: SecretStr | None = Field(default=None, repr=False)
    qmt_session_id: int | None = Field(default=None, ge=1, le=2_147_483_647)

    @field_validator("qmt_userdata_path", mode="before")
    @classmethod
    def empty_qmt_path_is_unconfigured(cls, value: object) -> object:
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator("tushare_api_url")
    @classmethod
    def require_tushare_https(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("Tushare API URL must use HTTPS")
        return value.rstrip("/")

    @model_validator(mode="after")
    def reject_unsafe_live_flag(self) -> "AppSettings":
        if self.live_trading_enabled:
            raise ValueError("live trading is hard-locked in this release")
        return self

    @field_validator("paper_account_id")
    @classmethod
    def require_safe_paper_account_id(cls, value: str) -> str:
        normalized = value.strip()
        if (
            not 1 <= len(normalized) <= 64
            or not normalized[0].isalnum()
            or any(
                character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
                for character in normalized
            )
        ):
            raise ValueError("paper_account_id must be a 1-64 character safe identifier")
        return normalized

    def require_rqdata(self) -> RqdataCredentials:
        password = self.rqdata_password
        if (
            not self.rqdata_username
            or not self.rqdata_username.strip()
            or password is None
            or not password.get_secret_value().strip()
        ):
            raise MissingCapabilityError("RQData credentials are not configured")
        return RqdataCredentials(username=self.rqdata_username, password=password)

    def require_tushare(self) -> TushareCredentials:
        token = self.tushare_token
        if token is None or not token.get_secret_value().strip():
            raise MissingCapabilityError("Tushare token is not configured")
        return TushareCredentials(token=token)

    @field_validator("web_host")
    @classmethod
    def require_web_loopback(cls, value: str) -> str:
        normalized = value.strip().casefold()
        if normalized not in {"127.0.0.1", "::1", "localhost"}:
            raise ValueError("Web console must bind to a loopback host")
        return normalized

    def require_web(self) -> WebCredentials:
        username = self.web_username.strip()
        password = self.web_password
        if (
            not username
            or password is None
            or len(password.get_secret_value().strip()) < 16
        ):
            raise MissingCapabilityError("Web credentials are not configured")
        return WebCredentials(username=username, password=password)
