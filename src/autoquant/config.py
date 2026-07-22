from enum import StrEnum
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

    @field_validator("tushare_api_url")
    @classmethod
    def require_tushare_https(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("Tushare API URL must use HTTPS")
        return value.rstrip("/")

    @model_validator(mode="after")
    def reject_unsafe_live_flag(self) -> "AppSettings":
        if self.live_trading_enabled and self.environment is not RuntimeEnvironment.LIVE:
            raise ValueError("live_trading_enabled requires the live environment")
        return self

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
