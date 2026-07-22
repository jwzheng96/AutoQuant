from enum import StrEnum

from pydantic import BaseModel, Field, SecretStr, model_validator
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


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AQ_", env_file=".env", extra="forbid")

    environment: RuntimeEnvironment = RuntimeEnvironment.BACKTEST
    live_trading_enabled: bool = False
    rqdata_username: str | None = None
    rqdata_password: SecretStr | None = None
    rqdata_auth_url: str = "https://rqdata.ricequant.com/auth"
    rqdata_api_url: str = "https://rqdata.ricequant.com/api"
    postgres_dsn: SecretStr | None = None
    clickhouse_dsn: SecretStr | None = None

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
