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


class PaperRuntimeCredentials(BaseModel):
    holder_id: str
    lease_token: SecretStr = Field(repr=False)


class QmtRuntimeCredentials(BaseModel):
    holder_id: str
    lease_token: SecretStr = Field(repr=False)


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
    paper_strategy_id: str = "validated-sma-paper"
    paper_initial_cash: Decimal = Field(
        default=Decimal("1000000"), ge=Decimal("10000"), le=Decimal("1000000000")
    )
    paper_scheduler_holder_id: str | None = None
    paper_scheduler_lease_token: SecretStr | None = Field(
        default=None,
        repr=False,
    )
    paper_poll_interval_seconds: Decimal = Field(
        default=Decimal("1"),
        ge=Decimal("0.1"),
        le=Decimal("60"),
    )
    research_data_max_inactive_bytes: int = Field(
        default=6 * 1024 * 1024 * 1024,
        ge=256 * 1024 * 1024,
        le=1024 * 1024 * 1024 * 1024,
    )
    research_data_max_inactive_parts: int = Field(
        default=24_000,
        ge=1_000,
        le=1_000_000,
    )
    paper_scheduler_lease_ttl_seconds: int = Field(default=30, ge=5, le=300)
    paper_scheduler_renewal_seconds: int = Field(default=10, ge=1, le=299)
    qmt_userdata_path: Path | None = None
    qmt_account_id: SecretStr | None = Field(default=None, repr=False)
    qmt_session_id: int | None = Field(default=None, ge=1, le=2_147_483_647)
    qmt_holder_id: str | None = None
    qmt_lease_token: SecretStr | None = Field(default=None, repr=False)
    qmt_lease_ttl_seconds: int = Field(default=30, ge=5, le=300)
    qmt_callback_poll_interval_seconds: Decimal = Field(
        default=Decimal("0.25"),
        ge=Decimal("0.05"),
        le=Decimal("1"),
    )
    qmt_reconciliation_interval_seconds: Decimal = Field(
        default=Decimal("30"),
        ge=Decimal("5"),
        le=Decimal("300"),
    )

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

    @field_validator("paper_account_id", "paper_strategy_id")
    @classmethod
    def require_safe_paper_runtime_id(cls, value: str) -> str:
        normalized = value.strip()
        if (
            not 1 <= len(normalized) <= 64
            or not normalized[0].isalnum()
            or any(
                character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
                for character in normalized
            )
        ):
            raise ValueError(
                "paper_account_id and paper_strategy_id must be 1-64 character safe identifiers"
            )
        return normalized

    @field_validator("paper_scheduler_holder_id", "qmt_holder_id")
    @classmethod
    def require_safe_scheduler_holder_id(
        cls,
        value: str | None,
    ) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if (
            not 1 <= len(normalized) <= 64
            or not normalized[0].isalnum()
            or any(
                character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
                for character in normalized
            )
        ):
            raise ValueError("runtime holder identifiers must be 1-64 character safe identifiers")
        return normalized

    @model_validator(mode="after")
    def require_scheduler_renewal_before_expiry(self) -> "AppSettings":
        if self.paper_scheduler_renewal_seconds >= self.paper_scheduler_lease_ttl_seconds:
            raise ValueError("paper scheduler renewal interval must be smaller than its lease TTL")
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
        if not username or password is None or len(password.get_secret_value().strip()) < 16:
            raise MissingCapabilityError("Web credentials are not configured")
        return WebCredentials(username=username, password=password)

    def require_paper_runtime(self) -> PaperRuntimeCredentials:
        holder_id = self.paper_scheduler_holder_id
        token = self.paper_scheduler_lease_token
        if holder_id is None or token is None or len(token.get_secret_value()) < 32:
            raise MissingCapabilityError(
                "Paper runtime scheduler lease credentials are not configured"
            )
        return PaperRuntimeCredentials(
            holder_id=holder_id,
            lease_token=token,
        )

    def require_qmt_runtime(self) -> QmtRuntimeCredentials:
        holder_id = self.qmt_holder_id
        token = self.qmt_lease_token
        if holder_id is None or token is None or len(token.get_secret_value()) < 32:
            raise MissingCapabilityError("QMT session lease credentials are not configured")
        return QmtRuntimeCredentials(
            holder_id=holder_id,
            lease_token=token,
        )
