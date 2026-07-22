from __future__ import annotations

import re
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Self
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator

_INSTRUMENT = re.compile(r"[0-9]{6}\.(?:XSHG|XSHE)\Z")
_IDEMPOTENCY_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{15,127}\Z")


class OperatorJobState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


BacktestRunState = OperatorJobState


class DailyIngestionJobRequest(BaseModel):
    instruments: tuple[str, ...] = Field(min_length=1, max_length=20)
    start: date
    end: date
    idempotency_key: str

    @field_validator("instruments")
    @classmethod
    def validate_instruments(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(instrument.strip().upper() for instrument in value)
        if len(set(normalized)) != len(normalized):
            raise ValueError("instruments must be unique")
        if any(_INSTRUMENT.fullmatch(instrument) is None for instrument in normalized):
            raise ValueError("instrument must use the 000001.XSHE form")
        return normalized

    @field_validator("idempotency_key")
    @classmethod
    def validate_idempotency_key(cls, value: str) -> str:
        normalized = value.strip()
        if _IDEMPOTENCY_KEY.fullmatch(normalized) is None:
            raise ValueError("idempotency_key must be 16-128 safe characters")
        return normalized

    @model_validator(mode="after")
    def validate_interval(self) -> Self:
        if self.start > self.end:
            raise ValueError("start cannot be after end")
        if (self.end - self.start).days > 365:
            raise ValueError("daily ingestion interval cannot exceed 366 days")
        return self


class OperatorJob(BaseModel):
    job_id: UUID
    job_type: str = "daily_ingestion"
    state: OperatorJobState
    request: DailyIngestionJobRequest
    requested_by: str
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    result: dict[str, object] | None = None
    error_code: str | None = None

    @field_validator("created_at", "started_at", "completed_at")
    @classmethod
    def require_aware_time(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("job timestamps must be timezone-aware")
        return value.astimezone(UTC)


class DataCoverage(BaseModel):
    daily_rows: int = Field(ge=0)
    factor_rows: int = Field(ge=0)
    first_session: date | None = None
    last_session: date | None = None


class ControlSummary(BaseModel):
    manifests: int = Field(ge=0)
    quality_reports: int = Field(ge=0)
    audit_events: int = Field(ge=0)
    checkpoints: int = Field(ge=0)


class OperatorOverview(BaseModel):
    status: str
    postgres: str
    clickhouse: str
    tushare: str
    control: ControlSummary | None = None
    coverage: DataCoverage | None = None
    generated_at: datetime


class BacktestRunRequest(BaseModel):
    manifest_hash: str
    instrument: str
    initial_cash: Decimal = Field(default=Decimal("1000000"), ge=10_000, le=1_000_000_000)
    allocation: Decimal = Field(default=Decimal("0.95"), gt=0, le=1)
    slippage_bps: Decimal = Field(default=Decimal("5"), ge=0, le=100)
    liquidate_at_end: bool = True
    idempotency_key: str

    @field_validator("manifest_hash")
    @classmethod
    def validate_manifest_hash(cls, value: str) -> str:
        normalized = value.strip().lower()
        if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
            raise ValueError("manifest_hash must be a lowercase SHA-256 hash")
        return normalized

    @field_validator("instrument")
    @classmethod
    def validate_instrument(cls, value: str) -> str:
        normalized = value.strip().upper()
        if _INSTRUMENT.fullmatch(normalized) is None:
            raise ValueError("instrument must use the 000001.XSHE form")
        return normalized

    @field_validator("idempotency_key")
    @classmethod
    def validate_backtest_idempotency_key(cls, value: str) -> str:
        normalized = value.strip()
        if _IDEMPOTENCY_KEY.fullmatch(normalized) is None:
            raise ValueError("idempotency_key must be 16-128 safe characters")
        return normalized


class BacktestMetrics(BaseModel):
    initial_cash: Decimal
    ending_equity: Decimal
    total_return: Decimal
    max_drawdown: Decimal
    turnover: Decimal
    total_fees: Decimal
    rule_versions: tuple[str, ...]
    fee_version: str
    execution_version: str
    artifact_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class BacktestRun(BaseModel):
    run_id: UUID
    state: BacktestRunState
    strategy_id: str
    request: BacktestRunRequest
    requested_by: str
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    as_of: datetime | None = None
    result_hash: str | None = None
    ledger_hash: str | None = None
    metrics: BacktestMetrics | None = None
    error_code: str | None = None

    @field_validator("created_at", "started_at", "completed_at", "as_of")
    @classmethod
    def require_aware_backtest_time(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("backtest timestamps must be timezone-aware")
        return value.astimezone(UTC)


class BacktestExecutionView(BaseModel):
    client_order_id: str
    instrument: str
    side: str
    requested_quantity: int
    state: str
    session_date: date
    filled_quantity: int
    fill_price: Decimal | None
    gross_amount: Decimal
    commission: Decimal
    stamp_duty: Decimal
    transfer_fee: Decimal
    rejection_code: str | None
    ledger_hash: str


class BacktestSnapshotView(BaseModel):
    session_date: date
    cash: Decimal
    market_value: Decimal
    equity: Decimal
    positions: tuple[dict[str, object], ...]
    ledger_hash: str


class BacktestEventView(BaseModel):
    sequence: int
    event_type: str
    session_date: date
    client_order_id: str
    payload: dict[str, str]
    previous_hash: str
    event_hash: str


class BacktestRunDetail(BaseModel):
    run: BacktestRun
    executions: tuple[BacktestExecutionView, ...]
    snapshots: tuple[BacktestSnapshotView, ...]
    events: tuple[BacktestEventView, ...]


class ResearchManifest(BaseModel):
    manifest_hash: str
    instruments: tuple[str, ...]
    start_time: datetime
    end_time: datetime
    as_of: datetime
    row_count: int = Field(ge=0)
