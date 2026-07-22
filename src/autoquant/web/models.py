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


class SmaCandidateRequest(BaseModel):
    fast_sessions: int = Field(ge=2, le=60)
    slow_sessions: int = Field(ge=5, le=250)

    @model_validator(mode="after")
    def validate_windows(self) -> Self:
        if self.fast_sessions >= self.slow_sessions:
            raise ValueError("fast_sessions must be smaller than slow_sessions")
        return self


class WalkForwardJobRequest(BaseModel):
    manifest_hash: str
    instrument: str
    initial_cash: Decimal = Field(default=Decimal("1000000"), ge=10_000, le=1_000_000_000)
    allocation: Decimal = Field(default=Decimal("0.95"), gt=0, le=1)
    slippage_bps: Decimal = Field(default=Decimal("5"), ge=0, le=100)
    train_sessions: int = Field(default=120, ge=60, le=750)
    test_sessions: int = Field(default=40, ge=20, le=250)
    embargo_sessions: int = Field(default=1, ge=1, le=20)
    candidates: tuple[SmaCandidateRequest, ...] = Field(
        default=(
            SmaCandidateRequest(fast_sessions=5, slow_sessions=20),
            SmaCandidateRequest(fast_sessions=10, slow_sessions=30),
            SmaCandidateRequest(fast_sessions=20, slow_sessions=60),
        ),
        min_length=1,
        max_length=25,
    )
    idempotency_key: str

    @field_validator("manifest_hash")
    @classmethod
    def validate_validation_manifest_hash(cls, value: str) -> str:
        normalized = value.strip().lower()
        if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
            raise ValueError("manifest_hash must be a lowercase SHA-256 hash")
        return normalized

    @field_validator("instrument")
    @classmethod
    def validate_validation_instrument(cls, value: str) -> str:
        normalized = value.strip().upper()
        if _INSTRUMENT.fullmatch(normalized) is None:
            raise ValueError("instrument must use the 000001.XSHE form")
        return normalized

    @field_validator("idempotency_key")
    @classmethod
    def validate_validation_idempotency_key(cls, value: str) -> str:
        normalized = value.strip()
        if _IDEMPOTENCY_KEY.fullmatch(normalized) is None:
            raise ValueError("idempotency_key must be 16-128 safe characters")
        return normalized

    @model_validator(mode="after")
    def validate_validation_grid(self) -> Self:
        pairs = tuple(
            (candidate.fast_sessions, candidate.slow_sessions)
            for candidate in self.candidates
        )
        if len(set(pairs)) != len(pairs):
            raise ValueError("candidates must be unique")
        if max(candidate.slow_sessions for candidate in self.candidates) >= self.train_sessions:
            raise ValueError("every slow window must be smaller than train_sessions")
        return self


class ValidationSummary(BaseModel):
    fold_count: int = Field(ge=1)
    compounded_oos_return: Decimal
    mean_oos_return: Decimal
    worst_oos_drawdown: Decimal
    profitable_fold_rate: Decimal
    mean_training_return: Decimal
    selection_optimism: Decimal
    validation_version: str
    objective_version: str
    benchmark_compounded_oos_return: Decimal | None = None
    excess_oos_return: Decimal | None = None
    oos_sessions: int = Field(default=0, ge=0)
    evidence_status: str = "insufficient"
    gate_failures: tuple[str, ...] = ("legacy_assessment_missing",)


class ValidationExperiment(BaseModel):
    experiment_id: UUID
    state: OperatorJobState
    validator_id: str
    request: WalkForwardJobRequest
    requested_by: str
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    as_of: datetime | None = None
    result_hash: str | None = None
    summary: ValidationSummary | None = None
    error_code: str | None = None

    @field_validator("created_at", "started_at", "completed_at", "as_of")
    @classmethod
    def require_aware_validation_time(
        cls, value: datetime | None
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("validation timestamps must be timezone-aware")
        return value.astimezone(UTC)


class ValidationFoldView(BaseModel):
    sequence: int = Field(ge=1)
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    selected: SmaCandidateRequest
    selection_score: Decimal
    training: BacktestMetrics
    test: BacktestMetrics
    benchmark: BacktestMetrics | None = None
    training_result_hash: str
    test_result_hash: str
    fold_hash: str


class ValidationExperimentDetail(BaseModel):
    experiment: ValidationExperiment
    folds: tuple[ValidationFoldView, ...]


class RiskDecisionView(BaseModel):
    decision_hash: str
    account_id: str
    client_order_id: str
    instrument: str
    side: str
    quantity: int = Field(gt=0)
    mode: str
    state: str
    violations: tuple[str, ...]
    evaluated_at: datetime
    policy_hash: str
    account_state_hash: str
    quote_hash: str
    order_notional: Decimal

    @field_validator("evaluated_at")
    @classmethod
    def require_aware_risk_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("risk decision time must be timezone-aware")
        return value.astimezone(UTC)


class RiskControlStatus(BaseModel):
    status: str
    live_trading_locked: bool
    paper_gateway_available: bool
    decision_count: int = Field(ge=0)
    recent_decisions: tuple[RiskDecisionView, ...]
    remaining_gates: tuple[str, ...]


class PaperExecutionStatus(BaseModel):
    status: str
    persistence_available: bool
    recovery_verified: bool
    gateway_available: bool
    order_count: int = Field(ge=0)
    event_count: int = Field(ge=0)
    reconciliation_count: int = Field(ge=0)
    open_order_count: int = Field(ge=0)
    latest_reconciliation_at: datetime | None = None
    latest_reconciled: bool | None = None
    remaining_gates: tuple[str, ...]

    @field_validator("latest_reconciliation_at")
    @classmethod
    def require_aware_reconciliation_time(
        cls, value: datetime | None
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("reconciliation time must be timezone-aware")
        return value.astimezone(UTC)
