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


class ResearchUniverseSnapshotView(BaseModel):
    snapshot_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    index_code: str = Field(pattern=r"^[0-9]{6}\.(?:SH|SZ)$")
    reference_date: date
    index_constituent_date: date
    liquidity_date: date
    knowledge_as_of: datetime
    member_count: int = Field(ge=20, le=1000)
    created_at: datetime
    live_trading_locked: bool = True

    @field_validator("knowledge_as_of", "created_at")
    @classmethod
    def require_aware_universe_time(
        cls,
        value: datetime,
    ) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(
                "research universe timestamps must be timezone-aware"
            )
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def require_universe_research_lock(self) -> Self:
        if (
            not self.live_trading_locked
            or self.index_constituent_date > self.reference_date
            or self.liquidity_date > self.reference_date
        ):
            raise ValueError("research universe view is inconsistent")
        return self


class ResearchUniverseMemberView(BaseModel):
    instrument: str = Field(pattern=r"^[0-9]{6}\.(?:XSHG|XSHE)$")
    index_weight: Decimal = Field(gt=0)
    turnover_rate_f: Decimal = Field(ge=0)
    volume_ratio: Decimal | None = Field(default=None, ge=0)
    circulating_market_value: Decimal = Field(gt=0)


class ResearchUniverseSnapshotDetail(BaseModel):
    snapshot: ResearchUniverseSnapshotView
    index_response_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    liquidity_response_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    members: tuple[ResearchUniverseMemberView, ...] = Field(
        min_length=20,
        max_length=1000,
    )


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


class MomentumCandidateRequest(BaseModel):
    lookback_sessions: int = Field(ge=20, le=252)
    rebalance_sessions: int = Field(ge=5, le=63)
    selection_count: int = Field(ge=1, le=10)


class PortfolioWalkForwardJobRequest(BaseModel):
    manifest_hash: str
    initial_cash: Decimal = Field(
        default=Decimal("1000000"),
        ge=10_000,
        le=1_000_000_000,
    )
    gross_allocation: Decimal = Field(
        default=Decimal("0.29"),
        gt=0,
        le=Decimal("0.80"),
    )
    maximum_order_notional: Decimal = Field(
        default=Decimal("100000"),
        gt=0,
    )
    slippage_bps: Decimal = Field(default=Decimal("5"), ge=0, le=100)
    train_sessions: int = Field(default=252, ge=126, le=750)
    test_sessions: int = Field(default=21, ge=20, le=126)
    embargo_sessions: int = Field(default=1, ge=1, le=20)
    candidates: tuple[MomentumCandidateRequest, ...] = Field(
        default=(
            MomentumCandidateRequest(
                lookback_sessions=20,
                rebalance_sessions=5,
                selection_count=3,
            ),
            MomentumCandidateRequest(
                lookback_sessions=60,
                rebalance_sessions=10,
                selection_count=3,
            ),
            MomentumCandidateRequest(
                lookback_sessions=120,
                rebalance_sessions=20,
                selection_count=3,
            ),
        ),
        min_length=1,
        max_length=12,
    )
    idempotency_key: str

    @field_validator("manifest_hash")
    @classmethod
    def validate_portfolio_manifest_hash(cls, value: str) -> str:
        normalized = value.strip().lower()
        if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
            raise ValueError(
                "manifest_hash must be a lowercase SHA-256 hash"
            )
        return normalized

    @field_validator("idempotency_key")
    @classmethod
    def validate_portfolio_idempotency_key(cls, value: str) -> str:
        normalized = value.strip()
        if _IDEMPOTENCY_KEY.fullmatch(normalized) is None:
            raise ValueError(
                "idempotency_key must be 16-128 safe characters"
            )
        return normalized

    @model_validator(mode="after")
    def validate_portfolio_grid(self) -> Self:
        candidates = tuple(
            (
                value.lookback_sessions,
                value.rebalance_sessions,
                value.selection_count,
            )
            for value in self.candidates
        )
        if len(set(candidates)) != len(candidates):
            raise ValueError("portfolio candidates must be unique")
        if max(value.lookback_sessions for value in self.candidates) >= (
            self.train_sessions
        ):
            raise ValueError(
                "portfolio candidate lookback must be smaller than training"
            )
        if any(
            self.gross_allocation / value.selection_count
            > Decimal("0.20")
            or self.initial_cash
            * self.gross_allocation
            / value.selection_count
            * (
                Decimal("1")
                + self.slippage_bps / Decimal("10000")
            )
            > self.maximum_order_notional
            for value in self.candidates
        ):
            raise ValueError(
                "portfolio candidate allocation exceeds risk limits"
            )
        return self


class PortfolioValidationSummary(BaseModel):
    fold_count: int = Field(ge=1)
    oos_sessions: int = Field(ge=1)
    compounded_oos_return: Decimal
    benchmark_compounded_oos_return: Decimal
    excess_oos_return: Decimal
    profitable_fold_rate: Decimal = Field(ge=0, le=1)
    worst_oos_drawdown: Decimal = Field(ge=0, le=1)
    mean_training_return: Decimal
    selection_optimism: Decimal
    rejected_order_count: int = Field(ge=0)
    evidence_status: str
    gate_failures: tuple[str, ...]
    validation_version: str
    objective_version: str
    policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    assessment_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class MomentumSelectionFrequencyView(BaseModel):
    parameters: MomentumCandidateRequest
    count: int = Field(ge=1)
    share: Decimal = Field(gt=0, le=1)


class PortfolioValidationDiagnosticsView(BaseModel):
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    fold_count: int = Field(ge=1)
    positive_excess_fold_rate: Decimal = Field(ge=0, le=1)
    median_fold_excess_return: Decimal
    mean_positive_fold_excess: Decimal
    mean_nonpositive_fold_excess: Decimal
    first_half_excess_return: Decimal
    second_half_excess_return: Decimal
    mean_strategy_turnover: Decimal = Field(ge=0)
    mean_benchmark_turnover: Decimal = Field(ge=0)
    strategy_fee_rate: Decimal = Field(ge=0)
    benchmark_fee_rate: Decimal = Field(ge=0)
    selection_frequencies: tuple[
        MomentumSelectionFrequencyView,
        ...,
    ] = Field(min_length=1)
    maximum_selection_share: Decimal = Field(gt=0, le=1)
    assessment_gate_failures: tuple[str, ...]
    diagnostic_codes: tuple[str, ...]
    version: str
    diagnostic_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    oos_tuning_permitted: bool = False

    @model_validator(mode="after")
    def require_diagnostic_read_only_boundary(self) -> Self:
        if self.oos_tuning_permitted:
            raise ValueError(
                "portfolio OOS diagnostics cannot authorize tuning"
            )
        return self


class PortfolioValidationExperiment(BaseModel):
    experiment_id: UUID
    state: OperatorJobState
    validator_id: str
    request: PortfolioWalkForwardJobRequest
    requested_by: str
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    as_of: datetime | None = None
    result_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    summary: PortfolioValidationSummary | None = None
    error_code: str | None = None
    live_trading_locked: bool = True

    @field_validator("created_at", "started_at", "completed_at", "as_of")
    @classmethod
    def require_aware_portfolio_validation_time(
        cls,
        value: datetime | None,
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(
                "portfolio validation timestamps must be timezone-aware"
            )
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def require_portfolio_research_lock(self) -> Self:
        if not self.live_trading_locked:
            raise ValueError(
                "portfolio validation cannot unlock live trading"
            )
        return self


class PortfolioValidationFoldView(BaseModel):
    sequence: int = Field(ge=1)
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    selected: MomentumCandidateRequest
    selection_score: Decimal
    training: BacktestMetrics
    test: BacktestMetrics
    benchmark: BacktestMetrics
    training_result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    test_result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    benchmark_result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    fold_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class PortfolioValidationExperimentDetail(BaseModel):
    experiment: PortfolioValidationExperiment
    folds: tuple[PortfolioValidationFoldView, ...]
    diagnostics: PortfolioValidationDiagnosticsView | None = None


class ValidationCampaignComponentView(BaseModel):
    sequence: int = Field(ge=1)
    instrument: str
    experiment_id: UUID
    state: OperatorJobState
    evidence_status: str | None = None
    gate_failures: tuple[str, ...] = ()
    result_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )


class ValidationCampaignView(BaseModel):
    campaign_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    campaign_key: str
    manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    instruments: tuple[str, ...] = Field(min_length=3, max_length=20)
    created_at: datetime
    status: str
    components: tuple[ValidationCampaignComponentView, ...]
    live_trading_locked: bool = True

    @field_validator("created_at")
    @classmethod
    def require_aware_campaign_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("campaign creation time must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def require_campaign_components(self) -> Self:
        if (
            len(self.components) != len(self.instruments)
            or tuple(value.sequence for value in self.components)
            != tuple(range(1, len(self.components) + 1))
            or tuple(value.instrument for value in self.components)
            != self.instruments
            or not self.live_trading_locked
        ):
            raise ValueError("validation campaign view is inconsistent")
        return self


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
    kill_switch_active: bool
    kill_switch_reason: str
    kill_switch_version: int = Field(ge=0)
    simulated_broker_available: bool
    simulated_broker_recovery_verified: bool
    simulated_broker_order_count: int = Field(ge=0)
    simulated_broker_fact_count: int = Field(ge=0)
    scheduler_evidence_available: bool = False
    scheduler_recovery_verified: bool = False
    scheduler_cycle_count: int = Field(default=0, ge=0)
    latest_scheduler_at: datetime | None = None
    remaining_gates: tuple[str, ...]

    @field_validator("latest_reconciliation_at", "latest_scheduler_at")
    @classmethod
    def require_aware_reconciliation_time(
        cls, value: datetime | None
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("reconciliation time must be timezone-aware")
        return value.astimezone(UTC)


class QmtReadOnlyStatus(BaseModel):
    status: str
    live_trading_locked: bool = True
    current_host_read_only_ready: bool
    checks: dict[str, str]
    latest_evidence_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    latest_observed_at: datetime | None = None
    evidence_age_seconds: int | None = Field(default=None, ge=0)
    evidence_fresh: bool
    position_count: int | None = Field(default=None, ge=0)
    order_count: int | None = Field(default=None, ge=0)
    trade_count: int | None = Field(default=None, ge=0)
    remaining_gates: tuple[str, ...]

    @field_validator("latest_observed_at")
    @classmethod
    def require_aware_qmt_evidence_time(
        cls,
        value: datetime | None,
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("QMT evidence time must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def require_consistent_qmt_evidence(self) -> Self:
        details = (
            self.latest_evidence_hash,
            self.latest_observed_at,
            self.evidence_age_seconds,
            self.position_count,
            self.order_count,
            self.trade_count,
        )
        if any(value is None for value in details) != all(
            value is None for value in details
        ):
            raise ValueError("QMT evidence summary is incomplete")
        if self.evidence_fresh and self.latest_evidence_hash is None:
            raise ValueError("fresh QMT evidence requires a persisted artifact")
        if self.status not in {"accepted", "blocked", "stale"}:
            raise ValueError("QMT read-only status is invalid")
        return self


class PromotionGateView(BaseModel):
    status: str
    actual: str
    required: str

    @model_validator(mode="after")
    def require_known_status(self) -> Self:
        if self.status not in {"pass", "blocked"}:
            raise ValueError("promotion gate status is invalid")
        return self


class PaperPromotionStatus(BaseModel):
    status: str
    live_trading_ready: bool = False
    evidence_gates_passed: bool = False
    evaluated_at: datetime | None = None
    policy_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    fact_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    report_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    blockers: tuple[str, ...]
    gates: dict[str, PromotionGateView]

    @field_validator("evaluated_at")
    @classmethod
    def require_aware_promotion_time(
        cls,
        value: datetime | None,
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("promotion audit time must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def enforce_live_lock_and_complete_evidence(self) -> Self:
        if self.live_trading_ready:
            raise ValueError("operator console cannot mark live trading ready")
        hashes = (self.policy_hash, self.fact_hash, self.report_hash)
        if any(value is None for value in hashes) != all(
            value is None for value in hashes
        ):
            raise ValueError("promotion audit hashes are incomplete")
        if self.status not in {"blocked", "unavailable"}:
            raise ValueError("promotion audit status is invalid")
        return self


class PaperStrategyComponentStatus(BaseModel):
    experiment_id: UUID
    instrument: str
    fast_sessions: int = Field(ge=2)
    slow_sessions: int = Field(ge=3)
    allocation: Decimal = Field(gt=0, le=1)
    validation_result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    signal_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_windows(self) -> Self:
        if self.fast_sessions >= self.slow_sessions:
            raise ValueError("paper component windows are invalid")
        return self


class PaperPortfolioOosStatus(BaseModel):
    assessment_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    fold_count: int = Field(ge=1)
    compounded_return: Decimal
    profitable_fold_rate: Decimal = Field(ge=0, le=1)
    maximum_drawdown: Decimal = Field(ge=0, le=1)
    maximum_pairwise_correlation: Decimal | None = Field(
        default=None,
        ge=-1,
        le=1,
    )
    maximum_component_contribution: Decimal = Field(ge=0, le=1)


class PaperStrategyStatus(BaseModel):
    status: str
    active: bool
    live_trading_locked: bool = True
    account_id: str
    strategy_id: str
    deployment_kind: str | None = None
    registration_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    strategy_version: str | None = None
    experiment_id: UUID | None = None
    instrument: str | None = None
    fast_sessions: int | None = Field(default=None, ge=2)
    slow_sessions: int | None = Field(default=None, ge=3)
    allocation: Decimal | None = None
    validation_result_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    signal_manifest_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    approved_by: str | None = None
    approved_at: datetime | None = None
    instruments: tuple[str, ...] = ()
    components: tuple[PaperStrategyComponentStatus, ...] = ()
    total_allocation: Decimal | None = Field(default=None, gt=0, le=1)
    valuation_manifest_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    portfolio_oos: PaperPortfolioOosStatus | None = None
    remaining_gates: tuple[str, ...]

    @field_validator("approved_at")
    @classmethod
    def require_aware_strategy_time(
        cls,
        value: datetime | None,
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("strategy approval time must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def require_consistent_strategy_state(self) -> Self:
        common_details = (
            self.registration_hash,
            self.strategy_version,
            self.approved_by,
            self.approved_at,
        )
        if self.active != all(
            value is not None for value in common_details
        ):
            raise ValueError("paper strategy status details do not match active state")
        if self.active and self.status != "approved":
            raise ValueError("active paper strategy status must be approved")
        if not self.active and self.status != "inactive":
            raise ValueError("inactive paper strategy status must be inactive")
        if not self.active and (
            self.deployment_kind is not None
            or self.instruments
            or self.components
            or self.total_allocation is not None
            or self.valuation_manifest_hash is not None
            or self.portfolio_oos is not None
        ):
            raise ValueError("inactive paper strategy cannot contain deployment data")
        if self.active and self.deployment_kind == "portfolio":
            if (
                len(self.instruments) < 3
                or len(self.components) < 3
                or tuple(
                    sorted(value.instrument for value in self.components)
                )
                != tuple(sorted(self.instruments))
                or self.total_allocation is None
                or self.valuation_manifest_hash is None
                or self.portfolio_oos is None
                or any(
                    value is not None
                    for value in (
                        self.experiment_id,
                        self.instrument,
                        self.fast_sessions,
                        self.slow_sessions,
                        self.allocation,
                        self.validation_result_hash,
                        self.signal_manifest_hash,
                    )
                )
            ):
                raise ValueError(
                    "paper portfolio status details are inconsistent"
                )
        if self.active and self.deployment_kind != "portfolio":
            single_details = (
                self.experiment_id,
                self.instrument,
                self.fast_sessions,
                self.slow_sessions,
                self.allocation,
                self.validation_result_hash,
                self.signal_manifest_hash,
            )
            if not all(value is not None for value in single_details):
                raise ValueError(
                    "single paper strategy status details are incomplete"
                )
            if self.portfolio_oos is not None:
                raise ValueError(
                    "single paper strategy cannot contain portfolio OOS data"
                )
        if (
            self.fast_sessions is not None
            and self.slow_sessions is not None
            and self.fast_sessions >= self.slow_sessions
        ):
            raise ValueError("paper strategy windows are invalid")
        return self


class KillSwitchActivationRequest(BaseModel):
    command_id: str
    reason: str

    @field_validator("command_id")
    @classmethod
    def validate_command_id(cls, value: str) -> str:
        normalized = value.strip()
        if _IDEMPOTENCY_KEY.fullmatch(normalized) is None:
            raise ValueError("command_id must be 16-128 safe characters")
        return normalized

    @field_validator("reason")
    @classmethod
    def validate_activation_reason(cls, value: str) -> str:
        normalized = value.strip().casefold()
        if normalized not in {"manual", "drill"}:
            raise ValueError("operator activation reason must be manual or drill")
        return normalized
