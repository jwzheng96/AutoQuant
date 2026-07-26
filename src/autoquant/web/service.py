from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Protocol
from uuid import UUID, uuid4

from autoquant.adapters.clickhouse_daily import ClickHouseDailyRepository
from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.backtest.fundamental_portfolio import (
    FUNDAMENTAL_PORTFOLIO_STRATEGY_ID,
)
from autoquant.backtest.low_volatility_portfolio import (
    LOW_VOLATILITY_STRATEGY_ID,
)
from autoquant.backtest.models import (
    BacktestResult,
    ExecutionState,
    backtest_artifact_hash,
)
from autoquant.backtest.portfolio_validation import (
    PortfolioWalkForwardConfig,
    PortfolioWalkForwardResult,
)
from autoquant.backtest.validation import WalkForwardConfig, WalkForwardResult
from autoquant.clock import to_shanghai
from autoquant.config import AppSettings
from autoquant.data.daily_availability import (
    completed_daily_session_availability,
)
from autoquant.errors import AutoQuantError, PersistenceUnavailableError
from autoquant.execution.control import KillSwitchReason
from autoquant.execution.control_store import PostgresExecutionControlRepository
from autoquant.execution.low_volatility_paper_deployment import (
    PostgresLowVolatilityPaperDeploymentReader,
)
from autoquant.execution.paper_deployment import (
    PostgresPaperDeploymentRegistry,
)
from autoquant.execution.paper_scheduler_store import PostgresPaperSchedulerRepository
from autoquant.execution.promotion_audit import (
    PaperPromotionAuditor,
    PaperPromotionPolicy,
    PostgresPaperPromotionFactRepository,
)
from autoquant.execution.qmt_preflight import inspect_qmt_readiness
from autoquant.execution.qmt_readonly_store import (
    PostgresQmtReadOnlyAcceptanceRepository,
)
from autoquant.execution.qmt_session_store import (
    PostgresQmtSessionLeaseRepository,
)
from autoquant.execution.simulated_broker import PersistentSimulatedBroker
from autoquant.execution.store import PostgresPaperExecutionRepository
from autoquant.execution.validated_sma_portfolio import (
    ValidatedSmaPortfolioRegistration,
)
from autoquant.operations import run_daily_ingestion
from autoquant.web.backtest_store import PostgresBacktestRepository
from autoquant.web.fundamental_validation_store import (
    FundamentalValidationIndexRecord,
    FundamentalValidationRecord,
    PostgresFundamentalValidationRepository,
)
from autoquant.web.low_volatility_execution_compatibility_run_store import (
    PostgresLowVolatilityExecutionCompatibilityRunRepository,
)
from autoquant.web.low_volatility_execution_compatibility_store import (
    PostgresLowVolatilityExecutionCompatibilityRepository,
)
from autoquant.web.low_volatility_forward_evaluation_store import (
    PostgresLowVolatilityForwardEvaluationRepository,
)
from autoquant.web.low_volatility_forward_session_store import (
    PostgresLowVolatilityForwardSessionRepository,
)
from autoquant.web.low_volatility_forward_store import (
    PostgresLowVolatilityForwardEvidenceSpecRepository,
)
from autoquant.web.low_volatility_validation_store import (
    LowVolatilityValidationRecord,
    PostgresLowVolatilityValidationRepository,
)
from autoquant.web.models import (
    BacktestRun,
    BacktestRunDetail,
    BacktestRunRequest,
    DailyIngestionJobRequest,
    DataCoverage,
    FundamentalValidationDetailView,
    FundamentalValidationFoldView,
    FundamentalValidationListItemView,
    FundamentalValidationPhaseView,
    FundamentalValidationSummaryView,
    LowVolatilityForwardProgressView,
    LowVolatilityForwardSessionView,
    LowVolatilityValidationDetailView,
    LowVolatilityValidationFoldView,
    LowVolatilityValidationListItemView,
    LowVolatilityValidationPhaseView,
    OperatorJob,
    OperatorOverview,
    PaperExecutionStatus,
    PaperPortfolioOosStatus,
    PaperPromotionStatus,
    PaperStrategyComponentStatus,
    PaperStrategyStatus,
    PortfolioValidationExperiment,
    PortfolioValidationExperimentDetail,
    PortfolioWalkForwardJobRequest,
    PromotionGateView,
    QmtBrokerOrderView,
    QmtBrokerTradeView,
    QmtOperationsStatus,
    QmtReadOnlyStatus,
    ResearchManifest,
    ResearchUniverseSnapshotDetail,
    ResearchUniverseSnapshotView,
    RiskControlStatus,
    ValidationCampaignComponentView,
    ValidationCampaignView,
    ValidationExperiment,
    ValidationExperimentDetail,
    WalkForwardJobRequest,
)
from autoquant.web.portfolio_validation_store import (
    PostgresPortfolioValidationRepository,
    portfolio_validation_config,
)
from autoquant.web.qmt_operations_store import (
    PostgresQmtOperationsRepository,
)
from autoquant.web.risk_store import PostgresRiskDecisionRepository
from autoquant.web.store import PostgresOperatorRepository
from autoquant.web.universe_store import (
    PostgresResearchUniverseRepository,
)
from autoquant.web.validation_campaign_store import (
    PostgresValidationCampaignRepository,
    ValidationCampaignStatus,
)
from autoquant.web.validation_store import (
    PostgresValidationRepository,
    validation_config,
)

IngestionRunner = Callable[[AppSettings, tuple[str, ...], date, date], Awaitable[dict[str, object]]]
_QMT_ACCEPTANCE_MAX_AGE = timedelta(hours=24)


class BacktestRunnerPort(Protocol):
    async def run(self, request: BacktestRunRequest) -> BacktestResult: ...


class WalkForwardRunnerPort(Protocol):
    async def run(
        self,
        *,
        manifest_hash: str,
        instrument: str,
        config: WalkForwardConfig,
    ) -> WalkForwardResult: ...


class PortfolioWalkForwardRunnerPort(Protocol):
    async def run(
        self,
        *,
        manifest_hash: str,
        config: PortfolioWalkForwardConfig,
    ) -> PortfolioWalkForwardResult: ...


class ConsoleServicePort(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def overview(self) -> OperatorOverview: ...

    async def list_jobs(self, *, limit: int = 50) -> tuple[OperatorJob, ...]: ...

    async def create_daily_job(
        self, request: DailyIngestionJobRequest, *, requested_by: str
    ) -> OperatorJob: ...

    async def list_backtests(self, *, limit: int = 50) -> tuple[BacktestRun, ...]: ...

    async def list_research_manifests(
        self, *, limit: int = 100
    ) -> tuple[ResearchManifest, ...]: ...

    async def list_research_universes(
        self, *, limit: int = 50
    ) -> tuple[ResearchUniverseSnapshotView, ...]: ...

    async def research_universe_detail(
        self, snapshot_hash: str
    ) -> ResearchUniverseSnapshotDetail: ...

    async def create_backtest(
        self, request: BacktestRunRequest, *, requested_by: str
    ) -> BacktestRun: ...

    async def backtest_detail(self, run_id: UUID) -> BacktestRunDetail: ...

    async def list_validations(self, *, limit: int = 50) -> tuple[ValidationExperiment, ...]: ...

    async def create_validation(
        self, request: WalkForwardJobRequest, *, requested_by: str
    ) -> ValidationExperiment: ...

    async def validation_detail(self, experiment_id: UUID) -> ValidationExperimentDetail: ...

    async def list_portfolio_validations(
        self, *, limit: int = 50
    ) -> tuple[PortfolioValidationExperiment, ...]: ...

    async def create_portfolio_validation(
        self, request: PortfolioWalkForwardJobRequest, *, requested_by: str
    ) -> PortfolioValidationExperiment: ...

    async def portfolio_validation_detail(
        self, experiment_id: UUID
    ) -> PortfolioValidationExperimentDetail: ...

    async def list_validation_campaigns(
        self, *, limit: int = 50
    ) -> tuple[ValidationCampaignView, ...]: ...

    async def list_fundamental_validations(
        self, *, limit: int = 20
    ) -> tuple[FundamentalValidationListItemView, ...]: ...

    async def fundamental_validation_detail(
        self, result_hash: str
    ) -> FundamentalValidationDetailView: ...

    async def list_low_volatility_validations(
        self, *, limit: int = 20
    ) -> tuple[LowVolatilityValidationListItemView, ...]: ...

    async def low_volatility_validation_detail(
        self, result_hash: str
    ) -> LowVolatilityValidationDetailView: ...

    async def low_volatility_forward_progress(
        self,
    ) -> LowVolatilityForwardProgressView: ...

    async def risk_status(self) -> RiskControlStatus: ...

    async def execution_status(self) -> PaperExecutionStatus: ...

    async def paper_strategy_status(self) -> PaperStrategyStatus: ...

    async def qmt_readonly_status(self) -> QmtReadOnlyStatus: ...

    async def qmt_operations_status(self) -> QmtOperationsStatus: ...

    async def promotion_status(self) -> PaperPromotionStatus: ...

    async def activate_kill_switch(
        self, *, command_id: str, reason: str, requested_by: str
    ) -> PaperExecutionStatus: ...

    async def bars(
        self,
        *,
        instrument: str,
        start: date,
        end: date,
        as_of: datetime,
    ) -> tuple[dict[str, object], ...]: ...


class ConsoleService:
    def __init__(
        self,
        *,
        settings: AppSettings,
        operator_repository: PostgresOperatorRepository,
        control_repository: PostgresControlRepository,
        market_repository: ClickHouseDailyRepository,
        backtest_repository: PostgresBacktestRepository | None = None,
        backtest_runner: BacktestRunnerPort | None = None,
        validation_repository: PostgresValidationRepository | None = None,
        validation_runner: WalkForwardRunnerPort | None = None,
        portfolio_validation_repository: (PostgresPortfolioValidationRepository | None) = None,
        portfolio_validation_runner: (PortfolioWalkForwardRunnerPort | None) = None,
        validation_campaign_repository: (PostgresValidationCampaignRepository | None) = None,
        fundamental_validation_repository: (PostgresFundamentalValidationRepository | None) = None,
        low_volatility_validation_repository: (
            PostgresLowVolatilityValidationRepository | None
        ) = None,
        low_volatility_forward_spec_repository: (
            PostgresLowVolatilityForwardEvidenceSpecRepository | None
        ) = None,
        low_volatility_forward_session_repository: (
            PostgresLowVolatilityForwardSessionRepository | None
        ) = None,
        low_volatility_forward_evaluation_repository: (
            PostgresLowVolatilityForwardEvaluationRepository | None
        ) = None,
        low_volatility_compatibility_spec_repository: (
            PostgresLowVolatilityExecutionCompatibilityRepository | None
        ) = None,
        low_volatility_compatibility_run_repository: (
            PostgresLowVolatilityExecutionCompatibilityRunRepository | None
        ) = None,
        low_volatility_deployment_reader: (
            PostgresLowVolatilityPaperDeploymentReader | None
        ) = None,
        universe_repository: (PostgresResearchUniverseRepository | None) = None,
        risk_repository: PostgresRiskDecisionRepository | None = None,
        execution_repository: PostgresPaperExecutionRepository | None = None,
        execution_control_repository: PostgresExecutionControlRepository | None = None,
        simulated_broker: PersistentSimulatedBroker | None = None,
        scheduler_repository: PostgresPaperSchedulerRepository | None = None,
        strategy_registry: PostgresPaperDeploymentRegistry | None = None,
        qmt_acceptance_repository: (PostgresQmtReadOnlyAcceptanceRepository | None) = None,
        qmt_session_repository: PostgresQmtSessionLeaseRepository | None = None,
        qmt_operations_repository: PostgresQmtOperationsRepository | None = None,
        promotion_repository: (PostgresPaperPromotionFactRepository | None) = None,
        ingestion_runner: IngestionRunner = run_daily_ingestion,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        poll_interval: float = 1.0,
    ) -> None:
        self._settings = settings
        self._operators = operator_repository
        self._control = control_repository
        self._market = market_repository
        if (backtest_repository is None) != (backtest_runner is None):
            raise ValueError("backtest repository and runner must be configured together")
        self._backtests = backtest_repository
        self._backtest_runner = backtest_runner
        if (validation_repository is None) != (validation_runner is None):
            raise ValueError("validation repository and runner must be configured together")
        self._validations = validation_repository
        self._validation_runner = validation_runner
        if (portfolio_validation_repository is None) != (portfolio_validation_runner is None):
            raise ValueError(
                "portfolio validation repository and runner must be configured together"
            )
        self._portfolio_validations = portfolio_validation_repository
        self._portfolio_validation_runner = portfolio_validation_runner
        self._validation_campaigns = validation_campaign_repository
        self._fundamental_validations = fundamental_validation_repository
        self._low_volatility_validations = low_volatility_validation_repository
        if (low_volatility_forward_spec_repository is None) != (
            low_volatility_forward_session_repository is None
        ):
            raise ValueError(
                "forward evidence and session repositories must be configured together"
            )
        self._low_volatility_forward_specs = low_volatility_forward_spec_repository
        self._low_volatility_forward_sessions = low_volatility_forward_session_repository
        self._low_volatility_forward_evaluations = low_volatility_forward_evaluation_repository
        compatibility_dependencies = (
            low_volatility_compatibility_spec_repository,
            low_volatility_compatibility_run_repository,
            low_volatility_deployment_reader,
        )
        if any(value is not None for value in compatibility_dependencies) and not all(
            value is not None for value in compatibility_dependencies
        ):
            raise ValueError("compatibility and deployment readers must be configured together")
        self._low_volatility_compatibility_specs = low_volatility_compatibility_spec_repository
        self._low_volatility_compatibility_runs = low_volatility_compatibility_run_repository
        self._low_volatility_deployment = low_volatility_deployment_reader
        self._universes = universe_repository
        self._risk = risk_repository
        self._execution = execution_repository
        self._execution_controls = execution_control_repository
        self._simulated_broker = simulated_broker
        self._scheduler = scheduler_repository
        self._strategy_registry = strategy_registry
        if (qmt_acceptance_repository is None) != (qmt_session_repository is None):
            raise ValueError("QMT acceptance and session repositories must be configured together")
        self._qmt_acceptances = qmt_acceptance_repository
        self._qmt_sessions = qmt_session_repository
        self._qmt_operations = qmt_operations_repository
        self._promotion = promotion_repository
        self._ingestion_runner = ingestion_runner
        self._now = now
        self._poll_interval = poll_interval
        self._wake = asyncio.Event()
        self._worker: asyncio.Task[None] | None = None
        self._backtest_wake = asyncio.Event()
        self._backtest_worker: asyncio.Task[None] | None = None
        self._validation_wake = asyncio.Event()
        self._validation_worker: asyncio.Task[None] | None = None
        self._portfolio_validation_wake = asyncio.Event()
        self._portfolio_validation_worker: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._execution_controls is not None:
            await self._execution_controls.ensure_fail_closed(
                account_id=self._settings.paper_account_id,
                now=self._now(),
            )
        if self._execution is not None:
            try:
                await self._execution.verify_recovery()
            except Exception:
                if self._execution_controls is not None:
                    await self._execution_controls.activate(
                        account_id=self._settings.paper_account_id,
                        command_id=f"startup-recovery-{uuid4()}",
                        reason=KillSwitchReason.RECOVERY_FAILED,
                        actor="console-startup",
                        now=self._now(),
                    )
                raise
        if self._simulated_broker is not None:
            try:
                await self._simulated_broker.verify_recovery()
            except Exception:
                if self._execution_controls is not None:
                    await self._execution_controls.activate(
                        account_id=self._settings.paper_account_id,
                        command_id=f"broker-recovery-{uuid4()}",
                        reason=KillSwitchReason.RECOVERY_FAILED,
                        actor="console-startup",
                        now=self._now(),
                    )
                raise
        if self._scheduler is not None:
            try:
                await self._scheduler.replay(account_id=self._settings.paper_account_id)
            except Exception:
                if self._execution_controls is not None:
                    await self._execution_controls.activate(
                        account_id=self._settings.paper_account_id,
                        command_id=f"scheduler-recovery-{uuid4()}",
                        reason=KillSwitchReason.RECOVERY_FAILED,
                        actor="console-startup",
                        now=self._now(),
                    )
                raise
        if self._strategy_registry is not None:
            try:
                await self._strategy_registry.active(
                    account_id=self._settings.paper_account_id,
                    strategy_id=self._settings.paper_strategy_id,
                )
            except Exception:
                if self._execution_controls is not None:
                    await self._execution_controls.activate(
                        account_id=self._settings.paper_account_id,
                        command_id=f"strategy-registry-recovery-{uuid4()}",
                        reason=KillSwitchReason.RECOVERY_FAILED,
                        actor="console-startup",
                        now=self._now(),
                    )
                raise
        if self._qmt_acceptances is not None:
            try:
                await self._qmt_acceptances.check_connection()
                await self._qmt_acceptances.latest(
                    logical_account_id=self._settings.paper_account_id
                )
                if self._qmt_sessions is None:
                    raise PersistenceUnavailableError("QMT session repository is unavailable")
                await self._qmt_sessions.active_session_ids(now=self._now())
            except Exception:
                if self._execution_controls is not None:
                    await self._execution_controls.activate(
                        account_id=self._settings.paper_account_id,
                        command_id=f"qmt-acceptance-recovery-{uuid4()}",
                        reason=KillSwitchReason.RECOVERY_FAILED,
                        actor="console-startup",
                        now=self._now(),
                    )
                raise
        if self._qmt_operations is not None:
            try:
                await self._qmt_operations.check_connection()
                await self._qmt_operations.snapshot(account_id=self._settings.paper_account_id)
            except Exception:
                if self._execution_controls is not None:
                    await self._execution_controls.activate(
                        account_id=self._settings.paper_account_id,
                        command_id=f"qmt-operations-recovery-{uuid4()}",
                        reason=KillSwitchReason.RECOVERY_FAILED,
                        actor="console-startup",
                        now=self._now(),
                    )
                raise
        if self._execution_controls is not None:
            await self._execution_controls.replay(account_id=self._settings.paper_account_id)
        await self._operators.interrupt_running_jobs(now=self._now())
        if self._worker is None:
            self._worker = asyncio.create_task(self._work_loop(), name="operator-job-worker")
        if self._backtests is not None:
            await self._backtests.interrupt_running_runs(now=self._now())
            if self._backtest_worker is None:
                self._backtest_worker = asyncio.create_task(
                    self._backtest_work_loop(), name="backtest-run-worker"
                )
        if self._validations is not None:
            await self._validations.interrupt_running_experiments(now=self._now())
            if self._validation_worker is None:
                self._validation_worker = asyncio.create_task(
                    self._validation_work_loop(), name="validation-experiment-worker"
                )
        if self._portfolio_validations is not None:
            await self._portfolio_validations.interrupt_running_experiments(now=self._now())
            if self._portfolio_validation_worker is None:
                self._portfolio_validation_worker = asyncio.create_task(
                    self._portfolio_validation_work_loop(),
                    name="portfolio-validation-experiment-worker",
                )

    async def stop(self) -> None:
        worker = self._worker
        self._worker = None
        if worker is not None:
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
        backtest_worker = self._backtest_worker
        self._backtest_worker = None
        if backtest_worker is not None:
            backtest_worker.cancel()
            try:
                await backtest_worker
            except asyncio.CancelledError:
                pass
        if self._backtests is not None:
            await self._backtests.close()
        validation_worker = self._validation_worker
        self._validation_worker = None
        if validation_worker is not None:
            validation_worker.cancel()
            try:
                await validation_worker
            except asyncio.CancelledError:
                pass
        if self._validations is not None:
            await self._validations.close()
        portfolio_validation_worker = self._portfolio_validation_worker
        self._portfolio_validation_worker = None
        if portfolio_validation_worker is not None:
            portfolio_validation_worker.cancel()
            try:
                await portfolio_validation_worker
            except asyncio.CancelledError:
                pass
        if self._portfolio_validations is not None:
            await self._portfolio_validations.close()
        if self._validation_campaigns is not None:
            await self._validation_campaigns.close()
        if self._fundamental_validations is not None:
            await self._fundamental_validations.close()
        if self._low_volatility_validations is not None:
            await self._low_volatility_validations.close()
        if self._low_volatility_forward_sessions is not None:
            await self._low_volatility_forward_sessions.close()
        if self._low_volatility_forward_evaluations is not None:
            await self._low_volatility_forward_evaluations.close()
        if self._low_volatility_forward_specs is not None:
            await self._low_volatility_forward_specs.close()
        if self._low_volatility_deployment is not None:
            await self._low_volatility_deployment.close()
        if self._low_volatility_compatibility_runs is not None:
            await self._low_volatility_compatibility_runs.close()
        if self._low_volatility_compatibility_specs is not None:
            await self._low_volatility_compatibility_specs.close()
        if self._universes is not None:
            await self._universes.close()
        if self._risk is not None:
            await self._risk.close()
        if self._execution is not None:
            await self._execution.close()
        if self._execution_controls is not None:
            await self._execution_controls.close()
        if self._simulated_broker is not None:
            await self._simulated_broker.close()
        if self._scheduler is not None:
            await self._scheduler.close()
        if self._strategy_registry is not None:
            await self._strategy_registry.close()
        if self._qmt_acceptances is not None:
            await self._qmt_acceptances.close()
        if self._qmt_sessions is not None:
            await self._qmt_sessions.close()
        if self._qmt_operations is not None:
            await self._qmt_operations.close()
        if self._promotion is not None:
            await self._promotion.close()
        await self._operators.close()
        await self._control.close()
        await self._market.client.close()

    async def overview(self) -> OperatorOverview:
        postgres_status = "ok"
        clickhouse_status = "ok"
        control = None
        coverage = None
        try:
            control = await self._operators.control_summary()
        except (AutoQuantError, ValueError):
            postgres_status = "unavailable"
        try:
            await self._market.check_connection()
            coverage = await self._coverage()
        except (AutoQuantError, ValueError):
            clickhouse_status = "unavailable"
        statuses = (postgres_status, clickhouse_status)
        token = self._settings.tushare_token
        tushare_status = (
            "configured"
            if token is not None and bool(token.get_secret_value().strip())
            else "missing"
        )
        return OperatorOverview(
            status="ok" if all(status == "ok" for status in statuses) else "degraded",
            postgres=postgres_status,
            clickhouse=clickhouse_status,
            tushare=tushare_status,
            control=control,
            coverage=coverage,
            generated_at=self._now(),
        )

    async def list_jobs(self, *, limit: int = 50) -> tuple[OperatorJob, ...]:
        return await self._operators.list_jobs(limit=limit)

    async def create_daily_job(
        self, request: DailyIngestionJobRequest, *, requested_by: str
    ) -> OperatorJob:
        self._settings.require_tushare()
        now = self._now()
        job = await self._operators.create_job(request, requested_by=requested_by, now=now)
        try:
            await self._audit(
                "operator.daily_ingestion.requested",
                job.job_id,
                {
                    "instruments": list(request.instruments),
                    "start": request.start.isoformat(),
                    "end": request.end.isoformat(),
                    "requested_by": requested_by,
                },
            )
        except AutoQuantError:
            await self._operators.reject_queued_job(
                job.job_id, error_code="audit_unavailable", now=self._now()
            )
            raise PersistenceUnavailableError("Operator audit is unavailable") from None
        self._wake.set()
        return job

    async def list_backtests(self, *, limit: int = 50) -> tuple[BacktestRun, ...]:
        repository, _ = self._require_backtests()
        return await repository.list_runs(limit=limit)

    async def list_research_manifests(self, *, limit: int = 100) -> tuple[ResearchManifest, ...]:
        repository, _ = self._require_backtests()
        return await repository.list_manifests(limit=limit)

    async def list_research_universes(
        self,
        *,
        limit: int = 50,
    ) -> tuple[ResearchUniverseSnapshotView, ...]:
        if self._universes is None:
            raise PersistenceUnavailableError("Research universe service is unavailable")
        return await self._universes.list(limit=limit)

    async def research_universe_detail(
        self,
        snapshot_hash: str,
    ) -> ResearchUniverseSnapshotDetail:
        if self._universes is None:
            raise PersistenceUnavailableError("Research universe service is unavailable")
        return await self._universes.detail(snapshot_hash)

    async def create_backtest(
        self, request: BacktestRunRequest, *, requested_by: str
    ) -> BacktestRun:
        repository, _ = self._require_backtests()
        manifest = await self._control.read_manifest(request.manifest_hash)
        if not manifest.production_complete:
            raise ValueError("backtests require a production-complete manifest")
        if request.instrument not in manifest.instruments:
            raise ValueError("instrument is not present in the selected manifest")
        run = await repository.create_run(request, requested_by=requested_by, now=self._now())
        try:
            await self._audit(
                "operator.backtest.requested",
                run.run_id,
                {
                    "manifest_hash": request.manifest_hash,
                    "instrument": request.instrument,
                    "strategy_id": run.strategy_id,
                    "requested_by": requested_by,
                },
            )
        except AutoQuantError:
            await repository.fail_run(
                run.run_id,
                error_code="audit_unavailable",
                now=self._now(),
                queued=True,
            )
            raise PersistenceUnavailableError("Operator audit is unavailable") from None
        self._backtest_wake.set()
        return run

    async def backtest_detail(self, run_id: UUID) -> BacktestRunDetail:
        repository, _ = self._require_backtests()
        return await repository.detail(run_id)

    async def list_validations(self, *, limit: int = 50) -> tuple[ValidationExperiment, ...]:
        repository, _ = self._require_validations()
        return await repository.list_experiments(limit=limit)

    async def create_validation(
        self, request: WalkForwardJobRequest, *, requested_by: str
    ) -> ValidationExperiment:
        repository, _ = self._require_validations()
        manifest = await self._control.read_manifest(request.manifest_hash)
        if not manifest.production_complete:
            raise ValueError("validation requires a production-complete manifest")
        if request.instrument not in manifest.instruments:
            raise ValueError("instrument is not present in the selected manifest")
        experiment = await repository.create_experiment(
            request, requested_by=requested_by, now=self._now()
        )
        try:
            await self._audit(
                "operator.validation.requested",
                experiment.experiment_id,
                {
                    "manifest_hash": request.manifest_hash,
                    "instrument": request.instrument,
                    "validator_id": experiment.validator_id,
                    "requested_by": requested_by,
                },
            )
        except AutoQuantError:
            await repository.fail_experiment(
                experiment.experiment_id,
                error_code="audit_unavailable",
                now=self._now(),
                queued=True,
            )
            raise PersistenceUnavailableError("Operator audit is unavailable") from None
        self._validation_wake.set()
        return experiment

    async def validation_detail(self, experiment_id: UUID) -> ValidationExperimentDetail:
        repository, _ = self._require_validations()
        return await repository.detail(experiment_id)

    async def list_portfolio_validations(
        self,
        *,
        limit: int = 50,
    ) -> tuple[PortfolioValidationExperiment, ...]:
        repository, _ = self._require_portfolio_validations()
        return await repository.list_experiments(limit=limit)

    async def create_portfolio_validation(
        self,
        request: PortfolioWalkForwardJobRequest,
        *,
        requested_by: str,
    ) -> PortfolioValidationExperiment:
        repository, _ = self._require_portfolio_validations()
        manifest = await self._control.read_manifest(request.manifest_hash)
        if not manifest.production_complete:
            raise ValueError("portfolio validation requires a production-complete manifest")
        if len(manifest.instruments) < 3:
            raise ValueError("portfolio validation requires at least three instruments")
        if any(
            candidate.selection_count > len(manifest.instruments)
            for candidate in request.candidates
        ):
            raise ValueError(
                "portfolio candidate selects more instruments than the manifest contains"
            )
        experiment = await repository.create_experiment(
            request,
            requested_by=requested_by,
            now=self._now(),
        )
        try:
            await self._audit(
                "operator.portfolio_validation.requested",
                experiment.experiment_id,
                {
                    "manifest_hash": request.manifest_hash,
                    "instrument_count": len(manifest.instruments),
                    "validator_id": experiment.validator_id,
                    "requested_by": requested_by,
                },
            )
        except AutoQuantError:
            await repository.fail_experiment(
                experiment.experiment_id,
                error_code="audit_unavailable",
                now=self._now(),
                queued=True,
            )
            raise PersistenceUnavailableError("Operator audit is unavailable") from None
        self._portfolio_validation_wake.set()
        return experiment

    async def portfolio_validation_detail(
        self,
        experiment_id: UUID,
    ) -> PortfolioValidationExperimentDetail:
        repository, _ = self._require_portfolio_validations()
        return await repository.detail(experiment_id)

    async def list_validation_campaigns(
        self,
        *,
        limit: int = 50,
    ) -> tuple[ValidationCampaignView, ...]:
        if self._validation_campaigns is None:
            return ()
        values = await self._validation_campaigns.list_campaigns(limit=limit)
        return tuple(_validation_campaign_view(value) for value in values)

    async def list_fundamental_validations(
        self,
        *,
        limit: int = 20,
    ) -> tuple[FundamentalValidationListItemView, ...]:
        if self._fundamental_validations is None:
            return ()
        records = await self._fundamental_validations.list_recent(
            limit=limit,
        )
        return tuple(_fundamental_validation_list_item(value) for value in records)

    async def fundamental_validation_detail(
        self,
        result_hash: str,
    ) -> FundamentalValidationDetailView:
        if self._fundamental_validations is None:
            raise LookupError("fundamental validation service is unavailable")
        record = await self._fundamental_validations.read(result_hash)
        return FundamentalValidationDetailView(
            summary=_fundamental_validation_summary(record),
            folds=tuple(
                FundamentalValidationFoldView(
                    sequence=fold.sequence,
                    train_start=fold.train_start,
                    train_end=fold.train_end,
                    test_start=fold.test_start,
                    test_end=fold.test_end,
                    training=_fundamental_validation_phase(fold.training_result),
                    test=_fundamental_validation_phase(fold.test_result),
                    benchmark=_fundamental_validation_phase(fold.benchmark_result),
                    fold_hash=fold.fold_hash,
                )
                for fold in record.result.folds
            ),
        )

    async def list_low_volatility_validations(
        self,
        *,
        limit: int = 20,
    ) -> tuple[LowVolatilityValidationListItemView, ...]:
        if self._low_volatility_validations is None:
            return ()
        records = await self._low_volatility_validations.list_recent(
            limit=limit,
        )
        return tuple(_low_volatility_validation_summary(value) for value in records)

    async def low_volatility_validation_detail(
        self,
        result_hash: str,
    ) -> LowVolatilityValidationDetailView:
        if self._low_volatility_validations is None:
            raise LookupError("low-volatility validation service is unavailable")
        record = await self._low_volatility_validations.read(result_hash)
        return LowVolatilityValidationDetailView(
            summary=_low_volatility_validation_summary(record),
            folds=tuple(
                LowVolatilityValidationFoldView(
                    sequence=fold.sequence,
                    train_start=fold.train_start,
                    train_end=fold.train_end,
                    test_start=fold.test_start,
                    test_end=fold.test_end,
                    training=_low_volatility_validation_phase(fold.training_result),
                    test=_low_volatility_validation_phase(fold.test_result),
                    benchmark=_low_volatility_validation_phase(fold.benchmark_result),
                    fold_hash=fold.fold_hash,
                )
                for fold in record.result.folds
            ),
        )

    async def low_volatility_forward_progress(
        self,
    ) -> LowVolatilityForwardProgressView:
        if (
            self._low_volatility_forward_specs is None
            or self._low_volatility_forward_sessions is None
        ):
            raise LookupError("low-volatility forward progress is unavailable")
        specification = await self._low_volatility_forward_specs.latest()
        if specification is None:
            raise LookupError("low-volatility forward specification does not exist")
        spec = specification.spec
        records = await self._low_volatility_forward_sessions.list_for_spec(
            forward_spec_hash=spec.spec_hash,
        )
        evaluation = (
            None
            if self._low_volatility_forward_evaluations is None
            else await self._low_volatility_forward_evaluations.read_for_spec(spec.spec_hash)
        )
        now = self._now()
        local_date = to_shanghai(now).date()
        calendar = await self._market.query_sessions_as_of(
            spec.forward_start_date,
            max(
                spec.forward_start_date,
                local_date + timedelta(days=14),
            ),
            now,
        )
        availability = completed_daily_session_availability(calendar, now)
        open_dates = availability.eligible_open_dates
        historical_open_dates = tuple(
            session.session_date
            for session in calendar
            if session.is_open and session.session_date < local_date
        )
        safe_cutoff = open_dates[-1] if open_dates else spec.forward_start_date - timedelta(days=1)
        required_window = open_dates[: spec.minimum_forward_sessions]
        pending_required = availability.pending_open_dates[
            : max(spec.minimum_forward_sessions - len(required_window), 0)
        ]
        bound_dates = tuple(record.binding.session_date for record in records)
        bound_set = set(bound_dates)
        open_set = set(historical_open_dates)
        required_set = set(required_window)
        missing = tuple(value for value in required_window if value not in bound_set)
        conflicts = tuple(value for value in bound_dates if value not in open_set)
        completed_required = len(bound_set.intersection(required_set))
        if conflicts:
            status = "calendar_conflict"
        elif missing:
            status = "backfill_required"
        elif len(required_window) < (spec.minimum_forward_sessions):
            status = (
                "waiting_for_data_availability"
                if pending_required
                else "collecting_forward_sessions"
            )
        elif evaluation is not None:
            status = (
                "forward_evaluation_passed_awaiting_paper_approval"
                if evaluation.assessment.evidence_status == "paper_candidate"
                else "forward_evaluation_rejected"
            )
        else:
            status = "session_gate_complete_awaiting_evaluation"
        compatibility_spec_hash: str | None = None
        compatibility_run_hash: str | None = None
        compatibility_status = "not_configured"
        compatibility_gate_failures: tuple[str, ...] = ()
        execution_timing_compatible = False
        candidate_approval_hash: str | None = None
        deployment_contract_hash: str | None = None
        deployment_contract_status = "not_configured"
        daily_signal_hash: str | None = None
        decision_signal_status = "not_configured"
        deployment_blockers: tuple[str, ...] = ("deployment_gate_unavailable",)
        if (
            self._low_volatility_compatibility_specs is not None
            and self._low_volatility_compatibility_runs is not None
            and self._low_volatility_deployment is not None
        ):
            try:
                compatibility_spec = (
                    await self._low_volatility_compatibility_specs.for_forward_spec(spec.spec_hash)
                )
            except LookupError:
                compatibility_status = "not_preregistered"
            else:
                compatibility_spec_hash = compatibility_spec.spec_hash
                try:
                    compatibility_run = await self._low_volatility_compatibility_runs.for_spec(
                        compatibility_spec.spec_hash
                    )
                except LookupError:
                    compatibility_status = "awaiting_terminal_evaluation"
                else:
                    compatibility_run_hash = compatibility_run.run_hash
                    compatibility_status = compatibility_run.compatibility_status
                    compatibility_gate_failures = compatibility_run.gate_failures
                    execution_timing_compatible = compatibility_run.execution_timing_compatible
            deployment = await self._low_volatility_deployment.inspect(
                session_date=to_shanghai(now).date(),
            )
            try:
                deployment_contract = (
                    await self._low_volatility_deployment.contract_for_forward_spec(spec.spec_hash)
                )
            except LookupError:
                deployment_contract_status = "not_frozen"
            else:
                deployment_contract_hash = deployment_contract.contract_hash
                deployment_contract_status = "frozen_without_activation_authority"
            candidate_approval_hash = deployment.candidate_approval_hash
            daily_signal_hash = deployment.daily_signal_hash
            decision_signal_status = (
                "not_available"
                if daily_signal_hash is None
                else "prepared_without_activation_authority"
            )
            deployment_blockers = tuple(value.value for value in deployment.blockers)
        return LowVolatilityForwardProgressView(
            spec_hash=spec.spec_hash,
            forward_start_date=spec.forward_start_date,
            safe_cutoff_date=safe_cutoff,
            minimum_forward_sessions=(spec.minimum_forward_sessions),
            minimum_paper_sessions=spec.minimum_paper_sessions,
            observed_open_sessions=len(open_dates),
            completed_sessions=len(records),
            completed_required_sessions=completed_required,
            remaining_required_sessions=(spec.minimum_forward_sessions - completed_required),
            missing_session_dates=missing,
            calendar_conflict_dates=conflicts,
            pending_availability_session_dates=pending_required,
            next_collection_eligible_at=(
                availability.next_eligible_at if pending_required else None
            ),
            required_window_end=(
                required_window[-1]
                if len(required_window) == spec.minimum_forward_sessions
                else None
            ),
            status=status,
            evaluation_result_hash=(None if evaluation is None else evaluation.result.result_hash),
            evaluation_assessment_hash=(
                None if evaluation is None else evaluation.assessment.assessment_hash
            ),
            evaluation_evidence_status=(
                None if evaluation is None else evaluation.assessment.evidence_status
            ),
            evaluation_gate_failures=(
                () if evaluation is None else evaluation.assessment.gate_failures
            ),
            paper_trading_eligible=(
                False if evaluation is None else evaluation.assessment.paper_trading_eligible
            ),
            compatibility_spec_hash=(compatibility_spec_hash),
            compatibility_run_hash=compatibility_run_hash,
            compatibility_status=compatibility_status,
            compatibility_gate_failures=(compatibility_gate_failures),
            execution_timing_compatible=(execution_timing_compatible),
            candidate_approval_hash=candidate_approval_hash,
            deployment_contract_hash=(deployment_contract_hash),
            deployment_contract_status=(deployment_contract_status),
            daily_signal_hash=daily_signal_hash,
            decision_signal_status=decision_signal_status,
            deployment_blockers=deployment_blockers,
            sessions=tuple(
                LowVolatilityForwardSessionView(
                    binding_hash=record.binding.binding_hash,
                    dataset_manifest_hash=(record.binding.dataset_manifest_hash),
                    session_date=record.binding.session_date,
                    snapshot_hash=(record.binding.snapshot_hash),
                    snapshot_reference_date=(record.binding.snapshot_reference_date),
                    instrument_count=len(record.binding.instruments),
                    completed_at=record.completed_at,
                )
                for record in records
            ),
        )

    async def risk_status(self) -> RiskControlStatus:
        if self._risk is None:
            return RiskControlStatus(
                status="locked",
                live_trading_locked=True,
                paper_gateway_available=False,
                decision_count=0,
                recent_decisions=(),
                remaining_gates=(
                    "risk_audit_store",
                    "scheduler_runtime_wiring",
                    "external_realtime_quote_adapter",
                    "reconciliation_loop",
                    "kill_switch_drill",
                    "qmt_gateway",
                ),
            )
        count = await self._risk.count()
        recent = await self._risk.list_recent(limit=20)
        return RiskControlStatus(
            status="locked",
            live_trading_locked=True,
            paper_gateway_available=False,
            decision_count=count,
            recent_decisions=recent,
            remaining_gates=(
                "scheduler_runtime_wiring",
                "external_realtime_quote_adapter",
                "reconciliation_loop",
                "kill_switch_drill",
                "qmt_gateway",
            ),
        )

    async def execution_status(self) -> PaperExecutionStatus:
        if self._execution is None:
            return PaperExecutionStatus(
                status="locked",
                persistence_available=False,
                recovery_verified=False,
                gateway_available=False,
                order_count=0,
                event_count=0,
                reconciliation_count=0,
                open_order_count=0,
                kill_switch_active=True,
                kill_switch_reason="control_store_unavailable",
                kill_switch_version=0,
                simulated_broker_available=False,
                simulated_broker_recovery_verified=False,
                simulated_broker_order_count=0,
                simulated_broker_fact_count=0,
                scheduler_evidence_available=False,
                scheduler_recovery_verified=False,
                scheduler_cycle_count=0,
                remaining_gates=(
                    "paper_execution_store",
                    "paper_broker_adapter",
                    "reconciliation_loop",
                    "restart_recovery_drill",
                    "kill_switch_drill",
                ),
            )
        summary = await self._execution.verify_recovery()
        broker_summary = (
            None
            if self._simulated_broker is None
            else await self._simulated_broker.verify_recovery()
        )
        scheduler_summary = (
            None
            if self._scheduler is None
            else await self._scheduler.replay(account_id=self._settings.paper_account_id)
        )
        control = (
            None
            if self._execution_controls is None
            else await self._execution_controls.replay(account_id=self._settings.paper_account_id)
        )
        return PaperExecutionStatus(
            status="locked",
            persistence_available=True,
            recovery_verified=summary.recovery_verified,
            gateway_available=False,
            order_count=summary.order_count,
            event_count=summary.event_count,
            reconciliation_count=summary.reconciliation_count,
            open_order_count=summary.open_order_count,
            latest_reconciliation_at=summary.latest_reconciliation_at,
            latest_reconciled=summary.latest_reconciled,
            kill_switch_active=True if control is None else control.active,
            kill_switch_reason=(
                "control_store_unavailable" if control is None else control.reason.value
            ),
            kill_switch_version=0 if control is None else control.version,
            simulated_broker_available=broker_summary is not None,
            simulated_broker_recovery_verified=(
                False if broker_summary is None else broker_summary.recovery_verified
            ),
            simulated_broker_order_count=(
                0 if broker_summary is None else broker_summary.order_count
            ),
            simulated_broker_fact_count=(
                0 if broker_summary is None else broker_summary.fact_count
            ),
            scheduler_evidence_available=scheduler_summary is not None,
            scheduler_recovery_verified=(
                False if scheduler_summary is None else scheduler_summary.recovery_verified
            ),
            scheduler_cycle_count=(
                0 if scheduler_summary is None else scheduler_summary.event_count
            ),
            latest_scheduler_at=(
                None if scheduler_summary is None else scheduler_summary.latest_evaluated_at
            ),
            remaining_gates=(
                "external_realtime_quote_adapter",
                "scheduler_runtime_wiring",
                "operational_kill_switch_reset_drill",
                "paper_evidence_period",
                "qmt_windows_read_only_reconciliation",
            ),
        )

    async def paper_strategy_status(self) -> PaperStrategyStatus:
        account_id = self._settings.paper_account_id
        strategy_id = self._settings.paper_strategy_id
        if self._strategy_registry is None:
            return PaperStrategyStatus(
                status="inactive",
                active=False,
                account_id=account_id,
                strategy_id=strategy_id,
                remaining_gates=(
                    "strategy_registry",
                    "sample_out_candidate",
                    "explicit_paper_approval",
                ),
            )
        registration = await self._strategy_registry.active(
            account_id=account_id,
            strategy_id=strategy_id,
        )
        if registration is None:
            return PaperStrategyStatus(
                status="inactive",
                active=False,
                account_id=account_id,
                strategy_id=strategy_id,
                remaining_gates=(
                    "sample_out_candidate",
                    "explicit_paper_approval",
                ),
            )
        if isinstance(
            registration,
            ValidatedSmaPortfolioRegistration,
        ):
            return PaperStrategyStatus(
                status="approved",
                active=True,
                account_id=account_id,
                strategy_id=strategy_id,
                deployment_kind="portfolio",
                registration_hash=registration.registration_hash,
                strategy_version=registration.strategy_version,
                instruments=registration.instruments,
                components=tuple(
                    PaperStrategyComponentStatus(
                        experiment_id=value.experiment_id,
                        instrument=value.instrument,
                        fast_sessions=value.fast_sessions,
                        slow_sessions=value.slow_sessions,
                        allocation=value.allocation,
                        validation_result_hash=(value.validation_result_hash),
                        signal_manifest_hash=(value.signal_manifest_hash),
                    )
                    for value in registration.components
                ),
                total_allocation=registration.total_allocation,
                valuation_manifest_hash=(registration.valuation_manifest_hash),
                portfolio_oos=PaperPortfolioOosStatus(
                    assessment_hash=(registration.oos_assessment.assessment_hash),
                    policy_hash=(registration.oos_assessment.policy_hash),
                    fold_count=registration.oos_assessment.fold_count,
                    compounded_return=(registration.oos_assessment.compounded_return),
                    profitable_fold_rate=(registration.oos_assessment.profitable_fold_rate),
                    maximum_drawdown=(registration.oos_assessment.maximum_drawdown),
                    maximum_pairwise_correlation=(
                        registration.oos_assessment.maximum_pairwise_correlation
                    ),
                    maximum_component_contribution=(
                        registration.oos_assessment.maximum_component_contribution
                    ),
                ),
                approved_by=registration.approved_by,
                approved_at=registration.approved_at,
                remaining_gates=(
                    "resident_scheduler_runtime",
                    "windows_qmt_readonly_reconciliation",
                    "continuous_paper_evidence",
                ),
            )
        return PaperStrategyStatus(
            status="approved",
            active=True,
            account_id=account_id,
            strategy_id=strategy_id,
            deployment_kind="single",
            registration_hash=registration.registration_hash,
            strategy_version=registration.strategy_version,
            experiment_id=registration.experiment_id,
            instrument=registration.instrument,
            fast_sessions=registration.fast_sessions,
            slow_sessions=registration.slow_sessions,
            allocation=registration.allocation,
            validation_result_hash=registration.validation_result_hash,
            signal_manifest_hash=registration.signal_manifest_hash,
            approved_by=registration.approved_by,
            approved_at=registration.approved_at,
            instruments=registration.instruments,
            total_allocation=registration.total_allocation,
            valuation_manifest_hash=(registration.valuation_manifest_hash),
            remaining_gates=(
                "resident_scheduler_runtime",
                "windows_qmt_readonly_reconciliation",
                "continuous_paper_evidence",
            ),
        )

    async def qmt_readonly_status(self) -> QmtReadOnlyStatus:
        if self._qmt_acceptances is None or self._qmt_sessions is None:
            return QmtReadOnlyStatus(
                status="blocked",
                current_host_read_only_ready=False,
                checks={"qmt_acceptance_store": "blocked"},
                evidence_fresh=False,
                remaining_gates=(
                    "qmt_acceptance_store",
                    "windows_qmt_readonly_acceptance",
                    "qmt_disconnect_recovery_drill",
                    "miniqmt_restart_recovery_drill",
                    "continuous_paper_evidence",
                ),
            )
        control = (
            None
            if self._execution_controls is None
            else await self._execution_controls.replay(account_id=self._settings.paper_account_id)
        )
        active_session_ids = await self._qmt_sessions.active_session_ids(now=self._now())
        readiness = inspect_qmt_readiness(
            self._settings,
            kill_switch_active=(None if control is None else control.active),
            active_session_ids=active_session_ids,
        )
        checks = {
            check.code.value: "pass" if check.passed else "blocked" for check in readiness.checks
        }
        evidence = await self._qmt_acceptances.latest(
            logical_account_id=self._settings.paper_account_id
        )
        gates = [
            "qmt_disconnect_recovery_drill",
            "miniqmt_restart_recovery_drill",
            "continuous_paper_evidence",
        ]
        if evidence is None:
            gates.insert(0, "windows_qmt_readonly_acceptance")
            return QmtReadOnlyStatus(
                status="blocked",
                current_host_read_only_ready=readiness.read_only_ready,
                checks=checks,
                evidence_fresh=False,
                remaining_gates=tuple(gates),
            )
        age = self._now().astimezone(UTC) - evidence.observed_at
        evidence_fresh = timedelta(0) <= age <= _QMT_ACCEPTANCE_MAX_AGE
        if not evidence_fresh:
            gates.insert(0, "fresh_windows_qmt_readonly_acceptance")
        return QmtReadOnlyStatus(
            status="accepted" if evidence_fresh else "stale",
            current_host_read_only_ready=readiness.read_only_ready,
            checks=checks,
            latest_evidence_hash=evidence.evidence_hash,
            latest_observed_at=evidence.observed_at,
            evidence_age_seconds=max(0, int(age.total_seconds())),
            evidence_fresh=evidence_fresh,
            position_count=evidence.position_count,
            order_count=evidence.order_count,
            trade_count=evidence.trade_count,
            remaining_gates=tuple(gates),
        )

    async def qmt_operations_status(self) -> QmtOperationsStatus:
        if self._qmt_operations is None:
            return QmtOperationsStatus(
                status="unavailable",
                integrity_verified=False,
                lease_active=False,
                callback_cursor=0,
                processing_event_count=0,
                processing_hash="0" * 64,
                broker_state_known=False,
                reconciliation_current=False,
                orders=(),
                trades=(),
            )
        snapshot = await self._qmt_operations.snapshot(account_id=self._settings.paper_account_id)
        reconciliation = snapshot.latest_reconciliation
        if snapshot.gateway_holder_id is None:
            status = "idle"
        elif not snapshot.broker_state_known or snapshot.fatal_reason is not None:
            status = "unknown"
        elif (
            snapshot.lease_active
            and snapshot.reconciliation_current
            and reconciliation is not None
            and reconciliation.state.value == "passed"
        ):
            status = "reconciled"
        else:
            status = "pending"
        return QmtOperationsStatus(
            status=status,
            integrity_verified=snapshot.integrity_verified,
            gateway_holder_id=snapshot.gateway_holder_id,
            qmt_session_id=snapshot.qmt_session_id,
            qmt_lease_generation=snapshot.qmt_lease_generation,
            lease_active=snapshot.lease_active,
            lease_expires_at=snapshot.lease_expires_at,
            callback_cursor=snapshot.last_local_sequence,
            processing_event_count=snapshot.processing_event_count,
            processing_hash=snapshot.last_processing_hash,
            broker_state_known=snapshot.broker_state_known,
            fatal_reason=snapshot.fatal_reason,
            reconciliation_state=(None if reconciliation is None else reconciliation.state.value),
            reconciliation_current=snapshot.reconciliation_current,
            reconciliation_report_hash=(
                None if reconciliation is None else reconciliation.report_hash
            ),
            reconciliation_observed_at=(
                None if reconciliation is None else reconciliation.observed_at
            ),
            reconciliation_issues=(
                ()
                if reconciliation is None
                else tuple(item.value for item in reconciliation.issues)
            ),
            orders=tuple(
                QmtBrokerOrderView(
                    client_order_id=item.client_order_id,
                    broker_order_id=item.broker_order_id,
                    instrument=item.instrument,
                    side=item.side.value,
                    quantity=item.quantity,
                    limit_price=item.limit_price,
                    order_state=item.order_state.value,
                    reported_traded_volume=item.reported_traded_volume,
                    trade_volume=item.trade_volume,
                    trade_amount=item.trade_amount,
                    convergence=item.convergence.value,
                    updated_at=item.updated_at,
                    projection_hash=item.projection_hash,
                )
                for item in snapshot.projections
            ),
            trades=tuple(
                QmtBrokerTradeView(
                    trade_id=item.trade_id,
                    client_order_id=item.client_order_id,
                    broker_order_id=item.broker_order_id,
                    instrument=item.instrument,
                    side=item.side.value,
                    volume=item.volume,
                    price=item.price,
                    amount=item.amount,
                    observed_at=item.observed_at,
                    fact_hash=item.fact_hash,
                )
                for item in snapshot.trade_facts
            ),
        )

    async def promotion_status(self) -> PaperPromotionStatus:
        if self._promotion is None:
            return PaperPromotionStatus(
                status="unavailable",
                blockers=("promotion_audit_store",),
                gates={},
            )
        policy = PaperPromotionPolicy()
        facts = await self._promotion.read(
            account_id=self._settings.paper_account_id,
            strategy_id=self._settings.paper_strategy_id,
            now=self._now(),
            lookback_days=policy.evidence_lookback_days,
            policy_hash=policy.policy_hash,
        )
        report = PaperPromotionAuditor(policy=policy).evaluate(facts)
        return PaperPromotionStatus(
            status="blocked",
            live_trading_ready=report.live_trading_ready,
            evidence_gates_passed=report.evidence_gates_passed,
            evaluated_at=report.evaluated_at,
            policy_hash=report.policy_hash,
            fact_hash=report.fact_hash,
            report_hash=report.report_hash,
            blockers=tuple(code.value for code in report.blockers),
            gates={
                gate.code.value: PromotionGateView(
                    status="pass" if gate.passed else "blocked",
                    actual=gate.actual,
                    required=gate.required,
                )
                for gate in report.gates
            },
        )

    async def activate_kill_switch(
        self, *, command_id: str, reason: str, requested_by: str
    ) -> PaperExecutionStatus:
        if self._execution_controls is None:
            raise PersistenceUnavailableError("Execution control service is unavailable")
        reason_code = {
            "manual": KillSwitchReason.MANUAL,
            "drill": KillSwitchReason.DRILL,
        }.get(reason)
        if reason_code is None:
            raise ValueError("operator activation reason must be manual or drill")
        await self._execution_controls.activate(
            account_id=self._settings.paper_account_id,
            command_id=command_id,
            reason=reason_code,
            actor=requested_by,
            now=self._now(),
        )
        return await self.execution_status()

    async def bars(
        self,
        *,
        instrument: str,
        start: date,
        end: date,
        as_of: datetime,
    ) -> tuple[dict[str, object], ...]:
        if start > end or (end - start).days > 365:
            raise ValueError("bar query interval must be between 1 and 366 days")
        records = await self._market.query_bars_as_of((instrument,), start, end, as_of=as_of)
        return tuple(
            {
                "instrument": record.instrument,
                "session_date": record.session_date.isoformat(),
                "open": str(record.open_price),
                "high": str(record.high_price),
                "low": str(record.low_price),
                "close": str(record.close_price),
                "pre_close": str(record.pre_close),
                "volume": record.volume,
                "turnover": str(record.turnover),
                "available_at": record.available_at.isoformat(),
                "content_hash": record.content_hash,
            }
            for record in records
        )

    async def _coverage(self) -> DataCoverage:
        try:
            bars = await self._market.client.query(
                "SELECT count(), minOrNull(session_date), maxOrNull(session_date) "
                "FROM daily_bar_revisions WHERE source = 'tushare'"
            )
            factors = await self._market.client.query(
                "SELECT count() FROM adjustment_factor_revisions WHERE source = 'tushare'"
            )
            bar_row = bars.result_rows[0]
            factor_row = factors.result_rows[0]
            return DataCoverage(
                daily_rows=int(bar_row[0]),
                factor_rows=int(factor_row[0]),
                first_session=bar_row[1],
                last_session=bar_row[2],
            )
        except Exception:
            raise PersistenceUnavailableError("ClickHouse coverage query failed") from None

    async def _work_loop(self) -> None:
        while True:
            try:
                job = await self._operators.claim_next_job(now=self._now())
            except AutoQuantError:
                await asyncio.sleep(self._poll_interval)
                continue
            if job is None:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self._poll_interval)
                except TimeoutError:
                    pass
                continue
            await self._run_job(job)

    async def _backtest_work_loop(self) -> None:
        repository, _ = self._require_backtests()
        while True:
            try:
                run = await repository.claim_next_run(now=self._now())
            except AutoQuantError:
                await asyncio.sleep(self._poll_interval)
                continue
            if run is None:
                self._backtest_wake.clear()
                try:
                    await asyncio.wait_for(self._backtest_wake.wait(), timeout=self._poll_interval)
                except TimeoutError:
                    pass
                continue
            await self._run_backtest(run)

    async def _validation_work_loop(self) -> None:
        repository, _ = self._require_validations()
        while True:
            try:
                experiment = await repository.claim_next_experiment(now=self._now())
            except AutoQuantError:
                await asyncio.sleep(self._poll_interval)
                continue
            if experiment is None:
                self._validation_wake.clear()
                try:
                    await asyncio.wait_for(
                        self._validation_wake.wait(), timeout=self._poll_interval
                    )
                except TimeoutError:
                    pass
                continue
            await self._run_validation(experiment)

    async def _portfolio_validation_work_loop(self) -> None:
        repository, _ = self._require_portfolio_validations()
        while True:
            try:
                experiment = await repository.claim_next_experiment(now=self._now())
            except AutoQuantError:
                await asyncio.sleep(self._poll_interval)
                continue
            if experiment is None:
                self._portfolio_validation_wake.clear()
                try:
                    await asyncio.wait_for(
                        self._portfolio_validation_wake.wait(),
                        timeout=self._poll_interval,
                    )
                except TimeoutError:
                    pass
                continue
            await self._run_portfolio_validation(experiment)

    async def _run_backtest(self, run: BacktestRun) -> None:
        repository, runner = self._require_backtests()
        try:
            await self._audit("operator.backtest.started", run.run_id, {})
            result = await runner.run(run.request)
            await repository.complete_run(run.run_id, result=result, now=self._now())
        except ValueError:
            await repository.fail_run(
                run.run_id, error_code="invalid_backtest_input", now=self._now()
            )
            await self._best_effort_backtest_failure_audit(run.run_id, "invalid_backtest_input")
        except AutoQuantError:
            await repository.fail_run(
                run.run_id, error_code="backtest_dependency_failed", now=self._now()
            )
            await self._best_effort_backtest_failure_audit(run.run_id, "backtest_dependency_failed")
        except Exception:
            await repository.fail_run(run.run_id, error_code="internal_error", now=self._now())
            await self._best_effort_backtest_failure_audit(run.run_id, "internal_error")
        else:
            try:
                await self._audit(
                    "operator.backtest.completed",
                    run.run_id,
                    {
                        "result_hash": result.result_hash,
                        "ledger_hash": result.ledger_hash,
                        "manifest_hash": result.manifest_hash,
                    },
                )
            except AutoQuantError:
                pass

    async def _run_validation(self, experiment: ValidationExperiment) -> None:
        repository, runner = self._require_validations()
        try:
            await self._audit("operator.validation.started", experiment.experiment_id, {})
            result = await self._run_validation_with_retry(
                runner=runner,
                experiment=experiment,
            )
            await repository.complete_experiment(
                experiment.experiment_id, result=result, now=self._now()
            )
        except ValueError:
            await repository.fail_experiment(
                experiment.experiment_id,
                error_code="invalid_validation_input",
                now=self._now(),
            )
            await self._best_effort_validation_failure_audit(
                experiment.experiment_id, "invalid_validation_input"
            )
        except AutoQuantError:
            await repository.fail_experiment(
                experiment.experiment_id,
                error_code="validation_dependency_failed",
                now=self._now(),
            )
            await self._best_effort_validation_failure_audit(
                experiment.experiment_id, "validation_dependency_failed"
            )
        except Exception:
            await repository.fail_experiment(
                experiment.experiment_id,
                error_code="internal_error",
                now=self._now(),
            )
            await self._best_effort_validation_failure_audit(
                experiment.experiment_id, "internal_error"
            )
        else:
            try:
                await self._audit(
                    "operator.validation.completed",
                    experiment.experiment_id,
                    {
                        "result_hash": result.result_hash,
                        "manifest_hash": result.manifest_hash,
                        "fold_count": len(result.folds),
                    },
                )
            except AutoQuantError:
                pass

    async def _run_portfolio_validation(
        self,
        experiment: PortfolioValidationExperiment,
    ) -> None:
        repository, runner = self._require_portfolio_validations()
        try:
            await self._audit(
                "operator.portfolio_validation.started",
                experiment.experiment_id,
                {},
            )
            result = await self._run_portfolio_validation_with_retry(
                runner=runner,
                experiment=experiment,
            )
            await repository.complete_experiment(
                experiment.experiment_id,
                result=result,
                now=self._now(),
            )
        except ValueError:
            error_code = "invalid_portfolio_validation_input"
            await repository.fail_experiment(
                experiment.experiment_id,
                error_code=error_code,
                now=self._now(),
            )
            await self._best_effort_portfolio_validation_failure_audit(
                experiment.experiment_id,
                error_code,
            )
        except AutoQuantError:
            error_code = "portfolio_validation_dependency_failed"
            await repository.fail_experiment(
                experiment.experiment_id,
                error_code=error_code,
                now=self._now(),
            )
            await self._best_effort_portfolio_validation_failure_audit(
                experiment.experiment_id,
                error_code,
            )
        except Exception:
            error_code = "internal_error"
            await repository.fail_experiment(
                experiment.experiment_id,
                error_code=error_code,
                now=self._now(),
            )
            await self._best_effort_portfolio_validation_failure_audit(
                experiment.experiment_id,
                error_code,
            )
        else:
            try:
                await self._audit(
                    "operator.portfolio_validation.completed",
                    experiment.experiment_id,
                    {
                        "result_hash": result.result_hash,
                        "manifest_hash": result.manifest_hash,
                        "instrument_count": len(result.instruments),
                        "fold_count": len(result.folds),
                    },
                )
            except AutoQuantError:
                pass

    async def _run_validation_with_retry(
        self,
        *,
        runner: WalkForwardRunnerPort,
        experiment: ValidationExperiment,
    ) -> WalkForwardResult:
        maximum_attempts = 3
        for attempt in range(1, maximum_attempts + 1):
            try:
                return await runner.run(
                    manifest_hash=experiment.request.manifest_hash,
                    instrument=experiment.request.instrument,
                    config=validation_config(experiment.request),
                )
            except AutoQuantError:
                if attempt == maximum_attempts:
                    raise
                await asyncio.sleep(self._poll_interval * attempt)
        raise AssertionError("validation retry loop exhausted")

    async def _run_portfolio_validation_with_retry(
        self,
        *,
        runner: PortfolioWalkForwardRunnerPort,
        experiment: PortfolioValidationExperiment,
    ) -> PortfolioWalkForwardResult:
        maximum_attempts = 3
        for attempt in range(1, maximum_attempts + 1):
            try:
                return await runner.run(
                    manifest_hash=experiment.request.manifest_hash,
                    config=portfolio_validation_config(experiment.request),
                )
            except AutoQuantError:
                if attempt == maximum_attempts:
                    raise
                await asyncio.sleep(self._poll_interval * attempt)
        raise AssertionError("portfolio validation retry loop exhausted")

    async def _run_job(self, job: OperatorJob) -> None:
        try:
            await self._audit("operator.daily_ingestion.started", job.job_id, {})
            result = await self._ingestion_runner(
                self._settings,
                job.request.instruments,
                job.request.start,
                job.request.end,
            )
            if result.get("status") != "completed":
                raise PersistenceUnavailableError("Ingestion did not complete")
            await self._operators.complete_job(job.job_id, result=result, now=self._now())
        except AutoQuantError:
            await self._operators.fail_job(
                job.job_id, error_code="ingestion_failed", now=self._now()
            )
            await self._best_effort_failure_audit(job.job_id, "ingestion_failed")
        except Exception:
            await self._operators.fail_job(job.job_id, error_code="internal_error", now=self._now())
            await self._best_effort_failure_audit(job.job_id, "internal_error")
        else:
            try:
                await self._audit(
                    "operator.daily_ingestion.completed",
                    job.job_id,
                    _safe_result(result),
                )
            except AutoQuantError:
                pass

    async def _best_effort_failure_audit(self, job_id: UUID, error_code: str) -> None:
        try:
            await self._audit("operator.daily_ingestion.failed", job_id, {"error_code": error_code})
        except AutoQuantError:
            pass

    async def _best_effort_backtest_failure_audit(self, run_id: UUID, error_code: str) -> None:
        try:
            await self._audit("operator.backtest.failed", run_id, {"error_code": error_code})
        except AutoQuantError:
            pass

    async def _best_effort_validation_failure_audit(
        self, experiment_id: UUID, error_code: str
    ) -> None:
        try:
            await self._audit(
                "operator.validation.failed",
                experiment_id,
                {"error_code": error_code},
            )
        except AutoQuantError:
            pass

    async def _best_effort_portfolio_validation_failure_audit(
        self,
        experiment_id: UUID,
        error_code: str,
    ) -> None:
        try:
            await self._audit(
                "operator.portfolio_validation.failed",
                experiment_id,
                {"error_code": error_code},
            )
        except AutoQuantError:
            pass

    def _require_backtests(
        self,
    ) -> tuple[PostgresBacktestRepository, BacktestRunnerPort]:
        if self._backtests is None or self._backtest_runner is None:
            raise PersistenceUnavailableError("Backtest service is unavailable")
        return self._backtests, self._backtest_runner

    def _require_validations(
        self,
    ) -> tuple[PostgresValidationRepository, WalkForwardRunnerPort]:
        if self._validations is None or self._validation_runner is None:
            raise PersistenceUnavailableError("Validation service is unavailable")
        return self._validations, self._validation_runner

    def _require_portfolio_validations(
        self,
    ) -> tuple[
        PostgresPortfolioValidationRepository,
        PortfolioWalkForwardRunnerPort,
    ]:
        if self._portfolio_validations is None or self._portfolio_validation_runner is None:
            raise PersistenceUnavailableError("Portfolio validation service is unavailable")
        return (
            self._portfolio_validations,
            self._portfolio_validation_runner,
        )

    async def _audit(self, event_type: str, job_id: UUID, payload: Mapping[str, object]) -> None:
        normalized = {"job_id": str(job_id), **dict(payload)}
        await self._control.append_audit_event(event_type, self._now(), normalized)


def _validation_campaign_view(
    value: ValidationCampaignStatus,
) -> ValidationCampaignView:
    return ValidationCampaignView(
        campaign_hash=value.spec.campaign_hash,
        campaign_key=value.spec.campaign_key,
        manifest_hash=value.spec.manifest_hash,
        instruments=value.spec.instruments,
        created_at=value.created_at,
        status=value.status,
        components=tuple(
            ValidationCampaignComponentView(
                sequence=component.sequence,
                instrument=component.instrument,
                experiment_id=component.experiment_id,
                state=component.state,
                evidence_status=component.evidence_status,
                gate_failures=component.gate_failures,
                result_hash=component.result_hash,
            )
            for component in value.components
        ),
    )


def _fundamental_validation_summary(
    record: FundamentalValidationRecord,
) -> FundamentalValidationSummaryView:
    result = record.result
    strategy_unresolved = sum(
        len(run.snapshots[-1].positions)
        for fold in result.folds
        for run in (fold.training_result, fold.test_result)
    )
    benchmark_unresolved = sum(
        len(fold.benchmark_result.snapshots[-1].positions) for fold in result.folds
    )
    return FundamentalValidationSummaryView(
        result_hash=result.result_hash,
        assessment_hash=record.evidence.assessment_hash,
        spec_hash=result.spec_hash,
        strategy_id=result.folds[0].test_result.strategy_id,
        evidence_status=record.evidence.evidence_status,
        gate_failures=record.evidence.gate_failures,
        fold_count=len(result.folds),
        oos_sessions=record.evidence.oos_sessions,
        compounded_oos_return=result.compounded_oos_return,
        benchmark_compounded_oos_return=(result.benchmark_compounded_oos_return),
        excess_oos_return=result.excess_oos_return,
        profitable_fold_rate=result.profitable_fold_rate,
        worst_oos_drawdown=result.worst_oos_drawdown,
        train_test_gap=result.train_test_gap,
        rejected_order_count=result.rejected_order_count,
        unresolved_position_count=result.unresolved_position_count,
        strategy_unresolved_position_count=strategy_unresolved,
        benchmark_unresolved_position_count=benchmark_unresolved,
        requested_by=record.requested_by,
        completed_at=record.completed_at,
    )


def _fundamental_validation_list_item(
    record: FundamentalValidationIndexRecord,
) -> FundamentalValidationListItemView:
    return FundamentalValidationListItemView(
        result_hash=record.result_hash,
        assessment_hash=record.evidence.assessment_hash,
        spec_hash=record.spec_hash,
        strategy_id=FUNDAMENTAL_PORTFOLIO_STRATEGY_ID,
        evidence_status=record.evidence.evidence_status,
        gate_failures=record.evidence.gate_failures,
        fold_count=record.evidence.fold_count,
        oos_sessions=record.evidence.oos_sessions,
        compounded_oos_return=record.compounded_oos_return,
        benchmark_compounded_oos_return=(record.benchmark_compounded_oos_return),
        excess_oos_return=record.excess_oos_return,
        profitable_fold_rate=record.profitable_fold_rate,
        worst_oos_drawdown=record.worst_oos_drawdown,
        train_test_gap=record.train_test_gap,
        rejected_order_count=record.evidence.rejected_order_count,
        unresolved_position_count=(record.evidence.unresolved_position_count),
        requested_by=record.requested_by,
        completed_at=record.completed_at,
    )


def _fundamental_validation_phase(
    result: BacktestResult,
) -> FundamentalValidationPhaseView:
    return FundamentalValidationPhaseView(
        total_return=result.total_return,
        max_drawdown=result.max_drawdown,
        ending_equity=result.snapshots[-1].equity,
        rejected_order_count=sum(
            value.state is ExecutionState.REJECTED for value in result.reports
        ),
        unresolved_position_count=len(result.snapshots[-1].positions),
        artifact_hash=backtest_artifact_hash(result),
    )


def _low_volatility_validation_summary(
    record: LowVolatilityValidationRecord,
) -> LowVolatilityValidationListItemView:
    result = record.result
    evidence = record.evidence
    return LowVolatilityValidationListItemView(
        result_hash=result.result_hash,
        assessment_hash=evidence.assessment_hash,
        spec_hash=result.spec_hash,
        strategy_id=LOW_VOLATILITY_STRATEGY_ID,
        evidence_status=evidence.evidence_status,
        gate_failures=evidence.gate_failures,
        fold_count=evidence.fold_count,
        oos_sessions=evidence.oos_sessions,
        compounded_oos_return=result.compounded_oos_return,
        benchmark_compounded_oos_return=(result.benchmark_compounded_oos_return),
        excess_oos_return=result.excess_oos_return,
        profitable_fold_rate=result.profitable_fold_rate,
        worst_oos_drawdown=result.worst_oos_drawdown,
        train_test_gap=result.train_test_gap,
        strategy_rejected_order_count=(result.strategy_rejected_order_count),
        benchmark_rejected_order_count=(result.benchmark_rejected_order_count),
        strategy_unresolved_position_count=(result.strategy_unresolved_position_count),
        benchmark_unresolved_position_count=(result.benchmark_unresolved_position_count),
        requested_by=record.requested_by,
        completed_at=record.completed_at,
    )


def _low_volatility_validation_phase(
    result: BacktestResult,
) -> LowVolatilityValidationPhaseView:
    return LowVolatilityValidationPhaseView(
        total_return=result.total_return,
        max_drawdown=result.max_drawdown,
        ending_equity=result.snapshots[-1].equity,
        rejected_order_count=sum(
            value.state is ExecutionState.REJECTED for value in result.reports
        ),
        unresolved_position_count=len(result.snapshots[-1].positions),
        artifact_hash=backtest_artifact_hash(result),
    )


def _safe_result(result: Mapping[str, object]) -> dict[str, object]:
    allowed = {
        "fetched_bars",
        "fetched_factors",
        "manifest_hash",
        "persisted_bars",
        "persisted_factors",
        "quality_hash",
        "status",
    }
    return {key: value for key, value in result.items() if key in allowed}
