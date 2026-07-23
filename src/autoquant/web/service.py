from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, date, datetime
from typing import Protocol
from uuid import UUID, uuid4

from autoquant.adapters.clickhouse_daily import ClickHouseDailyRepository
from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.backtest.models import BacktestResult
from autoquant.backtest.validation import WalkForwardConfig, WalkForwardResult
from autoquant.config import AppSettings
from autoquant.errors import AutoQuantError, PersistenceUnavailableError
from autoquant.execution.control import KillSwitchReason
from autoquant.execution.control_store import PostgresExecutionControlRepository
from autoquant.execution.paper_scheduler_store import PostgresPaperSchedulerRepository
from autoquant.execution.simulated_broker import PersistentSimulatedBroker
from autoquant.execution.store import PostgresPaperExecutionRepository
from autoquant.operations import run_daily_ingestion
from autoquant.web.backtest_store import PostgresBacktestRepository
from autoquant.web.models import (
    BacktestRun,
    BacktestRunDetail,
    BacktestRunRequest,
    DailyIngestionJobRequest,
    DataCoverage,
    OperatorJob,
    OperatorOverview,
    PaperExecutionStatus,
    ResearchManifest,
    RiskControlStatus,
    ValidationExperiment,
    ValidationExperimentDetail,
    WalkForwardJobRequest,
)
from autoquant.web.risk_store import PostgresRiskDecisionRepository
from autoquant.web.store import PostgresOperatorRepository
from autoquant.web.validation_store import (
    PostgresValidationRepository,
    validation_config,
)

IngestionRunner = Callable[[AppSettings, tuple[str, ...], date, date], Awaitable[dict[str, object]]]


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

    async def create_backtest(
        self, request: BacktestRunRequest, *, requested_by: str
    ) -> BacktestRun: ...

    async def backtest_detail(self, run_id: UUID) -> BacktestRunDetail: ...

    async def list_validations(self, *, limit: int = 50) -> tuple[ValidationExperiment, ...]: ...

    async def create_validation(
        self, request: WalkForwardJobRequest, *, requested_by: str
    ) -> ValidationExperiment: ...

    async def validation_detail(self, experiment_id: UUID) -> ValidationExperimentDetail: ...

    async def risk_status(self) -> RiskControlStatus: ...

    async def execution_status(self) -> PaperExecutionStatus: ...

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
        risk_repository: PostgresRiskDecisionRepository | None = None,
        execution_repository: PostgresPaperExecutionRepository | None = None,
        execution_control_repository: PostgresExecutionControlRepository | None = None,
        simulated_broker: PersistentSimulatedBroker | None = None,
        scheduler_repository: PostgresPaperSchedulerRepository | None = None,
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
        self._risk = risk_repository
        self._execution = execution_repository
        self._execution_controls = execution_control_repository
        self._simulated_broker = simulated_broker
        self._scheduler = scheduler_repository
        self._ingestion_runner = ingestion_runner
        self._now = now
        self._poll_interval = poll_interval
        self._wake = asyncio.Event()
        self._worker: asyncio.Task[None] | None = None
        self._backtest_wake = asyncio.Event()
        self._backtest_worker: asyncio.Task[None] | None = None
        self._validation_wake = asyncio.Event()
        self._validation_worker: asyncio.Task[None] | None = None

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
                await self._scheduler.replay(
                    account_id=self._settings.paper_account_id
                )
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
                False
                if scheduler_summary is None
                else scheduler_summary.recovery_verified
            ),
            scheduler_cycle_count=(
                0 if scheduler_summary is None else scheduler_summary.event_count
            ),
            latest_scheduler_at=(
                None
                if scheduler_summary is None
                else scheduler_summary.latest_evaluated_at
            ),
            remaining_gates=(
                "external_realtime_quote_adapter",
                "scheduler_runtime_wiring",
                "operational_kill_switch_reset_drill",
                "paper_evidence_period",
                "qmt_windows_read_only_reconciliation",
            ),
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
            result = await runner.run(
                manifest_hash=experiment.request.manifest_hash,
                instrument=experiment.request.instrument,
                config=validation_config(experiment.request),
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

    async def _audit(self, event_type: str, job_id: UUID, payload: Mapping[str, object]) -> None:
        normalized = {"job_id": str(job_id), **dict(payload)}
        await self._control.append_audit_event(event_type, self._now(), normalized)


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
