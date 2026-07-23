from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from autoquant.config import AppSettings
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.control import KillSwitchReason
from autoquant.execution.portfolio_validation import (
    PortfolioOosComponentEvidence,
    PortfolioOosFold,
    assess_portfolio_oos,
)
from autoquant.execution.validated_sma import ValidatedSmaRegistration
from autoquant.execution.validated_sma_portfolio import (
    ValidatedSmaPortfolioRegistration,
)
from autoquant.web.models import (
    BacktestRun,
    BacktestRunRequest,
    DailyIngestionJobRequest,
    OperatorJob,
    OperatorJobState,
    PaperExecutionStatus,
    PaperStrategyStatus,
    QmtReadOnlyStatus,
    RiskControlStatus,
    ValidationExperiment,
    WalkForwardJobRequest,
)
from autoquant.web.service import ConsoleService

NOW = datetime(2025, 1, 3, tzinfo=UTC)


def _request() -> DailyIngestionJobRequest:
    return DailyIngestionJobRequest(
        instruments=("000001.XSHE",),
        start=date(2025, 1, 1),
        end=date(2025, 1, 2),
        idempotency_key="operator-service-test-0001",
    )


def _job(state: OperatorJobState = OperatorJobState.RUNNING) -> OperatorJob:
    return OperatorJob(
        job_id=uuid4(),
        state=state,
        request=_request(),
        requested_by="operator",
        created_at=NOW,
        started_at=NOW if state is not OperatorJobState.QUEUED else None,
    )


def _service(
    *,
    operator: MagicMock,
    control: MagicMock,
    runner: AsyncMock,
    backtests: MagicMock | None = None,
    backtest_runner: MagicMock | None = None,
    validations: MagicMock | None = None,
    validation_runner: MagicMock | None = None,
    risks: MagicMock | None = None,
    executions: MagicMock | None = None,
    execution_controls: MagicMock | None = None,
    simulated_broker: MagicMock | None = None,
    scheduler: MagicMock | None = None,
    strategy_registry: MagicMock | None = None,
    qmt_acceptances: MagicMock | None = None,
    qmt_sessions: MagicMock | None = None,
) -> ConsoleService:
    market = MagicMock()
    market.client = MagicMock()
    return ConsoleService(
        settings=AppSettings(_env_file=None, tushare_token="configured-token"),
        operator_repository=operator,
        control_repository=control,
        market_repository=market,
        ingestion_runner=runner,
        backtest_repository=backtests,
        backtest_runner=backtest_runner,
        validation_repository=validations,
        validation_runner=validation_runner,
        risk_repository=risks,
        execution_repository=executions,
        execution_control_repository=execution_controls,
        simulated_broker=simulated_broker,
        scheduler_repository=scheduler,
        strategy_registry=strategy_registry,
        qmt_acceptance_repository=qmt_acceptances,
        qmt_session_repository=qmt_sessions,
        now=lambda: NOW,
        poll_interval=0.01,
    )


def _backtest_request() -> BacktestRunRequest:
    return BacktestRunRequest(
        manifest_hash="a" * 64,
        instrument="000001.XSHE",
        idempotency_key="service-backtest-request-0001",
    )


def _backtest_run() -> BacktestRun:
    return BacktestRun(
        run_id=uuid4(),
        state=OperatorJobState.RUNNING,
        strategy_id="manifest_buy_hold_v1",
        request=_backtest_request(),
        requested_by="operator",
        created_at=NOW,
        started_at=NOW,
    )


def _validation_experiment() -> ValidationExperiment:
    return ValidationExperiment(
        experiment_id=uuid4(),
        state=OperatorJobState.RUNNING,
        validator_id="sma_cross_walk_forward_v1",
        request=WalkForwardJobRequest(
            manifest_hash="a" * 64,
            instrument="000001.XSHE",
            train_sessions=60,
            test_sessions=20,
            candidates=({"fast_sessions": 5, "slow_sessions": 20},),
            idempotency_key="service-validation-request-0001",
        ),
        requested_by="operator",
        created_at=NOW,
        started_at=NOW,
    )


@pytest.mark.asyncio
async def test_risk_status_keeps_live_trading_locked_and_reports_audit_count() -> None:
    risks = MagicMock()
    risks.count = AsyncMock(return_value=12)
    risks.list_recent = AsyncMock(return_value=())
    service = _service(
        operator=MagicMock(),
        control=MagicMock(),
        runner=AsyncMock(),
        risks=risks,
    )

    status = await service.risk_status()

    assert isinstance(status, RiskControlStatus)
    assert status.live_trading_locked is True
    assert status.paper_gateway_available is False
    assert status.decision_count == 12
    assert "qmt_gateway" in status.remaining_gates


@pytest.mark.asyncio
async def test_execution_status_requires_gateway_even_after_verified_recovery() -> None:
    executions = MagicMock()
    summary = MagicMock()
    summary.recovery_verified = True
    summary.order_count = 2
    summary.event_count = 3
    summary.reconciliation_count = 1
    summary.open_order_count = 1
    summary.latest_reconciliation_at = NOW
    summary.latest_reconciled = True
    executions.verify_recovery = AsyncMock(return_value=summary)
    scheduler = MagicMock()
    scheduler_summary = MagicMock()
    scheduler_summary.recovery_verified = True
    scheduler_summary.event_count = 7
    scheduler_summary.latest_evaluated_at = NOW
    scheduler.replay = AsyncMock(return_value=scheduler_summary)
    service = _service(
        operator=MagicMock(),
        control=MagicMock(),
        runner=AsyncMock(),
        executions=executions,
        scheduler=scheduler,
    )

    status = await service.execution_status()

    assert isinstance(status, PaperExecutionStatus)
    assert status.recovery_verified is True
    assert status.gateway_available is False
    assert status.scheduler_recovery_verified is True
    assert status.scheduler_cycle_count == 7
    assert "scheduler_runtime_wiring" in status.remaining_gates


@pytest.mark.asyncio
async def test_startup_recovery_failure_activates_kill_switch_and_aborts() -> None:
    executions = MagicMock()
    executions.verify_recovery = AsyncMock(
        side_effect=PersistenceUnavailableError("corrupt projection")
    )
    controls = MagicMock()
    controls.ensure_fail_closed = AsyncMock()
    controls.activate = AsyncMock()
    service = _service(
        operator=MagicMock(),
        control=MagicMock(),
        runner=AsyncMock(),
        executions=executions,
        execution_controls=controls,
    )

    with pytest.raises(PersistenceUnavailableError, match="corrupt projection"):
        await service.start()

    controls.ensure_fail_closed.assert_awaited_once()
    assert controls.activate.await_args.kwargs["reason"] is KillSwitchReason.RECOVERY_FAILED


@pytest.mark.asyncio
async def test_simulated_broker_recovery_failure_also_aborts_startup() -> None:
    executions = MagicMock()
    executions.verify_recovery = AsyncMock()
    broker = MagicMock()
    broker.verify_recovery = AsyncMock(
        side_effect=PersistenceUnavailableError("broker fact mismatch")
    )
    controls = MagicMock()
    controls.ensure_fail_closed = AsyncMock()
    controls.activate = AsyncMock()
    service = _service(
        operator=MagicMock(),
        control=MagicMock(),
        runner=AsyncMock(),
        executions=executions,
        execution_controls=controls,
        simulated_broker=broker,
    )

    with pytest.raises(PersistenceUnavailableError, match="broker fact mismatch"):
        await service.start()

    assert controls.activate.await_args.kwargs["reason"] is KillSwitchReason.RECOVERY_FAILED


@pytest.mark.asyncio
async def test_scheduler_evidence_recovery_failure_also_aborts_startup() -> None:
    scheduler = MagicMock()
    scheduler.replay = AsyncMock(
        side_effect=PersistenceUnavailableError("scheduler chain mismatch")
    )
    controls = MagicMock()
    controls.ensure_fail_closed = AsyncMock()
    controls.activate = AsyncMock()
    service = _service(
        operator=MagicMock(),
        control=MagicMock(),
        runner=AsyncMock(),
        execution_controls=controls,
        scheduler=scheduler,
    )

    with pytest.raises(PersistenceUnavailableError, match="scheduler chain mismatch"):
        await service.start()

    assert controls.activate.await_args.kwargs["reason"] is KillSwitchReason.RECOVERY_FAILED


@pytest.mark.asyncio
async def test_strategy_registry_status_is_inactive_without_approval() -> None:
    registry = MagicMock()
    registry.active = AsyncMock(return_value=None)
    service = _service(
        operator=MagicMock(),
        control=MagicMock(),
        runner=AsyncMock(),
        strategy_registry=registry,
    )

    status = await service.paper_strategy_status()

    assert isinstance(status, PaperStrategyStatus)
    assert status.active is False
    assert status.live_trading_locked is True
    assert "explicit_paper_approval" in status.remaining_gates


@pytest.mark.asyncio
async def test_strategy_status_exposes_portfolio_components() -> None:
    instruments = (
        "000001.XSHE",
        "600000.XSHG",
        "600519.XSHG",
    )
    components = tuple(
        ValidatedSmaRegistration(
            account_id="paper-main",
            strategy_id="validated-sma-paper",
            strategy_version=f"sma-paper-v1:{index}",
            experiment_id=uuid4(),
            validation_result_hash=f"{index:x}" * 64,
            validation_manifest_hash=f"{index + 3:x}" * 64,
            signal_manifest_hash=f"{index + 6:x}" * 64,
            signal_manifest_as_of=NOW - timedelta(hours=1),
            instrument=instrument,
            fast_sessions=5,
            slow_sessions=20,
            allocation=Decimal("0.20"),
            slippage_bps=Decimal("5"),
            risk_policy_hash="a" * 64,
            rule_version=f"rules-{index}",
            approved_by="operator",
            approved_at=NOW,
        )
        for index, instrument in enumerate(instruments, start=1)
    )
    portfolio = ValidatedSmaPortfolioRegistration(
        account_id="paper-main",
        strategy_id="validated-sma-paper",
        strategy_version="sma-portfolio-paper-v1:test",
        components=components,
        oos_assessment=assess_portfolio_oos(
            tuple(
                PortfolioOosComponentEvidence(
                    experiment_id=component.experiment_id,
                    validation_result_hash=(
                        component.validation_result_hash
                    ),
                    instrument=component.instrument,
                    allocation=component.allocation,
                    folds=tuple(
                        PortfolioOosFold(
                            sequence=sequence,
                            test_start=date(2024, sequence, 1),
                            test_end=date(2024, sequence, 20),
                            total_return=Decimal(value),
                            max_drawdown=Decimal("0.01"),
                        )
                        for sequence, value in enumerate(
                            returns,
                            start=1,
                        )
                    ),
                )
                for component, returns in zip(
                    components,
                    (
                        (
                            "0.010",
                            "0.020",
                            "-0.005",
                            "0.015",
                            "0.003",
                            "0.012",
                        ),
                        (
                            "0.008",
                            "-0.003",
                            "0.018",
                            "0.004",
                            "0.014",
                            "0.006",
                        ),
                        (
                            "-0.002",
                            "0.011",
                            "0.005",
                            "0.017",
                            "0.007",
                            "0.009",
                        ),
                    ),
                    strict=True,
                )
            )
        ),
        valuation_manifest_hash="b" * 64,
        valuation_manifest_as_of=NOW - timedelta(hours=1),
        risk_policy_hash="a" * 64,
        approved_by="operator",
        approved_at=NOW,
    )
    registry = MagicMock()
    registry.active = AsyncMock(return_value=portfolio)
    service = _service(
        operator=MagicMock(),
        control=MagicMock(),
        runner=AsyncMock(),
        strategy_registry=registry,
    )

    status = await service.paper_strategy_status()

    assert status.deployment_kind == "portfolio"
    assert status.instruments == instruments
    assert len(status.components) == 3
    assert status.total_allocation == Decimal("0.60")
    assert status.instrument is None
    assert status.portfolio_oos is not None
    assert status.portfolio_oos.fold_count == 6
    assert status.portfolio_oos.compounded_return > 0


@pytest.mark.asyncio
async def test_strategy_registry_recovery_failure_aborts_startup() -> None:
    registry = MagicMock()
    registry.active = AsyncMock(
        side_effect=PersistenceUnavailableError("strategy chain mismatch")
    )
    controls = MagicMock()
    controls.ensure_fail_closed = AsyncMock()
    controls.activate = AsyncMock()
    scheduler = MagicMock()
    scheduler.replay = AsyncMock()
    service = _service(
        operator=MagicMock(),
        control=MagicMock(),
        runner=AsyncMock(),
        execution_controls=controls,
        scheduler=scheduler,
        strategy_registry=registry,
    )

    with pytest.raises(PersistenceUnavailableError, match="strategy chain mismatch"):
        await service.start()

    assert controls.activate.await_args.kwargs["reason"] is KillSwitchReason.RECOVERY_FAILED


@pytest.mark.asyncio
async def test_qmt_status_reports_fresh_redacted_evidence_and_host_blockers() -> None:
    acceptances = MagicMock()
    evidence = MagicMock(
        evidence_hash="a" * 64,
        observed_at=NOW - timedelta(hours=1),
        position_count=2,
        order_count=3,
        trade_count=1,
    )
    acceptances.latest = AsyncMock(return_value=evidence)
    sessions = MagicMock()
    sessions.active_session_ids = AsyncMock(return_value=())
    controls = MagicMock()
    controls.replay = AsyncMock(
        return_value=MagicMock(active=True)
    )
    service = _service(
        operator=MagicMock(),
        control=MagicMock(),
        runner=AsyncMock(),
        execution_controls=controls,
        qmt_acceptances=acceptances,
        qmt_sessions=sessions,
    )

    status = await service.qmt_readonly_status()

    assert isinstance(status, QmtReadOnlyStatus)
    assert status.status == "accepted"
    assert status.evidence_fresh is True
    assert status.current_host_read_only_ready is False
    assert status.latest_evidence_hash == "a" * 64
    assert status.position_count == 2
    assert status.live_trading_locked is True
    assert status.checks["windows_runtime"] == "blocked"
    assert "qmt_disconnect_recovery_drill" in status.remaining_gates


@pytest.mark.asyncio
async def test_qmt_status_without_repository_remains_blocked() -> None:
    service = _service(
        operator=MagicMock(),
        control=MagicMock(),
        runner=AsyncMock(),
    )

    status = await service.qmt_readonly_status()

    assert status.status == "blocked"
    assert status.latest_evidence_hash is None
    assert "qmt_acceptance_store" in status.remaining_gates


@pytest.mark.asyncio
async def test_operator_can_activate_but_not_reset_kill_switch_through_service() -> None:
    executions = MagicMock()
    summary = MagicMock(
        recovery_verified=True,
        order_count=0,
        event_count=0,
        reconciliation_count=0,
        open_order_count=0,
        latest_reconciliation_at=None,
        latest_reconciled=None,
    )
    executions.verify_recovery = AsyncMock(return_value=summary)
    controls = MagicMock()
    state = MagicMock(active=True, reason=KillSwitchReason.MANUAL, version=2)
    controls.activate = AsyncMock(return_value=state)
    controls.replay = AsyncMock(return_value=state)
    service = _service(
        operator=MagicMock(),
        control=MagicMock(),
        runner=AsyncMock(),
        executions=executions,
        execution_controls=controls,
    )

    status = await service.activate_kill_switch(
        command_id="service-kill-switch-0001",
        reason="manual",
        requested_by="operator",
    )

    assert status.kill_switch_active is True
    assert status.kill_switch_reason == "manual"
    controls.activate.assert_awaited_once()


@pytest.mark.asyncio
async def test_worker_completes_job_and_audits_only_safe_result_fields() -> None:
    operator = MagicMock()
    operator.complete_job = AsyncMock()
    operator.fail_job = AsyncMock()
    control = MagicMock()
    control.append_audit_event = AsyncMock(return_value="a" * 64)
    runner = AsyncMock(
        return_value={
            "status": "completed",
            "persisted_bars": 2,
            "persisted_factors": 2,
            "manifest_hash": "b" * 64,
            "quality_hash": "c" * 64,
            "secret": "must-not-be-audited",
        }
    )
    service = _service(operator=operator, control=control, runner=runner)
    job = _job()

    await service._run_job(job)

    operator.complete_job.assert_awaited_once()
    operator.fail_job.assert_not_awaited()
    completion_payload = control.append_audit_event.await_args_list[-1].args[2]
    assert completion_payload["job_id"] == str(job.job_id)
    assert "secret" not in completion_payload


@pytest.mark.asyncio
async def test_worker_fails_closed_on_ingestion_error() -> None:
    operator = MagicMock()
    operator.complete_job = AsyncMock()
    operator.fail_job = AsyncMock()
    control = MagicMock()
    control.append_audit_event = AsyncMock(return_value="a" * 64)
    runner = AsyncMock(side_effect=PersistenceUnavailableError("vendor unavailable"))
    service = _service(operator=operator, control=control, runner=runner)

    await service._run_job(_job())

    operator.complete_job.assert_not_awaited()
    assert operator.fail_job.await_args.kwargs["error_code"] == "ingestion_failed"
    audit_payload = control.append_audit_event.await_args_list[-1].args[2]
    assert audit_payload["error_code"] == "ingestion_failed"
    assert "vendor unavailable" not in str(audit_payload)


@pytest.mark.asyncio
async def test_job_creation_rejects_queue_when_audit_is_unavailable() -> None:
    queued = _job(OperatorJobState.QUEUED)
    operator = MagicMock()
    operator.create_job = AsyncMock(return_value=queued)
    operator.reject_queued_job = AsyncMock()
    control = MagicMock()
    control.append_audit_event = AsyncMock(
        side_effect=PersistenceUnavailableError("audit unavailable")
    )
    service = _service(operator=operator, control=control, runner=AsyncMock())

    with pytest.raises(PersistenceUnavailableError, match="audit is unavailable"):
        await service.create_daily_job(_request(), requested_by="operator")

    operator.reject_queued_job.assert_awaited_once_with(
        queued.job_id,
        error_code="audit_unavailable",
        now=NOW,
    )


@pytest.mark.asyncio
async def test_backtest_worker_commits_result_and_audits_only_hashes() -> None:
    backtests = MagicMock()
    backtests.complete_run = AsyncMock()
    backtests.fail_run = AsyncMock()
    result = MagicMock()
    result.result_hash = "b" * 64
    result.ledger_hash = "c" * 64
    result.manifest_hash = "a" * 64
    runner = MagicMock()
    runner.run = AsyncMock(return_value=result)
    control = MagicMock()
    control.append_audit_event = AsyncMock(return_value="d" * 64)
    service = _service(
        operator=MagicMock(),
        control=control,
        runner=AsyncMock(),
        backtests=backtests,
        backtest_runner=runner,
    )
    run = _backtest_run()

    await service._run_backtest(run)

    backtests.complete_run.assert_awaited_once_with(run.run_id, result=result, now=NOW)
    backtests.fail_run.assert_not_awaited()
    audit = control.append_audit_event.await_args_list[-1].args[2]
    assert audit["result_hash"] == "b" * 64
    assert set(audit) == {"job_id", "result_hash", "ledger_hash", "manifest_hash"}


@pytest.mark.asyncio
async def test_backtest_worker_exposes_stable_failure_code_not_exception_detail() -> None:
    backtests = MagicMock()
    backtests.complete_run = AsyncMock()
    backtests.fail_run = AsyncMock()
    runner = MagicMock()
    runner.run = AsyncMock(side_effect=ValueError("sensitive research detail"))
    control = MagicMock()
    control.append_audit_event = AsyncMock(return_value="d" * 64)
    service = _service(
        operator=MagicMock(),
        control=control,
        runner=AsyncMock(),
        backtests=backtests,
        backtest_runner=runner,
    )
    run = _backtest_run()

    await service._run_backtest(run)

    backtests.complete_run.assert_not_awaited()
    backtests.fail_run.assert_awaited_once_with(
        run.run_id, error_code="invalid_backtest_input", now=NOW
    )
    audit = control.append_audit_event.await_args_list[-1].args[2]
    assert "sensitive research detail" not in str(audit)


@pytest.mark.asyncio
async def test_validation_worker_commits_result_and_safe_audit_summary() -> None:
    validations = MagicMock()
    validations.complete_experiment = AsyncMock()
    validations.fail_experiment = AsyncMock()
    result = MagicMock()
    result.result_hash = "b" * 64
    result.manifest_hash = "a" * 64
    result.folds = (MagicMock(), MagicMock())
    runner = MagicMock()
    runner.run = AsyncMock(return_value=result)
    control = MagicMock()
    control.append_audit_event = AsyncMock(return_value="d" * 64)
    service = _service(
        operator=MagicMock(),
        control=control,
        runner=AsyncMock(),
        validations=validations,
        validation_runner=runner,
    )
    experiment = _validation_experiment()

    await service._run_validation(experiment)

    validations.complete_experiment.assert_awaited_once_with(
        experiment.experiment_id, result=result, now=NOW
    )
    validations.fail_experiment.assert_not_awaited()
    audit = control.append_audit_event.await_args_list[-1].args[2]
    assert audit["fold_count"] == 2
    assert set(audit) == {"job_id", "result_hash", "manifest_hash", "fold_count"}


@pytest.mark.asyncio
async def test_validation_worker_uses_stable_failure_code() -> None:
    validations = MagicMock()
    validations.complete_experiment = AsyncMock()
    validations.fail_experiment = AsyncMock()
    runner = MagicMock()
    runner.run = AsyncMock(side_effect=ValueError("do not expose this detail"))
    control = MagicMock()
    control.append_audit_event = AsyncMock(return_value="d" * 64)
    service = _service(
        operator=MagicMock(),
        control=control,
        runner=AsyncMock(),
        validations=validations,
        validation_runner=runner,
    )
    experiment = _validation_experiment()

    await service._run_validation(experiment)

    validations.fail_experiment.assert_awaited_once_with(
        experiment.experiment_id,
        error_code="invalid_validation_input",
        now=NOW,
    )
    audit = control.append_audit_event.await_args_list[-1].args[2]
    assert "do not expose this detail" not in str(audit)
