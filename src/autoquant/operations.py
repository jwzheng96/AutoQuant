from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import SecretStr

from autoquant.adapters.clickhouse_daily import ClickHouseDailyRepository
from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.adapters.tushare import TushareDailySource, TushareHttpClient
from autoquant.backtest.runner import ManifestMarketCompiler
from autoquant.backtest.validation import (
    SmaParameters,
    compile_common_calendar_markets,
)
from autoquant.clock import to_shanghai
from autoquant.config import AppSettings, RuntimeEnvironment
from autoquant.data.calendar_refresh import TradingCalendarRefreshService
from autoquant.data.daily_ingestion import (
    DailyIngestionRequest,
    DailyIngestionService,
    ValidatedDailyDataset,
    ValidatedDailyDatasetReader,
)
from autoquant.data.daily_quality import DailyQualityGate
from autoquant.data.session_reference import SessionReferenceRefreshService
from autoquant.errors import (
    MissingCapabilityError,
    PersistenceUnavailableError,
)
from autoquant.execution.control import KillSwitchReason
from autoquant.execution.control_store import PostgresExecutionControlRepository
from autoquant.execution.paper_deployment import (
    PostgresPaperDeploymentRegistry,
)
from autoquant.execution.paper_policy import default_paper_policy
from autoquant.execution.paper_runtime import (
    ExactTradingCalendarReader,
    PaperRuntimeReadinessGate,
)
from autoquant.execution.paper_scheduler_lease_store import (
    PostgresPaperSchedulerLeaseRepository,
)
from autoquant.execution.paper_scheduler_store import (
    PostgresPaperSchedulerRepository,
)
from autoquant.execution.paper_unlock import (
    PostgresPaperRuntimeUnlockRepository,
)
from autoquant.execution.paper_unlock_service import (
    PaperRuntimeUnlockService,
)
from autoquant.execution.pre_open_marks import DailyClosePreOpenMarkReader
from autoquant.execution.promotion_audit import (
    PaperPromotionAuditor,
    PaperPromotionPolicy,
    PostgresPaperPromotionFactRepository,
)
from autoquant.execution.qmt_preflight import inspect_qmt_readiness
from autoquant.execution.qmt_quote_runtime import (
    ImportedXtDataClient,
    QmtFullTickSnapshotReader,
)
from autoquant.execution.qmt_readonly_store import (
    PostgresQmtReadOnlyAcceptanceRepository,
    QmtReadOnlyAcceptanceEvidence,
)
from autoquant.execution.qmt_recovery_drill import (
    PostgresQmtRecoveryDrillRepository,
    QmtRecoveryDrillKind,
)
from autoquant.execution.qmt_session_store import (
    PostgresQmtSessionLeaseRepository,
)
from autoquant.execution.qmt_windows_readonly import (
    QmtReadOnlyAcceptance,
    QmtReadOnlyWindowsSession,
    QmtVendorBindings,
)
from autoquant.execution.session_risk_store import (
    PostgresPaperSessionRiskRepository,
)
from autoquant.execution.session_rules import ExactSessionRuleReader
from autoquant.execution.simulated_broker import PersistentSimulatedBroker
from autoquant.execution.store import PostgresPaperExecutionRepository
from autoquant.execution.strategy_portfolio_store import (
    PostgresPaperPortfolioRegistry,
)
from autoquant.execution.strategy_registry_store import PostgresPaperStrategyRegistry
from autoquant.execution.validated_sma_portfolio import (
    ValidatedSmaPortfolioRegistration,
)
from autoquant.web.strategy_promotion import (
    PaperPortfolioComponentApproval,
    PaperPortfolioPromotionService,
    PaperStrategyPromotionService,
)
from autoquant.web.validation_campaign_store import (
    PostgresValidationCampaignRepository,
    ValidationCampaignSpec,
    ValidationCampaignStatus,
)
from autoquant.web.validation_store import PostgresValidationRepository


def configured_dsn(value: SecretStr | None, *, capability: str) -> str:
    if value is None or not value.get_secret_value().strip():
        raise MissingCapabilityError(f"{capability} is not configured")
    return value.get_secret_value()


def tushare_source(settings: AppSettings) -> TushareDailySource:
    return TushareDailySource(
        client=TushareHttpClient(
            credentials=settings.require_tushare(),
            api_url=settings.tushare_api_url,
        ),
        now=lambda: datetime.now(UTC),
    )


async def run_daily_ingestion(
    settings: AppSettings,
    instruments: tuple[str, ...],
    start: date,
    end: date,
) -> dict[str, object]:
    source: TushareDailySource | None = None
    clickhouse: ClickHouseDailyRepository | None = None
    postgres: PostgresControlRepository | None = None
    try:
        source = tushare_source(settings)
        clickhouse = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(settings.clickhouse_dsn, capability="ClickHouse"),
            source="tushare",
        )
        postgres = PostgresControlRepository.connect(
            dsn=configured_dsn(settings.postgres_dsn, capability="PostgreSQL")
        )
        service = DailyIngestionService(
            source=source,
            quality_gate=DailyQualityGate(),
            market_repository=clickhouse,
            control_repository=postgres,
            now=lambda: datetime.now(UTC),
        )
        result = await service.run(
            DailyIngestionRequest(
                instruments=instruments,
                start=start,
                end=end,
                as_of=None,
                production_complete_requested=True,
            )
        )
    finally:
        if source is not None:
            await source.close()
        if postgres is not None:
            await postgres.close()
        if clickhouse is not None:
            await clickhouse.client.close()
    return {
        "fetched_bars": result.fetched_bars,
        "fetched_factors": result.fetched_factors,
        "manifest_hash": result.manifest_hash,
        "persisted_bars": result.persisted_bars,
        "persisted_factors": result.persisted_factors,
        "quality_hash": result.quality_hash,
        "status": result.status,
    }


async def create_validation_campaign(
    settings: AppSettings,
    *,
    campaign_key: str,
    manifest_hash: str,
    instruments: tuple[str, ...],
    allocation: Decimal,
    slippage_bps: Decimal,
    train_sessions: int,
    test_sessions: int,
    embargo_sessions: int,
    candidates: tuple[SmaParameters, ...],
    requested_by: str,
) -> dict[str, object]:
    """Preflight one common data cutoff and atomically queue aligned validations."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError(
            "validation campaigns require live trading to remain locked"
        )
    normalized = tuple(sorted(instruments))
    policy = default_paper_policy(normalized)
    if (
        len(normalized) < 3
        or len(normalized) > 20
        or len(set(normalized)) != len(normalized)
        or allocation > policy.max_position_weight
        or allocation * len(normalized) > policy.max_gross_exposure
    ):
        raise ValueError(
            "campaign universe or allocation exceeds paper risk controls"
        )
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    clickhouse: ClickHouseDailyRepository | None = None
    control: PostgresControlRepository | None = None
    campaigns: PostgresValidationCampaignRepository | None = None
    try:
        clickhouse = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(
                settings.clickhouse_dsn,
                capability="ClickHouse",
            ),
            source="tushare",
        )
        control = PostgresControlRepository.connect(dsn=postgres_dsn)
        campaigns = PostgresValidationCampaignRepository.connect(
            dsn=postgres_dsn
        )
        manifest = await control.read_manifest(manifest_hash)
        if (
            not manifest.production_complete
            or tuple(sorted(manifest.instruments)) != normalized
        ):
            raise ValueError(
                "campaign manifest must exactly cover the requested universe"
            )
        dataset = await ValidatedDailyDatasetReader(
            control_repository=control,
            market_repository=clickhouse,
        ).query(
            manifest.manifest_hash,
            manifest.as_of,
        )
        _validate_campaign_dataset(
            dataset=dataset,
            instruments=normalized,
            minimum_sessions=(
                train_sessions
                + embargo_sessions
                + 6 * test_sessions
            ),
            initial_cash=settings.paper_initial_cash,
            allocation=allocation,
            slippage_bps=slippage_bps,
            maximum_order_notional=policy.max_order_notional,
        )
        spec = ValidationCampaignSpec(
            campaign_key=campaign_key,
            manifest_hash=manifest.manifest_hash,
            instruments=normalized,
            initial_cash=settings.paper_initial_cash,
            allocation=allocation,
            slippage_bps=slippage_bps,
            train_sessions=train_sessions,
            test_sessions=test_sessions,
            embargo_sessions=embargo_sessions,
            candidates=candidates,
            requested_by=requested_by,
        )
        status = await campaigns.create(
            spec,
            created_at=datetime.now(UTC),
        )
        return _validation_campaign_payload(status)
    finally:
        if campaigns is not None:
            await campaigns.close()
        if control is not None:
            await control.close()
        if clickhouse is not None:
            await clickhouse.client.close()


async def inspect_validation_campaign(
    settings: AppSettings,
    *,
    campaign_hash: str,
) -> dict[str, object]:
    repository = PostgresValidationCampaignRepository.connect(
        dsn=configured_dsn(
            settings.postgres_dsn,
            capability="PostgreSQL",
        )
    )
    try:
        return _validation_campaign_payload(
            await repository.status(campaign_hash=campaign_hash)
        )
    finally:
        await repository.close()


def _validate_campaign_dataset(
    *,
    dataset: ValidatedDailyDataset,
    instruments: tuple[str, ...],
    minimum_sessions: int,
    initial_cash: Decimal,
    allocation: Decimal,
    slippage_bps: Decimal,
    maximum_order_notional: Decimal,
    compiler: ManifestMarketCompiler | None = None,
) -> None:
    market_compiler = compiler or ManifestMarketCompiler()
    bar_keys = tuple(
        (value.instrument, value.session_date)
        for value in dataset.bars
    )
    factor_keys = tuple(
        (value.instrument, value.session_date)
        for value in dataset.factors
    )
    common_markets = compile_common_calendar_markets(
        instruments=instruments,
        dataset=dataset,
        compiler=market_compiler,
    )
    common_dates = tuple(
        value.bar.session_date
        for value in common_markets[instruments[0]]
    )
    allocated_cash = initial_cash * allocation
    slippage_multiplier = (
        Decimal("1") + slippage_bps / Decimal("10000")
    )
    minimum_lots_affordable = all(
        (
            max(
                value.bar.pre_close,
                value.bar.high_price,
            )
            * value.rules.buy_minimum
            * slippage_multiplier
            <= allocated_cash
            and max(
                value.bar.pre_close,
                value.bar.high_price,
            )
            * value.rules.buy_minimum
            <= maximum_order_notional
        )
        for markets in common_markets.values()
        for value in markets
    )
    if (
        not instruments
        or len(set(bar_keys)) != len(bar_keys)
        or len(set(factor_keys)) != len(factor_keys)
        or set(bar_keys) != set(factor_keys)
        or {value[0] for value in bar_keys} != set(instruments)
        or len(common_dates) < minimum_sessions
        or not minimum_lots_affordable
    ):
        raise ValueError(
            "campaign data lacks aligned, adjusted, affordable minimum OOS history"
        )


def _validation_campaign_payload(
    status: ValidationCampaignStatus,
) -> dict[str, object]:
    return {
        "campaign_hash": status.spec.campaign_hash,
        "campaign_key": status.spec.campaign_key,
        "components": [
            {
                "evidence_status": value.evidence_status,
                "experiment_id": str(value.experiment_id),
                "gate_failures": list(value.gate_failures),
                "instrument": value.instrument,
                "state": value.state,
            }
            for value in status.components
        ],
        "created_at": status.created_at.isoformat(),
        "instrument_count": len(status.spec.instruments),
        "live_trading_locked": True,
        "manifest_hash": status.spec.manifest_hash,
        "status": status.status,
    }


async def inspect_paper_pre_open(
    settings: AppSettings,
    instruments: tuple[str, ...],
    as_of: datetime,
    manifest_hash: str,
) -> dict[str, object]:
    """Prove the database-backed pre-open valuation boundary without enabling trading."""

    if settings.environment.value != "paper":
        raise MissingCapabilityError("paper environment is not configured")
    clickhouse: ClickHouseDailyRepository | None = None
    evidence: PostgresControlRepository | None = None
    controls: PostgresExecutionControlRepository | None = None
    try:
        clickhouse = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(settings.clickhouse_dsn, capability="ClickHouse"),
            source="tushare",
        )
        controls = PostgresExecutionControlRepository.connect(
            dsn=configured_dsn(settings.postgres_dsn, capability="PostgreSQL")
        )
        evidence = PostgresControlRepository.connect(
            dsn=configured_dsn(settings.postgres_dsn, capability="PostgreSQL")
        )
        await clickhouse.check_connection()
        control = await controls.replay(account_id=settings.paper_account_id)
        if not control.active:
            raise MissingCapabilityError(
                "paper pre-open inspection requires the kill switch to remain active"
            )
        marks = await DailyClosePreOpenMarkReader(
            repository=clickhouse,
            evidence_repository=evidence,
            manifest_hash=manifest_hash,
            source="tushare",
        )(
            to_shanghai(as_of, name="paper pre-open inspection time").date(),
            instruments,
            as_of,
        )
        return {
            "instrument_count": len(marks.marks),
            "kill_switch_active": control.active,
            "marks_hash": marks.marks_hash,
            "session_date": marks.session_date.isoformat(),
            "source_evidence_hash": marks.source_evidence_hash,
            "status": "ok",
            "valuation_session_date": marks.valuation_session_date.isoformat(),
        }
    finally:
        if controls is not None:
            await controls.close()
        if evidence is not None:
            await evidence.close()
        if clickhouse is not None:
            await clickhouse.client.close()


async def inspect_paper_runtime_readiness(
    settings: AppSettings,
) -> dict[str, object]:
    """Replay cold-start evidence without opening QMT or resetting the kill switch."""

    if settings.environment is not RuntimeEnvironment.PAPER:
        raise MissingCapabilityError("paper environment is not configured")
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    clickhouse: ClickHouseDailyRepository | None = None
    evidence: PostgresControlRepository | None = None
    controls: PostgresExecutionControlRepository | None = None
    executions: PostgresPaperExecutionRepository | None = None
    broker: PersistentSimulatedBroker | None = None
    scheduler_events: PostgresPaperSchedulerRepository | None = None
    registry: PostgresPaperDeploymentRegistry | None = None
    try:
        clickhouse = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(
                settings.clickhouse_dsn,
                capability="ClickHouse",
            ),
            source="tushare",
        )
        evidence = PostgresControlRepository.connect(dsn=postgres_dsn)
        controls = PostgresExecutionControlRepository.connect(dsn=postgres_dsn)
        executions = PostgresPaperExecutionRepository.connect(dsn=postgres_dsn)
        broker = PersistentSimulatedBroker.connect(dsn=postgres_dsn)
        scheduler_events = PostgresPaperSchedulerRepository.connect(
            dsn=postgres_dsn
        )
        registry = PostgresPaperDeploymentRegistry.connect(dsn=postgres_dsn)
        cold_start_control = await controls.ensure_fail_closed(
            account_id=settings.paper_account_id,
            now=datetime.now(UTC),
        )
        if not cold_start_control.active:
            await controls.activate(
                account_id=settings.paper_account_id,
                command_id=f"paper-runtime-cold-start-{uuid4()}",
                reason=KillSwitchReason.DEPENDENCY_UNAVAILABLE,
                actor="paper-runtime-readiness",
                now=max(datetime.now(UTC), cold_start_control.changed_at),
            )
            raise MissingCapabilityError(
                "paper runtime cold start re-armed the inactive kill switch"
            )
        registration = await registry.active(
            account_id=settings.paper_account_id,
            strategy_id=settings.paper_strategy_id,
        )
        if registration is None:
            raise MissingCapabilityError(
                "paper runtime requires an active approved strategy"
            )
        now = datetime.now(UTC)
        report = await PaperRuntimeReadinessGate(
            account_id=settings.paper_account_id,
            strategy_id=settings.paper_strategy_id,
            controls=controls,
            strategies=registry,
            executions=executions,
            broker=broker,
            scheduler_events=scheduler_events,
            calendar=ExactTradingCalendarReader(
                instruments=registration.instruments,
                market_repository=clickhouse,
                control_repository=evidence,
            ),
        ).verify(now=now)
        return {
            "account_id": report.account_id,
            "broker_order_count": report.broker_order_count,
            "calendar_hash": report.calendar_hash,
            "checked_at": report.checked_at.isoformat(),
            "execution_order_count": report.execution_order_count,
            "instrument": (
                report.instruments[0]
                if len(report.instruments) == 1
                else None
            ),
            "instruments": list(report.instruments),
            "kill_switch_active": True,
            "live_trading_locked": True,
            "registration_hash": report.registration_hash,
            "scheduler_event_count": report.scheduler_event_count,
            "status": "ready_for_quote_connection",
            "strategy_id": report.strategy_id,
        }
    finally:
        if registry is not None:
            await registry.close()
        if scheduler_events is not None:
            await scheduler_events.close()
        if broker is not None:
            await broker.close()
        if executions is not None:
            await executions.close()
        if controls is not None:
            await controls.close()
        if evidence is not None:
            await evidence.close()
        if clickhouse is not None:
            await clickhouse.client.close()


async def inspect_paper_promotion(
    settings: AppSettings,
) -> dict[str, object]:
    """Evaluate redacted paper-to-live gates without changing runtime state."""

    if settings.environment is not RuntimeEnvironment.PAPER:
        raise MissingCapabilityError("paper environment is not configured")
    repository = PostgresPaperPromotionFactRepository.connect(
        dsn=configured_dsn(
            settings.postgres_dsn,
            capability="PostgreSQL",
        )
    )
    policy = PaperPromotionPolicy()
    try:
        facts = await repository.read(
            account_id=settings.paper_account_id,
            strategy_id=settings.paper_strategy_id,
            now=datetime.now(UTC),
            lookback_days=policy.evidence_lookback_days,
        )
        report = PaperPromotionAuditor(policy=policy).evaluate(facts)
        return {
            "blockers": [code.value for code in report.blockers],
            "evaluated_at": report.evaluated_at.isoformat(),
            "evidence_gates_passed": report.evidence_gates_passed,
            "fact_hash": report.fact_hash,
            "gates": {
                gate.code.value: {
                    "actual": gate.actual,
                    "required": gate.required,
                    "status": "pass" if gate.passed else "blocked",
                }
                for gate in report.gates
            },
            "live_trading_ready": report.live_trading_ready,
            "policy_hash": report.policy_hash,
            "report_hash": report.report_hash,
            "status": "blocked",
        }
    finally:
        await repository.close()


async def start_qmt_recovery_drill(
    settings: AppSettings,
    *,
    kind: QmtRecoveryDrillKind,
    actor: str,
) -> dict[str, object]:
    """Create a bounded drill challenge from fresh read-only QMT evidence."""

    if settings.environment is not RuntimeEnvironment.PAPER:
        raise MissingCapabilityError("paper environment is not configured")
    repository = PostgresQmtRecoveryDrillRepository.connect(
        dsn=configured_dsn(
            settings.postgres_dsn,
            capability="PostgreSQL",
        )
    )
    try:
        event = await repository.start(
            account_id=settings.paper_account_id,
            kind=kind,
            actor=actor,
            now=datetime.now(UTC),
        )
        return {
            "baseline_qmt_evidence_hash": (
                event.baseline_qmt_evidence_hash
            ),
            "drill_id": str(event.drill_id),
            "event_hash": event.event_hash,
            "expires_at": event.expires_at.isoformat(),
            "kind": event.kind.value,
            "live_trading_locked": True,
            "status": "drill_started",
        }
    finally:
        await repository.close()


async def complete_qmt_recovery_drill(
    settings: AppSettings,
    *,
    drill_id: UUID,
    actor: str,
) -> dict[str, object]:
    """Complete a drill only after fail-close and fresh QMT recovery evidence."""

    if settings.environment is not RuntimeEnvironment.PAPER:
        raise MissingCapabilityError("paper environment is not configured")
    repository = PostgresQmtRecoveryDrillRepository.connect(
        dsn=configured_dsn(
            settings.postgres_dsn,
            capability="PostgreSQL",
        )
    )
    try:
        event = await repository.complete(
            drill_id=drill_id,
            actor=actor,
            now=datetime.now(UTC),
        )
        return {
            "drill_id": str(event.drill_id),
            "event_hash": event.event_hash,
            "failure_control_event_hash": (
                event.failure_control_event_hash
            ),
            "kind": event.kind.value,
            "live_trading_locked": True,
            "recovery_qmt_evidence_hash": (
                event.recovery_qmt_evidence_hash
            ),
            "status": "drill_completed",
        }
    finally:
        await repository.close()


async def unlock_paper_runtime(
    settings: AppSettings,
    *,
    actor: str,
) -> dict[str, object]:
    """Reset only the paper control from a fresh Windows XtData evidence bundle."""

    if settings.environment is not RuntimeEnvironment.PAPER:
        raise MissingCapabilityError("paper environment is not configured")
    lease_credentials = settings.require_paper_runtime()
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    clickhouse: ClickHouseDailyRepository | None = None
    evidence: PostgresControlRepository | None = None
    controls: PostgresExecutionControlRepository | None = None
    executions: PostgresPaperExecutionRepository | None = None
    broker: PersistentSimulatedBroker | None = None
    sessions: PostgresPaperSessionRiskRepository | None = None
    strategies: PostgresPaperDeploymentRegistry | None = None
    leases: PostgresPaperSchedulerLeaseRepository | None = None
    unlocks: PostgresPaperRuntimeUnlockRepository | None = None
    try:
        clickhouse = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(
                settings.clickhouse_dsn,
                capability="ClickHouse",
            ),
            source="tushare",
        )
        evidence = PostgresControlRepository.connect(dsn=postgres_dsn)
        controls = PostgresExecutionControlRepository.connect(dsn=postgres_dsn)
        executions = PostgresPaperExecutionRepository.connect(dsn=postgres_dsn)
        broker = PersistentSimulatedBroker.connect(dsn=postgres_dsn)
        sessions = PostgresPaperSessionRiskRepository.connect(dsn=postgres_dsn)
        strategies = PostgresPaperDeploymentRegistry.connect(
            dsn=postgres_dsn
        )
        leases = PostgresPaperSchedulerLeaseRepository.connect(dsn=postgres_dsn)
        unlocks = PostgresPaperRuntimeUnlockRepository.connect(dsn=postgres_dsn)
        await unlocks.check_connection()
        registration = await strategies.active(
            account_id=settings.paper_account_id,
            strategy_id=settings.paper_strategy_id,
        )
        if registration is None:
            raise MissingCapabilityError(
                "paper unlock requires an active approved strategy"
            )
        result = await PaperRuntimeUnlockService(
            account_id=settings.paper_account_id,
            strategy_id=settings.paper_strategy_id,
            initial_cash=settings.paper_initial_cash,
            holder_id=lease_credentials.holder_id,
            lease_token=lease_credentials.lease_token,
            controls=controls,
            executions=executions,
            broker=broker,
            sessions=sessions,
            strategies=strategies,
            leases=leases,
            unlocks=unlocks,
            calendar=ExactTradingCalendarReader(
                instruments=registration.instruments,
                market_repository=clickhouse,
                control_repository=evidence,
            ),
            quotes=QmtFullTickSnapshotReader(
                client=ImportedXtDataClient.load(),
            ),
        ).unlock(actor=actor)
        return {
            "account_id": result.control.account_id,
            "control_state_hash": result.control.state_hash,
            "control_version": result.control.version,
            "evidence_hash": result.evidence.evidence_hash,
            "live_trading_locked": True,
            "status": "paper_unlocked",
            "strategy_id": result.evidence.strategy_id,
        }
    finally:
        if unlocks is not None:
            await unlocks.close()
        if leases is not None:
            await leases.close()
        if strategies is not None:
            await strategies.close()
        if sessions is not None:
            await sessions.close()
        if broker is not None:
            await broker.close()
        if executions is not None:
            await executions.close()
        if controls is not None:
            await controls.close()
        if evidence is not None:
            await evidence.close()
        if clickhouse is not None:
            await clickhouse.client.close()


async def run_qmt_readonly_acceptance(
    settings: AppSettings,
    *,
    actor: str,
) -> dict[str, object]:
    """Capture a redacted, lease-fenced XtTrader baseline without broker mutations."""

    if settings.environment is not RuntimeEnvironment.PAPER:
        raise MissingCapabilityError("paper environment is not configured")
    if not actor.strip() or len(actor) > 128:
        raise ValueError("QMT acceptance actor must contain 1-128 characters")
    userdata_path = settings.qmt_userdata_path
    account_secret = settings.qmt_account_id
    session_id = settings.qmt_session_id
    if userdata_path is None or account_secret is None or session_id is None:
        raise MissingCapabilityError("QMT read-only settings are not configured")
    broker_account_id = account_secret.get_secret_value().strip()
    if not broker_account_id:
        raise MissingCapabilityError("QMT account identifier is not configured")
    credentials = settings.require_qmt_runtime()
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    controls: PostgresExecutionControlRepository | None = None
    leases: PostgresQmtSessionLeaseRepository | None = None
    acceptances: PostgresQmtReadOnlyAcceptanceRepository | None = None
    acquired = False
    completed = False
    release_failed = False
    try:
        controls = PostgresExecutionControlRepository.connect(dsn=postgres_dsn)
        leases = PostgresQmtSessionLeaseRepository.connect(dsn=postgres_dsn)
        acceptances = PostgresQmtReadOnlyAcceptanceRepository.connect(
            dsn=postgres_dsn
        )
        await acceptances.check_connection()
        control = await controls.replay(account_id=settings.paper_account_id)
        active_session_ids = await leases.active_session_ids(
            now=datetime.now(UTC)
        )
        readiness = inspect_qmt_readiness(
            settings,
            kill_switch_active=control.active,
            active_session_ids=active_session_ids,
        )
        if not readiness.read_only_ready:
            blockers = ",".join(
                check.code.value
                for check in readiness.checks
                if not check.passed
            )
            raise MissingCapabilityError(
                f"QMT read-only preflight is blocked: {blockers}"
            )
        lease = await leases.acquire(
            session_id=session_id,
            holder_id=credentials.holder_id,
            token=credentials.lease_token,
            now=datetime.now(UTC),
            ttl=timedelta(seconds=settings.qmt_lease_ttl_seconds),
        )
        acquired = True
        bindings = QmtVendorBindings.load()

        def query_once() -> QmtReadOnlyAcceptance:
            qmt = QmtReadOnlyWindowsSession(
                userdata_path=userdata_path,
                session_id=session_id,
                broker_account_id=broker_account_id,
                logical_account_id=settings.paper_account_id,
                bindings=bindings,
            )
            qmt.open()
            try:
                return qmt.query()
            finally:
                qmt.close()

        acceptance = await asyncio.to_thread(query_once)
        lease = await leases.verify_owner(
            session_id=session_id,
            holder_id=credentials.holder_id,
            token=credentials.lease_token,
            now=datetime.now(UTC),
        )
        latest_control = await controls.replay(
            account_id=settings.paper_account_id
        )
        if not latest_control.active:
            raise MissingCapabilityError(
                "QMT acceptance requires the kill switch to remain active"
            )
        evidence = QmtReadOnlyAcceptanceEvidence.from_baseline(
            baseline=acceptance.baseline,
            package_manifest_hash=acceptance.package_manifest_hash,
            lease=lease,
        )
        evidence = await acceptances.append(
            evidence,
            now=datetime.now(UTC),
        )
        completed = True
        return {
            "account_snapshot_hash": evidence.account_snapshot_hash,
            "evidence_hash": evidence.evidence_hash,
            "live_trading_locked": True,
            "order_count": evidence.order_count,
            "position_count": evidence.position_count,
            "status": "qmt_readonly_accepted",
            "trade_count": evidence.trade_count,
        }
    finally:
        if acquired and leases is not None and session_id is not None:
            try:
                await leases.release(
                    session_id=session_id,
                    holder_id=credentials.holder_id,
                    token=credentials.lease_token,
                    now=datetime.now(UTC),
                )
            except Exception:
                completed = False
                release_failed = True
        if not completed and controls is not None:
            try:
                state = await controls.replay(
                    account_id=settings.paper_account_id
                )
                if not state.active:
                    await controls.activate(
                        account_id=settings.paper_account_id,
                        command_id=f"qmt-readonly-failure-{uuid4()}",
                        reason=KillSwitchReason.DEPENDENCY_UNAVAILABLE,
                        actor="qmt-readonly-acceptance",
                        now=datetime.now(UTC),
                    )
            except Exception:
                pass
        if acceptances is not None:
            await acceptances.close()
        if leases is not None:
            await leases.close()
        if controls is not None:
            await controls.close()
        if release_failed:
            raise PersistenceUnavailableError(
                "QMT session lease release failed"
            )


async def run_trading_calendar_refresh(
    settings: AppSettings,
    start: date,
    end: date,
) -> dict[str, object]:
    """Refresh exact Tushare calendar evidence without requesting incomplete daily bars."""

    source: TushareDailySource | None = None
    clickhouse: ClickHouseDailyRepository | None = None
    postgres: PostgresControlRepository | None = None
    try:
        source = tushare_source(settings)
        clickhouse = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(settings.clickhouse_dsn, capability="ClickHouse"),
            source="tushare",
        )
        postgres = PostgresControlRepository.connect(
            dsn=configured_dsn(settings.postgres_dsn, capability="PostgreSQL")
        )
        result = await TradingCalendarRefreshService(
            source=source,
            market_repository=clickhouse,
            control_repository=postgres,
            now=lambda: datetime.now(UTC),
        ).run(start=start, end=end)
    finally:
        if source is not None:
            await source.close()
        if postgres is not None:
            await postgres.close()
        if clickhouse is not None:
            await clickhouse.client.close()
    return {
        "audit_event_hash": result.audit_event_hash,
        "session_count": result.session_count,
        "source_evidence_hash": result.source_evidence_hash,
        "status": result.status,
    }


async def run_session_reference_refresh(
    settings: AppSettings,
    instruments: tuple[str, ...],
    session_date: date,
) -> dict[str, object]:
    """Refresh exact session controls without requesting the unfinished daily bar."""

    source: TushareDailySource | None = None
    clickhouse: ClickHouseDailyRepository | None = None
    postgres: PostgresControlRepository | None = None
    try:
        source = tushare_source(settings)
        clickhouse = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(settings.clickhouse_dsn, capability="ClickHouse"),
            source="tushare",
        )
        postgres = PostgresControlRepository.connect(
            dsn=configured_dsn(settings.postgres_dsn, capability="PostgreSQL")
        )
        result = await SessionReferenceRefreshService(
            source=source,
            market_repository=clickhouse,
            control_repository=postgres,
            now=lambda: datetime.now(UTC),
        ).run(instruments=instruments, session_date=session_date)
    finally:
        if source is not None:
            await source.close()
        if postgres is not None:
            await postgres.close()
        if clickhouse is not None:
            await clickhouse.client.close()
    return {
        "audit_event_hash": result.audit_event_hash,
        "instrument_count": result.instrument_count,
        "reference_hash": result.reference_hash,
        "session_date": result.session_date.isoformat(),
        "status": result.status,
    }


async def approve_paper_sma_strategy(
    settings: AppSettings,
    *,
    experiment_id: UUID,
    signal_manifest_hash: str,
    reference_session_date: date,
    approved_by: str,
) -> dict[str, object]:
    """Approve one OOS candidate for paper only while every safety fence remains active."""

    if settings.environment is not RuntimeEnvironment.PAPER:
        raise MissingCapabilityError("paper environment is not configured")
    now = datetime.now(UTC)
    shanghai_today = to_shanghai(now).date()
    lag_days = (shanghai_today - reference_session_date).days
    if lag_days < 0 or lag_days > 4:
        raise ValueError(
            "paper approval requires a current or recent exact session reference"
        )
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    clickhouse: ClickHouseDailyRepository | None = None
    control: PostgresControlRepository | None = None
    execution_controls: PostgresExecutionControlRepository | None = None
    validations: PostgresValidationRepository | None = None
    registry: PostgresPaperStrategyRegistry | None = None
    try:
        clickhouse = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(settings.clickhouse_dsn, capability="ClickHouse"),
            source="tushare",
        )
        control = PostgresControlRepository.connect(dsn=postgres_dsn)
        execution_controls = PostgresExecutionControlRepository.connect(
            dsn=postgres_dsn
        )
        validations = PostgresValidationRepository.connect(dsn=postgres_dsn)
        registry = PostgresPaperStrategyRegistry.connect(dsn=postgres_dsn)
        fence = await execution_controls.replay(
            account_id=settings.paper_account_id
        )
        if not fence.active:
            raise MissingCapabilityError(
                "paper strategy approval requires the kill switch to remain active"
            )
        detail = await validations.detail(experiment_id)
        instrument = detail.experiment.request.instrument
        rules = await ExactSessionRuleReader(
            market_repository=clickhouse,
            control_repository=control,
        ).read(
            instruments=(instrument,),
            session_date=reference_session_date,
            as_of=now,
        )
        if rules.suspended_instruments:
            raise ValueError("paper strategy cannot be approved while suspended")
        policy = default_paper_policy((instrument,))
        registration = await PaperStrategyPromotionService(
            validations=validations,
            controls=control,
            datasets=ValidatedDailyDatasetReader(
                control_repository=control,
                market_repository=clickhouse,
            ),
            registrations=registry,
        ).approve_sma(
            account_id=settings.paper_account_id,
            strategy_id=settings.paper_strategy_id,
            experiment_id=experiment_id,
            signal_manifest_hash=signal_manifest_hash,
            rules=rules.rules[0],
            policy=policy,
            approved_by=approved_by,
            approved_at=now,
        )
        return {
            "account_id": registration.account_id,
            "execution_mode": registration.execution_mode,
            "experiment_id": str(registration.experiment_id),
            "fast_sessions": registration.fast_sessions,
            "instrument": registration.instrument,
            "live_trading_locked": True,
            "registration_hash": registration.registration_hash,
            "signal_manifest_hash": registration.signal_manifest_hash,
            "slow_sessions": registration.slow_sessions,
            "status": "approved",
            "strategy_id": registration.strategy_id,
            "strategy_version": registration.strategy_version,
        }
    finally:
        if registry is not None:
            await registry.close()
        if validations is not None:
            await validations.close()
        if execution_controls is not None:
            await execution_controls.close()
        if control is not None:
            await control.close()
        if clickhouse is not None:
            await clickhouse.client.close()


async def approve_paper_sma_portfolio_strategy(
    settings: AppSettings,
    *,
    experiment_ids: tuple[UUID, ...],
    signal_manifest_hashes: tuple[str, ...],
    valuation_manifest_hash: str,
    reference_session_date: date,
    approved_by: str,
) -> dict[str, object]:
    """Approve 3-20 independently validated SMA components for paper only."""

    if settings.environment is not RuntimeEnvironment.PAPER:
        raise MissingCapabilityError("paper environment is not configured")
    if (
        len(experiment_ids) < 3
        or len(experiment_ids) > 20
        or len(experiment_ids) != len(signal_manifest_hashes)
    ):
        raise ValueError(
            "paper portfolio requires 3-20 matched experiments and manifests"
        )
    now = datetime.now(UTC)
    lag_days = (
        to_shanghai(now).date() - reference_session_date
    ).days
    if lag_days < 0 or lag_days > 4:
        raise ValueError(
            "paper approval requires a current or recent exact session reference"
        )
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    clickhouse: ClickHouseDailyRepository | None = None
    control: PostgresControlRepository | None = None
    execution_controls: PostgresExecutionControlRepository | None = None
    validations: PostgresValidationRepository | None = None
    registry: PostgresPaperPortfolioRegistry | None = None
    try:
        clickhouse = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(
                settings.clickhouse_dsn,
                capability="ClickHouse",
            ),
            source="tushare",
        )
        control = PostgresControlRepository.connect(dsn=postgres_dsn)
        execution_controls = PostgresExecutionControlRepository.connect(
            dsn=postgres_dsn
        )
        validations = PostgresValidationRepository.connect(
            dsn=postgres_dsn
        )
        registry = PostgresPaperPortfolioRegistry.connect(
            dsn=postgres_dsn
        )
        fence = await execution_controls.replay(
            account_id=settings.paper_account_id
        )
        if not fence.active:
            raise MissingCapabilityError(
                "portfolio approval requires the kill switch to remain active"
            )
        details = tuple(
            [
                await validations.detail(experiment_id)
                for experiment_id in experiment_ids
            ]
        )
        instruments = tuple(
            sorted(
                value.experiment.request.instrument
                for value in details
            )
        )
        if len(set(instruments)) != len(instruments):
            raise ValueError(
                "paper portfolio experiments must use unique instruments"
            )
        rule_set = await ExactSessionRuleReader(
            market_repository=clickhouse,
            control_repository=control,
        ).read(
            instruments=instruments,
            session_date=reference_session_date,
            as_of=now,
        )
        if rule_set.suspended_instruments:
            raise ValueError(
                "paper portfolio cannot be approved while a component is suspended"
            )
        rules_by_instrument = {
            value.instrument: value for value in rule_set.rules
        }
        if set(rules_by_instrument) != set(instruments):
            raise ValueError(
                "paper portfolio session rules are incomplete"
            )
        policy = default_paper_policy(instruments)
        registration = await PaperPortfolioPromotionService(
            validations=validations,
            controls=control,
            datasets=ValidatedDailyDatasetReader(
                control_repository=control,
                market_repository=clickhouse,
            ),
            registrations=registry,
        ).approve_sma_portfolio(
            account_id=settings.paper_account_id,
            strategy_id=settings.paper_strategy_id,
            components=tuple(
                PaperPortfolioComponentApproval(
                    experiment_id=experiment_id,
                    signal_manifest_hash=signal_manifest_hash,
                    rules=rules_by_instrument[
                        detail.experiment.request.instrument
                    ],
                )
                for experiment_id, signal_manifest_hash, detail in zip(
                    experiment_ids,
                    signal_manifest_hashes,
                    details,
                    strict=True,
                )
            ),
            valuation_manifest_hash=valuation_manifest_hash,
            policy=policy,
            expected_initial_cash=settings.paper_initial_cash,
            approved_by=approved_by,
            approved_at=now,
        )
        return {
            "account_id": registration.account_id,
            "components": [
                {
                    "experiment_id": str(value.experiment_id),
                    "fast_sessions": value.fast_sessions,
                    "instrument": value.instrument,
                    "signal_manifest_hash": value.signal_manifest_hash,
                    "slow_sessions": value.slow_sessions,
                }
                for value in registration.components
            ],
            "execution_mode": registration.execution_mode,
            "instruments": list(registration.instruments),
            "live_trading_locked": True,
            "oos_assessment": {
                "assessment_hash": (
                    registration.oos_assessment.assessment_hash
                ),
                "compounded_return": str(
                    registration.oos_assessment.compounded_return
                ),
                "fold_count": (
                    registration.oos_assessment.fold_count
                ),
                "maximum_component_contribution": str(
                    registration.oos_assessment
                    .maximum_component_contribution
                ),
                "maximum_drawdown": str(
                    registration.oos_assessment.maximum_drawdown
                ),
                "maximum_pairwise_correlation": (
                    None
                    if registration.oos_assessment
                    .maximum_pairwise_correlation is None
                    else str(
                        registration.oos_assessment
                        .maximum_pairwise_correlation
                    )
                ),
                "policy_hash": (
                    registration.oos_assessment.policy_hash
                ),
                "profitable_fold_rate": str(
                    registration.oos_assessment.profitable_fold_rate
                ),
            },
            "registration_hash": registration.registration_hash,
            "status": "approved",
            "strategy_id": registration.strategy_id,
            "strategy_version": registration.strategy_version,
            "valuation_manifest_hash": (
                registration.valuation_manifest_hash
            ),
        }
    finally:
        if registry is not None:
            await registry.close()
        if validations is not None:
            await validations.close()
        if execution_controls is not None:
            await execution_controls.close()
        if control is not None:
            await control.close()
        if clickhouse is not None:
            await clickhouse.client.close()


async def revoke_paper_strategy(
    settings: AppSettings,
    *,
    revoked_by: str,
    reason: str,
) -> dict[str, object]:
    """Append a paper-strategy revocation while keeping the kill switch active."""

    if settings.environment is not RuntimeEnvironment.PAPER:
        raise MissingCapabilityError("paper environment is not configured")
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    controls = PostgresExecutionControlRepository.connect(dsn=postgres_dsn)
    registry = PostgresPaperDeploymentRegistry.connect(dsn=postgres_dsn)
    now = datetime.now(UTC)
    try:
        fence = await controls.replay(account_id=settings.paper_account_id)
        if not fence.active:
            raise MissingCapabilityError(
                "paper strategy revocation requires the kill switch to remain active"
            )
        deployment = await registry.active(
            account_id=settings.paper_account_id,
            strategy_id=settings.paper_strategy_id,
        )
        if deployment is None:
            raise LookupError("paper deployment is not active")
        target = (
            registry.portfolios
            if isinstance(
                deployment,
                ValidatedSmaPortfolioRegistration,
            )
            else registry.singles
        )
        await target.revoke(
            account_id=settings.paper_account_id,
            strategy_id=settings.paper_strategy_id,
            revoked_by=revoked_by,
            reason=reason,
            revoked_at=now,
        )
        return {
            "account_id": settings.paper_account_id,
            "live_trading_locked": True,
            "status": "revoked",
            "strategy_id": settings.paper_strategy_id,
        }
    finally:
        await registry.close()
        await controls.close()
