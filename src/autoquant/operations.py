from __future__ import annotations

from datetime import UTC, date, datetime
from uuid import UUID, uuid4

from pydantic import SecretStr

from autoquant.adapters.clickhouse_daily import ClickHouseDailyRepository
from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.adapters.tushare import TushareDailySource, TushareHttpClient
from autoquant.clock import to_shanghai
from autoquant.config import AppSettings, RuntimeEnvironment
from autoquant.data.calendar_refresh import TradingCalendarRefreshService
from autoquant.data.daily_ingestion import (
    DailyIngestionRequest,
    DailyIngestionService,
    ValidatedDailyDatasetReader,
)
from autoquant.data.daily_quality import DailyQualityGate
from autoquant.data.session_reference import SessionReferenceRefreshService
from autoquant.errors import MissingCapabilityError
from autoquant.execution.control import KillSwitchReason
from autoquant.execution.control_store import PostgresExecutionControlRepository
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
from autoquant.execution.qmt_quote_runtime import (
    ImportedXtDataClient,
    QmtFullTickSnapshotReader,
)
from autoquant.execution.session_risk_store import (
    PostgresPaperSessionRiskRepository,
)
from autoquant.execution.session_rules import ExactSessionRuleReader
from autoquant.execution.simulated_broker import PersistentSimulatedBroker
from autoquant.execution.store import PostgresPaperExecutionRepository
from autoquant.execution.strategy_registry_store import PostgresPaperStrategyRegistry
from autoquant.web.strategy_promotion import PaperStrategyPromotionService
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
    registry: PostgresPaperStrategyRegistry | None = None
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
        registry = PostgresPaperStrategyRegistry.connect(dsn=postgres_dsn)
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
                instruments=(registration.instrument,),
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
            "instrument": report.instrument,
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
    strategies: PostgresPaperStrategyRegistry | None = None
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
        strategies = PostgresPaperStrategyRegistry.connect(dsn=postgres_dsn)
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
                instruments=(registration.instrument,),
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
    registry = PostgresPaperStrategyRegistry.connect(dsn=postgres_dsn)
    now = datetime.now(UTC)
    try:
        fence = await controls.replay(account_id=settings.paper_account_id)
        if not fence.active:
            raise MissingCapabilityError(
                "paper strategy revocation requires the kill switch to remain active"
            )
        await registry.revoke(
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
