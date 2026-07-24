from __future__ import annotations

import asyncio
import calendar
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import SecretStr

from autoquant.adapters.clickhouse_daily import ClickHouseDailyRepository
from autoquant.adapters.clickhouse_fundamental import (
    ClickHouseFundamentalRepository,
)
from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.adapters.tushare import TushareDailySource, TushareHttpClient
from autoquant.backtest.dynamic_panel import DynamicMarketPanelCompiler
from autoquant.backtest.dynamic_portfolio import (
    DYNAMIC_PORTFOLIO_STRATEGY_ID,
    DYNAMIC_REGIME_PORTFOLIO_SPEC_VERSION,
    DYNAMIC_REGIME_PORTFOLIO_STRATEGY_ID,
    DynamicPortfolioResearchSpec,
    DynamicRegimeFilter,
)
from autoquant.backtest.dynamic_validation import (
    DynamicWalkForwardValidator,
    assess_dynamic_validation,
)
from autoquant.backtest.fundamental_panel import (
    FundamentalMarketBinding,
    FundamentalMarketSessionBinding,
    FundamentalPanelCompiler,
    FundamentalResearchPanel,
)
from autoquant.backtest.fundamental_portfolio import (
    FundamentalPortfolioResearchSpec,
)
from autoquant.backtest.fundamental_strategy import (
    compile_fundamental_executable_panel,
)
from autoquant.backtest.fundamental_validation import (
    FundamentalWalkForwardValidator,
    assess_fundamental_validation,
)
from autoquant.backtest.low_volatility_forward import (
    LowVolatilityForwardEvidenceSpec,
    LowVolatilityForwardSessionBinding,
)
from autoquant.backtest.low_volatility_portfolio import (
    LowVolatilityResearchSpec,
)
from autoquant.backtest.low_volatility_strategy import (
    compile_low_volatility_executable_panel,
)
from autoquant.backtest.low_volatility_validation import (
    LowVolatilityWalkForwardValidator,
    assess_low_volatility_validation,
)
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
from autoquant.data.fundamental_dataset import (
    FundamentalResearchDatasetManifest,
    ValidatedFundamentalDatasetReader,
)
from autoquant.data.fundamental_ingestion import (
    FundamentalIngestionRequest,
    FundamentalIngestionService,
)
from autoquant.data.fundamental_quality import FundamentalQualityGate
from autoquant.data.research_data_campaign import ResearchDataCampaignSpec
from autoquant.data.research_input import (
    ExactManifestResearchDatasetReader,
    ResearchInputPlan,
    ResearchUniverseBinding,
    ValidatedResearchDatasetReader,
    compile_research_input_plan,
)
from autoquant.data.session_reference import SessionReferenceRefreshService
from autoquant.data.universe import (
    PointInTimeUniversePolicy,
    build_point_in_time_universe,
)
from autoquant.errors import (
    AutoQuantError,
    MissingCapabilityError,
    PersistenceUnavailableError,
    VendorAuthenticationError,
    VendorPermissionError,
    VendorRateLimitError,
    VendorResponseError,
)
from autoquant.execution.compliance_approval import (
    ComplianceApproval,
    ComplianceRevocation,
    ComplianceRevocationReason,
    PostgresComplianceApprovalRepository,
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
from autoquant.execution.qmt_lease_guard import (
    QmtSessionLeaseGuard,
    run_fenced_blocking,
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
from autoquant.web.dynamic_research_store import (
    PostgresDynamicResearchSpecRepository,
)
from autoquant.web.dynamic_validation_store import (
    PostgresDynamicValidationRepository,
)
from autoquant.web.fundamental_data_store import (
    PostgresFundamentalDatasetRepository,
)
from autoquant.web.fundamental_panel_store import (
    PostgresFundamentalPanelRepository,
)
from autoquant.web.fundamental_research_store import (
    PostgresFundamentalResearchSpecRepository,
)
from autoquant.web.fundamental_validation_store import (
    FundamentalValidationRecord,
    PostgresFundamentalValidationRepository,
)
from autoquant.web.low_volatility_forward_session_store import (
    PostgresLowVolatilityForwardSessionRepository,
)
from autoquant.web.low_volatility_forward_store import (
    PostgresLowVolatilityForwardEvidenceSpecRepository,
)
from autoquant.web.low_volatility_research_store import (
    PostgresLowVolatilityResearchSpecRepository,
)
from autoquant.web.low_volatility_validation_store import (
    LowVolatilityValidationRecord,
    PostgresLowVolatilityValidationRepository,
)
from autoquant.web.models import (
    PortfolioValidationExperiment,
    PortfolioWalkForwardJobRequest,
    ResearchUniverseSnapshotView,
)
from autoquant.web.portfolio_validation_store import (
    PostgresPortfolioValidationRepository,
)
from autoquant.web.research_data_store import (
    PostgresResearchDataCampaignRepository,
    ResearchDataCampaignStatus,
)
from autoquant.web.strategy_promotion import (
    PaperPortfolioComponentApproval,
    PaperPortfolioPromotionService,
    PaperStrategyPromotionService,
)
from autoquant.web.universe_store import (
    PostgresResearchUniverseRepository,
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


class _RecyclingDailyDatasetReader:
    def __init__(
        self,
        *,
        clickhouse_dsn: str,
        control_repository: PostgresControlRepository,
        recycle_after: int = 5,
    ) -> None:
        if recycle_after < 1 or recycle_after > 100:
            raise ValueError("ClickHouse recycle interval is invalid")
        self._dsn = clickhouse_dsn
        self._control = control_repository
        self._recycle_after = recycle_after
        self._market: ClickHouseDailyRepository | None = None
        self._reader: ValidatedDailyDatasetReader | None = None
        self._queries = 0

    async def query(
        self,
        manifest_hash: str,
        as_of: datetime,
    ) -> ValidatedDailyDataset:
        if self._reader is None or self._queries >= self._recycle_after:
            await self._reconnect()
        reader = self._reader
        if reader is None:
            raise PersistenceUnavailableError("ClickHouse recycling reader is unavailable")
        try:
            dataset = await reader.query(manifest_hash, as_of)
        except PersistenceUnavailableError:
            await self.close()
            raise
        self._queries += 1
        return dataset

    async def close(self) -> None:
        if self._market is not None:
            await self._market.client.close()
        self._market = None
        self._reader = None
        self._queries = 0

    async def _reconnect(self) -> None:
        await self.close()
        self._market = await ClickHouseDailyRepository.connect(
            dsn=self._dsn,
            source="tushare",
        )
        await self._purge_allocator(strict=True)
        self._reader = ValidatedDailyDatasetReader(
            control_repository=self._control,
            market_repository=self._market,
        )

    async def _purge_allocator(self, *, strict: bool) -> None:
        if self._market is None:
            return
        try:
            await self._market.client.command("SYSTEM JEMALLOC PURGE")
        except Exception:
            if strict:
                raise PersistenceUnavailableError("ClickHouse allocator purge failed") from None


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


async def run_fundamental_ingestion(
    settings: AppSettings,
    instruments: tuple[str, ...],
    start: date,
    end: date,
) -> dict[str, object]:
    if settings.live_trading_enabled:
        raise MissingCapabilityError("fundamental ingestion requires live trading locked")
    source: TushareDailySource | None = None
    clickhouse: ClickHouseFundamentalRepository | None = None
    postgres: PostgresControlRepository | None = None
    try:
        source = tushare_source(settings)
        clickhouse = await ClickHouseFundamentalRepository.connect(
            dsn=configured_dsn(
                settings.clickhouse_dsn,
                capability="ClickHouse",
            ),
            source="tushare",
        )
        postgres = PostgresControlRepository.connect(
            dsn=configured_dsn(
                settings.postgres_dsn,
                capability="PostgreSQL",
            )
        )
        service = FundamentalIngestionService(
            source=source,
            quality_gate=FundamentalQualityGate(),
            fundamental_repository=clickhouse,
            control_repository=postgres,
            now=lambda: datetime.now(UTC),
        )
        result = await service.run(
            FundamentalIngestionRequest(
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
        "fetched_indicators": result.fetched_indicators,
        "fetched_valuations": result.fetched_valuations,
        "manifest_hash": result.manifest_hash,
        "persisted_indicators": result.persisted_indicators,
        "persisted_valuations": result.persisted_valuations,
        "quality_hash": result.quality_hash,
        "status": result.status,
    }


async def create_research_universe_snapshot(
    settings: AppSettings,
    *,
    index_code: str,
    reference_date: date,
    minimum_turnover_rate_f: Decimal,
    minimum_circulating_market_value: Decimal,
    requested_by: str,
) -> dict[str, object]:
    if settings.live_trading_enabled:
        raise MissingCapabilityError("research universe creation requires live trading locked")
    if not requested_by.strip() or requested_by != requested_by.strip() or len(requested_by) > 128:
        raise ValueError("requested_by must contain 1-128 trimmed characters")
    source: TushareDailySource | None = None
    control: PostgresControlRepository | None = None
    repository: PostgresResearchUniverseRepository | None = None
    now = datetime.now(UTC)
    try:
        source = tushare_source(settings)
        dsn = configured_dsn(
            settings.postgres_dsn,
            capability="PostgreSQL",
        )
        control = PostgresControlRepository.connect(dsn=dsn)
        repository = PostgresResearchUniverseRepository.connect(dsn=dsn)
        policy = PointInTimeUniversePolicy(
            index_code=index_code,
            minimum_members=250,
            maximum_members=350,
            minimum_turnover_rate_f=minimum_turnover_rate_f,
            minimum_circulating_market_value=(minimum_circulating_market_value),
        )
        existing = await repository.find(
            policy_hash=policy.policy_hash,
            reference_date=reference_date,
        )
        if existing is not None:
            return _research_universe_payload(
                existing.snapshot,
                status="existing",
            )
        batch = await source.fetch_index_universe(
            index_code=index_code,
            reference_date=reference_date,
        )
        snapshot = build_point_in_time_universe(
            policy=policy,
            reference_date=reference_date,
            constituents=batch.constituents,
            liquidity=batch.liquidity,
        )
        async with control.transaction() as transaction:
            for evidence in batch.source_evidence:
                await transaction.save_source_evidence(evidence)
            await transaction.save_research_universe(
                snapshot,
                created_at=now,
            )
            await transaction.append_audit_event(
                "research.universe.created",
                now,
                {
                    "index_code": index_code,
                    "member_count": len(snapshot.members),
                    "policy_hash": snapshot.policy.policy_hash,
                    "reference_date": reference_date.isoformat(),
                    "requested_by": requested_by,
                    "snapshot_hash": snapshot.snapshot_hash,
                },
            )
        detail = await repository.detail(snapshot.snapshot_hash)
        return _research_universe_payload(
            detail.snapshot,
            status="created",
        )
    finally:
        if repository is not None:
            await repository.close()
        if control is not None:
            await control.close()
        if source is not None:
            await source.close()


def _research_universe_payload(
    snapshot: ResearchUniverseSnapshotView,
    *,
    status: str,
) -> dict[str, object]:
    return {
        "index_constituent_date": (snapshot.index_constituent_date.isoformat()),
        "index_code": snapshot.index_code,
        "knowledge_as_of": snapshot.knowledge_as_of.isoformat(),
        "liquidity_date": snapshot.liquidity_date.isoformat(),
        "live_trading_locked": True,
        "member_count": snapshot.member_count,
        "policy_hash": snapshot.policy_hash,
        "reference_date": snapshot.reference_date.isoformat(),
        "snapshot_hash": snapshot.snapshot_hash,
        "status": status,
    }


async def backfill_research_universe_snapshots(
    settings: AppSettings,
    *,
    start_month: date,
    end_month: date,
    index_code: str,
    requested_by: str,
) -> dict[str, object]:
    if settings.live_trading_enabled:
        raise MissingCapabilityError("research universe backfill requires live trading locked")
    if not requested_by.strip() or requested_by != requested_by.strip() or len(requested_by) > 128:
        raise ValueError("requested_by must contain 1-128 trimmed characters")
    months = _month_intervals(start_month, end_month)
    source = tushare_source(settings)
    control = PostgresControlRepository.connect(
        dsn=configured_dsn(
            settings.postgres_dsn,
            capability="PostgreSQL",
        )
    )
    results: list[dict[str, object]] = []
    safe_reference_cutoff = to_shanghai(datetime.now(UTC)).date() - timedelta(days=1)
    try:
        for month_start, month_end in months:
            bounded_end = min(month_end, safe_reference_cutoff)
            if month_start > bounded_end:
                raise ValueError("universe backfill cannot include a future month")
            calendar_batch = await source.fetch_trading_calendar(
                month_start,
                bounded_end,
            )
            open_dates = tuple(
                value.session_date for value in calendar_batch.sessions if value.is_open
            )
            if not open_dates:
                raise ValueError("universe backfill month contains no open session")
            await control.save_source_evidence(calendar_batch.source_evidence[0])
            results.append(
                await create_research_universe_snapshot(
                    settings,
                    index_code=index_code,
                    reference_date=max(open_dates),
                    minimum_turnover_rate_f=Decimal("0"),
                    minimum_circulating_market_value=Decimal("0"),
                    requested_by=requested_by,
                )
            )
        await control.append_audit_event(
            "research.universe.backfill.completed",
            datetime.now(UTC),
            {
                "index_code": index_code,
                "month_count": len(months),
                "requested_by": requested_by,
                "snapshot_hashes": [str(value["snapshot_hash"]) for value in results],
            },
        )
        return {
            "created_count": sum(value["status"] == "created" for value in results),
            "existing_count": sum(value["status"] == "existing" for value in results),
            "live_trading_locked": True,
            "month_count": len(months),
            "snapshot_hashes": [str(value["snapshot_hash"]) for value in results],
            "status": "completed",
        }
    finally:
        await control.close()
        await source.close()


def _month_intervals(
    start_month: date,
    end_month: date,
) -> tuple[tuple[date, date], ...]:
    start = start_month.replace(day=1)
    end = end_month.replace(day=1)
    if start_month != start or end_month != end or start > end:
        raise ValueError("backfill bounds must be ordered first days of months")
    values: list[tuple[date, date]] = []
    current = start
    while current <= end:
        last_day = calendar.monthrange(
            current.year,
            current.month,
        )[1]
        values.append((current, current.replace(day=last_day)))
        current = (
            date(current.year + 1, 1, 1)
            if current.month == 12
            else date(current.year, current.month + 1, 1)
        )
    if len(values) > 12:
        raise ValueError("one universe backfill cannot exceed 12 months")
    return tuple(values)


async def create_research_data_campaign(
    settings: AppSettings,
    *,
    campaign_key: str,
    index_code: str,
    start_date: date,
    end_date: date,
    requested_by: str,
    max_attempts: int = 3,
) -> dict[str, object]:
    """Freeze one survivorship-free daily data plan without enabling trading."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError("research data campaign creation requires live trading locked")
    if start_date > end_date:
        raise ValueError("research data campaign start cannot follow end")
    safe_cutoff = to_shanghai(datetime.now(UTC)).date() - timedelta(days=1)
    if end_date > safe_cutoff:
        raise ValueError("research data campaign cannot include future data")
    dsn = configured_dsn(settings.postgres_dsn, capability="PostgreSQL")
    universes = PostgresResearchUniverseRepository.connect(dsn=dsn)
    campaigns = PostgresResearchDataCampaignRepository.connect(dsn=dsn)
    control = PostgresControlRepository.connect(dsn=dsn)
    try:
        views = tuple(
            sorted(
                (
                    value
                    for value in await universes.list(limit=200)
                    if value.index_code == index_code
                    and start_date <= value.reference_date <= end_date
                ),
                key=lambda value: value.reference_date,
            )
        )
        _require_monthly_snapshot_coverage(
            views=views,
            start_date=start_date,
            end_date=end_date,
        )
        policy_hashes = {value.policy_hash for value in views}
        if len(policy_hashes) != 1:
            raise ValueError("research data campaign requires one universe policy")
        details = tuple([await universes.detail(value.snapshot_hash) for value in views])
        instruments = tuple(
            sorted({member.instrument for detail in details for member in detail.members})
        )
        spec = ResearchDataCampaignSpec(
            campaign_key=campaign_key,
            policy_hash=next(iter(policy_hashes)),
            snapshot_hashes=tuple(value.snapshot_hash for value in views),
            instruments=instruments,
            start_date=start_date,
            end_date=end_date,
            requested_by=requested_by,
            max_attempts=max_attempts,
        )
        status = await campaigns.create(spec, created_at=datetime.now(UTC))
        await control.append_audit_event(
            "research.data.campaign.created",
            datetime.now(UTC),
            {
                "campaign_hash": spec.campaign_hash,
                "end_date": end_date.isoformat(),
                "index_code": index_code,
                "instrument_count": len(spec.instruments),
                "requested_by": requested_by,
                "snapshot_count": len(spec.snapshot_hashes),
                "start_date": start_date.isoformat(),
            },
        )
        return _research_data_campaign_payload(status)
    finally:
        await control.close()
        await campaigns.close()
        await universes.close()


async def inspect_research_data_campaign(
    settings: AppSettings,
    *,
    campaign_hash: str,
) -> dict[str, object]:
    repository = PostgresResearchDataCampaignRepository.connect(
        dsn=configured_dsn(
            settings.postgres_dsn,
            capability="PostgreSQL",
        )
    )
    try:
        return _research_data_campaign_payload(await repository.status(campaign_hash=campaign_hash))
    finally:
        await repository.close()


async def compile_research_input(
    settings: AppSettings,
    *,
    manifest_hash: str,
    requested_by: str,
) -> dict[str, object]:
    """Compile an immutable aggregate manifest into a point-in-time plan."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError("research input compilation requires live trading locked")
    if not requested_by.strip() or requested_by != requested_by.strip() or len(requested_by) > 128:
        raise ValueError("requested_by must contain 1-128 trimmed characters")
    dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    campaigns = PostgresResearchDataCampaignRepository.connect(dsn=dsn)
    universes = PostgresResearchUniverseRepository.connect(dsn=dsn)
    control = PostgresControlRepository.connect(dsn=dsn)
    try:
        plan = await _load_research_input_plan(
            campaigns=campaigns,
            universes=universes,
            manifest_hash=manifest_hash,
        )
        payload: dict[str, object] = {
            "activation_rule": plan.activation_rule,
            "campaign_hash": plan.campaign_hash,
            "end_date": plan.end_date.isoformat(),
            "first_reference_date": (plan.universes[0].reference_date.isoformat()),
            "instrument_count": len(plan.shards),
            "last_reference_date": (plan.universes[-1].reference_date.isoformat()),
            "live_trading_locked": True,
            "manifest_hash": plan.dataset_manifest_hash,
            "plan_hash": plan.plan_hash,
            "policy_hash": plan.policy_hash,
            "snapshot_count": len(plan.universes),
            "start_date": plan.start_date.isoformat(),
            "status": "compiled",
            "version": plan.version,
        }
        await control.append_audit_event(
            "research.input.plan.compiled",
            datetime.now(UTC),
            {
                **payload,
                "requested_by": requested_by,
            },
        )
        return payload
    finally:
        await control.close()
        await universes.close()
        await campaigns.close()


async def freeze_dynamic_research_spec(
    settings: AppSettings,
    *,
    manifest_hash: str,
    requested_by: str,
) -> dict[str, object]:
    """Freeze the next dynamic strategy before any result is observed."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError(
            "dynamic research pre-registration requires live trading locked"
        )
    if not requested_by.strip() or requested_by != requested_by.strip() or len(requested_by) > 128:
        raise ValueError("requested_by must contain 1-128 trimmed characters")
    dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    campaigns = PostgresResearchDataCampaignRepository.connect(dsn=dsn)
    universes = PostgresResearchUniverseRepository.connect(dsn=dsn)
    specifications = PostgresDynamicResearchSpecRepository.connect(dsn=dsn)
    control = PostgresControlRepository.connect(dsn=dsn)
    try:
        plan = await _load_research_input_plan(
            campaigns=campaigns,
            universes=universes,
            manifest_hash=manifest_hash,
        )
        spec = DynamicPortfolioResearchSpec(
            dataset_manifest_hash=plan.dataset_manifest_hash,
            plan_hash=plan.plan_hash,
            policy_hash=plan.policy_hash,
            start_date=plan.start_date,
            end_date=plan.end_date,
        )
        record = await specifications.freeze(
            spec,
            requested_by=requested_by,
            created_at=datetime.now(UTC),
        )
        payload: dict[str, object] = {
            "candidate_count": len(record.spec.candidates),
            "created_at": record.created_at.isoformat(),
            "dataset_manifest_hash": (record.spec.dataset_manifest_hash),
            "embargo_sessions": record.spec.embargo_sessions,
            "evidence_policy_hash": (record.spec.evidence_policy.policy_hash),
            "gross_allocation": str(record.spec.gross_allocation),
            "live_trading_locked": record.live_trading_locked,
            "maximum_position_weight": str(record.spec.maximum_position_weight),
            "plan_hash": record.spec.plan_hash,
            "requested_by": record.requested_by,
            "slippage_bps": str(record.spec.slippage_bps),
            "spec_hash": record.spec.spec_hash,
            "status": "frozen",
            "strategy_id": record.spec.strategy_id,
            "test_sessions": record.spec.test_sessions,
            "train_sessions": record.spec.train_sessions,
            "version": record.spec.version,
        }
        await control.append_audit_event(
            "research.dynamic_strategy.spec.frozen",
            datetime.now(UTC),
            {
                **payload,
                "specification": record.spec.payload(),
            },
        )
        return payload
    finally:
        await control.close()
        await specifications.close()
        await universes.close()
        await campaigns.close()


async def compile_dynamic_market_panel(
    settings: AppSettings,
    *,
    spec_hash: str,
    requested_by: str,
) -> dict[str, object]:
    """Compile all frozen shards into a point-in-time dynamic market panel."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError(
            "dynamic market panel compilation requires live trading locked"
        )
    if not requested_by.strip() or requested_by != requested_by.strip() or len(requested_by) > 128:
        raise ValueError("requested_by must contain 1-128 trimmed characters")
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    specifications = PostgresDynamicResearchSpecRepository.connect(dsn=postgres_dsn)
    campaigns = PostgresResearchDataCampaignRepository.connect(dsn=postgres_dsn)
    universes = PostgresResearchUniverseRepository.connect(dsn=postgres_dsn)
    control = PostgresControlRepository.connect(dsn=postgres_dsn)
    dataset_reader: _RecyclingDailyDatasetReader | None = None
    try:
        record = await specifications.read(spec_hash)
        plan = await _load_research_input_plan(
            campaigns=campaigns,
            universes=universes,
            manifest_hash=record.spec.dataset_manifest_hash,
        )
        dataset_reader = _RecyclingDailyDatasetReader(
            clickhouse_dsn=configured_dsn(
                settings.clickhouse_dsn,
                capability="ClickHouse",
            ),
            control_repository=control,
        )
        shard_reader = ValidatedResearchDatasetReader(
            plan=plan,
            manifest_reader=control,
            dataset_reader=dataset_reader,
        )
        panel = await DynamicMarketPanelCompiler(
            shard_reader=shard_reader,
        ).compile(
            plan=plan,
            spec=record.spec,
        )
        market_count = sum(len(value.markets) for value in panel.histories)
        payload: dict[str, object] = {
            "as_of": panel.as_of.isoformat(),
            "dataset_manifest_hash": panel.dataset_manifest_hash,
            "first_session": (panel.sessions[0].session_date.isoformat()),
            "history_count": len(panel.histories),
            "last_session": (panel.sessions[-1].session_date.isoformat()),
            "live_trading_locked": True,
            "market_state_count": market_count,
            "maximum_active_members": max(len(value.active_members) for value in panel.sessions),
            "minimum_active_members": min(len(value.active_members) for value in panel.sessions),
            "panel_hash": panel.panel_hash,
            "plan_hash": panel.plan_hash,
            "session_count": len(panel.sessions),
            "spec_hash": panel.spec_hash,
            "status": "compiled",
            "version": panel.version,
        }
        await control.append_audit_event(
            "research.dynamic_market_panel.compiled",
            datetime.now(UTC),
            {
                **payload,
                "requested_by": requested_by,
            },
        )
        return payload
    finally:
        if dataset_reader is not None:
            await dataset_reader.close()
        await control.close()
        await universes.close()
        await campaigns.close()
        await specifications.close()


async def run_dynamic_validation(
    settings: AppSettings,
    *,
    spec_hash: str,
    requested_by: str,
) -> dict[str, object]:
    """Run and persist the frozen nested walk-forward validation."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError("dynamic validation requires live trading locked")
    if not requested_by.strip() or requested_by != requested_by.strip() or len(requested_by) > 128:
        raise ValueError("requested_by must contain 1-128 trimmed characters")
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    specifications = PostgresDynamicResearchSpecRepository.connect(dsn=postgres_dsn)
    validations = PostgresDynamicValidationRepository.connect(dsn=postgres_dsn)
    campaigns = PostgresResearchDataCampaignRepository.connect(dsn=postgres_dsn)
    universes = PostgresResearchUniverseRepository.connect(dsn=postgres_dsn)
    control = PostgresControlRepository.connect(dsn=postgres_dsn)
    dataset_reader: _RecyclingDailyDatasetReader | None = None
    try:
        spec_record = await specifications.read(spec_hash)
        plan = await _load_research_input_plan(
            campaigns=campaigns,
            universes=universes,
            manifest_hash=(spec_record.spec.dataset_manifest_hash),
        )
        dataset_reader = _RecyclingDailyDatasetReader(
            clickhouse_dsn=configured_dsn(
                settings.clickhouse_dsn,
                capability="ClickHouse",
            ),
            control_repository=control,
        )
        panel = await DynamicMarketPanelCompiler(
            shard_reader=ValidatedResearchDatasetReader(
                plan=plan,
                manifest_reader=control,
                dataset_reader=dataset_reader,
            )
        ).compile(
            plan=plan,
            spec=spec_record.spec,
        )
        result = DynamicWalkForwardValidator().run(
            panel=panel,
            spec=spec_record.spec,
        )
        evidence = assess_dynamic_validation(
            result,
            policy=spec_record.spec.evidence_policy,
        )
        completed_at = datetime.now(UTC)
        record = await validations.save(
            result,
            evidence,
            requested_by=requested_by,
            completed_at=completed_at,
        )
        payload: dict[str, object] = {
            "assessment_hash": (record.evidence.assessment_hash),
            "benchmark_compounded_oos_return": str(record.result.benchmark_compounded_oos_return),
            "compounded_oos_return": str(record.result.compounded_oos_return),
            "evidence_status": (record.evidence.evidence_status),
            "excess_oos_return": str(record.result.excess_oos_return),
            "fold_count": record.evidence.fold_count,
            "gate_failures": list(record.evidence.gate_failures),
            "live_trading_locked": True,
            "oos_sessions": record.evidence.oos_sessions,
            "panel_hash": record.result.panel_hash,
            "profitable_fold_rate": str(record.result.profitable_fold_rate),
            "rejected_order_count": (record.evidence.rejected_order_count),
            "result_hash": record.result.result_hash,
            "selection_optimism": str(record.result.selection_optimism),
            "spec_hash": record.result.spec_hash,
            "status": "completed",
            "worst_oos_drawdown": str(record.result.worst_oos_drawdown),
        }
        await control.append_audit_event(
            "research.dynamic_validation.completed",
            completed_at,
            {
                **payload,
                "requested_by": requested_by,
            },
        )
        return payload
    finally:
        if dataset_reader is not None:
            await dataset_reader.close()
        await control.close()
        await universes.close()
        await campaigns.close()
        await validations.close()
        await specifications.close()


async def freeze_dynamic_regime_research_spec(
    settings: AppSettings,
    *,
    predecessor_result_hash: str,
    requested_by: str,
) -> dict[str, object]:
    """Pre-register v2 only from an immutable rejected v1 result."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError("dynamic regime specification requires live trading locked")
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    specifications = PostgresDynamicResearchSpecRepository.connect(dsn=postgres_dsn)
    validations = PostgresDynamicValidationRepository.connect(dsn=postgres_dsn)
    control = PostgresControlRepository.connect(dsn=postgres_dsn)
    try:
        predecessor = await validations.read(predecessor_result_hash)
        if predecessor.evidence.evidence_status != "rejected":
            raise ValueError("dynamic regime v2 requires a rejected predecessor")
        base_record = await specifications.read(predecessor.result.spec_hash)
        base = base_record.spec
        if base.regime_filter is not None or base.strategy_id != DYNAMIC_PORTFOLIO_STRATEGY_ID:
            raise ValueError("dynamic regime predecessor must be the v1 strategy")
        regime = DynamicRegimeFilter(
            predecessor_result_hash=(predecessor.result.result_hash),
        )
        spec = DynamicPortfolioResearchSpec(
            dataset_manifest_hash=base.dataset_manifest_hash,
            plan_hash=base.plan_hash,
            policy_hash=base.policy_hash,
            start_date=base.start_date,
            end_date=base.end_date,
            initial_cash=base.initial_cash,
            gross_allocation=base.gross_allocation,
            maximum_position_weight=base.maximum_position_weight,
            maximum_order_notional=base.maximum_order_notional,
            slippage_bps=base.slippage_bps,
            maximum_volume_participation=(base.maximum_volume_participation),
            train_sessions=base.train_sessions,
            test_sessions=base.test_sessions,
            embargo_sessions=base.embargo_sessions,
            signal_lag_sessions=base.signal_lag_sessions,
            minimum_member_history_sessions=(base.minimum_member_history_sessions),
            candidates=base.candidates,
            regime_filter=regime,
            evidence_policy=base.evidence_policy,
            strategy_id=DYNAMIC_REGIME_PORTFOLIO_STRATEGY_ID,
            benchmark_version=base.benchmark_version,
            valuation_version=base.valuation_version,
            version=DYNAMIC_REGIME_PORTFOLIO_SPEC_VERSION,
        )
        created_at = datetime.now(UTC)
        record = await specifications.freeze(
            spec,
            requested_by=requested_by,
            created_at=created_at,
        )
        payload: dict[str, object] = {
            "created_at": record.created_at.isoformat(),
            "dataset_manifest_hash": (record.spec.dataset_manifest_hash),
            "live_trading_locked": True,
            "minimum_positive_breadth": str(regime.minimum_positive_breadth),
            "predecessor_result_hash": (regime.predecessor_result_hash),
            "regime_lookback_sessions": (regime.lookback_sessions),
            "spec_hash": record.spec.spec_hash,
            "status": "frozen",
            "strategy_id": record.spec.strategy_id,
            "version": record.spec.version,
        }
        await control.append_audit_event(
            "research.dynamic_regime.spec.frozen",
            created_at,
            {
                **payload,
                "requested_by": requested_by,
                "specification": record.spec.payload(),
            },
        )
        return payload
    finally:
        await control.close()
        await validations.close()
        await specifications.close()


async def freeze_fundamental_research_spec(
    settings: AppSettings,
    *,
    predecessor_result_hash: str,
    requested_by: str,
) -> dict[str, object]:
    """Pre-register v3 only from the immutable rejected v2 result."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError("fundamental specification requires live trading locked")
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    dynamic_specs = PostgresDynamicResearchSpecRepository.connect(dsn=postgres_dsn)
    validations = PostgresDynamicValidationRepository.connect(dsn=postgres_dsn)
    fundamentals = PostgresFundamentalResearchSpecRepository.connect(dsn=postgres_dsn)
    control = PostgresControlRepository.connect(dsn=postgres_dsn)
    try:
        predecessor = await validations.read(predecessor_result_hash)
        if predecessor.evidence.evidence_status != "rejected":
            raise ValueError("fundamental v3 requires a rejected predecessor")
        dynamic_record = await dynamic_specs.read(predecessor.result.spec_hash)
        base = dynamic_record.spec
        if base.regime_filter is None or base.strategy_id != DYNAMIC_REGIME_PORTFOLIO_STRATEGY_ID:
            raise ValueError("fundamental predecessor must be the v2 strategy")
        spec = FundamentalPortfolioResearchSpec(
            predecessor_result_hash=(predecessor.result.result_hash),
            daily_dataset_manifest_hash=(base.dataset_manifest_hash),
            plan_hash=base.plan_hash,
            universe_policy_hash=base.policy_hash,
            start_date=base.start_date,
            end_date=base.end_date,
            evidence_policy=base.evidence_policy,
            initial_cash=base.initial_cash,
            gross_allocation=base.gross_allocation,
            maximum_position_weight=(base.maximum_position_weight),
            maximum_order_notional=base.maximum_order_notional,
            slippage_bps=base.slippage_bps,
            maximum_volume_participation=(base.maximum_volume_participation),
            train_sessions=base.train_sessions,
            test_sessions=base.test_sessions,
            embargo_sessions=base.embargo_sessions,
            signal_lag_sessions=base.signal_lag_sessions,
        )
        created_at = datetime.now(UTC)
        record = await fundamentals.freeze(
            spec,
            requested_by=requested_by,
            created_at=created_at,
        )
        payload: dict[str, object] = {
            "created_at": record.created_at.isoformat(),
            "data_policy_hash": (record.spec.data_policy.policy_hash),
            "factor_count": len(record.spec.factors),
            "live_trading_locked": True,
            "predecessor_result_hash": (record.spec.predecessor_result_hash),
            "rebalance_sessions": (record.spec.rebalance_sessions),
            "selection_count": record.spec.selection_count,
            "spec_hash": record.spec.spec_hash,
            "status": "frozen",
            "strategy_id": record.spec.strategy_id,
            "version": record.spec.version,
        }
        await control.append_audit_event(
            "research.fundamental.spec.frozen",
            created_at,
            {
                **payload,
                "requested_by": requested_by,
                "specification": record.spec.payload(),
            },
        )
        return payload
    finally:
        await control.close()
        await fundamentals.close()
        await validations.close()
        await dynamic_specs.close()


async def freeze_low_volatility_research_spec(
    settings: AppSettings,
    *,
    predecessor_result_hash: str,
    requested_by: str,
) -> dict[str, object]:
    """Pre-register v4 only from an immutable rejected v3 result."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError("low-volatility specification requires live trading locked")
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    predecessors = PostgresFundamentalValidationRepository.connect(dsn=postgres_dsn)
    fundamental_specs = PostgresFundamentalResearchSpecRepository.connect(dsn=postgres_dsn)
    specifications = PostgresLowVolatilityResearchSpecRepository.connect(dsn=postgres_dsn)
    control = PostgresControlRepository.connect(dsn=postgres_dsn)
    try:
        predecessor = await predecessors.read(predecessor_result_hash)
        if predecessor.evidence.evidence_status != "rejected":
            raise ValueError("low-volatility v4 requires a rejected predecessor")
        fundamental = await fundamental_specs.read(predecessor.result.spec_hash)
        base = fundamental.spec
        spec = LowVolatilityResearchSpec(
            predecessor_result_hash=(predecessor.result.result_hash),
            dataset_manifest_hash=(base.daily_dataset_manifest_hash),
            plan_hash=base.plan_hash,
            policy_hash=base.universe_policy_hash,
            start_date=base.start_date,
            end_date=base.end_date,
            evidence_policy=base.evidence_policy,
            initial_cash=base.initial_cash,
            gross_allocation=base.gross_allocation,
            maximum_position_weight=(base.maximum_position_weight),
            maximum_order_notional=(base.maximum_order_notional),
            slippage_bps=base.slippage_bps,
            maximum_volume_participation=(base.maximum_volume_participation),
            train_sessions=base.train_sessions,
            test_sessions=base.test_sessions,
            embargo_sessions=base.embargo_sessions,
            signal_lag_sessions=base.signal_lag_sessions,
        )
        created_at = datetime.now(UTC)
        record = await specifications.freeze(
            spec,
            requested_by=requested_by,
            created_at=created_at,
        )
        payload: dict[str, object] = {
            "created_at": record.created_at.isoformat(),
            "live_trading_locked": True,
            "minimum_history_sessions": (record.spec.minimum_history_sessions),
            "predecessor_result_hash": (record.spec.predecessor_result_hash),
            "rebalance_sessions": (record.spec.rebalance_sessions),
            "selection_count": record.spec.selection_count,
            "spec_hash": record.spec.spec_hash,
            "status": "frozen",
            "strategy_id": record.spec.strategy_id,
            "version": record.spec.version,
            "volatility_lookback_sessions": (record.spec.volatility_lookback_sessions),
        }
        await control.append_audit_event(
            "research.low_volatility.spec.frozen",
            created_at,
            {
                **payload,
                "requested_by": requested_by,
                "specification": record.spec.payload(),
            },
        )
        return payload
    finally:
        await control.close()
        await specifications.close()
        await fundamental_specs.close()
        await predecessors.close()


async def freeze_low_volatility_forward_evidence_spec(
    settings: AppSettings,
    *,
    predecessor_result_hash: str,
    requested_by: str,
) -> dict[str, object]:
    """Freeze a future-only methodology correction after rejected v4."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError("forward evidence specification requires live trading locked")
    if not requested_by.strip() or requested_by != requested_by.strip() or len(requested_by) > 128:
        raise ValueError("requested_by must contain 1-128 trimmed characters")
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    validations = PostgresLowVolatilityValidationRepository.connect(dsn=postgres_dsn)
    source_specs = PostgresLowVolatilityResearchSpecRepository.connect(dsn=postgres_dsn)
    forward_specs = PostgresLowVolatilityForwardEvidenceSpecRepository.connect(dsn=postgres_dsn)
    control = PostgresControlRepository.connect(dsn=postgres_dsn)
    try:
        predecessor = await validations.read(predecessor_result_hash)
        if (
            predecessor.evidence.evidence_status != "rejected"
            or predecessor.evidence.gate_failures != ("train_test_gap",)
        ):
            raise ValueError("forward correction requires the isolated v4 stability-gate rejection")
        source_record = await source_specs.read(predecessor.result.spec_hash)
        source = source_record.spec
        policy = source.evidence_policy
        spec = LowVolatilityForwardEvidenceSpec(
            predecessor_result_hash=(predecessor.result.result_hash),
            predecessor_assessment_hash=(predecessor.evidence.assessment_hash),
            source_spec_hash=source.spec_hash,
            source_dataset_manifest_hash=(source.dataset_manifest_hash),
            forward_start_date=(source.end_date + timedelta(days=1)),
            maximum_annualized_stability_gap=(policy.maximum_selection_optimism),
            minimum_profitable_block_rate=(policy.minimum_profitable_fold_rate),
            maximum_forward_drawdown=(policy.maximum_oos_drawdown),
            minimum_forward_compounded_return=(policy.minimum_compounded_oos_return),
            minimum_forward_excess_return=(policy.minimum_excess_oos_return),
            maximum_rejected_orders=(policy.maximum_rejected_orders),
        )
        created_at = datetime.now(UTC)
        record = await forward_specs.freeze(
            spec,
            requested_by=requested_by,
            created_at=created_at,
        )
        payload: dict[str, object] = {
            "created_at": record.created_at.isoformat(),
            "formal_hypothesis_count": (record.spec.formal_hypothesis_count),
            "forward_start_date": (record.spec.forward_start_date.isoformat()),
            "historical_result_eligible_for_promotion": False,
            "live_trading_locked": True,
            "minimum_forward_sessions": (record.spec.minimum_forward_sessions),
            "minimum_paper_sessions": (record.spec.minimum_paper_sessions),
            "outcome_observed_at_design": True,
            "predecessor_result_hash": (record.spec.predecessor_result_hash),
            "retrospective_reclassification_allowed": False,
            "spec_hash": record.spec.spec_hash,
            "stability_method_version": (record.spec.stability_method_version),
            "status": "frozen_awaiting_forward_data",
            "strategy_parameters_unchanged": True,
            "version": record.spec.version,
        }
        await control.append_audit_event(
            "research.low_volatility.forward_evidence.frozen",
            created_at,
            {
                **payload,
                "requested_by": requested_by,
                "specification": record.spec.payload(),
            },
        )
        return payload
    finally:
        await control.close()
        await forward_specs.close()
        await source_specs.close()
        await validations.close()


async def create_low_volatility_forward_session_campaign(
    settings: AppSettings,
    *,
    forward_spec_hash: str,
    session_date: date,
    requested_by: str,
) -> dict[str, object]:
    """Create one resumable, point-in-time forward-session data queue."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError("forward session collection requires live trading locked")
    if not requested_by.strip() or requested_by != requested_by.strip() or len(requested_by) > 128:
        raise ValueError("requested_by must contain 1-128 trimmed characters")
    safe_cutoff = to_shanghai(datetime.now(UTC)).date() - timedelta(days=1)
    if session_date > safe_cutoff:
        raise ValueError("forward session must be a completed Shanghai date")
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    forward_specs = PostgresLowVolatilityForwardEvidenceSpecRepository.connect(dsn=postgres_dsn)
    source_specs = PostgresLowVolatilityResearchSpecRepository.connect(dsn=postgres_dsn)
    universes = PostgresResearchUniverseRepository.connect(dsn=postgres_dsn)
    campaigns = PostgresResearchDataCampaignRepository.connect(dsn=postgres_dsn)
    control = PostgresControlRepository.connect(dsn=postgres_dsn)
    market: ClickHouseDailyRepository | None = None
    try:
        forward = (await forward_specs.read(forward_spec_hash)).spec
        if session_date < forward.forward_start_date:
            raise ValueError("forward session precedes the frozen start date")
        source = (await source_specs.read(forward.source_spec_hash)).spec
        now = datetime.now(UTC)
        market = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(
                settings.clickhouse_dsn,
                capability="ClickHouse",
            ),
            source="tushare",
        )
        sessions = await market.query_sessions_as_of(
            session_date,
            session_date,
            now,
        )
        if (
            len(sessions) != 1
            or sessions[0].session_date != session_date
            or not sessions[0].is_open
        ):
            raise ValueError("forward session is not proven open")
        candidates = tuple(
            value
            for value in await universes.list(limit=200)
            if value.policy_hash == source.policy_hash and value.reference_date < session_date
        )
        if not candidates:
            raise LookupError("forward session has no prior universe snapshot")
        snapshot = max(
            candidates,
            key=lambda value: (
                value.reference_date,
                value.snapshot_hash,
            ),
        )
        detail = await universes.detail(snapshot.snapshot_hash)
        instruments = tuple(sorted(member.instrument for member in detail.members))
        campaign_spec = ResearchDataCampaignSpec(
            campaign_key=(f"low-vol-forward:{forward.spec_hash[:16]}:{session_date:%Y%m%d}"),
            policy_hash=source.policy_hash,
            snapshot_hashes=(snapshot.snapshot_hash,),
            instruments=instruments,
            start_date=session_date,
            end_date=session_date,
            requested_by=requested_by,
        )
        status = await campaigns.create(
            campaign_spec,
            created_at=now,
        )
        payload: dict[str, object] = {
            "campaign_hash": campaign_spec.campaign_hash,
            "completed_items": sum(value.state == "completed" for value in status.items),
            "forward_spec_hash": forward.spec_hash,
            "instrument_count": len(instruments),
            "live_trading_locked": True,
            "session_date": session_date.isoformat(),
            "snapshot_hash": snapshot.snapshot_hash,
            "snapshot_reference_date": (snapshot.reference_date.isoformat()),
            "status": status.status,
        }
        await control.append_audit_event(
            "research.low_volatility.forward_session.created",
            now,
            {
                **payload,
                "calendar_content_hash": (sessions[0].content_hash),
                "requested_by": requested_by,
            },
        )
        return payload
    finally:
        if market is not None:
            await market.client.close()
        await control.close()
        await campaigns.close()
        await universes.close()
        await source_specs.close()
        await forward_specs.close()


async def finalize_low_volatility_forward_session(
    settings: AppSettings,
    *,
    forward_spec_hash: str,
    dataset_manifest_hash: str,
    requested_by: str,
) -> dict[str, object]:
    """Verify and immutably bind one completed forward-session dataset."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError("forward session finalization requires live trading locked")
    if not requested_by.strip() or requested_by != requested_by.strip() or len(requested_by) > 128:
        raise ValueError("requested_by must contain 1-128 trimmed characters")
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    forward_specs = PostgresLowVolatilityForwardEvidenceSpecRepository.connect(dsn=postgres_dsn)
    source_specs = PostgresLowVolatilityResearchSpecRepository.connect(dsn=postgres_dsn)
    sessions = PostgresLowVolatilityForwardSessionRepository.connect(dsn=postgres_dsn)
    campaigns = PostgresResearchDataCampaignRepository.connect(dsn=postgres_dsn)
    universes = PostgresResearchUniverseRepository.connect(dsn=postgres_dsn)
    control = PostgresControlRepository.connect(dsn=postgres_dsn)
    market: ClickHouseDailyRepository | None = None
    try:
        forward = (await forward_specs.read(forward_spec_hash)).spec
        source = (await source_specs.read(forward.source_spec_hash)).spec
        dataset = await campaigns.read_manifest(dataset_manifest_hash)
        if (
            dataset.source != "tushare"
            or dataset.start_date != dataset.end_date
            or dataset.start_date < forward.forward_start_date
            or dataset.policy_hash != source.policy_hash
            or len(dataset.snapshot_hashes) != 1
        ):
            raise ValueError("forward dataset does not match the frozen evidence spec")
        session_date = dataset.start_date
        snapshot_hash = dataset.snapshot_hashes[0]
        snapshot = await universes.detail(snapshot_hash)
        instruments = tuple(sorted(member.instrument for member in snapshot.members))
        if (
            snapshot.snapshot.snapshot_hash != snapshot_hash
            or snapshot.snapshot.policy_hash != source.policy_hash
            or snapshot.snapshot.reference_date >= session_date
            or instruments != dataset.instruments
        ):
            raise ValueError("forward universe snapshot does not precede and match the session")
        shard_cutoffs: list[datetime] = []
        for shard in dataset.shards:
            manifest = await control.read_manifest(shard.manifest_hash)
            if (
                manifest.source != "tushare"
                or not manifest.production_complete
                or manifest.instruments != (shard.instrument,)
                or to_shanghai(manifest.start_time).date() != session_date
                or to_shanghai(manifest.end_time).date() != session_date
            ):
                raise ValueError("forward dataset shard failed exact-session verification")
            shard_cutoffs.append(manifest.as_of)
        calendar_as_of = min(shard_cutoffs)
        market = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(
                settings.clickhouse_dsn,
                capability="ClickHouse",
            ),
            source="tushare",
        )
        calendar = await market.query_sessions_as_of(
            session_date,
            session_date,
            calendar_as_of,
        )
        if (
            len(calendar) != 1
            or calendar[0].session_date != session_date
            or not calendar[0].is_open
        ):
            raise ValueError("forward dataset session is not proven open as of collection")
        binding = LowVolatilityForwardSessionBinding(
            forward_spec_hash=forward.spec_hash,
            dataset_manifest_hash=dataset.manifest_hash,
            policy_hash=source.policy_hash,
            session_date=session_date,
            snapshot_hash=snapshot_hash,
            snapshot_reference_date=(snapshot.snapshot.reference_date),
            calendar_content_hash=calendar[0].content_hash,
            instruments=instruments,
        )
        completed_at = datetime.now(UTC)
        record = await sessions.freeze(
            binding,
            requested_by=requested_by,
            completed_at=completed_at,
        )
        payload: dict[str, object] = {
            "binding_hash": record.binding.binding_hash,
            "calendar_as_of": calendar_as_of.isoformat(),
            "completed_at": record.completed_at.isoformat(),
            "dataset_manifest_hash": (record.binding.dataset_manifest_hash),
            "forward_spec_hash": (record.binding.forward_spec_hash),
            "instrument_count": len(record.binding.instruments),
            "live_trading_locked": True,
            "session_date": (record.binding.session_date.isoformat()),
            "snapshot_hash": record.binding.snapshot_hash,
            "snapshot_reference_date": (record.binding.snapshot_reference_date.isoformat()),
            "status": "frozen",
            "version": record.binding.version,
        }
        await control.append_audit_event(
            "research.low_volatility.forward_session.frozen",
            completed_at,
            {
                **payload,
                "calendar_content_hash": (record.binding.calendar_content_hash),
                "policy_hash": record.binding.policy_hash,
                "requested_by": requested_by,
            },
        )
        return payload
    finally:
        if market is not None:
            await market.client.close()
        await control.close()
        await universes.close()
        await campaigns.close()
        await sessions.close()
        await source_specs.close()
        await forward_specs.close()


async def run_low_volatility_forward_cycle(
    settings: AppSettings,
    *,
    forward_spec_hash: str,
    requested_by: str,
    max_items: int,
    pause_seconds: Decimal,
) -> dict[str, object]:
    """Advance the earliest missing completed forward session once."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError("forward cycle requires live trading locked")
    if not requested_by.strip() or requested_by != requested_by.strip() or len(requested_by) > 128:
        raise ValueError("requested_by must contain 1-128 trimmed characters")
    if max_items < 1 or max_items > 25:
        raise ValueError("max_items must be between 1 and 25")
    if pause_seconds < 0 or pause_seconds > Decimal("60"):
        raise ValueError("pause_seconds must be between 0 and 60")
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    forward_specs = PostgresLowVolatilityForwardEvidenceSpecRepository.connect(dsn=postgres_dsn)
    sessions = PostgresLowVolatilityForwardSessionRepository.connect(dsn=postgres_dsn)
    market: ClickHouseDailyRepository | None = None
    try:
        forward = (await forward_specs.read(forward_spec_hash)).spec
        records = await sessions.list_for_spec(forward_spec_hash=forward.spec_hash)
        now = datetime.now(UTC)
        safe_cutoff = to_shanghai(now).date() - timedelta(days=1)
        market = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(
                settings.clickhouse_dsn,
                capability="ClickHouse",
            ),
            source="tushare",
        )
        calendar = (
            ()
            if safe_cutoff < forward.forward_start_date
            else await market.query_sessions_as_of(
                forward.forward_start_date,
                safe_cutoff,
                now,
            )
        )
        open_dates = tuple(value.session_date for value in calendar if value.is_open)
        bound_dates = tuple(value.binding.session_date for value in records)
        conflicts = tuple(value for value in bound_dates if value not in set(open_dates))
        if conflicts:
            raise ValueError("forward cycle found a calendar conflict")
        target = _next_low_volatility_forward_session(
            open_dates=open_dates,
            bound_dates=bound_dates,
            minimum_sessions=(forward.minimum_forward_sessions),
        )
        completed_required = len(
            set(bound_dates).intersection(open_dates[: forward.minimum_forward_sessions])
        )
        base: dict[str, object] = {
            "completed_required_sessions": (completed_required),
            "forward_spec_hash": forward.spec_hash,
            "live_trading_locked": True,
            "minimum_forward_sessions": (forward.minimum_forward_sessions),
            "remaining_required_sessions": (forward.minimum_forward_sessions - completed_required),
            "safe_cutoff_date": safe_cutoff.isoformat(),
        }
        if target is None:
            base["status"] = (
                "session_gate_complete_awaiting_evaluation"
                if len(open_dates) >= forward.minimum_forward_sessions
                else "waiting_for_completed_session"
            )
            return base
    finally:
        if market is not None:
            await market.client.close()
        await sessions.close()
        await forward_specs.close()

    creation = await create_low_volatility_forward_session_campaign(
        settings,
        forward_spec_hash=forward_spec_hash,
        session_date=target,
        requested_by=requested_by,
    )
    campaign_hash = str(creation["campaign_hash"])
    batch = await run_research_data_campaign(
        settings,
        campaign_hash=campaign_hash,
        max_items=max_items,
        pause_seconds=pause_seconds,
    )
    payload = {
        **base,
        "campaign_hash": campaign_hash,
        "item_counts": batch["item_counts"],
        "manifest_hash": batch["manifest_hash"],
        "session_date": target.isoformat(),
        "status": "batch_progress",
    }
    if batch["status"] == "completed" and isinstance(batch["manifest_hash"], str):
        frozen = await finalize_low_volatility_forward_session(
            settings,
            forward_spec_hash=forward_spec_hash,
            dataset_manifest_hash=batch["manifest_hash"],
            requested_by=requested_by,
        )
        payload.update(
            {
                "binding_hash": frozen["binding_hash"],
                "completed_required_sessions": (completed_required + 1),
                "remaining_required_sessions": (
                    forward.minimum_forward_sessions - completed_required - 1
                ),
                "status": "session_frozen",
            }
        )
    elif batch["status"] == "failed":
        payload["status"] = "failed"
    return payload


async def inspect_fundamental_data_backfill(
    settings: AppSettings,
    *,
    spec_hash: str,
) -> dict[str, object]:
    dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    specifications = PostgresFundamentalResearchSpecRepository.connect(dsn=dsn)
    daily_datasets = PostgresResearchDataCampaignRepository.connect(dsn=dsn)
    fundamentals = PostgresFundamentalDatasetRepository.connect(dsn=dsn)
    try:
        spec_record = await specifications.read(spec_hash)
        daily = await daily_datasets.read_manifest(spec_record.spec.daily_dataset_manifest_hash)
        if (
            daily.start_date != spec_record.spec.start_date
            or daily.end_date != spec_record.spec.end_date
            or daily.policy_hash != spec_record.spec.universe_policy_hash
        ):
            raise ValueError("fundamental spec daily dataset binding differs")
        frozen = await fundamentals.read_for_spec(spec_hash)
        completed = await fundamentals.completed_shards(
            instruments=daily.instruments,
            start_date=spec_record.spec.start_date,
            end_date=spec_record.spec.end_date,
        )
        return {
            "completed_instruments": len(completed),
            "dataset_manifest_hash": (None if frozen is None else frozen.manifest_hash),
            "live_trading_locked": True,
            "remaining_instruments": (len(daily.instruments) - len(completed)),
            "spec_hash": spec_hash,
            "status": ("completed" if frozen is not None else "collecting"),
            "total_instruments": len(daily.instruments),
        }
    finally:
        await fundamentals.close()
        await daily_datasets.close()
        await specifications.close()


async def run_fundamental_data_backfill(
    settings: AppSettings,
    *,
    spec_hash: str,
    max_items: int,
    pause_seconds: Decimal,
) -> dict[str, object]:
    """Resume a bounded v3 data batch from immutable shard manifests."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError("fundamental data backfill requires live trading locked")
    if max_items < 1 or max_items > 25:
        raise ValueError("max_items must be between 1 and 25")
    if pause_seconds < 0 or pause_seconds > Decimal("60"):
        raise ValueError("pause_seconds must be between 0 and 60")
    dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    specifications = PostgresFundamentalResearchSpecRepository.connect(dsn=dsn)
    daily_datasets = PostgresResearchDataCampaignRepository.connect(dsn=dsn)
    fundamentals = PostgresFundamentalDatasetRepository.connect(dsn=dsn)
    control = PostgresControlRepository.connect(dsn=dsn)
    source: TushareDailySource | None = None
    clickhouse: ClickHouseFundamentalRepository | None = None
    processed = 0
    completed_now = 0
    failed = 0
    maintenance_wait = False
    inactive_bytes = 0
    inactive_parts = 0
    try:
        spec_record = await specifications.read(spec_hash)
        spec = spec_record.spec
        daily = await daily_datasets.read_manifest(spec.daily_dataset_manifest_hash)
        if (
            daily.start_date != spec.start_date
            or daily.end_date != spec.end_date
            or daily.policy_hash != spec.universe_policy_hash
            or daily.instruments != tuple(sorted(daily.instruments))
        ):
            raise ValueError("fundamental spec daily dataset binding differs")
        frozen = await fundamentals.read_for_spec(spec.spec_hash)
        if frozen is not None:
            return {
                "completed_instruments": len(frozen.instruments),
                "dataset_manifest_hash": frozen.manifest_hash,
                "failed": 0,
                "live_trading_locked": True,
                "processed": 0,
                "remaining_instruments": 0,
                "spec_hash": spec.spec_hash,
                "status": "completed",
                "total_instruments": len(daily.instruments),
            }
        existing = await fundamentals.completed_shards(
            instruments=daily.instruments,
            start_date=spec.start_date,
            end_date=spec.end_date,
        )
        completed_instruments = {value.instrument for value in existing}
        missing = tuple(value for value in daily.instruments if value not in completed_instruments)
        source = tushare_source(settings)
        clickhouse = await ClickHouseFundamentalRepository.connect(
            dsn=configured_dsn(
                settings.clickhouse_dsn,
                capability="ClickHouse",
            ),
            source="tushare",
        )
        service = FundamentalIngestionService(
            source=source,
            quality_gate=FundamentalQualityGate(),
            fundamental_repository=clickhouse,
            control_repository=control,
            now=lambda: datetime.now(UTC),
        )
        pressure = await clickhouse.merge_pressure()
        inactive_bytes = pressure.inactive_bytes
        inactive_parts = pressure.inactive_parts
        maintenance_wait = (
            inactive_bytes >= settings.research_data_max_inactive_bytes
            or inactive_parts >= settings.research_data_max_inactive_parts
        )
        for instrument in missing[:max_items]:
            if maintenance_wait:
                break
            processed += 1
            try:
                result = await service.run(
                    FundamentalIngestionRequest(
                        instruments=(instrument,),
                        start=spec.start_date,
                        end=spec.end_date,
                        as_of=None,
                        production_complete_requested=True,
                    )
                )
            except AutoQuantError:
                failed += 1
            else:
                if result.status == "completed" and result.manifest_hash is not None:
                    completed_now += 1
                else:
                    failed += 1
            if processed % 5 == 0:
                await clickhouse.purge_allocator()
                pressure = await clickhouse.merge_pressure()
                inactive_bytes = pressure.inactive_bytes
                inactive_parts = pressure.inactive_parts
                maintenance_wait = (
                    inactive_bytes >= settings.research_data_max_inactive_bytes
                    or inactive_parts >= settings.research_data_max_inactive_parts
                )
            if pause_seconds and processed < min(
                max_items,
                len(missing),
            ):
                await asyncio.sleep(float(pause_seconds))
        await clickhouse.purge_allocator()
        completed = await fundamentals.completed_shards(
            instruments=daily.instruments,
            start_date=spec.start_date,
            end_date=spec.end_date,
        )
        dataset = await fundamentals.finalize(
            spec=spec,
            instruments=daily.instruments,
            created_at=datetime.now(UTC),
        )
        payload: dict[str, object] = {
            "completed_instruments": len(completed),
            "completed_now": completed_now,
            "dataset_manifest_hash": (None if dataset is None else dataset.manifest_hash),
            "failed": failed,
            "inactive_bytes": inactive_bytes,
            "inactive_parts": inactive_parts,
            "live_trading_locked": True,
            "maintenance_wait": maintenance_wait,
            "processed": processed,
            "remaining_instruments": (len(daily.instruments) - len(completed)),
            "spec_hash": spec.spec_hash,
            "status": ("completed" if dataset is not None else "collecting"),
            "total_instruments": len(daily.instruments),
        }
        await control.append_audit_event(
            "research.fundamental.data.batch",
            datetime.now(UTC),
            {
                **payload,
                "requested_by": spec_record.requested_by,
            },
        )
        return payload
    finally:
        if clickhouse is not None:
            await clickhouse.client.close()
        if source is not None:
            await source.close()
        await control.close()
        await fundamentals.close()
        await daily_datasets.close()
        await specifications.close()


async def compile_fundamental_research_panel(
    settings: AppSettings,
    *,
    spec_hash: str,
    requested_by: str,
) -> dict[str, object]:
    """Compile and freeze the v3 point-in-time feature panel."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError("fundamental panel compilation requires live trading locked")
    if not requested_by.strip() or requested_by != requested_by.strip() or len(requested_by) > 128:
        raise ValueError("requested_by must contain 1-128 trimmed characters")
    dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    specifications = PostgresFundamentalResearchSpecRepository.connect(dsn=dsn)
    campaigns = PostgresResearchDataCampaignRepository.connect(dsn=dsn)
    universes = PostgresResearchUniverseRepository.connect(dsn=dsn)
    datasets = PostgresFundamentalDatasetRepository.connect(dsn=dsn)
    panels = PostgresFundamentalPanelRepository.connect(dsn=dsn)
    control = PostgresControlRepository.connect(dsn=dsn)
    daily_market: ClickHouseDailyRepository | None = None
    fundamental_reader: ClickHouseFundamentalRepository | None = None
    try:
        spec_record = await specifications.read(spec_hash)
        spec = spec_record.spec
        plan = await _load_research_input_plan(
            campaigns=campaigns,
            universes=universes,
            manifest_hash=spec.daily_dataset_manifest_hash,
        )
        if (
            plan.plan_hash != spec.plan_hash
            or plan.policy_hash != spec.universe_policy_hash
            or plan.start_date != spec.start_date
            or plan.end_date != spec.end_date
        ):
            raise ValueError("fundamental spec research plan binding differs")
        dataset = await datasets.read_for_spec(spec.spec_hash)
        if dataset is None:
            raise LookupError("fundamental dataset is not complete")
        daily_cutoffs: list[datetime] = []
        for shard in plan.shards:
            manifest = await control.read_manifest(shard.manifest_hash)
            if (
                manifest.source != "tushare"
                or not manifest.production_complete
                or manifest.instruments != (shard.instrument,)
                or to_shanghai(manifest.start_time).date() != plan.start_date
                or to_shanghai(manifest.end_time).date() != plan.end_date
            ):
                raise ValueError("daily shard does not match the research plan")
            daily_cutoffs.append(manifest.as_of)
        calendar_as_of = min(daily_cutoffs)
        daily_market = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(
                settings.clickhouse_dsn,
                capability="ClickHouse",
            ),
            source="tushare",
        )
        trading_sessions = await daily_market.query_sessions_as_of(
            plan.start_date,
            plan.end_date,
            calendar_as_of,
        )
        market_sessions: list[FundamentalMarketSessionBinding] = []
        for session in trading_sessions:
            if not session.is_open:
                continue
            universe = plan.universe_for(session.session_date)
            if universe is None:
                continue
            market_sessions.append(
                FundamentalMarketSessionBinding(
                    session_date=session.session_date,
                    snapshot_hash=universe.snapshot_hash,
                    active_members=universe.members,
                )
            )
        daily_binding = FundamentalMarketBinding(
            daily_dataset_manifest_hash=(spec.daily_dataset_manifest_hash),
            plan_hash=plan.plan_hash,
            spec_hash=spec.spec_hash,
            as_of=max(daily_cutoffs),
            calendar_as_of=calendar_as_of,
            instruments=plan.instruments,
            sessions=tuple(market_sessions),
        )
        fundamental_reader = await ClickHouseFundamentalRepository.connect(
            dsn=configured_dsn(
                settings.clickhouse_dsn,
                capability="ClickHouse",
            ),
            source="tushare",
        )
        fundamental_shards = ValidatedFundamentalDatasetReader(
            aggregate=dataset,
            manifest_reader=control,
            data_reader=fundamental_reader,
        )
        panel = await FundamentalPanelCompiler(
            shard_reader=fundamental_shards,
        ).compile(
            spec=spec,
            daily_binding=daily_binding,
            dataset=dataset,
        )
        created_at = datetime.now(UTC)
        record = await panels.freeze(
            panel,
            requested_by=requested_by,
            created_at=created_at,
        )
        counts = [len(value.observations) for value in panel.sessions]
        eligible_session_count = sum(value >= spec.minimum_eligible_members for value in counts)
        payload: dict[str, object] = {
            "as_of": panel.as_of.isoformat(),
            "daily_panel_hash": panel.daily_panel_hash,
            "eligible_session_count": eligible_session_count,
            "first_execution_date": (panel.sessions[0].execution_date.isoformat()),
            "fundamental_dataset_manifest_hash": (panel.fundamental_dataset_manifest_hash),
            "insufficient_session_count": (len(panel.sessions) - eligible_session_count),
            "last_execution_date": (panel.sessions[-1].execution_date.isoformat()),
            "live_trading_locked": record.live_trading_locked,
            "maximum_eligible_members": max(counts),
            "minimum_eligible_members": min(counts),
            "minimum_required_members": (panel.minimum_required_members),
            "observation_count": sum(counts),
            "panel_hash": record.panel_hash,
            "requested_by": record.requested_by,
            "session_count": len(panel.sessions),
            "spec_hash": panel.spec_hash,
            "status": "frozen",
            "version": panel.version,
        }
        await control.append_audit_event(
            "research.fundamental.panel.frozen",
            created_at,
            payload,
        )
        return payload
    finally:
        if fundamental_reader is not None:
            await fundamental_reader.client.close()
        if daily_market is not None:
            await daily_market.client.close()
        await control.close()
        await panels.close()
        await datasets.close()
        await universes.close()
        await campaigns.close()
        await specifications.close()


async def run_fundamental_validation(
    settings: AppSettings,
    *,
    spec_hash: str,
    requested_by: str,
) -> dict[str, object]:
    """Run the pre-registered v3 validation with live trading locked."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError("fundamental validation requires live trading locked")
    if not requested_by.strip() or requested_by != requested_by.strip() or len(requested_by) > 128:
        raise ValueError("requested_by must contain 1-128 trimmed characters")
    dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    specifications = PostgresFundamentalResearchSpecRepository.connect(dsn=dsn)
    campaigns = PostgresResearchDataCampaignRepository.connect(dsn=dsn)
    universes = PostgresResearchUniverseRepository.connect(dsn=dsn)
    datasets = PostgresFundamentalDatasetRepository.connect(dsn=dsn)
    panels = PostgresFundamentalPanelRepository.connect(dsn=dsn)
    validations = PostgresFundamentalValidationRepository.connect(dsn=dsn)
    control = PostgresControlRepository.connect(dsn=dsn)
    daily_market: ClickHouseDailyRepository | None = None
    fundamental_reader: ClickHouseFundamentalRepository | None = None
    try:
        spec_record = await specifications.read(spec_hash)
        spec = spec_record.spec
        existing = await validations.read_for_spec(spec.spec_hash)
        if existing is not None:
            return _fundamental_validation_payload(
                existing,
                status="stored",
            )
        plan = await _load_research_input_plan(
            campaigns=campaigns,
            universes=universes,
            manifest_hash=spec.daily_dataset_manifest_hash,
        )
        if (
            plan.plan_hash != spec.plan_hash
            or plan.policy_hash != spec.universe_policy_hash
            or plan.start_date != spec.start_date
            or plan.end_date != spec.end_date
        ):
            raise ValueError("fundamental spec research plan binding differs")
        dataset = await datasets.read_for_spec(spec.spec_hash)
        if dataset is None:
            raise LookupError("fundamental dataset is not complete")
        frozen_feature = await panels.read_for_spec(spec.spec_hash)
        if frozen_feature is None:
            raise LookupError("fundamental feature panel is not frozen")
        clickhouse_dsn = configured_dsn(
            settings.clickhouse_dsn,
            capability="ClickHouse",
        )
        daily_market = await ClickHouseDailyRepository.connect(
            dsn=clickhouse_dsn,
            source="tushare",
        )
        fundamental_reader = await ClickHouseFundamentalRepository.connect(
            dsn=clickhouse_dsn,
            source="tushare",
        )
        feature_panel = await _rebuild_fundamental_feature_panel(
            spec=spec,
            plan=plan,
            dataset=dataset,
            control=control,
            daily_market=daily_market,
            fundamental_reader=fundamental_reader,
        )
        if feature_panel.panel_hash != frozen_feature.panel_hash:
            raise ValueError("rebuilt fundamental panel differs from frozen evidence")
        await fundamental_reader.client.close()
        fundamental_reader = None
        await daily_market.purge_allocator(strict=True)
        market_panel = await DynamicMarketPanelCompiler(
            shard_reader=ExactManifestResearchDatasetReader(
                plan=plan,
                control_reader=control,
                record_reader=daily_market,
                batch_size=4,
            )
        ).compile_bound(
            plan=plan,
            dataset_manifest_hash=(spec.daily_dataset_manifest_hash),
            policy_hash=spec.universe_policy_hash,
            start_date=spec.start_date,
            end_date=spec.end_date,
            spec_hash=spec.spec_hash,
        )
        executable = compile_fundamental_executable_panel(
            spec=spec,
            features=feature_panel,
            markets=market_panel,
        )
        result = FundamentalWalkForwardValidator().run(
            panel=executable,
            spec=spec,
        )
        evidence = assess_fundamental_validation(
            result,
            spec=spec,
        )
        completed_at = datetime.now(UTC)
        record = await validations.save(
            result,
            evidence,
            requested_by=requested_by,
            completed_at=completed_at,
        )
        payload = _fundamental_validation_payload(
            record,
            status="completed",
        )
        await control.append_audit_event(
            "research.fundamental.validation.completed",
            completed_at,
            payload,
        )
        return payload
    finally:
        if fundamental_reader is not None:
            await fundamental_reader.client.close()
        if daily_market is not None:
            await daily_market.client.close()
        await control.close()
        await validations.close()
        await panels.close()
        await datasets.close()
        await universes.close()
        await campaigns.close()
        await specifications.close()


async def run_low_volatility_validation(
    settings: AppSettings,
    *,
    spec_hash: str,
    requested_by: str,
) -> dict[str, object]:
    """Run the pre-registered v4 validation with live trading locked."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError("low-volatility validation requires live trading locked")
    if not requested_by.strip() or requested_by != requested_by.strip() or len(requested_by) > 128:
        raise ValueError("requested_by must contain 1-128 trimmed characters")
    dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    specifications = PostgresLowVolatilityResearchSpecRepository.connect(dsn=dsn)
    campaigns = PostgresResearchDataCampaignRepository.connect(dsn=dsn)
    universes = PostgresResearchUniverseRepository.connect(dsn=dsn)
    validations = PostgresLowVolatilityValidationRepository.connect(dsn=dsn)
    control = PostgresControlRepository.connect(dsn=dsn)
    daily_market: ClickHouseDailyRepository | None = None
    try:
        spec_record = await specifications.read(spec_hash)
        spec = spec_record.spec
        existing = await validations.read_for_spec(spec.spec_hash)
        if existing is not None:
            return _low_volatility_validation_payload(
                existing,
                status="stored",
            )
        plan = await _load_research_input_plan(
            campaigns=campaigns,
            universes=universes,
            manifest_hash=spec.dataset_manifest_hash,
        )
        if (
            plan.plan_hash != spec.plan_hash
            or plan.policy_hash != spec.policy_hash
            or plan.start_date != spec.start_date
            or plan.end_date != spec.end_date
        ):
            raise ValueError("low-volatility spec research plan binding differs")
        daily_market = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(
                settings.clickhouse_dsn,
                capability="ClickHouse",
            ),
            source="tushare",
        )
        market_panel = await DynamicMarketPanelCompiler(
            shard_reader=ExactManifestResearchDatasetReader(
                plan=plan,
                control_reader=control,
                record_reader=daily_market,
                batch_size=4,
            )
        ).compile_bound(
            plan=plan,
            dataset_manifest_hash=spec.dataset_manifest_hash,
            policy_hash=spec.policy_hash,
            start_date=spec.start_date,
            end_date=spec.end_date,
            spec_hash=spec.spec_hash,
        )
        executable = compile_low_volatility_executable_panel(
            spec=spec,
            markets=market_panel,
        )
        result = LowVolatilityWalkForwardValidator().run(
            panel=executable,
            spec=spec,
        )
        evidence = assess_low_volatility_validation(
            result,
            spec=spec,
        )
        completed_at = datetime.now(UTC)
        record = await validations.save(
            result,
            evidence,
            requested_by=requested_by,
            completed_at=completed_at,
        )
        payload = _low_volatility_validation_payload(
            record,
            status="completed",
        )
        await control.append_audit_event(
            "research.low_volatility.validation.completed",
            completed_at,
            payload,
        )
        return payload
    finally:
        if daily_market is not None:
            await daily_market.client.close()
        await control.close()
        await validations.close()
        await universes.close()
        await campaigns.close()
        await specifications.close()


async def _rebuild_fundamental_feature_panel(
    *,
    spec: FundamentalPortfolioResearchSpec,
    plan: ResearchInputPlan,
    dataset: FundamentalResearchDatasetManifest,
    control: PostgresControlRepository,
    daily_market: ClickHouseDailyRepository,
    fundamental_reader: ClickHouseFundamentalRepository,
) -> FundamentalResearchPanel:
    daily_cutoffs: list[datetime] = []
    for shard in plan.shards:
        manifest = await control.read_manifest(shard.manifest_hash)
        if (
            manifest.source != "tushare"
            or not manifest.production_complete
            or manifest.instruments != (shard.instrument,)
            or to_shanghai(manifest.start_time).date() != plan.start_date
            or to_shanghai(manifest.end_time).date() != plan.end_date
        ):
            raise ValueError("daily shard does not match the research plan")
        daily_cutoffs.append(manifest.as_of)
    calendar_as_of = min(daily_cutoffs)
    trading_sessions = await daily_market.query_sessions_as_of(
        plan.start_date,
        plan.end_date,
        calendar_as_of,
    )
    bindings: list[FundamentalMarketSessionBinding] = []
    for session in trading_sessions:
        if not session.is_open:
            continue
        universe = plan.universe_for(session.session_date)
        if universe is None:
            continue
        bindings.append(
            FundamentalMarketSessionBinding(
                session_date=session.session_date,
                snapshot_hash=universe.snapshot_hash,
                active_members=universe.members,
            )
        )
    daily_binding = FundamentalMarketBinding(
        daily_dataset_manifest_hash=(spec.daily_dataset_manifest_hash),
        plan_hash=plan.plan_hash,
        spec_hash=spec.spec_hash,
        as_of=max(daily_cutoffs),
        calendar_as_of=calendar_as_of,
        instruments=plan.instruments,
        sessions=tuple(bindings),
    )
    return await FundamentalPanelCompiler(
        shard_reader=ValidatedFundamentalDatasetReader(
            aggregate=dataset,
            manifest_reader=control,
            data_reader=fundamental_reader,
        ),
    ).compile(
        spec=spec,
        daily_binding=daily_binding,
        dataset=dataset,
    )


def _fundamental_validation_payload(
    record: FundamentalValidationRecord,
    *,
    status: str,
) -> dict[str, object]:
    result = record.result
    evidence = record.evidence
    return {
        "assessment_hash": evidence.assessment_hash,
        "benchmark_compounded_oos_return": str(result.benchmark_compounded_oos_return),
        "compounded_oos_return": str(result.compounded_oos_return),
        "evidence_status": evidence.evidence_status,
        "excess_oos_return": str(result.excess_oos_return),
        "feature_panel_hash": result.feature_panel_hash,
        "fold_count": evidence.fold_count,
        "gate_failures": list(evidence.gate_failures),
        "live_trading_locked": True,
        "market_panel_hash": result.market_panel_hash,
        "oos_sessions": evidence.oos_sessions,
        "panel_hash": result.panel_hash,
        "profitable_fold_rate": str(result.profitable_fold_rate),
        "rejected_order_count": (result.rejected_order_count),
        "requested_by": record.requested_by,
        "result_hash": result.result_hash,
        "spec_hash": result.spec_hash,
        "status": status,
        "train_test_gap": str(result.train_test_gap),
        "unresolved_position_count": (result.unresolved_position_count),
        "version": result.version,
        "worst_oos_drawdown": str(result.worst_oos_drawdown),
    }


def _low_volatility_validation_payload(
    record: LowVolatilityValidationRecord,
    *,
    status: str,
) -> dict[str, object]:
    result = record.result
    evidence = record.evidence
    return {
        "assessment_hash": evidence.assessment_hash,
        "benchmark_compounded_oos_return": str(result.benchmark_compounded_oos_return),
        "benchmark_rejected_order_count": (result.benchmark_rejected_order_count),
        "benchmark_unresolved_position_count": (result.benchmark_unresolved_position_count),
        "compounded_oos_return": str(result.compounded_oos_return),
        "evidence_status": evidence.evidence_status,
        "excess_oos_return": str(result.excess_oos_return),
        "fold_count": evidence.fold_count,
        "gate_failures": list(evidence.gate_failures),
        "live_trading_locked": True,
        "market_panel_hash": result.market_panel_hash,
        "oos_sessions": evidence.oos_sessions,
        "panel_hash": result.panel_hash,
        "profitable_fold_rate": str(result.profitable_fold_rate),
        "requested_by": record.requested_by,
        "result_hash": result.result_hash,
        "spec_hash": result.spec_hash,
        "status": status,
        "strategy_rejected_order_count": (result.strategy_rejected_order_count),
        "strategy_unresolved_position_count": (result.strategy_unresolved_position_count),
        "train_test_gap": str(result.train_test_gap),
        "version": result.version,
        "worst_oos_drawdown": str(result.worst_oos_drawdown),
    }


async def inspect_research_input_shard(
    settings: AppSettings,
    *,
    manifest_hash: str,
    instrument: str,
    requested_by: str,
) -> dict[str, object]:
    """Verify one aggregate shard through quality and record-hash checks."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError("research shard verification requires live trading locked")
    if not requested_by.strip() or requested_by != requested_by.strip() or len(requested_by) > 128:
        raise ValueError("requested_by must contain 1-128 trimmed characters")
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    campaigns = PostgresResearchDataCampaignRepository.connect(dsn=postgres_dsn)
    universes = PostgresResearchUniverseRepository.connect(dsn=postgres_dsn)
    control = PostgresControlRepository.connect(dsn=postgres_dsn)
    market: ClickHouseDailyRepository | None = None
    try:
        plan = await _load_research_input_plan(
            campaigns=campaigns,
            universes=universes,
            manifest_hash=manifest_hash,
        )
        market = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(
                settings.clickhouse_dsn,
                capability="ClickHouse",
            ),
            source="tushare",
        )
        reader = ValidatedResearchDatasetReader(
            plan=plan,
            manifest_reader=control,
            dataset_reader=ValidatedDailyDatasetReader(
                control_repository=control,
                market_repository=market,
            ),
        )
        shard = await reader.query_instrument(instrument)
        payload: dict[str, object] = {
            "as_of": shard.manifest.as_of.isoformat(),
            "bar_count": len(shard.dataset.bars),
            "factor_count": len(shard.dataset.factors),
            "instrument": shard.instrument,
            "lifecycle_count": len(shard.dataset.coverage.lifecycles),
            "live_trading_locked": True,
            "manifest_hash": plan.dataset_manifest_hash,
            "plan_hash": plan.plan_hash,
            "price_limit_count": len(shard.dataset.coverage.price_limits),
            "session_count": len(shard.dataset.coverage.sessions),
            "shard_manifest_hash": shard.manifest.manifest_hash,
            "status": "verified",
            "suspension_count": len(shard.dataset.coverage.suspensions),
        }
        await control.append_audit_event(
            "research.input.shard.verified",
            datetime.now(UTC),
            {
                **payload,
                "requested_by": requested_by,
            },
        )
        return payload
    finally:
        if market is not None:
            await market.client.close()
        await control.close()
        await universes.close()
        await campaigns.close()


async def _load_research_input_plan(
    *,
    campaigns: PostgresResearchDataCampaignRepository,
    universes: PostgresResearchUniverseRepository,
    manifest_hash: str,
) -> ResearchInputPlan:
    manifest = await campaigns.read_manifest(manifest_hash)
    details = tuple(
        [await universes.detail(snapshot_hash) for snapshot_hash in manifest.snapshot_hashes]
    )
    bindings = tuple(
        ResearchUniverseBinding(
            sequence=sequence,
            snapshot_hash=detail.snapshot.snapshot_hash,
            policy_hash=detail.snapshot.policy_hash,
            reference_date=detail.snapshot.reference_date,
            knowledge_as_of=detail.snapshot.knowledge_as_of,
            members=tuple(sorted(member.instrument for member in detail.members)),
        )
        for sequence, detail in enumerate(details, start=1)
    )
    return compile_research_input_plan(
        manifest=manifest,
        universes=bindings,
    )


async def run_research_data_campaign(
    settings: AppSettings,
    *,
    campaign_hash: str,
    max_items: int,
    pause_seconds: Decimal,
) -> dict[str, object]:
    """Run a bounded number of persistent shards and finalize when complete."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError(
            "research data campaign execution requires live trading locked"
        )
    if max_items < 1 or max_items > 25:
        raise ValueError("max_items must be between 1 and 25")
    if pause_seconds < 0 or pause_seconds > Decimal("60"):
        raise ValueError("pause_seconds must be between 0 and 60")
    dsn = configured_dsn(settings.postgres_dsn, capability="PostgreSQL")
    repository = PostgresResearchDataCampaignRepository.connect(dsn=dsn)
    control = PostgresControlRepository.connect(dsn=dsn)
    source: TushareDailySource | None = None
    market: ClickHouseDailyRepository | None = None
    processed = 0
    completed = 0
    requeued = 0
    failed = 0
    recovered = 0
    maintenance_wait = False
    last_inactive_bytes = 0
    last_inactive_parts = 0
    try:
        status = await repository.status(campaign_hash=campaign_hash)
        recovered = await repository.recover_running(campaign_hash=status.spec.campaign_hash)
        source = tushare_source(settings)
        market = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(
                settings.clickhouse_dsn,
                capability="ClickHouse",
            ),
            source="tushare",
        )
        service = DailyIngestionService(
            source=source,
            quality_gate=DailyQualityGate(),
            market_repository=market,
            control_repository=control,
            now=lambda: datetime.now(UTC),
        )
        for _ in range(max_items):
            pressure = await market.merge_pressure()
            last_inactive_bytes = pressure.inactive_bytes
            last_inactive_parts = pressure.inactive_parts
            if (
                pressure.inactive_bytes >= settings.research_data_max_inactive_bytes
                or pressure.inactive_parts >= settings.research_data_max_inactive_parts
            ):
                maintenance_wait = True
                break
            item = await repository.claim_next(
                campaign_hash=status.spec.campaign_hash,
                now=datetime.now(UTC),
            )
            if item is None:
                break
            processed += 1
            try:
                result = await service.run(
                    DailyIngestionRequest(
                        instruments=(item.instrument,),
                        start=status.spec.start_date,
                        end=status.spec.end_date,
                        as_of=None,
                        production_complete_requested=True,
                    )
                )
                if result.status == "completed" and result.manifest_hash is not None:
                    await repository.complete_item(
                        campaign_hash=status.spec.campaign_hash,
                        sequence=item.sequence,
                        manifest_hash=result.manifest_hash,
                        now=datetime.now(UTC),
                    )
                    completed += 1
                else:
                    outcome = await repository.fail_item(
                        campaign_hash=status.spec.campaign_hash,
                        sequence=item.sequence,
                        error_code=f"daily_{result.status}"[:80],
                        retryable=result.status == "persistence_failed",
                        now=datetime.now(UTC),
                    )
                    if outcome.state == "queued":
                        requeued += 1
                    else:
                        failed += 1
            except AutoQuantError as error:
                outcome = await repository.fail_item(
                    campaign_hash=status.spec.campaign_hash,
                    sequence=item.sequence,
                    error_code=_research_data_error_code(error),
                    retryable=isinstance(
                        error,
                        (VendorRateLimitError, PersistenceUnavailableError),
                    ),
                    now=datetime.now(UTC),
                )
                if outcome.state == "queued":
                    requeued += 1
                else:
                    failed += 1
            if pause_seconds and processed < max_items:
                await asyncio.sleep(float(pause_seconds))
        final_status = await repository.status(campaign_hash=status.spec.campaign_hash)
        manifest = await repository.finalize(
            campaign_hash=status.spec.campaign_hash,
            created_at=datetime.now(UTC),
        )
        final_status = await repository.status(campaign_hash=status.spec.campaign_hash)
        await control.append_audit_event(
            "research.data.campaign.batch.completed",
            datetime.now(UTC),
            {
                "campaign_hash": status.spec.campaign_hash,
                "completed_count": completed,
                "failed_count": failed,
                "manifest_hash": (None if manifest is None else manifest.manifest_hash),
                "maintenance_wait": maintenance_wait,
                "processed_count": processed,
                "recovered_count": recovered,
                "requeued_count": requeued,
                "inactive_bytes": last_inactive_bytes,
                "inactive_parts": last_inactive_parts,
            },
        )
        payload = _research_data_campaign_payload(final_status)
        payload["batch"] = {
            "completed_count": completed,
            "failed_count": failed,
            "inactive_bytes": last_inactive_bytes,
            "inactive_parts": last_inactive_parts,
            "maintenance_wait": maintenance_wait,
            "processed_count": processed,
            "recovered_count": recovered,
            "requeued_count": requeued,
        }
        return payload
    finally:
        if market is not None:
            await market.client.close()
        if source is not None:
            await source.close()
        await control.close()
        await repository.close()


async def retry_research_data_campaign_item(
    settings: AppSettings,
    *,
    campaign_hash: str,
    sequence: int,
    authorized_by: str,
) -> dict[str, object]:
    """Explicitly requeue one terminal data shard while live stays locked."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError("research data retry requires live trading locked")
    if (
        not authorized_by.strip()
        or authorized_by != authorized_by.strip()
        or len(authorized_by) > 128
    ):
        raise ValueError("authorized_by must contain 1-128 trimmed characters")
    dsn = configured_dsn(settings.postgres_dsn, capability="PostgreSQL")
    repository = PostgresResearchDataCampaignRepository.connect(dsn=dsn)
    control = PostgresControlRepository.connect(dsn=dsn)
    try:
        item = await repository.retry_failed_item(
            campaign_hash=campaign_hash,
            sequence=sequence,
        )
        await control.append_audit_event(
            "research.data.campaign.item.retry_authorized",
            datetime.now(UTC),
            {
                "authorized_by": authorized_by,
                "campaign_hash": campaign_hash,
                "instrument": item.instrument,
                "sequence": sequence,
            },
        )
        status = await repository.status(campaign_hash=campaign_hash)
        return _research_data_campaign_payload(status)
    finally:
        await control.close()
        await repository.close()


def _research_data_error_code(error: AutoQuantError) -> str:
    if isinstance(error, VendorRateLimitError):
        return "vendor_rate_limited"
    if isinstance(error, VendorAuthenticationError):
        return "vendor_authentication_failed"
    if isinstance(error, VendorPermissionError):
        return "vendor_permission_denied"
    if isinstance(error, VendorResponseError):
        return "vendor_response_invalid"
    if isinstance(error, PersistenceUnavailableError):
        return "persistence_unavailable"
    return "autoquant_error"


def _require_monthly_snapshot_coverage(
    *,
    views: tuple[ResearchUniverseSnapshotView, ...],
    start_date: date,
    end_date: date,
) -> None:
    expected_values: list[tuple[int, int]] = []
    current = start_date.replace(day=1)
    final = end_date.replace(day=1)
    while current <= final:
        expected_values.append((current.year, current.month))
        current = (
            date(current.year + 1, 1, 1)
            if current.month == 12
            else date(current.year, current.month + 1, 1)
        )
    expected = tuple(expected_values)
    actual = tuple((value.reference_date.year, value.reference_date.month) for value in views)
    if actual != expected or len(set(actual)) != len(actual):
        raise ValueError("research data campaign requires one snapshot for every month")


def _research_data_campaign_payload(
    status: ResearchDataCampaignStatus,
) -> dict[str, object]:
    counts = {
        state: sum(value.state == state for value in status.items)
        for state in ("queued", "running", "completed", "failed")
    }
    return {
        "campaign_hash": status.spec.campaign_hash,
        "campaign_key": status.spec.campaign_key,
        "created_at": status.created_at.isoformat(),
        "end_date": status.spec.end_date.isoformat(),
        "instrument_count": len(status.spec.instruments),
        "item_counts": counts,
        "live_trading_locked": True,
        "manifest_hash": (None if status.manifest is None else status.manifest.manifest_hash),
        "policy_hash": status.spec.policy_hash,
        "snapshot_count": len(status.spec.snapshot_hashes),
        "start_date": status.spec.start_date.isoformat(),
        "status": status.status,
    }


def _next_low_volatility_forward_session(
    *,
    open_dates: tuple[date, ...],
    bound_dates: tuple[date, ...],
    minimum_sessions: int,
) -> date | None:
    if (
        minimum_sessions < 1
        or open_dates != tuple(sorted(open_dates))
        or len(set(open_dates)) != len(open_dates)
        or bound_dates != tuple(sorted(bound_dates))
        or len(set(bound_dates)) != len(bound_dates)
    ):
        raise ValueError("forward cycle session inputs are invalid")
    bound = set(bound_dates)
    return next(
        (value for value in open_dates[:minimum_sessions] if value not in bound),
        None,
    )


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
        raise MissingCapabilityError("validation campaigns require live trading to remain locked")
    normalized = tuple(sorted(instruments))
    policy = default_paper_policy(normalized)
    if (
        len(normalized) < 3
        or len(normalized) > 20
        or len(set(normalized)) != len(normalized)
        or allocation > policy.max_position_weight
        or allocation * len(normalized) > policy.max_gross_exposure
    ):
        raise ValueError("campaign universe or allocation exceeds paper risk controls")
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
        campaigns = PostgresValidationCampaignRepository.connect(dsn=postgres_dsn)
        manifest = await control.read_manifest(manifest_hash)
        if not manifest.production_complete or tuple(sorted(manifest.instruments)) != normalized:
            raise ValueError("campaign manifest must exactly cover the requested universe")
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
            minimum_sessions=(train_sessions + embargo_sessions + 6 * test_sessions),
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
        return _validation_campaign_payload(await repository.status(campaign_hash=campaign_hash))
    finally:
        await repository.close()


async def create_portfolio_validation(
    settings: AppSettings,
    *,
    request: PortfolioWalkForwardJobRequest,
    requested_by: str,
) -> dict[str, object]:
    """Queue one live-locked, immutable portfolio validation."""

    if settings.live_trading_enabled:
        raise MissingCapabilityError("portfolio validation requires live trading to remain locked")
    dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    control = PostgresControlRepository.connect(dsn=dsn)
    repository = PostgresPortfolioValidationRepository.connect(dsn=dsn)
    try:
        manifest = await control.read_manifest(request.manifest_hash)
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
            now=datetime.now(UTC),
        )
        try:
            await control.append_audit_event(
                "operator.portfolio_validation.requested",
                datetime.now(UTC),
                {
                    "job_id": str(experiment.experiment_id),
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
                now=datetime.now(UTC),
                queued=True,
            )
            raise PersistenceUnavailableError("portfolio validation audit is unavailable") from None
        return _portfolio_validation_payload(
            experiment,
            fold_count=0,
        )
    finally:
        await repository.close()
        await control.close()


async def inspect_portfolio_validation(
    settings: AppSettings,
    *,
    experiment_id: UUID,
) -> dict[str, object]:
    """Verify and return one redacted portfolio experiment."""

    repository = PostgresPortfolioValidationRepository.connect(
        dsn=configured_dsn(
            settings.postgres_dsn,
            capability="PostgreSQL",
        )
    )
    try:
        detail = await repository.detail(experiment_id)
        return _portfolio_validation_payload(
            detail.experiment,
            fold_count=len(detail.folds),
            diagnostics=(
                None if detail.diagnostics is None else detail.diagnostics.model_dump(mode="json")
            ),
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
    bar_keys = tuple((value.instrument, value.session_date) for value in dataset.bars)
    factor_keys = tuple((value.instrument, value.session_date) for value in dataset.factors)
    common_markets = compile_common_calendar_markets(
        instruments=instruments,
        dataset=dataset,
        compiler=market_compiler,
    )
    common_dates = tuple(value.bar.session_date for value in common_markets[instruments[0]])
    allocated_cash = initial_cash * allocation
    slippage_multiplier = Decimal("1") + slippage_bps / Decimal("10000")
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
        raise ValueError("campaign data lacks aligned, adjusted, affordable minimum OOS history")


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


def _portfolio_validation_payload(
    experiment: PortfolioValidationExperiment,
    *,
    fold_count: int,
    diagnostics: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "assessment": (
            None if experiment.summary is None else experiment.summary.model_dump(mode="json")
        ),
        "completed_at": (
            None if experiment.completed_at is None else experiment.completed_at.isoformat()
        ),
        "diagnostics": diagnostics,
        "error_code": experiment.error_code,
        "experiment_id": str(experiment.experiment_id),
        "fold_count": fold_count,
        "live_trading_locked": True,
        "manifest_hash": experiment.request.manifest_hash,
        "result_hash": experiment.result_hash,
        "state": experiment.state.value,
        "validator_id": experiment.validator_id,
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
        scheduler_events = PostgresPaperSchedulerRepository.connect(dsn=postgres_dsn)
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
            raise MissingCapabilityError("paper runtime requires an active approved strategy")
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
            "instrument": (report.instruments[0] if len(report.instruments) == 1 else None),
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
            policy_hash=policy.policy_hash,
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


async def create_compliance_approval(
    settings: AppSettings,
    *,
    external_artifact_hash: str,
    approval_reference: str,
    approved_by: str,
    valid_until: datetime,
) -> dict[str, object]:
    """Persist separately attested paper-promotion compliance scope."""

    if settings.environment is not RuntimeEnvironment.PAPER:
        raise MissingCapabilityError("paper environment is not configured")
    if settings.live_trading_enabled:
        raise MissingCapabilityError("compliance approval requires live trading locked")
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    registry = PostgresPaperDeploymentRegistry.connect(dsn=postgres_dsn)
    execution_controls = PostgresExecutionControlRepository.connect(dsn=postgres_dsn)
    approvals = PostgresComplianceApprovalRepository.connect(dsn=postgres_dsn)
    control = PostgresControlRepository.connect(dsn=postgres_dsn)
    try:
        registration = await registry.active(
            account_id=settings.paper_account_id,
            strategy_id=settings.paper_strategy_id,
        )
        if registration is None:
            raise ValueError("compliance approval requires an active paper registration")
        if approved_by.strip().casefold() == registration.approved_by.strip().casefold():
            raise ValueError("compliance actor must differ from strategy approver")
        execution_control = await execution_controls.replay(account_id=settings.paper_account_id)
        if not execution_control.active:
            raise MissingCapabilityError("compliance approval requires the kill switch active")
        policy = PaperPromotionPolicy()
        approval = ComplianceApproval(
            account_id=settings.paper_account_id,
            strategy_id=settings.paper_strategy_id,
            registration_hash=(registration.registration_hash),
            policy_hash=policy.policy_hash,
            external_artifact_hash=external_artifact_hash,
            approval_reference=approval_reference,
            approved_by=approved_by,
            approved_at=datetime.now(UTC),
            valid_until=valid_until,
        )
        stored = await approvals.approve(approval)
        payload: dict[str, object] = {
            "account_scope": "configured_paper_account",
            "approval_hash": stored.approval_hash,
            "approval_reference": (stored.approval_reference),
            "approved_at": stored.approved_at.isoformat(),
            "external_artifact_hash": (stored.external_artifact_hash),
            "live_trading_locked": True,
            "policy_hash": stored.policy_hash,
            "registration_hash": stored.registration_hash,
            "status": "approved_for_promotion_audit_only",
            "strategy_id": stored.strategy_id,
            "valid_until": stored.valid_until.isoformat(),
            "version": stored.version,
        }
        await control.append_audit_event(
            "compliance.paper_promotion.approved",
            stored.approved_at,
            {
                **payload,
                "approved_by": stored.approved_by,
            },
        )
        return payload
    finally:
        await control.close()
        await approvals.close()
        await execution_controls.close()
        await registry.close()


async def revoke_compliance_approval(
    settings: AppSettings,
    *,
    approval_hash: str,
    revoked_by: str,
    reason: ComplianceRevocationReason,
) -> dict[str, object]:
    """Append a safety revocation without changing any trading control."""

    if settings.environment is not RuntimeEnvironment.PAPER:
        raise MissingCapabilityError("paper environment is not configured")
    if settings.live_trading_enabled:
        raise MissingCapabilityError("compliance revocation requires live trading locked")
    postgres_dsn = configured_dsn(
        settings.postgres_dsn,
        capability="PostgreSQL",
    )
    approvals = PostgresComplianceApprovalRepository.connect(dsn=postgres_dsn)
    control = PostgresControlRepository.connect(dsn=postgres_dsn)
    try:
        approval = await approvals.read(approval_hash)
        if (
            approval.account_id != settings.paper_account_id
            or approval.strategy_id != settings.paper_strategy_id
        ):
            raise ValueError("compliance approval is outside configured scope")
        revocation = ComplianceRevocation(
            approval_hash=approval.approval_hash,
            revoked_by=revoked_by,
            revoked_at=datetime.now(UTC),
            reason=reason,
        )
        stored = await approvals.revoke(revocation)
        payload: dict[str, object] = {
            "approval_hash": stored.approval_hash,
            "live_trading_locked": True,
            "reason": stored.reason.value,
            "revocation_hash": stored.revocation_hash,
            "revoked_at": stored.revoked_at.isoformat(),
            "status": "revoked",
            "version": stored.version,
        }
        await control.append_audit_event(
            "compliance.paper_promotion.revoked",
            stored.revoked_at,
            {
                **payload,
                "revoked_by": stored.revoked_by,
            },
        )
        return payload
    finally:
        await control.close()
        await approvals.close()


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
            "baseline_qmt_evidence_hash": (event.baseline_qmt_evidence_hash),
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
            "failure_control_event_hash": (event.failure_control_event_hash),
            "kind": event.kind.value,
            "live_trading_locked": True,
            "recovery_qmt_evidence_hash": (event.recovery_qmt_evidence_hash),
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
        strategies = PostgresPaperDeploymentRegistry.connect(dsn=postgres_dsn)
        leases = PostgresPaperSchedulerLeaseRepository.connect(dsn=postgres_dsn)
        unlocks = PostgresPaperRuntimeUnlockRepository.connect(dsn=postgres_dsn)
        await unlocks.check_connection()
        registration = await strategies.active(
            account_id=settings.paper_account_id,
            strategy_id=settings.paper_strategy_id,
        )
        if registration is None:
            raise MissingCapabilityError("paper unlock requires an active approved strategy")
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
    lease_guard: QmtSessionLeaseGuard | None = None
    completed = False
    lease_guard_failed = False
    try:
        controls = PostgresExecutionControlRepository.connect(dsn=postgres_dsn)
        leases = PostgresQmtSessionLeaseRepository.connect(dsn=postgres_dsn)
        acceptances = PostgresQmtReadOnlyAcceptanceRepository.connect(dsn=postgres_dsn)
        await acceptances.check_connection()
        control = await controls.replay(account_id=settings.paper_account_id)
        active_session_ids = await leases.active_session_ids(now=datetime.now(UTC))
        readiness = inspect_qmt_readiness(
            settings,
            kill_switch_active=control.active,
            active_session_ids=active_session_ids,
        )
        if not readiness.read_only_ready:
            blockers = ",".join(check.code.value for check in readiness.checks if not check.passed)
            raise MissingCapabilityError(f"QMT read-only preflight is blocked: {blockers}")
        bindings = await asyncio.to_thread(QmtVendorBindings.load)
        lease_guard = QmtSessionLeaseGuard(
            repository=leases,
            session_id=session_id,
            holder_id=credentials.holder_id,
            token=credentials.lease_token,
            ttl=timedelta(seconds=settings.qmt_lease_ttl_seconds),
            now=lambda: datetime.now(UTC),
        )
        await lease_guard.start()

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

        acceptance = await run_fenced_blocking(query_once)
        lease = await lease_guard.verify()
        latest_control = await controls.replay(account_id=settings.paper_account_id)
        if not latest_control.active:
            raise MissingCapabilityError("QMT acceptance requires the kill switch to remain active")
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
        if lease_guard is not None:
            try:
                await lease_guard.close()
            except Exception:
                completed = False
                lease_guard_failed = True
        if not completed and controls is not None:
            try:
                state = await controls.replay(account_id=settings.paper_account_id)
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
        if lease_guard_failed:
            raise PersistenceUnavailableError("QMT session lease guard failed")


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
        raise ValueError("paper approval requires a current or recent exact session reference")
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
        execution_controls = PostgresExecutionControlRepository.connect(dsn=postgres_dsn)
        validations = PostgresValidationRepository.connect(dsn=postgres_dsn)
        registry = PostgresPaperStrategyRegistry.connect(dsn=postgres_dsn)
        fence = await execution_controls.replay(account_id=settings.paper_account_id)
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
        raise ValueError("paper portfolio requires 3-20 matched experiments and manifests")
    now = datetime.now(UTC)
    lag_days = (to_shanghai(now).date() - reference_session_date).days
    if lag_days < 0 or lag_days > 4:
        raise ValueError("paper approval requires a current or recent exact session reference")
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
        execution_controls = PostgresExecutionControlRepository.connect(dsn=postgres_dsn)
        validations = PostgresValidationRepository.connect(dsn=postgres_dsn)
        registry = PostgresPaperPortfolioRegistry.connect(dsn=postgres_dsn)
        fence = await execution_controls.replay(account_id=settings.paper_account_id)
        if not fence.active:
            raise MissingCapabilityError(
                "portfolio approval requires the kill switch to remain active"
            )
        details = tuple(
            [await validations.detail(experiment_id) for experiment_id in experiment_ids]
        )
        instruments = tuple(sorted(value.experiment.request.instrument for value in details))
        if len(set(instruments)) != len(instruments):
            raise ValueError("paper portfolio experiments must use unique instruments")
        rule_set = await ExactSessionRuleReader(
            market_repository=clickhouse,
            control_repository=control,
        ).read(
            instruments=instruments,
            session_date=reference_session_date,
            as_of=now,
        )
        if rule_set.suspended_instruments:
            raise ValueError("paper portfolio cannot be approved while a component is suspended")
        rules_by_instrument = {value.instrument: value for value in rule_set.rules}
        if set(rules_by_instrument) != set(instruments):
            raise ValueError("paper portfolio session rules are incomplete")
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
                    rules=rules_by_instrument[detail.experiment.request.instrument],
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
                "assessment_hash": (registration.oos_assessment.assessment_hash),
                "compounded_return": str(registration.oos_assessment.compounded_return),
                "fold_count": (registration.oos_assessment.fold_count),
                "maximum_component_contribution": str(
                    registration.oos_assessment.maximum_component_contribution
                ),
                "maximum_drawdown": str(registration.oos_assessment.maximum_drawdown),
                "maximum_pairwise_correlation": (
                    None
                    if registration.oos_assessment.maximum_pairwise_correlation is None
                    else str(registration.oos_assessment.maximum_pairwise_correlation)
                ),
                "policy_hash": (registration.oos_assessment.policy_hash),
                "profitable_fold_rate": str(registration.oos_assessment.profitable_fold_rate),
            },
            "registration_hash": registration.registration_hash,
            "status": "approved",
            "strategy_id": registration.strategy_id,
            "strategy_version": registration.strategy_version,
            "valuation_manifest_hash": (registration.valuation_manifest_hash),
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
