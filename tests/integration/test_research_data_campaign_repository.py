from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio

from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.backtest.dynamic_portfolio import (
    DYNAMIC_REGIME_PORTFOLIO_SPEC_VERSION,
    DYNAMIC_REGIME_PORTFOLIO_STRATEGY_ID,
    DynamicPortfolioResearchSpec,
    DynamicRegimeFilter,
)
from autoquant.backtest.dynamic_validation import (
    DynamicCandidateEvaluation,
    DynamicValidationFold,
    DynamicValidationResult,
    _selection_score,
    assess_dynamic_validation,
)
from autoquant.backtest.fundamental_portfolio import (
    FundamentalPortfolioResearchSpec,
)
from autoquant.backtest.models import AccountSnapshot, BacktestResult
from autoquant.data.models import DatasetManifest
from autoquant.data.quality import QualityReport
from autoquant.data.research_data_campaign import ResearchDataCampaignSpec
from autoquant.web.dynamic_research_store import (
    PostgresDynamicResearchSpecRepository,
)
from autoquant.web.dynamic_validation_store import (
    PostgresDynamicValidationRepository,
)
from autoquant.web.fundamental_data_store import (
    PostgresFundamentalDatasetRepository,
)
from autoquant.web.fundamental_research_store import (
    PostgresFundamentalResearchSpecRepository,
)
from autoquant.web.research_data_store import (
    PostgresResearchDataCampaignRepository,
)

POSTGRES_DSN = os.environ.get("AQ_POSTGRES_DSN", "").strip()
NOW = datetime(2026, 7, 23, 8, tzinfo=UTC)
START_TIME = datetime(2019, 12, 31, 16, tzinfo=UTC)
END_TIME = datetime(2026, 7, 22, 7, tzinfo=UTC)
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not POSTGRES_DSN,
        reason="AQ_POSTGRES_DSN is not configured; PostgreSQL infrastructure unavailable",
    ),
]


@pytest_asyncio.fixture
async def repositories() -> AsyncIterator[
    tuple[
        PostgresControlRepository,
        PostgresResearchDataCampaignRepository,
        PostgresDynamicResearchSpecRepository,
        PostgresDynamicValidationRepository,
        PostgresFundamentalResearchSpecRepository,
        PostgresFundamentalDatasetRepository,
        str,
    ]
]:
    schema = f"autoquant_test_{uuid4().hex}"
    control = PostgresControlRepository.connect(dsn=POSTGRES_DSN, schema=schema)
    campaigns = PostgresResearchDataCampaignRepository.connect(
        dsn=POSTGRES_DSN,
        schema=schema,
    )
    specs = PostgresDynamicResearchSpecRepository.connect(
        dsn=POSTGRES_DSN,
        schema=schema,
    )
    validations = PostgresDynamicValidationRepository.connect(
        dsn=POSTGRES_DSN,
        schema=schema,
    )
    fundamental_specs = (
        PostgresFundamentalResearchSpecRepository.connect(
            dsn=POSTGRES_DSN,
            schema=schema,
        )
    )
    fundamental_data = PostgresFundamentalDatasetRepository.connect(
        dsn=POSTGRES_DSN,
        schema=schema,
    )
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/023_research_universes.sql",
            "migrations/postgres/024_research_data_campaigns.sql",
            "migrations/postgres/025_dynamic_research_specs.sql",
            "migrations/postgres/026_dynamic_validation_evidence.sql",
            "migrations/postgres/027_dynamic_regime_research.sql",
            "migrations/postgres/028_fundamental_research.sql",
            "migrations/postgres/029_fundamental_dataset.sql",
        )
    )
    try:
        await control.initialize(migration)
        yield (
            control,
            campaigns,
            specs,
            validations,
            fundamental_specs,
            fundamental_data,
            schema,
        )
    finally:
        await fundamental_data.close()
        await fundamental_specs.close()
        await validations.close()
        await specs.close()
        await campaigns.close()
        try:
            await control.drop_test_schema()
        finally:
            await control.close()


async def save_shard_manifest(
    control: PostgresControlRepository,
    *,
    instrument: str,
    record_hash: str,
    source: str = "tushare",
) -> DatasetManifest:
    report = QualityReport(
        requested_instruments=(instrument,),
        start=START_TIME,
        end=END_TIME,
        as_of=NOW,
        issues=(),
        production_complete=True,
    )
    manifest = DatasetManifest(
        source=source,
        instruments=(instrument,),
        start_time=START_TIME,
        end_time=END_TIME,
        as_of=NOW,
        record_hashes=(record_hash,),
        quality_report_hash=report.report_hash,
        production_complete=True,
        row_count=1,
    )
    await control.save_quality_report(report)
    await control.save_manifest(manifest)
    return manifest


@pytest.mark.asyncio
async def test_campaign_recovers_retries_and_finalizes_verified_shards(
    repositories: tuple[
        PostgresControlRepository,
        PostgresResearchDataCampaignRepository,
        PostgresDynamicResearchSpecRepository,
        PostgresDynamicValidationRepository,
        PostgresFundamentalResearchSpecRepository,
        PostgresFundamentalDatasetRepository,
        str,
    ],
) -> None:
    (
        control,
        campaigns,
        specs,
        validations,
        fundamental_specs,
        fundamental_data,
        schema,
    ) = repositories
    spec = ResearchDataCampaignSpec(
        campaign_key="integration-csi300-history-v1",
        policy_hash="a" * 64,
        snapshot_hashes=("b" * 64, "c" * 64),
        instruments=("000001.XSHE", "600000.XSHG"),
        start_date=date(2020, 1, 1),
        end_date=date(2026, 7, 22),
        requested_by="test",
    )
    first_manifest = await save_shard_manifest(
        control,
        instrument="000001.XSHE",
        record_hash="d" * 64,
    )
    second_manifest = await save_shard_manifest(
        control,
        instrument="600000.XSHG",
        record_hash="e" * 64,
    )

    created = await campaigns.create(spec, created_at=NOW)
    repeated = await campaigns.create(spec, created_at=NOW)
    contender = PostgresResearchDataCampaignRepository.connect(
        dsn=POSTGRES_DSN,
        schema=schema,
    )
    try:
        assert await campaigns.try_acquire_worker_lock(campaign_hash=spec.campaign_hash)
        assert not await contender.try_acquire_worker_lock(campaign_hash=spec.campaign_hash)
        await campaigns.release_worker_lock()
        assert await contender.try_acquire_worker_lock(campaign_hash=spec.campaign_hash)
    finally:
        await contender.close()
    assert await campaigns.try_acquire_worker_lock(
        campaign_hash=spec.campaign_hash
    )
    await campaigns.release_worker_lock()
    first = await campaigns.claim_next(campaign_hash=spec.campaign_hash, now=NOW)
    assert first is not None
    assert first.instrument == "000001.XSHE"
    assert await campaigns.recover_running(campaign_hash=spec.campaign_hash) == 1
    first = await campaigns.claim_next(campaign_hash=spec.campaign_hash, now=NOW)
    assert first is not None
    assert first.attempts == 2
    await campaigns.complete_item(
        campaign_hash=spec.campaign_hash,
        sequence=first.sequence,
        manifest_hash=first_manifest.manifest_hash,
        now=NOW,
    )
    second = await campaigns.claim_next(campaign_hash=spec.campaign_hash, now=NOW)
    assert second is not None
    retried = await campaigns.fail_item(
        campaign_hash=spec.campaign_hash,
        sequence=second.sequence,
        error_code="temporary_vendor_error",
        retryable=True,
        now=NOW,
    )
    assert retried.state == "queued"
    second = await campaigns.claim_next(campaign_hash=spec.campaign_hash, now=NOW)
    assert second is not None
    terminal = await campaigns.fail_item(
        campaign_hash=spec.campaign_hash,
        sequence=second.sequence,
        error_code="deterministic_mapping_error",
        retryable=False,
        now=NOW,
    )
    assert terminal.state == "failed"
    authorized = await campaigns.retry_failed_item(
        campaign_hash=spec.campaign_hash,
        sequence=second.sequence,
    )
    assert authorized.state == "queued"
    assert authorized.max_attempts == 6
    second = await campaigns.claim_next(campaign_hash=spec.campaign_hash, now=NOW)
    assert second is not None
    await campaigns.complete_item(
        campaign_hash=spec.campaign_hash,
        sequence=second.sequence,
        manifest_hash=second_manifest.manifest_hash,
        now=NOW,
    )

    manifest = await campaigns.finalize(
        campaign_hash=spec.campaign_hash,
        created_at=NOW,
    )
    status = await campaigns.status(campaign_hash=spec.campaign_hash)

    assert created.spec == repeated.spec == spec
    assert manifest is not None
    assert status.status == "completed"
    assert status.manifest == manifest
    assert manifest.instruments == spec.instruments
    assert tuple(value.manifest_hash for value in manifest.shards) == (
        first_manifest.manifest_hash,
        second_manifest.manifest_hash,
    )
    assert await campaigns.read_manifest(manifest.manifest_hash) == manifest
    with pytest.raises(LookupError):
        await campaigns.read_manifest("0" * 64)

    frozen = DynamicPortfolioResearchSpec(
        dataset_manifest_hash=manifest.manifest_hash,
        plan_hash="f" * 64,
        policy_hash=manifest.policy_hash,
        start_date=manifest.start_date,
        end_date=manifest.end_date,
    )
    record = await specs.freeze(
        frozen,
        requested_by="test",
        created_at=NOW,
    )
    repeated_record = await specs.freeze(
        frozen,
        requested_by="another-operator",
        created_at=NOW,
    )

    assert record == repeated_record
    assert await specs.read(frozen.spec_hash) == record

    candidate_evaluations = tuple(
        DynamicCandidateEvaluation(
            parameters=parameters,
            score=Decimal("0"),
            result=_empty_backtest(
                strategy_id=parameters.strategy_id,
                manifest_hash=manifest.manifest_hash,
                start=date(2025, 1, 1),
                end=date(2025, 1, 2),
            ),
        )
        for parameters in frozen.candidates
    )
    winner = max(
        candidate_evaluations,
        key=lambda value: (
            value.score,
            -value.parameters.lookback_sessions,
            -value.parameters.rebalance_sessions,
            -value.parameters.selection_count,
        ),
    )
    fold = DynamicValidationFold(
        sequence=1,
        train_start=date(2025, 1, 1),
        train_end=date(2025, 1, 2),
        test_start=date(2025, 1, 4),
        test_end=date(2025, 1, 5),
        selected=winner.parameters,
        selection_score=winner.score,
        candidate_evaluations=candidate_evaluations,
        training_result=winner.result,
        test_result=_empty_backtest(
            strategy_id=winner.parameters.strategy_id,
            manifest_hash=manifest.manifest_hash,
            start=date(2025, 1, 4),
            end=date(2025, 1, 5),
        ),
        benchmark_result=_empty_backtest(
            strategy_id=frozen.benchmark_version,
            manifest_hash=manifest.manifest_hash,
            start=date(2025, 1, 4),
            end=date(2025, 1, 5),
        ),
    )
    validation_result = DynamicValidationResult(
        panel_hash="9" * 64,
        spec_hash=frozen.spec_hash,
        dataset_manifest_hash=manifest.manifest_hash,
        as_of=NOW,
        folds=(fold,),
        compounded_oos_return=Decimal("0"),
        benchmark_compounded_oos_return=Decimal("0"),
        excess_oos_return=Decimal("0"),
        profitable_fold_rate=Decimal("0"),
        worst_oos_drawdown=Decimal("0"),
        mean_training_return=Decimal("0"),
        selection_optimism=Decimal("0"),
        rejected_order_count=0,
    )
    evidence = assess_dynamic_validation(
        validation_result,
        policy=frozen.evidence_policy,
    )
    validation_record = await validations.save(
        validation_result,
        evidence,
        requested_by="test",
        completed_at=NOW,
    )
    repeated_validation = await validations.save(
        validation_result,
        evidence,
        requested_by="another-operator",
        completed_at=NOW + timedelta(seconds=1),
    )

    assert repeated_validation == validation_record
    assert (
        await validations.read(validation_result.result_hash)
        == validation_record
    )
    regime_spec = DynamicPortfolioResearchSpec(
        dataset_manifest_hash=frozen.dataset_manifest_hash,
        plan_hash=frozen.plan_hash,
        policy_hash=frozen.policy_hash,
        start_date=frozen.start_date,
        end_date=frozen.end_date,
        regime_filter=DynamicRegimeFilter(
            predecessor_result_hash=(
                validation_result.result_hash
            )
        ),
        strategy_id=DYNAMIC_REGIME_PORTFOLIO_STRATEGY_ID,
        version=DYNAMIC_REGIME_PORTFOLIO_SPEC_VERSION,
    )
    regime_record = await specs.freeze(
        regime_spec,
        requested_by="test",
        created_at=NOW,
    )

    assert await specs.read(regime_spec.spec_hash) == regime_record
    fundamental_spec = FundamentalPortfolioResearchSpec(
        predecessor_result_hash=validation_result.result_hash,
        daily_dataset_manifest_hash=manifest.manifest_hash,
        plan_hash=frozen.plan_hash,
        universe_policy_hash=frozen.policy_hash,
        start_date=frozen.start_date,
        end_date=frozen.end_date,
    )
    fundamental_record = await fundamental_specs.freeze(
        fundamental_spec,
        requested_by="test",
        created_at=NOW,
    )

    assert (
        await fundamental_specs.read(fundamental_spec.spec_hash)
        == fundamental_record
    )
    first_fundamental = await save_shard_manifest(
        control,
        instrument="000001.XSHE",
        record_hash="1" * 64,
        source="tushare-fundamental",
    )
    second_fundamental = await save_shard_manifest(
        control,
        instrument="600000.XSHG",
        record_hash="2" * 64,
        source="tushare-fundamental",
    )
    completed_fundamental = await fundamental_data.completed_shards(
        instruments=spec.instruments,
        start_date=spec.start_date,
        end_date=spec.end_date,
    )
    aggregate_fundamental = await fundamental_data.finalize(
        spec=fundamental_spec,
        instruments=spec.instruments,
        created_at=NOW,
    )

    assert tuple(
        value.manifest_hash for value in completed_fundamental
    ) == (
        first_fundamental.manifest_hash,
        second_fundamental.manifest_hash,
    )
    assert aggregate_fundamental is not None
    assert (
        await fundamental_data.read_for_spec(
            fundamental_spec.spec_hash
        )
        == aggregate_fundamental
    )
    with pytest.raises(ValueError, match="already frozen"):
        await specs.freeze(
            DynamicPortfolioResearchSpec(
                dataset_manifest_hash=manifest.manifest_hash,
                plan_hash="f" * 64,
                policy_hash=manifest.policy_hash,
                start_date=manifest.start_date,
                end_date=manifest.end_date,
                slippage_bps=Decimal("11"),
            ),
            requested_by="test",
            created_at=NOW,
        )


def _empty_backtest(
    *,
    strategy_id: str,
    manifest_hash: str,
    start: date,
    end: date,
) -> BacktestResult:
    snapshots = tuple(
        AccountSnapshot(
            session_date=session_date,
            cash=Decimal("1000000"),
            market_value=Decimal("0"),
            equity=Decimal("1000000"),
            positions=(),
            ledger_hash="0" * 64,
        )
        for session_date in (start, end)
    )
    result = BacktestResult(
        strategy_id=strategy_id,
        manifest_hash=manifest_hash,
        as_of=NOW,
        initial_cash=Decimal("1000000"),
        ending_equity=Decimal("1000000"),
        total_return=Decimal("0"),
        max_drawdown=Decimal("0"),
        turnover=Decimal("0"),
        total_fees=Decimal("0"),
        reports=(),
        snapshots=snapshots,
        events=(),
        rule_versions=(),
        fee_version="integration-fees-v1",
        execution_version="integration-execution-v1",
        ledger_hash="0" * 64,
    )
    assert _selection_score(result) == 0
    return result
