from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio

from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.backtest.dynamic_portfolio import (
    DynamicPortfolioResearchSpec,
)
from autoquant.data.models import DatasetManifest
from autoquant.data.quality import QualityReport
from autoquant.data.research_data_campaign import ResearchDataCampaignSpec
from autoquant.web.dynamic_research_store import (
    PostgresDynamicResearchSpecRepository,
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
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/023_research_universes.sql",
            "migrations/postgres/024_research_data_campaigns.sql",
            "migrations/postgres/025_dynamic_research_specs.sql",
        )
    )
    try:
        await control.initialize(migration)
        yield control, campaigns, specs
    finally:
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
        source="tushare",
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
    ],
) -> None:
    control, campaigns, specs = repositories
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
