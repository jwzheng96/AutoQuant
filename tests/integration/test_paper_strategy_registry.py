from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.data.models import DatasetManifest
from autoquant.data.quality import QualityReport
from autoquant.execution.strategy_registry_store import (
    PostgresPaperStrategyRegistry,
)
from autoquant.execution.validated_sma import ValidatedSmaRegistration
from autoquant.risk.models import RiskPolicy
from autoquant.web.models import WalkForwardJobRequest
from autoquant.web.validation_store import PostgresValidationRepository

POSTGRES_DSN = os.environ.get("AQ_POSTGRES_DSN", "").strip()
AS_OF = datetime(2026, 7, 22, 8, tzinfo=UTC)
APPROVED_AT = datetime(2026, 7, 22, 9, tzinfo=UTC)
INSTRUMENT = "600000.XSHG"
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not POSTGRES_DSN,
        reason="AQ_POSTGRES_DSN is not configured; PostgreSQL infrastructure unavailable",
    ),
]


@pytest_asyncio.fixture
async def registry_fixture() -> AsyncIterator[
    tuple[PostgresPaperStrategyRegistry, ValidatedSmaRegistration, AsyncEngine, str]
]:
    schema = f"autoquant_test_{uuid4().hex}"
    control = PostgresControlRepository.connect(dsn=POSTGRES_DSN, schema=schema)
    validations = PostgresValidationRepository.connect(
        dsn=POSTGRES_DSN,
        schema=schema,
    )
    registry = PostgresPaperStrategyRegistry.connect(
        dsn=POSTGRES_DSN,
        schema=schema,
    )
    engine = create_async_engine(POSTGRES_DSN, pool_pre_ping=True)
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/005_walk_forward_validation.sql",
            "migrations/postgres/006_validation_benchmark.sql",
            "migrations/postgres/015_paper_strategy_registry.sql",
        )
    )
    report = QualityReport(
        requested_instruments=(INSTRUMENT,),
        start=datetime(2025, 1, 1, tzinfo=UTC),
        end=datetime(2026, 7, 22, 7, tzinfo=UTC),
        as_of=AS_OF,
        issues=(),
        production_complete=True,
    )
    manifest = DatasetManifest(
        source="tushare",
        instruments=(INSTRUMENT,),
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2026, 7, 22, 7, tzinfo=UTC),
        as_of=AS_OF,
        record_hashes=("a" * 64,),
        quality_report_hash=report.report_hash,
        production_complete=True,
        row_count=1,
    )
    try:
        await control.initialize(migration)
        await control.save_quality_report(report)
        await control.save_manifest(manifest)
        experiment = await validations.create_experiment(
            WalkForwardJobRequest(
                manifest_hash=manifest.manifest_hash,
                instrument=INSTRUMENT,
                allocation=Decimal("0.20"),
                slippage_bps=Decimal("5"),
                train_sessions=60,
                test_sessions=20,
                candidates=({"fast_sessions": 5, "slow_sessions": 20},),
                idempotency_key="paper-registry-integration-0001",
            ),
            requested_by="researcher",
            now=AS_OF,
        )
        async with engine.begin() as connection:
            await connection.exec_driver_sql(
                f'SET LOCAL search_path TO "{schema}"'
            )
            await connection.execute(
                text(
                    """
                    UPDATE validation_experiments
                    SET state = 'completed', started_at = :as_of,
                        completed_at = :as_of, as_of = :as_of,
                        result_hash = :result_hash,
                        summary_payload = CAST(:summary AS jsonb)
                    WHERE experiment_id = :experiment_id
                    """
                ),
                {
                    "as_of": AS_OF,
                    "experiment_id": experiment.experiment_id,
                    "result_hash": "b" * 64,
                    "summary": json.dumps(
                        {
                            "evidence_status": "research_candidate",
                            "gate_failures": [],
                        }
                    ),
                },
            )
            for sequence in range(1, 7):
                await connection.execute(
                    text(
                        """
                        INSERT INTO validation_folds
                            (experiment_id, sequence, train_start, train_end,
                             test_start, test_end, selected_fast, selected_slow,
                             selection_score, fold_hash, training_payload,
                             test_payload, benchmark_payload)
                        VALUES
                            (:experiment_id, :sequence, '2025-01-01',
                             '2025-03-01', '2025-03-03', '2025-03-22',
                             5, 20, 1, :fold_hash, '{}'::jsonb, '{}'::jsonb,
                             '{}'::jsonb)
                        """
                    ),
                    {
                        "experiment_id": experiment.experiment_id,
                        "sequence": sequence,
                        "fold_hash": f"{sequence:064x}",
                    },
                )
        rules = AshareRuleBook().resolve(
            INSTRUMENT,
            date(2026, 7, 23),
            SecurityStatus(risk_warning=False, listing_session_number=1000),
        )
        policy = RiskPolicy(
            allowed_instruments=(INSTRUMENT,),
            max_position_weight=Decimal("0.20"),
            max_gross_exposure=Decimal("0.20"),
        )
        registration = ValidatedSmaRegistration(
            account_id="paper-main",
            strategy_id="validated-sma-paper",
            strategy_version="sma-paper-v1:integration:5-20",
            experiment_id=experiment.experiment_id,
            validation_result_hash="b" * 64,
            validation_manifest_hash=manifest.manifest_hash,
            signal_manifest_hash=manifest.manifest_hash,
            signal_manifest_as_of=manifest.as_of,
            instrument=INSTRUMENT,
            fast_sessions=5,
            slow_sessions=20,
            allocation=Decimal("0.20"),
            slippage_bps=Decimal("5"),
            risk_policy_hash=policy.policy_hash,
            rule_version=rules.rule_version,
            approved_by="operator",
            approved_at=APPROVED_AT,
        )
        yield registry, registration, engine, schema
    finally:
        try:
            await registry.close()
        finally:
            try:
                await validations.close()
            finally:
                try:
                    await engine.dispose()
                finally:
                    try:
                        await control.drop_test_schema()
                    finally:
                        await control.close()


@pytest.mark.asyncio
async def test_registry_replays_immutable_approval_and_revocation_chain(
    registry_fixture: tuple[
        PostgresPaperStrategyRegistry,
        ValidatedSmaRegistration,
        AsyncEngine,
        str,
    ],
) -> None:
    registry, registration, _, _ = registry_fixture

    first = await registry.approve(registration)
    repeated = await registry.approve(registration)
    active = await registry.active(
        account_id=registration.account_id,
        strategy_id=registration.strategy_id,
    )

    assert first == repeated == active

    with pytest.raises(ValueError, match="revoked before replacement"):
        await registry.approve(
            replace(registration, strategy_version="replacement-v2")
        )

    await registry.revoke(
        account_id=registration.account_id,
        strategy_id=registration.strategy_id,
        revoked_by="operator",
        reason="scheduled_research_refresh",
        revoked_at=APPROVED_AT,
    )

    assert (
        await registry.active(
            account_id=registration.account_id,
            strategy_id=registration.strategy_id,
        )
        is None
    )


@pytest.mark.asyncio
async def test_registry_tables_reject_mutation(
    registry_fixture: tuple[
        PostgresPaperStrategyRegistry,
        ValidatedSmaRegistration,
        AsyncEngine,
        str,
    ],
) -> None:
    registry, registration, engine, schema = registry_fixture
    await registry.approve(registration)

    with pytest.raises(SQLAlchemyError):
        async with engine.begin() as connection:
            await connection.exec_driver_sql(
                f'SET LOCAL search_path TO "{schema}"'
            )
            await connection.execute(
                text(
                    """
                    UPDATE paper_strategy_registrations
                    SET approved_by = 'tampered'
                    WHERE registration_hash = :registration_hash
                    """
                ),
                {"registration_hash": registration.registration_hash},
            )
