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
from autoquant.backtest.engine import BacktestEngine
from autoquant.backtest.models import BacktestResult, BacktestSession, MarketState
from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.backtest.validation import (
    SmaParameters,
    WalkForwardConfig,
    WalkForwardFold,
    WalkForwardResult,
)
from autoquant.data.daily_models import DailyBarRevision
from autoquant.data.models import DatasetManifest
from autoquant.data.quality import QualityReport
from autoquant.web.models import OperatorJobState, WalkForwardJobRequest
from autoquant.web.validation_store import PostgresValidationRepository

POSTGRES_DSN = os.environ.get("AQ_POSTGRES_DSN", "").strip()
AS_OF = datetime(2026, 12, 31, tzinfo=UTC)
INSTRUMENT = "000001.XSHE"
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not POSTGRES_DSN,
        reason="AQ_POSTGRES_DSN is not configured; PostgreSQL infrastructure unavailable",
    ),
]


@pytest_asyncio.fixture
async def repositories() -> AsyncIterator[
    tuple[PostgresControlRepository, PostgresValidationRepository, DatasetManifest]
]:
    schema = f"autoquant_test_{uuid4().hex}"
    control = PostgresControlRepository.connect(dsn=POSTGRES_DSN, schema=schema)
    validations = PostgresValidationRepository.connect(dsn=POSTGRES_DSN, schema=schema)
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/005_walk_forward_validation.sql",
            "migrations/postgres/006_validation_benchmark.sql",
        )
    )
    report = QualityReport(
        requested_instruments=(INSTRUMENT,),
        start=datetime(2026, 1, 1, 7, tzinfo=UTC),
        end=datetime(2026, 12, 1, 7, tzinfo=UTC),
        as_of=AS_OF,
        issues=(),
        production_complete=True,
    )
    manifest = DatasetManifest(
        source="tushare",
        instruments=(INSTRUMENT,),
        start_time=datetime(2026, 1, 1, 7, tzinfo=UTC),
        end_time=datetime(2026, 12, 1, 7, tzinfo=UTC),
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
        yield control, validations, manifest
    finally:
        try:
            await validations.close()
        finally:
            try:
                await control.drop_test_schema()
            finally:
                await control.close()


def _result(
    manifest: DatasetManifest, *, start: date, end: date
) -> BacktestResult:
    sessions: list[BacktestSession] = []
    current = start
    index = 0
    while current <= end:
        event = datetime.combine(current, datetime.min.time(), tzinfo=UTC) + timedelta(
            hours=7
        )
        price = Decimal("10") + Decimal(index) / Decimal("10")
        bar = DailyBarRevision.from_values(
            source="tushare",
            instrument=INSTRUMENT,
            session_date=current,
            event_time=event,
            available_at=event + timedelta(hours=1),
            ingested_at=AS_OF,
            source_revision="validation-integration",
            availability_policy="test-v1",
            evidence_hash="b" * 64,
            open_price=str(price),
            high_price=str(price + Decimal("0.2")),
            low_price=str(price - Decimal("0.2")),
            close_price=str(price + Decimal("0.1")),
            pre_close=str(price),
            volume=100_000,
            turnover="1000000",
        )
        market = MarketState(
            bar=bar,
            rules=AshareRuleBook().resolve(
                INSTRUMENT,
                current,
                SecurityStatus(risk_warning=False, listing_session_number=1000),
            ),
            suspended=False,
        )
        sessions.append(BacktestSession(current, (market,), ()))
        current += timedelta(days=1)
        index += 1
    return BacktestEngine().run(
        strategy_id="sma_cross_v1:fast=5:slow=20",
        manifest_hash=manifest.manifest_hash,
        as_of=AS_OF,
        initial_cash=Decimal("100000"),
        sessions=tuple(sessions),
    )


def _walk_forward_result(manifest: DatasetManifest) -> WalkForwardResult:
    training = _result(
        manifest, start=date(2026, 1, 1), end=date(2026, 3, 1)
    )
    test = _result(
        manifest, start=date(2026, 3, 3), end=date(2026, 3, 22)
    )
    fold = WalkForwardFold(
        sequence=1,
        train_start=date(2026, 1, 1),
        train_end=date(2026, 3, 1),
        test_start=date(2026, 3, 3),
        test_end=date(2026, 3, 22),
        selected=SmaParameters(5, 20),
        selection_score=Decimal("0"),
        training_result=training,
        test_result=test,
    )
    return WalkForwardResult(
        manifest_hash=manifest.manifest_hash,
        instrument=INSTRUMENT,
        as_of=AS_OF,
        config=WalkForwardConfig(
            initial_cash=Decimal("100000"),
            train_sessions=60,
            test_sessions=20,
            candidates=(SmaParameters(5, 20),),
        ),
        folds=(fold,),
        compounded_oos_return=test.total_return,
        mean_oos_return=test.total_return,
        worst_oos_drawdown=test.max_drawdown,
        profitable_fold_rate=Decimal(test.total_return > 0),
        mean_training_return=training.total_return,
        selection_optimism=training.total_return - test.total_return,
    )


def _request(manifest: DatasetManifest) -> WalkForwardJobRequest:
    return WalkForwardJobRequest(
        manifest_hash=manifest.manifest_hash,
        instrument=INSTRUMENT,
        initial_cash=Decimal("100000"),
        train_sessions=60,
        test_sessions=20,
        candidates=({"fast_sessions": 5, "slow_sessions": 20},),
        idempotency_key="integration-validation-0001",
    )


@pytest.mark.asyncio
async def test_validation_queue_atomically_persists_and_verifies_fold_artifacts(
    repositories: tuple[
        PostgresControlRepository, PostgresValidationRepository, DatasetManifest
    ],
) -> None:
    _, repository, manifest = repositories
    request = _request(manifest)
    created = await repository.create_experiment(
        request, requested_by="operator", now=AS_OF
    )
    repeated = await repository.create_experiment(
        request, requested_by="operator", now=AS_OF
    )
    claimed = await repository.claim_next_experiment(now=AS_OF)
    assert claimed is not None

    completed = await repository.complete_experiment(
        claimed.experiment_id,
        result=_walk_forward_result(manifest),
        now=AS_OF,
    )
    detail = await repository.detail(completed.experiment_id)

    assert repeated.experiment_id == created.experiment_id
    assert completed.state is OperatorJobState.COMPLETED
    assert detail.experiment.result_hash == completed.result_hash
    assert len(detail.folds) == 1
    assert detail.folds[0].training.artifact_hash is not None
    assert detail.folds[0].test.artifact_hash is not None
