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
from autoquant.backtest.models import (
    BacktestResult,
    BacktestSession,
    MarketState,
    OrderIntent,
    OrderSide,
)
from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.data.daily_models import DailyBarRevision
from autoquant.data.models import DatasetManifest
from autoquant.data.quality import QualityReport
from autoquant.web.backtest_store import PostgresBacktestRepository
from autoquant.web.models import BacktestRunRequest, OperatorJobState

POSTGRES_DSN = os.environ.get("AQ_POSTGRES_DSN", "").strip()
DAY = date(2026, 7, 20)
AS_OF = datetime(2026, 7, 21, 8, tzinfo=UTC)
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
    tuple[PostgresControlRepository, PostgresBacktestRepository, DatasetManifest]
]:
    schema = f"autoquant_test_{uuid4().hex}"
    control = PostgresControlRepository.connect(dsn=POSTGRES_DSN, schema=schema)
    backtests = PostgresBacktestRepository.connect(dsn=POSTGRES_DSN, schema=schema)
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/004_backtest_runs.sql",
        )
    )
    report = QualityReport(
        requested_instruments=(INSTRUMENT,),
        start=datetime(2026, 7, 20, 7, tzinfo=UTC),
        end=datetime(2026, 7, 20, 7, tzinfo=UTC),
        as_of=AS_OF,
        issues=(),
        production_complete=True,
    )
    manifest = DatasetManifest(
        source="tushare",
        instruments=(INSTRUMENT,),
        start_time=datetime(2026, 7, 20, 7, tzinfo=UTC),
        end_time=datetime(2026, 7, 20, 7, tzinfo=UTC),
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
        yield control, backtests, manifest
    finally:
        try:
            await backtests.close()
        finally:
            try:
                await control.drop_test_schema()
            finally:
                await control.close()


def _request(manifest: DatasetManifest) -> BacktestRunRequest:
    return BacktestRunRequest(
        manifest_hash=manifest.manifest_hash,
        instrument=INSTRUMENT,
        initial_cash=Decimal("10000"),
        allocation=Decimal("0.5"),
        slippage_bps=Decimal("5"),
        idempotency_key="integration-backtest-run-0001",
    )


def _result(manifest: DatasetManifest) -> BacktestResult:
    event = datetime(2026, 7, 20, 7, tzinfo=UTC)
    bar = DailyBarRevision.from_values(
        source="tushare",
        instrument=INSTRUMENT,
        session_date=DAY,
        event_time=event,
        available_at=event + timedelta(hours=1),
        ingested_at=AS_OF,
        source_revision="integration",
        availability_policy="test-v1",
        evidence_hash="b" * 64,
        open_price="10",
        high_price="10.2",
        low_price="9.8",
        close_price="10.1",
        pre_close="9.9",
        volume=100_000,
        turnover="1000000",
    )
    market = MarketState(
        bar=bar,
        rules=AshareRuleBook().resolve(
            INSTRUMENT,
            DAY,
            SecurityStatus(risk_warning=False, listing_session_number=1000),
        ),
        suspended=False,
    )
    order = OrderIntent(
        client_order_id="integration-buy",
        instrument=INSTRUMENT,
        side=OrderSide.BUY,
        quantity=100,
        session_date=DAY,
        submitted_at=datetime(2026, 7, 19, tzinfo=UTC),
    )
    return BacktestEngine().run(
        strategy_id="manifest_buy_hold_v1",
        manifest_hash=manifest.manifest_hash,
        as_of=AS_OF,
        initial_cash=Decimal("10000"),
        sessions=(BacktestSession(DAY, (market,), (order,)),),
    )


@pytest.mark.asyncio
async def test_backtest_queue_and_result_commit_are_idempotent_and_queryable(
    repositories: tuple[
        PostgresControlRepository, PostgresBacktestRepository, DatasetManifest
    ],
) -> None:
    _, repository, manifest = repositories
    request = _request(manifest)
    created = await repository.create_run(request, requested_by="operator", now=AS_OF)
    repeated = await repository.create_run(request, requested_by="operator", now=AS_OF)

    assert repeated.run_id == created.run_id
    claimed = await repository.claim_next_run(now=AS_OF)
    assert claimed is not None
    completed = await repository.complete_run(
        claimed.run_id, result=_result(manifest), now=AS_OF
    )
    detail = await repository.detail(completed.run_id)
    manifests = await repository.list_manifests()

    assert completed.state is OperatorJobState.COMPLETED
    assert len(detail.executions) == 1
    assert len(detail.snapshots) == 1
    assert len(detail.events) == 1
    assert detail.events[0].event_hash == completed.ledger_hash
    assert manifests[0].manifest_hash == manifest.manifest_hash
