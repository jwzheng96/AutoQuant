from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from autoquant.backtest.dynamic_panel import DynamicMarketPanelCompiler
from autoquant.backtest.dynamic_portfolio import (
    DynamicPortfolioResearchSpec,
)
from autoquant.backtest.models import MarketState
from autoquant.backtest.rules import AshareRuleBook
from autoquant.data.daily_ingestion import ValidatedDailyDataset
from autoquant.data.daily_models import (
    DailyBarRevision,
    DailyCoverageEvidence,
    DailyPriceLimit,
    InstrumentLifecycle,
    TradingSession,
)
from autoquant.data.models import DatasetManifest
from autoquant.data.research_data_campaign import (
    ResearchDatasetManifest,
    ResearchDatasetShard,
)
from autoquant.data.research_input import (
    ResearchUniverseBinding,
    ValidatedResearchShard,
    compile_research_input_plan,
)

AS_OF = datetime(2026, 7, 23, tzinfo=UTC)
FIRST = "000001.XSHE"
SECOND = "600000.XSHG"
OPEN_DATES = (
    date(2020, 1, 31),
    date(2020, 2, 3),
    date(2020, 3, 2),
)


def _daily_manifest(instrument: str) -> DatasetManifest:
    return DatasetManifest(
        source="tushare",
        instruments=(instrument,),
        start_time=datetime(2019, 12, 31, 16, tzinfo=UTC),
        end_time=datetime(2020, 3, 31, 7, tzinfo=UTC),
        as_of=AS_OF,
        record_hashes=(),
        quality_report_hash="quality-report",
        production_complete=True,
        row_count=0,
    )


def _plan(
    first: DatasetManifest,
    second: DatasetManifest,
):
    manifest = ResearchDatasetManifest(
        campaign_hash="a" * 64,
        policy_hash="b" * 64,
        snapshot_hashes=("c" * 64, "d" * 64, "e" * 64),
        start_date=date(2020, 1, 1),
        end_date=date(2020, 3, 31),
        shards=(
            ResearchDatasetShard(
                1,
                FIRST,
                first.manifest_hash,
            ),
            ResearchDatasetShard(
                2,
                SECOND,
                second.manifest_hash,
            ),
        ),
    )
    universes = (
        ResearchUniverseBinding(
            sequence=1,
            snapshot_hash="c" * 64,
            policy_hash="b" * 64,
            reference_date=date(2020, 1, 31),
            knowledge_as_of=AS_OF,
            members=(FIRST,),
        ),
        ResearchUniverseBinding(
            sequence=2,
            snapshot_hash="d" * 64,
            policy_hash="b" * 64,
            reference_date=date(2020, 2, 29),
            knowledge_as_of=AS_OF,
            members=(FIRST, SECOND),
        ),
        ResearchUniverseBinding(
            sequence=3,
            snapshot_hash="e" * 64,
            policy_hash="b" * 64,
            reference_date=date(2020, 3, 31),
            knowledge_as_of=AS_OF,
            members=(SECOND,),
        ),
    )
    return compile_research_input_plan(
        manifest=manifest,
        universes=universes,
    )


def _coverage(
    instrument: str,
    *,
    dates: tuple[date, ...] = OPEN_DATES,
) -> DailyCoverageEvidence:
    return DailyCoverageEvidence(
        sessions=tuple(
            TradingSession(
                source="tushare",
                session_date=value,
                is_open=True,
                available_at=AS_OF,
                response_hash="f" * 64,
            )
            for value in dates
        ),
        lifecycles=(
            InstrumentLifecycle(
                source="tushare",
                instrument=instrument,
                list_date=(
                    date(2020, 1, 1)
                    if instrument == FIRST
                    else date(2020, 3, 1)
                ),
                delist_date=None,
                available_at=AS_OF,
                response_hash="1" * 64,
            ),
        ),
        suspensions=(),
        price_limits=(),
    )


def _market(
    instrument: str,
    session_date: date,
    close: str,
) -> MarketState:
    price = Decimal(close)
    bar = DailyBarRevision.from_values(
        source="tushare",
        instrument=instrument,
        session_date=session_date,
        event_time=datetime.combine(
            session_date,
            datetime.min.time(),
            tzinfo=UTC,
        ),
        available_at=AS_OF,
        ingested_at=AS_OF,
        source_revision="dynamic-panel-test",
        availability_policy="test-v1",
        evidence_hash="2" * 64,
        open_price=close,
        high_price=str(price + Decimal("1")),
        low_price=str(price - Decimal("1")),
        close_price=close,
        pre_close=close,
        volume=1_000_000,
        turnover="10000000",
    )
    limit = DailyPriceLimit(
        source="tushare",
        instrument=instrument,
        session_date=session_date,
        pre_close=price,
        up_limit=price * Decimal("1.1"),
        down_limit=price * Decimal("0.9"),
        available_at=AS_OF,
        response_hash="3" * 64,
    )
    return MarketState(
        bar=bar,
        rules=AshareRuleBook().resolve_with_price_limit(
            instrument,
            session_date,
            limit,
        ),
        suspended=False,
        daily_price_limit=limit,
    )


class _ShardReader:
    def __init__(
        self,
        shards: tuple[ValidatedResearchShard, ...],
    ) -> None:
        self.shards = shards

    async def iter_all(self):
        for shard in self.shards:
            yield shard


class _MarketCompiler:
    def __init__(
        self,
        values: dict[str, tuple[MarketState, ...]],
    ) -> None:
        self.values = values

    def compile(
        self,
        instrument: str,
        _dataset: ValidatedDailyDataset,
    ) -> tuple[MarketState, ...]:
        return self.values[instrument]


def _shard(
    manifest: DatasetManifest,
    coverage: DailyCoverageEvidence,
) -> ValidatedResearchShard:
    return ValidatedResearchShard(
        instrument=manifest.instruments[0],
        manifest=manifest,
        dataset=ValidatedDailyDataset(
            bars=(),
            factors=(),
            coverage=coverage,
        ),
    )


@pytest.mark.asyncio
async def test_panel_uses_strict_historical_membership_and_real_markets() -> None:
    first = _daily_manifest(FIRST)
    second = _daily_manifest(SECOND)
    plan = _plan(first, second)
    spec = DynamicPortfolioResearchSpec(
        dataset_manifest_hash=plan.dataset_manifest_hash,
        plan_hash=plan.plan_hash,
        policy_hash=plan.policy_hash,
        start_date=plan.start_date,
        end_date=plan.end_date,
    )
    compiler = DynamicMarketPanelCompiler(
        shard_reader=_ShardReader(
            (
                _shard(first, _coverage(FIRST)),
                _shard(second, _coverage(SECOND)),
            )
        ),
        market_compiler=_MarketCompiler(
            {
                FIRST: (
                    _market(FIRST, date(2020, 2, 3), "10"),
                    _market(FIRST, date(2020, 3, 2), "11"),
                ),
                SECOND: (
                    _market(SECOND, date(2020, 3, 2), "20"),
                ),
            }
        ),
    )

    panel = await compiler.compile(plan=plan, spec=spec)

    assert tuple(value.session_date for value in panel.sessions) == (
        date(2020, 2, 3),
        date(2020, 3, 2),
    )
    assert panel.sessions[0].snapshot_hash == "c" * 64
    assert panel.sessions[0].active_members == (FIRST,)
    assert panel.sessions[1].snapshot_hash == "d" * 64
    assert panel.sessions[1].active_members == (FIRST, SECOND)
    assert tuple(
        value.bar.instrument
        for value in panel.sessions[1].markets
    ) == (FIRST, SECOND)
    assert len(panel.panel_hash) == 64


@pytest.mark.asyncio
async def test_panel_rejects_calendar_drift_between_shards() -> None:
    first = _daily_manifest(FIRST)
    second = _daily_manifest(SECOND)
    plan = _plan(first, second)
    spec = DynamicPortfolioResearchSpec(
        dataset_manifest_hash=plan.dataset_manifest_hash,
        plan_hash=plan.plan_hash,
        policy_hash=plan.policy_hash,
        start_date=plan.start_date,
        end_date=plan.end_date,
    )
    compiler = DynamicMarketPanelCompiler(
        shard_reader=_ShardReader(
            (
                _shard(first, _coverage(FIRST)),
                _shard(
                    second,
                    _coverage(SECOND, dates=OPEN_DATES[:-1]),
                ),
            )
        ),
        market_compiler=_MarketCompiler(
            {
                FIRST: (
                    _market(FIRST, date(2020, 2, 3), "10"),
                ),
                SECOND: (
                    _market(SECOND, date(2020, 2, 3), "20"),
                ),
            }
        ),
    )

    with pytest.raises(ValueError, match="trading calendar"):
        await compiler.compile(plan=plan, spec=spec)
