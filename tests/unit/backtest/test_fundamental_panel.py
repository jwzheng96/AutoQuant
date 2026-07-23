from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from autoquant.backtest.fundamental_panel import (
    FundamentalMarketBinding,
    FundamentalMarketSessionBinding,
    FundamentalPanelCompiler,
)
from autoquant.backtest.fundamental_portfolio import (
    FundamentalPortfolioResearchSpec,
)
from autoquant.data.fundamental_dataset import (
    FundamentalDatasetShard,
    FundamentalResearchDatasetManifest,
    ValidatedFundamentalShard,
)
from autoquant.data.fundamental_models import (
    DailyValuationRevision,
    FinancialIndicatorRevision,
)
from autoquant.data.models import DatasetManifest

INSTRUMENT = "600519.XSHG"
SIGNAL_DATE = date(2026, 7, 20)
EXECUTION_DATE = date(2026, 7, 21)
AS_OF = datetime(2026, 7, 23, 8, tzinfo=UTC)


def _spec() -> FundamentalPortfolioResearchSpec:
    return FundamentalPortfolioResearchSpec(
        predecessor_result_hash="a" * 64,
        daily_dataset_manifest_hash="b" * 64,
        plan_hash="c" * 64,
        universe_policy_hash="d" * 64,
        start_date=date(2020, 1, 1),
        end_date=date(2026, 7, 22),
    )


def _valuation() -> DailyValuationRevision:
    return DailyValuationRevision.from_values(
        source="tushare",
        instrument=INSTRUMENT,
        session_date=SIGNAL_DATE,
        event_time=datetime(2026, 7, 20, 7, tzinfo=UTC),
        available_at=datetime(
            2026, 7, 21, 1, 30, tzinfo=UTC
        ),
        ingested_at=AS_OF,
        source_revision="tushare:daily_basic:test",
        availability_policy="next-open-v1",
        evidence_hash="1" * 64,
        close_price="1500",
        free_float_turnover_rate_percent="0.3",
        pe_ttm="20",
        pb="5",
        ps_ttm="10",
        dividend_yield_ttm_percent="2",
        total_market_value_cny="100000000",
        circulating_market_value_cny="90000000",
    )


def _indicator() -> FinancialIndicatorRevision:
    return FinancialIndicatorRevision.from_values(
        source="tushare",
        instrument=INSTRUMENT,
        report_period=date(2026, 3, 31),
        announced_date=date(2026, 4, 25),
        updated=False,
        event_time=datetime(2026, 4, 25, 7, tzinfo=UTC),
        available_at=datetime(
            2026, 4, 27, 1, 30, tzinfo=UTC
        ),
        ingested_at=AS_OF,
        source_revision="tushare:fina_indicator:test",
        availability_policy="next-open-v1",
        evidence_hash="2" * 64,
        roe_diluted_percent="12",
        roa_percent="8",
        gross_profit_margin_percent="90",
        debt_to_assets_percent="20",
        operating_cashflow_to_revenue_percent="40",
    )


class _ShardReader:
    def __init__(self, shard: ValidatedFundamentalShard) -> None:
        self._shard = shard

    async def iter_all(self):
        yield self._shard


@pytest.mark.asyncio
async def test_panel_uses_previous_session_and_visible_financials() -> None:
    spec = _spec()
    valuation = _valuation()
    indicator = _indicator()
    manifest = DatasetManifest(
        source="tushare-fundamental",
        instruments=(INSTRUMENT,),
        start_time=datetime(2019, 12, 31, 16, tzinfo=UTC),
        end_time=datetime(2026, 7, 22, 7, tzinfo=UTC),
        as_of=AS_OF,
        record_hashes=(
            valuation.content_hash,
            indicator.content_hash,
        ),
        quality_report_hash="quality",
        production_complete=True,
        row_count=2,
    )
    dataset = FundamentalResearchDatasetManifest(
        spec_hash=spec.spec_hash,
        start_date=spec.start_date,
        end_date=spec.end_date,
        shards=(
            FundamentalDatasetShard(
                sequence=1,
                instrument=INSTRUMENT,
                manifest_hash=manifest.manifest_hash,
            ),
        ),
    )
    daily_binding = FundamentalMarketBinding(
        daily_dataset_manifest_hash=(
            spec.daily_dataset_manifest_hash
        ),
        plan_hash=spec.plan_hash,
        spec_hash=spec.spec_hash,
        as_of=AS_OF,
        calendar_as_of=AS_OF,
        instruments=(INSTRUMENT,),
        sessions=(
            FundamentalMarketSessionBinding(
                session_date=SIGNAL_DATE,
                snapshot_hash="3" * 64,
                active_members=(INSTRUMENT,),
            ),
            FundamentalMarketSessionBinding(
                session_date=EXECUTION_DATE,
                snapshot_hash="3" * 64,
                active_members=(INSTRUMENT,),
            ),
        ),
    )
    panel = await FundamentalPanelCompiler(
        shard_reader=_ShardReader(
            ValidatedFundamentalShard(
                instrument=INSTRUMENT,
                manifest=manifest,
                valuations=(valuation,),
                indicators=(indicator,),
            )
        )
    ).compile(
        spec=spec,
        daily_binding=daily_binding,
        dataset=dataset,
    )

    assert len(panel.sessions) == 1
    observation = panel.sessions[0].observations[0]
    assert observation.signal_date == SIGNAL_DATE
    assert observation.execution_date == EXECUTION_DATE
    assert observation.earnings_yield == Decimal("0.05")
    assert observation.book_to_price == Decimal("0.2")
    assert observation.indicator_hash == indicator.content_hash
    assert len(panel.panel_hash) == 64


@pytest.mark.asyncio
async def test_panel_excludes_financials_not_visible_by_execution_open() -> None:
    spec = _spec()
    valuation = _valuation()
    indicator = replace(
        _indicator(),
        available_at=datetime(
            2026, 7, 21, 1, 30, 0, 1, tzinfo=UTC
        ),
    )
    manifest = DatasetManifest(
        source="tushare-fundamental",
        instruments=(INSTRUMENT,),
        start_time=datetime(2019, 12, 31, 16, tzinfo=UTC),
        end_time=datetime(2026, 7, 22, 7, tzinfo=UTC),
        as_of=AS_OF,
        record_hashes=(
            valuation.content_hash,
            indicator.content_hash,
        ),
        quality_report_hash="quality",
        production_complete=True,
        row_count=2,
    )
    dataset = FundamentalResearchDatasetManifest(
        spec_hash=spec.spec_hash,
        start_date=spec.start_date,
        end_date=spec.end_date,
        shards=(
            FundamentalDatasetShard(
                1,
                INSTRUMENT,
                manifest.manifest_hash,
            ),
        ),
    )
    daily_binding = FundamentalMarketBinding(
        daily_dataset_manifest_hash=(
            spec.daily_dataset_manifest_hash
        ),
        plan_hash=spec.plan_hash,
        spec_hash=spec.spec_hash,
        as_of=AS_OF,
        calendar_as_of=AS_OF,
        instruments=(INSTRUMENT,),
        sessions=(
            FundamentalMarketSessionBinding(
                session_date=SIGNAL_DATE,
                snapshot_hash="3" * 64,
                active_members=(INSTRUMENT,),
            ),
            FundamentalMarketSessionBinding(
                session_date=EXECUTION_DATE,
                snapshot_hash="3" * 64,
                active_members=(INSTRUMENT,),
            ),
        ),
    )
    panel = await FundamentalPanelCompiler(
        shard_reader=_ShardReader(
            ValidatedFundamentalShard(
                INSTRUMENT,
                manifest,
                (valuation,),
                (indicator,),
            )
        )
    ).compile(
        spec=spec,
        daily_binding=daily_binding,
        dataset=dataset,
    )

    assert panel.sessions[0].observations == ()
