from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from autoquant.backtest.low_volatility_forward import (
    LowVolatilityForwardEvidenceSpec,
    LowVolatilityForwardSessionBinding,
)
from autoquant.backtest.low_volatility_forward_panel import (
    LowVolatilityForwardPanelCompiler,
    _compile_panel,
)
from autoquant.backtest.low_volatility_portfolio import (
    LowVolatilityResearchSpec,
)
from autoquant.backtest.models import MarketState
from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.data.daily_ingestion import ValidatedDailyDataset
from autoquant.data.daily_models import (
    DailyBarRevision,
    DailyCoverageEvidence,
    InstrumentLifecycle,
    TradingSession,
)
from autoquant.data.models import DatasetManifest
from autoquant.data.research_data_campaign import (
    ResearchDatasetManifest,
    ResearchDatasetShard,
)
from autoquant.data.research_input import (
    ResearchInputPlan,
    ResearchUniverseBinding,
    ValidatedResearchShard,
)

_INSTRUMENT = "000001.XSHE"
_POLICY_HASH = "a" * 64


class _MarketCompiler:
    def __init__(self) -> None:
        self.calls = 0

    def compile(
        self,
        instrument: str,
        dataset: ValidatedDailyDataset,
    ) -> tuple[MarketState, ...]:
        self.calls += 1
        return tuple(
            MarketState(
                bar=bar,
                rules=AshareRuleBook().resolve(
                    instrument,
                    bar.session_date,
                    SecurityStatus(
                        risk_warning=False,
                        listing_session_number=1000,
                    ),
                ),
                suspended=False,
            )
            for bar in dataset.bars
        )


class _ShardReader:
    def __init__(
        self,
        values: tuple[ValidatedResearchShard, ...],
    ) -> None:
        self._values = values

    async def iter_all(self):
        for value in self._values:
            yield value


def _manifest(
    *,
    start: date,
    end: date,
    as_of: datetime,
    salt: str,
) -> DatasetManifest:
    start_time = datetime.combine(
        start,
        datetime.min.time(),
        tzinfo=UTC,
    )
    end_time = datetime.combine(
        end,
        datetime.min.time(),
        tzinfo=UTC,
    )
    return DatasetManifest(
        source="tushare",
        instruments=(_INSTRUMENT,),
        start_time=start_time,
        end_time=end_time,
        as_of=as_of,
        record_hashes=(salt * 64,),
        quality_report_hash="f" * 64,
        production_complete=True,
        row_count=1,
    )


def _dataset(
    dates: tuple[date, ...],
    *,
    close: str = "10",
) -> ValidatedDailyDataset:
    bars = []
    sessions = []
    for sequence, session_date in enumerate(dates, start=1):
        event_time = datetime.combine(
            session_date,
            datetime.min.time(),
            tzinfo=UTC,
        ) + timedelta(hours=7)
        bars.append(
            DailyBarRevision.from_values(
                source="tushare",
                instrument=_INSTRUMENT,
                session_date=session_date,
                event_time=event_time,
                available_at=event_time + timedelta(hours=1),
                ingested_at=event_time + timedelta(hours=2),
                source_revision=f"forward-panel-{sequence}",
                availability_policy="test-v1",
                evidence_hash=f"{sequence + 10:064x}",
                open_price=close,
                high_price=str(Decimal(close) + Decimal("1")),
                low_price=str(Decimal(close) - Decimal("1")),
                close_price=close,
                pre_close=close,
                volume=1_000_000,
                turnover="10000000",
            )
        )
        sessions.append(
            TradingSession(
                source="tushare",
                session_date=session_date,
                is_open=True,
                available_at=event_time + timedelta(hours=1),
                response_hash=f"{sequence + 100:064x}",
            )
        )
    lifecycle = InstrumentLifecycle(
        source="tushare",
        instrument=_INSTRUMENT,
        list_date=date(2020, 1, 1),
        delist_date=None,
        available_at=max(value.available_at for value in sessions),
        response_hash="e" * 64,
    )
    return ValidatedDailyDataset(
        bars=tuple(bars),
        factors=(),
        coverage=DailyCoverageEvidence(
            sessions=tuple(sessions),
            lifecycles=(lifecycle,),
            suspensions=(),
            price_limits=(),
        ),
    )


def _source() -> tuple[
    ResearchInputPlan,
    LowVolatilityResearchSpec,
    ValidatedResearchShard,
]:
    start = date(2026, 1, 1)
    end = date(2026, 1, 3)
    as_of = datetime(2026, 1, 4, tzinfo=UTC)
    manifest = _manifest(
        start=start,
        end=end,
        as_of=as_of,
        salt="1",
    )
    plan = ResearchInputPlan(
        dataset_manifest_hash="2" * 64,
        campaign_hash="3" * 64,
        policy_hash=_POLICY_HASH,
        start_date=start,
        end_date=end,
        shards=(
            ResearchDatasetShard(
                sequence=1,
                instrument=_INSTRUMENT,
                manifest_hash=manifest.manifest_hash,
            ),
        ),
        universes=(
            ResearchUniverseBinding(
                sequence=1,
                snapshot_hash="4" * 64,
                policy_hash=_POLICY_HASH,
                reference_date=start,
                knowledge_as_of=datetime(
                    2026,
                    1,
                    1,
                    8,
                    tzinfo=UTC,
                ),
                members=(_INSTRUMENT,),
            ),
        ),
    )
    spec = LowVolatilityResearchSpec(
        predecessor_result_hash="5" * 64,
        dataset_manifest_hash=plan.dataset_manifest_hash,
        plan_hash=plan.plan_hash,
        policy_hash=plan.policy_hash,
        start_date=plan.start_date,
        end_date=plan.end_date,
    )
    fragment = ValidatedResearchShard(
        instrument=_INSTRUMENT,
        manifest=manifest,
        dataset=_dataset(
            (
                date(2026, 1, 2),
                date(2026, 1, 3),
            )
        ),
    )
    return plan, spec, fragment


def _forward_fragment(
    session_date: date,
    *,
    salt: str,
    close: str = "10",
) -> ValidatedResearchShard:
    as_of = datetime.combine(
        session_date + timedelta(days=1),
        datetime.min.time(),
        tzinfo=UTC,
    )
    return ValidatedResearchShard(
        instrument=_INSTRUMENT,
        manifest=_manifest(
            start=session_date,
            end=session_date,
            as_of=as_of,
            salt=salt,
        ),
        dataset=_dataset((session_date,), close=close),
    )


def _binding(
    session_date: date,
    *,
    sequence: int,
    forward_spec_hash: str = "6" * 64,
) -> LowVolatilityForwardSessionBinding:
    return LowVolatilityForwardSessionBinding(
        forward_spec_hash=forward_spec_hash,
        dataset_manifest_hash=f"{sequence + 1000:064x}",
        policy_hash=_POLICY_HASH,
        session_date=session_date,
        snapshot_hash=f"{sequence + 2000:064x}",
        snapshot_reference_date=session_date - timedelta(days=1),
        calendar_content_hash=f"{sequence + 3000:064x}",
        instruments=(_INSTRUMENT,),
    )


def test_forward_panel_combines_source_and_ordered_frozen_sessions() -> None:
    plan, spec, source_fragment = _source()
    forward_dates = (
        date(2026, 1, 4),
        date(2026, 1, 5),
    )
    fragments = (
        source_fragment,
        _forward_fragment(
            forward_dates[0],
            salt="7",
        ),
        _forward_fragment(
            forward_dates[1],
            salt="8",
        ),
    )
    bindings = tuple(
        _binding(value, sequence=sequence)
        for sequence, value in enumerate(
            forward_dates,
            start=1,
        )
    )
    compiler = _MarketCompiler()

    panel = _compile_panel(
        fragments=fragments,
        source_plan=plan,
        source_spec=spec,
        bindings=bindings,
        market_compiler=compiler,
    )

    assert compiler.calls == 1
    assert tuple(
        value.session_date for value in panel.sessions
    ) == (
        date(2026, 1, 2),
        date(2026, 1, 3),
        *forward_dates,
    )
    assert tuple(
        value.snapshot_hash for value in panel.sessions[-2:]
    ) == tuple(value.snapshot_hash for value in bindings)
    assert len(panel.histories) == 1
    assert len(panel.histories[0].markets) == 4
    assert panel.as_of == datetime(2026, 1, 6, tzinfo=UTC)


def test_forward_panel_rejects_conflicting_frozen_rows() -> None:
    plan, spec, source_fragment = _source()
    conflict = _forward_fragment(
        date(2026, 1, 3),
        salt="9",
        close="11",
    )

    with pytest.raises(ValueError, match="conflict"):
        _compile_panel(
            fragments=(source_fragment, conflict),
            source_plan=plan,
            source_spec=spec,
            bindings=(
                _binding(
                    date(2026, 1, 4),
                    sequence=1,
                ),
            ),
            market_compiler=_MarketCompiler(),
        )


@pytest.mark.asyncio
async def test_public_forward_panel_requires_full_union_dataset() -> None:
    plan, spec, source_fragment = _source()
    forward_start = date(2026, 1, 4)
    forward_spec = LowVolatilityForwardEvidenceSpec(
        predecessor_result_hash="7" * 64,
        predecessor_assessment_hash="8" * 64,
        source_spec_hash=spec.spec_hash,
        source_dataset_manifest_hash=spec.dataset_manifest_hash,
        forward_start_date=forward_start,
        maximum_annualized_stability_gap=Decimal("0.15"),
    )
    forward_dates = tuple(
        forward_start + timedelta(days=index)
        for index in range(126)
    )
    bindings = tuple(
        _binding(
            session_date,
            sequence=sequence,
            forward_spec_hash=forward_spec.spec_hash,
        )
        for sequence, session_date in enumerate(
            forward_dates,
            start=1,
        )
    )
    shard_manifest = _manifest(
        start=forward_dates[0],
        end=forward_dates[-1],
        as_of=datetime(2026, 5, 10, tzinfo=UTC),
        salt="9",
    )
    aggregate = ResearchDatasetManifest(
        campaign_hash="a" * 64,
        policy_hash=spec.policy_hash,
        snapshot_hashes=tuple(
            value.snapshot_hash for value in bindings
        ),
        start_date=forward_dates[0],
        end_date=forward_dates[-1],
        shards=(
            ResearchDatasetShard(
                sequence=1,
                instrument=_INSTRUMENT,
                manifest_hash=shard_manifest.manifest_hash,
            ),
        ),
    )
    forward_fragment = ValidatedResearchShard(
        instrument=_INSTRUMENT,
        manifest=shard_manifest,
        dataset=_dataset(forward_dates),
    )
    market_compiler = _MarketCompiler()

    panel = await LowVolatilityForwardPanelCompiler(
        historical_reader=_ShardReader((source_fragment,)),
        forward_reader=_ShardReader((forward_fragment,)),
        market_compiler=market_compiler,
    ).compile(
        source_plan=plan,
        source_spec=spec,
        forward_spec=forward_spec,
        forward_manifest=aggregate,
        bindings=bindings,
    )

    assert len(panel.sessions) == 128
    assert tuple(
        value.session_date for value in panel.sessions[-126:]
    ) == forward_dates
    assert market_compiler.calls == 1
