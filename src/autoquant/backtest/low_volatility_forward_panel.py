from __future__ import annotations

from collections.abc import (
    AsyncIterator,
    Callable,
    Iterable,
)
from datetime import date
from itertools import chain
from typing import Any, Protocol, TypeVar

from autoquant.backtest.dynamic_panel import (
    DynamicMarketPanel,
    DynamicMarketSession,
    InstrumentMarketHistory,
)
from autoquant.backtest.low_volatility_forward import (
    LowVolatilityForwardEvidenceSpec,
    LowVolatilityForwardSessionBinding,
)
from autoquant.backtest.low_volatility_portfolio import (
    LowVolatilityResearchSpec,
)
from autoquant.backtest.models import MarketState
from autoquant.backtest.runner import ManifestMarketCompiler
from autoquant.data.daily_ingestion import ValidatedDailyDataset
from autoquant.data.daily_models import (
    DailyCoverageEvidence,
    InstrumentLifecycle,
)
from autoquant.data.research_data_campaign import (
    ResearchDatasetManifest,
)
from autoquant.data.research_input import (
    ResearchInputPlan,
    ValidatedResearchShard,
)


class ForwardResearchShardStream(Protocol):
    def iter_all(
        self,
    ) -> AsyncIterator[ValidatedResearchShard]: ...


class ForwardMarketCompiler(Protocol):
    def compile(
        self,
        instrument: str,
        dataset: ValidatedDailyDataset,
    ) -> tuple[MarketState, ...]: ...


class LowVolatilityForwardPanelCompiler:
    """Rebuild one hash-addressed historical-plus-forward market panel."""

    def __init__(
        self,
        *,
        historical_reader: ForwardResearchShardStream,
        forward_reader: ForwardResearchShardStream,
        market_compiler: ForwardMarketCompiler | None = None,
    ) -> None:
        self._historical_reader = historical_reader
        self._forward_reader = forward_reader
        self._market_compiler = (
            market_compiler or ManifestMarketCompiler()
        )

    async def compile(
        self,
        *,
        source_plan: ResearchInputPlan,
        source_spec: LowVolatilityResearchSpec,
        forward_spec: LowVolatilityForwardEvidenceSpec,
        forward_manifest: ResearchDatasetManifest,
        bindings: tuple[LowVolatilityForwardSessionBinding, ...],
    ) -> DynamicMarketPanel:
        ordered_bindings = tuple(bindings)
        if (
            source_plan.dataset_manifest_hash
            != source_spec.dataset_manifest_hash
            or source_plan.plan_hash != source_spec.plan_hash
            or source_plan.policy_hash != source_spec.policy_hash
            or source_plan.start_date != source_spec.start_date
            or source_plan.end_date != source_spec.end_date
            or forward_spec.source_spec_hash != source_spec.spec_hash
            or forward_spec.source_dataset_manifest_hash
            != source_spec.dataset_manifest_hash
            or len(ordered_bindings)
            != forward_spec.minimum_forward_sessions
        ):
            raise ValueError(
                "low-volatility forward panel provenance is invalid"
            )
        forward_dates = tuple(
            value.session_date for value in ordered_bindings
        )
        if (
            forward_dates[0] != forward_spec.forward_start_date
            or forward_dates
            != tuple(sorted(forward_dates))
            or len(set(forward_dates)) != len(forward_dates)
        ):
            raise ValueError(
                "low-volatility forward panel window is invalid"
            )
        fragments: list[ValidatedResearchShard] = []
        historical = tuple(
            [value async for value in self._historical_reader.iter_all()]
        )
        if (
            tuple(value.instrument for value in historical)
            != source_plan.instruments
            or any(
                value.manifest.manifest_hash
                != source_plan.shard_manifest_for(value.instrument)
                for value in historical
            )
            or forward_manifest.source != "tushare"
            or forward_manifest.policy_hash
            != source_spec.policy_hash
            or forward_manifest.start_date != forward_dates[0]
            or forward_manifest.end_date != forward_dates[-1]
            or forward_manifest.snapshot_hashes
            != _ordered_unique(
                value.snapshot_hash for value in ordered_bindings
            )
            or forward_manifest.instruments
            != tuple(
                sorted(
                    {
                        instrument
                        for value in ordered_bindings
                        for instrument in value.instruments
                    }
                )
            )
        ):
            raise ValueError(
                "low-volatility historical shards are incomplete"
            )
        fragments.extend(historical)
        values = tuple(
            [value async for value in self._forward_reader.iter_all()]
        )
        if (
            tuple(value.instrument for value in values)
            != forward_manifest.instruments
            or tuple(
                value.manifest.manifest_hash for value in values
            )
            != tuple(
                value.manifest_hash
                for value in forward_manifest.shards
            )
            or any(
                not value.dataset.bars
                or value.dataset.bars[0].session_date
                < forward_dates[0]
                or value.dataset.bars[-1].session_date
                > forward_dates[-1]
                for value in values
            )
        ):
            raise ValueError(
                "low-volatility forward evaluation shards are incomplete"
            )
        fragments.extend(values)
        return _compile_panel(
            fragments=tuple(fragments),
            source_plan=source_plan,
            source_spec=source_spec,
            bindings=ordered_bindings,
            market_compiler=self._market_compiler,
        )


def _compile_panel(
    *,
    fragments: tuple[ValidatedResearchShard, ...],
    source_plan: ResearchInputPlan,
    source_spec: LowVolatilityResearchSpec,
    bindings: tuple[LowVolatilityForwardSessionBinding, ...],
    market_compiler: ForwardMarketCompiler,
) -> DynamicMarketPanel:
    instruments = tuple(
        sorted({value.instrument for value in fragments})
    )
    sessions = _unique(
        chain.from_iterable(
            value.dataset.coverage.sessions for value in fragments
        ),
        key=lambda value: value.session_date,
        order=lambda value: value.session_date,
        name="trading session",
    )
    session_by_date = {
        value.session_date: value for value in sessions
    }
    histories: list[InstrumentMarketHistory] = []
    markets_by_date: dict[date, list[MarketState]] = {}
    for instrument in instruments:
        instrument_fragments = tuple(
            value for value in fragments
            if value.instrument == instrument
        )
        bars = _unique(
            chain.from_iterable(
                value.dataset.bars
                for value in instrument_fragments
            ),
            key=lambda value: value.session_date,
            order=lambda value: value.session_date,
            name=f"{instrument} bar",
        )
        factors = _unique(
            chain.from_iterable(
                value.dataset.factors
                for value in instrument_fragments
            ),
            key=lambda value: value.session_date,
            order=lambda value: value.session_date,
            name=f"{instrument} factor",
        )
        suspensions = _unique(
            chain.from_iterable(
                value.dataset.coverage.suspensions
                for value in instrument_fragments
            ),
            key=lambda value: value.session_date,
            order=lambda value: value.session_date,
            name=f"{instrument} suspension",
        )
        price_limits = _unique(
            chain.from_iterable(
                value.dataset.coverage.price_limits
                for value in instrument_fragments
            ),
            key=lambda value: value.session_date,
            order=lambda value: value.session_date,
            name=f"{instrument} price limit",
        )
        lifecycle = _latest_lifecycle(
            instrument,
            tuple(
                chain.from_iterable(
                    value.dataset.coverage.lifecycles
                    for value in instrument_fragments
                )
            ),
        )
        calendar = tuple(
            session_by_date[value.session_date] for value in bars
        )
        dataset = ValidatedDailyDataset(
            bars=bars,
            factors=factors,
            coverage=DailyCoverageEvidence(
                sessions=calendar,
                lifecycles=(lifecycle,),
                suspensions=suspensions,
                price_limits=price_limits,
            ),
        )
        markets = self_consistent_markets(
            compiler=market_compiler,
            instrument=instrument,
            dataset=dataset,
        )
        history = InstrumentMarketHistory(
            instrument=instrument,
            markets=markets,
            list_date=lifecycle.list_date,
            delist_date=lifecycle.delist_date,
        )
        histories.append(history)
        for market in markets:
            markets_by_date.setdefault(
                market.bar.session_date,
                [],
            ).append(market)
    session_bindings = {
        value.session_date: value for value in bindings
    }
    dynamic_sessions: list[DynamicMarketSession] = []
    expected_dates = tuple(
        value.session_date
        for value in sessions
        if (
            source_plan.start_date
            <= value.session_date
            <= source_plan.end_date
            and value.is_open
            and source_plan.universe_for(value.session_date)
            is not None
        )
    ) + tuple(value.session_date for value in bindings)
    forward_open_dates = tuple(
        value.session_date
        for value in sessions
        if (
            bindings[0].session_date
            <= value.session_date
            <= bindings[-1].session_date
            and value.is_open
        )
    )
    if forward_open_dates != tuple(
        value.session_date for value in bindings
    ):
        raise ValueError(
            "low-volatility forward evaluation calendar differs "
            "from frozen bindings"
        )
    if len(set(expected_dates)) != len(expected_dates):
        raise ValueError(
            "low-volatility forward sessions overlap source history"
        )
    for session_date in expected_dates:
        forward = session_bindings.get(session_date)
        if forward is None:
            universe = source_plan.universe_for(session_date)
            if universe is None:
                raise ValueError(
                    "low-volatility source universe is missing"
                )
            snapshot_hash = universe.snapshot_hash
            active_members = universe.members
        else:
            snapshot_hash = forward.snapshot_hash
            active_members = forward.instruments
        markets = tuple(
            sorted(
                markets_by_date.get(session_date, ()),
                key=lambda value: value.bar.instrument,
            )
        )
        if not markets:
            raise ValueError(
                "low-volatility forward session market coverage is "
                "incomplete"
            )
        dynamic_sessions.append(
            DynamicMarketSession(
                session_date=session_date,
                snapshot_hash=snapshot_hash,
                active_members=active_members,
                markets=markets,
            )
        )
    return DynamicMarketPanel(
        dataset_manifest_hash=source_plan.dataset_manifest_hash,
        plan_hash=source_plan.plan_hash,
        spec_hash=source_spec.spec_hash,
        as_of=max(value.manifest.as_of for value in fragments),
        sessions=tuple(dynamic_sessions),
        histories=tuple(histories),
    )


def self_consistent_markets(
    *,
    compiler: ForwardMarketCompiler,
    instrument: str,
    dataset: ValidatedDailyDataset,
) -> tuple[MarketState, ...]:
    """Compile once and verify exact date coverage before panel assembly."""

    markets = compiler.compile(instrument, dataset)
    if tuple(value.bar.session_date for value in markets) != tuple(
        value.session_date for value in dataset.bars
    ):
        raise ValueError(
            "low-volatility compiled market dates differ"
        )
    return markets


_T = TypeVar("_T")
_K = TypeVar("_K")


def _unique(
    values: Iterable[_T],
    *,
    key: Callable[[_T], _K],
    order: Callable[[_T], Any],
    name: str,
) -> tuple[_T, ...]:
    unique: dict[_K, _T] = {}
    for value in values:
        identity = key(value)
        existing = unique.setdefault(identity, value)
        if existing != value:
            raise ValueError(
                f"low-volatility forward {name} conflict"
            )
    return tuple(sorted(unique.values(), key=order))


def _latest_lifecycle(
    instrument: str,
    values: tuple[InstrumentLifecycle, ...],
) -> InstrumentLifecycle:
    matches = tuple(
        value for value in values if value.instrument == instrument
    )
    if (
        not matches
        or len({value.list_date for value in matches}) != 1
    ):
        raise ValueError(
            "low-volatility forward lifecycle is inconsistent"
        )
    return max(
        matches,
        key=lambda value: (
            value.available_at,
            value.content_hash,
        ),
    )


def _ordered_unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(values))
