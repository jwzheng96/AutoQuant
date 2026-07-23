from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import date, datetime
from itertools import pairwise
from typing import Protocol

from autoquant.backtest.dynamic_portfolio import (
    DynamicPortfolioResearchSpec,
)
from autoquant.backtest.models import MarketState
from autoquant.backtest.runner import ManifestMarketCompiler
from autoquant.clock import to_utc
from autoquant.data.daily_ingestion import ValidatedDailyDataset
from autoquant.data.models import (
    _canonical_hash,
    _require_lowercase_sha256,
)
from autoquant.data.research_input import (
    ResearchInputPlan,
    ValidatedResearchShard,
)

DYNAMIC_MARKET_PANEL_VERSION = "dynamic-point-in-time-market-panel-v1"


class ResearchShardStream(Protocol):
    def iter_all(self) -> AsyncIterator[ValidatedResearchShard]: ...


class ResearchMarketCompiler(Protocol):
    def compile(
        self,
        instrument: str,
        dataset: ValidatedDailyDataset,
    ) -> tuple[MarketState, ...]: ...


@dataclass(frozen=True, slots=True)
class InstrumentMarketHistory:
    instrument: str
    markets: tuple[MarketState, ...]
    list_date: date
    delist_date: date | None

    def __post_init__(self) -> None:
        markets = tuple(self.markets)
        if (
            not markets
            or any(
                value.bar.instrument != self.instrument
                for value in markets
            )
            or any(
                current.bar.session_date
                >= following.bar.session_date
                for current, following in pairwise(markets)
            )
            or self.list_date > markets[0].bar.session_date
            or (
                self.delist_date is not None
                and self.delist_date <= markets[-1].bar.session_date
            )
        ):
            raise ValueError("instrument market history is inconsistent")
        object.__setattr__(self, "markets", markets)


@dataclass(frozen=True, slots=True)
class DynamicMarketSession:
    session_date: date
    snapshot_hash: str
    active_members: tuple[str, ...]
    markets: tuple[MarketState, ...]

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.snapshot_hash,
            name="dynamic session snapshot hash",
        )
        active_members = tuple(self.active_members)
        markets = tuple(
            sorted(
                self.markets,
                key=lambda value: value.bar.instrument,
            )
        )
        if (
            not active_members
            or active_members != tuple(sorted(active_members))
            or len(set(active_members)) != len(active_members)
            or not markets
            or any(
                value.bar.session_date != self.session_date
                for value in markets
            )
            or len(
                {value.bar.instrument for value in markets}
            )
            != len(markets)
        ):
            raise ValueError("dynamic market session is inconsistent")
        object.__setattr__(self, "active_members", active_members)
        object.__setattr__(self, "markets", markets)


@dataclass(frozen=True, slots=True)
class DynamicMarketPanel:
    dataset_manifest_hash: str
    plan_hash: str
    spec_hash: str
    as_of: datetime
    sessions: tuple[DynamicMarketSession, ...]
    histories: tuple[InstrumentMarketHistory, ...]
    version: str = DYNAMIC_MARKET_PANEL_VERSION
    panel_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.dataset_manifest_hash,
            name="dynamic panel dataset manifest hash",
        )
        _require_lowercase_sha256(
            self.plan_hash,
            name="dynamic panel input plan hash",
        )
        _require_lowercase_sha256(
            self.spec_hash,
            name="dynamic panel strategy spec hash",
        )
        sessions = tuple(self.sessions)
        histories = tuple(
            sorted(self.histories, key=lambda value: value.instrument)
        )
        if (
            not sessions
            or any(
                current.session_date >= following.session_date
                for current, following in pairwise(sessions)
            )
            or not histories
            or len({value.instrument for value in histories})
            != len(histories)
            or self.version != DYNAMIC_MARKET_PANEL_VERSION
        ):
            raise ValueError("dynamic market panel is inconsistent")
        object.__setattr__(self, "sessions", sessions)
        object.__setattr__(self, "histories", histories)
        object.__setattr__(
            self,
            "as_of",
            to_utc(self.as_of, name="dynamic panel as_of"),
        )
        object.__setattr__(
            self,
            "panel_hash",
            _canonical_hash(
                {
                    "as_of": self.as_of.isoformat(),
                    "dataset_manifest_hash": self.dataset_manifest_hash,
                    "histories": [
                        {
                            "delist_date": (
                                None
                                if value.delist_date is None
                                else value.delist_date.isoformat()
                            ),
                            "first_market_date": (
                                value.markets[0]
                                .bar.session_date.isoformat()
                            ),
                            "instrument": value.instrument,
                            "last_market_date": (
                                value.markets[-1]
                                .bar.session_date.isoformat()
                            ),
                            "list_date": value.list_date.isoformat(),
                            "market_count": len(value.markets),
                        }
                        for value in histories
                    ],
                    "plan_hash": self.plan_hash,
                    "sessions": [
                        {
                            "active_member_count": len(
                                value.active_members
                            ),
                            "market_count": len(value.markets),
                            "session_date": (
                                value.session_date.isoformat()
                            ),
                            "snapshot_hash": value.snapshot_hash,
                        }
                        for value in sessions
                    ],
                    "spec_hash": self.spec_hash,
                    "version": self.version,
                }
            ),
        )

    def history(self, instrument: str) -> InstrumentMarketHistory:
        for value in self.histories:
            if value.instrument == instrument:
                return value
        raise LookupError("instrument history is not in the dynamic panel")


class DynamicMarketPanelCompiler:
    def __init__(
        self,
        *,
        shard_reader: ResearchShardStream,
        market_compiler: ResearchMarketCompiler | None = None,
    ) -> None:
        self._reader = shard_reader
        self._compiler = market_compiler or ManifestMarketCompiler()

    async def compile(
        self,
        *,
        plan: ResearchInputPlan,
        spec: DynamicPortfolioResearchSpec,
    ) -> DynamicMarketPanel:
        return await self.compile_bound(
            plan=plan,
            dataset_manifest_hash=spec.dataset_manifest_hash,
            policy_hash=spec.policy_hash,
            start_date=spec.start_date,
            end_date=spec.end_date,
            spec_hash=spec.spec_hash,
        )

    async def compile_bound(
        self,
        *,
        plan: ResearchInputPlan,
        dataset_manifest_hash: str,
        policy_hash: str,
        start_date: date,
        end_date: date,
        spec_hash: str,
    ) -> DynamicMarketPanel:
        if (
            dataset_manifest_hash != plan.dataset_manifest_hash
            or policy_hash != plan.policy_hash
            or start_date != plan.start_date
            or end_date != plan.end_date
        ):
            raise ValueError(
                "dynamic strategy spec does not match the research plan"
            )
        open_dates: tuple[date, ...] | None = None
        histories: list[InstrumentMarketHistory] = []
        markets_by_date: dict[date, list[MarketState]] = {}
        cutoffs: list[datetime] = []
        async for shard in self._reader.iter_all():
            shard_open_dates = tuple(
                value.session_date
                for value in shard.dataset.coverage.sessions
                if value.is_open
                and plan.start_date <= value.session_date <= plan.end_date
            )
            if open_dates is None:
                open_dates = shard_open_dates
            elif shard_open_dates != open_dates:
                raise ValueError(
                    "research shards do not share one trading calendar"
                )
            lifecycles = tuple(
                value
                for value in shard.dataset.coverage.lifecycles
                if value.instrument == shard.instrument
            )
            if len(lifecycles) != 1:
                raise ValueError(
                    "research shard must have one instrument lifecycle"
                )
            markets = self._compiler.compile(
                shard.instrument,
                shard.dataset,
            )
            history = InstrumentMarketHistory(
                instrument=shard.instrument,
                markets=markets,
                list_date=lifecycles[0].list_date,
                delist_date=lifecycles[0].delist_date,
            )
            histories.append(history)
            cutoffs.append(shard.manifest.as_of)
            for market in history.markets:
                markets_by_date.setdefault(
                    market.bar.session_date,
                    [],
                ).append(market)
        if (
            open_dates is None
            or not open_dates
            or len(histories) != len(plan.shards)
            or {value.instrument for value in histories}
            != set(plan.instruments)
        ):
            raise ValueError(
                "dynamic panel is missing calendar or shard histories"
            )
        sessions: list[DynamicMarketSession] = []
        for session_date in open_dates:
            universe = plan.universe_for(session_date)
            if universe is None:
                continue
            markets = tuple(markets_by_date.get(session_date, ()))
            if not markets:
                raise ValueError(
                    "open dynamic session has no observable markets"
                )
            sessions.append(
                DynamicMarketSession(
                    session_date=session_date,
                    snapshot_hash=universe.snapshot_hash,
                    active_members=universe.members,
                    markets=markets,
                )
            )
        return DynamicMarketPanel(
            dataset_manifest_hash=plan.dataset_manifest_hash,
            plan_hash=plan.plan_hash,
            spec_hash=spec_hash,
            as_of=max(cutoffs),
            sessions=tuple(sessions),
            histories=tuple(histories),
        )
