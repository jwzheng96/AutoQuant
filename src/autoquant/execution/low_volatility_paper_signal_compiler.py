from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import date, datetime
from typing import Protocol

from autoquant.backtest.low_volatility_portfolio import (
    LowVolatilityResearchSpec,
)
from autoquant.backtest.low_volatility_strategy import (
    LOW_VOLATILITY_EXECUTABLE_PANEL_VERSION,
    LowVolatilityObservation,
    _realized_volatility,
)
from autoquant.backtest.models import MarketState
from autoquant.backtest.runner import ManifestMarketCompiler
from autoquant.data.daily_ingestion import ValidatedDailyDataset
from autoquant.data.models import _canonical_hash
from autoquant.data.research_data_campaign import (
    ResearchDatasetManifest,
)
from autoquant.data.research_input import (
    ValidatedResearchShard,
)
from autoquant.execution.low_volatility_paper_approval import (
    LowVolatilityPaperCandidateApproval,
)
from autoquant.execution.low_volatility_paper_signal import (
    LOW_VOLATILITY_PAPER_SIGNAL_GENESIS_HASH,
    LowVolatilityPaperDailySignal,
    LowVolatilityPaperValuation,
)
from autoquant.execution.session_rules import SessionRuleSet


class LowVolatilityPaperShardStream(Protocol):
    def iter_all(
        self,
    ) -> AsyncIterator[ValidatedResearchShard]: ...


class LowVolatilityPaperMarketCompiler(Protocol):
    def compile(
        self,
        instrument: str,
        dataset: ValidatedDailyDataset,
    ) -> tuple[MarketState, ...]: ...


class LowVolatilityPaperSignalCompiler:
    """Compile only prior-close observations; execution parity stays blocked."""

    def __init__(
        self,
        *,
        reader: LowVolatilityPaperShardStream,
        market_compiler: (LowVolatilityPaperMarketCompiler | None) = None,
    ) -> None:
        self._reader = reader
        self._market_compiler = market_compiler or ManifestMarketCompiler()

    async def compile(
        self,
        *,
        candidate: LowVolatilityPaperCandidateApproval,
        spec: LowVolatilityResearchSpec,
        manifest: ResearchDatasetManifest,
        snapshot_hash: str,
        universe_members: tuple[str, ...],
        rule_set: SessionRuleSet,
        session_date: date,
        previous: LowVolatilityPaperDailySignal | None,
        prepared_by: str,
        prepared_at: datetime,
    ) -> LowVolatilityPaperDailySignal:
        members = tuple(universe_members)
        evidence_instruments = manifest.instruments
        expected_previous_selected = () if previous is None else previous.selected_instruments
        if (
            candidate.source_spec_hash != spec.spec_hash
            or manifest.policy_hash != spec.policy_hash
            or manifest.snapshot_hashes != (snapshot_hash,)
            or manifest.end_date >= session_date
            or members != tuple(sorted(members))
            or not members
            or not set(members) <= set(evidence_instruments)
            or not set(evidence_instruments) <= set(candidate.instruments)
            or not set(expected_previous_selected) <= set(evidence_instruments)
            or rule_set.session_date != session_date
            or rule_set.as_of != prepared_at
            or tuple(value.instrument for value in rule_set.rules) != evidence_instruments
        ):
            raise ValueError("low-volatility paper signal provenance is invalid")
        if previous is not None and (
            previous.candidate_approval_hash != candidate.approval_hash
            or previous.account_id != candidate.account_id
            or previous.strategy_id != candidate.strategy_id
            or previous.source_spec_hash != spec.spec_hash
            or previous.risk_policy_hash != candidate.risk_policy_hash
            or previous.session_date >= session_date
        ):
            raise ValueError("low-volatility previous paper signal is inconsistent")
        shards = tuple([value async for value in self._reader.iter_all()])
        if tuple(value.instrument for value in shards) != evidence_instruments or tuple(
            value.manifest.manifest_hash for value in shards
        ) != tuple(value.manifest_hash for value in manifest.shards):
            raise ValueError("low-volatility paper signal shards are incomplete")
        open_dates: tuple[date, ...] | None = None
        observations: list[LowVolatilityObservation] = []
        valuations: list[LowVolatilityPaperValuation] = []
        for shard in shards:
            shard_open_dates = tuple(
                value.session_date
                for value in shard.dataset.coverage.sessions
                if value.is_open and manifest.start_date <= value.session_date <= manifest.end_date
            )
            if open_dates is None:
                open_dates = shard_open_dates
            elif shard_open_dates != open_dates:
                raise ValueError("paper signal shards do not share one calendar")
            markets = self._market_compiler.compile(
                shard.instrument,
                shard.dataset,
            )
            if not markets:
                raise ValueError("paper signal shard has no observable market")
            latest = markets[-1].bar
            if latest.session_date > manifest.end_date:
                raise ValueError("paper signal valuation exceeds its cutoff")
            valuations.append(
                LowVolatilityPaperValuation(
                    instrument=shard.instrument,
                    price_date=latest.session_date,
                    adjusted_close=latest.close_price,
                    prior_volume=latest.volume,
                    adjusted_bar_hash=latest.content_hash,
                )
            )
            market_dates = tuple(value.bar.session_date for value in markets)
            if shard.instrument not in members or market_dates != shard_open_dates:
                continue
            closes = tuple(value.bar.close_price for value in markets)
            observations.append(
                LowVolatilityObservation(
                    instrument=shard.instrument,
                    signal_date=manifest.end_date,
                    execution_date=session_date,
                    volatility=_realized_volatility(closes),
                    window_hash=_canonical_hash(
                        {
                            "bar_hashes": [value.bar.content_hash for value in markets],
                            "instrument": shard.instrument,
                            "version": (LOW_VOLATILITY_EXECUTABLE_PANEL_VERSION),
                        }
                    ),
                )
            )
        if (
            open_dates is None
            or len(open_dates) != spec.minimum_history_sessions
            or open_dates[0] != manifest.start_date
            or open_dates[-1] != manifest.end_date
        ):
            raise ValueError("paper signal window is not exactly 253 sessions")
        session_sequence = 1 if previous is None else previous.session_sequence + 1
        rebalance_due = (session_sequence - 1) % (spec.rebalance_sessions) == 0
        ordered_observations = tuple(
            sorted(
                observations,
                key=lambda value: value.instrument,
            )
        )
        if rebalance_due:
            selected = (
                tuple(
                    value.instrument
                    for value in sorted(
                        ordered_observations,
                        key=lambda value: (
                            value.volatility,
                            value.instrument,
                        ),
                    )[: spec.selection_count]
                )
                if len(ordered_observations) >= spec.minimum_eligible_members
                else ()
            )
            selected = tuple(sorted(selected))
        else:
            selected = expected_previous_selected
        return LowVolatilityPaperDailySignal(
            candidate_approval_hash=candidate.approval_hash,
            account_id=candidate.account_id,
            strategy_id=candidate.strategy_id,
            source_spec_hash=spec.spec_hash,
            risk_policy_hash=candidate.risk_policy_hash,
            session_sequence=session_sequence,
            session_date=session_date,
            signal_date=manifest.end_date,
            window_start_date=manifest.start_date,
            previous_signal_hash=(
                LOW_VOLATILITY_PAPER_SIGNAL_GENESIS_HASH
                if previous is None
                else previous.signal_hash
            ),
            snapshot_hash=snapshot_hash,
            dataset_manifest_hash=manifest.manifest_hash,
            rule_set_hash=rule_set.rule_set_hash,
            universe_members=members,
            evidence_instruments=evidence_instruments,
            observations=ordered_observations,
            valuations=tuple(valuations),
            selected_instruments=selected,
            rebalance_due=rebalance_due,
            prepared_by=prepared_by,
            prepared_at=prepared_at,
        )
