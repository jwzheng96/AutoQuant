from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from itertools import pairwise
from typing import Protocol

from autoquant.backtest.dynamic_panel import DynamicMarketPanel
from autoquant.backtest.fundamental_portfolio import (
    FundamentalPortfolioResearchSpec,
)
from autoquant.clock import SHANGHAI, to_utc
from autoquant.data.fundamental_dataset import (
    FundamentalResearchDatasetManifest,
    ValidatedFundamentalShard,
)
from autoquant.data.fundamental_models import (
    DailyValuationRevision,
    FinancialIndicatorRevision,
)
from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
)

FUNDAMENTAL_PANEL_VERSION = "fundamental-point-in-time-panel-v1"


class FundamentalShardStream(Protocol):
    def iter_all(
        self,
    ) -> AsyncIterator[ValidatedFundamentalShard]: ...


@dataclass(frozen=True, slots=True)
class FundamentalFeatureObservation:
    instrument: str
    signal_date: date
    execution_date: date
    report_period: date
    earnings_yield: Decimal
    book_to_price: Decimal
    roe_diluted_percent: Decimal
    roa_percent: Decimal
    operating_cashflow_to_revenue_percent: Decimal
    valuation_hash: str
    indicator_hash: str
    observation_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            self.signal_date >= self.execution_date
            or self.report_period > self.signal_date
        ):
            raise ValueError(
                "fundamental observation dates are inconsistent"
            )
        for value in (
            self.earnings_yield,
            self.book_to_price,
        ):
            if (
                not isinstance(value, Decimal)
                or not value.is_finite()
                or value <= 0
            ):
                raise ValueError(
                    "fundamental value factors must be positive"
                )
        for value in (
            self.roe_diluted_percent,
            self.roa_percent,
            self.operating_cashflow_to_revenue_percent,
        ):
            if not isinstance(value, Decimal) or not value.is_finite():
                raise ValueError(
                    "fundamental quality factors must be finite"
                )
        _require_lowercase_sha256(
            self.valuation_hash,
            name="valuation content hash",
        )
        _require_lowercase_sha256(
            self.indicator_hash,
            name="indicator content hash",
        )
        object.__setattr__(
            self,
            "observation_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "book_to_price": _decimal_text(self.book_to_price),
            "earnings_yield": _decimal_text(self.earnings_yield),
            "execution_date": self.execution_date.isoformat(),
            "indicator_hash": self.indicator_hash,
            "instrument": self.instrument,
            "operating_cashflow_to_revenue_percent": _decimal_text(
                self.operating_cashflow_to_revenue_percent
            ),
            "report_period": self.report_period.isoformat(),
            "roa_percent": _decimal_text(self.roa_percent),
            "roe_diluted_percent": _decimal_text(
                self.roe_diluted_percent
            ),
            "signal_date": self.signal_date.isoformat(),
            "valuation_hash": self.valuation_hash,
        }


@dataclass(frozen=True, slots=True)
class FundamentalFeatureSession:
    execution_date: date
    signal_date: date
    snapshot_hash: str
    active_member_count: int
    observations: tuple[FundamentalFeatureObservation, ...]
    session_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.snapshot_hash,
            name="fundamental session snapshot hash",
        )
        observations = tuple(
            sorted(
                self.observations,
                key=lambda value: value.instrument,
            )
        )
        if (
            self.signal_date >= self.execution_date
            or self.active_member_count < 1
            or len(observations) > self.active_member_count
            or len({value.instrument for value in observations})
            != len(observations)
            or any(
                value.signal_date != self.signal_date
                or value.execution_date != self.execution_date
                for value in observations
            )
        ):
            raise ValueError(
                "fundamental feature session is inconsistent"
            )
        object.__setattr__(self, "observations", observations)
        object.__setattr__(
            self,
            "session_hash",
            _canonical_hash(
                {
                    "active_member_count": self.active_member_count,
                    "execution_date": self.execution_date.isoformat(),
                    "observation_hashes": [
                        value.observation_hash
                        for value in observations
                    ],
                    "signal_date": self.signal_date.isoformat(),
                    "snapshot_hash": self.snapshot_hash,
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class FundamentalResearchPanel:
    spec_hash: str
    daily_panel_hash: str
    fundamental_dataset_manifest_hash: str
    as_of: datetime
    sessions: tuple[FundamentalFeatureSession, ...]
    minimum_required_members: int
    version: str = FUNDAMENTAL_PANEL_VERSION
    panel_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.spec_hash, "fundamental panel spec hash"),
            (self.daily_panel_hash, "daily market panel hash"),
            (
                self.fundamental_dataset_manifest_hash,
                "fundamental dataset manifest hash",
            ),
        ):
            _require_lowercase_sha256(value, name=name)
        sessions = tuple(self.sessions)
        if (
            not sessions
            or any(
                current.execution_date >= following.execution_date
                for current, following in pairwise(sessions)
            )
            or self.minimum_required_members < 1
            or self.version != FUNDAMENTAL_PANEL_VERSION
        ):
            raise ValueError(
                "fundamental research panel is inconsistent"
            )
        object.__setattr__(self, "sessions", sessions)
        object.__setattr__(
            self,
            "as_of",
            to_utc(self.as_of, name="fundamental panel as_of"),
        )
        object.__setattr__(
            self,
            "panel_hash",
            _canonical_hash(self.identity_payload()),
        )

    def identity_payload(self) -> dict[str, object]:
        return {
            "as_of": self.as_of.isoformat(),
            "daily_panel_hash": self.daily_panel_hash,
            "fundamental_dataset_manifest_hash": (
                self.fundamental_dataset_manifest_hash
            ),
            "minimum_required_members": (
                self.minimum_required_members
            ),
            "session_hashes": [
                value.session_hash for value in self.sessions
            ],
            "spec_hash": self.spec_hash,
            "version": self.version,
        }

    def summary_payload(self) -> dict[str, object]:
        counts = [
            len(value.observations) for value in self.sessions
        ]
        return {
            **self.identity_payload(),
            "eligible_session_count": sum(
                value >= self.minimum_required_members
                for value in counts
            ),
            "first_execution_date": (
                self.sessions[0].execution_date.isoformat()
            ),
            "last_execution_date": (
                self.sessions[-1].execution_date.isoformat()
            ),
            "maximum_eligible_members": max(counts),
            "minimum_eligible_members": min(counts),
            "observation_count": sum(counts),
            "panel_hash": self.panel_hash,
            "session_count": len(self.sessions),
            "sessions": [
                {
                    "active_member_count": (
                        value.active_member_count
                    ),
                    "eligible_member_count": len(
                        value.observations
                    ),
                    "execution_date": (
                        value.execution_date.isoformat()
                    ),
                    "session_hash": value.session_hash,
                    "signal_date": value.signal_date.isoformat(),
                    "snapshot_hash": value.snapshot_hash,
                }
                for value in self.sessions
            ],
        }


class FundamentalPanelCompiler:
    def __init__(
        self,
        *,
        shard_reader: FundamentalShardStream,
    ) -> None:
        self._reader = shard_reader

    async def compile(
        self,
        *,
        spec: FundamentalPortfolioResearchSpec,
        daily_panel: DynamicMarketPanel,
        dataset: FundamentalResearchDatasetManifest,
    ) -> FundamentalResearchPanel:
        if (
            daily_panel.dataset_manifest_hash
            != spec.daily_dataset_manifest_hash
            or daily_panel.plan_hash != spec.plan_hash
            or daily_panel.spec_hash != spec.spec_hash
            or dataset.spec_hash != spec.spec_hash
            or dataset.start_date != spec.start_date
            or dataset.end_date != spec.end_date
            or dataset.instruments
            != tuple(value.instrument for value in daily_panel.histories)
        ):
            raise ValueError(
                "fundamental panel inputs do not match the frozen spec"
            )
        session_inputs = tuple(
            (
                previous.session_date,
                current.session_date,
                current.snapshot_hash,
                frozenset(current.active_members),
            )
            for previous, current in pairwise(daily_panel.sessions)
        )
        observations: dict[
            date, list[FundamentalFeatureObservation]
        ] = {
            execution_date: []
            for _, execution_date, _, _ in session_inputs
        }
        cutoffs = [daily_panel.as_of]
        seen: list[str] = []
        async for shard in self._reader.iter_all():
            seen.append(shard.instrument)
            cutoffs.append(shard.manifest.as_of)
            valuations = {
                value.session_date: value
                for value in shard.valuations
            }
            indicators = tuple(
                sorted(
                    shard.indicators,
                    key=lambda value: (
                        value.available_at,
                        value.report_period,
                        value.announced_date,
                        value.updated,
                        value.content_hash,
                    ),
                )
            )
            for (
                signal_date,
                execution_date,
                _,
                active_members,
            ) in session_inputs:
                if shard.instrument not in active_members:
                    continue
                execution_open = _session_open(execution_date)
                valuation = valuations.get(signal_date)
                indicator = _latest_indicator(
                    indicators,
                    signal_date=signal_date,
                    visible_at=execution_open,
                )
                observation = _observation(
                    spec=spec,
                    instrument=shard.instrument,
                    signal_date=signal_date,
                    execution_date=execution_date,
                    visible_at=execution_open,
                    valuation=valuation,
                    indicator=indicator,
                )
                if observation is not None:
                    observations[execution_date].append(observation)
        if tuple(seen) != dataset.instruments:
            raise ValueError(
                "fundamental panel did not read every frozen shard"
            )
        sessions = tuple(
            FundamentalFeatureSession(
                execution_date=execution_date,
                signal_date=signal_date,
                snapshot_hash=snapshot_hash,
                active_member_count=len(active_members),
                observations=tuple(observations[execution_date]),
            )
            for (
                signal_date,
                execution_date,
                snapshot_hash,
                active_members,
            ) in session_inputs
        )
        return FundamentalResearchPanel(
            spec_hash=spec.spec_hash,
            daily_panel_hash=daily_panel.panel_hash,
            fundamental_dataset_manifest_hash=dataset.manifest_hash,
            as_of=max(cutoffs),
            sessions=sessions,
            minimum_required_members=spec.minimum_eligible_members,
        )


def _latest_indicator(
    indicators: tuple[FinancialIndicatorRevision, ...],
    *,
    signal_date: date,
    visible_at: datetime,
) -> FinancialIndicatorRevision | None:
    candidates = tuple(
        value
        for value in indicators
        if value.report_period <= signal_date
        and value.available_at <= visible_at
    )
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda value: (
            value.report_period,
            value.available_at,
            value.announced_date,
            value.updated,
            value.content_hash,
        ),
    )


def _observation(
    *,
    spec: FundamentalPortfolioResearchSpec,
    instrument: str,
    signal_date: date,
    execution_date: date,
    visible_at: datetime,
    valuation: DailyValuationRevision | None,
    indicator: FinancialIndicatorRevision | None,
) -> FundamentalFeatureObservation | None:
    if (
        valuation is None
        or indicator is None
        or valuation.available_at > visible_at
        or valuation.pe_ttm is None
        or valuation.pe_ttm <= 0
        or valuation.pb is None
        or valuation.pb <= 0
        or indicator.roe_diluted_percent is None
        or indicator.roa_percent is None
        or indicator.operating_cashflow_to_revenue_percent is None
        or indicator.debt_to_assets_percent is None
        or indicator.debt_to_assets_percent
        > spec.maximum_debt_to_assets_percent
        or (signal_date - indicator.report_period).days
        > spec.maximum_financial_age_days
    ):
        return None
    return FundamentalFeatureObservation(
        instrument=instrument,
        signal_date=signal_date,
        execution_date=execution_date,
        report_period=indicator.report_period,
        earnings_yield=Decimal("1") / valuation.pe_ttm,
        book_to_price=Decimal("1") / valuation.pb,
        roe_diluted_percent=indicator.roe_diluted_percent,
        roa_percent=indicator.roa_percent,
        operating_cashflow_to_revenue_percent=(
            indicator.operating_cashflow_to_revenue_percent
        ),
        valuation_hash=valuation.content_hash,
        indicator_hash=indicator.content_hash,
    )


def _session_open(session_date: date) -> datetime:
    return to_utc(
        datetime.combine(
            session_date,
            time(9, 30),
            tzinfo=SHANGHAI,
        )
    )
