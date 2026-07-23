"""Tests for the cross-sectional portfolio validation engine."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from autoquant.backtest.models import MarketState, OrderSide
from autoquant.backtest.portfolio_diagnostics import (
    diagnose_portfolio_validation,
)
from autoquant.backtest.portfolio_validation import (
    CrossSectionalMomentumParameters,
    PortfolioWalkForwardConfig,
    PortfolioWalkForwardValidator,
    _run_cross_sectional,
    assess_portfolio_validation,
)
from autoquant.backtest.rules import AshareRuleBook
from autoquant.data.daily_ingestion import ValidatedDailyDataset
from autoquant.data.daily_models import (
    DailyBarRevision,
    DailyCoverageEvidence,
    DailyPriceLimit,
)
from autoquant.data.models import DatasetManifest

INSTRUMENTS = (
    "000333.XSHE",
    "600276.XSHG",
    "601899.XSHG",
)
AS_OF = datetime(2027, 1, 1, tzinfo=UTC)


def _market(
    *,
    instrument: str,
    index: int,
    close: Decimal,
    previous: Decimal,
) -> MarketState:
    session_date = date(2026, 1, 1) + timedelta(days=index)
    event_time = datetime.combine(
        session_date,
        datetime.min.time(),
        tzinfo=UTC,
    ) + timedelta(hours=7)
    high = max(close, previous) + Decimal("0.5")
    low = min(close, previous) - Decimal("0.5")
    bar = DailyBarRevision.from_values(
        source="tushare",
        instrument=instrument,
        session_date=session_date,
        event_time=event_time,
        available_at=event_time + timedelta(hours=1),
        ingested_at=AS_OF,
        source_revision="portfolio-validation-test",
        availability_policy="test-v1",
        evidence_hash="a" * 64,
        open_price=str(previous),
        high_price=str(high),
        low_price=str(low),
        close_price=str(close),
        pre_close=str(previous),
        volume=100_000_000,
        turnover="1000000000",
    )
    daily_limit = DailyPriceLimit(
        source="tushare",
        instrument=instrument,
        session_date=session_date,
        pre_close=previous,
        up_limit=previous * Decimal("1.5"),
        down_limit=previous * Decimal("0.5"),
        available_at=AS_OF,
        response_hash="b" * 64,
    )
    return MarketState(
        bar=bar,
        rules=AshareRuleBook().resolve_with_price_limit(
            instrument,
            session_date,
            daily_limit,
        ),
        suspended=False,
        daily_price_limit=daily_limit,
    )


def _markets(count: int) -> dict[str, tuple[MarketState, ...]]:
    result: dict[str, tuple[MarketState, ...]] = {}
    for instrument_index, instrument in enumerate(INSTRUMENTS):
        closes = tuple(
            Decimal("10")
            + Decimal(index) * Decimal(
                ("0.04", "0.025", "0.01")[instrument_index]
            )
            + Decimal(index % (9 + instrument_index))
            * Decimal("0.02")
            for index in range(count)
        )
        result[instrument] = tuple(
            _market(
                instrument=instrument,
                index=index,
                close=value,
                previous=(
                    Decimal("10")
                    if index == 0
                    else closes[index - 1]
                ),
            )
            for index, value in enumerate(closes)
        )
    return result


def _sessions(
    values: dict[str, tuple[MarketState, ...]],
) -> tuple[tuple[MarketState, ...], ...]:
    return tuple(
        tuple(values[instrument][index] for instrument in INSTRUMENTS)
        for index in range(len(values[INSTRUMENTS[0]]))
    )


class Control:
    def __init__(self, manifest: DatasetManifest) -> None:
        self.manifest = manifest

    async def read_manifest(self, manifest_hash: str) -> DatasetManifest:
        assert manifest_hash == self.manifest.manifest_hash
        return self.manifest


class Reader:
    async def query(
        self,
        manifest_hash: str,
        as_of: datetime,
    ) -> ValidatedDailyDataset:
        assert as_of == AS_OF
        return ValidatedDailyDataset(
            bars=(),
            factors=(),
            coverage=DailyCoverageEvidence((), (), ()),
        )


class Compiler:
    def __init__(
        self,
        values: dict[str, tuple[MarketState, ...]],
    ) -> None:
        self.values = values

    def compile(
        self,
        instrument: str,
        dataset: ValidatedDailyDataset,
    ) -> tuple[MarketState, ...]:
        assert isinstance(dataset, ValidatedDailyDataset)
        return self.values[instrument]


def _manifest() -> DatasetManifest:
    return DatasetManifest(
        source="tushare",
        instruments=INSTRUMENTS,
        start_time=datetime(2026, 1, 1, 7, tzinfo=UTC),
        end_time=datetime(2026, 12, 31, 7, tzinfo=UTC),
        as_of=AS_OF,
        record_hashes=(),
        quality_report_hash="c" * 64,
        production_complete=True,
        row_count=0,
    )


def test_cross_sectional_signal_never_uses_current_session_close() -> None:
    values = _markets(25)
    sessions = _sessions(values)
    parameters = CrossSectionalMomentumParameters(20, 5, 2)
    config = PortfolioWalkForwardConfig(
        initial_cash=Decimal("1000000"),
        gross_allocation=Decimal("0.20"),
        maximum_order_notional=Decimal("101000"),
        train_sessions=126,
        test_sessions=20,
        candidates=(parameters,),
    )

    original = _run_cross_sectional(
        manifest_hash="d" * 64,
        as_of=AS_OF,
        sessions=sessions,
        parameters=parameters,
        config=config,
        trade_start=sessions[0][0].bar.session_date,
        trade_end=sessions[-1][0].bar.session_date,
    )
    changed_values = dict(values)
    changed_instrument = INSTRUMENTS[-1]
    changed = list(changed_values[changed_instrument])
    current = changed[21]
    changed[21] = _market(
        instrument=changed_instrument,
        index=21,
        close=Decimal("1000"),
        previous=current.bar.pre_close,
    )
    changed_values[changed_instrument] = tuple(changed)
    changed_result = _run_cross_sectional(
        manifest_hash="d" * 64,
        as_of=AS_OF,
        sessions=_sessions(changed_values),
        parameters=parameters,
        config=config,
        trade_start=sessions[0][0].bar.session_date,
        trade_end=sessions[-1][0].bar.session_date,
    )

    original_entries = tuple(
        report.instrument
        for report in original.reports
        if report.session_date == sessions[21][0].bar.session_date
        and report.side is OrderSide.BUY
    )
    changed_entries = tuple(
        report.instrument
        for report in changed_result.reports
        if report.session_date == sessions[21][0].bar.session_date
        and report.side is OrderSide.BUY
    )
    assert original_entries == changed_entries
    assert original_entries == INSTRUMENTS[:2]
    assert all(
        report.session_date >= sessions[21][0].bar.session_date
        for report in original.reports
    )


@pytest.mark.asyncio
async def test_portfolio_walk_forward_is_deterministic_and_disjoint() -> None:
    manifest = _manifest()
    values = _markets(190)
    config = PortfolioWalkForwardConfig(
        train_sessions=126,
        test_sessions=20,
        embargo_sessions=1,
        candidates=(
            CrossSectionalMomentumParameters(20, 5, 3),
            CrossSectionalMomentumParameters(60, 10, 3),
        ),
    )
    validator = PortfolioWalkForwardValidator(
        control_repository=Control(manifest),
        dataset_reader=Reader(),
        compiler=Compiler(values),
    )

    first = await validator.run(
        manifest_hash=manifest.manifest_hash,
        config=config,
    )
    second = await validator.run(
        manifest_hash=manifest.manifest_hash,
        config=config,
    )

    assert first.result_hash == second.result_hash
    assert len(first.folds) == 3
    assert all(
        left.test_end < right.test_start
        for left, right in zip(
            first.folds,
            first.folds[1:],
            strict=False,
        )
    )
    assert all(
        fold.test_result.snapshots[0].session_date
        == fold.test_start
        and fold.benchmark_result.snapshots[-1].session_date
        == fold.test_end
        for fold in first.folds
    )
    evidence = assess_portfolio_validation(first)
    repeated_evidence = assess_portfolio_validation(second)
    diagnostics = diagnose_portfolio_validation(first)
    repeated_diagnostics = diagnose_portfolio_validation(second)
    assert evidence.assessment_hash == repeated_evidence.assessment_hash
    assert evidence.fold_count == 3
    assert evidence.oos_sessions == 60
    assert "minimum_fold_count" in evidence.gate_failures
    assert (
        diagnostics.diagnostic_hash
        == repeated_diagnostics.diagnostic_hash
    )
    assert diagnostics.result_hash == first.result_hash
    assert diagnostics.fold_count == len(first.folds)
    assert sum(
        value.count for value in diagnostics.selection_frequencies
    ) == len(first.folds)
    assert (
        diagnostics.assessment_gate_failures
        == evidence.gate_failures
    )
    assert "small_universe" in diagnostics.diagnostic_codes
