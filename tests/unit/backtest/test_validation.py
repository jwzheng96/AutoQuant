from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from autoquant.backtest.models import MarketState
from autoquant.backtest.rules import AshareRuleBook
from autoquant.backtest.validation import (
    SmaParameters,
    WalkForwardConfig,
    WalkForwardValidator,
    _sma_sessions,
)
from autoquant.data.daily_ingestion import ValidatedDailyDataset
from autoquant.data.daily_models import (
    DailyBarRevision,
    DailyCoverageEvidence,
    DailyPriceLimit,
)
from autoquant.data.models import DatasetManifest

INSTRUMENT = "000001.XSHE"
AS_OF = datetime(2027, 1, 1, tzinfo=UTC)


def _market(index: int, close: Decimal, previous: Decimal) -> MarketState:
    session_date = date(2026, 1, 1) + timedelta(days=index)
    event = datetime.combine(session_date, datetime.min.time(), tzinfo=UTC) + timedelta(
        hours=7
    )
    high = max(close, previous) + Decimal("0.2")
    low = min(close, previous) - Decimal("0.2")
    bar = DailyBarRevision.from_values(
        source="tushare",
        instrument=INSTRUMENT,
        session_date=session_date,
        event_time=event,
        available_at=event + timedelta(hours=1),
        ingested_at=AS_OF,
        source_revision="validation-test",
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
        instrument=INSTRUMENT,
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
            INSTRUMENT, session_date, daily_limit
        ),
        suspended=False,
        daily_price_limit=daily_limit,
    )


def _markets(count: int) -> tuple[MarketState, ...]:
    values: list[Decimal] = []
    for index in range(count):
        cycle = index % 30
        values.append(
            Decimal("10")
            + Decimal(index) / Decimal("100")
            + (Decimal(cycle) if cycle < 15 else Decimal(30 - cycle)) / Decimal("10")
        )
    return tuple(
        _market(index, value, Decimal("10") if index == 0 else values[index - 1])
        for index, value in enumerate(values)
    )


def _manifest() -> DatasetManifest:
    return DatasetManifest(
        source="tushare",
        instruments=(INSTRUMENT,),
        start_time=datetime(2026, 1, 1, 7, tzinfo=UTC),
        end_time=datetime(2026, 6, 1, 7, tzinfo=UTC),
        as_of=AS_OF,
        record_hashes=(),
        quality_report_hash="q" * 64,
        production_complete=True,
        row_count=0,
    )


class Control:
    async def read_manifest(self, manifest_hash: str) -> DatasetManifest:
        assert manifest_hash == _manifest().manifest_hash
        return _manifest()


class Reader:
    async def query(self, manifest_hash: str, as_of: datetime) -> ValidatedDailyDataset:
        assert manifest_hash == _manifest().manifest_hash
        assert as_of == AS_OF
        return ValidatedDailyDataset(
            bars=(),
            factors=(),
            coverage=DailyCoverageEvidence((), (), ()),
        )


class Compiler:
    def __init__(self, markets: tuple[MarketState, ...]) -> None:
        self.markets = markets

    def compile(
        self, instrument: str, dataset: ValidatedDailyDataset
    ) -> tuple[MarketState, ...]:
        assert instrument == INSTRUMENT
        assert isinstance(dataset, ValidatedDailyDataset)
        return self.markets


def _config() -> WalkForwardConfig:
    return WalkForwardConfig(
        initial_cash=Decimal("100000"),
        allocation=Decimal("0.8"),
        slippage_bps=Decimal("5"),
        train_sessions=60,
        test_sessions=20,
        embargo_sessions=1,
        candidates=(SmaParameters(2, 5), SmaParameters(3, 8)),
    )


def test_sma_signal_uses_prior_closes_and_never_current_close() -> None:
    closes = [Decimal("10")] * 5 + [Decimal("100"), Decimal("101")]
    markets = tuple(
        _market(index, value, Decimal("10") if index == 0 else closes[index - 1])
        for index, value in enumerate(closes)
    )

    sessions = _sma_sessions(
        markets=markets,
        parameters=SmaParameters(2, 5),
        config=_config(),
        trade_start=markets[5].bar.session_date,
        trade_end=markets[-1].bar.session_date,
    )

    assert sessions[0].orders == ()
    assert sessions[1].orders[0].side.value == "buy"


@pytest.mark.asyncio
async def test_walk_forward_selection_is_deterministic_and_tests_are_disjoint() -> None:
    validator = WalkForwardValidator(
        control_repository=Control(),
        dataset_reader=Reader(),
        compiler=Compiler(_markets(101)),
    )

    first = await validator.run(
        manifest_hash=_manifest().manifest_hash,
        instrument=INSTRUMENT,
        config=_config(),
    )
    second = await validator.run(
        manifest_hash=_manifest().manifest_hash,
        instrument=INSTRUMENT,
        config=_config(),
    )

    assert len(first.folds) == 2
    assert first.result_hash == second.result_hash
    assert first.folds[0].train_end < first.folds[0].test_start
    assert first.folds[0].test_end < first.folds[1].test_start
    assert first.folds[0].selected in _config().candidates
    assert first.folds[0].benchmark_result is not None
    assert first.benchmark_compounded_oos_return is not None
    assert first.excess_oos_return == (
        first.compounded_oos_return - first.benchmark_compounded_oos_return
    )


@pytest.mark.asyncio
async def test_walk_forward_requires_full_train_embargo_and_test_windows() -> None:
    validator = WalkForwardValidator(
        control_repository=Control(),
        dataset_reader=Reader(),
        compiler=Compiler(_markets(80)),
    )

    with pytest.raises(ValueError, match="at least 81"):
        await validator.run(
            manifest_hash=_manifest().manifest_hash,
            instrument=INSTRUMENT,
            config=_config(),
        )
