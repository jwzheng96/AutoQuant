from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from autoquant.backtest.models import ExecutionState
from autoquant.backtest.runner import ManifestBacktestRunner
from autoquant.data.daily_ingestion import ValidatedDailyDataset
from autoquant.data.daily_models import (
    AdjustmentFactorRevision,
    DailyBarRevision,
    DailyCoverageEvidence,
    DailyPriceLimit,
    DailySuspensionStatus,
    InstrumentLifecycle,
    TradingSession,
)
from autoquant.data.models import DatasetManifest
from autoquant.web.models import BacktestRunRequest

INSTRUMENT = "000001.XSHE"
FIRST = date(2026, 7, 20)
SECOND = date(2026, 7, 21)
AS_OF = datetime(2026, 7, 22, tzinfo=UTC)


def _bar(session_date: date, *, open_price: str, pre_close: str) -> DailyBarRevision:
    event = datetime.combine(session_date, datetime.min.time(), tzinfo=UTC) + timedelta(
        hours=7
    )
    return DailyBarRevision.from_values(
        source="tushare",
        instrument=INSTRUMENT,
        session_date=session_date,
        event_time=event,
        available_at=event + timedelta(hours=1),
        ingested_at=AS_OF,
        source_revision="runner-test",
        availability_policy="test-v1",
        evidence_hash="a" * 64,
        open_price=open_price,
        high_price=str(Decimal(open_price) + Decimal("0.3")),
        low_price=str(Decimal(open_price) - Decimal("0.3")),
        close_price=open_price,
        pre_close=pre_close,
        volume=1_000_000,
        turnover="10000000",
    )


def _factor(session_date: date, value: str = "1") -> AdjustmentFactorRevision:
    event = datetime.combine(session_date, datetime.min.time(), tzinfo=UTC) + timedelta(
        hours=7
    )
    return AdjustmentFactorRevision.from_values(
        source="tushare",
        instrument=INSTRUMENT,
        session_date=session_date,
        event_time=event,
        available_at=event + timedelta(hours=1),
        ingested_at=AS_OF,
        source_revision="runner-test",
        availability_policy="test-v1",
        evidence_hash="b" * 64,
        factor=value,
    )


def _limit(session_date: date, pre_close: str) -> DailyPriceLimit:
    reference = Decimal(pre_close)
    return DailyPriceLimit(
        source="tushare",
        instrument=INSTRUMENT,
        session_date=session_date,
        pre_close=reference,
        up_limit=(reference * Decimal("1.1")).quantize(Decimal("0.01")),
        down_limit=(reference * Decimal("0.9")).quantize(Decimal("0.01")),
        available_at=AS_OF,
        response_hash="c" * 64,
    )


def _dataset(*, second_factor: str = "1") -> ValidatedDailyDataset:
    bars = (
        _bar(FIRST, open_price="10.5", pre_close="10"),
        _bar(SECOND, open_price="11", pre_close="10.5"),
    )
    return ValidatedDailyDataset(
        bars=bars,
        factors=(_factor(FIRST), _factor(SECOND, second_factor)),
        coverage=DailyCoverageEvidence(
            sessions=(
                TradingSession("tushare", FIRST, True, AS_OF, "d" * 64),
                TradingSession("tushare", SECOND, True, AS_OF, "d" * 64),
            ),
            lifecycles=(
                InstrumentLifecycle(
                    "tushare", INSTRUMENT, date(1991, 4, 3), None, AS_OF, "e" * 64
                ),
            ),
            suspensions=(
                DailySuspensionStatus(
                    "tushare", INSTRUMENT, FIRST, False, AS_OF, "f" * 64
                ),
                DailySuspensionStatus(
                    "tushare", INSTRUMENT, SECOND, False, AS_OF, "f" * 64
                ),
            ),
            price_limits=(_limit(FIRST, "10"), _limit(SECOND, "10.5")),
        ),
    )


def _manifest() -> DatasetManifest:
    return DatasetManifest(
        source="tushare",
        instruments=(INSTRUMENT,),
        start_time=datetime(2026, 7, 19, 16, tzinfo=UTC),
        end_time=datetime(2026, 7, 21, 7, tzinfo=UTC),
        as_of=AS_OF,
        record_hashes=(),
        quality_report_hash="q" * 64,
        production_complete=True,
        row_count=0,
    )


class Control:
    async def read_manifest(self, manifest_hash: str) -> DatasetManifest:
        manifest = _manifest()
        assert manifest_hash == manifest.manifest_hash
        return manifest


class Reader:
    def __init__(self, dataset: ValidatedDailyDataset) -> None:
        self.dataset = dataset

    async def query(self, manifest_hash: str, as_of: datetime) -> ValidatedDailyDataset:
        assert manifest_hash == _manifest().manifest_hash
        assert as_of == AS_OF
        return self.dataset


def _request() -> BacktestRunRequest:
    return BacktestRunRequest(
        manifest_hash=_manifest().manifest_hash,
        instrument=INSTRUMENT,
        initial_cash=Decimal("100000"),
        allocation=Decimal("0.50"),
        slippage_bps=Decimal("5"),
        liquidate_at_end=True,
        idempotency_key="runner-backtest-request-0001",
    )


@pytest.mark.asyncio
async def test_runner_uses_previous_close_for_quantity_and_exact_manifest_cutoff() -> None:
    runner = ManifestBacktestRunner(
        control_repository=Control(), dataset_reader=Reader(_dataset())
    )

    result = await runner.run(_request())

    assert result.strategy_id == "manifest_buy_hold_v1"
    assert result.as_of == AS_OF
    assert result.reports[0].requested_quantity == 4900
    assert result.reports[0].state is ExecutionState.FILLED
    assert result.reports[1].state is ExecutionState.FILLED
    assert result.events[-1].event_hash == result.ledger_hash


@pytest.mark.asyncio
async def test_runner_fails_closed_when_corporate_action_factor_changes() -> None:
    runner = ManifestBacktestRunner(
        control_repository=Control(),
        dataset_reader=Reader(_dataset(second_factor="1.1")),
    )

    with pytest.raises(ValueError, match="corporate-action accounting"):
        await runner.run(_request())
