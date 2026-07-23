from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.backtest.validation import SmaParameters
from autoquant.data.daily_ingestion import ValidatedDailyDataset
from autoquant.data.daily_models import (
    AdjustmentFactorRevision,
    DailyBarRevision,
    DailyCoverageEvidence,
)
from autoquant.data.models import DatasetManifest
from autoquant.execution.market_clock import AShareTradingPhase
from autoquant.execution.paper_scheduler import PaperStrategyContext
from autoquant.execution.quote_book import ContinuousQuoteBook
from autoquant.execution.session_rules import SessionRuleSet
from autoquant.execution.strategy_account import PaperStrategyAccountEvidence
from autoquant.execution.validated_sma import (
    ValidatedSmaRegistration,
    ValidatedSmaTargetProvider,
    select_deployment_parameters,
)
from autoquant.risk.models import MarketQuote, RiskAccountState, RiskPolicy

NOW = datetime(2026, 7, 23, 1, 30, tzinfo=UTC)
MANIFEST_AS_OF = datetime(2026, 7, 22, 8, tzinfo=UTC)
INSTRUMENT = "600000.XSHG"
STRATEGY_ID = "validated-sma-paper"


def _rules():
    return AshareRuleBook().resolve(
        INSTRUMENT,
        NOW.date(),
        SecurityStatus(risk_warning=False, listing_session_number=1000),
    )


def _policy() -> RiskPolicy:
    return RiskPolicy(
        allowed_instruments=(INSTRUMENT,),
        max_position_weight=Decimal("0.20"),
        max_gross_exposure=Decimal("0.20"),
    )


def _manifest() -> DatasetManifest:
    return DatasetManifest(
        source="tushare",
        instruments=(INSTRUMENT,),
        start_time=datetime(2026, 7, 3, tzinfo=UTC),
        end_time=datetime(2026, 7, 22, 7, tzinfo=UTC),
        as_of=MANIFEST_AS_OF,
        record_hashes=("a" * 64,),
        quality_report_hash="b" * 64,
        production_complete=True,
        row_count=1,
    )


def _dataset(*, descending: bool = False) -> ValidatedDailyDataset:
    bars = []
    factors = []
    for index in range(20):
        session_date = date(2026, 7, 3) + timedelta(days=index)
        event_time = datetime.combine(
            session_date,
            datetime.min.time(),
            tzinfo=UTC,
        ) + timedelta(hours=7)
        raw_price = Decimal("30") - index if descending else Decimal("10") + index
        bars.append(
            DailyBarRevision.from_values(
                source="tushare",
                instrument=INSTRUMENT,
                session_date=session_date,
                event_time=event_time,
                available_at=event_time + timedelta(hours=1),
                ingested_at=MANIFEST_AS_OF,
                source_revision=f"daily-{index}",
                availability_policy="test-v1",
                evidence_hash="c" * 64,
                open_price=str(raw_price),
                high_price=str(raw_price + Decimal("0.2")),
                low_price=str(raw_price - Decimal("0.2")),
                close_price=str(raw_price + Decimal("0.1")),
                pre_close=str(raw_price),
                volume=100_000,
                turnover="1000000",
            )
        )
        factors.append(
            AdjustmentFactorRevision.from_values(
                source="tushare",
                instrument=INSTRUMENT,
                session_date=session_date,
                event_time=event_time,
                available_at=event_time + timedelta(hours=1),
                ingested_at=MANIFEST_AS_OF,
                source_revision=f"factor-{index}",
                availability_policy="test-v1",
                evidence_hash="d" * 64,
                factor="1",
            )
        )
    return ValidatedDailyDataset(
        bars=tuple(bars),
        factors=tuple(factors),
        coverage=DailyCoverageEvidence((), (), (), ()),
    )


def _context() -> PaperStrategyContext:
    quote = MarketQuote(
        instrument=INSTRUMENT,
        as_of=NOW,
        last_price=Decimal("10"),
        bid_price=Decimal("9.99"),
        ask_price=Decimal("10.01"),
        market_open=True,
    )
    book = ContinuousQuoteBook(source="qmt")
    book.reset(
        quotes=(quote,),
        source_sequence=1,
        received_at=NOW,
        reset_id="validated-sma-test",
    )
    account = RiskAccountState(
        account_id="paper-main",
        as_of=NOW,
        cash=Decimal("100000"),
        equity=Decimal("100000"),
        day_start_equity=Decimal("100000"),
        peak_equity=Decimal("100000"),
        gross_exposure=Decimal("0"),
        daily_turnover=Decimal("0"),
        open_order_count=0,
        reconciled=True,
        kill_switch=False,
    )
    evidence = PaperStrategyAccountEvidence(
        session_date=NOW.date(),
        account=account,
        reconciliation_hash="1" * 64,
        internal_snapshot_hash="2" * 64,
        broker_snapshot_hash="3" * 64,
        session_state_hash="4" * 64,
    )
    return PaperStrategyContext(
        account_id="paper-main",
        session_date=NOW.date(),
        now=NOW,
        phase=AShareTradingPhase.MORNING_CONTINUOUS,
        quote_snapshot=book.snapshot(
            instruments=(INSTRUMENT,),
            now=NOW,
            max_age=timedelta(seconds=3),
            require_market_open=True,
        ),
        account_evidence=evidence,
    )


def _registration(
    *,
    manifest: DatasetManifest,
    policy: RiskPolicy,
) -> ValidatedSmaRegistration:
    return ValidatedSmaRegistration(
        account_id="paper-main",
        strategy_id=STRATEGY_ID,
        strategy_version="sma-paper-v1:result:5-20",
        experiment_id=uuid4(),
        validation_result_hash="5" * 64,
        validation_manifest_hash="6" * 64,
        signal_manifest_hash=manifest.manifest_hash,
        signal_manifest_as_of=manifest.as_of,
        instrument=INSTRUMENT,
        fast_sessions=5,
        slow_sessions=20,
        allocation=Decimal("0.20"),
        slippage_bps=Decimal("5"),
        risk_policy_hash=policy.policy_hash,
        rule_version=_rules().rule_version,
        approved_by="operator",
        approved_at=datetime(2026, 7, 22, 9, tzinfo=UTC),
    )


def _provider(
    *,
    dataset: ValidatedDailyDataset,
) -> tuple[ValidatedSmaTargetProvider, ValidatedSmaRegistration]:
    manifest = _manifest()
    policy = _policy()
    registration = _registration(manifest=manifest, policy=policy)
    registry = MagicMock()
    registry.active = AsyncMock(return_value=registration)
    controls = MagicMock()
    controls.read_manifest = AsyncMock(return_value=manifest)
    reader = MagicMock()
    reader.query = AsyncMock(return_value=dataset)
    session_rules = MagicMock()
    session_rules.read = AsyncMock(
        return_value=SessionRuleSet(
            session_date=NOW.date(),
            as_of=NOW,
            rules=(_rules(),),
            suspended_instruments=(),
            source_evidence_hashes=("7" * 64,),
        )
    )
    return (
        ValidatedSmaTargetProvider(
            strategy_id=STRATEGY_ID,
            registry=registry,
            control_repository=controls,
            dataset_reader=reader,
            session_rule_reader=session_rules,
            policy=policy,
        ),
        registration,
    )


def test_deployment_parameter_selection_uses_mode_with_deterministic_tie_break() -> None:
    selected = select_deployment_parameters(
        (
            SmaParameters(10, 30),
            SmaParameters(5, 20),
            SmaParameters(10, 30),
            SmaParameters(5, 20),
        )
    )

    assert selected == SmaParameters(5, 20)


def test_registration_is_strictly_paper_only() -> None:
    manifest = _manifest()
    policy = _policy()
    registration = _registration(manifest=manifest, policy=policy)

    with pytest.raises(ValueError, match="paper-only"):
        replace(registration, execution_mode="live")


@pytest.mark.asyncio
async def test_provider_builds_bullish_target_from_exact_manifest_evidence() -> None:
    provider, registration = _provider(dataset=_dataset())

    signal = await provider.target(_context())

    assert signal.strategy_version == registration.strategy_version
    assert signal.targets[0].target_quantity > 0
    assert signal.targets[0].target_quantity <= _rules().max_order_quantity
    assert len(signal.source_evidence_hash) == 64


@pytest.mark.asyncio
async def test_provider_builds_zero_target_for_bearish_history() -> None:
    provider, _ = _provider(dataset=_dataset(descending=True))

    signal = await provider.target(_context())

    assert signal.targets[0].target_quantity == 0


@pytest.mark.asyncio
async def test_provider_rejects_unregistered_or_stale_signal_data() -> None:
    provider, _ = _provider(dataset=_dataset())
    provider._registry.active = AsyncMock(return_value=None)

    with pytest.raises(ValueError, match="no active"):
        await provider.target(_context())

    stale = _dataset()
    stale_provider, _ = _provider(
        dataset=ValidatedDailyDataset(
            bars=tuple(
                value
                for value in stale.bars
                if value.session_date <= date(2026, 7, 18)
            ),
            factors=tuple(
                value
                for value in stale.factors
                if value.session_date <= date(2026, 7, 18)
            ),
            coverage=stale.coverage,
        )
    )
    with pytest.raises(ValueError, match=r"valid SMA history|stale"):
        await stale_provider.target(_context())


@pytest.mark.asyncio
async def test_provider_rejects_adjustment_factor_change() -> None:
    dataset = _dataset()
    changed = (
        *dataset.factors[:-1],
        AdjustmentFactorRevision.from_values(
            source="tushare",
            instrument=INSTRUMENT,
            session_date=dataset.factors[-1].session_date,
            event_time=dataset.factors[-1].event_time,
            available_at=dataset.factors[-1].available_at,
            ingested_at=dataset.factors[-1].ingested_at,
            source_revision="factor-change",
            availability_policy="test-v1",
            evidence_hash="d" * 64,
            factor="2",
        ),
    )
    provider, _ = _provider(
        dataset=ValidatedDailyDataset(
            bars=dataset.bars,
            factors=changed,
            coverage=dataset.coverage,
        )
    )

    with pytest.raises(ValueError, match="adjustment factors"):
        await provider.target(_context())


@pytest.mark.asyncio
async def test_provider_rejects_current_session_suspension() -> None:
    provider, _ = _provider(dataset=_dataset())
    provider._session_rules.read = AsyncMock(
        return_value=SessionRuleSet(
            session_date=NOW.date(),
            as_of=NOW,
            rules=(_rules(),),
            suspended_instruments=(INSTRUMENT,),
            source_evidence_hashes=("7" * 64,),
        )
    )

    with pytest.raises(ValueError, match="do not permit"):
        await provider.target(_context())
