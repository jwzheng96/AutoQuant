from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from autoquant.backtest.models import InstrumentRules
from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
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
from autoquant.execution.validated_sma import ValidatedSmaRegistration
from autoquant.execution.validated_sma_portfolio import (
    ValidatedSmaPortfolioRegistration,
    ValidatedSmaPortfolioTargetProvider,
)
from autoquant.risk.models import MarketQuote, RiskAccountState, RiskPolicy

NOW = datetime(2026, 7, 23, 1, 30, tzinfo=UTC)
AS_OF = datetime(2026, 7, 22, 8, tzinfo=UTC)
APPROVED_AT = datetime(2026, 7, 22, 9, tzinfo=UTC)
INSTRUMENTS = ("000001.XSHE", "000002.XSHE", "600000.XSHG")
STRATEGY_ID = "validated-sma-portfolio"


def _policy() -> RiskPolicy:
    return RiskPolicy(
        allowed_instruments=INSTRUMENTS,
        max_position_weight=Decimal("0.20"),
        max_gross_exposure=Decimal("0.60"),
    )


def _rules(instrument: str) -> InstrumentRules:
    return AshareRuleBook().resolve(
        instrument,
        NOW.date(),
        SecurityStatus(risk_warning=False, listing_session_number=1000),
    )


def _manifest(instrument: str) -> DatasetManifest:
    marker = INSTRUMENTS.index(instrument) + 1
    return DatasetManifest(
        source="tushare",
        instruments=(instrument,),
        start_time=datetime(2026, 7, 3, tzinfo=UTC),
        end_time=datetime(2026, 7, 22, 7, tzinfo=UTC),
        as_of=AS_OF,
        record_hashes=(f"{marker:064x}",),
        quality_report_hash=f"{marker + 10:064x}",
        production_complete=True,
        row_count=1,
    )


def _valuation_manifest() -> DatasetManifest:
    return DatasetManifest(
        source="tushare",
        instruments=INSTRUMENTS,
        start_time=datetime(2026, 7, 3, tzinfo=UTC),
        end_time=datetime(2026, 7, 22, 7, tzinfo=UTC),
        as_of=AS_OF,
        record_hashes=("d" * 64, "e" * 64, "f" * 64),
        quality_report_hash="e" * 64,
        production_complete=True,
        row_count=3,
    )


def _dataset(
    instrument: str,
    *,
    descending: bool = False,
) -> ValidatedDailyDataset:
    bars = []
    factors = []
    for index in range(20):
        session_date = date(2026, 7, 3) + timedelta(days=index)
        event_time = datetime.combine(
            session_date,
            datetime.min.time(),
            tzinfo=UTC,
        ) + timedelta(hours=7)
        price = (
            Decimal("30") - index
            if descending
            else Decimal("10") + index
        )
        bars.append(
            DailyBarRevision.from_values(
                source="tushare",
                instrument=instrument,
                session_date=session_date,
                event_time=event_time,
                available_at=event_time + timedelta(hours=1),
                ingested_at=AS_OF,
                source_revision=f"daily-{instrument}-{index}",
                availability_policy="test-v1",
                evidence_hash="a" * 64,
                open_price=str(price),
                high_price=str(price + Decimal("0.2")),
                low_price=str(price - Decimal("0.2")),
                close_price=str(price + Decimal("0.1")),
                pre_close=str(price),
                volume=100_000,
                turnover="1000000",
            )
        )
        factors.append(
            AdjustmentFactorRevision.from_values(
                source="tushare",
                instrument=instrument,
                session_date=session_date,
                event_time=event_time,
                available_at=event_time + timedelta(hours=1),
                ingested_at=AS_OF,
                source_revision=f"factor-{instrument}-{index}",
                availability_policy="test-v1",
                evidence_hash="b" * 64,
                factor="1",
            )
        )
    return ValidatedDailyDataset(
        bars=tuple(bars),
        factors=tuple(factors),
        coverage=DailyCoverageEvidence((), (), (), ()),
    )


def _portfolio() -> ValidatedSmaPortfolioRegistration:
    policy = _policy()
    components = tuple(
        ValidatedSmaRegistration(
            account_id="paper-main",
            strategy_id=STRATEGY_ID,
            strategy_version=f"sma-component-v1:{instrument}:5-20",
            experiment_id=uuid4(),
            validation_result_hash=f"{index + 20:064x}",
            validation_manifest_hash=f"{index + 30:064x}",
            signal_manifest_hash=_manifest(instrument).manifest_hash,
            signal_manifest_as_of=AS_OF,
            instrument=instrument,
            fast_sessions=5,
            slow_sessions=20,
            allocation=Decimal("0.15"),
            slippage_bps=Decimal("5"),
            risk_policy_hash=policy.policy_hash,
            rule_version=_rules(instrument).rule_version,
            approved_by="operator",
            approved_at=APPROVED_AT,
        )
        for index, instrument in enumerate(INSTRUMENTS)
    )
    valuation = _valuation_manifest()
    return ValidatedSmaPortfolioRegistration(
        account_id="paper-main",
        strategy_id=STRATEGY_ID,
        strategy_version="sma-portfolio-v1:test",
        components=components,
        valuation_manifest_hash=valuation.manifest_hash,
        valuation_manifest_as_of=valuation.as_of,
        risk_policy_hash=policy.policy_hash,
        approved_by="operator",
        approved_at=APPROVED_AT,
    )


def _context() -> PaperStrategyContext:
    quotes = tuple(
        MarketQuote(
            instrument=instrument,
            as_of=NOW,
            last_price=Decimal("10"),
            bid_price=Decimal("9.99"),
            ask_price=Decimal("10.01"),
            market_open=True,
        )
        for instrument in INSTRUMENTS
    )
    book = ContinuousQuoteBook(source="qmt")
    book.reset(
        quotes=quotes,
        source_sequence=1,
        received_at=NOW,
        reset_id="portfolio-test",
    )
    account = RiskAccountState(
        account_id="paper-main",
        as_of=NOW,
        cash=Decimal("1000000"),
        equity=Decimal("1000000"),
        day_start_equity=Decimal("1000000"),
        peak_equity=Decimal("1000000"),
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
            instruments=INSTRUMENTS,
            now=NOW,
            max_age=timedelta(seconds=3),
            require_market_open=True,
        ),
        account_evidence=evidence,
    )


def _provider(
    *,
    suspended: tuple[str, ...] = (),
) -> ValidatedSmaPortfolioTargetProvider:
    registration = _portfolio()
    registry = MagicMock()
    registry.active = AsyncMock(return_value=registration)
    manifests = {
        value.signal_manifest_hash: _manifest(value.instrument)
        for value in registration.components
    }
    controls = MagicMock()
    controls.read_manifest = AsyncMock(
        side_effect=lambda manifest_hash: manifests[manifest_hash]
    )
    datasets = {
        value.signal_manifest_hash: _dataset(value.instrument)
        for value in registration.components
    }
    reader = MagicMock()
    reader.query = AsyncMock(
        side_effect=lambda manifest_hash, _as_of: datasets[manifest_hash]
    )
    session_rules = MagicMock()
    session_rules.read = AsyncMock(
        return_value=SessionRuleSet(
            session_date=NOW.date(),
            as_of=NOW,
            rules=tuple(_rules(value) for value in INSTRUMENTS),
            suspended_instruments=suspended,
            source_evidence_hashes=("7" * 64,),
        )
    )
    return ValidatedSmaPortfolioTargetProvider(
        strategy_id=STRATEGY_ID,
        registry=registry,
        control_repository=controls,
        dataset_reader=reader,
        session_rule_reader=session_rules,
        policy=_policy(),
    )


def test_portfolio_registration_requires_diversification_and_shared_controls() -> None:
    registration = _portfolio()

    assert registration.instruments == tuple(sorted(INSTRUMENTS))
    assert registration.total_allocation == Decimal("0.45")
    assert len(registration.registration_hash) == 64

    with pytest.raises(ValueError, match="3-20"):
        replace(registration, components=registration.components[:2])
    with pytest.raises(ValueError, match="share deployment"):
        replace(
            registration,
            components=(
                replace(
                    registration.components[0],
                    approved_by="another-operator",
                ),
                *registration.components[1:],
            ),
        )


@pytest.mark.asyncio
async def test_portfolio_provider_emits_one_bounded_target_per_component() -> None:
    signal = await _provider().target(_context())

    assert tuple(value.instrument for value in signal.targets) == tuple(
        sorted(INSTRUMENTS)
    )
    assert all(value.target_quantity > 0 for value in signal.targets)
    assert all(value.policy.policy_hash == _policy().policy_hash for value in signal.targets)
    assert len(signal.source_evidence_hash) == 64


@pytest.mark.asyncio
async def test_suspended_component_holds_current_quantity_without_blocking_portfolio() -> None:
    suspended = INSTRUMENTS[0]
    signal = await _provider(suspended=(suspended,)).target(_context())
    targets = {value.instrument: value for value in signal.targets}

    assert targets[suspended].target_quantity == 0
    assert all(
        value.target_quantity > 0
        for instrument, value in targets.items()
        if instrument != suspended
    )


@pytest.mark.asyncio
async def test_portfolio_provider_fails_closed_on_component_manifest_drift() -> None:
    provider = _provider()
    cast(Any, provider._control).read_manifest = AsyncMock(
        return_value=replace(
            _manifest(INSTRUMENTS[0]),
            production_complete=False,
        )
    )

    with pytest.raises(ValueError, match="does not match"):
        await provider.target(_context())
