from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from autoquant.backtest.models import OrderSide
from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.execution.market_clock import AShareTradingPhase
from autoquant.execution.paper_scheduler import PaperStrategyContext
from autoquant.execution.quote_book import ContinuousQuoteBook
from autoquant.execution.strategy_account import PaperStrategyAccountEvidence
from autoquant.execution.target_strategy import (
    TargetInstrumentPosition,
    TargetPortfolioSignal,
    TargetPositionPaperIntentSource,
)
from autoquant.risk.models import (
    MarketQuote,
    RiskAccountState,
    RiskPolicy,
    RiskPosition,
)

NOW = datetime(2026, 7, 23, 1, 30, tzinfo=UTC)
SESSION_DATE = date(2026, 7, 23)
INSTRUMENT = "600000.XSHG"


def _rules():
    return AshareRuleBook().resolve(
        INSTRUMENT,
        SESSION_DATE,
        SecurityStatus(risk_warning=False, listing_session_number=1000),
    )


def _context(
    *,
    quantity: int = 0,
    sellable: int = 0,
) -> PaperStrategyContext:
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
        reset_id="target-strategy-test",
    )
    market_value = Decimal(quantity) * Decimal("10")
    positions = (
        ()
        if quantity == 0
        else (
            RiskPosition(
                instrument=INSTRUMENT,
                total_quantity=quantity,
                sellable_quantity=sellable,
                market_value=market_value,
            ),
        )
    )
    account = RiskAccountState(
        account_id="paper-main",
        as_of=NOW,
        cash=Decimal("1000000") - market_value,
        equity=Decimal("1000000"),
        day_start_equity=Decimal("1000000"),
        peak_equity=Decimal("1000000"),
        gross_exposure=market_value,
        daily_turnover=Decimal("0"),
        open_order_count=0,
        reconciled=True,
        kill_switch=False,
        positions=positions,
    )
    evidence = PaperStrategyAccountEvidence(
        session_date=SESSION_DATE,
        account=account,
        reconciliation_hash="a" * 64,
        internal_snapshot_hash="b" * 64,
        broker_snapshot_hash="c" * 64,
        session_state_hash="d" * 64,
    )
    return PaperStrategyContext(
        account_id="paper-main",
        session_date=SESSION_DATE,
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


def _signal(*, target_quantity: int) -> TargetPortfolioSignal:
    return TargetPortfolioSignal(
        strategy_id="audited-target-v1",
        strategy_version="audited-target-implementation-v1",
        session_date=SESSION_DATE,
        evaluated_at=NOW,
        targets=(
            TargetInstrumentPosition(
                instrument=INSTRUMENT,
                target_quantity=target_quantity,
                rules=_rules(),
                policy=RiskPolicy(allowed_instruments=(INSTRUMENT,)),
            ),
        ),
        source_evidence_hash="f" * 64,
    )


@pytest.mark.asyncio
async def test_target_strategy_creates_bounded_idempotent_buy_delta() -> None:
    provider = MagicMock()
    provider.target = AsyncMock(return_value=_signal(target_quantity=250))
    source = TargetPositionPaperIntentSource(
        strategy_id="audited-target-v1",
        provider=provider,
    )
    context = _context()

    first = await source.evaluate(context)
    second = await source.evaluate(context)

    assert first.evaluation_hash == second.evaluation_hash
    assert len(first.intents) == 1
    assert first.intents[0].order.side is OrderSide.BUY
    assert first.intents[0].order.quantity == 200
    assert first.intents[0].order.client_order_id == (
        second.intents[0].order.client_order_id
    )


@pytest.mark.asyncio
async def test_target_strategy_respects_sellable_quantity_and_sell_step() -> None:
    provider = MagicMock()
    provider.target = AsyncMock(return_value=_signal(target_quantity=0))
    source = TargetPositionPaperIntentSource(
        strategy_id="audited-target-v1",
        provider=provider,
    )

    evaluation = await source.evaluate(_context(quantity=300, sellable=100))

    assert len(evaluation.intents) == 1
    assert evaluation.intents[0].order.side is OrderSide.SELL
    assert evaluation.intents[0].order.quantity == 100


@pytest.mark.asyncio
async def test_target_strategy_audits_unreachable_t_plus_one_target_without_order() -> None:
    provider = MagicMock()
    provider.target = AsyncMock(return_value=_signal(target_quantity=0))
    source = TargetPositionPaperIntentSource(
        strategy_id="audited-target-v1",
        provider=provider,
    )

    evaluation = await source.evaluate(_context(quantity=300, sellable=0))

    assert evaluation.intents == ()
    assert len(evaluation.signal_evidence_hash) == 64


@pytest.mark.asyncio
async def test_target_strategy_rejects_signal_from_another_strategy() -> None:
    signal = _signal(target_quantity=0)
    provider = MagicMock()
    provider.target = AsyncMock(
        return_value=TargetPortfolioSignal(
            strategy_id="other-strategy",
            strategy_version=signal.strategy_version,
            session_date=signal.session_date,
            evaluated_at=signal.evaluated_at,
            targets=signal.targets,
            source_evidence_hash=signal.source_evidence_hash,
        )
    )
    source = TargetPositionPaperIntentSource(
        strategy_id="audited-target-v1",
        provider=provider,
    )

    with pytest.raises(ValueError, match="does not match"):
        await source.evaluate(_context())
