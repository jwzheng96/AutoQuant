from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from autoquant.backtest.models import OrderSide
from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.data.daily_models import TradingSession
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.control import (
    KillSwitchControl,
    KillSwitchReason,
)
from autoquant.execution.coordinator import PaperCoordinationStatus
from autoquant.execution.market_clock import AShareTradingPhase
from autoquant.execution.paper_scheduler import (
    PaperSchedulerStatus,
    PaperStrategyIntent,
    PaperTradingScheduler,
    PreOpenMarks,
)
from autoquant.execution.quote_book import ContinuousQuoteBook
from autoquant.risk.models import MarketQuote, ProposedOrder, RiskPolicy

INSTRUMENT = "600000.XSHG"
SESSION_DATE = date(2026, 7, 23)
OPEN = datetime(2026, 7, 23, 1, 30, tzinfo=UTC)
PRE_OPEN = datetime(2026, 7, 23, 1, 0, tzinfo=UTC)
ZERO_HASH = "0" * 64


def _control(*, active: bool) -> KillSwitchControl:
    return KillSwitchControl(
        account_id="paper-main",
        active=active,
        version=1,
        reason=(KillSwitchReason.INITIALIZING if active else KillSwitchReason.RESET_APPROVED),
        changed_at=datetime(2026, 7, 23, 0, tzinfo=UTC),
        changed_by="test",
        last_event_hash=ZERO_HASH,
    )


def _session(*, is_open: bool = True) -> TradingSession:
    return TradingSession(
        source="tushare",
        session_date=SESSION_DATE,
        is_open=is_open,
        available_at=datetime(2026, 7, 22, tzinfo=UTC),
        response_hash="a" * 64,
    )


def _quote(*, as_of: datetime = OPEN) -> MarketQuote:
    return MarketQuote(
        instrument=INSTRUMENT,
        as_of=as_of,
        last_price=Decimal("10"),
        bid_price=Decimal("9.99"),
        ask_price=Decimal("10.01"),
        market_open=True,
    )


def _quote_book() -> ContinuousQuoteBook:
    book = ContinuousQuoteBook(source="qmt")
    book.reset(
        quotes=(_quote(),),
        source_sequence=10,
        received_at=OPEN,
        reset_id="scheduler-baseline",
    )
    return book


def _intent(*, submitted_at: datetime = OPEN, order_id: str = "paper-signal-0001"):
    rules = AshareRuleBook().resolve(
        INSTRUMENT,
        SESSION_DATE,
        SecurityStatus(risk_warning=False, listing_session_number=1000),
    )
    return PaperStrategyIntent(
        order=ProposedOrder(
            client_order_id=order_id,
            instrument=INSTRUMENT,
            side=OrderSide.BUY,
            quantity=100,
            submitted_at=submitted_at,
        ),
        rules=rules,
        policy=RiskPolicy(allowed_instruments=(INSTRUMENT,)),
    )


def _scheduler(
    *,
    now_control: KillSwitchControl,
    calendar_reader: AsyncMock | None = None,
    pre_open_mark_reader: AsyncMock | None = None,
    quotes: ContinuousQuoteBook | None = None,
    initializer: MagicMock | None = None,
    coordinator: MagicMock | None = None,
    controls: MagicMock | None = None,
    sessions: MagicMock | None = None,
    intent_source: MagicMock | None = None,
) -> tuple[PaperTradingScheduler, dict[str, object]]:
    active_controls = MagicMock() if controls is None else controls
    if controls is None:
        active_controls.ensure_fail_closed = AsyncMock(return_value=now_control)
        active_controls.get = AsyncMock(return_value=now_control)
        active_controls.activate = AsyncMock(return_value=_control(active=True))
    active_sessions = MagicMock() if sessions is None else sessions
    if sessions is None:
        active_sessions.replay = AsyncMock(return_value=MagicMock())
    active_initializer = MagicMock() if initializer is None else initializer
    if initializer is None:
        active_initializer.initialize = AsyncMock()
    active_coordinator = MagicMock() if coordinator is None else coordinator
    if coordinator is None:
        active_coordinator.submit = AsyncMock()
    active_intents = MagicMock() if intent_source is None else intent_source
    if intent_source is None:
        active_intents.generate = AsyncMock(return_value=())
    active_calendar = (
        AsyncMock(return_value=_session()) if calendar_reader is None else calendar_reader
    )
    active_marks = AsyncMock() if pre_open_mark_reader is None else pre_open_mark_reader
    book = _quote_book() if quotes is None else quotes
    scheduler = PaperTradingScheduler(
        account_id="paper-main",
        strategy_id="test-strategy-v1",
        instruments=(INSTRUMENT,),
        calendar_reader=active_calendar,
        pre_open_mark_reader=active_marks,
        quotes=book,
        initializer=active_initializer,
        coordinator=active_coordinator,
        controls=active_controls,
        sessions=active_sessions,
        intent_source=active_intents,
    )
    return scheduler, {
        "calendar": active_calendar,
        "marks": active_marks,
        "initializer": active_initializer,
        "coordinator": active_coordinator,
        "controls": active_controls,
        "sessions": active_sessions,
        "intents": active_intents,
        "quotes": book,
    }


@pytest.mark.asyncio
async def test_closed_or_auction_phase_is_idle_and_never_calls_strategy() -> None:
    scheduler, dependencies = _scheduler(now_control=_control(active=False))

    cycle = await scheduler.tick(now=datetime(2026, 7, 23, 1, 20, tzinfo=UTC))

    assert cycle.phase is AShareTradingPhase.OPENING_AUCTION
    assert cycle.status is PaperSchedulerStatus.IDLE
    dependencies["intents"].generate.assert_not_awaited()  # type: ignore[union-attr]
    dependencies["coordinator"].submit.assert_not_awaited()  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_pre_open_initializes_once_then_replays_existing_session() -> None:
    sessions = MagicMock()
    sessions.replay = AsyncMock(side_effect=[LookupError("missing"), MagicMock()])
    initialization = MagicMock()
    initialization.control = _control(active=True)
    initialization.state.state_hash = "b" * 64
    initialization.reconciliation.report_hash = "e" * 64
    initializer = MagicMock()
    initializer.initialize = AsyncMock(return_value=initialization)
    marks = PreOpenMarks(
        session_date=SESSION_DATE,
        valuation_session_date=date(2026, 7, 22),
        as_of=datetime(2026, 7, 22, 7, tzinfo=UTC),
        marks={INSTRUMENT: Decimal("10")},
        source_evidence_hash="c" * 64,
    )
    mark_reader = AsyncMock(return_value=marks)
    scheduler, _ = _scheduler(
        now_control=_control(active=True),
        sessions=sessions,
        initializer=initializer,
        pre_open_mark_reader=mark_reader,
    )

    initialized = await scheduler.tick(now=PRE_OPEN)
    ready = await scheduler.tick(now=PRE_OPEN + timedelta(minutes=1))

    assert initialized.status is PaperSchedulerStatus.SESSION_INITIALIZED
    assert initialized.initialization is initialization
    assert ready.status is PaperSchedulerStatus.SESSION_READY
    initializer.initialize.assert_awaited_once()
    mark_reader.assert_awaited_once()


@pytest.mark.asyncio
async def test_active_control_blocks_continuous_cycle_before_quotes_or_strategy() -> None:
    scheduler, dependencies = _scheduler(now_control=_control(active=True))
    book = dependencies["quotes"]
    assert isinstance(book, ContinuousQuoteBook)
    book.disconnect(reason="test")

    cycle = await scheduler.tick(now=OPEN)

    assert cycle.status is PaperSchedulerStatus.LOCKED
    dependencies["sessions"].replay.assert_not_awaited()  # type: ignore[union-attr]
    dependencies["intents"].generate.assert_not_awaited()  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_continuous_cycle_with_no_intents_commits_quote_evidence() -> None:
    scheduler, dependencies = _scheduler(now_control=_control(active=False))

    cycle = await scheduler.tick(now=OPEN)

    assert cycle.status is PaperSchedulerStatus.NO_INTENTS
    assert cycle.quote_evidence_hash is not None
    assert len(cycle.cycle_hash) == 64
    dependencies["sessions"].replay.assert_awaited_once()  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_continuous_cycle_routes_bounded_intent_through_coordinator() -> None:
    intent_source = MagicMock()
    intent_source.generate = AsyncMock(return_value=(_intent(),))
    projection = MagicMock()
    projection.projection_hash = "d" * 64
    result = MagicMock()
    result.control = _control(active=False)
    result.status = PaperCoordinationStatus.FILLED
    result.projection = projection
    result.decision = None
    result.post_reconciliation = None
    result.pre_reconciliation = None
    result.updates = ()
    coordinator = MagicMock()
    coordinator.submit = AsyncMock(return_value=result)
    scheduler, dependencies = _scheduler(
        now_control=_control(active=False),
        intent_source=intent_source,
        coordinator=coordinator,
    )

    cycle = await scheduler.tick(now=OPEN)

    assert cycle.status is PaperSchedulerStatus.COMPLETED
    assert cycle.coordination_results == (result,)
    request = coordinator.submit.await_args.args[0]
    assert request.order.client_order_id == "paper-signal-0001"
    assert request.quote.quote_hash == _quote().quote_hash
    assert request.marks == {INSTRUMENT: Decimal("10")}
    dependencies["controls"].activate.assert_not_awaited()  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_stale_quote_or_invalid_strategy_fails_closed() -> None:
    controls = MagicMock()
    inactive = _control(active=False)
    controls.ensure_fail_closed = AsyncMock(return_value=inactive)
    controls.get = AsyncMock(return_value=inactive)
    controls.activate = AsyncMock(return_value=_control(active=True))
    stale_book = _quote_book()
    scheduler, dependencies = _scheduler(
        now_control=inactive,
        controls=controls,
        quotes=stale_book,
    )

    stale = await scheduler.tick(now=OPEN + timedelta(seconds=4))

    assert stale.status is PaperSchedulerStatus.FAILED
    assert stale.error_code == "quote_stream_unavailable"
    assert stale.control.active is True
    assert controls.activate.await_args.kwargs["reason"] is (
        KillSwitchReason.DEPENDENCY_UNAVAILABLE
    )
    dependencies["intents"].generate.assert_not_awaited()  # type: ignore[union-attr]

    controls.activate.reset_mock()
    controls.ensure_fail_closed.return_value = inactive
    invalid_intents = MagicMock()
    invalid_intents.generate = AsyncMock(
        return_value=(
            _intent(order_id="duplicate-order"),
            _intent(order_id="duplicate-order"),
        )
    )
    valid_book = _quote_book()
    invalid_scheduler, _ = _scheduler(
        now_control=inactive,
        controls=controls,
        quotes=valid_book,
        intent_source=invalid_intents,
    )
    invalid = await invalid_scheduler.tick(now=OPEN)

    assert invalid.status is PaperSchedulerStatus.FAILED
    assert invalid.error_code == "invalid_scheduler_input"
    controls.activate.assert_awaited_once()


@pytest.mark.asyncio
async def test_scheduler_rejects_overlapping_cycle() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked_calendar(_session_date: date, _now: datetime) -> TradingSession:
        entered.set()
        await release.wait()
        return _session()

    scheduler, dependencies = _scheduler(
        now_control=_control(active=False),
        calendar_reader=AsyncMock(side_effect=blocked_calendar),
    )
    first = asyncio.create_task(scheduler.tick(now=OPEN))
    await entered.wait()

    with pytest.raises(PersistenceUnavailableError, match="overlap"):
        await scheduler.tick(now=OPEN)
    dependencies["controls"].activate.assert_awaited_once()  # type: ignore[union-attr]

    release.set()
    await first


@pytest.mark.asyncio
async def test_scheduler_run_stops_after_persisted_sink_cycle() -> None:
    scheduler, _ = _scheduler(now_control=_control(active=False))
    stop = asyncio.Event()
    cycles = []

    async def sink(cycle):  # type: ignore[no-untyped-def]
        cycles.append(cycle)
        stop.set()

    await scheduler.run(
        stop=stop,
        poll_interval=timedelta(seconds=1),
        now=lambda: OPEN,
        sink=sink,
    )

    assert len(cycles) == 1


@pytest.mark.asyncio
async def test_scheduler_run_activates_control_when_cycle_sink_fails() -> None:
    scheduler, dependencies = _scheduler(now_control=_control(active=False))
    stop = asyncio.Event()

    async def failing_sink(_cycle):  # type: ignore[no-untyped-def]
        raise RuntimeError("database unavailable")

    with pytest.raises(PersistenceUnavailableError, match="evidence persistence"):
        await scheduler.run(
            stop=stop,
            poll_interval=timedelta(seconds=1),
            now=lambda: OPEN,
            sink=failing_sink,
        )

    dependencies["controls"].activate.assert_awaited_once()  # type: ignore[union-attr]
