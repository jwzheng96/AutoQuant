from datetime import UTC, date, datetime

import pytest

from autoquant.data.daily_models import TradingSession
from autoquant.errors import MarketCalendarUnavailableError
from autoquant.execution.market_clock import AShareMarketClock, AShareTradingPhase

RESPONSE_HASH = "a" * 64


def _instant(hour: int, minute: int) -> datetime:
    return datetime(2026, 7, 23, hour - 8, minute, tzinfo=UTC)


def _session(*, is_open: bool = True, day: int = 23) -> TradingSession:
    return TradingSession(
        source="tushare",
        session_date=date(2026, 7, day),
        is_open=is_open,
        available_at=datetime(2026, 7, 22, tzinfo=UTC),
        response_hash=RESPONSE_HASH,
    )


@pytest.mark.parametrize(
    ("hour", "minute", "expected"),
    [
        (8, 44, AShareTradingPhase.CLOSED),
        (8, 45, AShareTradingPhase.PRE_OPEN),
        (9, 14, AShareTradingPhase.PRE_OPEN),
        (9, 15, AShareTradingPhase.OPENING_AUCTION),
        (9, 24, AShareTradingPhase.OPENING_AUCTION),
        (9, 25, AShareTradingPhase.AUCTION_PAUSE),
        (9, 29, AShareTradingPhase.AUCTION_PAUSE),
        (9, 30, AShareTradingPhase.MORNING_CONTINUOUS),
        (11, 29, AShareTradingPhase.MORNING_CONTINUOUS),
        (11, 30, AShareTradingPhase.MIDDAY_BREAK),
        (12, 59, AShareTradingPhase.MIDDAY_BREAK),
        (13, 0, AShareTradingPhase.AFTERNOON_CONTINUOUS),
        (14, 56, AShareTradingPhase.AFTERNOON_CONTINUOUS),
        (14, 57, AShareTradingPhase.CLOSING_AUCTION),
        (14, 59, AShareTradingPhase.CLOSING_AUCTION),
        (15, 0, AShareTradingPhase.CLOSED),
    ],
)
def test_market_clock_classifies_stock_auction_boundaries(
    hour: int, minute: int, expected: AShareTradingPhase
) -> None:
    phase = AShareMarketClock().phase(now=_instant(hour, minute), session=_session())

    assert phase is expected
    assert phase.accepts_strategy_orders is (
        expected
        in {
            AShareTradingPhase.MORNING_CONTINUOUS,
            AShareTradingPhase.AFTERNOON_CONTINUOUS,
        }
    )


def test_closed_calendar_day_never_accepts_orders() -> None:
    phase = AShareMarketClock().phase(now=_instant(10, 0), session=_session(is_open=False))

    assert phase is AShareTradingPhase.NON_TRADING_DAY
    assert phase.accepts_strategy_orders is False


def test_market_clock_requires_point_in_time_matching_calendar() -> None:
    with pytest.raises(MarketCalendarUnavailableError, match="Shanghai date"):
        AShareMarketClock().phase(now=_instant(10, 0), session=_session(day=22))

    future_calendar = TradingSession(
        source="tushare",
        session_date=date(2026, 7, 23),
        is_open=True,
        available_at=datetime(2026, 7, 23, 3, tzinfo=UTC),
        response_hash=RESPONSE_HASH,
    )
    with pytest.raises(MarketCalendarUnavailableError, match="not available"):
        AShareMarketClock().phase(now=_instant(10, 0), session=future_calendar)
