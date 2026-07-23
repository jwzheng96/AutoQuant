from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from enum import StrEnum
from zoneinfo import ZoneInfo

from autoquant.clock import to_utc
from autoquant.data.daily_models import TradingSession
from autoquant.errors import MarketCalendarUnavailableError

SHANGHAI = ZoneInfo("Asia/Shanghai")


class AShareTradingPhase(StrEnum):
    NON_TRADING_DAY = "non_trading_day"
    CLOSED = "closed"
    PRE_OPEN = "pre_open"
    OPENING_AUCTION = "opening_auction"
    AUCTION_PAUSE = "auction_pause"
    MORNING_CONTINUOUS = "morning_continuous"
    MIDDAY_BREAK = "midday_break"
    AFTERNOON_CONTINUOUS = "afternoon_continuous"
    CLOSING_AUCTION = "closing_auction"

    @property
    def accepts_strategy_orders(self) -> bool:
        return self in {
            AShareTradingPhase.MORNING_CONTINUOUS,
            AShareTradingPhase.AFTERNOON_CONTINUOUS,
        }


@dataclass(frozen=True, slots=True)
class AShareMarketClock:
    """Point-in-time A-share stock phase classifier.

    The schedule covers stock auction trading only. Special halts and instrument-level
    suspensions remain quote/rule inputs and cannot be inferred from wall-clock time.
    """

    rule_version: str = "cn-equity-auction-hours-2026-v1"
    pre_open_start: time = time(8, 45)

    def phase(self, *, now: datetime, session: TradingSession) -> AShareTradingPhase:
        instant = to_utc(now, name="market clock time")
        local = instant.astimezone(SHANGHAI)
        if session.session_date != local.date():
            raise MarketCalendarUnavailableError(
                "trading calendar session does not match the Shanghai date"
            )
        if session.available_at > instant:
            raise MarketCalendarUnavailableError(
                "trading calendar was not available at the observation time"
            )
        if not session.is_open:
            return AShareTradingPhase.NON_TRADING_DAY

        current = local.timetz().replace(tzinfo=None)
        if current < self.pre_open_start:
            return AShareTradingPhase.CLOSED
        if current < time(9, 15):
            return AShareTradingPhase.PRE_OPEN
        if current < time(9, 25):
            return AShareTradingPhase.OPENING_AUCTION
        if current < time(9, 30):
            return AShareTradingPhase.AUCTION_PAUSE
        if current < time(11, 30):
            return AShareTradingPhase.MORNING_CONTINUOUS
        if current < time(13, 0):
            return AShareTradingPhase.MIDDAY_BREAK
        if current < time(14, 57):
            return AShareTradingPhase.AFTERNOON_CONTINUOUS
        if current < time(15, 0):
            return AShareTradingPhase.CLOSING_AUCTION
        return AShareTradingPhase.CLOSED
