from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal

from autoquant.backtest.models import FeeBreakdown, InstrumentRules, OrderSide, PriceLimit
from autoquant.data.daily_models import DailyPriceLimit

RULE_CHANGE_2026 = date(2026, 7, 6)
REGISTRATION_RULE_CHANGE_2023 = date(2023, 2, 17)
LEGACY_CASH_RULE_EFFECTIVE = date(1990, 12, 19)
STAMP_DUTY_CHANGE_2023 = date(2023, 8, 28)
TRANSFER_FEE_CHANGE_2022 = date(2022, 4, 29)


@dataclass(frozen=True, slots=True)
class SecurityStatus:
    risk_warning: bool
    listing_session_number: int

    def __post_init__(self) -> None:
        if type(self.risk_warning) is not bool:
            raise TypeError("risk_warning must be a bool")
        if (
            not isinstance(self.listing_session_number, int)
            or isinstance(self.listing_session_number, bool)
            or self.listing_session_number < 1
        ):
            raise ValueError("listing_session_number must be a positive integer")


class AshareRuleBook:
    """Versioned cash-equity rules; security status must be point-in-time data."""

    def resolve(
        self, instrument: str, session_date: date, status: SecurityStatus
    ) -> InstrumentRules:
        code, separator, venue = instrument.partition(".")
        if (
            separator != "."
            or len(code) != 6
            or not code.isdigit()
            or venue not in {"XSHG", "XSHE"}
        ):
            raise ValueError("instrument must use the 000001.XSHE form")
        star = venue == "XSHG" and code.startswith(("688", "689"))
        chinext = venue == "XSHE" and code.startswith(("300", "301"))
        if status.listing_session_number <= 5:
            price_limit = PriceLimit(
                rate=None,
                reason="first_five_listing_sessions",
                rule_version="cn-ipo-no-limit-v1",
            )
        elif star or chinext:
            price_limit = PriceLimit(
                rate=Decimal("0.20"),
                reason="innovation_board",
                rule_version="cn-innovation-board-20pct-v1",
            )
        elif status.risk_warning and session_date < RULE_CHANGE_2026:
            price_limit = PriceLimit(
                rate=Decimal("0.05"),
                reason="pre_2026_main_board_risk_warning",
                rule_version="cn-main-risk-warning-5pct-pre-2026-07-06",
            )
        else:
            price_limit = PriceLimit(
                rate=Decimal("0.10"),
                reason="main_board",
                rule_version="cn-main-board-10pct-v1",
            )
        version_date, rule_version = _cash_rule_version(session_date)
        return InstrumentRules(
            instrument=instrument,
            buy_minimum=200 if star else 100,
            buy_step=1 if star else 100,
            sell_step=1 if star else 100,
            price_tick=Decimal("0.01"),
            max_order_quantity=1_000_000,
            t_plus_one=True,
            price_limit=price_limit,
            effective_from=version_date,
            rule_version=rule_version,
        )

    def resolve_with_price_limit(
        self,
        instrument: str,
        session_date: date,
        daily_limit: DailyPriceLimit,
    ) -> InstrumentRules:
        """Resolve order rules while using the vendor's exact daily boundaries."""
        if (
            daily_limit.instrument != instrument
            or daily_limit.session_date != session_date
        ):
            raise ValueError("daily price limit must match instrument and session")
        code, separator, venue = instrument.partition(".")
        if (
            separator != "."
            or len(code) != 6
            or not code.isdigit()
            or venue not in {"XSHG", "XSHE"}
        ):
            raise ValueError("instrument must use the 000001.XSHE form")
        star = venue == "XSHG" and code.startswith(("688", "689"))
        implied_rate = max(
            (daily_limit.up_limit / daily_limit.pre_close) - Decimal("1"),
            Decimal("1") - (daily_limit.down_limit / daily_limit.pre_close),
        )
        version_date, rule_version = _cash_rule_version(session_date)
        return InstrumentRules(
            instrument=instrument,
            buy_minimum=200 if star else 100,
            buy_step=1 if star else 100,
            sell_step=1 if star else 100,
            price_tick=Decimal("0.01"),
            max_order_quantity=1_000_000,
            t_plus_one=True,
            price_limit=PriceLimit(
                rate=min(implied_rate, Decimal("0.999999")),
                reason="vendor_exact_daily_limit",
                rule_version="tushare-stk-limit-v1",
            ),
            effective_from=version_date,
            rule_version=rule_version,
        )


def _cash_rule_version(session_date: date) -> tuple[date, str]:
    if session_date >= RULE_CHANGE_2026:
        return RULE_CHANGE_2026, "sse-szse-cash-equity-2026-07-06"
    if session_date >= REGISTRATION_RULE_CHANGE_2023:
        return (
            REGISTRATION_RULE_CHANGE_2023,
            "sse-szse-cash-equity-pre-2026-07-06",
        )
    return LEGACY_CASH_RULE_EFFECTIVE, "sse-szse-cash-equity-legacy-v1"


@dataclass(frozen=True, slots=True)
class FeeSchedule:
    commission_rate: Decimal = Decimal("0.0003")
    minimum_commission: Decimal = Decimal("5")
    sell_stamp_duty_rate: Decimal = Decimal("0.0005")
    legacy_sell_stamp_duty_rate: Decimal = Decimal("0.001")
    transfer_fee_rate: Decimal = Decimal("0.00001")
    legacy_transfer_fee_rate: Decimal = Decimal("0.00002")
    version: str = "cn-a-share-reference-fees-historical-v2"

    def __post_init__(self) -> None:
        for name, value in (
            ("commission_rate", self.commission_rate),
            ("minimum_commission", self.minimum_commission),
            ("sell_stamp_duty_rate", self.sell_stamp_duty_rate),
            ("legacy_sell_stamp_duty_rate", self.legacy_sell_stamp_duty_rate),
            ("transfer_fee_rate", self.transfer_fee_rate),
            ("legacy_transfer_fee_rate", self.legacy_transfer_fee_rate),
        ):
            if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
                raise ValueError(f"{name} must be a nonnegative finite Decimal")
        if not self.version.strip():
            raise ValueError("fee version cannot be empty")

    def calculate(
        self, *, side: OrderSide, gross_amount: Decimal, session_date: date
    ) -> FeeBreakdown:
        cent = Decimal("0.01")
        commission = max(
            gross_amount * self.commission_rate, self.minimum_commission
        ).quantize(cent, rounding=ROUND_HALF_UP)
        stamp = (
            gross_amount
            * (
                self.sell_stamp_duty_rate
                if session_date >= STAMP_DUTY_CHANGE_2023
                else self.legacy_sell_stamp_duty_rate
            )
            if side is OrderSide.SELL
            else Decimal("0")
        ).quantize(cent, rounding=ROUND_HALF_UP)
        transfer_rate = (
            self.transfer_fee_rate
            if session_date >= TRANSFER_FEE_CHANGE_2022
            else self.legacy_transfer_fee_rate
        )
        transfer = (gross_amount * transfer_rate).quantize(
            cent, rounding=ROUND_HALF_UP
        )
        return FeeBreakdown(
            commission=commission,
            stamp_duty=stamp,
            transfer_fee=transfer,
        )
