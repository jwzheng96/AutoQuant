from datetime import date
from decimal import Decimal

import pytest

from autoquant.backtest.models import OrderSide
from autoquant.backtest.rules import AshareRuleBook, FeeSchedule, SecurityStatus


def test_current_main_board_risk_warning_uses_ten_percent_limit() -> None:
    rules = AshareRuleBook().resolve(
        "600000.XSHG",
        date(2026, 7, 22),
        SecurityStatus(risk_warning=True, listing_session_number=500),
    )

    assert rules.price_limit.rate == Decimal("0.10")
    assert rules.buy_minimum == 100
    assert rules.buy_step == 100
    assert rules.rule_version == "sse-szse-cash-equity-2026-07-06"


def test_historical_risk_warning_and_listing_rules_are_versioned() -> None:
    rulebook = AshareRuleBook()
    historical = rulebook.resolve(
        "000001.XSHE",
        date(2025, 7, 22),
        SecurityStatus(risk_warning=True, listing_session_number=500),
    )
    newly_listed = rulebook.resolve(
        "300001.XSHE",
        date(2026, 7, 22),
        SecurityStatus(risk_warning=False, listing_session_number=3),
    )

    assert historical.price_limit.rate == Decimal("0.05")
    assert newly_listed.price_limit.rate is None
    assert newly_listed.price_limit.reason == "first_five_listing_sessions"


def test_star_market_uses_200_share_minimum_with_one_share_step() -> None:
    rules = AshareRuleBook().resolve(
        "688001.XSHG",
        date(2026, 7, 22),
        SecurityStatus(risk_warning=False, listing_session_number=500),
    )

    assert rules.buy_minimum == 200
    assert rules.buy_step == 1
    assert rules.price_limit.rate == Decimal("0.20")


def test_security_status_rejects_unknown_listing_age() -> None:
    with pytest.raises(ValueError, match="listing_session_number"):
        SecurityStatus(risk_warning=False, listing_session_number=0)


def test_fee_schedule_charges_sell_tax_and_bilateral_transfer_fee() -> None:
    schedule = FeeSchedule()

    buy = schedule.calculate(
        side=OrderSide.BUY,
        gross_amount=Decimal("10000"),
        session_date=date(2026, 7, 22),
    )
    sell = schedule.calculate(
        side=OrderSide.SELL,
        gross_amount=Decimal("10000"),
        session_date=date(2026, 7, 22),
    )

    assert buy.commission == Decimal("5.00")
    assert buy.stamp_duty == Decimal("0.00")
    assert buy.transfer_fee == Decimal("0.10")
    assert sell.commission == Decimal("5.00")
    assert sell.stamp_duty == Decimal("5.00")
    assert sell.transfer_fee == Decimal("0.10")
