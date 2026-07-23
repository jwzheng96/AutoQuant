from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from autoquant.data.daily_models import (
    DailyCoverageEvidence,
    DailyPriceLimit,
    DailySuspensionStatus,
    InstrumentLifecycle,
    TradingSession,
)
from autoquant.data.models import SourceEvidence
from autoquant.execution.session_rules import ExactSessionRuleReader

NOW = datetime(2026, 7, 23, 1, tzinfo=UTC)
SESSION_DATE = date(2026, 7, 23)
INSTRUMENT = "600000.XSHG"


def _evidence(method: str) -> SourceEvidence:
    body = f"trusted-{method}".encode()
    return SourceEvidence(
        source="tushare",
        method=method,
        requested_at=NOW - timedelta(minutes=5),
        response_body=body,
        response_hash=hashlib.sha256(body).hexdigest(),
    )


def _coverage(*, suspended: bool = False) -> tuple[
    DailyCoverageEvidence,
    dict[str, SourceEvidence],
]:
    values = {
        method: _evidence(method)
        for method in ("trade_cal", "stock_basic", "suspend_d", "stk_limit")
    }
    coverage = DailyCoverageEvidence(
        sessions=(
            TradingSession(
                source="tushare",
                session_date=SESSION_DATE,
                is_open=True,
                available_at=NOW - timedelta(minutes=5),
                response_hash=values["trade_cal"].response_hash,
            ),
        ),
        lifecycles=(
            InstrumentLifecycle(
                source="tushare",
                instrument=INSTRUMENT,
                list_date=date(1999, 11, 10),
                delist_date=None,
                available_at=NOW - timedelta(minutes=5),
                response_hash=values["stock_basic"].response_hash,
            ),
        ),
        suspensions=(
            DailySuspensionStatus(
                source="tushare",
                instrument=INSTRUMENT,
                session_date=SESSION_DATE,
                suspended=suspended,
                available_at=NOW - timedelta(minutes=5),
                response_hash=values["suspend_d"].response_hash,
            ),
        ),
        price_limits=(
            DailyPriceLimit(
                source="tushare",
                instrument=INSTRUMENT,
                session_date=SESSION_DATE,
                pre_close=Decimal("10"),
                up_limit=Decimal("11"),
                down_limit=Decimal("9"),
                available_at=NOW - timedelta(minutes=5),
                response_hash=values["stk_limit"].response_hash,
            ),
        ),
    )
    return coverage, {
        value.response_hash: value for value in values.values()
    }


def _reader(*, suspended: bool = False) -> ExactSessionRuleReader:
    coverage, evidence = _coverage(suspended=suspended)
    market = MagicMock()
    market.query_coverage_as_of = AsyncMock(return_value=coverage)
    control = MagicMock()
    control.read_source_evidence = AsyncMock(
        side_effect=lambda value: evidence[value]
    )
    return ExactSessionRuleReader(
        market_repository=market,
        control_repository=control,
    )


@pytest.mark.asyncio
async def test_exact_session_rule_reader_binds_vendor_limits_and_evidence() -> None:
    result = await _reader().read(
        instruments=(INSTRUMENT,),
        session_date=SESSION_DATE,
        as_of=NOW,
    )

    assert result.rules[0].price_limit.reason == "vendor_exact_daily_limit"
    assert result.rules[0].price_limit.rate == Decimal("0.10")
    assert result.suspended_instruments == ()
    assert len(result.source_evidence_hashes) == 4
    assert len(result.rule_set_hash) == 64


@pytest.mark.asyncio
async def test_exact_session_rule_reader_preserves_suspension_state() -> None:
    result = await _reader(suspended=True).read(
        instruments=(INSTRUMENT,),
        session_date=SESSION_DATE,
        as_of=NOW,
    )

    assert result.suspended_instruments == (INSTRUMENT,)


@pytest.mark.asyncio
async def test_exact_session_rule_reader_rejects_missing_limit() -> None:
    coverage, _ = _coverage()
    market = MagicMock()
    market.query_coverage_as_of = AsyncMock(
        return_value=DailyCoverageEvidence(
            sessions=coverage.sessions,
            lifecycles=coverage.lifecycles,
            suspensions=coverage.suspensions,
            price_limits=(),
        )
    )
    control = MagicMock()
    reader = ExactSessionRuleReader(
        market_repository=market,
        control_repository=control,
    )

    with pytest.raises(ValueError, match="incomplete"):
        await reader.read(
            instruments=(INSTRUMENT,),
            session_date=SESSION_DATE,
            as_of=NOW,
        )
