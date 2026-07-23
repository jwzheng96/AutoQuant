from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from autoquant.data.daily_models import (
    DailyPriceLimit,
    DailySuspensionStatus,
    InstrumentLifecycle,
    SessionReferenceBatch,
    TradingSession,
)
from autoquant.data.models import SourceEvidence
from autoquant.data.session_reference import SessionReferenceRefreshService
from autoquant.errors import PersistenceUnavailableError

NOW = datetime(2026, 7, 22, 8, tzinfo=UTC)
SESSION_DATE = date(2026, 7, 23)
INSTRUMENT = "600000.XSHG"


def _evidence(method: str) -> SourceEvidence:
    body = f"trusted-{method}".encode()
    return SourceEvidence(
        source="tushare",
        method=method,
        requested_at=NOW,
        response_body=body,
        response_hash=hashlib.sha256(body).hexdigest(),
    )


def _batch() -> SessionReferenceBatch:
    trade = _evidence("trade_cal")
    basic = _evidence("stock_basic")
    suspend = _evidence("suspend_d")
    limit = _evidence("stk_limit")
    return SessionReferenceBatch(
        session=TradingSession(
            source="tushare",
            session_date=SESSION_DATE,
            is_open=True,
            available_at=NOW,
            response_hash=trade.response_hash,
        ),
        lifecycles=(
            InstrumentLifecycle(
                source="tushare",
                instrument=INSTRUMENT,
                list_date=date(1999, 11, 10),
                delist_date=None,
                available_at=NOW,
                response_hash=basic.response_hash,
            ),
        ),
        suspensions=(
            DailySuspensionStatus(
                source="tushare",
                instrument=INSTRUMENT,
                session_date=SESSION_DATE,
                suspended=False,
                available_at=NOW,
                response_hash=suspend.response_hash,
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
                available_at=NOW,
                response_hash=limit.response_hash,
            ),
        ),
        source_evidence=(trade, basic, suspend, limit),
    )


def _service(
    *,
    append_error: Exception | None = None,
) -> tuple[SessionReferenceRefreshService, MagicMock, MagicMock]:
    source = MagicMock()
    source.fetch_session_reference = AsyncMock(return_value=_batch())
    market = MagicMock()
    market.append_coverage = AsyncMock(
        side_effect=append_error,
        return_value=4,
    )
    control = MagicMock()
    control.save_source_evidence = AsyncMock()
    control.append_audit_event = AsyncMock(return_value="a" * 64)
    return (
        SessionReferenceRefreshService(
            source=source,
            market_repository=market,
            control_repository=control,
            now=lambda: NOW,
        ),
        market,
        control,
    )


@pytest.mark.asyncio
async def test_session_reference_persists_rows_evidence_and_audit() -> None:
    service, market, control = _service()

    result = await service.run(
        instruments=(INSTRUMENT,),
        session_date=SESSION_DATE,
    )

    assert result.status == "completed"
    assert result.instrument_count == 1
    assert result.reference_hash is not None
    assert control.save_source_evidence.await_count == 4
    market.append_coverage.assert_awaited_once()
    audit = control.append_audit_event.await_args
    assert audit.args[0] == "session_reference_refreshed"
    assert audit.args[2]["session_date"] == "2026-07-23"


@pytest.mark.asyncio
async def test_session_reference_fails_closed_on_partial_persistence() -> None:
    service, _, control = _service(
        append_error=PersistenceUnavailableError("unavailable")
    )

    result = await service.run(
        instruments=(INSTRUMENT,),
        session_date=SESSION_DATE,
    )

    assert result.status == "persistence_failed"
    assert result.audit_event_hash is None
    control.append_audit_event.assert_not_awaited()
