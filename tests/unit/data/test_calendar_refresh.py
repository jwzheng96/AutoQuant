from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from autoquant.data.calendar_refresh import TradingCalendarRefreshService
from autoquant.data.daily_models import TradingCalendarBatch, TradingSession
from autoquant.data.models import SourceEvidence
from autoquant.errors import PersistenceUnavailableError

NOW = datetime(2026, 7, 22, 8, tzinfo=UTC)


def _batch() -> TradingCalendarBatch:
    body = b"trusted-calendar-response"
    evidence = SourceEvidence(
        source="tushare",
        method="trade_cal",
        requested_at=NOW,
        response_body=body,
        response_hash=hashlib.sha256(body).hexdigest(),
    )
    return TradingCalendarBatch(
        sessions=(
            TradingSession(
                source="tushare",
                session_date=date(2026, 7, 23),
                is_open=True,
                available_at=NOW,
                response_hash=evidence.response_hash,
            ),
        ),
        source_evidence=(evidence,),
    )


def _service(
    *,
    append_side_effect: Exception | None = None,
) -> tuple[TradingCalendarRefreshService, MagicMock, MagicMock]:
    source = MagicMock()
    source.fetch_trading_calendar = AsyncMock(return_value=_batch())
    market = MagicMock()
    market.append_coverage = AsyncMock(
        side_effect=append_side_effect,
        return_value=1,
    )
    control = MagicMock()
    control.save_source_evidence = AsyncMock()
    control.append_audit_event = AsyncMock(return_value="a" * 64)
    return (
        TradingCalendarRefreshService(
            source=source,
            market_repository=market,
            control_repository=control,
            now=lambda: NOW,
        ),
        market,
        control,
    )


@pytest.mark.asyncio
async def test_calendar_refresh_persists_source_sessions_and_audit() -> None:
    service, market, control = _service()

    result = await service.run(
        start=date(2026, 7, 23),
        end=date(2026, 7, 23),
    )

    assert result.status == "completed"
    assert result.session_count == 1
    assert result.audit_event_hash == "a" * 64
    control.save_source_evidence.assert_awaited_once()
    market.append_coverage.assert_awaited_once()
    audit = control.append_audit_event.await_args
    assert audit.args[0] == "trading_calendar_refreshed"
    assert audit.args[2]["start"] == "2026-07-23"


@pytest.mark.asyncio
async def test_calendar_refresh_reports_persistence_failure_without_audit_success() -> None:
    service, _, control = _service(
        append_side_effect=PersistenceUnavailableError("unavailable")
    )

    result = await service.run(
        start=date(2026, 7, 23),
        end=date(2026, 7, 23),
    )

    assert result.status == "persistence_failed"
    assert result.audit_event_hash is None
    control.append_audit_event.assert_not_awaited()
