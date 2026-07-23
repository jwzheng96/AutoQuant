from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime

from autoquant.clock import to_utc
from autoquant.data.daily_models import DailyCoverageEvidence
from autoquant.data.daily_ports import (
    DailyMarketRepository,
    TradingCalendarSource,
)
from autoquant.data.ingestion import ControlRepository
from autoquant.errors import PersistenceUnavailableError


@dataclass(frozen=True, slots=True)
class TradingCalendarRefreshResult:
    status: str
    session_count: int
    source_evidence_hash: str | None
    audit_event_hash: str | None


class TradingCalendarRefreshService:
    """Persist an exact vendor calendar interval without treating it as daily bars."""

    def __init__(
        self,
        *,
        source: TradingCalendarSource,
        market_repository: DailyMarketRepository,
        control_repository: ControlRepository,
        now: Callable[[], datetime],
    ) -> None:
        self._source = source
        self._market = market_repository
        self._control = control_repository
        self._now = now

    async def run(
        self,
        *,
        start: date,
        end: date,
    ) -> TradingCalendarRefreshResult:
        if start > end:
            raise ValueError("calendar start cannot follow end")
        batch = await self._source.fetch_trading_calendar(start, end)
        evidence = batch.source_evidence[0]
        try:
            await self._control.save_source_evidence(evidence)
            written = await self._market.append_coverage(
                DailyCoverageEvidence(
                    sessions=batch.sessions,
                    lifecycles=(),
                    suspensions=(),
                )
            )
            if written != len(batch.sessions):
                raise PersistenceUnavailableError(
                    "trading calendar persistence row count mismatch"
                )
            event_hash = await self._control.append_audit_event(
                "trading_calendar_refreshed",
                to_utc(self._now(), name="calendar audit time"),
                {
                    "end": end.isoformat(),
                    "session_hashes": [
                        value.content_hash for value in batch.sessions
                    ],
                    "source": evidence.source,
                    "source_evidence_hash": evidence.response_hash,
                    "start": start.isoformat(),
                },
            )
        except PersistenceUnavailableError:
            return TradingCalendarRefreshResult(
                status="persistence_failed",
                session_count=len(batch.sessions),
                source_evidence_hash=evidence.response_hash,
                audit_event_hash=None,
            )
        return TradingCalendarRefreshResult(
            status="completed",
            session_count=len(batch.sessions),
            source_evidence_hash=evidence.response_hash,
            audit_event_hash=event_hash,
        )
