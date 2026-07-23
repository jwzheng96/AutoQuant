from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime

from autoquant.clock import to_utc
from autoquant.data.daily_models import DailyCoverageEvidence
from autoquant.data.daily_ports import DailyMarketRepository, SessionReferenceSource
from autoquant.data.ingestion import ControlRepository
from autoquant.data.models import _canonical_hash
from autoquant.errors import PersistenceUnavailableError


@dataclass(frozen=True, slots=True)
class SessionReferenceRefreshResult:
    status: str
    session_date: date
    instrument_count: int
    reference_hash: str | None
    audit_event_hash: str | None


class SessionReferenceRefreshService:
    """Persist exact open-session controls without endorsing an unfinished daily bar."""

    def __init__(
        self,
        *,
        source: SessionReferenceSource,
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
        instruments: tuple[str, ...],
        session_date: date,
    ) -> SessionReferenceRefreshResult:
        normalized = tuple(sorted(instruments))
        if (
            not normalized
            or len(normalized) > 20
            or len(set(normalized)) != len(normalized)
            or any(not value.strip() for value in normalized)
        ):
            raise ValueError(
                "session reference instruments must be 1-20 nonblank unique values"
            )
        batch = await self._source.fetch_session_reference(
            normalized,
            session_date,
        )
        observed = tuple(value.instrument for value in batch.lifecycles)
        if observed != normalized or batch.session.session_date != session_date:
            raise ValueError("session reference does not match the request")
        coverage = DailyCoverageEvidence(
            sessions=(batch.session,),
            lifecycles=batch.lifecycles,
            suspensions=batch.suspensions,
            price_limits=batch.price_limits,
        )
        row_hashes = (
            batch.session.content_hash,
            *(value.content_hash for value in batch.lifecycles),
            *(value.content_hash for value in batch.suspensions),
            *(value.content_hash for value in batch.price_limits),
        )
        evidence_hashes = tuple(
            value.response_hash for value in batch.source_evidence
        )
        reference_hash = _canonical_hash(
            {
                "evidence_hashes": list(evidence_hashes),
                "instruments": list(normalized),
                "row_hashes": list(row_hashes),
                "session_date": session_date.isoformat(),
                "version": "tushare-session-reference-v1",
            }
        )
        try:
            for evidence in batch.source_evidence:
                await self._control.save_source_evidence(evidence)
            written = await self._market.append_coverage(coverage)
            if written != len(row_hashes):
                raise PersistenceUnavailableError(
                    "session reference persistence row count mismatch"
                )
            audit_hash = await self._control.append_audit_event(
                "session_reference_refreshed",
                to_utc(self._now(), name="session reference audit time"),
                {
                    "instruments": list(normalized),
                    "reference_hash": reference_hash,
                    "row_hashes": list(row_hashes),
                    "session_date": session_date.isoformat(),
                    "source_evidence_hashes": list(evidence_hashes),
                },
            )
        except PersistenceUnavailableError:
            return SessionReferenceRefreshResult(
                status="persistence_failed",
                session_date=session_date,
                instrument_count=len(normalized),
                reference_hash=reference_hash,
                audit_event_hash=None,
            )
        return SessionReferenceRefreshResult(
            status="completed",
            session_date=session_date,
            instrument_count=len(normalized),
            reference_hash=reference_hash,
            audit_event_hash=audit_hash,
        )
