from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Protocol

from autoquant.clock import to_shanghai, to_utc
from autoquant.data.daily_models import DailyBarRevision, TradingSession
from autoquant.data.daily_ports import DailyMarketRepository
from autoquant.data.models import (
    DatasetManifest,
    SourceEvidence,
    _canonical_hash,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.data.quality import QualityReport
from autoquant.errors import (
    MarketCalendarUnavailableError,
    PersistenceUnavailableError,
)
from autoquant.execution.paper_scheduler import SHANGHAI, PreOpenMarks


class PreOpenEvidenceRepository(Protocol):
    async def read_manifest(self, manifest_hash: str) -> DatasetManifest: ...

    async def read_quality_report(self, report_hash: str) -> QualityReport: ...

    async def read_source_evidence(self, evidence_hash: str) -> SourceEvidence: ...


@dataclass(frozen=True, slots=True)
class PreOpenMarkPolicy:
    """Point-in-time policy for a complete, single-session pre-open valuation."""

    lookback_days: int = 14
    max_valuation_lag_days: int = 4
    allowed_availability_policies: tuple[str, ...] = ("tushare-daily-v1",)
    version: str = "trusted-daily-close-preopen-v1"

    def __post_init__(self) -> None:
        if (
            not isinstance(self.lookback_days, int)
            or isinstance(self.lookback_days, bool)
            or self.lookback_days < 1
        ):
            raise ValueError("lookback_days must be a positive integer")
        if (
            not isinstance(self.max_valuation_lag_days, int)
            or isinstance(self.max_valuation_lag_days, bool)
            or self.max_valuation_lag_days < 1
            or self.max_valuation_lag_days > self.lookback_days
        ):
            raise ValueError(
                "max_valuation_lag_days must be positive and no greater than lookback_days"
            )
        policies = tuple(sorted(self.allowed_availability_policies))
        if not policies or len(set(policies)) != len(policies):
            raise ValueError("allowed availability policies must be nonempty and unique")
        if any(not value.strip() for value in policies):
            raise ValueError("allowed availability policies cannot be blank")
        _require_nonblank(self.version, name="pre-open mark policy version")
        object.__setattr__(self, "allowed_availability_policies", policies)


class DailyClosePreOpenMarkReader:
    """Read complete prior-session marks without crossing the evidence visibility time."""

    def __init__(
        self,
        *,
        repository: DailyMarketRepository,
        evidence_repository: PreOpenEvidenceRepository,
        manifest_hash: str,
        source: str,
        policy: PreOpenMarkPolicy | None = None,
    ) -> None:
        _require_nonblank(source, name="pre-open mark source")
        _require_lowercase_sha256(manifest_hash, name="pre-open manifest_hash")
        self._repository = repository
        self._evidence_repository = evidence_repository
        self._manifest_hash = manifest_hash
        self._source = source
        self._policy = policy or PreOpenMarkPolicy()

    async def __call__(
        self,
        session_date: date,
        instruments: tuple[str, ...],
        as_of: datetime,
    ) -> PreOpenMarks:
        instant = to_utc(as_of, name="pre-open mark query time")
        normalized = tuple(sorted(instruments))
        if not normalized or len(set(normalized)) != len(normalized):
            raise ValueError("pre-open mark instruments must be nonempty and unique")
        if any(not value.strip() for value in normalized):
            raise ValueError("pre-open mark instruments cannot be blank")
        if instant.astimezone(SHANGHAI).date() != session_date:
            raise ValueError("pre-open mark query date does not match Shanghai date")
        local_time = instant.astimezone(SHANGHAI).timetz().replace(tzinfo=None)
        if not time(8, 45) <= local_time < time(9, 15):
            raise ValueError("pre-open marks can only be read during the pre-open window")

        manifest = await self._validated_manifest(
            instruments=normalized,
            as_of=instant,
        )
        manifest_start = to_shanghai(manifest.start_time).date()
        manifest_end = to_shanghai(manifest.end_time).date()
        start = max(
            session_date - timedelta(days=self._policy.lookback_days),
            manifest_start,
        )
        end = min(session_date - timedelta(days=1), manifest_end)
        if start > end:
            raise PersistenceUnavailableError(
                "pre-open manifest does not cover a prior valuation session"
            )

        current_coverage = await self._repository.query_coverage_as_of(
            normalized,
            session_date,
            session_date,
            instant,
        )
        current_sessions = self._validate_sessions(
            current_coverage.sessions,
            start=session_date,
            session_date=session_date,
            as_of=instant,
        )
        current_session = current_sessions.get(session_date)
        if current_session is None or not current_session.is_open:
            raise MarketCalendarUnavailableError(
                "current pre-open session is not proven open"
            )
        await self._require_source_evidence(
            current_session.response_hash,
            method="trade_cal",
            as_of=instant,
        )

        coverage = await self._repository.query_coverage_as_of(
            normalized,
            start,
            end,
            manifest.as_of,
        )
        sessions = self._validate_sessions(
            coverage.sessions,
            start=start,
            session_date=end,
            as_of=manifest.as_of,
        )
        bars = await self._repository.query_bars_as_of(
            normalized,
            start,
            end,
            manifest.as_of,
        )
        indexed = self._validate_bars(
            bars,
            instruments=normalized,
            start=start,
            session_date=session_date,
            as_of=manifest.as_of,
        )
        valuation_session = self._select_valuation_session(
            sessions=sessions,
            indexed=indexed,
            instruments=normalized,
            session_date=session_date,
        )
        selected = tuple(indexed[(instrument, valuation_session)] for instrument in normalized)
        policies = {bar.availability_policy for bar in selected}
        if len(policies) != 1:
            raise PersistenceUnavailableError(
                "pre-open marks mix incompatible availability policies"
            )

        valuation_calendar = sessions[valuation_session]
        trusted_hashes = set(manifest.record_hashes)
        required_hashes = {
            valuation_calendar.content_hash,
            *(bar.content_hash for bar in selected),
        }
        if not required_hashes <= trusted_hashes:
            raise PersistenceUnavailableError(
                "pre-open valuation rows are not backed by the production manifest"
            )
        await self._require_source_evidence(
            valuation_calendar.response_hash,
            method="trade_cal",
            as_of=manifest.as_of,
        )
        for bar in selected:
            await self._require_source_evidence(
                bar.evidence_hash,
                method="daily",
                as_of=manifest.as_of,
            )
        evidence_as_of = max(
            current_session.available_at,
            valuation_calendar.available_at,
            *(max(bar.available_at, bar.ingested_at) for bar in selected),
        )
        source_evidence_hash = _canonical_hash(
            {
                "as_of": instant.isoformat(timespec="microseconds"),
                "bars": [
                    {
                        "availability_policy": bar.availability_policy,
                        "available_at": bar.available_at.isoformat(timespec="microseconds"),
                        "content_hash": bar.content_hash,
                        "evidence_hash": bar.evidence_hash,
                        "ingested_at": bar.ingested_at.isoformat(timespec="microseconds"),
                        "instrument": bar.instrument,
                    }
                    for bar in selected
                ],
                "current_session_hash": current_session.content_hash,
                "instruments": list(normalized),
                "manifest_hash": manifest.manifest_hash,
                "policy_version": self._policy.version,
                "quality_report_hash": manifest.quality_report_hash,
                "source": self._source,
                "valuation_session_date": valuation_session.isoformat(),
                "valuation_session_hash": valuation_calendar.content_hash,
            }
        )
        return PreOpenMarks(
            session_date=session_date,
            valuation_session_date=valuation_session,
            as_of=evidence_as_of,
            marks={bar.instrument: bar.close_price for bar in selected},
            source_evidence_hash=source_evidence_hash,
        )

    async def _validated_manifest(
        self,
        *,
        instruments: tuple[str, ...],
        as_of: datetime,
    ) -> DatasetManifest:
        manifest = await self._evidence_repository.read_manifest(self._manifest_hash)
        report = await self._evidence_repository.read_quality_report(
            manifest.quality_report_hash
        )
        if (
            manifest.manifest_hash != self._manifest_hash
            or manifest.source != self._source
            or set(manifest.instruments) != set(instruments)
            or not manifest.production_complete
            or manifest.as_of > as_of
            or report.report_hash != manifest.quality_report_hash
            or not report.passed
            or not report.production_complete
            or set(report.requested_instruments) != set(instruments)
        ):
            raise PersistenceUnavailableError(
                "pre-open valuation manifest is not trusted for this universe"
            )
        return manifest

    async def _require_source_evidence(
        self,
        evidence_hash: str,
        *,
        method: str,
        as_of: datetime,
    ) -> None:
        evidence = await self._evidence_repository.read_source_evidence(evidence_hash)
        if (
            evidence.response_hash != evidence_hash
            or evidence.source != self._source
            or evidence.method != method
            or evidence.requested_at > as_of
        ):
            raise PersistenceUnavailableError(
                "pre-open source evidence does not match the trusted revision"
            )

    def _validate_sessions(
        self,
        sessions: tuple[TradingSession, ...],
        *,
        start: date,
        session_date: date,
        as_of: datetime,
    ) -> dict[date, TradingSession]:
        indexed: dict[date, TradingSession] = {}
        for value in sessions:
            if (
                value.source != self._source
                or value.session_date < start
                or value.session_date > session_date
                or value.available_at > as_of
            ):
                raise MarketCalendarUnavailableError(
                    "pre-open calendar evidence is inconsistent"
                )
            if value.session_date in indexed:
                raise MarketCalendarUnavailableError(
                    "pre-open calendar contains duplicate sessions"
                )
            indexed[value.session_date] = value
        return indexed

    def _validate_bars(
        self,
        bars: tuple[DailyBarRevision, ...],
        *,
        instruments: tuple[str, ...],
        start: date,
        session_date: date,
        as_of: datetime,
    ) -> dict[tuple[str, date], DailyBarRevision]:
        expected = set(instruments)
        allowed_policies = set(self._policy.allowed_availability_policies)
        indexed: dict[tuple[str, date], DailyBarRevision] = {}
        for value in bars:
            if (
                value.source != self._source
                or value.instrument not in expected
                or value.session_date < start
                or value.session_date >= session_date
                or value.available_at > as_of
                or value.ingested_at > as_of
                or value.availability_policy not in allowed_policies
            ):
                raise PersistenceUnavailableError(
                    "pre-open daily-bar evidence is inconsistent"
                )
            key = (value.instrument, value.session_date)
            if key in indexed:
                raise PersistenceUnavailableError(
                    "pre-open daily-bar evidence contains duplicate revisions"
                )
            indexed[key] = value
        return indexed

    def _select_valuation_session(
        self,
        *,
        sessions: dict[date, TradingSession],
        indexed: dict[tuple[str, date], DailyBarRevision],
        instruments: tuple[str, ...],
        session_date: date,
    ) -> date:
        candidates = sorted(
            (
                candidate
                for candidate, session in sessions.items()
                if candidate < session_date
                and session.is_open
                and all((instrument, candidate) in indexed for instrument in instruments)
            ),
            reverse=True,
        )
        if not candidates:
            raise PersistenceUnavailableError(
                "no complete visible pre-open valuation session is available"
            )
        selected = candidates[0]
        if (session_date - selected).days > self._policy.max_valuation_lag_days:
            raise PersistenceUnavailableError(
                "latest complete pre-open valuation session is stale"
            )
        return selected
