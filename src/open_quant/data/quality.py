from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum

from open_quant.clock import SHANGHAI, to_shanghai, to_utc
from open_quant.data.models import (
    MarketCoverageEvidence,
    MinuteBarRevision,
    SuspensionStatus,
    TradingPeriod,
)

IssueAdder = Callable[[str, str, datetime, str], None]


class QualitySeverity(StrEnum):
    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class QualityIssue:
    severity: QualitySeverity
    code: str
    instrument: str
    event_time: datetime
    message: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "event_time",
            to_utc(self.event_time, name="issue.event_time"),
        )
        if not self.code.strip():
            raise ValueError("issue code cannot be empty")
        if not self.instrument.strip():
            raise ValueError("issue instrument cannot be empty")
        if not self.message.strip():
            raise ValueError("issue message cannot be empty")


@dataclass(frozen=True, slots=True)
class QualityReport:
    requested_instruments: tuple[str, ...]
    start: datetime
    end: datetime
    as_of: datetime | None
    issues: tuple[QualityIssue, ...]
    production_complete: bool
    passed: bool = field(init=False)
    report_hash: str = field(init=False)

    def __post_init__(self) -> None:
        requested_instruments = tuple(self.requested_instruments)
        if (
            not requested_instruments
            or any(
                not isinstance(instrument, str) or not instrument.strip()
                for instrument in requested_instruments
            )
        ):
            raise ValueError("requested_instruments cannot be empty")
        requested_instruments = tuple(sorted(set(requested_instruments)))
        start = to_utc(self.start, name="report.start")
        end = to_utc(self.end, name="report.end")
        as_of = (
            None if self.as_of is None else to_utc(self.as_of, name="report.as_of")
        )
        object.__setattr__(self, "requested_instruments", requested_instruments)
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)
        object.__setattr__(self, "as_of", as_of)
        issues = tuple(self.issues)
        if any(not isinstance(issue, QualityIssue) for issue in issues):
            raise TypeError("issues must contain QualityIssue values")
        issues = tuple(
            sorted(
                issues,
                key=lambda issue: (issue.instrument, issue.event_time, issue.code),
            )
        )
        object.__setattr__(self, "issues", issues)
        passed = not any(
            issue.severity is QualitySeverity.ERROR for issue in issues
        )
        object.__setattr__(self, "passed", passed)
        payload = {
            "as_of": None if as_of is None else as_of.isoformat(timespec="microseconds"),
            "end": end.isoformat(timespec="microseconds"),
            "issues": [
                {
                    "code": issue.code,
                    "event_time": issue.event_time.isoformat(timespec="microseconds"),
                    "instrument": issue.instrument,
                    "message": issue.message,
                    "severity": issue.severity.value,
                }
                for issue in issues
            ],
            "passed": passed,
            "production_complete": self.production_complete,
            "requested_instruments": list(requested_instruments),
            "start": start.isoformat(timespec="microseconds"),
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        object.__setattr__(self, "report_hash", hashlib.sha256(encoded).hexdigest())


class MinuteBarQualityGate:
    def evaluate(
        self,
        *,
        records: tuple[MinuteBarRevision, ...],
        requested_instruments: tuple[str, ...],
        start: datetime,
        end: datetime,
        coverage: MarketCoverageEvidence,
        as_of: datetime | None = None,
    ) -> QualityReport:
        requested_instruments = tuple(requested_instruments)
        if (
            not requested_instruments
            or any(
                not isinstance(instrument, str) or not instrument.strip()
                for instrument in requested_instruments
            )
        ):
            raise ValueError("requested_instruments cannot be empty")
        requested_instruments = tuple(sorted(set(requested_instruments)))
        normalized_start = to_utc(start, name="start")
        normalized_end = to_utc(end, name="end")
        normalized_as_of = (
            None if as_of is None else to_utc(as_of, name="as_of")
        )
        records = tuple(records)
        if any(not isinstance(record, MinuteBarRevision) for record in records):
            raise TypeError("records must contain MinuteBarRevision values")
        if not isinstance(coverage, MarketCoverageEvidence):
            raise TypeError("coverage must be MarketCoverageEvidence")

        issues: list[QualityIssue] = []

        def add_issue(
            code: str,
            instrument: str,
            event_time: datetime,
            message: str,
        ) -> None:
            issues.append(
                QualityIssue(
                    severity=QualitySeverity.ERROR,
                    code=code,
                    instrument=instrument,
                    event_time=event_time,
                    message=message,
                )
            )

        if normalized_start > normalized_end:
            add_issue(
                "invalid_interval",
                requested_instruments[0],
                normalized_end,
                "requested start follows requested end",
            )
            return QualityReport(
                requested_instruments=requested_instruments,
                start=normalized_start,
                end=normalized_end,
                as_of=normalized_as_of,
                issues=tuple(issues),
                production_complete=False,
            )

        requested_set = set(requested_instruments)
        self._check_records(
            records=records,
            requested_instruments=requested_set,
            start=normalized_start,
            end=normalized_end,
            add_issue=add_issue,
        )
        coverage_complete = self._check_coverage(
            records=records,
            requested_instruments=requested_instruments,
            start=normalized_start,
            end=normalized_end,
            coverage=coverage,
            as_of=normalized_as_of,
            add_issue=add_issue,
        )
        return QualityReport(
            requested_instruments=requested_instruments,
            start=normalized_start,
            end=normalized_end,
            as_of=normalized_as_of,
            issues=tuple(issues),
            production_complete=coverage_complete
            and not any(
                issue.severity is QualitySeverity.ERROR for issue in issues
            ),
        )

    @staticmethod
    def _check_records(
        *,
        records: tuple[MinuteBarRevision, ...],
        requested_instruments: set[str],
        start: datetime,
        end: datetime,
        add_issue: IssueAdder,
    ) -> None:
        seen: dict[
            tuple[str, str, datetime, str], MinuteBarRevision
        ] = {}
        last_event_time: dict[tuple[str, str], datetime] = {}
        for record in records:
            if record.instrument not in requested_instruments:
                add_issue(
                    "unexpected_instrument",
                    record.instrument,
                    record.event_time,
                    "bar instrument was not requested",
                )
            if not start <= record.event_time <= end:
                add_issue(
                    "outside_requested_interval",
                    record.instrument,
                    record.event_time,
                    "bar endpoint is outside the requested interval",
                )

            stream_key = (record.source, record.instrument)
            previous_event_time = last_event_time.get(stream_key)
            if (
                previous_event_time is not None
                and record.event_time < previous_event_time
            ):
                add_issue(
                    "non_monotonic_event_time",
                    record.instrument,
                    record.event_time,
                    "bar endpoints are not monotonic within their source stream",
                )
            last_event_time[stream_key] = record.event_time

            revision_key = (
                record.source,
                record.instrument,
                record.event_time,
                record.source_revision,
            )
            previous = seen.get(revision_key)
            if previous is None:
                seen[revision_key] = record
                continue
            previous_ohlc = (
                previous.open_price,
                previous.high_price,
                previous.low_price,
                previous.close_price,
            )
            current_ohlc = (
                record.open_price,
                record.high_price,
                record.low_price,
                record.close_price,
            )
            if previous_ohlc != current_ohlc:
                add_issue(
                    "schema_conflict",
                    record.instrument,
                    record.event_time,
                    "one source revision contains conflicting OHLC values",
                )
            else:
                add_issue(
                    "duplicate_revision",
                    record.instrument,
                    record.event_time,
                    "source revision appears more than once",
                )

    @staticmethod
    def _check_coverage(
        *,
        records: tuple[MinuteBarRevision, ...],
        requested_instruments: tuple[str, ...],
        start: datetime,
        end: datetime,
        coverage: MarketCoverageEvidence,
        as_of: datetime | None,
        add_issue: IssueAdder,
    ) -> bool:
        periods: defaultdict[
            tuple[str, date], list[TradingPeriod]
        ] = defaultdict(list)
        suspensions: defaultdict[
            tuple[str, date], list[SuspensionStatus]
        ] = defaultdict(list)
        for period in coverage.periods:
            periods[(period.instrument, period.session_date)].append(period)
        for suspension in coverage.suspensions:
            suspensions[(suspension.instrument, suspension.session_date)].append(
                suspension
            )

        coverage_complete = as_of is not None
        requested_dates = _dates_between(
            to_shanghai(start).date(),
            to_shanghai(end).date(),
        )
        in_range_records = tuple(
            record for record in records if start <= record.event_time <= end
        )
        session_suspensions: dict[tuple[str, date], bool] = {}

        for instrument in requested_instruments:
            for session_date in requested_dates:
                key = (instrument, session_date)
                session_time = datetime.combine(
                    session_date,
                    time.min,
                    tzinfo=SHANGHAI,
                ).astimezone(UTC)
                period_values = periods[key]
                suspension_values = suspensions[key]
                if not period_values or not suspension_values:
                    coverage_complete = False
                    add_issue(
                        "missing_coverage",
                        instrument,
                        session_time,
                        "trading-period and suspension evidence are both required",
                    )
                    continue
                if (
                    len(period_values) != 1
                    or len(suspension_values) != 1
                    or period_values[0].source != suspension_values[0].source
                ):
                    coverage_complete = False
                    add_issue(
                        "conflicting_coverage",
                        instrument,
                        session_time,
                        "coverage evidence is ambiguous for instrument and session",
                    )
                    continue

                period = period_values[0]
                suspension = suspension_values[0]
                session_suspensions[key] = suspension.suspended
                if as_of is not None and (
                    period.available_at > as_of or suspension.available_at > as_of
                ):
                    coverage_complete = False
                    add_issue(
                        "future_coverage",
                        instrument,
                        session_time,
                        "coverage evidence was unavailable at the requested cutoff",
                    )

                actual_records = tuple(
                    record
                    for record in in_range_records
                    if record.instrument == instrument
                    and to_shanghai(record.event_time).date() == session_date
                )
                if any(record.source != period.source for record in actual_records):
                    add_issue(
                        "schema_conflict",
                        instrument,
                        session_time,
                        "bar source does not match coverage source",
                    )
                if suspension.suspended:
                    if actual_records:
                        add_issue(
                            "suspended_session_has_bars",
                            instrument,
                            actual_records[0].event_time,
                            "a suspended session contains traded bars",
                        )
                    continue

                expected_endpoints = {
                    endpoint
                    for endpoint in period.minute_ends
                    if start <= endpoint <= end
                    and to_shanghai(endpoint).date() == session_date
                }
                actual_endpoints = {
                    record.event_time for record in actual_records
                }
                for endpoint in expected_endpoints - actual_endpoints:
                    add_issue(
                        "missing_bar",
                        instrument,
                        endpoint,
                        "expected trading-period endpoint has no bar",
                    )
                for endpoint in actual_endpoints - expected_endpoints:
                    add_issue(
                        "off_session_bar",
                        instrument,
                        endpoint,
                        "bar endpoint is absent from trading-period evidence",
                    )

        present_instruments = {
            record.instrument
            for record in in_range_records
            if record.instrument in requested_instruments
        }
        for instrument in requested_instruments:
            if instrument in present_instruments:
                continue
            proven_suspended = bool(requested_dates) and all(
                session_suspensions.get((instrument, session_date)) is True
                for session_date in requested_dates
            )
            if not proven_suspended:
                add_issue(
                    "missing_instrument",
                    instrument,
                    start,
                    "requested instrument has no bars",
                )

        return coverage_complete


def _dates_between(start: date, end: date) -> tuple[date, ...]:
    dates: list[date] = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return tuple(dates)
