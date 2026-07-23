from __future__ import annotations

from datetime import date, datetime, time, timedelta

from autoquant.clock import SHANGHAI, to_utc
from autoquant.data.daily_models import (
    AdjustmentFactorRevision,
    DailyBarRevision,
    DailyDatasetBatch,
    InstrumentLifecycle,
)
from autoquant.data.quality import QualityIssue, QualityReport, QualitySeverity

_REQUIRED_METHODS = frozenset(
    {"daily", "adj_factor", "trade_cal", "stock_basic", "suspend_d", "stk_limit"}
)


class DailyQualityGate:
    def evaluate(
        self,
        *,
        batch: DailyDatasetBatch,
        requested_instruments: tuple[str, ...],
        start: date,
        end: date,
        as_of: datetime | None,
    ) -> QualityReport:
        if not isinstance(batch, DailyDatasetBatch):
            raise TypeError("batch must be DailyDatasetBatch")
        if (
            not requested_instruments
            or any(
                not isinstance(value, str) or not value.strip()
                for value in requested_instruments
            )
            or len(set(requested_instruments)) != len(requested_instruments)
        ):
            raise ValueError("requested_instruments must be nonempty and unique")

        instruments = tuple(sorted(requested_instruments))
        start_time = self._start_time(start)
        end_time = self._event_time(end)
        normalized_as_of = None if as_of is None else to_utc(as_of, name="as_of")
        issues: list[QualityIssue] = []

        def add(code: str, instrument: str, session_date: date, message: str) -> None:
            issues.append(
                QualityIssue(
                    severity=QualitySeverity.ERROR,
                    code=code,
                    instrument=instrument,
                    event_time=self._event_time(session_date),
                    message=message,
                )
            )

        if start > end:
            add("invalid_interval", instruments[0], end, "requested start follows end")
            return QualityReport(
                requested_instruments=instruments,
                start=start_time,
                end=end_time,
                as_of=normalized_as_of,
                issues=tuple(issues),
                production_complete=False,
            )

        evidence_methods = {value.method for value in batch.source_evidence}
        for method in sorted(_REQUIRED_METHODS - evidence_methods):
            add(
                f"missing_{method}_evidence",
                instruments[0],
                start,
                f"required {method} source evidence is absent",
            )
        if normalized_as_of is not None:
            for evidence in batch.source_evidence:
                if evidence.requested_at > normalized_as_of:
                    add(
                        "evidence_not_visible",
                        instruments[0],
                        start,
                        f"{evidence.method} evidence is later than as_of",
                    )

        requested = set(instruments)
        bars = {(value.instrument, value.session_date): value for value in batch.bars}
        factors = {
            (value.instrument, value.session_date): value for value in batch.factors
        }
        sessions = {value.session_date: value for value in batch.coverage.sessions}
        lifecycles = {value.instrument: value for value in batch.coverage.lifecycles}
        suspensions = {
            (value.instrument, value.session_date): value
            for value in batch.coverage.suspensions
        }
        price_limits = {
            (value.instrument, value.session_date): value
            for value in batch.coverage.price_limits
        }

        def check_record(
            record: DailyBarRevision | AdjustmentFactorRevision,
        ) -> None:
            if record.instrument not in requested or not start <= record.session_date <= end:
                add(
                    "record_outside_request",
                    record.instrument,
                    record.session_date,
                    "daily record is outside the requested instruments or dates",
                )
            if normalized_as_of is not None and record.available_at > normalized_as_of:
                add(
                    "record_not_visible",
                    record.instrument,
                    record.session_date,
                    "daily record is not visible at as_of",
                )

        for bar_record in batch.bars:
            check_record(bar_record)
        for factor_record in batch.factors:
            check_record(factor_record)

        if normalized_as_of is not None:
            for session_coverage in batch.coverage.sessions:
                if session_coverage.available_at > normalized_as_of:
                    add(
                        "coverage_not_visible",
                        instruments[0],
                        session_coverage.session_date,
                        "coverage evidence is not visible at as_of",
                    )
            for lifecycle_coverage in batch.coverage.lifecycles:
                if lifecycle_coverage.available_at > normalized_as_of:
                    add(
                        "coverage_not_visible",
                        lifecycle_coverage.instrument,
                        start,
                        "coverage evidence is not visible at as_of",
                    )
            for suspension_coverage in batch.coverage.suspensions:
                if suspension_coverage.available_at > normalized_as_of:
                    add(
                        "coverage_not_visible",
                        suspension_coverage.instrument,
                        suspension_coverage.session_date,
                        "coverage evidence is not visible at as_of",
                    )
            for limit_coverage in batch.coverage.price_limits:
                if limit_coverage.available_at > normalized_as_of:
                    add(
                        "coverage_not_visible",
                        limit_coverage.instrument,
                        limit_coverage.session_date,
                        "price-limit evidence is not visible at as_of",
                    )

        for instrument in instruments:
            lifecycle = lifecycles.get(instrument)
            if lifecycle is None:
                add(
                    "missing_lifecycle",
                    instrument,
                    start,
                    "instrument lifecycle evidence is absent",
                )
            for session_date in self._dates(start, end):
                key = (instrument, session_date)
                session = sessions.get(session_date)
                if session is None:
                    add(
                        "missing_trading_session",
                        instrument,
                        session_date,
                        "trade calendar does not cover the requested date",
                    )
                    continue
                expected = session.is_open and self._inside_lifecycle(
                    lifecycle, session_date
                )
                suspension = suspensions.get(key)
                if expected and suspension is None:
                    add(
                        "missing_suspension_status",
                        instrument,
                        session_date,
                        "open listed session lacks suspension coverage",
                    )
                    continue
                suspended = suspension.suspended if suspension is not None else False
                should_have_record = expected and not suspended
                has_bar = key in bars
                has_factor = key in factors
                if should_have_record:
                    if key not in price_limits:
                        add(
                            "missing_price_limit",
                            instrument,
                            session_date,
                            "open unsuspended session lacks price-limit coverage",
                        )
                    if not has_bar:
                        add(
                            "missing_daily_bar",
                            instrument,
                            session_date,
                            "open unsuspended session lacks a daily bar",
                        )
                    if not has_factor:
                        add(
                            "missing_adjustment_factor",
                            instrument,
                            session_date,
                            "open unsuspended session lacks an adjustment factor",
                        )
                else:
                    if has_bar:
                        add(
                            "unexpected_daily_bar",
                            instrument,
                            session_date,
                            "daily bar exists for a closed, inactive, or suspended session",
                        )
                    if has_factor:
                        add(
                            "unexpected_adjustment_factor",
                            instrument,
                            session_date,
                            "adjustment factor exists without an expected trading record",
                        )

        production_complete = normalized_as_of is not None and not issues
        return QualityReport(
            requested_instruments=instruments,
            start=start_time,
            end=end_time,
            as_of=normalized_as_of,
            issues=tuple(issues),
            production_complete=production_complete,
        )

    @staticmethod
    def _inside_lifecycle(
        lifecycle: InstrumentLifecycle | None, session_date: date
    ) -> bool:
        if lifecycle is None or session_date < lifecycle.list_date:
            return False
        return lifecycle.delist_date is None or session_date < lifecycle.delist_date

    @staticmethod
    def _dates(start: date, end: date) -> tuple[date, ...]:
        if start > end:
            return ()
        return tuple(
            start + timedelta(days=offset) for offset in range((end - start).days + 1)
        )

    @staticmethod
    def _start_time(session_date: date) -> datetime:
        return to_utc(datetime.combine(session_date, time.min, tzinfo=SHANGHAI))

    @staticmethod
    def _event_time(session_date: date) -> datetime:
        return to_utc(datetime.combine(session_date, time(15, 0), tzinfo=SHANGHAI))
