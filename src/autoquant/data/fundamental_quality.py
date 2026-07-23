from __future__ import annotations

from datetime import date, datetime, time, timedelta

from autoquant.clock import SHANGHAI, to_utc
from autoquant.data.fundamental_models import FundamentalDatasetBatch
from autoquant.data.quality import (
    QualityIssue,
    QualityReport,
    QualitySeverity,
)

_REQUIRED_METHODS = frozenset(
    {"trade_cal", "daily_basic", "fina_indicator"}
)


class FundamentalQualityGate:
    """Structural gate for a point-in-time fundamental shard.

    Exact feature coverage against tradable bars is intentionally deferred to
    panel compilation, where lifecycle and suspension evidence are available.
    """

    def evaluate(
        self,
        *,
        batch: FundamentalDatasetBatch,
        requested_instruments: tuple[str, ...],
        start: date,
        end: date,
        as_of: datetime,
    ) -> QualityReport:
        if not isinstance(batch, FundamentalDatasetBatch):
            raise TypeError("batch must be FundamentalDatasetBatch")
        if (
            not requested_instruments
            or len(set(requested_instruments))
            != len(requested_instruments)
            or any(
                not isinstance(value, str) or not value.strip()
                for value in requested_instruments
            )
        ):
            raise ValueError(
                "requested_instruments must be nonempty and unique"
            )
        if start > end:
            raise ValueError("start cannot follow end")
        instruments = tuple(sorted(requested_instruments))
        cutoff = to_utc(as_of, name="as_of")
        issues: list[QualityIssue] = []

        def add(
            code: str,
            instrument: str,
            event_date: date,
            message: str,
        ) -> None:
            issues.append(
                QualityIssue(
                    severity=QualitySeverity.ERROR,
                    code=code,
                    instrument=instrument,
                    event_time=self._event_time(event_date),
                    message=message,
                )
            )

        methods = {value.method for value in batch.source_evidence}
        for method in sorted(_REQUIRED_METHODS - methods):
            add(
                f"missing_{method}_evidence",
                instruments[0],
                start,
                f"required {method} source evidence is absent",
            )
        for evidence in batch.source_evidence:
            if evidence.requested_at > cutoff:
                add(
                    "evidence_not_visible",
                    instruments[0],
                    start,
                    f"{evidence.method} evidence is later than as_of",
                )

        requested = set(instruments)
        sessions = {
            value.session_date: value for value in batch.sessions
        }
        for calendar_date in self._dates(start, end):
            if calendar_date not in sessions:
                add(
                    "missing_trading_session",
                    instruments[0],
                    calendar_date,
                    "trade calendar does not cover requested date",
                )

        valuations_by_instrument = {
            instrument: 0 for instrument in instruments
        }
        for valuation in batch.valuations:
            if (
                valuation.instrument not in requested
                or not start <= valuation.session_date <= end
            ):
                add(
                    "valuation_outside_request",
                    valuation.instrument,
                    valuation.session_date,
                    "daily valuation is outside requested scope",
                )
            else:
                valuations_by_instrument[valuation.instrument] += 1
            session = sessions.get(valuation.session_date)
            if session is None or not session.is_open:
                add(
                    "valuation_on_closed_session",
                    valuation.instrument,
                    valuation.session_date,
                    "daily valuation does not belong to an open session",
                )
            if (
                valuation.available_at > cutoff
                or valuation.ingested_at > cutoff
            ):
                add(
                    "valuation_not_visible",
                    valuation.instrument,
                    valuation.session_date,
                    "daily valuation is later than as_of",
                )

        for indicator in batch.indicators:
            if (
                indicator.instrument not in requested
                or not start <= indicator.announced_date <= end
            ):
                add(
                    "indicator_outside_request",
                    indicator.instrument,
                    indicator.announced_date,
                    "financial indicator is outside requested scope",
                )
            if (
                indicator.available_at > cutoff
                or indicator.ingested_at > cutoff
            ):
                add(
                    "indicator_not_visible",
                    indicator.instrument,
                    indicator.announced_date,
                    "financial indicator is later than as_of",
                )

        for instrument in instruments:
            if valuations_by_instrument[instrument] == 0:
                add(
                    "missing_valuation_history",
                    instrument,
                    start,
                    "instrument has no daily valuation history",
                )
        return QualityReport(
            requested_instruments=instruments,
            start=self._start_time(start),
            end=self._event_time(end),
            as_of=cutoff,
            issues=tuple(issues),
            production_complete=not issues,
        )

    @staticmethod
    def _dates(start: date, end: date) -> tuple[date, ...]:
        return tuple(
            start + timedelta(days=offset)
            for offset in range((end - start).days + 1)
        )

    @staticmethod
    def _start_time(value: date) -> datetime:
        return to_utc(
            datetime.combine(value, time.min, tzinfo=SHANGHAI)
        )

    @staticmethod
    def _event_time(value: date) -> datetime:
        return to_utc(
            datetime.combine(value, time(15), tzinfo=SHANGHAI)
        )
