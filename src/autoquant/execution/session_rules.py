from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from autoquant.backtest.models import InstrumentRules
from autoquant.backtest.rules import AshareRuleBook
from autoquant.clock import to_utc
from autoquant.data.daily_ports import DailyMarketRepository
from autoquant.data.ingestion import ControlRepository
from autoquant.data.models import (
    _canonical_hash,
    _datetime_text,
    _require_lowercase_sha256,
)


@dataclass(frozen=True, slots=True)
class SessionRuleSet:
    session_date: date
    as_of: datetime
    rules: tuple[InstrumentRules, ...]
    suspended_instruments: tuple[str, ...]
    source_evidence_hashes: tuple[str, ...]
    rule_set_hash: str = field(init=False)

    def __post_init__(self) -> None:
        as_of = to_utc(self.as_of, name="session rules as_of")
        rules = tuple(sorted(self.rules, key=lambda value: value.instrument))
        suspended = tuple(sorted(self.suspended_instruments))
        evidence_hashes = tuple(sorted(self.source_evidence_hashes))
        if (
            not rules
            or len({value.instrument for value in rules}) != len(rules)
            or any(value.effective_from > self.session_date for value in rules)
        ):
            raise ValueError("session rule set requires unique effective rules")
        instruments = {value.instrument for value in rules}
        if (
            len(set(suspended)) != len(suspended)
            or not set(suspended) <= instruments
        ):
            raise ValueError("suspended instruments must belong to the rule set")
        if not evidence_hashes:
            raise ValueError("session rule set requires source evidence")
        for value in evidence_hashes:
            _require_lowercase_sha256(value, name="source_evidence_hash")
        object.__setattr__(self, "as_of", as_of)
        object.__setattr__(self, "rules", rules)
        object.__setattr__(self, "suspended_instruments", suspended)
        object.__setattr__(self, "source_evidence_hashes", evidence_hashes)
        object.__setattr__(
            self,
            "rule_set_hash",
            _canonical_hash(
                {
                    "as_of": _datetime_text(as_of),
                    "instruments": [
                        {
                            "instrument": value.instrument,
                            "price_limit_rule_version": (
                                value.price_limit.rule_version
                            ),
                            "rule_version": value.rule_version,
                        }
                        for value in rules
                    ],
                    "session_date": self.session_date.isoformat(),
                    "source_evidence_hashes": list(evidence_hashes),
                    "suspended_instruments": list(suspended),
                    "version": "exact-session-rules-v1",
                }
            ),
        )


class ExactSessionRuleReader:
    """Compile rules only from exact, source-backed session reference revisions."""

    def __init__(
        self,
        *,
        market_repository: DailyMarketRepository,
        control_repository: ControlRepository,
        source: str = "tushare",
        rulebook: AshareRuleBook | None = None,
    ) -> None:
        if not source.strip():
            raise ValueError("session rule source cannot be empty")
        self._market = market_repository
        self._control = control_repository
        self._source = source
        self._rulebook = rulebook or AshareRuleBook()

    async def read(
        self,
        *,
        instruments: tuple[str, ...],
        session_date: date,
        as_of: datetime,
    ) -> SessionRuleSet:
        normalized = tuple(sorted(instruments))
        if (
            not normalized
            or len(set(normalized)) != len(normalized)
            or any(not value.strip() for value in normalized)
        ):
            raise ValueError("session rule instruments must be nonempty and unique")
        cutoff = to_utc(as_of, name="session rules as_of")
        coverage = await self._market.query_coverage_as_of(
            normalized,
            session_date,
            session_date,
            cutoff,
        )
        sessions = tuple(
            value
            for value in coverage.sessions
            if value.source == self._source
            and value.session_date == session_date
        )
        lifecycles = tuple(
            sorted(
                (
                    value
                    for value in coverage.lifecycles
                    if value.source == self._source
                    and value.instrument in normalized
                ),
                key=lambda value: value.instrument,
            )
        )
        suspensions = tuple(
            sorted(
                (
                    value
                    for value in coverage.suspensions
                    if value.source == self._source
                    and value.session_date == session_date
                    and value.instrument in normalized
                ),
                key=lambda value: value.instrument,
            )
        )
        limits = tuple(
            sorted(
                (
                    value
                    for value in coverage.price_limits
                    if value.source == self._source
                    and value.session_date == session_date
                    and value.instrument in normalized
                ),
                key=lambda value: value.instrument,
            )
        )
        if (
            len(sessions) != 1
            or not sessions[0].is_open
            or tuple(value.instrument for value in lifecycles) != normalized
            or tuple(value.instrument for value in suspensions) != normalized
            or tuple(value.instrument for value in limits) != normalized
        ):
            raise ValueError("exact session rule coverage is incomplete")
        if any(
            value.list_date > session_date
            or (
                value.delist_date is not None
                and value.delist_date < session_date
            )
            for value in lifecycles
        ):
            raise ValueError("session rule coverage contains an inactive instrument")
        if (
            sessions[0].available_at > cutoff
            or any(value.available_at > cutoff for value in lifecycles)
            or any(value.available_at > cutoff for value in suspensions)
            or any(value.available_at > cutoff for value in limits)
        ):
            raise ValueError("session rule coverage is not yet visible")
        expected = {
            ("trade_cal", sessions[0].response_hash),
            *(("stock_basic", value.response_hash) for value in lifecycles),
            *(("suspend_d", value.response_hash) for value in suspensions),
            *(("stk_limit", value.response_hash) for value in limits),
        }
        evidence_hashes: list[str] = []
        for method, response_hash in sorted(expected):
            evidence = await self._control.read_source_evidence(response_hash)
            if (
                evidence.source != self._source
                or evidence.method != method
                or evidence.requested_at > cutoff
            ):
                raise ValueError("session rule source evidence does not match")
            evidence_hashes.append(evidence.response_hash)
        rule_by_instrument = {
            value.instrument: self._rulebook.resolve_with_price_limit(
                value.instrument,
                session_date,
                value,
            )
            for value in limits
        }
        return SessionRuleSet(
            session_date=session_date,
            as_of=cutoff,
            rules=tuple(rule_by_instrument[value] for value in normalized),
            suspended_instruments=tuple(
                value.instrument for value in suspensions if value.suspended
            ),
            source_evidence_hashes=tuple(evidence_hashes),
        )
