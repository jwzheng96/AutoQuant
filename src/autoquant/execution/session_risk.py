from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.execution.models import ZERO_HASH, PaperOrderHistory

_SHANGHAI = ZoneInfo("Asia/Shanghai")
SESSION_RISK_VERSION = "paper-session-risk-v1"


def _nonnegative(value: Decimal, *, name: str, positive: bool = False) -> None:
    if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
        raise ValueError(f"{name} must be a nonnegative finite Decimal")
    if positive and value == 0:
        raise ValueError(f"{name} must be positive")


@dataclass(frozen=True, slots=True)
class SessionTurnoverEvidence:
    account_id: str
    session_date: date
    cumulative_turnover: Decimal
    fill_evidence: tuple[tuple[str, str, str], ...]
    evidence_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(self.account_id, name="account_id")
        _nonnegative(self.cumulative_turnover, name="cumulative_turnover")
        facts = tuple(sorted(self.fill_evidence))
        object.__setattr__(self, "fill_evidence", facts)
        for order_hash, update_hash, gross_amount in facts:
            _require_lowercase_sha256(order_hash, name="turnover order_hash")
            _require_lowercase_sha256(update_hash, name="turnover update_hash")
            _nonnegative(Decimal(gross_amount), name="turnover fill gross", positive=True)
        object.__setattr__(
            self,
            "evidence_hash",
            _canonical_hash(
                {
                    "account_id": self.account_id,
                    "cumulative_turnover": _decimal_text(self.cumulative_turnover),
                    "fill_evidence": [list(item) for item in facts],
                    "session_date": self.session_date.isoformat(),
                    "version": SESSION_RISK_VERSION,
                }
            ),
        )


def derive_session_turnover(
    *,
    account_id: str,
    session_date: date,
    histories: tuple[PaperOrderHistory, ...],
) -> SessionTurnoverEvidence:
    _require_nonblank(account_id, name="account_id")
    histories = tuple(sorted(histories, key=lambda item: item.order.client_order_id))
    identifiers = tuple(item.order.client_order_id for item in histories)
    if len(set(identifiers)) != len(identifiers):
        raise ValueError("session histories contain duplicate client_order_id")
    if any(item.order.account_id != account_id for item in histories):
        raise ValueError("session history belongs to another account")
    total = Decimal("0")
    facts: list[tuple[str, str, str]] = []
    for history in histories:
        prior_quantity = 0
        prior_gross = Decimal("0")
        for update in history.updates:
            cumulative = update.cumulative_filled_quantity
            if cumulative == prior_quantity:
                continue
            if cumulative < prior_quantity or update.average_fill_price is None:
                raise ValueError("session fill facts are inconsistent")
            cumulative_gross = update.average_fill_price * cumulative
            fill_gross = cumulative_gross - prior_gross
            if fill_gross <= 0:
                raise ValueError("session incremental fill gross must be positive")
            if update.occurred_at.astimezone(_SHANGHAI).date() == session_date:
                total += fill_gross
                facts.append(
                    (
                        history.order.order_hash,
                        update.update_hash,
                        _decimal_text(fill_gross),
                    )
                )
            prior_quantity = cumulative
            prior_gross = cumulative_gross
    return SessionTurnoverEvidence(
        account_id=account_id,
        session_date=session_date,
        cumulative_turnover=total,
        fill_evidence=tuple(facts),
    )


@dataclass(frozen=True, slots=True)
class SessionRiskObservation:
    account_id: str
    session_date: date
    as_of: datetime
    equity: Decimal
    cumulative_turnover: Decimal
    snapshot_hash: str
    turnover_evidence_hash: str
    observation_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(self.account_id, name="account_id")
        as_of = to_utc(self.as_of, name="session risk observation as_of")
        object.__setattr__(self, "as_of", as_of)
        if as_of.astimezone(_SHANGHAI).date() != self.session_date:
            raise ValueError("session observation time must belong to session_date")
        _nonnegative(self.equity, name="session equity", positive=True)
        _nonnegative(self.cumulative_turnover, name="cumulative_turnover")
        _require_lowercase_sha256(self.snapshot_hash, name="snapshot_hash")
        _require_lowercase_sha256(
            self.turnover_evidence_hash,
            name="turnover_evidence_hash",
        )
        object.__setattr__(
            self,
            "observation_hash",
            _canonical_hash(observation_payload(self)),
        )


@dataclass(frozen=True, slots=True)
class PaperSessionRiskState:
    account_id: str
    session_date: date
    day_start_equity: Decimal
    peak_equity: Decimal
    cumulative_turnover: Decimal
    as_of: datetime
    latest_observation_hash: str
    latest_snapshot_hash: str
    turnover_evidence_hash: str
    version: int
    last_event_hash: str
    state_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(self.account_id, name="account_id")
        _nonnegative(self.day_start_equity, name="day_start_equity", positive=True)
        _nonnegative(self.peak_equity, name="peak_equity", positive=True)
        _nonnegative(self.cumulative_turnover, name="cumulative_turnover")
        if self.peak_equity < self.day_start_equity:
            raise ValueError("peak_equity cannot be below day_start_equity")
        as_of = to_utc(self.as_of, name="session risk state as_of")
        object.__setattr__(self, "as_of", as_of)
        if as_of.astimezone(_SHANGHAI).date() != self.session_date:
            raise ValueError("session state time must belong to session_date")
        if self.version < 1:
            raise ValueError("session risk version must be positive")
        for name, value in (
            ("latest_observation_hash", self.latest_observation_hash),
            ("latest_snapshot_hash", self.latest_snapshot_hash),
            ("turnover_evidence_hash", self.turnover_evidence_hash),
            ("last_event_hash", self.last_event_hash),
        ):
            _require_lowercase_sha256(value, name=name)
        object.__setattr__(self, "state_hash", _canonical_hash(state_payload(self)))


@dataclass(frozen=True, slots=True)
class SessionRiskEvent:
    sequence: int
    observation: SessionRiskObservation
    previous_hash: str
    transition_state_hash: str
    event_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if self.sequence < 1:
            raise ValueError("session event sequence must be positive")
        _require_lowercase_sha256(self.previous_hash, name="previous_hash")
        _require_lowercase_sha256(
            self.transition_state_hash,
            name="transition_state_hash",
        )
        object.__setattr__(
            self,
            "event_hash",
            _canonical_hash(
                {
                    "observation_hash": self.observation.observation_hash,
                    "previous_hash": self.previous_hash,
                    "sequence": self.sequence,
                    "transition_state_hash": self.transition_state_hash,
                }
            ),
        )


def apply_session_observation(
    current: PaperSessionRiskState | None,
    observation: SessionRiskObservation,
) -> tuple[PaperSessionRiskState, SessionRiskEvent]:
    if current is None:
        if observation.cumulative_turnover != 0:
            raise ValueError("session must be initialized before its first fill")
        version = 1
        previous_hash = ZERO_HASH
        day_start_equity = observation.equity
        peak_equity = observation.equity
    else:
        if (
            current.account_id != observation.account_id
            or current.session_date != observation.session_date
        ):
            raise ValueError("session observation identity does not match state")
        if observation.as_of < current.as_of:
            raise ValueError("session observation time cannot move backwards")
        if observation.cumulative_turnover < current.cumulative_turnover:
            raise ValueError("session turnover cannot decrease")
        version = current.version + 1
        previous_hash = current.last_event_hash
        day_start_equity = current.day_start_equity
        peak_equity = max(current.peak_equity, observation.equity)
    provisional = PaperSessionRiskState(
        account_id=observation.account_id,
        session_date=observation.session_date,
        day_start_equity=day_start_equity,
        peak_equity=peak_equity,
        cumulative_turnover=observation.cumulative_turnover,
        as_of=observation.as_of,
        latest_observation_hash=observation.observation_hash,
        latest_snapshot_hash=observation.snapshot_hash,
        turnover_evidence_hash=observation.turnover_evidence_hash,
        version=version,
        last_event_hash=previous_hash,
    )
    event = SessionRiskEvent(
        sequence=version,
        observation=observation,
        previous_hash=previous_hash,
        transition_state_hash=provisional.state_hash,
    )
    state = PaperSessionRiskState(
        account_id=provisional.account_id,
        session_date=provisional.session_date,
        day_start_equity=provisional.day_start_equity,
        peak_equity=provisional.peak_equity,
        cumulative_turnover=provisional.cumulative_turnover,
        as_of=provisional.as_of,
        latest_observation_hash=provisional.latest_observation_hash,
        latest_snapshot_hash=provisional.latest_snapshot_hash,
        turnover_evidence_hash=provisional.turnover_evidence_hash,
        version=provisional.version,
        last_event_hash=event.event_hash,
    )
    return state, event


def observation_payload(observation: SessionRiskObservation) -> dict[str, object]:
    return {
        "account_id": observation.account_id,
        "as_of": observation.as_of.isoformat(timespec="microseconds"),
        "cumulative_turnover": _decimal_text(observation.cumulative_turnover),
        "equity": _decimal_text(observation.equity),
        "session_date": observation.session_date.isoformat(),
        "snapshot_hash": observation.snapshot_hash,
        "turnover_evidence_hash": observation.turnover_evidence_hash,
        "version": SESSION_RISK_VERSION,
    }


def state_payload(state: PaperSessionRiskState) -> dict[str, object]:
    return {
        "account_id": state.account_id,
        "as_of": state.as_of.isoformat(timespec="microseconds"),
        "cumulative_turnover": _decimal_text(state.cumulative_turnover),
        "day_start_equity": _decimal_text(state.day_start_equity),
        "last_event_hash": state.last_event_hash,
        "latest_observation_hash": state.latest_observation_hash,
        "latest_snapshot_hash": state.latest_snapshot_hash,
        "peak_equity": _decimal_text(state.peak_equity),
        "session_date": state.session_date.isoformat(),
        "turnover_evidence_hash": state.turnover_evidence_hash,
        "version": state.version,
    }
