from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from threading import Lock

from autoquant.clock import to_utc
from autoquant.data.models import _canonical_hash, _require_nonblank
from autoquant.errors import QuoteStreamUnavailableError
from autoquant.risk.models import MarketQuote


@dataclass(frozen=True, slots=True)
class QuoteObservation:
    source: str
    source_sequence: int
    received_at: datetime
    quote: MarketQuote
    observation_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(self.source, name="quote source")
        if (
            not isinstance(self.source_sequence, int)
            or isinstance(self.source_sequence, bool)
            or self.source_sequence < 1
        ):
            raise ValueError("quote source_sequence must be a positive integer")
        received_at = to_utc(self.received_at, name="quote received_at")
        if self.quote.as_of > received_at:
            raise ValueError("quote event time cannot follow its receipt time")
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(
            self,
            "observation_hash",
            _canonical_hash(
                {
                    "quote_hash": self.quote.quote_hash,
                    "received_at": received_at.isoformat(timespec="microseconds"),
                    "source": self.source,
                    "source_sequence": self.source_sequence,
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class QuoteBookSnapshot:
    source: str
    source_sequence: int
    reset_id: str
    as_of: datetime
    observations: tuple[QuoteObservation, ...]
    evidence_hash: str = field(init=False)

    def __post_init__(self) -> None:
        observations = tuple(sorted(self.observations, key=lambda item: item.quote.instrument))
        object.__setattr__(self, "observations", observations)
        if not observations:
            raise ValueError("quote snapshot cannot be empty")
        if len({item.quote.instrument for item in observations}) != len(observations):
            raise ValueError("quote snapshot instruments must be unique")
        object.__setattr__(self, "as_of", to_utc(self.as_of, name="quote snapshot as_of"))
        object.__setattr__(
            self,
            "evidence_hash",
            _canonical_hash(
                {
                    "as_of": self.as_of.isoformat(timespec="microseconds"),
                    "observation_hashes": [item.observation_hash for item in observations],
                    "reset_id": self.reset_id,
                    "source": self.source,
                    "source_sequence": self.source_sequence,
                }
            ),
        )

    @property
    def quotes(self) -> dict[str, MarketQuote]:
        return {item.quote.instrument: item.quote for item in self.observations}

    @property
    def marks(self) -> dict[str, Decimal]:
        return {item.quote.instrument: item.quote.last_price for item in self.observations}


class ContinuousQuoteBook:
    """Thread-safe latest-value book with explicit baseline and sequence fencing."""

    def __init__(self, *, source: str) -> None:
        _require_nonblank(source, name="quote source")
        self._source = source
        self._lock = Lock()
        self._connected = False
        self._healthy = False
        self._reset_id: str | None = None
        self._source_sequence = 0
        self._observations: dict[str, QuoteObservation] = {}
        self._last_observation: QuoteObservation | None = None
        self._disconnect_reason = "not_initialized"

    @property
    def connected(self) -> bool:
        with self._lock:
            return self._connected and self._healthy

    def reset(
        self,
        *,
        quotes: tuple[MarketQuote, ...],
        source_sequence: int,
        received_at: datetime,
        reset_id: str,
    ) -> None:
        _require_nonblank(reset_id, name="quote reset_id")
        if (
            not isinstance(source_sequence, int)
            or isinstance(source_sequence, bool)
            or source_sequence < 1
        ):
            raise ValueError("quote baseline sequence must be a positive integer")
        if not quotes:
            raise ValueError("quote baseline cannot be empty")
        if len({quote.instrument for quote in quotes}) != len(quotes):
            raise ValueError("quote baseline instruments must be unique")
        received = to_utc(received_at, name="quote baseline received_at")
        observations = {
            quote.instrument: QuoteObservation(
                source=self._source,
                source_sequence=source_sequence,
                received_at=received,
                quote=quote,
            )
            for quote in quotes
        }
        with self._lock:
            self._observations = observations
            self._source_sequence = source_sequence
            self._reset_id = reset_id
            self._last_observation = None
            self._disconnect_reason = ""
            self._connected = True
            self._healthy = True

    def publish(self, observation: QuoteObservation) -> None:
        if not isinstance(observation, QuoteObservation):
            raise TypeError("observation must be QuoteObservation")
        if observation.source != self._source:
            raise ValueError("quote observation belongs to another source")
        with self._lock:
            if not self._connected or not self._healthy or self._reset_id is None:
                raise QuoteStreamUnavailableError(
                    f"quote stream requires a new baseline: {self._disconnect_reason}"
                )
            if observation.source_sequence == self._source_sequence:
                if (
                    self._last_observation is not None
                    and observation.observation_hash == self._last_observation.observation_hash
                ):
                    return
                self._healthy = False
                self._disconnect_reason = "conflicting_duplicate"
                raise QuoteStreamUnavailableError(
                    "quote stream repeated a sequence with conflicting facts"
                )
            if observation.source_sequence != self._source_sequence + 1:
                self._healthy = False
                self._disconnect_reason = "sequence_gap"
                raise QuoteStreamUnavailableError("quote stream sequence gap detected")
            previous = self._observations.get(observation.quote.instrument)
            if previous is not None and (
                observation.quote.as_of < previous.quote.as_of
                or observation.received_at < previous.received_at
            ):
                self._healthy = False
                self._disconnect_reason = "time_regression"
                raise QuoteStreamUnavailableError("quote stream time moved backwards")
            self._observations[observation.quote.instrument] = observation
            self._source_sequence = observation.source_sequence
            self._last_observation = observation

    def disconnect(self, *, reason: str) -> None:
        _require_nonblank(reason, name="quote disconnect reason")
        with self._lock:
            self._connected = False
            self._healthy = False
            self._disconnect_reason = reason

    def snapshot(
        self,
        *,
        instruments: tuple[str, ...],
        now: datetime,
        max_age: timedelta,
        require_market_open: bool,
    ) -> QuoteBookSnapshot:
        requested = tuple(sorted(instruments))
        if not requested or len(set(requested)) != len(requested):
            raise ValueError("snapshot instruments must be nonempty and unique")
        if any(not instrument.strip() for instrument in requested):
            raise ValueError("snapshot instruments cannot be blank")
        if max_age <= timedelta(0):
            raise ValueError("max_age must be positive")
        instant = to_utc(now, name="quote snapshot time")
        with self._lock:
            if not self._connected or not self._healthy or self._reset_id is None:
                raise QuoteStreamUnavailableError(
                    f"quote stream is unavailable: {self._disconnect_reason}"
                )
            observations: list[QuoteObservation] = []
            for instrument in requested:
                observation = self._observations.get(instrument)
                if observation is None:
                    raise QuoteStreamUnavailableError(
                        f"quote stream omitted configured instrument: {instrument}"
                    )
                if (
                    observation.received_at > instant
                    or observation.quote.as_of > instant
                    or instant - observation.quote.as_of > max_age
                ):
                    raise QuoteStreamUnavailableError(
                        f"quote is stale or from the future: {instrument}"
                    )
                if require_market_open and not observation.quote.market_open:
                    raise QuoteStreamUnavailableError(
                        f"quote reports a closed market: {instrument}"
                    )
                observations.append(observation)
            return QuoteBookSnapshot(
                source=self._source,
                source_sequence=self._source_sequence,
                reset_id=self._reset_id,
                as_of=instant,
                observations=tuple(observations),
            )
