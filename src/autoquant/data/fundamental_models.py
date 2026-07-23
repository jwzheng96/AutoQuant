from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

from autoquant.clock import to_shanghai, to_utc
from autoquant.data.daily_models import TradingSession
from autoquant.data.models import (
    SourceEvidence,
    _canonical_hash,
    _datetime_text,
    _decimal_text,
    _require_lowercase_sha256,
    _require_nonblank,
)


def _finite_decimal(
    value: Decimal | None,
    *,
    name: str,
    positive: bool = False,
    nonnegative: bool = False,
) -> None:
    if value is None:
        return
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal or None")
    if positive and value <= 0:
        raise ValueError(f"{name} must be positive")
    if nonnegative and value < 0:
        raise ValueError(f"{name} cannot be negative")


def _decimal_payload(value: Decimal | None) -> str | None:
    return None if value is None else _decimal_text(value)


def _revision_times(
    *,
    source: str,
    instrument: str,
    event_time: datetime,
    available_at: datetime,
    ingested_at: datetime,
    source_revision: str,
    availability_policy: str,
    evidence_hash: str,
) -> tuple[datetime, datetime, datetime]:
    _require_nonblank(source, name="source")
    _require_nonblank(instrument, name="instrument")
    _require_nonblank(source_revision, name="source_revision")
    _require_nonblank(availability_policy, name="availability_policy")
    _require_lowercase_sha256(evidence_hash, name="evidence_hash")
    event = to_utc(event_time, name="event_time")
    available = to_utc(available_at, name="available_at")
    ingested = to_utc(ingested_at, name="ingested_at")
    if available < event:
        raise ValueError("available_at cannot precede event_time")
    if ingested < event:
        raise ValueError("ingested_at cannot precede event_time")
    return event, available, ingested


@dataclass(frozen=True, slots=True)
class DailyValuationRevision:
    source: str
    instrument: str
    session_date: date
    event_time: datetime
    available_at: datetime
    ingested_at: datetime
    source_revision: str
    availability_policy: str
    evidence_hash: str
    close_price: Decimal
    free_float_turnover_rate_percent: Decimal | None
    pe_ttm: Decimal | None
    pb: Decimal | None
    ps_ttm: Decimal | None
    dividend_yield_ttm_percent: Decimal | None
    total_market_value_cny: Decimal
    circulating_market_value_cny: Decimal
    content_hash: str = field(init=False)

    def __post_init__(self) -> None:
        event, available, ingested = _revision_times(
            source=self.source,
            instrument=self.instrument,
            event_time=self.event_time,
            available_at=self.available_at,
            ingested_at=self.ingested_at,
            source_revision=self.source_revision,
            availability_policy=self.availability_policy,
            evidence_hash=self.evidence_hash,
        )
        object.__setattr__(self, "event_time", event)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "ingested_at", ingested)
        if to_shanghai(event).date() != self.session_date:
            raise ValueError(
                "event_time must belong to session_date in Asia/Shanghai"
            )
        _finite_decimal(self.close_price, name="close_price", positive=True)
        _finite_decimal(
            self.free_float_turnover_rate_percent,
            name="free_float_turnover_rate_percent",
            nonnegative=True,
        )
        for name in ("pe_ttm", "pb", "ps_ttm"):
            _finite_decimal(getattr(self, name), name=name)
        _finite_decimal(
            self.dividend_yield_ttm_percent,
            name="dividend_yield_ttm_percent",
            nonnegative=True,
        )
        _finite_decimal(
            self.total_market_value_cny,
            name="total_market_value_cny",
            positive=True,
        )
        _finite_decimal(
            self.circulating_market_value_cny,
            name="circulating_market_value_cny",
            positive=True,
        )
        object.__setattr__(self, "content_hash", _canonical_hash(self.payload()))

    def payload(self) -> dict[str, object]:
        return {
            "availability_policy": self.availability_policy,
            "available_at": _datetime_text(self.available_at),
            "circulating_market_value_cny": _decimal_text(
                self.circulating_market_value_cny
            ),
            "close_price": _decimal_text(self.close_price),
            "dividend_yield_ttm_percent": _decimal_payload(
                self.dividend_yield_ttm_percent
            ),
            "event_time": _datetime_text(self.event_time),
            "evidence_hash": self.evidence_hash,
            "free_float_turnover_rate_percent": _decimal_payload(
                self.free_float_turnover_rate_percent
            ),
            "instrument": self.instrument,
            "pb": _decimal_payload(self.pb),
            "pe_ttm": _decimal_payload(self.pe_ttm),
            "ps_ttm": _decimal_payload(self.ps_ttm),
            "session_date": self.session_date.isoformat(),
            "source": self.source,
            "source_revision": self.source_revision,
            "total_market_value_cny": _decimal_text(
                self.total_market_value_cny
            ),
        }

    @classmethod
    def from_values(
        cls,
        *,
        source: str,
        instrument: str,
        session_date: date,
        event_time: datetime,
        available_at: datetime,
        ingested_at: datetime,
        source_revision: str,
        availability_policy: str,
        evidence_hash: str,
        close_price: Decimal | str,
        free_float_turnover_rate_percent: Decimal | str | None,
        pe_ttm: Decimal | str | None,
        pb: Decimal | str | None,
        ps_ttm: Decimal | str | None,
        dividend_yield_ttm_percent: Decimal | str | None,
        total_market_value_cny: Decimal | str,
        circulating_market_value_cny: Decimal | str,
    ) -> DailyValuationRevision:
        optional = (
            free_float_turnover_rate_percent,
            pe_ttm,
            pb,
            ps_ttm,
            dividend_yield_ttm_percent,
        )
        converted = tuple(
            None if value is None else Decimal(value) for value in optional
        )
        return cls(
            source=source,
            instrument=instrument,
            session_date=session_date,
            event_time=event_time,
            available_at=available_at,
            ingested_at=ingested_at,
            source_revision=source_revision,
            availability_policy=availability_policy,
            evidence_hash=evidence_hash,
            close_price=Decimal(close_price),
            free_float_turnover_rate_percent=converted[0],
            pe_ttm=converted[1],
            pb=converted[2],
            ps_ttm=converted[3],
            dividend_yield_ttm_percent=converted[4],
            total_market_value_cny=Decimal(total_market_value_cny),
            circulating_market_value_cny=Decimal(
                circulating_market_value_cny
            ),
        )


@dataclass(frozen=True, slots=True)
class FinancialIndicatorRevision:
    source: str
    instrument: str
    report_period: date
    announced_date: date
    updated: bool
    event_time: datetime
    available_at: datetime
    ingested_at: datetime
    source_revision: str
    availability_policy: str
    evidence_hash: str
    roe_diluted_percent: Decimal | None
    roa_percent: Decimal | None
    gross_profit_margin_percent: Decimal | None
    debt_to_assets_percent: Decimal | None
    operating_cashflow_to_revenue_percent: Decimal | None
    content_hash: str = field(init=False)

    def __post_init__(self) -> None:
        event, available, ingested = _revision_times(
            source=self.source,
            instrument=self.instrument,
            event_time=self.event_time,
            available_at=self.available_at,
            ingested_at=self.ingested_at,
            source_revision=self.source_revision,
            availability_policy=self.availability_policy,
            evidence_hash=self.evidence_hash,
        )
        object.__setattr__(self, "event_time", event)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "ingested_at", ingested)
        if self.announced_date < self.report_period:
            raise ValueError("announced_date cannot precede report_period")
        if to_shanghai(event).date() != self.announced_date:
            raise ValueError(
                "event_time must belong to announced_date in Asia/Shanghai"
            )
        if not isinstance(self.updated, bool):
            raise TypeError("updated must be a bool")
        values = (
            self.roe_diluted_percent,
            self.roa_percent,
            self.gross_profit_margin_percent,
            self.debt_to_assets_percent,
            self.operating_cashflow_to_revenue_percent,
        )
        for name, value in zip(
            (
                "roe_diluted_percent",
                "roa_percent",
                "gross_profit_margin_percent",
                "debt_to_assets_percent",
                "operating_cashflow_to_revenue_percent",
            ),
            values,
            strict=True,
        ):
            _finite_decimal(value, name=name)
        if all(value is None for value in values):
            raise ValueError("financial indicator revision cannot be empty")
        object.__setattr__(self, "content_hash", _canonical_hash(self.payload()))

    def payload(self) -> dict[str, object]:
        return {
            "announced_date": self.announced_date.isoformat(),
            "availability_policy": self.availability_policy,
            "available_at": _datetime_text(self.available_at),
            "debt_to_assets_percent": _decimal_payload(
                self.debt_to_assets_percent
            ),
            "event_time": _datetime_text(self.event_time),
            "evidence_hash": self.evidence_hash,
            "gross_profit_margin_percent": _decimal_payload(
                self.gross_profit_margin_percent
            ),
            "instrument": self.instrument,
            "operating_cashflow_to_revenue_percent": _decimal_payload(
                self.operating_cashflow_to_revenue_percent
            ),
            "report_period": self.report_period.isoformat(),
            "roa_percent": _decimal_payload(self.roa_percent),
            "roe_diluted_percent": _decimal_payload(
                self.roe_diluted_percent
            ),
            "source": self.source,
            "source_revision": self.source_revision,
            "updated": self.updated,
        }

    @classmethod
    def from_values(
        cls,
        *,
        source: str,
        instrument: str,
        report_period: date,
        announced_date: date,
        updated: bool,
        event_time: datetime,
        available_at: datetime,
        ingested_at: datetime,
        source_revision: str,
        availability_policy: str,
        evidence_hash: str,
        roe_diluted_percent: Decimal | str | None,
        roa_percent: Decimal | str | None,
        gross_profit_margin_percent: Decimal | str | None,
        debt_to_assets_percent: Decimal | str | None,
        operating_cashflow_to_revenue_percent: Decimal | str | None,
    ) -> FinancialIndicatorRevision:
        optional = (
            roe_diluted_percent,
            roa_percent,
            gross_profit_margin_percent,
            debt_to_assets_percent,
            operating_cashflow_to_revenue_percent,
        )
        values = tuple(
            None if value is None else Decimal(value) for value in optional
        )
        return cls(
            source=source,
            instrument=instrument,
            report_period=report_period,
            announced_date=announced_date,
            updated=updated,
            event_time=event_time,
            available_at=available_at,
            ingested_at=ingested_at,
            source_revision=source_revision,
            availability_policy=availability_policy,
            evidence_hash=evidence_hash,
            roe_diluted_percent=values[0],
            roa_percent=values[1],
            gross_profit_margin_percent=values[2],
            debt_to_assets_percent=values[3],
            operating_cashflow_to_revenue_percent=values[4],
        )


@dataclass(frozen=True, slots=True)
class FundamentalDatasetBatch:
    valuations: tuple[DailyValuationRevision, ...]
    indicators: tuple[FinancialIndicatorRevision, ...]
    sessions: tuple[TradingSession, ...]
    source_evidence: tuple[SourceEvidence, ...]

    def __post_init__(self) -> None:
        valuations = tuple(self.valuations)
        indicators = tuple(self.indicators)
        sessions = tuple(self.sessions)
        evidence = tuple(self.source_evidence)
        object.__setattr__(self, "valuations", valuations)
        object.__setattr__(self, "indicators", indicators)
        object.__setattr__(self, "sessions", sessions)
        object.__setattr__(self, "source_evidence", evidence)
        if any(
            not isinstance(value, DailyValuationRevision)
            for value in valuations
        ):
            raise TypeError(
                "valuations must contain DailyValuationRevision values"
            )
        if any(
            not isinstance(value, FinancialIndicatorRevision)
            for value in indicators
        ):
            raise TypeError(
                "indicators must contain FinancialIndicatorRevision values"
            )
        if not sessions or any(
            not isinstance(value, TradingSession) for value in sessions
        ):
            raise ValueError("sessions must contain TradingSession values")
        if not evidence or any(
            not isinstance(value, SourceEvidence) for value in evidence
        ):
            raise ValueError(
                "source_evidence must contain SourceEvidence values"
            )
        valuation_keys = {
            (value.source, value.instrument, value.session_date)
            for value in valuations
        }
        if len(valuation_keys) != len(valuations):
            raise ValueError("duplicate daily valuation revision")
        indicator_keys = {
            (
                value.source,
                value.instrument,
                value.report_period,
                value.announced_date,
                value.updated,
            )
            for value in indicators
        }
        if len(indicator_keys) != len(indicators):
            raise ValueError("duplicate financial indicator revision")
        evidence_keys = {
            (value.source, value.method, value.response_hash)
            for value in evidence
        }
        if any(
            (value.source, "daily_basic", value.evidence_hash)
            not in evidence_keys
            for value in valuations
        ):
            raise ValueError("daily_basic evidence does not back valuations")
        if any(
            (value.source, "fina_indicator", value.evidence_hash)
            not in evidence_keys
            for value in indicators
        ):
            raise ValueError(
                "fina_indicator evidence does not back indicators"
            )
        if any(
            (value.source, "trade_cal", value.response_hash)
            not in evidence_keys
            for value in sessions
        ):
            raise ValueError("trade_cal evidence does not back sessions")
