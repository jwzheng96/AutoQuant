from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol

from autoquant.backtest.models import InstrumentRules
from autoquant.data.ingestion import ControlRepository
from autoquant.data.models import (
    DatasetManifest,
    _canonical_hash,
    _datetime_text,
    _decimal_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.execution.paper_scheduler import PaperStrategyContext
from autoquant.execution.session_rules import SessionRuleSet
from autoquant.execution.target_strategy import (
    TargetInstrumentPosition,
    TargetPortfolioSignal,
)
from autoquant.execution.validated_sma import (
    MAX_SIGNAL_BAR_LAG_DAYS,
    DailyDatasetReader,
    SessionRuleReader,
    ValidatedSmaRegistration,
)
from autoquant.risk.models import RiskPolicy

PORTFOLIO_VERSION = "validated-sma-portfolio-v1"


class PaperPortfolioRegistry(Protocol):
    async def active(
        self,
        *,
        account_id: str,
        strategy_id: str,
    ) -> ValidatedSmaPortfolioRegistration | None: ...


@dataclass(frozen=True, slots=True)
class ValidatedSmaPortfolioRegistration:
    account_id: str
    strategy_id: str
    strategy_version: str
    components: tuple[ValidatedSmaRegistration, ...]
    valuation_manifest_hash: str
    valuation_manifest_as_of: datetime
    risk_policy_hash: str
    approved_by: str
    approved_at: datetime
    execution_mode: str = "paper"
    portfolio_version: str = PORTFOLIO_VERSION
    registration_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for name, value in (
            ("account_id", self.account_id),
            ("strategy_id", self.strategy_id),
            ("strategy_version", self.strategy_version),
            ("approved_by", self.approved_by),
            ("portfolio_version", self.portfolio_version),
        ):
            _require_nonblank(value, name=name)
            if len(value) > 128:
                raise ValueError(f"{name} cannot exceed 128 characters")
        if self.execution_mode != "paper":
            raise ValueError("validated SMA portfolio is paper-only")
        for name, value in (
            ("valuation_manifest_hash", self.valuation_manifest_hash),
            ("risk_policy_hash", self.risk_policy_hash),
        ):
            _require_lowercase_sha256(value, name=name)
        components = tuple(
            sorted(self.components, key=lambda value: value.instrument)
        )
        if len(components) < 3 or len(components) > 20:
            raise ValueError(
                "validated SMA portfolio requires 3-20 components"
            )
        instruments = tuple(value.instrument for value in components)
        if len(set(instruments)) != len(instruments):
            raise ValueError("portfolio component instruments must be unique")
        if any(
            not isinstance(value, ValidatedSmaRegistration)
            or value.account_id != self.account_id
            or value.strategy_id != self.strategy_id
            or value.risk_policy_hash != self.risk_policy_hash
            or value.approved_by != self.approved_by
            or value.approved_at != self.approved_at
            or value.execution_mode != "paper"
            for value in components
        ):
            raise ValueError(
                "portfolio components do not share deployment controls"
            )
        total_allocation = sum(
            (value.allocation for value in components),
            Decimal("0"),
        )
        if total_allocation > Decimal("1"):
            raise ValueError("portfolio allocation cannot exceed one")
        valuation_as_of = _aware_utc(
            self.valuation_manifest_as_of,
            name="portfolio valuation manifest time",
        )
        approved_at = _aware_utc(
            self.approved_at,
            name="portfolio approval time",
        )
        if approved_at < valuation_as_of or any(
            approved_at < value.signal_manifest_as_of
            for value in components
        ):
            raise ValueError(
                "portfolio approval cannot precede deployment evidence"
            )
        object.__setattr__(self, "components", components)
        object.__setattr__(
            self,
            "valuation_manifest_as_of",
            valuation_as_of,
        )
        object.__setattr__(self, "approved_at", approved_at)
        object.__setattr__(
            self,
            "registration_hash",
            _canonical_hash(self.artifact_payload()),
        )

    @property
    def instruments(self) -> tuple[str, ...]:
        return tuple(value.instrument for value in self.components)

    @property
    def total_allocation(self) -> Decimal:
        return sum(
            (value.allocation for value in self.components),
            Decimal("0"),
        )

    @property
    def maximum_slippage_bps(self) -> Decimal:
        return max(value.slippage_bps for value in self.components)

    def artifact_payload(self) -> dict[str, object]:
        return {
            "account_id": self.account_id,
            "approved_at": _datetime_text(self.approved_at),
            "approved_by": self.approved_by,
            "component_hashes": [
                value.registration_hash for value in self.components
            ],
            "execution_mode": self.execution_mode,
            "portfolio_version": self.portfolio_version,
            "risk_policy_hash": self.risk_policy_hash,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "total_allocation": _decimal_text(self.total_allocation),
            "valuation_manifest_as_of": _datetime_text(
                self.valuation_manifest_as_of
            ),
            "valuation_manifest_hash": self.valuation_manifest_hash,
        }


class ValidatedSmaPortfolioTargetProvider:
    """Evaluate every independently approved component as one bounded target."""

    def __init__(
        self,
        *,
        strategy_id: str,
        registry: PaperPortfolioRegistry,
        control_repository: ControlRepository,
        dataset_reader: DailyDatasetReader,
        session_rule_reader: SessionRuleReader,
        policy: RiskPolicy,
    ) -> None:
        _require_nonblank(strategy_id, name="portfolio strategy_id")
        if len(policy.allowed_instruments) < 3:
            raise ValueError(
                "portfolio provider requires at least three instruments"
            )
        self._strategy_id = strategy_id
        self._registry = registry
        self._control = control_repository
        self._reader = dataset_reader
        self._session_rules = session_rule_reader
        self._policy = policy

    async def target(
        self,
        context: PaperStrategyContext,
    ) -> TargetPortfolioSignal:
        registration = await self._registry.active(
            account_id=context.account_id,
            strategy_id=self._strategy_id,
        )
        if registration is None:
            raise ValueError("paper portfolio has no active registration")
        self._validate_registration(
            registration=registration,
            context=context,
        )
        rule_set = await self._session_rules.read(
            instruments=registration.instruments,
            session_date=context.session_date,
            as_of=context.now,
        )
        rules = self._validated_rules(
            registration=registration,
            context=context,
            rule_set=rule_set,
        )
        targets: list[TargetInstrumentPosition] = []
        component_evidence: list[dict[str, object]] = []
        held = {
            value.instrument: value.total_quantity
            for value in context.account_evidence.account.positions
        }
        for component in registration.components:
            instrument = component.instrument
            if instrument in rule_set.suspended_instruments:
                target_quantity = held.get(instrument, 0)
                component_evidence.append(
                    {
                        "component_hash": component.registration_hash,
                        "instrument": instrument,
                        "state": "suspended_hold",
                        "target_quantity": target_quantity,
                    }
                )
            else:
                (
                    target_quantity,
                    signal_evidence,
                ) = await self._component_target(
                    component=component,
                    context=context,
                    rules=rules[instrument],
                )
                component_evidence.append(signal_evidence)
            targets.append(
                TargetInstrumentPosition(
                    instrument=instrument,
                    target_quantity=target_quantity,
                    rules=rules[instrument],
                    policy=self._policy,
                )
            )
        source_evidence_hash = _canonical_hash(
            {
                "account_state_hash": (
                    context.account_evidence.account.state_hash
                ),
                "component_evidence": component_evidence,
                "evaluated_at": _datetime_text(context.now),
                "portfolio_registration_hash": (
                    registration.registration_hash
                ),
                "quote_evidence_hash": (
                    context.quote_snapshot.evidence_hash
                ),
                "rule_set_hash": rule_set.rule_set_hash,
                "version": "validated-sma-portfolio-signal-v1",
            }
        )
        return TargetPortfolioSignal(
            strategy_id=registration.strategy_id,
            strategy_version=registration.strategy_version,
            session_date=context.session_date,
            evaluated_at=context.now,
            targets=tuple(targets),
            source_evidence_hash=source_evidence_hash,
        )

    def _validate_registration(
        self,
        *,
        registration: ValidatedSmaPortfolioRegistration,
        context: PaperStrategyContext,
    ) -> None:
        if (
            not isinstance(
                registration,
                ValidatedSmaPortfolioRegistration,
            )
            or registration.execution_mode != "paper"
            or registration.account_id != context.account_id
            or registration.strategy_id != self._strategy_id
            or registration.instruments != self._policy.allowed_instruments
            or registration.risk_policy_hash != self._policy.policy_hash
            or registration.total_allocation
            > self._policy.max_gross_exposure
            or any(
                value.allocation > self._policy.max_position_weight
                for value in registration.components
            )
            or registration.approved_at > context.now
        ):
            raise ValueError(
                "active portfolio registration does not match runtime controls"
            )

    @staticmethod
    def _validated_rules(
        *,
        registration: ValidatedSmaPortfolioRegistration,
        context: PaperStrategyContext,
        rule_set: SessionRuleSet,
    ) -> dict[str, InstrumentRules]:
        rules = {value.instrument: value for value in rule_set.rules}
        if (
            rule_set.session_date != context.session_date
            or rule_set.as_of != context.now
            or set(rules) != set(registration.instruments)
            or any(
                rules[value.instrument].rule_version
                != value.rule_version
                for value in registration.components
            )
        ):
            raise ValueError(
                "current session rules do not match portfolio components"
            )
        return rules

    async def _component_target(
        self,
        *,
        component: ValidatedSmaRegistration,
        context: PaperStrategyContext,
        rules: InstrumentRules,
    ) -> tuple[int, dict[str, object]]:
        manifest = await self._control.read_manifest(
            component.signal_manifest_hash
        )
        self._validate_manifest(
            manifest=manifest,
            component=component,
        )
        dataset = await self._reader.query(
            manifest.manifest_hash,
            manifest.as_of,
        )
        bars = tuple(
            sorted(
                dataset.bars,
                key=lambda value: value.session_date,
            )
        )
        factors = tuple(
            sorted(
                dataset.factors,
                key=lambda value: value.session_date,
            )
        )
        if (
            len(bars) < component.slow_sessions
            or len(bars) != len(dataset.bars)
            or any(
                value.instrument != component.instrument
                for value in bars
            )
            or len({value.session_date for value in bars}) != len(bars)
            or any(
                value.session_date >= context.session_date
                for value in bars
            )
            or len(factors) != len(bars)
            or any(
                value.instrument != component.instrument
                for value in factors
            )
            or tuple(value.session_date for value in factors)
            != tuple(value.session_date for value in bars)
            or len({value.factor for value in factors}) != 1
        ):
            raise ValueError(
                "component signal manifest lacks exact SMA history"
            )
        latest = bars[-1]
        lag_days = (context.session_date - latest.session_date).days
        if lag_days < 1 or lag_days > MAX_SIGNAL_BAR_LAG_DAYS:
            raise ValueError("component signal manifest is stale")
        slow_bars = bars[-component.slow_sessions :]
        fast_bars = bars[-component.fast_sessions :]
        fast_average = sum(
            (value.close_price for value in fast_bars),
            Decimal("0"),
        ) / component.fast_sessions
        slow_average = sum(
            (value.close_price for value in slow_bars),
            Decimal("0"),
        ) / component.slow_sessions
        quote = context.quotes.get(component.instrument)
        if quote is None:
            raise ValueError(
                "quote universe does not contain portfolio component"
            )
        target_quantity = 0
        if fast_average > slow_average:
            estimated_price = quote.ask_price * (
                Decimal("1")
                + component.slippage_bps / Decimal("10000")
                + self._policy.fee_buffer_rate
            )
            target_quantity = min(
                int(
                    (
                        context.account_evidence.account.equity
                        * component.allocation
                    )
                    // estimated_price
                ),
                rules.max_order_quantity,
            )
        return target_quantity, {
            "bar_hashes": [
                value.content_hash for value in slow_bars
            ],
            "component_hash": component.registration_hash,
            "factor_hashes": [
                value.content_hash
                for value in factors[-len(slow_bars) :]
            ],
            "fast_average": _decimal_text(fast_average),
            "instrument": component.instrument,
            "last_session_date": latest.session_date.isoformat(),
            "signal_manifest_hash": manifest.manifest_hash,
            "slow_average": _decimal_text(slow_average),
            "state": "evaluated",
            "target_quantity": target_quantity,
        }

    @staticmethod
    def _validate_manifest(
        *,
        manifest: DatasetManifest,
        component: ValidatedSmaRegistration,
    ) -> None:
        if (
            manifest.manifest_hash != component.signal_manifest_hash
            or manifest.as_of != component.signal_manifest_as_of
            or not manifest.production_complete
            or manifest.instruments != (component.instrument,)
        ):
            raise ValueError(
                "signal manifest does not match portfolio component"
            )


def _aware_utc(value: datetime, *, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)
