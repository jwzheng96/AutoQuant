from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol
from uuid import UUID

from autoquant.backtest.models import InstrumentRules
from autoquant.backtest.validation import SmaParameters
from autoquant.clock import to_utc
from autoquant.data.daily_ingestion import ValidatedDailyDataset
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
from autoquant.risk.models import RiskPolicy

PARAMETER_SELECTION_VERSION = "modal-training-selections-v1"
SIGNAL_POLICY_VERSION = "prior-close-sma-target-v1"
MAX_SIGNAL_BAR_LAG_DAYS = 4


class DailyDatasetReader(Protocol):
    async def query(
        self,
        manifest_hash: str,
        as_of: datetime,
    ) -> ValidatedDailyDataset: ...


class PaperStrategyRegistry(Protocol):
    async def active(
        self,
        *,
        account_id: str,
        strategy_id: str,
    ) -> ValidatedSmaRegistration | None: ...


class SessionRuleReader(Protocol):
    async def read(
        self,
        *,
        instruments: tuple[str, ...],
        session_date: date,
        as_of: datetime,
    ) -> SessionRuleSet: ...


@dataclass(frozen=True, slots=True)
class ValidatedSmaRegistration:
    account_id: str
    strategy_id: str
    strategy_version: str
    experiment_id: UUID
    validation_result_hash: str
    validation_manifest_hash: str
    signal_manifest_hash: str
    signal_manifest_as_of: datetime
    instrument: str
    fast_sessions: int
    slow_sessions: int
    allocation: Decimal
    slippage_bps: Decimal
    risk_policy_hash: str
    rule_version: str
    approved_by: str
    approved_at: datetime
    execution_mode: str = "paper"
    parameter_selection_version: str = PARAMETER_SELECTION_VERSION
    signal_policy_version: str = SIGNAL_POLICY_VERSION
    registration_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for name, value in (
            ("account_id", self.account_id),
            ("strategy_id", self.strategy_id),
            ("strategy_version", self.strategy_version),
            ("instrument", self.instrument),
            ("rule_version", self.rule_version),
            ("approved_by", self.approved_by),
            ("parameter_selection_version", self.parameter_selection_version),
            ("signal_policy_version", self.signal_policy_version),
        ):
            _require_nonblank(value, name=name)
            if len(value) > 128:
                raise ValueError(f"{name} cannot exceed 128 characters")
        if self.execution_mode != "paper":
            raise ValueError("validated SMA registration is paper-only")
        for name, value in (
            ("validation_result_hash", self.validation_result_hash),
            ("validation_manifest_hash", self.validation_manifest_hash),
            ("signal_manifest_hash", self.signal_manifest_hash),
            ("risk_policy_hash", self.risk_policy_hash),
        ):
            _require_lowercase_sha256(value, name=name)
        if (
            not isinstance(self.fast_sessions, int)
            or isinstance(self.fast_sessions, bool)
            or not isinstance(self.slow_sessions, int)
            or isinstance(self.slow_sessions, bool)
            or self.fast_sessions < 2
            or self.slow_sessions <= self.fast_sessions
        ):
            raise ValueError("validated SMA windows are invalid")
        for decimal_name, decimal_value in (
            ("allocation", self.allocation),
            ("slippage_bps", self.slippage_bps),
        ):
            if (
                not isinstance(decimal_value, Decimal)
                or not decimal_value.is_finite()
            ):
                raise ValueError(f"{decimal_name} must be a finite Decimal")
        if not Decimal("0") < self.allocation <= Decimal("1"):
            raise ValueError("allocation must be between zero and one")
        if not Decimal("0") <= self.slippage_bps <= Decimal("100"):
            raise ValueError("slippage_bps must be between zero and 100")
        signal_manifest_as_of = to_utc(
            self.signal_manifest_as_of,
            name="signal_manifest_as_of",
        )
        approved_at = to_utc(self.approved_at, name="approved_at")
        if approved_at < signal_manifest_as_of:
            raise ValueError("approval cannot precede signal manifest evidence")
        object.__setattr__(self, "signal_manifest_as_of", signal_manifest_as_of)
        object.__setattr__(self, "approved_at", approved_at)
        object.__setattr__(
            self,
            "registration_hash",
            _canonical_hash(self.artifact_payload()),
        )

    def artifact_payload(self) -> dict[str, object]:
        return {
            "account_id": self.account_id,
            "allocation": _decimal_text(self.allocation),
            "approved_at": _datetime_text(self.approved_at),
            "approved_by": self.approved_by,
            "execution_mode": self.execution_mode,
            "experiment_id": str(self.experiment_id),
            "fast_sessions": self.fast_sessions,
            "instrument": self.instrument,
            "parameter_selection_version": self.parameter_selection_version,
            "risk_policy_hash": self.risk_policy_hash,
            "rule_version": self.rule_version,
            "signal_manifest_as_of": _datetime_text(self.signal_manifest_as_of),
            "signal_manifest_hash": self.signal_manifest_hash,
            "signal_policy_version": self.signal_policy_version,
            "slippage_bps": _decimal_text(self.slippage_bps),
            "slow_sessions": self.slow_sessions,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "validation_manifest_hash": self.validation_manifest_hash,
            "validation_result_hash": self.validation_result_hash,
        }

    @property
    def instruments(self) -> tuple[str, ...]:
        return (self.instrument,)

    @property
    def valuation_manifest_hash(self) -> str:
        return self.signal_manifest_hash

    @property
    def valuation_manifest_as_of(self) -> datetime:
        return self.signal_manifest_as_of

    @property
    def total_allocation(self) -> Decimal:
        return self.allocation

    @property
    def maximum_slippage_bps(self) -> Decimal:
        return self.slippage_bps


def select_deployment_parameters(
    selections: tuple[SmaParameters, ...],
) -> SmaParameters:
    """Select only from training-fold choices; OOS scores never tune deployment."""

    if not selections or any(
        not isinstance(value, SmaParameters) for value in selections
    ):
        raise ValueError("deployment selection requires training-fold parameters")
    counts = Counter(
        (value.fast_sessions, value.slow_sessions) for value in selections
    )
    pair = min(
        counts,
        key=lambda value: (-counts[value], value[0], value[1]),
    )
    return SmaParameters(*pair)


class ValidatedSmaTargetProvider:
    """Create a paper target from one approved OOS result and one exact data manifest."""

    def __init__(
        self,
        *,
        strategy_id: str,
        registry: PaperStrategyRegistry,
        control_repository: ControlRepository,
        dataset_reader: DailyDatasetReader,
        session_rule_reader: SessionRuleReader,
        policy: RiskPolicy,
    ) -> None:
        _require_nonblank(strategy_id, name="validated SMA strategy_id")
        if len(policy.allowed_instruments) != 1:
            raise ValueError("validated SMA provider requires one policy instrument")
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
            raise ValueError("paper strategy has no active approved registration")
        rule_set = await self._session_rules.read(
            instruments=(registration.instrument,),
            session_date=context.session_date,
            as_of=context.now,
        )
        if (
            rule_set.session_date != context.session_date
            or rule_set.as_of != context.now
            or len(rule_set.rules) != 1
            or rule_set.rules[0].instrument != registration.instrument
            or registration.instrument in rule_set.suspended_instruments
        ):
            raise ValueError("current session rules do not permit strategy evaluation")
        rules = rule_set.rules[0]
        self._validate_registration(
            registration=registration,
            context=context,
            rules=rules,
        )
        manifest = await self._control.read_manifest(
            registration.signal_manifest_hash
        )
        self._validate_manifest(manifest=manifest, registration=registration)
        dataset = await self._reader.query(
            manifest.manifest_hash,
            manifest.as_of,
        )
        bars = tuple(
            sorted(
                dataset.bars,
                key=lambda value: (value.session_date, value.instrument),
            )
        )
        if (
            len(bars) < registration.slow_sessions
            or any(value.instrument != registration.instrument for value in bars)
            or len({value.session_date for value in bars}) != len(bars)
        ):
            raise ValueError("signal manifest does not contain a valid SMA history")
        if any(value.session_date >= context.session_date for value in bars):
            raise ValueError("signal manifest contains current or future session bars")
        latest = bars[-1]
        lag_days = (context.session_date - latest.session_date).days
        if lag_days < 1 or lag_days > MAX_SIGNAL_BAR_LAG_DAYS:
            raise ValueError("signal manifest is stale for this paper session")
        factors = tuple(
            sorted(
                dataset.factors,
                key=lambda value: (value.session_date, value.instrument),
            )
        )
        if (
            len(factors) != len(bars)
            or any(value.instrument != registration.instrument for value in factors)
            or tuple(value.session_date for value in factors)
            != tuple(value.session_date for value in bars)
            or len({value.factor for value in factors}) != 1
        ):
            raise ValueError(
                "signal history requires unchanged, date-aligned adjustment factors"
            )
        fast_bars = bars[-registration.fast_sessions :]
        slow_bars = bars[-registration.slow_sessions :]
        fast_average = sum(
            (value.close_price for value in fast_bars),
            Decimal("0"),
        ) / registration.fast_sessions
        slow_average = sum(
            (value.close_price for value in slow_bars),
            Decimal("0"),
        ) / registration.slow_sessions
        quote = context.quotes.get(registration.instrument)
        if quote is None:
            raise ValueError("paper quote universe does not contain registered instrument")
        target_quantity = 0
        if fast_average > slow_average:
            estimated_price = quote.ask_price * (
                Decimal("1")
                + registration.slippage_bps / Decimal("10000")
                + self._policy.fee_buffer_rate
            )
            target_quantity = min(
                int(
                    (
                        context.account_evidence.account.equity
                        * registration.allocation
                    )
                    // estimated_price
                ),
                rules.max_order_quantity,
            )
        evidence_hash = _canonical_hash(
            {
                "account_state_hash": context.account_evidence.account.state_hash,
                "bar_hashes": [value.content_hash for value in slow_bars],
                "evaluated_at": _datetime_text(context.now),
                "factor_hashes": [value.content_hash for value in factors[-len(slow_bars) :]],
                "fast_average": _decimal_text(fast_average),
                "last_session_date": latest.session_date.isoformat(),
                "quote_evidence_hash": context.quote_snapshot.evidence_hash,
                "registration_hash": registration.registration_hash,
                "rule_set_hash": rule_set.rule_set_hash,
                "signal_manifest_hash": manifest.manifest_hash,
                "signal_policy_version": registration.signal_policy_version,
                "slow_average": _decimal_text(slow_average),
                "target_quantity": target_quantity,
            }
        )
        return TargetPortfolioSignal(
            strategy_id=registration.strategy_id,
            strategy_version=registration.strategy_version,
            session_date=context.session_date,
            evaluated_at=context.now,
            targets=(
                TargetInstrumentPosition(
                    instrument=registration.instrument,
                    target_quantity=target_quantity,
                    rules=rules,
                    policy=self._policy,
                ),
            ),
            source_evidence_hash=evidence_hash,
        )

    def _validate_registration(
        self,
        *,
        registration: ValidatedSmaRegistration,
        context: PaperStrategyContext,
        rules: InstrumentRules,
    ) -> None:
        if (
            not isinstance(registration, ValidatedSmaRegistration)
            or registration.execution_mode != "paper"
            or registration.account_id != context.account_id
            or registration.strategy_id != self._strategy_id
            or registration.instrument != rules.instrument
            or registration.instrument != self._policy.allowed_instruments[0]
            or registration.rule_version != rules.rule_version
            or registration.risk_policy_hash != self._policy.policy_hash
        ):
            raise ValueError("active paper registration does not match runtime controls")
        if registration.allocation > min(
            self._policy.max_position_weight,
            self._policy.max_gross_exposure,
        ):
            raise ValueError("registered allocation exceeds the runtime risk policy")
        if registration.approved_at > context.now:
            raise ValueError("paper registration is not yet effective")

    @staticmethod
    def _validate_manifest(
        *,
        manifest: DatasetManifest,
        registration: ValidatedSmaRegistration,
    ) -> None:
        if (
            manifest.manifest_hash != registration.signal_manifest_hash
            or manifest.as_of != registration.signal_manifest_as_of
            or not manifest.production_complete
            or manifest.instruments != (registration.instrument,)
        ):
            raise ValueError("signal manifest does not match the approved registration")
