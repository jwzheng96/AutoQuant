from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol

from autoquant.backtest.models import InstrumentRules, OrderSide
from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.execution.paper_scheduler import (
    PaperStrategyContext,
    PaperStrategyEvaluation,
    PaperStrategyIntent,
)
from autoquant.risk.models import ProposedOrder, RiskPolicy


@dataclass(frozen=True, slots=True)
class TargetInstrumentPosition:
    instrument: str
    target_quantity: int
    rules: InstrumentRules
    policy: RiskPolicy

    def __post_init__(self) -> None:
        _require_nonblank(self.instrument, name="target instrument")
        if (
            not isinstance(self.target_quantity, int)
            or isinstance(self.target_quantity, bool)
            or self.target_quantity < 0
        ):
            raise ValueError("target quantity must be a nonnegative integer")
        if (
            self.rules.instrument != self.instrument
            or self.instrument not in self.policy.allowed_instruments
        ):
            raise ValueError("target position is not covered by rules and policy")


@dataclass(frozen=True, slots=True)
class TargetPortfolioSignal:
    strategy_id: str
    strategy_version: str
    session_date: date
    evaluated_at: datetime
    targets: tuple[TargetInstrumentPosition, ...]
    source_evidence_hash: str
    signal_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(self.strategy_id, name="target strategy_id")
        _require_nonblank(self.strategy_version, name="target strategy_version")
        evaluated_at = to_utc(self.evaluated_at, name="target evaluated_at")
        _require_lowercase_sha256(
            self.source_evidence_hash,
            name="target source_evidence_hash",
        )
        targets = tuple(sorted(self.targets, key=lambda value: value.instrument))
        if not targets or any(
            not isinstance(value, TargetInstrumentPosition) for value in targets
        ):
            raise ValueError("target portfolio must contain target positions")
        instruments = tuple(value.instrument for value in targets)
        if len(set(instruments)) != len(instruments):
            raise ValueError("target portfolio instruments must be unique")
        if any(value.rules.effective_from > self.session_date for value in targets):
            raise ValueError("target portfolio contains rules that are not effective")
        object.__setattr__(self, "evaluated_at", evaluated_at)
        object.__setattr__(self, "targets", targets)
        object.__setattr__(
            self,
            "signal_hash",
            _canonical_hash(
                {
                    "evaluated_at": evaluated_at.isoformat(timespec="microseconds"),
                    "session_date": self.session_date.isoformat(),
                    "source_evidence_hash": self.source_evidence_hash,
                    "strategy_id": self.strategy_id,
                    "strategy_version": self.strategy_version,
                    "targets": [
                        {
                            "instrument": value.instrument,
                            "policy_hash": value.policy.policy_hash,
                            "rule_version": value.rules.rule_version,
                            "target_quantity": value.target_quantity,
                        }
                        for value in targets
                    ],
                }
            ),
        )


class TargetPortfolioProvider(Protocol):
    async def target(
        self,
        context: PaperStrategyContext,
    ) -> TargetPortfolioSignal: ...


class TargetPositionPaperIntentSource:
    """Convert audited targets into bounded, account-aware, idempotent paper intents."""

    def __init__(
        self,
        *,
        strategy_id: str,
        provider: TargetPortfolioProvider,
    ) -> None:
        _require_nonblank(strategy_id, name="target intent strategy_id")
        self._strategy_id = strategy_id
        self._provider = provider

    async def evaluate(
        self,
        context: PaperStrategyContext,
    ) -> PaperStrategyEvaluation:
        if not isinstance(context, PaperStrategyContext):
            raise TypeError("target intent source requires PaperStrategyContext")
        signal = await self._provider.target(context)
        self._validate_signal(signal=signal, context=context)
        current = {
            value.instrument: value
            for value in context.account_evidence.account.positions
        }
        intents: list[PaperStrategyIntent] = []
        for target in signal.targets:
            position = current.get(target.instrument)
            current_quantity = 0 if position is None else position.total_quantity
            delta = target.target_quantity - current_quantity
            if delta == 0:
                continue
            if delta > 0:
                quantity = _bounded_buy_quantity(
                    delta=delta,
                    rules=target.rules,
                    policy=target.policy,
                    reference_price=context.quotes[target.instrument].ask_price,
                )
                side = OrderSide.BUY
            else:
                sellable = 0 if position is None else position.sellable_quantity
                quantity = _bounded_sell_quantity(
                    requested=-delta,
                    sellable=sellable,
                    rules=target.rules,
                    policy=target.policy,
                    reference_price=context.quotes[target.instrument].bid_price,
                )
                side = OrderSide.SELL
            if quantity == 0:
                continue
            intents.append(
                PaperStrategyIntent(
                    order=ProposedOrder(
                        client_order_id=_client_order_id(
                            signal_hash=signal.signal_hash,
                            account_hash=context.account_evidence.evidence_hash,
                            instrument=target.instrument,
                            side=side,
                            session_date=context.session_date,
                        ),
                        instrument=target.instrument,
                        side=side,
                        quantity=quantity,
                        submitted_at=context.now,
                    ),
                    rules=target.rules,
                    policy=target.policy,
                )
            )
        return PaperStrategyEvaluation(
            strategy_id=signal.strategy_id,
            strategy_version=signal.strategy_version,
            evaluated_at=context.now,
            quote_evidence_hash=context.quote_snapshot.evidence_hash,
            account_evidence_hash=context.account_evidence.evidence_hash,
            signal_evidence_hash=signal.signal_hash,
            intents=tuple(intents),
        )

    def _validate_signal(
        self,
        *,
        signal: TargetPortfolioSignal,
        context: PaperStrategyContext,
    ) -> None:
        if not isinstance(signal, TargetPortfolioSignal):
            raise TypeError("target provider must return TargetPortfolioSignal")
        targets = {value.instrument for value in signal.targets}
        quote_universe = set(context.quotes)
        held = {
            value.instrument
            for value in context.account_evidence.account.positions
            if value.total_quantity > 0
        }
        if (
            signal.strategy_id != self._strategy_id
            or signal.session_date != context.session_date
            or signal.evaluated_at != context.now
            or targets != quote_universe
            or not held <= targets
        ):
            raise ValueError(
                "target signal does not match strategy, time, universe, or holdings"
            )


def _bounded_buy_quantity(
    *,
    delta: int,
    rules: InstrumentRules,
    policy: RiskPolicy,
    reference_price: Decimal,
) -> int:
    risk_maximum = int(policy.max_order_notional // reference_price)
    bounded = min(delta, rules.max_order_quantity, risk_maximum)
    if bounded < rules.buy_minimum:
        return 0
    return rules.buy_minimum + (
        (bounded - rules.buy_minimum) // rules.buy_step
    ) * rules.buy_step


def _bounded_sell_quantity(
    *,
    requested: int,
    sellable: int,
    rules: InstrumentRules,
    policy: RiskPolicy,
    reference_price: Decimal,
) -> int:
    risk_maximum = int(policy.max_order_notional // reference_price)
    bounded = min(
        requested,
        sellable,
        rules.max_order_quantity,
        risk_maximum,
    )
    return (bounded // rules.sell_step) * rules.sell_step


def _client_order_id(
    *,
    signal_hash: str,
    account_hash: str,
    instrument: str,
    side: OrderSide,
    session_date: date,
) -> str:
    digest = _canonical_hash(
        {
            "account_hash": account_hash,
            "instrument": instrument,
            "session_date": session_date.isoformat(),
            "side": side.value,
            "signal_hash": signal_hash,
        }
    )
    return f"target-{session_date:%Y%m%d}-{digest[:32]}"
