from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from autoquant.backtest.dynamic_portfolio import (
    DYNAMIC_PORTFOLIO_BENCHMARK_VERSION,
    DYNAMIC_PORTFOLIO_VALUATION_VERSION,
    DynamicPortfolioEvidencePolicy,
)
from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
)

LOW_VOLATILITY_STRATEGY_ID = "dynamic-universe-low-volatility-v4"
LOW_VOLATILITY_SPEC_VERSION = "low-volatility-research-spec-v4"
LOW_VOLATILITY_RANKING_VERSION = "trailing-daily-volatility-ascending-v1"


@dataclass(frozen=True, slots=True)
class LowVolatilityResearchSpec:
    predecessor_result_hash: str
    dataset_manifest_hash: str
    plan_hash: str
    policy_hash: str
    start_date: date
    end_date: date
    evidence_policy: DynamicPortfolioEvidencePolicy = field(
        default_factory=DynamicPortfolioEvidencePolicy
    )
    volatility_lookback_sessions: int = 252
    minimum_history_sessions: int = 253
    rebalance_sessions: int = 21
    selection_count: int = 20
    minimum_eligible_members: int = 60
    signal_lag_sessions: int = 1
    initial_cash: Decimal = Decimal("1000000")
    gross_allocation: Decimal = Decimal("0.50")
    maximum_position_weight: Decimal = Decimal("0.05")
    maximum_order_notional: Decimal = Decimal("100000")
    slippage_bps: Decimal = Decimal("10")
    maximum_volume_participation: Decimal = Decimal("0.05")
    train_sessions: int = 504
    test_sessions: int = 63
    embargo_sessions: int = 5
    strategy_id: str = LOW_VOLATILITY_STRATEGY_ID
    benchmark_version: str = DYNAMIC_PORTFOLIO_BENCHMARK_VERSION
    valuation_version: str = DYNAMIC_PORTFOLIO_VALUATION_VERSION
    ranking_version: str = LOW_VOLATILITY_RANKING_VERSION
    version: str = LOW_VOLATILITY_SPEC_VERSION
    spec_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (
                self.predecessor_result_hash,
                "low-volatility predecessor result hash",
            ),
            (
                self.dataset_manifest_hash,
                "low-volatility dataset manifest hash",
            ),
            (self.plan_hash, "low-volatility plan hash"),
            (self.policy_hash, "low-volatility universe policy hash"),
        ):
            _require_lowercase_sha256(value, name=name)
        if self.start_date > self.end_date:
            raise ValueError("low-volatility research start cannot follow end")
        if (
            self.volatility_lookback_sessions != 252
            or self.minimum_history_sessions != 253
            or self.rebalance_sessions != 21
            or self.selection_count != 20
            or self.minimum_eligible_members != 60
            or self.signal_lag_sessions != 1
        ):
            raise ValueError("low-volatility signal design is unsupported")
        if (
            self.initial_cash != Decimal("1000000")
            or self.gross_allocation != Decimal("0.50")
            or self.maximum_position_weight != Decimal("0.05")
            or self.maximum_order_notional != Decimal("100000")
            or self.slippage_bps != Decimal("10")
            or self.maximum_volume_participation != Decimal("0.05")
            or self.train_sessions != 504
            or self.test_sessions != 63
            or self.embargo_sessions != 5
        ):
            raise ValueError("low-volatility validation or risk design is unsupported")
        if (
            self.gross_allocation / self.selection_count > self.maximum_position_weight
            or self.initial_cash
            * self.gross_allocation
            / self.selection_count
            * (Decimal("1") + self.slippage_bps / Decimal("10000"))
            > self.maximum_order_notional
        ):
            raise ValueError("low-volatility strategy exceeds frozen risk limits")
        if (
            self.strategy_id != LOW_VOLATILITY_STRATEGY_ID
            or self.benchmark_version != DYNAMIC_PORTFOLIO_BENCHMARK_VERSION
            or self.valuation_version != DYNAMIC_PORTFOLIO_VALUATION_VERSION
            or self.ranking_version != LOW_VOLATILITY_RANKING_VERSION
            or self.version != LOW_VOLATILITY_SPEC_VERSION
        ):
            raise ValueError("low-volatility research version is unsupported")
        object.__setattr__(
            self,
            "spec_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "benchmark_version": self.benchmark_version,
            "dataset_manifest_hash": self.dataset_manifest_hash,
            "embargo_sessions": self.embargo_sessions,
            "end_date": self.end_date.isoformat(),
            "evidence_policy": self.evidence_policy.payload(),
            "gross_allocation": _decimal_text(self.gross_allocation),
            "initial_cash": _decimal_text(self.initial_cash),
            "maximum_order_notional": _decimal_text(self.maximum_order_notional),
            "maximum_position_weight": _decimal_text(self.maximum_position_weight),
            "maximum_volume_participation": _decimal_text(self.maximum_volume_participation),
            "minimum_eligible_members": (self.minimum_eligible_members),
            "minimum_history_sessions": (self.minimum_history_sessions),
            "plan_hash": self.plan_hash,
            "policy_hash": self.policy_hash,
            "predecessor_result_hash": (self.predecessor_result_hash),
            "ranking_version": self.ranking_version,
            "rebalance_sessions": self.rebalance_sessions,
            "selection_count": self.selection_count,
            "signal_lag_sessions": self.signal_lag_sessions,
            "slippage_bps": _decimal_text(self.slippage_bps),
            "start_date": self.start_date.isoformat(),
            "strategy_id": self.strategy_id,
            "test_sessions": self.test_sessions,
            "train_sessions": self.train_sessions,
            "valuation_version": self.valuation_version,
            "version": self.version,
            "volatility_lookback_sessions": (self.volatility_lookback_sessions),
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> LowVolatilityResearchSpec:
        raw_policy = payload["evidence_policy"]
        if not isinstance(raw_policy, dict):
            raise TypeError("low-volatility evidence policy is invalid")
        value = cls(
            predecessor_result_hash=str(payload["predecessor_result_hash"]),
            dataset_manifest_hash=str(payload["dataset_manifest_hash"]),
            plan_hash=str(payload["plan_hash"]),
            policy_hash=str(payload["policy_hash"]),
            start_date=date.fromisoformat(str(payload["start_date"])),
            end_date=date.fromisoformat(str(payload["end_date"])),
            evidence_policy=DynamicPortfolioEvidencePolicy.from_payload(
                {str(key): item for key, item in raw_policy.items()}
            ),
            volatility_lookback_sessions=int(str(payload["volatility_lookback_sessions"])),
            minimum_history_sessions=int(str(payload["minimum_history_sessions"])),
            rebalance_sessions=int(str(payload["rebalance_sessions"])),
            selection_count=int(str(payload["selection_count"])),
            minimum_eligible_members=int(str(payload["minimum_eligible_members"])),
            signal_lag_sessions=int(str(payload["signal_lag_sessions"])),
            initial_cash=Decimal(str(payload["initial_cash"])),
            gross_allocation=Decimal(str(payload["gross_allocation"])),
            maximum_position_weight=Decimal(str(payload["maximum_position_weight"])),
            maximum_order_notional=Decimal(str(payload["maximum_order_notional"])),
            slippage_bps=Decimal(str(payload["slippage_bps"])),
            maximum_volume_participation=Decimal(str(payload["maximum_volume_participation"])),
            train_sessions=int(str(payload["train_sessions"])),
            test_sessions=int(str(payload["test_sessions"])),
            embargo_sessions=int(str(payload["embargo_sessions"])),
            strategy_id=str(payload["strategy_id"]),
            benchmark_version=str(payload["benchmark_version"]),
            valuation_version=str(payload["valuation_version"]),
            ranking_version=str(payload["ranking_version"]),
            version=str(payload["version"]),
        )
        if value.payload() != payload:
            raise ValueError("low-volatility spec payload is not canonical")
        return value
