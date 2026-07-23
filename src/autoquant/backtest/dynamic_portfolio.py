from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from autoquant.backtest.portfolio_validation import (
    CrossSectionalMomentumParameters,
)
from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
)

DYNAMIC_PORTFOLIO_STRATEGY_ID = (
    "dynamic-universe-cross-sectional-momentum-v1"
)
DYNAMIC_PORTFOLIO_SPEC_VERSION = "dynamic-portfolio-research-spec-v1"
DYNAMIC_PORTFOLIO_BENCHMARK_VERSION = (
    "point-in-time-equal-weight-quarterly-v1"
)
DYNAMIC_PORTFOLIO_VALUATION_VERSION = (
    "last-observable-close-while-nontradable-v1"
)
DYNAMIC_PORTFOLIO_EVIDENCE_VERSION = (
    "dynamic-portfolio-evidence-gates-v1"
)


@dataclass(frozen=True, slots=True)
class DynamicPortfolioEvidencePolicy:
    minimum_folds: int = 8
    minimum_oos_sessions: int = 504
    minimum_profitable_fold_rate: Decimal = Decimal("0.55")
    maximum_oos_drawdown: Decimal = Decimal("0.18")
    maximum_selection_optimism: Decimal = Decimal("0.15")
    minimum_compounded_oos_return: Decimal = Decimal("0")
    minimum_excess_oos_return: Decimal = Decimal("0")
    maximum_rejected_orders: int = 0
    version: str = DYNAMIC_PORTFOLIO_EVIDENCE_VERSION
    policy_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if self.minimum_folds < 6 or self.minimum_oos_sessions < 252:
            raise ValueError("dynamic evidence sample is insufficient")
        for name, value in (
            (
                "minimum_profitable_fold_rate",
                self.minimum_profitable_fold_rate,
            ),
            ("maximum_oos_drawdown", self.maximum_oos_drawdown),
            (
                "maximum_selection_optimism",
                self.maximum_selection_optimism,
            ),
        ):
            if (
                not isinstance(value, Decimal)
                or not value.is_finite()
                or not Decimal("0") <= value <= Decimal("1")
            ):
                raise ValueError(f"{name} must be between zero and one")
        for name, value in (
            (
                "minimum_compounded_oos_return",
                self.minimum_compounded_oos_return,
            ),
            (
                "minimum_excess_oos_return",
                self.minimum_excess_oos_return,
            ),
        ):
            if not isinstance(value, Decimal) or not value.is_finite():
                raise ValueError(f"{name} must be a finite Decimal")
        if self.maximum_rejected_orders != 0:
            raise ValueError("dynamic research cannot allow rejected orders")
        if self.version != DYNAMIC_PORTFOLIO_EVIDENCE_VERSION:
            raise ValueError("dynamic evidence policy version is unsupported")
        object.__setattr__(
            self,
            "policy_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "maximum_oos_drawdown": _decimal_text(
                self.maximum_oos_drawdown
            ),
            "maximum_rejected_orders": self.maximum_rejected_orders,
            "maximum_selection_optimism": _decimal_text(
                self.maximum_selection_optimism
            ),
            "minimum_compounded_oos_return": _decimal_text(
                self.minimum_compounded_oos_return
            ),
            "minimum_excess_oos_return": _decimal_text(
                self.minimum_excess_oos_return
            ),
            "minimum_folds": self.minimum_folds,
            "minimum_oos_sessions": self.minimum_oos_sessions,
            "minimum_profitable_fold_rate": _decimal_text(
                self.minimum_profitable_fold_rate
            ),
            "version": self.version,
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> DynamicPortfolioEvidencePolicy:
        value = cls(
            minimum_folds=int(str(payload["minimum_folds"])),
            minimum_oos_sessions=int(
                str(payload["minimum_oos_sessions"])
            ),
            minimum_profitable_fold_rate=Decimal(
                str(payload["minimum_profitable_fold_rate"])
            ),
            maximum_oos_drawdown=Decimal(
                str(payload["maximum_oos_drawdown"])
            ),
            maximum_selection_optimism=Decimal(
                str(payload["maximum_selection_optimism"])
            ),
            minimum_compounded_oos_return=Decimal(
                str(payload["minimum_compounded_oos_return"])
            ),
            minimum_excess_oos_return=Decimal(
                str(payload["minimum_excess_oos_return"])
            ),
            maximum_rejected_orders=int(
                str(payload["maximum_rejected_orders"])
            ),
            version=str(payload["version"]),
        )
        if value.payload() != payload:
            raise ValueError("dynamic evidence payload is not canonical")
        return value


@dataclass(frozen=True, slots=True)
class DynamicPortfolioResearchSpec:
    dataset_manifest_hash: str
    plan_hash: str
    policy_hash: str
    start_date: date
    end_date: date
    initial_cash: Decimal = Decimal("1000000")
    gross_allocation: Decimal = Decimal("0.50")
    maximum_position_weight: Decimal = Decimal("0.05")
    maximum_order_notional: Decimal = Decimal("100000")
    slippage_bps: Decimal = Decimal("10")
    maximum_volume_participation: Decimal = Decimal("0.05")
    train_sessions: int = 504
    test_sessions: int = 63
    embargo_sessions: int = 5
    signal_lag_sessions: int = 1
    minimum_member_history_sessions: int = 252
    candidates: tuple[CrossSectionalMomentumParameters, ...] = (
        CrossSectionalMomentumParameters(20, 5, 10),
        CrossSectionalMomentumParameters(60, 10, 10),
        CrossSectionalMomentumParameters(120, 20, 10),
        CrossSectionalMomentumParameters(252, 21, 10),
    )
    evidence_policy: DynamicPortfolioEvidencePolicy = field(
        default_factory=DynamicPortfolioEvidencePolicy
    )
    strategy_id: str = DYNAMIC_PORTFOLIO_STRATEGY_ID
    benchmark_version: str = DYNAMIC_PORTFOLIO_BENCHMARK_VERSION
    valuation_version: str = DYNAMIC_PORTFOLIO_VALUATION_VERSION
    version: str = DYNAMIC_PORTFOLIO_SPEC_VERSION
    spec_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.dataset_manifest_hash,
            name="dynamic research dataset manifest hash",
        )
        _require_lowercase_sha256(
            self.plan_hash,
            name="dynamic research input plan hash",
        )
        _require_lowercase_sha256(
            self.policy_hash,
            name="dynamic universe policy hash",
        )
        if self.start_date > self.end_date:
            raise ValueError("dynamic research start cannot follow end")
        decimals = (
            self.initial_cash,
            self.gross_allocation,
            self.maximum_position_weight,
            self.maximum_order_notional,
            self.slippage_bps,
            self.maximum_volume_participation,
        )
        if any(
            not isinstance(value, Decimal) or not value.is_finite()
            for value in decimals
        ):
            raise ValueError("dynamic research numerics must be finite Decimals")
        if (
            self.initial_cash < Decimal("10000")
            or not Decimal("0") < self.gross_allocation <= Decimal("0.80")
            or not Decimal("0")
            < self.maximum_position_weight
            <= Decimal("0.20")
            or self.maximum_order_notional <= 0
            or not Decimal("0") <= self.slippage_bps <= Decimal("100")
            or not Decimal("0")
            < self.maximum_volume_participation
            <= Decimal("0.10")
        ):
            raise ValueError("dynamic research risk numerics are invalid")
        if (
            not 252 <= self.train_sessions <= 750
            or not 20 <= self.test_sessions <= 126
            or not 1 <= self.embargo_sessions <= 20
            or self.signal_lag_sessions != 1
            or not 20
            <= self.minimum_member_history_sessions
            <= self.train_sessions
        ):
            raise ValueError("dynamic research windows are invalid")
        candidates = tuple(sorted(self.candidates))
        if (
            not candidates
            or len(candidates) > 12
            or len(set(candidates)) != len(candidates)
            or max(value.lookback_sessions for value in candidates)
            >= self.train_sessions
        ):
            raise ValueError("dynamic research candidates are invalid")
        if any(
            value.selection_count
            * self.maximum_position_weight
            > self.gross_allocation
            or self.gross_allocation / value.selection_count
            > self.maximum_position_weight
            or self.initial_cash
            * self.gross_allocation
            / value.selection_count
            * (Decimal("1") + self.slippage_bps / Decimal("10000"))
            > self.maximum_order_notional
            for value in candidates
        ):
            raise ValueError("dynamic candidate exceeds frozen risk limits")
        if (
            self.strategy_id != DYNAMIC_PORTFOLIO_STRATEGY_ID
            or self.benchmark_version
            != DYNAMIC_PORTFOLIO_BENCHMARK_VERSION
            or self.valuation_version
            != DYNAMIC_PORTFOLIO_VALUATION_VERSION
            or self.version != DYNAMIC_PORTFOLIO_SPEC_VERSION
        ):
            raise ValueError("dynamic research version is unsupported")
        object.__setattr__(self, "candidates", candidates)
        object.__setattr__(self, "spec_hash", _canonical_hash(self.payload()))

    def payload(self) -> dict[str, object]:
        return {
            "benchmark_version": self.benchmark_version,
            "candidates": [value.payload() for value in self.candidates],
            "dataset_manifest_hash": self.dataset_manifest_hash,
            "embargo_sessions": self.embargo_sessions,
            "end_date": self.end_date.isoformat(),
            "evidence_policy": self.evidence_policy.payload(),
            "gross_allocation": _decimal_text(self.gross_allocation),
            "initial_cash": _decimal_text(self.initial_cash),
            "maximum_order_notional": _decimal_text(
                self.maximum_order_notional
            ),
            "maximum_position_weight": _decimal_text(
                self.maximum_position_weight
            ),
            "maximum_volume_participation": _decimal_text(
                self.maximum_volume_participation
            ),
            "minimum_member_history_sessions": (
                self.minimum_member_history_sessions
            ),
            "plan_hash": self.plan_hash,
            "policy_hash": self.policy_hash,
            "signal_lag_sessions": self.signal_lag_sessions,
            "slippage_bps": _decimal_text(self.slippage_bps),
            "start_date": self.start_date.isoformat(),
            "strategy_id": self.strategy_id,
            "test_sessions": self.test_sessions,
            "train_sessions": self.train_sessions,
            "valuation_version": self.valuation_version,
            "version": self.version,
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> DynamicPortfolioResearchSpec:
        raw_candidates = payload.get("candidates")
        raw_evidence = payload.get("evidence_policy")
        if not isinstance(raw_candidates, list) or not isinstance(
            raw_evidence,
            dict,
        ):
            raise TypeError("dynamic research nested payloads are invalid")
        candidates = tuple(
            CrossSectionalMomentumParameters(
                lookback_sessions=int(str(value["lookback_sessions"])),
                rebalance_sessions=int(str(value["rebalance_sessions"])),
                selection_count=int(str(value["selection_count"])),
            )
            for value in raw_candidates
            if isinstance(value, dict)
        )
        if len(candidates) != len(raw_candidates):
            raise TypeError("dynamic research candidate payload is invalid")
        value = cls(
            dataset_manifest_hash=str(payload["dataset_manifest_hash"]),
            plan_hash=str(payload["plan_hash"]),
            policy_hash=str(payload["policy_hash"]),
            start_date=date.fromisoformat(str(payload["start_date"])),
            end_date=date.fromisoformat(str(payload["end_date"])),
            initial_cash=Decimal(str(payload["initial_cash"])),
            gross_allocation=Decimal(str(payload["gross_allocation"])),
            maximum_position_weight=Decimal(
                str(payload["maximum_position_weight"])
            ),
            maximum_order_notional=Decimal(
                str(payload["maximum_order_notional"])
            ),
            slippage_bps=Decimal(str(payload["slippage_bps"])),
            maximum_volume_participation=Decimal(
                str(payload["maximum_volume_participation"])
            ),
            train_sessions=int(str(payload["train_sessions"])),
            test_sessions=int(str(payload["test_sessions"])),
            embargo_sessions=int(str(payload["embargo_sessions"])),
            signal_lag_sessions=int(str(payload["signal_lag_sessions"])),
            minimum_member_history_sessions=int(
                str(payload["minimum_member_history_sessions"])
            ),
            candidates=candidates,
            evidence_policy=DynamicPortfolioEvidencePolicy.from_payload(
                {str(key): item for key, item in raw_evidence.items()}
            ),
            strategy_id=str(payload["strategy_id"]),
            benchmark_version=str(payload["benchmark_version"]),
            valuation_version=str(payload["valuation_version"]),
            version=str(payload["version"]),
        )
        if value.payload() != payload:
            raise ValueError("dynamic research spec payload is not canonical")
        return value
