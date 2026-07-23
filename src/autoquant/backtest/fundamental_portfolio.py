from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from autoquant.backtest.dynamic_portfolio import (
    DynamicPortfolioEvidencePolicy,
)
from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
)

FUNDAMENTAL_PORTFOLIO_STRATEGY_ID = (
    "dynamic-universe-quality-value-v3"
)
FUNDAMENTAL_PORTFOLIO_SPEC_VERSION = (
    "fundamental-portfolio-research-spec-v3"
)
FUNDAMENTAL_DATA_POLICY_VERSION = (
    "tushare-announcement-point-in-time-v1"
)
FUNDAMENTAL_RANKING_VERSION = (
    "cross-sectional-percentile-equal-factor-v1"
)
FUNDAMENTAL_ELIGIBILITY_VERSION = (
    "positive-value-quality-complete-v1"
)
FUNDAMENTAL_FACTORS = (
    "earnings_yield",
    "book_to_price",
    "roe_diluted",
    "roa",
    "operating_cashflow_to_revenue",
)


@dataclass(frozen=True, slots=True)
class FundamentalDataPolicy:
    valuation_endpoint: str = "daily_basic"
    financial_endpoint: str = "fina_indicator"
    announcement_availability: str = (
        "next-trading-session-open-after-ann_date"
    )
    indicator_period_lookback_days: int = 550
    version: str = FUNDAMENTAL_DATA_POLICY_VERSION
    policy_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            self.valuation_endpoint != "daily_basic"
            or self.financial_endpoint != "fina_indicator"
            or self.announcement_availability
            != "next-trading-session-open-after-ann_date"
            or self.indicator_period_lookback_days != 550
            or self.version != FUNDAMENTAL_DATA_POLICY_VERSION
        ):
            raise ValueError(
                "fundamental data policy is unsupported"
            )
        object.__setattr__(
            self,
            "policy_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "announcement_availability": (
                self.announcement_availability
            ),
            "financial_endpoint": self.financial_endpoint,
            "indicator_period_lookback_days": (
                self.indicator_period_lookback_days
            ),
            "valuation_endpoint": self.valuation_endpoint,
            "version": self.version,
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> FundamentalDataPolicy:
        value = cls(
            valuation_endpoint=str(payload["valuation_endpoint"]),
            financial_endpoint=str(payload["financial_endpoint"]),
            announcement_availability=str(
                payload["announcement_availability"]
            ),
            indicator_period_lookback_days=int(
                str(payload["indicator_period_lookback_days"])
            ),
            version=str(payload["version"]),
        )
        if value.payload() != payload:
            raise ValueError(
                "fundamental data policy payload is not canonical"
            )
        return value


@dataclass(frozen=True, slots=True)
class FundamentalPortfolioResearchSpec:
    predecessor_result_hash: str
    daily_dataset_manifest_hash: str
    plan_hash: str
    universe_policy_hash: str
    start_date: date
    end_date: date
    data_policy: FundamentalDataPolicy = field(
        default_factory=FundamentalDataPolicy
    )
    evidence_policy: DynamicPortfolioEvidencePolicy = field(
        default_factory=DynamicPortfolioEvidencePolicy
    )
    factors: tuple[str, ...] = FUNDAMENTAL_FACTORS
    factor_weight: Decimal = Decimal("0.20")
    rebalance_sessions: int = 21
    selection_count: int = 20
    minimum_eligible_members: int = 60
    maximum_financial_age_days: int = 400
    maximum_debt_to_assets_percent: Decimal = Decimal("95")
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
    strategy_id: str = FUNDAMENTAL_PORTFOLIO_STRATEGY_ID
    ranking_version: str = FUNDAMENTAL_RANKING_VERSION
    eligibility_version: str = FUNDAMENTAL_ELIGIBILITY_VERSION
    version: str = FUNDAMENTAL_PORTFOLIO_SPEC_VERSION
    spec_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (
                self.predecessor_result_hash,
                "fundamental predecessor result hash",
            ),
            (
                self.daily_dataset_manifest_hash,
                "fundamental daily dataset manifest hash",
            ),
            (
                self.plan_hash,
                "fundamental research input plan hash",
            ),
            (
                self.universe_policy_hash,
                "fundamental universe policy hash",
            ),
        ):
            _require_lowercase_sha256(value, name=name)
        if self.start_date > self.end_date:
            raise ValueError(
                "fundamental research start cannot follow end"
            )
        if (
            tuple(self.factors) != FUNDAMENTAL_FACTORS
            or self.factor_weight != Decimal("0.20")
            or self.factor_weight * len(self.factors) != Decimal("1")
            or self.rebalance_sessions != 21
            or self.selection_count != 20
            or self.minimum_eligible_members != 60
            or self.maximum_financial_age_days != 400
            or self.maximum_debt_to_assets_percent != Decimal("95")
            or self.signal_lag_sessions != 1
        ):
            raise ValueError(
                "fundamental factor design is unsupported"
            )
        if (
            self.initial_cash != Decimal("1000000")
            or self.gross_allocation != Decimal("0.50")
            or self.maximum_position_weight != Decimal("0.05")
            or self.maximum_order_notional != Decimal("100000")
            or self.slippage_bps != Decimal("10")
            or self.maximum_volume_participation
            != Decimal("0.05")
            or self.train_sessions != 504
            or self.test_sessions != 63
            or self.embargo_sessions != 5
        ):
            raise ValueError(
                "fundamental validation or risk design is unsupported"
            )
        if (
            self.gross_allocation / self.selection_count
            > self.maximum_position_weight
            or self.initial_cash
            * self.gross_allocation
            / self.selection_count
            * (Decimal("1") + self.slippage_bps / Decimal("10000"))
            > self.maximum_order_notional
        ):
            raise ValueError(
                "fundamental strategy exceeds frozen risk limits"
            )
        if (
            self.strategy_id != FUNDAMENTAL_PORTFOLIO_STRATEGY_ID
            or self.ranking_version != FUNDAMENTAL_RANKING_VERSION
            or self.eligibility_version
            != FUNDAMENTAL_ELIGIBILITY_VERSION
            or self.version != FUNDAMENTAL_PORTFOLIO_SPEC_VERSION
        ):
            raise ValueError(
                "fundamental research version is unsupported"
            )
        object.__setattr__(self, "factors", tuple(self.factors))
        object.__setattr__(
            self,
            "spec_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "daily_dataset_manifest_hash": (
                self.daily_dataset_manifest_hash
            ),
            "data_policy": self.data_policy.payload(),
            "eligibility_version": self.eligibility_version,
            "embargo_sessions": self.embargo_sessions,
            "end_date": self.end_date.isoformat(),
            "evidence_policy": self.evidence_policy.payload(),
            "factor_weight": _decimal_text(self.factor_weight),
            "factors": list(self.factors),
            "gross_allocation": _decimal_text(self.gross_allocation),
            "initial_cash": _decimal_text(self.initial_cash),
            "maximum_debt_to_assets_percent": _decimal_text(
                self.maximum_debt_to_assets_percent
            ),
            "maximum_financial_age_days": (
                self.maximum_financial_age_days
            ),
            "maximum_order_notional": _decimal_text(
                self.maximum_order_notional
            ),
            "maximum_position_weight": _decimal_text(
                self.maximum_position_weight
            ),
            "maximum_volume_participation": _decimal_text(
                self.maximum_volume_participation
            ),
            "minimum_eligible_members": (
                self.minimum_eligible_members
            ),
            "plan_hash": self.plan_hash,
            "predecessor_result_hash": (
                self.predecessor_result_hash
            ),
            "ranking_version": self.ranking_version,
            "rebalance_sessions": self.rebalance_sessions,
            "selection_count": self.selection_count,
            "signal_lag_sessions": self.signal_lag_sessions,
            "slippage_bps": _decimal_text(self.slippage_bps),
            "start_date": self.start_date.isoformat(),
            "strategy_id": self.strategy_id,
            "test_sessions": self.test_sessions,
            "train_sessions": self.train_sessions,
            "universe_policy_hash": self.universe_policy_hash,
            "version": self.version,
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> FundamentalPortfolioResearchSpec:
        raw_factors = payload["factors"]
        raw_data_policy = payload["data_policy"]
        raw_evidence_policy = payload["evidence_policy"]
        if (
            not isinstance(raw_factors, list)
            or not isinstance(raw_data_policy, dict)
            or not isinstance(raw_evidence_policy, dict)
        ):
            raise TypeError(
                "fundamental research nested payload is invalid"
            )
        value = cls(
            predecessor_result_hash=str(
                payload["predecessor_result_hash"]
            ),
            daily_dataset_manifest_hash=str(
                payload["daily_dataset_manifest_hash"]
            ),
            plan_hash=str(payload["plan_hash"]),
            universe_policy_hash=str(
                payload["universe_policy_hash"]
            ),
            start_date=date.fromisoformat(str(payload["start_date"])),
            end_date=date.fromisoformat(str(payload["end_date"])),
            data_policy=FundamentalDataPolicy.from_payload(
                {
                    str(key): item
                    for key, item in raw_data_policy.items()
                }
            ),
            evidence_policy=(
                DynamicPortfolioEvidencePolicy.from_payload(
                    {
                        str(key): item
                        for key, item in raw_evidence_policy.items()
                    }
                )
            ),
            factors=tuple(str(item) for item in raw_factors),
            factor_weight=Decimal(str(payload["factor_weight"])),
            rebalance_sessions=int(
                str(payload["rebalance_sessions"])
            ),
            selection_count=int(str(payload["selection_count"])),
            minimum_eligible_members=int(
                str(payload["minimum_eligible_members"])
            ),
            maximum_financial_age_days=int(
                str(payload["maximum_financial_age_days"])
            ),
            maximum_debt_to_assets_percent=Decimal(
                str(payload["maximum_debt_to_assets_percent"])
            ),
            signal_lag_sessions=int(
                str(payload["signal_lag_sessions"])
            ),
            initial_cash=Decimal(str(payload["initial_cash"])),
            gross_allocation=Decimal(
                str(payload["gross_allocation"])
            ),
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
            strategy_id=str(payload["strategy_id"]),
            ranking_version=str(payload["ranking_version"]),
            eligibility_version=str(
                payload["eligibility_version"]
            ),
            version=str(payload["version"]),
        )
        if value.payload() != payload:
            raise ValueError(
                "fundamental research payload is not canonical"
            )
        return value
