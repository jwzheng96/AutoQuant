from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from autoquant.backtest.fundamental_portfolio import (
    FUNDAMENTAL_FACTORS,
    FundamentalDataPolicy,
    FundamentalPortfolioResearchSpec,
)


def spec() -> FundamentalPortfolioResearchSpec:
    return FundamentalPortfolioResearchSpec(
        predecessor_result_hash="a" * 64,
        daily_dataset_manifest_hash="b" * 64,
        plan_hash="c" * 64,
        universe_policy_hash="d" * 64,
        start_date=date(2020, 1, 1),
        end_date=date(2026, 7, 22),
    )


def test_fundamental_v3_is_fixed_canonical_and_round_trips() -> None:
    value = spec()

    restored = FundamentalPortfolioResearchSpec.from_payload(
        value.payload()
    )

    assert restored == value
    assert restored.spec_hash == value.spec_hash
    assert restored.factors == FUNDAMENTAL_FACTORS
    assert restored.factor_weight == Decimal("0.20")
    assert restored.rebalance_sessions == 21
    assert restored.selection_count == 20
    assert restored.minimum_eligible_members == 60
    assert restored.maximum_financial_age_days == 400
    assert restored.signal_lag_sessions == 1
    assert restored.data_policy == FundamentalDataPolicy()


def test_fundamental_v3_rejects_post_hoc_parameter_variants() -> None:
    with pytest.raises(ValueError, match="factor design"):
        FundamentalPortfolioResearchSpec(
            predecessor_result_hash="a" * 64,
            daily_dataset_manifest_hash="b" * 64,
            plan_hash="c" * 64,
            universe_policy_hash="d" * 64,
            start_date=date(2020, 1, 1),
            end_date=date(2026, 7, 22),
            selection_count=19,
        )


def test_fundamental_data_policy_hash_binds_announcement_semantics() -> None:
    first = FundamentalDataPolicy()
    second = FundamentalDataPolicy.from_payload(first.payload())

    assert first == second
    assert first.policy_hash == second.policy_hash
