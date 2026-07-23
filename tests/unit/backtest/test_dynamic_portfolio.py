from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from autoquant.backtest.dynamic_portfolio import (
    DynamicPortfolioEvidencePolicy,
    DynamicPortfolioResearchSpec,
)
from autoquant.backtest.portfolio_validation import (
    CrossSectionalMomentumParameters,
)


def _spec() -> DynamicPortfolioResearchSpec:
    return DynamicPortfolioResearchSpec(
        dataset_manifest_hash="a" * 64,
        plan_hash="b" * 64,
        policy_hash="c" * 64,
        start_date=date(2020, 1, 1),
        end_date=date(2026, 7, 22),
    )


def test_dynamic_spec_is_conservative_canonical_and_round_trips() -> None:
    value = _spec()

    restored = DynamicPortfolioResearchSpec.from_payload(
        value.payload()
    )

    assert restored == value
    assert restored.spec_hash == value.spec_hash
    assert restored.gross_allocation == Decimal("0.50")
    assert restored.maximum_position_weight == Decimal("0.05")
    assert restored.slippage_bps == Decimal("10")
    assert restored.maximum_volume_participation == Decimal("0.05")
    assert restored.train_sessions == 504
    assert restored.test_sessions == 63
    assert restored.embargo_sessions == 5
    assert restored.signal_lag_sessions == 1
    assert restored.minimum_member_history_sessions == 252
    assert tuple(
        (
            item.lookback_sessions,
            item.rebalance_sessions,
            item.selection_count,
        )
        for item in restored.candidates
    ) == (
        (20, 5, 10),
        (60, 10, 10),
        (120, 20, 10),
        (252, 21, 10),
    )
    assert restored.evidence_policy.minimum_folds == 8
    assert restored.evidence_policy.minimum_oos_sessions == 504
    assert (
        restored.evidence_policy.minimum_profitable_fold_rate
        == Decimal("0.55")
    )
    assert restored.evidence_policy.maximum_rejected_orders == 0


def test_dynamic_spec_hash_changes_with_any_frozen_assumption() -> None:
    baseline = _spec()
    changed = DynamicPortfolioResearchSpec(
        dataset_manifest_hash=baseline.dataset_manifest_hash,
        plan_hash=baseline.plan_hash,
        policy_hash=baseline.policy_hash,
        start_date=baseline.start_date,
        end_date=baseline.end_date,
        slippage_bps=Decimal("11"),
    )

    assert changed.spec_hash != baseline.spec_hash


def test_dynamic_spec_rejects_concentration_or_lookahead() -> None:
    with pytest.raises(ValueError, match="risk limits"):
        DynamicPortfolioResearchSpec(
            dataset_manifest_hash="a" * 64,
            plan_hash="b" * 64,
            policy_hash="c" * 64,
            start_date=date(2020, 1, 1),
            end_date=date(2026, 7, 22),
            candidates=(
                CrossSectionalMomentumParameters(60, 10, 9),
            ),
        )
    with pytest.raises(ValueError, match="windows"):
        DynamicPortfolioResearchSpec(
            dataset_manifest_hash="a" * 64,
            plan_hash="b" * 64,
            policy_hash="c" * 64,
            start_date=date(2020, 1, 1),
            end_date=date(2026, 7, 22),
            signal_lag_sessions=0,
        )


def test_dynamic_evidence_policy_cannot_allow_rejected_orders() -> None:
    with pytest.raises(ValueError, match="cannot allow"):
        DynamicPortfolioEvidencePolicy(maximum_rejected_orders=1)
