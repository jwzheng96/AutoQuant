from datetime import date
from decimal import Decimal

import pytest

from autoquant.backtest.low_volatility_portfolio import (
    LOW_VOLATILITY_STRATEGY_ID,
    LowVolatilityResearchSpec,
)


def _spec(**overrides: object) -> LowVolatilityResearchSpec:
    values: dict[str, object] = {
        "predecessor_result_hash": "a" * 64,
        "dataset_manifest_hash": "b" * 64,
        "plan_hash": "c" * 64,
        "policy_hash": "d" * 64,
        "start_date": date(2020, 1, 1),
        "end_date": date(2026, 7, 22),
    }
    values.update(overrides)
    return LowVolatilityResearchSpec(**values)  # type: ignore[arg-type]


def test_low_volatility_spec_is_fixed_and_round_trips() -> None:
    spec = _spec()

    restored = LowVolatilityResearchSpec.from_payload(spec.payload())

    assert restored == spec
    assert spec.strategy_id == LOW_VOLATILITY_STRATEGY_ID
    assert spec.volatility_lookback_sessions == 252
    assert spec.minimum_history_sessions == 253
    assert spec.rebalance_sessions == 21
    assert spec.selection_count == 20
    assert spec.gross_allocation == Decimal("0.50")
    assert len(spec.spec_hash) == 64


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("volatility_lookback_sessions", 120),
        ("minimum_history_sessions", 252),
        ("rebalance_sessions", 20),
        ("selection_count", 25),
        ("gross_allocation", Decimal("0.60")),
        ("slippage_bps", Decimal("5")),
        ("train_sessions", 252),
    ),
)
def test_low_volatility_spec_rejects_post_outcome_parameter_changes(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError):
        _spec(**{field: value})
