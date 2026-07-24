from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from autoquant.backtest.low_volatility_execution_compatibility import (
    DECISION_TIME_INPUTS,
    FORBIDDEN_INTENT_INPUTS,
    LOW_VOLATILITY_EXECUTION_COMPATIBILITY_VERSION,
    LowVolatilityExecutionCompatibilitySpec,
)
from autoquant.errors import PersistenceUnavailableError
from autoquant.web.low_volatility_execution_compatibility_store import (
    _spec,
)

FROZEN_AT = datetime(2026, 7, 24, 8, tzinfo=UTC)


def _compatibility() -> LowVolatilityExecutionCompatibilitySpec:
    return LowVolatilityExecutionCompatibilitySpec(
        source_spec_hash="a" * 64,
        forward_spec_hash="b" * 64,
        observed_forward_session_count=1,
        frozen_by="research-operator",
        frozen_at=FROZEN_AT,
    )


def test_compatibility_spec_round_trips_with_partial_disclosure() -> None:
    spec = _compatibility()

    restored = LowVolatilityExecutionCompatibilitySpec.from_payload(spec.payload())

    assert restored == spec
    assert spec.version == (LOW_VOLATILITY_EXECUTION_COMPATIBILITY_VERSION)
    assert spec.partial_outcome_observed_before_freeze is True
    assert spec.terminal_outcome_observed_before_freeze is False
    assert spec.decision_time_inputs == DECISION_TIME_INPUTS
    assert spec.forbidden_intent_inputs == FORBIDDEN_INTENT_INPUTS
    assert spec.compatibility_can_only_disqualify is True
    assert spec.paper_activation_allowed is False
    assert spec.live_trading_locked is True


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("observed_forward_session_count", 126),
        ("order_intent_invariance_required", False),
        ("same_forward_window_required", False),
        ("compatibility_can_only_disqualify", False),
        ("terminal_outcome_observed_before_freeze", True),
        ("historical_reclassification_allowed", True),
        ("paper_activation_allowed", True),
        ("live_trading_locked", False),
    ),
)
def test_compatibility_spec_rejects_late_or_weakened_freeze(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match="compatibility spec"):
        replace(_compatibility(), **{field: value})


def test_compatibility_store_row_verifies_integrity() -> None:
    spec = _compatibility()
    row: dict[str, object] = {
        **spec.payload(),
        "compatibility_version": spec.version,
        "frozen_at": spec.frozen_at,
        "payload": spec.payload(),
        "spec_hash": spec.spec_hash,
    }

    assert _spec(cast(Any, row)) == spec

    row["paper_activation_allowed"] = True
    with pytest.raises(PersistenceUnavailableError, match="integrity"):
        _spec(cast(Any, row))


def test_compatibility_migration_is_early_append_only_gate() -> None:
    sql = Path("migrations/postgres/048_low_volatility_execution_compatibility.sql").read_text(
        encoding="utf-8"
    )

    assert "low_volatility_execution_compatibility_specs" in sql
    assert "observed_forward_session_count BETWEEN 0 AND 125" in sql
    assert "frozen_session_count >= 126" in sql
    assert "low_volatility_forward_evaluation_runs" in sql
    assert "compatibility_can_only_disqualify" in sql
    assert "NOT paper_activation_allowed" in sql
    assert sql.count("autoquant_reject_immutable_change()") == 1
    assert "VALUES ('postgres', 48)" in sql
