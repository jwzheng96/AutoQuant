from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from autoquant.backtest.low_volatility_forward import (
    LOW_VOLATILITY_FORWARD_SPEC_VERSION,
    LowVolatilityForwardEvidenceSpec,
    annualized_geometric_return,
    annualized_stability_gap,
)
from autoquant.errors import PersistenceUnavailableError
from autoquant.web.low_volatility_forward_store import (
    LowVolatilityForwardEvidenceSpecRecord,
    _record,
)


def _spec() -> LowVolatilityForwardEvidenceSpec:
    return LowVolatilityForwardEvidenceSpec(
        predecessor_result_hash="a" * 64,
        predecessor_assessment_hash="b" * 64,
        source_spec_hash="c" * 64,
        source_dataset_manifest_hash="d" * 64,
        forward_start_date=date(2026, 7, 23),
        maximum_annualized_stability_gap=Decimal("0.15"),
    )


def test_forward_evidence_spec_is_future_only_and_round_trips() -> None:
    spec = _spec()

    restored = LowVolatilityForwardEvidenceSpec.from_payload(spec.payload())

    assert restored == spec
    assert spec.minimum_forward_sessions == 126
    assert spec.minimum_paper_sessions == 60
    assert spec.formal_hypothesis_count == 4
    assert spec.outcome_observed_at_design is True
    assert spec.strategy_parameters_unchanged is True
    assert spec.retrospective_reclassification_allowed is False
    assert spec.historical_result_eligible_for_promotion is False
    assert spec.version == LOW_VOLATILITY_FORWARD_SPEC_VERSION


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("minimum_forward_sessions", 63),
        ("minimum_paper_sessions", 0),
        ("formal_hypothesis_count", 1),
        ("outcome_observed_at_design", False),
        ("strategy_parameters_unchanged", False),
        ("retrospective_reclassification_allowed", True),
        ("historical_result_eligible_for_promotion", True),
    ),
)
def test_forward_spec_rejects_governance_weakening(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError):
        replace(_spec(), **{field: value})


def test_annualized_geometric_return_compares_equal_horizons() -> None:
    annual = annualized_geometric_return(
        Decimal("0.21"),
        sessions=252,
    )
    half_speed = annualized_geometric_return(
        Decimal("0.21"),
        sessions=504,
    )
    gap = annualized_stability_gap(
        training_return=Decimal("0.21"),
        training_sessions=504,
        evaluation_return=Decimal("0.10"),
        evaluation_sessions=252,
    )

    assert abs(annual - Decimal("0.21")) < Decimal("1e-30")
    assert abs(half_speed - Decimal("0.10")) < Decimal("1e-30")
    assert abs(gap) < Decimal("1e-30")


def test_forward_payload_rejects_string_boolean() -> None:
    payload = _spec().payload()
    payload["outcome_observed_at_design"] = "true"

    with pytest.raises(TypeError, match="boolean"):
        LowVolatilityForwardEvidenceSpec.from_payload(payload)


def test_forward_store_record_round_trips_and_detects_tampering() -> None:
    spec = _spec()
    created_at = datetime(2026, 7, 24, tzinfo=UTC)
    expected = LowVolatilityForwardEvidenceSpecRecord(
        spec=spec,
        requested_by="operator",
        created_at=created_at,
    )
    row: dict[str, object] = {
        "created_at": created_at,
        "formal_hypothesis_count": 4,
        "forward_start_date": spec.forward_start_date,
        "historical_result_eligible_for_promotion": False,
        "live_trading_locked": True,
        "methodology_version": spec.stability_method_version,
        "minimum_forward_sessions": 126,
        "minimum_paper_sessions": 60,
        "outcome_observed_at_design": True,
        "payload": spec.payload(),
        "predecessor_assessment_hash": (spec.predecessor_assessment_hash),
        "predecessor_result_hash": spec.predecessor_result_hash,
        "requested_by": "operator",
        "retrospective_reclassification_allowed": False,
        "source_dataset_manifest_hash": (spec.source_dataset_manifest_hash),
        "source_spec_hash": spec.source_spec_hash,
        "spec_hash": spec.spec_hash,
        "specification_version": spec.version,
        "strategy_id": spec.strategy_id,
        "strategy_parameters_unchanged": True,
    }

    assert _record(cast(Any, row)) == expected

    row["historical_result_eligible_for_promotion"] = True
    with pytest.raises(
        PersistenceUnavailableError,
        match="integrity",
    ):
        _record(cast(Any, row))


def test_forward_evidence_migration_is_immutable() -> None:
    sql = Path("migrations/postgres/034_low_volatility_forward_evidence.sql").read_text(
        encoding="utf-8"
    )

    assert ("CREATE TABLE IF NOT EXISTS low_volatility_forward_evidence_specs") in sql
    assert "autoquant_reject_immutable_change()" in sql
    assert "retrospective_reclassification_allowed" in sql
    assert "VALUES ('postgres', 34)" in sql
