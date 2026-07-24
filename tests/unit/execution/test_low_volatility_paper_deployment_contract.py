from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.low_volatility_paper_deployment_contract import (
    LOW_VOLATILITY_DEPLOYABLE_SIGNAL_POLICY_VERSION,
    LOW_VOLATILITY_PAPER_DEPLOYMENT_CONTRACT_VERSION,
    LowVolatilityPaperDeploymentContract,
)
from autoquant.execution.low_volatility_paper_deployment_contract_store import (
    _contract,
    _parameters,
)

FROZEN_AT = datetime(2026, 7, 24, 12, tzinfo=UTC)


def _deployment_contract() -> LowVolatilityPaperDeploymentContract:
    return LowVolatilityPaperDeploymentContract(
        source_spec_hash="a" * 64,
        forward_spec_hash="b" * 64,
        compatibility_spec_hash="c" * 64,
        observed_forward_session_count=1,
        frozen_by="risk-operator",
        frozen_at=FROZEN_AT,
    )


def test_deployment_contract_round_trips_without_authority() -> None:
    contract = _deployment_contract()

    restored = LowVolatilityPaperDeploymentContract.from_payload(contract.payload())

    assert restored == contract
    assert contract.version == (LOW_VOLATILITY_PAPER_DEPLOYMENT_CONTRACT_VERSION)
    assert contract.required_daily_signal_policy_version == (
        LOW_VOLATILITY_DEPLOYABLE_SIGNAL_POLICY_VERSION
    )
    assert contract.partial_outcome_observed_before_freeze is True
    assert contract.terminal_outcome_observed_before_freeze is False
    assert contract.paper_activation_authority_granted is False
    assert contract.runtime_activation_allowed is False
    assert contract.live_trading_locked is True


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("observed_forward_session_count", 126),
        (
            "candidate_approval_after_compatibility_required",
            False,
        ),
        ("exact_session_signal_required", False),
        ("decision_time_signal_required", False),
        ("preopen_signal_required", False),
        ("point_in_time_universe_required", False),
        ("held_position_valuation_coverage_required", False),
        ("exact_risk_policy_required", False),
        (
            "kill_switch_active_at_authorization_required",
            False,
        ),
        ("exclusive_paper_deployment_required", False),
        ("fresh_runtime_unlock_evidence_required", False),
        ("runtime_authorization_separate", False),
        ("terminal_outcome_observed_before_freeze", True),
        ("historical_reclassification_allowed", True),
        ("paper_activation_authority_granted", True),
        ("runtime_activation_allowed", True),
        ("live_trading_locked", False),
    ),
)
def test_deployment_contract_rejects_late_or_weakened_terms(
    field: str,
    value: object,
) -> None:
    with pytest.raises(
        ValueError,
        match="deployment contract",
    ):
        replace(
            _deployment_contract(),
            **{field: value},
        )


def test_deployment_contract_store_row_verifies_integrity() -> None:
    contract = _deployment_contract()
    row: dict[str, object] = {
        **contract.payload(),
        "contract_hash": contract.contract_hash,
        "contract_version": contract.version,
        "frozen_at": contract.frozen_at,
        "payload": contract.payload(),
    }

    assert _contract(cast(Any, row)) == contract
    assert _parameters(contract)["contract_hash"] == contract.contract_hash

    row["paper_activation_authority_granted"] = True
    with pytest.raises(
        PersistenceUnavailableError,
        match="integrity",
    ):
        _contract(cast(Any, row))


def test_deployment_contract_migration_is_early_and_immutable() -> None:
    sql = Path("migrations/postgres/050_low_volatility_paper_deployment_contracts.sql").read_text(
        encoding="utf-8"
    )

    assert "low_volatility_paper_deployment_contracts" in sql
    assert "frozen_session_count >= 126" in sql
    assert "low_volatility_forward_evaluation_runs" in sql
    assert "low_volatility_execution_compatibility_runs" in sql
    assert "low_volatility_paper_candidate_approvals" in sql
    assert "NOT paper_activation_authority_granted" in sql
    assert "NOT runtime_activation_allowed" in sql
    assert "live_trading_locked" in sql
    assert sql.count("autoquant_reject_immutable_change()") == 1
    assert "VALUES ('postgres', 50)" in sql
