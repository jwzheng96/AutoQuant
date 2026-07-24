from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.low_volatility_paper_deployment import (
    LowVolatilityPaperDeploymentBlocker,
    LowVolatilityPaperDeploymentGate,
)

SESSION = date(2026, 7, 27)


def _approval() -> SimpleNamespace:
    return SimpleNamespace(
        account_id="paper-main",
        strategy_id="low-volatility-paper",
        approval_hash="a" * 64,
        evaluation_result_hash="b" * 64,
        forward_spec_hash="c" * 64,
        source_spec_hash="d" * 64,
        risk_policy_hash="e" * 64,
        execution_mode="paper",
        runtime_activation_allowed=False,
        live_trading_locked=True,
    )


def _gate(
    *,
    candidate: object | None,
    spec: object | None = None,
    run: object | None = None,
    signal: object | None = None,
) -> tuple[
    LowVolatilityPaperDeploymentGate,
    AsyncMock,
    AsyncMock,
    AsyncMock,
    AsyncMock,
]:
    candidates = AsyncMock()
    specs = AsyncMock()
    runs = AsyncMock()
    signals = AsyncMock()
    candidates.active.return_value = candidate
    if spec is None:
        specs.for_forward_spec.side_effect = LookupError
    else:
        specs.for_forward_spec.return_value = spec
    if run is None:
        runs.for_spec.side_effect = LookupError
    else:
        runs.for_spec.return_value = run
    signals.for_session.return_value = signal
    return (
        LowVolatilityPaperDeploymentGate(
            account_id="paper-main",
            strategy_id="low-volatility-paper",
            candidates=cast(Any, candidates),
            compatibility_specs=cast(Any, specs),
            compatibility_runs=cast(Any, runs),
            signals=cast(Any, signals),
        ),
        candidates,
        specs,
        runs,
        signals,
    )


@pytest.mark.asyncio
async def test_deployment_gate_reports_absent_candidate_without_reads() -> None:
    gate, _, specs, runs, signals = _gate(candidate=None)

    report = await gate.inspect(session_date=SESSION)

    assert report.candidate_present is False
    assert report.ready_for_runtime is False
    assert report.paper_activation_allowed is False
    assert report.live_trading_locked is True
    assert report.blockers == (LowVolatilityPaperDeploymentBlocker.CANDIDATE_MISSING,)
    specs.for_forward_spec.assert_not_awaited()
    runs.for_spec.assert_not_awaited()
    signals.for_session.assert_not_awaited()


@pytest.mark.asyncio
async def test_deployment_gate_keeps_complete_v49_evidence_locked() -> None:
    approval = _approval()
    spec = SimpleNamespace(spec_hash="f" * 64)
    run = SimpleNamespace(
        run_hash="1" * 64,
        compatibility_spec_hash=spec.spec_hash,
        original_evaluation_result_hash=(approval.evaluation_result_hash),
        forward_spec_hash=approval.forward_spec_hash,
        source_spec_hash=approval.source_spec_hash,
        execution_timing_compatible=True,
        runtime_activation_allowed=False,
    )
    signal = SimpleNamespace(
        signal_hash="2" * 64,
        candidate_approval_hash=approval.approval_hash,
        account_id=approval.account_id,
        strategy_id=approval.strategy_id,
        source_spec_hash=approval.source_spec_hash,
        risk_policy_hash=approval.risk_policy_hash,
        session_date=SESSION,
        execution_timing_compatible=False,
        runtime_activation_allowed=False,
        live_trading_locked=True,
    )
    gate, _, _, _, _ = _gate(
        candidate=SimpleNamespace(approval=approval),
        spec=spec,
        run=run,
        signal=signal,
    )

    report = await gate.inspect(session_date=SESSION)

    assert report.candidate_present is True
    assert report.compatibility_run_hash == run.run_hash
    assert report.daily_signal_hash == signal.signal_hash
    assert report.blockers == (
        LowVolatilityPaperDeploymentBlocker.CANDIDATE_RUNTIME_LOCKED,
        LowVolatilityPaperDeploymentBlocker.COMPATIBILITY_RUNTIME_AUTHORITY_MISSING,
        LowVolatilityPaperDeploymentBlocker.DAILY_SIGNAL_EXECUTION_INCOMPATIBLE,
        LowVolatilityPaperDeploymentBlocker.DAILY_SIGNAL_RUNTIME_LOCKED,
    )
    assert report.ready_for_runtime is False


@pytest.mark.asyncio
async def test_deployment_gate_blocks_missing_daily_and_compatibility() -> None:
    approval = _approval()
    gate, _, _, _, _ = _gate(
        candidate=SimpleNamespace(approval=approval),
    )

    report = await gate.inspect(session_date=SESSION)

    assert report.blockers == (
        LowVolatilityPaperDeploymentBlocker.CANDIDATE_RUNTIME_LOCKED,
        LowVolatilityPaperDeploymentBlocker.COMPATIBILITY_SPEC_MISSING,
        LowVolatilityPaperDeploymentBlocker.DAILY_SIGNAL_MISSING,
    )


@pytest.mark.asyncio
async def test_deployment_gate_fails_closed_on_candidate_identity() -> None:
    approval = _approval()
    approval.account_id = "another-account"
    gate, _, _, _, _ = _gate(
        candidate=SimpleNamespace(approval=approval),
    )

    with pytest.raises(
        PersistenceUnavailableError,
        match="identity",
    ):
        await gate.inspect(session_date=SESSION)
