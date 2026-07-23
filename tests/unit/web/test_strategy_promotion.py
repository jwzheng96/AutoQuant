from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.data.models import DatasetManifest
from autoquant.risk.models import RiskPolicy
from autoquant.web.models import OperatorJobState
from autoquant.web.strategy_promotion import PaperStrategyPromotionService

NOW = datetime(2026, 7, 23, 1, tzinfo=UTC)
INSTRUMENT = "600000.XSHG"


def _manifest(*, suffix: str) -> DatasetManifest:
    return DatasetManifest(
        source="tushare",
        instruments=(INSTRUMENT,),
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2026, 7, 22, tzinfo=UTC),
        as_of=datetime(2026, 7, 22, 8, tzinfo=UTC),
        record_hashes=(suffix * 64,),
        quality_report_hash="f" * 64,
        production_complete=True,
        row_count=1,
    )


def _detail(validation_manifest: DatasetManifest, *, status: str = "research_candidate"):
    request = SimpleNamespace(
        manifest_hash=validation_manifest.manifest_hash,
        instrument=INSTRUMENT,
        allocation=Decimal("0.20"),
        slippage_bps=Decimal("5"),
    )
    experiment = SimpleNamespace(
        state=OperatorJobState.COMPLETED,
        result_hash="a" * 64,
        as_of=NOW,
        summary=SimpleNamespace(evidence_status=status, gate_failures=()),
        request=request,
    )
    folds = tuple(
        SimpleNamespace(
            selected=SimpleNamespace(fast_sessions=fast, slow_sessions=slow)
        )
        for fast, slow in ((10, 30), (5, 20), (5, 20))
    )
    return SimpleNamespace(experiment=experiment, folds=folds)


@pytest.mark.asyncio
async def test_promotion_requires_gate_passing_oos_and_persists_modal_parameters() -> None:
    validation_manifest = _manifest(suffix="1")
    signal_manifest = _manifest(suffix="2")
    validations = MagicMock()
    validations.detail = AsyncMock(return_value=_detail(validation_manifest))
    controls = MagicMock()
    controls.read_manifest = AsyncMock(
        side_effect=(validation_manifest, signal_manifest)
    )
    datasets = MagicMock()
    datasets.query = AsyncMock()
    registrations = MagicMock()
    registrations.approve = AsyncMock(side_effect=lambda value: value)
    service = PaperStrategyPromotionService(
        validations=validations,
        controls=controls,
        datasets=datasets,
        registrations=registrations,
    )
    service._validate_signal_dataset = MagicMock()
    rules = AshareRuleBook().resolve(
        INSTRUMENT,
        date(2026, 7, 23),
        SecurityStatus(risk_warning=False, listing_session_number=1000),
    )
    policy = RiskPolicy(
        allowed_instruments=(INSTRUMENT,),
        max_position_weight=Decimal("0.20"),
        max_gross_exposure=Decimal("0.20"),
    )

    registration = await service.approve_sma(
        account_id="paper-main",
        strategy_id="validated-sma-paper",
        experiment_id=uuid4(),
        signal_manifest_hash=signal_manifest.manifest_hash,
        rules=rules,
        policy=policy,
        approved_by="operator",
        approved_at=NOW,
    )

    assert (registration.fast_sessions, registration.slow_sessions) == (5, 20)
    assert registration.execution_mode == "paper"
    assert registration.validation_result_hash == "a" * 64
    registrations.approve.assert_awaited_once_with(registration)


@pytest.mark.asyncio
async def test_promotion_rejects_non_candidate_and_risk_policy_drift() -> None:
    validation_manifest = _manifest(suffix="1")
    signal_manifest = _manifest(suffix="2")
    validations = MagicMock()
    validations.detail = AsyncMock(
        return_value=_detail(validation_manifest, status="rejected")
    )
    controls = MagicMock()
    controls.read_manifest = AsyncMock(
        side_effect=(validation_manifest, signal_manifest)
    )
    service = PaperStrategyPromotionService(
        validations=validations,
        controls=controls,
        datasets=MagicMock(),
        registrations=MagicMock(),
    )
    rules = AshareRuleBook().resolve(
        INSTRUMENT,
        date(2026, 7, 23),
        SecurityStatus(risk_warning=False, listing_session_number=1000),
    )
    policy = RiskPolicy(
        allowed_instruments=(INSTRUMENT,),
        max_position_weight=Decimal("0.20"),
        max_gross_exposure=Decimal("0.20"),
    )

    with pytest.raises(ValueError, match="gate-passing"):
        await service.approve_sma(
            account_id="paper-main",
            strategy_id="validated-sma-paper",
            experiment_id=uuid4(),
            signal_manifest_hash=signal_manifest.manifest_hash,
            rules=rules,
            policy=policy,
            approved_by="operator",
            approved_at=NOW,
        )
