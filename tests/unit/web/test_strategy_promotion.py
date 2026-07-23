from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.data.models import DatasetManifest
from autoquant.execution.validated_sma import ValidatedSmaRegistration
from autoquant.risk.models import RiskPolicy
from autoquant.web.models import OperatorJobState
from autoquant.web.strategy_promotion import (
    PaperPortfolioComponentApproval,
    PaperPortfolioPromotionService,
    PaperStrategyPromotionService,
)

NOW = datetime(2026, 7, 23, 1, tzinfo=UTC)
INSTRUMENT = "600000.XSHG"


def _manifest(
    *,
    suffix: str,
    instruments: tuple[str, ...] = (INSTRUMENT,),
) -> DatasetManifest:
    return DatasetManifest(
        source="tushare",
        instruments=instruments,
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2026, 7, 22, tzinfo=UTC),
        as_of=datetime(2026, 7, 22, 8, tzinfo=UTC),
        record_hashes=tuple(
            f"{int(suffix, 16) + index:x}" * 64
            for index in range(len(instruments))
        ),
        quality_report_hash="f" * 64,
        production_complete=True,
        row_count=len(instruments),
    )


def _detail(
    validation_manifest: DatasetManifest,
    *,
    experiment_id: Any,
    instrument: str = INSTRUMENT,
    status: str = "research_candidate",
    returns: tuple[str, ...] = (
        "0.010",
        "0.020",
        "-0.005",
        "0.015",
        "0.003",
        "0.012",
    ),
) -> Any:
    request = SimpleNamespace(
        manifest_hash=validation_manifest.manifest_hash,
        instrument=instrument,
        initial_cash=Decimal("1000000"),
        allocation=Decimal("0.20"),
        slippage_bps=Decimal("5"),
    )
    experiment = SimpleNamespace(
        experiment_id=experiment_id,
        state=OperatorJobState.COMPLETED,
        result_hash="a" * 64,
        as_of=NOW,
        summary=SimpleNamespace(evidence_status=status, gate_failures=()),
        request=request,
    )
    selections = (
        (10, 30),
        (5, 20),
        (5, 20),
        (5, 20),
        (10, 30),
        (5, 20),
    )
    folds = tuple(
        SimpleNamespace(
            sequence=sequence,
            test_start=date(2025, sequence, 1),
            test_end=date(2025, sequence, 20),
            selected=SimpleNamespace(
                fast_sessions=fast,
                slow_sessions=slow,
            ),
            test=SimpleNamespace(
                total_return=Decimal(total_return),
                max_drawdown=Decimal("0.01"),
            ),
        )
        for sequence, ((fast, slow), total_return) in enumerate(
            zip(selections, returns, strict=True),
            start=1,
        )
    )
    return SimpleNamespace(experiment=experiment, folds=folds)


@pytest.mark.asyncio
async def test_promotion_requires_gate_passing_oos_and_persists_modal_parameters() -> None:
    validation_manifest = _manifest(
        suffix="1",
        instruments=(
            "000001.XSHE",
            INSTRUMENT,
            "600519.XSHG",
        ),
    )
    signal_manifest = _manifest(suffix="2")
    experiment_id = uuid4()
    validations = MagicMock()
    validations.detail = AsyncMock(
        return_value=_detail(
            validation_manifest,
            experiment_id=experiment_id,
        )
    )
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
    cast(Any, service)._validate_signal_dataset = MagicMock()
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
        experiment_id=experiment_id,
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
    experiment_id = uuid4()
    validations = MagicMock()
    validations.detail = AsyncMock(
        return_value=_detail(
            validation_manifest,
            experiment_id=experiment_id,
            status="rejected",
        )
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
            experiment_id=experiment_id,
            signal_manifest_hash=signal_manifest.manifest_hash,
            rules=rules,
            policy=policy,
            approved_by="operator",
            approved_at=NOW,
        )


@pytest.mark.asyncio
async def test_portfolio_promotion_binds_three_independent_components() -> None:
    instruments = (
        "000001.XSHE",
        "600000.XSHG",
        "600519.XSHG",
    )
    policy = RiskPolicy(
        allowed_instruments=instruments,
        max_position_weight=Decimal("0.20"),
        max_gross_exposure=Decimal("0.60"),
    )
    rule_book = AshareRuleBook()
    rules = tuple(
        rule_book.resolve(
            instrument,
            date(2026, 7, 23),
            SecurityStatus(
                risk_warning=False,
                listing_session_number=1000,
            ),
        )
        for instrument in instruments
    )
    experiment_ids = tuple(uuid4() for _ in instruments)
    component_registrations = tuple(
        ValidatedSmaRegistration(
            account_id="paper-main",
            strategy_id="validated-sma-paper",
            strategy_version=f"sma-paper-v1:{index}",
            experiment_id=experiment_id,
            validation_result_hash=f"{index + 1:x}" * 64,
            validation_manifest_hash=f"{index + 4:x}" * 64,
            signal_manifest_hash=f"{index + 7:x}" * 64,
            signal_manifest_as_of=datetime(
                2026,
                7,
                22,
                8,
                tzinfo=UTC,
            ),
            instrument=instrument,
            fast_sessions=5,
            slow_sessions=20,
            allocation=Decimal("0.20"),
            slippage_bps=Decimal("5"),
            risk_policy_hash=policy.policy_hash,
            rule_version=rule.rule_version,
            approved_by="operator",
            approved_at=NOW,
        )
        for index, (experiment_id, instrument, rule) in enumerate(
            zip(experiment_ids, instruments, rules, strict=True)
        )
    )
    valuation_manifest = DatasetManifest(
        source="tushare",
        instruments=instruments,
        start_time=datetime(2026, 7, 22, tzinfo=UTC),
        end_time=datetime(2026, 7, 22, 7, tzinfo=UTC),
        as_of=datetime(2026, 7, 22, 8, tzinfo=UTC),
        record_hashes=("a" * 64, "b" * 64, "c" * 64),
        quality_report_hash="f" * 64,
        production_complete=True,
        row_count=3,
    )
    registrations = MagicMock()
    registrations.approve = AsyncMock(side_effect=lambda value: value)
    controls = MagicMock()
    controls.read_manifest = AsyncMock(
        return_value=valuation_manifest
    )
    datasets = MagicMock()
    datasets.query = AsyncMock(return_value=MagicMock())
    validations = MagicMock()
    validations.detail = AsyncMock(
        side_effect=tuple(
            _detail(
                valuation_manifest,
                experiment_id=experiment_id,
                instrument=instrument,
                returns=returns,
            )
            for experiment_id, instrument, returns in zip(
                experiment_ids,
                instruments,
                (
                    (
                        "0.010",
                        "0.020",
                        "-0.005",
                        "0.015",
                        "0.003",
                        "0.012",
                    ),
                    (
                        "0.008",
                        "-0.003",
                        "0.018",
                        "0.004",
                        "0.014",
                        "0.006",
                    ),
                    (
                        "-0.002",
                        "0.011",
                        "0.005",
                        "0.017",
                        "0.007",
                        "0.009",
                    ),
                ),
                strict=True,
            )
        )
    )
    service = PaperPortfolioPromotionService(
        validations=validations,
        controls=controls,
        datasets=datasets,
        registrations=registrations,
    )
    cast(Any, service._component_builder)._prepare_sma = AsyncMock(
        side_effect=component_registrations
    )
    cast(Any, service)._validate_valuation_dataset = MagicMock()

    registration = await service.approve_sma_portfolio(
        account_id="paper-main",
        strategy_id="validated-sma-paper",
        components=tuple(
            PaperPortfolioComponentApproval(
                experiment_id=experiment_id,
                signal_manifest_hash=f"{index + 7:x}" * 64,
                rules=rule,
            )
            for index, (experiment_id, rule) in enumerate(
                zip(experiment_ids, rules, strict=True)
            )
        ),
        valuation_manifest_hash=valuation_manifest.manifest_hash,
        policy=policy,
        expected_initial_cash=Decimal("1000000"),
        approved_by="operator",
        approved_at=NOW,
    )

    assert registration.instruments == instruments
    assert registration.total_allocation == Decimal("0.60")
    assert registration.valuation_manifest_hash == (
        valuation_manifest.manifest_hash
    )
    assert (
        cast(Any, service._component_builder)._prepare_sma.await_count
        == 3
    )
    registrations.approve.assert_awaited_once_with(registration)
