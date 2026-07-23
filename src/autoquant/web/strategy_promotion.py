from __future__ import annotations

from datetime import datetime
from typing import Protocol
from uuid import UUID

from autoquant.backtest.models import InstrumentRules
from autoquant.backtest.validation import SmaParameters
from autoquant.clock import to_utc
from autoquant.data.daily_ingestion import ValidatedDailyDataset
from autoquant.data.ingestion import ControlRepository
from autoquant.execution.validated_sma import (
    DailyDatasetReader,
    ValidatedSmaRegistration,
    select_deployment_parameters,
)
from autoquant.risk.models import RiskPolicy
from autoquant.web.models import (
    OperatorJobState,
    ValidationExperimentDetail,
)


class ValidationDetailReader(Protocol):
    async def detail(
        self,
        experiment_id: UUID,
    ) -> ValidationExperimentDetail: ...


class RegistrationWriter(Protocol):
    async def approve(
        self,
        registration: ValidatedSmaRegistration,
    ) -> ValidatedSmaRegistration: ...


class PaperStrategyPromotionService:
    """Turn an integrity-checked research candidate into a paper-only artifact."""

    def __init__(
        self,
        *,
        validations: ValidationDetailReader,
        controls: ControlRepository,
        datasets: DailyDatasetReader,
        registrations: RegistrationWriter,
    ) -> None:
        self._validations = validations
        self._controls = controls
        self._datasets = datasets
        self._registrations = registrations

    async def approve_sma(
        self,
        *,
        account_id: str,
        strategy_id: str,
        experiment_id: UUID,
        signal_manifest_hash: str,
        rules: InstrumentRules,
        policy: RiskPolicy,
        approved_by: str,
        approved_at: datetime,
    ) -> ValidatedSmaRegistration:
        instant = to_utc(approved_at, name="strategy approval time")
        detail = await self._validations.detail(experiment_id)
        experiment = detail.experiment
        summary = experiment.summary
        if (
            experiment.state is not OperatorJobState.COMPLETED
            or experiment.result_hash is None
            or experiment.as_of is None
            or summary is None
            or summary.evidence_status != "research_candidate"
            or summary.gate_failures
            or not detail.folds
        ):
            raise ValueError(
                "only a completed, gate-passing OOS research candidate can be approved"
            )
        request = experiment.request
        if (
            request.instrument != rules.instrument
            or rules.instrument not in policy.allowed_instruments
            or len(policy.allowed_instruments) != 1
            or request.allocation
            > min(policy.max_position_weight, policy.max_gross_exposure)
        ):
            raise ValueError(
                "validation allocation or instrument exceeds paper risk controls"
            )
        selected = select_deployment_parameters(
            tuple(
                SmaParameters(
                    value.selected.fast_sessions,
                    value.selected.slow_sessions,
                )
                for value in detail.folds
            )
        )
        validation_manifest = await self._controls.read_manifest(
            request.manifest_hash
        )
        signal_manifest = await self._controls.read_manifest(
            signal_manifest_hash
        )
        for name, manifest in (
            ("validation", validation_manifest),
            ("signal", signal_manifest),
        ):
            if (
                not manifest.production_complete
                or manifest.instruments != (request.instrument,)
            ):
                raise ValueError(
                    f"{name} manifest is not exact and production-complete"
                )
        if validation_manifest.manifest_hash != request.manifest_hash:
            raise ValueError("validation manifest hash does not match the experiment")
        signal_dataset = await self._datasets.query(
            signal_manifest.manifest_hash,
            signal_manifest.as_of,
        )
        self._validate_signal_dataset(
            signal_dataset,
            instrument=request.instrument,
            slow_sessions=selected.slow_sessions,
        )
        strategy_version = (
            f"sma-paper-v1:{experiment.result_hash[:12]}:"
            f"{selected.fast_sessions}-{selected.slow_sessions}"
        )
        registration = ValidatedSmaRegistration(
            account_id=account_id,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            experiment_id=experiment_id,
            validation_result_hash=experiment.result_hash,
            validation_manifest_hash=validation_manifest.manifest_hash,
            signal_manifest_hash=signal_manifest.manifest_hash,
            signal_manifest_as_of=signal_manifest.as_of,
            instrument=request.instrument,
            fast_sessions=selected.fast_sessions,
            slow_sessions=selected.slow_sessions,
            allocation=request.allocation,
            slippage_bps=request.slippage_bps,
            risk_policy_hash=policy.policy_hash,
            rule_version=rules.rule_version,
            approved_by=approved_by,
            approved_at=instant,
        )
        return await self._registrations.approve(registration)

    @staticmethod
    def _validate_signal_dataset(
        dataset: ValidatedDailyDataset,
        *,
        instrument: str,
        slow_sessions: int,
    ) -> None:
        bars = tuple(value for value in dataset.bars if value.instrument == instrument)
        factors = tuple(
            value for value in dataset.factors if value.instrument == instrument
        )
        if (
            len(bars) < slow_sessions
            or len(bars) != len(dataset.bars)
            or len(factors) != len(bars)
            or len(factors) != len(dataset.factors)
            or len({value.session_date for value in bars}) != len(bars)
            or {value.session_date for value in factors}
            != {value.session_date for value in bars}
            or len({value.factor for value in factors}) != 1
        ):
            raise ValueError(
                "signal manifest lacks an exact corporate-action-safe SMA history"
            )
