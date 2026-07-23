from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol
from uuid import UUID

from autoquant.backtest.models import InstrumentRules
from autoquant.backtest.validation import SmaParameters
from autoquant.clock import to_utc
from autoquant.data.daily_ingestion import ValidatedDailyDataset
from autoquant.data.ingestion import ControlRepository
from autoquant.data.models import _canonical_hash
from autoquant.execution.portfolio_validation import (
    PortfolioOosComponentEvidence,
    PortfolioOosFold,
    PortfolioOosPolicy,
    assess_portfolio_oos,
)
from autoquant.execution.validated_sma import (
    DailyDatasetReader,
    ValidatedSmaRegistration,
    select_deployment_parameters,
)
from autoquant.execution.validated_sma_portfolio import (
    ValidatedSmaPortfolioRegistration,
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


class PortfolioRegistrationWriter(Protocol):
    async def approve(
        self,
        registration: ValidatedSmaPortfolioRegistration,
    ) -> ValidatedSmaPortfolioRegistration: ...


@dataclass(frozen=True, slots=True)
class PaperPortfolioComponentApproval:
    experiment_id: UUID
    signal_manifest_hash: str
    rules: InstrumentRules


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
        registration = await self._prepare_sma(
            account_id=account_id,
            strategy_id=strategy_id,
            experiment_id=experiment_id,
            signal_manifest_hash=signal_manifest_hash,
            rules=rules,
            policy=policy,
            approved_by=approved_by,
            approved_at=approved_at,
            allow_portfolio_policy=False,
        )
        return await self._registrations.approve(registration)

    async def _prepare_sma(
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
        allow_portfolio_policy: bool,
        detail: ValidationExperimentDetail | None = None,
    ) -> ValidatedSmaRegistration:
        instant = to_utc(approved_at, name="strategy approval time")
        resolved_detail = (
            await self._validations.detail(experiment_id)
            if detail is None
            else detail
        )
        experiment = resolved_detail.experiment
        summary = experiment.summary
        if (
            experiment.experiment_id != experiment_id
            or
            experiment.state is not OperatorJobState.COMPLETED
            or experiment.result_hash is None
            or experiment.as_of is None
            or summary is None
            or summary.evidence_status != "research_candidate"
            or summary.gate_failures
            or not resolved_detail.folds
        ):
            raise ValueError(
                "only a completed, gate-passing OOS research candidate can be approved"
            )
        request = experiment.request
        if (
            request.instrument != rules.instrument
            or rules.instrument not in policy.allowed_instruments
            or (
                not allow_portfolio_policy
                and len(policy.allowed_instruments) != 1
            )
            or request.allocation > policy.max_position_weight
            or (
                not allow_portfolio_policy
                and request.allocation > policy.max_gross_exposure
            )
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
                for value in resolved_detail.folds
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
        return ValidatedSmaRegistration(
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


class PaperPortfolioPromotionService:
    """Approve only a diversified set of independently passing OOS candidates."""

    def __init__(
        self,
        *,
        validations: ValidationDetailReader,
        controls: ControlRepository,
        datasets: DailyDatasetReader,
        registrations: PortfolioRegistrationWriter,
    ) -> None:
        self._controls = controls
        self._datasets = datasets
        self._registrations = registrations
        self._validations = validations
        self._component_builder = PaperStrategyPromotionService(
            validations=validations,
            controls=controls,
            datasets=datasets,
            registrations=_NoSingleRegistrationWriter(),
        )

    async def approve_sma_portfolio(
        self,
        *,
        account_id: str,
        strategy_id: str,
        components: tuple[PaperPortfolioComponentApproval, ...],
        valuation_manifest_hash: str,
        policy: RiskPolicy,
        expected_initial_cash: Decimal,
        approved_by: str,
        approved_at: datetime,
    ) -> ValidatedSmaPortfolioRegistration:
        if (
            not isinstance(expected_initial_cash, Decimal)
            or not expected_initial_cash.is_finite()
            or expected_initial_cash <= 0
        ):
            raise ValueError(
                "portfolio expected initial cash must be positive"
            )
        if (
            len(components) < 3
            or len(components) > 20
            or any(
                not isinstance(value, PaperPortfolioComponentApproval)
                for value in components
            )
        ):
            raise ValueError(
                "paper portfolio approval requires 3-20 components"
            )
        instruments = tuple(
            sorted(value.rules.instrument for value in components)
        )
        if (
            len(set(instruments)) != len(instruments)
            or instruments != tuple(sorted(policy.allowed_instruments))
        ):
            raise ValueError(
                "portfolio components must exactly match unique policy instruments"
            )
        instant = to_utc(approved_at, name="portfolio approval time")
        details = tuple(
            [
                await self._validations.detail(value.experiment_id)
                for value in components
            ]
        )
        if (
            any(
                detail.experiment.request.initial_cash
                != expected_initial_cash
                for detail in details
            )
            or len(
                {
                    detail.experiment.as_of
                    for detail in details
                }
            )
            != 1
        ):
            raise ValueError(
                "portfolio validations must share the configured capital and cutoff"
            )
        registrations = tuple(
            [
                await self._component_builder._prepare_sma(
                    account_id=account_id,
                    strategy_id=strategy_id,
                    experiment_id=value.experiment_id,
                    signal_manifest_hash=value.signal_manifest_hash,
                    rules=value.rules,
                    policy=policy,
                    approved_by=approved_by,
                    approved_at=instant,
                    allow_portfolio_policy=True,
                    detail=detail,
                )
                for value, detail in zip(
                    components,
                    details,
                    strict=True,
                )
            ]
        )
        if (
            sum(
                (value.allocation for value in registrations),
                Decimal("0"),
            )
            > policy.max_gross_exposure
        ):
            raise ValueError(
                "portfolio allocation exceeds maximum gross exposure"
            )
        valuation_manifest = await self._controls.read_manifest(
            valuation_manifest_hash
        )
        if (
            not valuation_manifest.production_complete
            or tuple(sorted(valuation_manifest.instruments)) != instruments
        ):
            raise ValueError(
                "portfolio valuation manifest is not exact and production-complete"
            )
        valuation_dataset = await self._datasets.query(
            valuation_manifest.manifest_hash,
            valuation_manifest.as_of,
        )
        self._validate_valuation_dataset(
            valuation_dataset,
            instruments=instruments,
        )
        assessment = assess_portfolio_oos(
            tuple(
                PortfolioOosComponentEvidence(
                    experiment_id=registration.experiment_id,
                    validation_result_hash=(
                        registration.validation_result_hash
                    ),
                    instrument=registration.instrument,
                    allocation=registration.allocation,
                    folds=tuple(
                        PortfolioOosFold(
                            sequence=fold.sequence,
                            test_start=fold.test_start,
                            test_end=fold.test_end,
                            total_return=fold.test.total_return,
                            max_drawdown=fold.test.max_drawdown,
                        )
                        for fold in detail.folds
                    ),
                )
                for registration, detail in zip(
                    registrations,
                    details,
                    strict=True,
                )
            ),
            policy=PortfolioOosPolicy(),
        )
        if not assessment.passed:
            raise ValueError(
                "portfolio OOS assessment failed: "
                + ",".join(assessment.gate_failures)
            )
        component_hashes = tuple(
            sorted(value.registration_hash for value in registrations)
        )
        version_hash = _canonical_hash(
            {
                "component_hashes": component_hashes,
                "oos_assessment_hash": assessment.assessment_hash,
                "valuation_manifest_hash": (
                    valuation_manifest.manifest_hash
                ),
                "version": "sma-portfolio-paper-v1",
            }
        )
        registration = ValidatedSmaPortfolioRegistration(
            account_id=account_id,
            strategy_id=strategy_id,
            strategy_version=(
                f"sma-portfolio-paper-v1:{version_hash[:12]}"
            ),
            components=registrations,
            oos_assessment=assessment,
            valuation_manifest_hash=valuation_manifest.manifest_hash,
            valuation_manifest_as_of=valuation_manifest.as_of,
            risk_policy_hash=policy.policy_hash,
            approved_by=approved_by,
            approved_at=instant,
        )
        return await self._registrations.approve(registration)

    @staticmethod
    def _validate_valuation_dataset(
        dataset: ValidatedDailyDataset,
        *,
        instruments: tuple[str, ...],
    ) -> None:
        bars_by_key = {
            (value.instrument, value.session_date)
            for value in dataset.bars
        }
        factors_by_key = {
            (value.instrument, value.session_date)
            for value in dataset.factors
        }
        if (
            not bars_by_key
            or len(bars_by_key) != len(dataset.bars)
            or len(factors_by_key) != len(dataset.factors)
            or bars_by_key != factors_by_key
            or {value[0] for value in bars_by_key} != set(instruments)
        ):
            raise ValueError(
                "valuation manifest lacks exact adjusted marks for every component"
            )


class _NoSingleRegistrationWriter:
    async def approve(
        self,
        registration: ValidatedSmaRegistration,
    ) -> ValidatedSmaRegistration:
        raise AssertionError(
            "portfolio component preparation cannot activate a single strategy"
        )
