from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from typing import Protocol

from autoquant.backtest.low_volatility_execution_compatibility import (
    LowVolatilityExecutionCompatibilityRun,
    LowVolatilityExecutionCompatibilitySpec,
)
from autoquant.data.models import _require_lowercase_sha256, _require_nonblank
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.low_volatility_paper_approval_store import (
    LowVolatilityPaperCandidateRecord,
    PostgresLowVolatilityPaperCandidateRepository,
)
from autoquant.execution.low_volatility_paper_deployment_contract import (
    LowVolatilityPaperDeploymentContract,
)
from autoquant.execution.low_volatility_paper_deployment_contract_store import (
    PostgresLowVolatilityPaperDeploymentContractRepository,
)
from autoquant.execution.low_volatility_paper_signal import (
    LowVolatilityPaperDailySignal,
)
from autoquant.execution.low_volatility_paper_signal_store import (
    PostgresLowVolatilityPaperSignalRepository,
)
from autoquant.web.low_volatility_execution_compatibility_run_store import (
    PostgresLowVolatilityExecutionCompatibilityRunRepository,
)
from autoquant.web.low_volatility_execution_compatibility_store import (
    PostgresLowVolatilityExecutionCompatibilityRepository,
)


class LowVolatilityPaperDeploymentBlocker(StrEnum):
    CANDIDATE_MISSING = "candidate_missing"
    CANDIDATE_RUNTIME_LOCKED = "candidate_runtime_locked"
    DEPLOYMENT_CONTRACT_MISSING = "deployment_contract_missing"
    DEPLOYMENT_CONTRACT_MISMATCH = "deployment_contract_mismatch"
    COMPATIBILITY_SPEC_MISSING = "compatibility_spec_missing"
    COMPATIBILITY_RUN_MISSING = "compatibility_run_missing"
    COMPATIBILITY_EVIDENCE_MISMATCH = "compatibility_evidence_mismatch"
    EXECUTION_TIMING_INCOMPATIBLE = "execution_timing_incompatible"
    COMPATIBILITY_RUNTIME_AUTHORITY_MISSING = "compatibility_runtime_authority_missing"
    DAILY_SIGNAL_MISSING = "daily_signal_missing"
    DAILY_SIGNAL_EVIDENCE_MISMATCH = "daily_signal_evidence_mismatch"
    DAILY_SIGNAL_EXECUTION_INCOMPATIBLE = "daily_signal_execution_incompatible"
    DAILY_SIGNAL_RUNTIME_LOCKED = "daily_signal_runtime_locked"


class LowVolatilityCandidateReader(Protocol):
    async def active(
        self,
        *,
        account_id: str,
        strategy_id: str,
    ) -> LowVolatilityPaperCandidateRecord | None: ...


class LowVolatilityCompatibilitySpecReader(Protocol):
    async def for_forward_spec(
        self,
        forward_spec_hash: str,
    ) -> LowVolatilityExecutionCompatibilitySpec: ...


class LowVolatilityDeploymentContractReader(Protocol):
    async def for_forward_spec(
        self,
        forward_spec_hash: str,
    ) -> LowVolatilityPaperDeploymentContract: ...


class LowVolatilityCompatibilityRunReader(Protocol):
    async def for_spec(
        self,
        spec_hash: str,
    ) -> LowVolatilityExecutionCompatibilityRun: ...


class LowVolatilityDailySignalReader(Protocol):
    async def for_session(
        self,
        *,
        candidate_approval_hash: str,
        session_date: date,
    ) -> LowVolatilityPaperDailySignal | None: ...


@dataclass(frozen=True, slots=True)
class LowVolatilityPaperDeploymentReadiness:
    account_id: str
    strategy_id: str
    session_date: date
    candidate_approval_hash: str | None
    compatibility_spec_hash: str | None
    compatibility_run_hash: str | None
    daily_signal_hash: str | None
    blockers: tuple[LowVolatilityPaperDeploymentBlocker, ...]
    live_trading_locked: bool = True
    paper_activation_allowed: bool = False

    def __post_init__(self) -> None:
        _require_nonblank(self.account_id, name="paper deployment account")
        _require_nonblank(self.strategy_id, name="paper deployment strategy")
        for name, value in (
            (
                "candidate_approval_hash",
                self.candidate_approval_hash,
            ),
            (
                "compatibility_spec_hash",
                self.compatibility_spec_hash,
            ),
            (
                "compatibility_run_hash",
                self.compatibility_run_hash,
            ),
            ("daily_signal_hash", self.daily_signal_hash),
        ):
            if value is not None:
                _require_lowercase_sha256(value, name=name)
        blockers = tuple(self.blockers)
        if (
            not blockers
            or len(set(blockers)) != len(blockers)
            or any(
                not isinstance(
                    value,
                    LowVolatilityPaperDeploymentBlocker,
                )
                for value in blockers
            )
            or not self.live_trading_locked
            or self.paper_activation_allowed
        ):
            raise ValueError("low-volatility paper deployment readiness is invalid")
        object.__setattr__(self, "blockers", blockers)

    @property
    def candidate_present(self) -> bool:
        return self.candidate_approval_hash is not None

    @property
    def ready_for_runtime(self) -> bool:
        return False


class LowVolatilityPaperDeploymentGate:
    """Read deployment evidence while v46-v49 runtime authority stays locked."""

    def __init__(
        self,
        *,
        account_id: str,
        strategy_id: str,
        candidates: LowVolatilityCandidateReader,
        contracts: LowVolatilityDeploymentContractReader,
        compatibility_specs: LowVolatilityCompatibilitySpecReader,
        compatibility_runs: LowVolatilityCompatibilityRunReader,
        signals: LowVolatilityDailySignalReader,
    ) -> None:
        _require_nonblank(account_id, name="paper deployment account")
        _require_nonblank(strategy_id, name="paper deployment strategy")
        self._account_id = account_id
        self._strategy_id = strategy_id
        self._candidates = candidates
        self._contracts = contracts
        self._compatibility_specs = compatibility_specs
        self._compatibility_runs = compatibility_runs
        self._signals = signals

    async def inspect(
        self,
        *,
        session_date: date,
    ) -> LowVolatilityPaperDeploymentReadiness:
        record = await self._candidates.active(
            account_id=self._account_id,
            strategy_id=self._strategy_id,
        )
        if record is None:
            return self._report(
                session_date=session_date,
                blockers=(LowVolatilityPaperDeploymentBlocker.CANDIDATE_MISSING,),
            )
        approval = record.approval
        if (
            approval.account_id != self._account_id
            or approval.strategy_id != self._strategy_id
            or approval.execution_mode != "paper"
            or not approval.live_trading_locked
        ):
            raise PersistenceUnavailableError("low-volatility candidate deployment identity failed")
        blockers: list[LowVolatilityPaperDeploymentBlocker] = []
        if not approval.runtime_activation_allowed:
            blockers.append(LowVolatilityPaperDeploymentBlocker.CANDIDATE_RUNTIME_LOCKED)
        contract: LowVolatilityPaperDeploymentContract | None
        try:
            contract = await self._contracts.for_forward_spec(approval.forward_spec_hash)
        except LookupError:
            contract = None
            blockers.append(LowVolatilityPaperDeploymentBlocker.DEPLOYMENT_CONTRACT_MISSING)
        if contract is not None and (
            contract.forward_spec_hash != approval.forward_spec_hash
            or contract.source_spec_hash != approval.source_spec_hash
        ):
            blockers.append(LowVolatilityPaperDeploymentBlocker.DEPLOYMENT_CONTRACT_MISMATCH)

        spec: LowVolatilityExecutionCompatibilitySpec | None
        try:
            spec = await self._compatibility_specs.for_forward_spec(approval.forward_spec_hash)
        except LookupError:
            spec = None
            blockers.append(LowVolatilityPaperDeploymentBlocker.COMPATIBILITY_SPEC_MISSING)

        run: LowVolatilityExecutionCompatibilityRun | None = None
        if spec is not None:
            try:
                run = await self._compatibility_runs.for_spec(spec.spec_hash)
            except LookupError:
                blockers.append(LowVolatilityPaperDeploymentBlocker.COMPATIBILITY_RUN_MISSING)
        if run is not None:
            if spec is None:
                raise PersistenceUnavailableError("compatibility run is missing its specification")
            if (
                contract is not None
                and contract.compatibility_spec_hash != spec.spec_hash
                and (
                    LowVolatilityPaperDeploymentBlocker.DEPLOYMENT_CONTRACT_MISMATCH not in blockers
                )
            ):
                blockers.append(LowVolatilityPaperDeploymentBlocker.DEPLOYMENT_CONTRACT_MISMATCH)
            if (
                run.original_evaluation_result_hash != approval.evaluation_result_hash
                or run.forward_spec_hash != approval.forward_spec_hash
                or run.source_spec_hash != approval.source_spec_hash
                or run.compatibility_spec_hash != spec.spec_hash
            ):
                blockers.append(LowVolatilityPaperDeploymentBlocker.COMPATIBILITY_EVIDENCE_MISMATCH)
            if not run.execution_timing_compatible:
                blockers.append(LowVolatilityPaperDeploymentBlocker.EXECUTION_TIMING_INCOMPATIBLE)
            if not run.runtime_activation_allowed:
                blockers.append(
                    LowVolatilityPaperDeploymentBlocker.COMPATIBILITY_RUNTIME_AUTHORITY_MISSING
                )

        signal = await self._signals.for_session(
            candidate_approval_hash=approval.approval_hash,
            session_date=session_date,
        )
        if signal is None:
            blockers.append(LowVolatilityPaperDeploymentBlocker.DAILY_SIGNAL_MISSING)
        else:
            if (
                signal.candidate_approval_hash != approval.approval_hash
                or signal.account_id != approval.account_id
                or signal.strategy_id != approval.strategy_id
                or signal.source_spec_hash != approval.source_spec_hash
                or signal.risk_policy_hash != approval.risk_policy_hash
                or signal.session_date != session_date
            ):
                blockers.append(LowVolatilityPaperDeploymentBlocker.DAILY_SIGNAL_EVIDENCE_MISMATCH)
            if not signal.execution_timing_compatible:
                blockers.append(
                    LowVolatilityPaperDeploymentBlocker.DAILY_SIGNAL_EXECUTION_INCOMPATIBLE
                )
            if not signal.runtime_activation_allowed:
                blockers.append(LowVolatilityPaperDeploymentBlocker.DAILY_SIGNAL_RUNTIME_LOCKED)
            if not signal.live_trading_locked:
                raise PersistenceUnavailableError("low-volatility signal live lock failed")

        return self._report(
            session_date=session_date,
            approval_hash=approval.approval_hash,
            compatibility_spec_hash=(None if spec is None else spec.spec_hash),
            compatibility_run_hash=(None if run is None else run.run_hash),
            signal_hash=(None if signal is None else signal.signal_hash),
            blockers=tuple(blockers),
        )

    def _report(
        self,
        *,
        session_date: date,
        blockers: tuple[
            LowVolatilityPaperDeploymentBlocker,
            ...,
        ],
        approval_hash: str | None = None,
        compatibility_spec_hash: str | None = None,
        compatibility_run_hash: str | None = None,
        signal_hash: str | None = None,
    ) -> LowVolatilityPaperDeploymentReadiness:
        return LowVolatilityPaperDeploymentReadiness(
            account_id=self._account_id,
            strategy_id=self._strategy_id,
            session_date=session_date,
            candidate_approval_hash=approval_hash,
            compatibility_spec_hash=compatibility_spec_hash,
            compatibility_run_hash=compatibility_run_hash,
            daily_signal_hash=signal_hash,
            blockers=blockers,
        )


class PostgresLowVolatilityPaperDeploymentReader:
    """Own the immutable readers used by the deployment gate."""

    def __init__(
        self,
        *,
        candidates: PostgresLowVolatilityPaperCandidateRepository,
        contracts: (PostgresLowVolatilityPaperDeploymentContractRepository),
        compatibility_specs: (PostgresLowVolatilityExecutionCompatibilityRepository),
        compatibility_runs: (PostgresLowVolatilityExecutionCompatibilityRunRepository),
        signals: PostgresLowVolatilityPaperSignalRepository,
        account_id: str,
        strategy_id: str,
    ) -> None:
        self._candidates = candidates
        self._contracts = contracts
        self._compatibility_specs = compatibility_specs
        self._compatibility_runs = compatibility_runs
        self._signals = signals
        self._gate = LowVolatilityPaperDeploymentGate(
            account_id=account_id,
            strategy_id=strategy_id,
            candidates=candidates,
            contracts=contracts,
            compatibility_specs=compatibility_specs,
            compatibility_runs=compatibility_runs,
            signals=signals,
        )

    @classmethod
    def connect(
        cls,
        *,
        dsn: str,
        account_id: str,
        strategy_id: str,
        schema: str = "public",
    ) -> PostgresLowVolatilityPaperDeploymentReader:
        candidates = PostgresLowVolatilityPaperCandidateRepository.connect(
            dsn=dsn,
            schema=schema,
        )
        contracts = PostgresLowVolatilityPaperDeploymentContractRepository.connect(
            dsn=dsn,
            schema=schema,
        )
        specs = PostgresLowVolatilityExecutionCompatibilityRepository.connect(
            dsn=dsn,
            schema=schema,
        )
        runs = PostgresLowVolatilityExecutionCompatibilityRunRepository.connect(
            dsn=dsn,
            schema=schema,
        )
        signals = PostgresLowVolatilityPaperSignalRepository.connect(
            dsn=dsn,
            schema=schema,
        )
        return cls(
            candidates=candidates,
            contracts=contracts,
            compatibility_specs=specs,
            compatibility_runs=runs,
            signals=signals,
            account_id=account_id,
            strategy_id=strategy_id,
        )

    async def close(self) -> None:
        await self._signals.close()
        await self._compatibility_runs.close()
        await self._compatibility_specs.close()
        await self._contracts.close()
        await self._candidates.close()

    async def inspect(
        self,
        *,
        session_date: date,
    ) -> LowVolatilityPaperDeploymentReadiness:
        return await self._gate.inspect(session_date=session_date)

    async def contract_for_forward_spec(
        self,
        forward_spec_hash: str,
    ) -> LowVolatilityPaperDeploymentContract:
        return await self._contracts.for_forward_spec(forward_spec_hash)
