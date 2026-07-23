from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from itertools import pairwise

from autoquant.backtest.dynamic_panel import (
    DynamicMarketPanel,
    DynamicMarketSession,
)
from autoquant.backtest.dynamic_portfolio import (
    DYNAMIC_PORTFOLIO_BENCHMARK_VERSION,
    DynamicPortfolioEvidencePolicy,
    DynamicPortfolioResearchSpec,
)
from autoquant.backtest.dynamic_strategy import (
    DynamicEqualWeightBenchmarkPolicy,
    DynamicMomentumOrderPolicy,
)
from autoquant.backtest.engine import BacktestEngine
from autoquant.backtest.ledger import ExecutionModel
from autoquant.backtest.models import (
    BacktestResult,
    ExecutionState,
    backtest_artifact_hash,
)
from autoquant.backtest.portfolio_validation import (
    CrossSectionalMomentumParameters,
)
from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
)

DYNAMIC_VALIDATION_VERSION = "dynamic-nested-walk-forward-v1"
DYNAMIC_SELECTION_OBJECTIVE_VERSION = (
    "return-minus-drawdown-turnover-v1"
)
DYNAMIC_EVIDENCE_ASSESSMENT_VERSION = (
    "dynamic-validation-assessment-v1"
)


@dataclass(frozen=True, slots=True)
class DynamicCandidateEvaluation:
    parameters: CrossSectionalMomentumParameters
    score: Decimal
    result: BacktestResult
    evaluation_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            self.result.strategy_id != self.parameters.strategy_id
            or not isinstance(self.score, Decimal)
            or not self.score.is_finite()
            or self.score != _selection_score(self.result)
        ):
            raise ValueError(
                "dynamic candidate evaluation is inconsistent"
            )
        object.__setattr__(
            self,
            "evaluation_hash",
            _canonical_hash(
                {
                    "artifact_hash": backtest_artifact_hash(
                        self.result
                    ),
                    "parameters": self.parameters.payload(),
                    "score": _decimal_text(self.score),
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class DynamicValidationFold:
    sequence: int
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    selected: CrossSectionalMomentumParameters
    selection_score: Decimal
    candidate_evaluations: tuple[DynamicCandidateEvaluation, ...]
    training_result: BacktestResult
    test_result: BacktestResult
    benchmark_result: BacktestResult
    fold_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            self.sequence < 1
            or not self.train_start
            <= self.train_end
            < self.test_start
            <= self.test_end
            or self.training_result.strategy_id
            != self.selected.strategy_id
            or self.test_result.strategy_id
            != self.selected.strategy_id
            or self.benchmark_result.strategy_id
            != DYNAMIC_PORTFOLIO_BENCHMARK_VERSION
            or not isinstance(self.selection_score, Decimal)
            or not self.selection_score.is_finite()
        ):
            raise ValueError("dynamic validation fold is inconsistent")
        results = (
            self.training_result,
            self.test_result,
            self.benchmark_result,
        )
        if (
            len({value.manifest_hash for value in results}) != 1
            or len({value.as_of for value in results}) != 1
        ):
            raise ValueError(
                "dynamic validation fold evidence is inconsistent"
            )
        evaluations = tuple(self.candidate_evaluations)
        if (
            not evaluations
            or len(
                {
                    value.parameters
                    for value in evaluations
                }
            )
            != len(evaluations)
        ):
            raise ValueError(
                "dynamic validation candidate evidence is inconsistent"
            )
        winner = max(
            evaluations,
            key=lambda value: (
                value.score,
                -value.parameters.lookback_sessions,
                -value.parameters.rebalance_sessions,
                -value.parameters.selection_count,
            ),
        )
        if (
            winner.parameters != self.selected
            or winner.score != self.selection_score
            or winner.result != self.training_result
        ):
            raise ValueError(
                "dynamic validation selection cannot be reproduced"
            )
        object.__setattr__(
            self,
            "candidate_evaluations",
            evaluations,
        )
        if (
            self.training_result.snapshots[0].session_date
            != self.train_start
            or self.training_result.snapshots[-1].session_date
            != self.train_end
            or self.test_result.snapshots[0].session_date
            != self.test_start
            or self.test_result.snapshots[-1].session_date
            != self.test_end
            or self.benchmark_result.snapshots[0].session_date
            != self.test_start
            or self.benchmark_result.snapshots[-1].session_date
            != self.test_end
        ):
            raise ValueError(
                "dynamic validation results do not cover the fold"
            )
        object.__setattr__(
            self,
            "fold_hash",
            _canonical_hash(
                {
                    "benchmark_artifact_hash": backtest_artifact_hash(
                        self.benchmark_result
                    ),
                    "candidate_evaluation_hashes": [
                        value.evaluation_hash
                        for value in evaluations
                    ],
                    "selected": self.selected.payload(),
                    "selection_score": _decimal_text(
                        self.selection_score
                    ),
                    "sequence": self.sequence,
                    "test_artifact_hash": backtest_artifact_hash(
                        self.test_result
                    ),
                    "test_end": self.test_end.isoformat(),
                    "test_start": self.test_start.isoformat(),
                    "train_end": self.train_end.isoformat(),
                    "train_start": self.train_start.isoformat(),
                    "training_artifact_hash": backtest_artifact_hash(
                        self.training_result
                    ),
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class DynamicValidationResult:
    panel_hash: str
    spec_hash: str
    dataset_manifest_hash: str
    as_of: datetime
    folds: tuple[DynamicValidationFold, ...]
    compounded_oos_return: Decimal
    benchmark_compounded_oos_return: Decimal
    excess_oos_return: Decimal
    profitable_fold_rate: Decimal
    worst_oos_drawdown: Decimal
    mean_training_return: Decimal
    selection_optimism: Decimal
    rejected_order_count: int
    version: str = DYNAMIC_VALIDATION_VERSION
    objective_version: str = DYNAMIC_SELECTION_OBJECTIVE_VERSION
    result_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.panel_hash, "dynamic validation panel hash"),
            (self.spec_hash, "dynamic validation spec hash"),
            (
                self.dataset_manifest_hash,
                "dynamic validation dataset manifest hash",
            ),
        ):
            _require_lowercase_sha256(value, name=name)
        folds = tuple(self.folds)
        if (
            not folds
            or tuple(value.sequence for value in folds)
            != tuple(range(1, len(folds) + 1))
            or any(
                current.test_end >= following.test_start
                for current, following in pairwise(folds)
            )
            or self.version != DYNAMIC_VALIDATION_VERSION
            or self.objective_version
            != DYNAMIC_SELECTION_OBJECTIVE_VERSION
            or any(
                result.manifest_hash != self.dataset_manifest_hash
                or result.as_of != self.as_of
                for fold in folds
                for result in (
                    fold.training_result,
                    fold.test_result,
                    fold.benchmark_result,
                )
            )
        ):
            raise ValueError("dynamic validation result is inconsistent")
        object.__setattr__(self, "folds", folds)
        test_returns = tuple(
            value.test_result.total_return for value in folds
        )
        benchmark_returns = tuple(
            value.benchmark_result.total_return for value in folds
        )
        training_returns = tuple(
            value.training_result.total_return for value in folds
        )
        expected_oos = _compound(test_returns)
        expected_benchmark = _compound(benchmark_returns)
        mean_test = sum(test_returns, Decimal("0")) / Decimal(
            len(folds)
        )
        expected = (
            (self.compounded_oos_return, expected_oos),
            (
                self.benchmark_compounded_oos_return,
                expected_benchmark,
            ),
            (
                self.excess_oos_return,
                expected_oos - expected_benchmark,
            ),
            (
                self.profitable_fold_rate,
                Decimal(sum(value > 0 for value in test_returns))
                / Decimal(len(folds)),
            ),
            (
                self.worst_oos_drawdown,
                max(value.test_result.max_drawdown for value in folds),
            ),
            (
                self.mean_training_return,
                sum(training_returns, Decimal("0"))
                / Decimal(len(folds)),
            ),
            (
                self.selection_optimism,
                self.mean_training_return - mean_test,
            ),
            (
                self.rejected_order_count,
                _rejected_orders(folds),
            ),
        )
        if any(actual != wanted for actual, wanted in expected):
            raise ValueError(
                "dynamic validation aggregate metrics are inconsistent"
            )
        object.__setattr__(
            self,
            "result_hash",
            _canonical_hash(
                {
                    "benchmark_compounded_oos_return": _decimal_text(
                        self.benchmark_compounded_oos_return
                    ),
                    "compounded_oos_return": _decimal_text(
                        self.compounded_oos_return
                    ),
                    "excess_oos_return": _decimal_text(
                        self.excess_oos_return
                    ),
                    "fold_hashes": [
                        value.fold_hash for value in folds
                    ],
                    "objective_version": self.objective_version,
                    "panel_hash": self.panel_hash,
                    "profitable_fold_rate": _decimal_text(
                        self.profitable_fold_rate
                    ),
                    "rejected_order_count": self.rejected_order_count,
                    "selection_optimism": _decimal_text(
                        self.selection_optimism
                    ),
                    "spec_hash": self.spec_hash,
                    "version": self.version,
                    "worst_oos_drawdown": _decimal_text(
                        self.worst_oos_drawdown
                    ),
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class DynamicValidationEvidence:
    result_hash: str
    policy_hash: str
    fold_count: int
    oos_sessions: int
    rejected_order_count: int
    evidence_status: str
    gate_failures: tuple[str, ...]
    version: str = DYNAMIC_EVIDENCE_ASSESSMENT_VERSION
    assessment_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.result_hash,
            name="dynamic evidence result hash",
        )
        _require_lowercase_sha256(
            self.policy_hash,
            name="dynamic evidence policy hash",
        )
        failures = tuple(sorted(self.gate_failures))
        if (
            self.fold_count < 1
            or self.oos_sessions < 1
            or self.rejected_order_count < 0
            or self.evidence_status
            not in {"research_candidate", "insufficient", "rejected"}
            or len(set(failures)) != len(failures)
            or self.version != DYNAMIC_EVIDENCE_ASSESSMENT_VERSION
        ):
            raise ValueError("dynamic validation evidence is inconsistent")
        object.__setattr__(self, "gate_failures", failures)
        object.__setattr__(
            self,
            "assessment_hash",
            _canonical_hash(
                {
                    "evidence_status": self.evidence_status,
                    "fold_count": self.fold_count,
                    "gate_failures": list(failures),
                    "oos_sessions": self.oos_sessions,
                    "policy_hash": self.policy_hash,
                    "rejected_order_count": self.rejected_order_count,
                    "result_hash": self.result_hash,
                    "version": self.version,
                }
            ),
        )


class DynamicWalkForwardValidator:
    def run(
        self,
        *,
        panel: DynamicMarketPanel,
        spec: DynamicPortfolioResearchSpec,
    ) -> DynamicValidationResult:
        if (
            panel.spec_hash != spec.spec_hash
            or panel.dataset_manifest_hash
            != spec.dataset_manifest_hash
            or panel.plan_hash != spec.plan_hash
            or panel.sessions[0].session_date < spec.start_date
            or panel.sessions[-1].session_date > spec.end_date
        ):
            raise ValueError(
                "dynamic panel and research specification do not match"
            )
        sessions = panel.sessions
        minimum = (
            spec.train_sessions
            + spec.embargo_sessions
            + spec.test_sessions
        )
        if len(sessions) < minimum:
            raise ValueError(
                "dynamic walk-forward history is insufficient"
            )
        folds: list[DynamicValidationFold] = []
        test_start = spec.train_sessions + spec.embargo_sessions
        while test_start + spec.test_sessions <= len(sessions):
            train_end = test_start - spec.embargo_sessions
            train_start = train_end - spec.train_sessions
            training_sessions = sessions[train_start:train_end]
            evaluations = tuple(
                DynamicCandidateEvaluation(
                    parameters=parameters,
                    score=_selection_score(result),
                    result=result,
                )
                for parameters, result in (
                    (
                        parameters,
                        _run_momentum(
                            panel=panel,
                            spec=spec,
                            parameters=parameters,
                            sessions=training_sessions,
                            start_index=0,
                            trade_session_count=len(
                                training_sessions
                            ),
                        ),
                    )
                    for parameters in spec.candidates
                )
            )
            winner = max(
                evaluations,
                key=lambda value: (
                    value.score,
                    -value.parameters.lookback_sessions,
                    -value.parameters.rebalance_sessions,
                    -value.parameters.selection_count,
                ),
            )
            score = winner.score
            selected = winner.parameters
            training = winner.result
            test = _run_momentum(
                panel=panel,
                spec=spec,
                parameters=selected,
                sessions=sessions,
                start_index=test_start,
                trade_session_count=spec.test_sessions,
            )
            benchmark = _run_benchmark(
                panel=panel,
                spec=spec,
                start_index=test_start,
                trade_session_count=spec.test_sessions,
            )
            test_end = test_start + spec.test_sessions - 1
            folds.append(
                DynamicValidationFold(
                    sequence=len(folds) + 1,
                    train_start=sessions[train_start].session_date,
                    train_end=sessions[train_end - 1].session_date,
                    test_start=sessions[test_start].session_date,
                    test_end=sessions[test_end].session_date,
                    selected=selected,
                    selection_score=score,
                    candidate_evaluations=evaluations,
                    training_result=training,
                    test_result=test,
                    benchmark_result=benchmark,
                )
            )
            test_start += spec.test_sessions
        return _result(panel=panel, spec=spec, folds=tuple(folds))


def assess_dynamic_validation(
    result: DynamicValidationResult,
    *,
    policy: DynamicPortfolioEvidencePolicy,
) -> DynamicValidationEvidence:
    failures: list[str] = []
    fold_count = len(result.folds)
    oos_sessions = sum(
        len(value.test_result.snapshots) for value in result.folds
    )
    if fold_count < policy.minimum_folds:
        failures.append("minimum_fold_count")
    if oos_sessions < policy.minimum_oos_sessions:
        failures.append("minimum_oos_sessions")
    if (
        result.compounded_oos_return
        <= policy.minimum_compounded_oos_return
    ):
        failures.append("nonpositive_oos_return")
    if result.excess_oos_return <= policy.minimum_excess_oos_return:
        failures.append("nonpositive_excess_return")
    if result.profitable_fold_rate < policy.minimum_profitable_fold_rate:
        failures.append("profitable_fold_rate")
    if result.worst_oos_drawdown > policy.maximum_oos_drawdown:
        failures.append("oos_drawdown_limit")
    if result.selection_optimism > policy.maximum_selection_optimism:
        failures.append("selection_optimism")
    if result.rejected_order_count > policy.maximum_rejected_orders:
        failures.append("execution_rejections")
    sample = {"minimum_fold_count", "minimum_oos_sessions"}
    status = (
        "research_candidate"
        if not failures
        else "insufficient"
        if set(failures).issubset(sample)
        else "rejected"
    )
    return DynamicValidationEvidence(
        result_hash=result.result_hash,
        policy_hash=policy.policy_hash,
        fold_count=fold_count,
        oos_sessions=oos_sessions,
        rejected_order_count=result.rejected_order_count,
        evidence_status=status,
        gate_failures=tuple(failures),
    )


def _run_momentum(
    *,
    panel: DynamicMarketPanel,
    spec: DynamicPortfolioResearchSpec,
    parameters: CrossSectionalMomentumParameters,
    sessions: tuple[DynamicMarketSession, ...],
    start_index: int,
    trade_session_count: int,
) -> BacktestResult:
    policy = DynamicMomentumOrderPolicy(
        sessions=sessions,
        start_index=start_index,
        trade_session_count=trade_session_count,
        parameters=parameters,
        spec=spec,
    )
    selected_sessions = sessions[
        start_index : start_index + trade_session_count
    ]
    return _engine(spec).run_dynamic(
        strategy_id=parameters.strategy_id,
        manifest_hash=panel.dataset_manifest_hash,
        as_of=panel.as_of,
        initial_cash=spec.initial_cash,
        market_sessions=tuple(
            value.markets for value in selected_sessions
        ),
        order_factory=policy,
    )


def _run_benchmark(
    *,
    panel: DynamicMarketPanel,
    spec: DynamicPortfolioResearchSpec,
    start_index: int,
    trade_session_count: int,
) -> BacktestResult:
    policy = DynamicEqualWeightBenchmarkPolicy(
        sessions=panel.sessions,
        start_index=start_index,
        trade_session_count=trade_session_count,
        spec=spec,
    )
    selected = panel.sessions[
        start_index : start_index + trade_session_count
    ]
    return _engine(spec).run_dynamic(
        strategy_id=spec.benchmark_version,
        manifest_hash=panel.dataset_manifest_hash,
        as_of=panel.as_of,
        initial_cash=spec.initial_cash,
        market_sessions=tuple(value.markets for value in selected),
        order_factory=policy,
    )


def _engine(spec: DynamicPortfolioResearchSpec) -> BacktestEngine:
    return BacktestEngine(
        execution=ExecutionModel(
            slippage_bps=spec.slippage_bps,
            max_volume_participation=(
                spec.maximum_volume_participation
            ),
        )
    )


def _selection_score(result: BacktestResult) -> Decimal:
    return (
        result.total_return
        - result.max_drawdown
        - result.turnover * Decimal("0.001")
    )


def _result(
    *,
    panel: DynamicMarketPanel,
    spec: DynamicPortfolioResearchSpec,
    folds: tuple[DynamicValidationFold, ...],
) -> DynamicValidationResult:
    if not folds:
        raise ValueError("dynamic validation produced no folds")
    tests = tuple(value.test_result.total_return for value in folds)
    benchmarks = tuple(
        value.benchmark_result.total_return for value in folds
    )
    training = tuple(
        value.training_result.total_return for value in folds
    )
    compounded = _compound(tests)
    benchmark = _compound(benchmarks)
    mean_training = sum(training, Decimal("0")) / Decimal(len(folds))
    mean_test = sum(tests, Decimal("0")) / Decimal(len(folds))
    return DynamicValidationResult(
        panel_hash=panel.panel_hash,
        spec_hash=spec.spec_hash,
        dataset_manifest_hash=panel.dataset_manifest_hash,
        as_of=panel.as_of,
        folds=folds,
        compounded_oos_return=compounded,
        benchmark_compounded_oos_return=benchmark,
        excess_oos_return=compounded - benchmark,
        profitable_fold_rate=Decimal(sum(value > 0 for value in tests))
        / Decimal(len(folds)),
        worst_oos_drawdown=max(
            value.test_result.max_drawdown for value in folds
        ),
        mean_training_return=mean_training,
        selection_optimism=mean_training - mean_test,
        rejected_order_count=_rejected_orders(folds),
    )


def _rejected_orders(
    folds: tuple[DynamicValidationFold, ...],
) -> int:
    return sum(
        report.state is ExecutionState.REJECTED
        for fold in folds
        for result in (
            fold.training_result,
            fold.test_result,
            fold.benchmark_result,
        )
        for report in result.reports
    )


def _compound(values: tuple[Decimal, ...]) -> Decimal:
    result = Decimal("1")
    for value in values:
        result *= Decimal("1") + value
    return result - Decimal("1")
