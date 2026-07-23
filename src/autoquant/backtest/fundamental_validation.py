from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from itertools import pairwise
from typing import cast

from autoquant.backtest.dynamic_panel import DynamicMarketSession
from autoquant.backtest.dynamic_portfolio import (
    DYNAMIC_PORTFOLIO_BENCHMARK_VERSION,
    DynamicPortfolioResearchSpec,
)
from autoquant.backtest.dynamic_strategy import (
    DynamicEqualWeightBenchmarkPolicy,
)
from autoquant.backtest.engine import BacktestEngine
from autoquant.backtest.fundamental_portfolio import (
    FundamentalPortfolioResearchSpec,
)
from autoquant.backtest.fundamental_strategy import (
    FundamentalExecutablePanel,
    FundamentalQualityValueOrderPolicy,
)
from autoquant.backtest.ledger import ExecutionModel
from autoquant.backtest.models import (
    BacktestResult,
    ExecutionState,
    backtest_artifact_hash,
)
from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
)

FUNDAMENTAL_VALIDATION_VERSION = "fundamental-fixed-walk-forward-v1"
FUNDAMENTAL_EVIDENCE_VERSION = "fundamental-fixed-evidence-v1"


@dataclass(frozen=True, slots=True)
class FundamentalValidationFold:
    sequence: int
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    training_result: BacktestResult
    test_result: BacktestResult
    benchmark_result: BacktestResult
    fold_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            self.sequence < 1
            or self.train_start > self.train_end
            or self.train_end >= self.test_start
            or self.test_start > self.test_end
            or self.training_result.strategy_id != self.test_result.strategy_id
            or self.benchmark_result.strategy_id != DYNAMIC_PORTFOLIO_BENCHMARK_VERSION
        ):
            raise ValueError("fundamental validation fold is inconsistent")
        results = (
            self.training_result,
            self.test_result,
            self.benchmark_result,
        )
        if (
            len({value.manifest_hash for value in results}) != 1
            or len({value.as_of for value in results}) != 1
            or any(not value.snapshots for value in results)
            or self.training_result.snapshots[0].session_date != self.train_start
            or self.training_result.snapshots[-1].session_date != self.train_end
            or self.test_result.snapshots[0].session_date != self.test_start
            or self.test_result.snapshots[-1].session_date != self.test_end
            or self.benchmark_result.snapshots[0].session_date != self.test_start
            or self.benchmark_result.snapshots[-1].session_date != self.test_end
        ):
            raise ValueError("fundamental fold results do not cover intervals")
        object.__setattr__(
            self,
            "fold_hash",
            _canonical_hash(
                {
                    "benchmark_artifact_hash": (backtest_artifact_hash(self.benchmark_result)),
                    "sequence": self.sequence,
                    "test_artifact_hash": backtest_artifact_hash(self.test_result),
                    "test_end": self.test_end.isoformat(),
                    "test_start": self.test_start.isoformat(),
                    "train_end": self.train_end.isoformat(),
                    "train_start": self.train_start.isoformat(),
                    "training_artifact_hash": (backtest_artifact_hash(self.training_result)),
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class FundamentalValidationResult:
    panel_hash: str
    feature_panel_hash: str
    market_panel_hash: str
    spec_hash: str
    as_of: datetime
    folds: tuple[FundamentalValidationFold, ...]
    compounded_oos_return: Decimal
    benchmark_compounded_oos_return: Decimal
    excess_oos_return: Decimal
    profitable_fold_rate: Decimal
    worst_oos_drawdown: Decimal
    mean_training_return: Decimal
    train_test_gap: Decimal
    rejected_order_count: int
    unresolved_position_count: int
    version: str = FUNDAMENTAL_VALIDATION_VERSION
    result_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.panel_hash, "fundamental validation panel hash"),
            (
                self.feature_panel_hash,
                "fundamental validation feature panel hash",
            ),
            (
                self.market_panel_hash,
                "fundamental validation market panel hash",
            ),
            (self.spec_hash, "fundamental validation spec hash"),
        ):
            _require_lowercase_sha256(value, name=name)
        folds = tuple(self.folds)
        if (
            not folds
            or tuple(value.sequence for value in folds) != tuple(range(1, len(folds) + 1))
            or any(
                current.test_end >= following.test_start for current, following in pairwise(folds)
            )
            or any(
                result.manifest_hash != self.panel_hash
                for fold in folds
                for result in (
                    fold.training_result,
                    fold.test_result,
                    fold.benchmark_result,
                )
            )
            or self.version != FUNDAMENTAL_VALIDATION_VERSION
        ):
            raise ValueError("fundamental validation result is inconsistent")
        object.__setattr__(self, "folds", folds)
        object.__setattr__(
            self,
            "as_of",
            to_utc(self.as_of, name="fundamental validation as_of"),
        )
        tests = tuple(value.test_result.total_return for value in folds)
        benchmarks = tuple(value.benchmark_result.total_return for value in folds)
        training = tuple(value.training_result.total_return for value in folds)
        expected_oos = _compound(tests)
        expected_benchmark = _compound(benchmarks)
        expected_training = sum(
            training,
            Decimal("0"),
        ) / Decimal(len(folds))
        expected_test_mean = sum(
            tests,
            Decimal("0"),
        ) / Decimal(len(folds))
        expected = (
            (
                self.compounded_oos_return,
                expected_oos,
            ),
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
                Decimal(sum(value > 0 for value in tests)) / Decimal(len(folds)),
            ),
            (
                self.worst_oos_drawdown,
                max(value.test_result.max_drawdown for value in folds),
            ),
            (
                self.mean_training_return,
                expected_training,
            ),
            (
                self.train_test_gap,
                expected_training - expected_test_mean,
            ),
        )
        if (
            any(actual != derived for actual, derived in expected)
            or self.rejected_order_count != _rejected_orders(folds)
            or self.unresolved_position_count != _unresolved_positions(folds)
        ):
            raise ValueError("fundamental validation metrics are not derived")
        object.__setattr__(
            self,
            "result_hash",
            _canonical_hash(
                {
                    "as_of": self.as_of.isoformat(),
                    "benchmark_compounded_oos_return": (
                        _decimal_text(self.benchmark_compounded_oos_return)
                    ),
                    "compounded_oos_return": _decimal_text(self.compounded_oos_return),
                    "excess_oos_return": _decimal_text(self.excess_oos_return),
                    "feature_panel_hash": self.feature_panel_hash,
                    "fold_hashes": [value.fold_hash for value in folds],
                    "market_panel_hash": self.market_panel_hash,
                    "mean_training_return": _decimal_text(self.mean_training_return),
                    "panel_hash": self.panel_hash,
                    "profitable_fold_rate": _decimal_text(self.profitable_fold_rate),
                    "rejected_order_count": (self.rejected_order_count),
                    "spec_hash": self.spec_hash,
                    "train_test_gap": _decimal_text(self.train_test_gap),
                    "unresolved_position_count": (self.unresolved_position_count),
                    "version": self.version,
                    "worst_oos_drawdown": _decimal_text(self.worst_oos_drawdown),
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class FundamentalValidationEvidence:
    result_hash: str
    policy_hash: str
    fold_count: int
    oos_sessions: int
    rejected_order_count: int
    unresolved_position_count: int
    evidence_status: str
    gate_failures: tuple[str, ...]
    version: str = FUNDAMENTAL_EVIDENCE_VERSION
    assessment_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.result_hash,
            name="fundamental evidence result hash",
        )
        _require_lowercase_sha256(
            self.policy_hash,
            name="fundamental evidence policy hash",
        )
        failures = tuple(self.gate_failures)
        if (
            self.fold_count < 1
            or self.oos_sessions < 1
            or self.rejected_order_count < 0
            or self.unresolved_position_count < 0
            or self.evidence_status not in {"research_candidate", "rejected", "insufficient"}
            or len(set(failures)) != len(failures)
            or self.version != FUNDAMENTAL_EVIDENCE_VERSION
        ):
            raise ValueError("fundamental validation evidence is invalid")
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
                    "rejected_order_count": (self.rejected_order_count),
                    "result_hash": self.result_hash,
                    "unresolved_position_count": (self.unresolved_position_count),
                    "version": self.version,
                }
            ),
        )


class FundamentalWalkForwardValidator:
    """Evaluate one pre-registered factor model without parameter search."""

    def run(
        self,
        *,
        panel: FundamentalExecutablePanel,
        spec: FundamentalPortfolioResearchSpec,
    ) -> FundamentalValidationResult:
        if panel.spec_hash != spec.spec_hash:
            raise ValueError("fundamental panel and specification do not match")
        sessions = panel.sessions
        minimum = spec.train_sessions + spec.embargo_sessions + spec.test_sessions
        if len(sessions) < minimum:
            raise ValueError("fundamental walk-forward history is insufficient")
        folds: list[FundamentalValidationFold] = []
        test_start = spec.train_sessions + spec.embargo_sessions
        while test_start + spec.test_sessions <= len(sessions):
            train_end = test_start - spec.embargo_sessions
            train_start = train_end - spec.train_sessions
            training = _run_strategy(
                panel=panel,
                spec=spec,
                start_index=train_start,
                trade_session_count=spec.train_sessions,
            )
            test = _run_strategy(
                panel=panel,
                spec=spec,
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
                FundamentalValidationFold(
                    sequence=len(folds) + 1,
                    train_start=(sessions[train_start].session_date),
                    train_end=(sessions[train_end - 1].session_date),
                    test_start=(sessions[test_start].session_date),
                    test_end=sessions[test_end].session_date,
                    training_result=training,
                    test_result=test,
                    benchmark_result=benchmark,
                )
            )
            test_start += spec.test_sessions
        return _result(
            panel=panel,
            spec=spec,
            folds=tuple(folds),
        )


def assess_fundamental_validation(
    result: FundamentalValidationResult,
    *,
    spec: FundamentalPortfolioResearchSpec,
) -> FundamentalValidationEvidence:
    if result.spec_hash != spec.spec_hash:
        raise ValueError("fundamental result and evidence policy do not match")
    policy = spec.evidence_policy
    failures: list[str] = []
    fold_count = len(result.folds)
    oos_sessions = sum(len(value.test_result.snapshots) for value in result.folds)
    if fold_count < policy.minimum_folds:
        failures.append("minimum_fold_count")
    if oos_sessions < policy.minimum_oos_sessions:
        failures.append("minimum_oos_sessions")
    if result.compounded_oos_return <= policy.minimum_compounded_oos_return:
        failures.append("nonpositive_oos_return")
    if result.excess_oos_return <= policy.minimum_excess_oos_return:
        failures.append("nonpositive_excess_return")
    if result.profitable_fold_rate < policy.minimum_profitable_fold_rate:
        failures.append("profitable_fold_rate")
    if result.worst_oos_drawdown > policy.maximum_oos_drawdown:
        failures.append("oos_drawdown_limit")
    if result.train_test_gap > policy.maximum_selection_optimism:
        failures.append("train_test_gap")
    if result.rejected_order_count > policy.maximum_rejected_orders:
        failures.append("execution_rejections")
    if result.unresolved_position_count:
        failures.append("unresolved_positions")
    sample = {"minimum_fold_count", "minimum_oos_sessions"}
    status = (
        "research_candidate"
        if not failures
        else "insufficient"
        if set(failures).issubset(sample)
        else "rejected"
    )
    return FundamentalValidationEvidence(
        result_hash=result.result_hash,
        policy_hash=policy.policy_hash,
        fold_count=fold_count,
        oos_sessions=oos_sessions,
        rejected_order_count=result.rejected_order_count,
        unresolved_position_count=(result.unresolved_position_count),
        evidence_status=status,
        gate_failures=tuple(failures),
    )


def _run_strategy(
    *,
    panel: FundamentalExecutablePanel,
    spec: FundamentalPortfolioResearchSpec,
    start_index: int,
    trade_session_count: int,
) -> BacktestResult:
    policy = FundamentalQualityValueOrderPolicy(
        sessions=panel.sessions,
        start_index=start_index,
        trade_session_count=trade_session_count,
        spec=spec,
    )
    selected = panel.sessions[start_index : start_index + trade_session_count]
    return _engine(spec).run_dynamic(
        strategy_id=spec.strategy_id,
        manifest_hash=panel.panel_hash,
        as_of=panel.as_of,
        initial_cash=spec.initial_cash,
        market_sessions=tuple(value.markets for value in selected),
        order_factory=policy,
    )


def _run_benchmark(
    *,
    panel: FundamentalExecutablePanel,
    spec: FundamentalPortfolioResearchSpec,
    start_index: int,
    trade_session_count: int,
) -> BacktestResult:
    dynamic_sessions = cast(
        tuple[DynamicMarketSession, ...],
        panel.sessions,
    )
    dynamic_spec = cast(DynamicPortfolioResearchSpec, spec)
    policy = DynamicEqualWeightBenchmarkPolicy(
        sessions=dynamic_sessions,
        start_index=start_index,
        trade_session_count=trade_session_count,
        spec=dynamic_spec,
    )
    selected = panel.sessions[start_index : start_index + trade_session_count]
    return _engine(spec).run_dynamic(
        strategy_id=DYNAMIC_PORTFOLIO_BENCHMARK_VERSION,
        manifest_hash=panel.panel_hash,
        as_of=panel.as_of,
        initial_cash=spec.initial_cash,
        market_sessions=tuple(value.markets for value in selected),
        order_factory=policy,
    )


def _engine(
    spec: FundamentalPortfolioResearchSpec,
) -> BacktestEngine:
    return BacktestEngine(
        execution=ExecutionModel(
            slippage_bps=spec.slippage_bps,
            max_volume_participation=(spec.maximum_volume_participation),
        )
    )


def _result(
    *,
    panel: FundamentalExecutablePanel,
    spec: FundamentalPortfolioResearchSpec,
    folds: tuple[FundamentalValidationFold, ...],
) -> FundamentalValidationResult:
    if not folds:
        raise ValueError("fundamental validation produced no folds")
    tests = tuple(value.test_result.total_return for value in folds)
    benchmarks = tuple(value.benchmark_result.total_return for value in folds)
    training = tuple(value.training_result.total_return for value in folds)
    compounded = _compound(tests)
    benchmark = _compound(benchmarks)
    mean_training = sum(
        training,
        Decimal("0"),
    ) / Decimal(len(folds))
    mean_test = sum(tests, Decimal("0")) / Decimal(len(folds))
    return FundamentalValidationResult(
        panel_hash=panel.panel_hash,
        feature_panel_hash=panel.feature_panel_hash,
        market_panel_hash=panel.market_panel_hash,
        spec_hash=spec.spec_hash,
        as_of=panel.as_of,
        folds=folds,
        compounded_oos_return=compounded,
        benchmark_compounded_oos_return=benchmark,
        excess_oos_return=compounded - benchmark,
        profitable_fold_rate=Decimal(sum(value > 0 for value in tests)) / Decimal(len(folds)),
        worst_oos_drawdown=max(value.test_result.max_drawdown for value in folds),
        mean_training_return=mean_training,
        train_test_gap=mean_training - mean_test,
        rejected_order_count=_rejected_orders(folds),
        unresolved_position_count=_unresolved_positions(folds),
    )


def _rejected_orders(
    folds: tuple[FundamentalValidationFold, ...],
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


def _unresolved_positions(
    folds: tuple[FundamentalValidationFold, ...],
) -> int:
    return sum(
        len(result.snapshots[-1].positions)
        for fold in folds
        for result in (
            fold.training_result,
            fold.test_result,
            fold.benchmark_result,
        )
    )


def _compound(values: tuple[Decimal, ...]) -> Decimal:
    result = Decimal("1")
    for value in values:
        result *= Decimal("1") + value
    return result - Decimal("1")
