from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from itertools import pairwise

from autoquant.backtest.dynamic_portfolio import (
    DYNAMIC_PORTFOLIO_BENCHMARK_VERSION,
)
from autoquant.backtest.low_volatility_forward import (
    LowVolatilityForwardEvidenceSpec,
    LowVolatilityForwardSessionBinding,
    annualized_geometric_return,
    annualized_stability_gap,
)
from autoquant.backtest.low_volatility_portfolio import (
    LOW_VOLATILITY_STRATEGY_ID,
    LowVolatilityResearchSpec,
)
from autoquant.backtest.low_volatility_strategy import (
    LowVolatilityExecutablePanel,
)
from autoquant.backtest.low_volatility_validation import (
    LowVolatilityValidationEvidence,
    LowVolatilityValidationResult,
    run_low_volatility_benchmark_interval,
    run_low_volatility_strategy_interval,
)
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

LOW_VOLATILITY_FORWARD_EVALUATION_VERSION = (
    "low-volatility-forward-evaluation-v1"
)
LOW_VOLATILITY_FORWARD_BLOCK_RESULT_VERSION = (
    "low-volatility-forward-21-session-block-v1"
)
LOW_VOLATILITY_FORWARD_ASSESSMENT_VERSION = (
    "low-volatility-forward-assessment-v1"
)
_SOURCE_TRAINING_SESSIONS = 504


@dataclass(frozen=True, slots=True)
class LowVolatilityForwardBlockResult:
    sequence: int
    start_date: date
    end_date: date
    session_count: int
    strategy_starting_equity: Decimal
    strategy_ending_equity: Decimal
    benchmark_starting_equity: Decimal
    benchmark_ending_equity: Decimal
    strategy_return: Decimal
    benchmark_return: Decimal
    version: str = LOW_VOLATILITY_FORWARD_BLOCK_RESULT_VERSION
    block_hash: str = field(init=False)

    def __post_init__(self) -> None:
        expected_strategy = (
            self.strategy_ending_equity / self.strategy_starting_equity
            - Decimal("1")
        )
        expected_benchmark = (
            self.benchmark_ending_equity / self.benchmark_starting_equity
            - Decimal("1")
        )
        values = (
            self.strategy_starting_equity,
            self.strategy_ending_equity,
            self.benchmark_starting_equity,
            self.benchmark_ending_equity,
        )
        if (
            self.sequence < 1
            or self.start_date > self.end_date
            or self.session_count != 21
            or any(
                not isinstance(value, Decimal)
                or not value.is_finite()
                or value <= 0
                for value in values
            )
            or self.strategy_return != expected_strategy
            or self.benchmark_return != expected_benchmark
            or self.version
            != LOW_VOLATILITY_FORWARD_BLOCK_RESULT_VERSION
        ):
            raise ValueError(
                "low-volatility forward block result is inconsistent"
            )
        object.__setattr__(
            self,
            "block_hash",
            _canonical_hash(self.payload()),
        )

    @property
    def excess_return(self) -> Decimal:
        return self.strategy_return - self.benchmark_return

    def payload(self) -> dict[str, object]:
        return {
            "benchmark_ending_equity": _decimal_text(
                self.benchmark_ending_equity
            ),
            "benchmark_return": _decimal_text(self.benchmark_return),
            "benchmark_starting_equity": _decimal_text(
                self.benchmark_starting_equity
            ),
            "end_date": self.end_date.isoformat(),
            "sequence": self.sequence,
            "session_count": self.session_count,
            "start_date": self.start_date.isoformat(),
            "strategy_ending_equity": _decimal_text(
                self.strategy_ending_equity
            ),
            "strategy_return": _decimal_text(self.strategy_return),
            "strategy_starting_equity": _decimal_text(
                self.strategy_starting_equity
            ),
            "version": self.version,
        }


@dataclass(frozen=True, slots=True)
class LowVolatilityForwardEvaluationResult:
    forward_spec_hash: str
    evaluation_dataset_manifest_hash: str
    source_spec_hash: str
    predecessor_result_hash: str
    predecessor_assessment_hash: str
    panel_hash: str
    market_panel_hash: str
    as_of: datetime
    session_bindings: tuple[LowVolatilityForwardSessionBinding, ...]
    strategy_result: BacktestResult
    benchmark_result: BacktestResult
    blocks: tuple[LowVolatilityForwardBlockResult, ...]
    source_mean_training_return: Decimal
    source_training_sessions: int
    forward_compounded_return: Decimal
    benchmark_compounded_return: Decimal
    forward_excess_return: Decimal
    profitable_block_rate: Decimal
    annualized_training_return: Decimal
    annualized_forward_return: Decimal
    annualized_stability_gap: Decimal
    strategy_rejected_order_count: int
    benchmark_rejected_order_count: int
    strategy_unresolved_position_count: int
    benchmark_unresolved_position_count: int
    version: str = LOW_VOLATILITY_FORWARD_EVALUATION_VERSION
    result_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.forward_spec_hash, "forward evaluation spec hash"),
            (
                self.evaluation_dataset_manifest_hash,
                "forward evaluation dataset manifest hash",
            ),
            (self.source_spec_hash, "forward evaluation source spec hash"),
            (
                self.predecessor_result_hash,
                "forward evaluation predecessor result hash",
            ),
            (
                self.predecessor_assessment_hash,
                "forward evaluation predecessor assessment hash",
            ),
            (self.panel_hash, "forward evaluation panel hash"),
            (
                self.market_panel_hash,
                "forward evaluation market panel hash",
            ),
        ):
            _require_lowercase_sha256(value, name=name)
        bindings = tuple(self.session_bindings)
        blocks = tuple(self.blocks)
        object.__setattr__(self, "session_bindings", bindings)
        object.__setattr__(self, "blocks", blocks)
        object.__setattr__(
            self,
            "as_of",
            to_utc(self.as_of, name="forward evaluation as_of"),
        )
        session_dates = tuple(value.session_date for value in bindings)
        strategy_dates = tuple(
            value.session_date for value in self.strategy_result.snapshots
        )
        benchmark_dates = tuple(
            value.session_date for value in self.benchmark_result.snapshots
        )
        if (
            len(bindings) != 126
            or len(set(value.binding_hash for value in bindings)) != 126
            or len(set(session_dates)) != 126
            or any(
                current >= following
                for current, following in pairwise(session_dates)
            )
            or any(
                value.forward_spec_hash != self.forward_spec_hash
                for value in bindings
            )
            or self.strategy_result.strategy_id
            != LOW_VOLATILITY_STRATEGY_ID
            or self.benchmark_result.strategy_id
            != DYNAMIC_PORTFOLIO_BENCHMARK_VERSION
            or {
                self.strategy_result.manifest_hash,
                self.benchmark_result.manifest_hash,
            }
            != {self.panel_hash}
            or {
                self.strategy_result.as_of,
                self.benchmark_result.as_of,
                self.as_of,
            }
            != {self.as_of}
            or strategy_dates != session_dates
            or benchmark_dates != session_dates
            or self.strategy_result.ending_equity
            != self.strategy_result.snapshots[-1].equity
            or self.benchmark_result.ending_equity
            != self.benchmark_result.snapshots[-1].equity
            or self.source_training_sessions != _SOURCE_TRAINING_SESSIONS
            or self.version != LOW_VOLATILITY_FORWARD_EVALUATION_VERSION
        ):
            raise ValueError(
                "low-volatility forward evaluation result is inconsistent"
            )
        expected_blocks = _blocks(
            strategy=self.strategy_result,
            benchmark=self.benchmark_result,
        )
        expected_metrics = (
            (
                self.forward_compounded_return,
                self.strategy_result.total_return,
            ),
            (
                self.benchmark_compounded_return,
                self.benchmark_result.total_return,
            ),
            (
                self.forward_excess_return,
                self.strategy_result.total_return
                - self.benchmark_result.total_return,
            ),
            (
                self.profitable_block_rate,
                Decimal(
                    sum(value.strategy_return > 0 for value in expected_blocks)
                )
                / Decimal(len(expected_blocks)),
            ),
            (
                self.annualized_training_return,
                annualized_geometric_return(
                    self.source_mean_training_return,
                    sessions=self.source_training_sessions,
                ),
            ),
            (
                self.annualized_forward_return,
                annualized_geometric_return(
                    self.strategy_result.total_return,
                    sessions=len(bindings),
                ),
            ),
            (
                self.annualized_stability_gap,
                annualized_stability_gap(
                    training_return=self.source_mean_training_return,
                    training_sessions=self.source_training_sessions,
                    evaluation_return=self.strategy_result.total_return,
                    evaluation_sessions=len(bindings),
                ),
            ),
        )
        diagnostics = _diagnostics(
            strategy=self.strategy_result,
            benchmark=self.benchmark_result,
        )
        if (
            blocks != expected_blocks
            or any(actual != expected for actual, expected in expected_metrics)
            or (
                self.strategy_rejected_order_count,
                self.benchmark_rejected_order_count,
                self.strategy_unresolved_position_count,
                self.benchmark_unresolved_position_count,
            )
            != diagnostics
        ):
            raise ValueError(
                "low-volatility forward evaluation metrics are not derived"
            )
        object.__setattr__(
            self,
            "result_hash",
            _canonical_hash(
                {
                    "annualized_forward_return": _decimal_text(
                        self.annualized_forward_return
                    ),
                    "annualized_stability_gap": _decimal_text(
                        self.annualized_stability_gap
                    ),
                    "annualized_training_return": _decimal_text(
                        self.annualized_training_return
                    ),
                    "as_of": self.as_of.isoformat(),
                    "benchmark_artifact_hash": backtest_artifact_hash(
                        self.benchmark_result
                    ),
                    "benchmark_compounded_return": _decimal_text(
                        self.benchmark_compounded_return
                    ),
                    "benchmark_rejected_order_count": (
                        self.benchmark_rejected_order_count
                    ),
                    "benchmark_unresolved_position_count": (
                        self.benchmark_unresolved_position_count
                    ),
                    "block_hashes": [value.block_hash for value in blocks],
                    "forward_compounded_return": _decimal_text(
                        self.forward_compounded_return
                    ),
                    "evaluation_dataset_manifest_hash": (
                        self.evaluation_dataset_manifest_hash
                    ),
                    "forward_excess_return": _decimal_text(
                        self.forward_excess_return
                    ),
                    "forward_spec_hash": self.forward_spec_hash,
                    "market_panel_hash": self.market_panel_hash,
                    "panel_hash": self.panel_hash,
                    "predecessor_assessment_hash": (
                        self.predecessor_assessment_hash
                    ),
                    "predecessor_result_hash": (
                        self.predecessor_result_hash
                    ),
                    "profitable_block_rate": _decimal_text(
                        self.profitable_block_rate
                    ),
                    "session_binding_hashes": [
                        value.binding_hash for value in bindings
                    ],
                    "source_mean_training_return": _decimal_text(
                        self.source_mean_training_return
                    ),
                    "source_spec_hash": self.source_spec_hash,
                    "source_training_sessions": (
                        self.source_training_sessions
                    ),
                    "strategy_artifact_hash": backtest_artifact_hash(
                        self.strategy_result
                    ),
                    "strategy_rejected_order_count": (
                        self.strategy_rejected_order_count
                    ),
                    "strategy_unresolved_position_count": (
                        self.strategy_unresolved_position_count
                    ),
                    "version": self.version,
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class LowVolatilityForwardAssessment:
    result_hash: str
    forward_spec_hash: str
    session_count: int
    block_count: int
    evidence_status: str
    gate_failures: tuple[str, ...]
    paper_trading_eligible: bool
    live_trading_locked: bool = True
    version: str = LOW_VOLATILITY_FORWARD_ASSESSMENT_VERSION
    assessment_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.result_hash,
            name="forward assessment result hash",
        )
        _require_lowercase_sha256(
            self.forward_spec_hash,
            name="forward assessment spec hash",
        )
        failures = tuple(self.gate_failures)
        if (
            self.session_count != 126
            or self.block_count != 6
            or self.evidence_status
            not in {"paper_candidate", "rejected"}
            or len(set(failures)) != len(failures)
            or self.paper_trading_eligible
            != (self.evidence_status == "paper_candidate")
            or not self.live_trading_locked
            or self.version != LOW_VOLATILITY_FORWARD_ASSESSMENT_VERSION
        ):
            raise ValueError(
                "low-volatility forward assessment is inconsistent"
            )
        object.__setattr__(self, "gate_failures", failures)
        object.__setattr__(
            self,
            "assessment_hash",
            _canonical_hash(
                {
                    "block_count": self.block_count,
                    "evidence_status": self.evidence_status,
                    "forward_spec_hash": self.forward_spec_hash,
                    "gate_failures": list(failures),
                    "live_trading_locked": self.live_trading_locked,
                    "paper_trading_eligible": (
                        self.paper_trading_eligible
                    ),
                    "result_hash": self.result_hash,
                    "session_count": self.session_count,
                    "version": self.version,
                }
            ),
        )


class LowVolatilityForwardEvaluator:
    """Evaluate exactly the frozen first 126 forward sessions once."""

    def run(
        self,
        *,
        panel: LowVolatilityExecutablePanel,
        source_spec: LowVolatilityResearchSpec,
        forward_spec: LowVolatilityForwardEvidenceSpec,
        evaluation_dataset_manifest_hash: str,
        bindings: tuple[LowVolatilityForwardSessionBinding, ...],
        predecessor_result: LowVolatilityValidationResult,
        predecessor_evidence: LowVolatilityValidationEvidence,
    ) -> LowVolatilityForwardEvaluationResult:
        ordered = tuple(bindings)
        if (
            forward_spec.source_spec_hash != source_spec.spec_hash
            or forward_spec.source_dataset_manifest_hash
            != source_spec.dataset_manifest_hash
            or panel.spec_hash != source_spec.spec_hash
            or predecessor_result.result_hash
            != forward_spec.predecessor_result_hash
            or predecessor_evidence.result_hash
            != predecessor_result.result_hash
            or predecessor_evidence.assessment_hash
            != forward_spec.predecessor_assessment_hash
            or predecessor_result.spec_hash != source_spec.spec_hash
            or predecessor_evidence.evidence_status != "rejected"
            or predecessor_evidence.gate_failures != ("train_test_gap",)
            or len(ordered) != forward_spec.minimum_forward_sessions
            or ordered[0].session_date
            != forward_spec.forward_start_date
            or any(
                value.forward_spec_hash != forward_spec.spec_hash
                or value.policy_hash != source_spec.policy_hash
                for value in ordered
            )
        ):
            raise ValueError(
                "low-volatility forward evaluation provenance is invalid"
            )
        forward_dates = tuple(value.session_date for value in ordered)
        panel_dates = tuple(value.session_date for value in panel.sessions)
        try:
            start_index = panel_dates.index(
                forward_spec.forward_start_date
            )
        except ValueError:
            raise ValueError(
                "low-volatility forward window is absent from the panel"
            ) from None
        if (
            forward_dates
            != panel_dates[
                start_index : start_index
                + forward_spec.minimum_forward_sessions
            ]
            or start_index < source_spec.minimum_history_sessions
        ):
            raise ValueError(
                "low-volatility forward bindings are not the first "
                "contiguous frozen window"
            )
        strategy = run_low_volatility_strategy_interval(
            panel=panel,
            spec=source_spec,
            start_index=start_index,
            trade_session_count=forward_spec.minimum_forward_sessions,
        )
        benchmark = run_low_volatility_benchmark_interval(
            panel=panel,
            spec=source_spec,
            start_index=start_index,
            trade_session_count=forward_spec.minimum_forward_sessions,
        )
        return _result(
            panel=panel,
            forward_spec=forward_spec,
            evaluation_dataset_manifest_hash=(
                evaluation_dataset_manifest_hash
            ),
            bindings=ordered,
            predecessor_result=predecessor_result,
            strategy=strategy,
            benchmark=benchmark,
        )


def assess_low_volatility_forward(
    result: LowVolatilityForwardEvaluationResult,
    *,
    spec: LowVolatilityForwardEvidenceSpec,
) -> LowVolatilityForwardAssessment:
    if result.forward_spec_hash != spec.spec_hash:
        raise ValueError(
            "low-volatility forward result and spec do not match"
        )
    failures: list[str] = []
    if (
        result.forward_compounded_return
        <= spec.minimum_forward_compounded_return
    ):
        failures.append("nonpositive_forward_return")
    if result.forward_excess_return <= spec.minimum_forward_excess_return:
        failures.append("nonpositive_forward_excess_return")
    if (
        result.profitable_block_rate
        < spec.minimum_profitable_block_rate
    ):
        failures.append("profitable_block_rate")
    if (
        result.strategy_result.max_drawdown
        > spec.maximum_forward_drawdown
    ):
        failures.append("forward_drawdown_limit")
    if (
        result.annualized_stability_gap
        > spec.maximum_annualized_stability_gap
    ):
        failures.append("annualized_stability_gap")
    if (
        result.strategy_rejected_order_count
        > spec.maximum_rejected_orders
    ):
        failures.append("execution_rejections")
    if result.strategy_unresolved_position_count:
        failures.append("unresolved_positions")
    status = "paper_candidate" if not failures else "rejected"
    return LowVolatilityForwardAssessment(
        result_hash=result.result_hash,
        forward_spec_hash=spec.spec_hash,
        session_count=len(result.session_bindings),
        block_count=len(result.blocks),
        evidence_status=status,
        gate_failures=tuple(failures),
        paper_trading_eligible=status == "paper_candidate",
    )


def build_low_volatility_forward_result(
    *,
    panel_hash: str,
    market_panel_hash: str,
    as_of: datetime,
    forward_spec: LowVolatilityForwardEvidenceSpec,
    evaluation_dataset_manifest_hash: str,
    bindings: tuple[LowVolatilityForwardSessionBinding, ...],
    predecessor_result: LowVolatilityValidationResult,
    strategy_result: BacktestResult,
    benchmark_result: BacktestResult,
) -> LowVolatilityForwardEvaluationResult:
    """Build a fully derived result from independently reproduced runs."""

    if (
        predecessor_result.result_hash
        != forward_spec.predecessor_result_hash
        or predecessor_result.spec_hash != forward_spec.source_spec_hash
    ):
        raise ValueError(
            "low-volatility forward predecessor does not match"
        )
    return _result(
        panel_hash=panel_hash,
        market_panel_hash=market_panel_hash,
        as_of=as_of,
        forward_spec=forward_spec,
        evaluation_dataset_manifest_hash=(
            evaluation_dataset_manifest_hash
        ),
        bindings=bindings,
        predecessor_result=predecessor_result,
        strategy=strategy_result,
        benchmark=benchmark_result,
    )


def _result(
    *,
    forward_spec: LowVolatilityForwardEvidenceSpec,
    evaluation_dataset_manifest_hash: str,
    bindings: tuple[LowVolatilityForwardSessionBinding, ...],
    predecessor_result: LowVolatilityValidationResult,
    strategy: BacktestResult,
    benchmark: BacktestResult,
    panel: LowVolatilityExecutablePanel | None = None,
    panel_hash: str | None = None,
    market_panel_hash: str | None = None,
    as_of: datetime | None = None,
) -> LowVolatilityForwardEvaluationResult:
    resolved_panel_hash = panel.panel_hash if panel is not None else panel_hash
    resolved_market_panel_hash = (
        panel.market_panel_hash if panel is not None else market_panel_hash
    )
    resolved_as_of = panel.as_of if panel is not None else as_of
    if (
        resolved_panel_hash is None
        or resolved_market_panel_hash is None
        or resolved_as_of is None
    ):
        raise ValueError("low-volatility forward panel identity is missing")
    blocks = _blocks(strategy=strategy, benchmark=benchmark)
    annualized_training = annualized_geometric_return(
        predecessor_result.mean_training_return,
        sessions=_SOURCE_TRAINING_SESSIONS,
        annualization_sessions=forward_spec.annualization_sessions,
    )
    annualized_forward = annualized_geometric_return(
        strategy.total_return,
        sessions=len(bindings),
        annualization_sessions=forward_spec.annualization_sessions,
    )
    diagnostics = _diagnostics(
        strategy=strategy,
        benchmark=benchmark,
    )
    return LowVolatilityForwardEvaluationResult(
        forward_spec_hash=forward_spec.spec_hash,
        evaluation_dataset_manifest_hash=(
            evaluation_dataset_manifest_hash
        ),
        source_spec_hash=forward_spec.source_spec_hash,
        predecessor_result_hash=forward_spec.predecessor_result_hash,
        predecessor_assessment_hash=(
            forward_spec.predecessor_assessment_hash
        ),
        panel_hash=resolved_panel_hash,
        market_panel_hash=resolved_market_panel_hash,
        as_of=resolved_as_of,
        session_bindings=bindings,
        strategy_result=strategy,
        benchmark_result=benchmark,
        blocks=blocks,
        source_mean_training_return=(
            predecessor_result.mean_training_return
        ),
        source_training_sessions=_SOURCE_TRAINING_SESSIONS,
        forward_compounded_return=strategy.total_return,
        benchmark_compounded_return=benchmark.total_return,
        forward_excess_return=(
            strategy.total_return - benchmark.total_return
        ),
        profitable_block_rate=Decimal(
            sum(value.strategy_return > 0 for value in blocks)
        )
        / Decimal(len(blocks)),
        annualized_training_return=annualized_training,
        annualized_forward_return=annualized_forward,
        annualized_stability_gap=(
            annualized_training - annualized_forward
        ),
        strategy_rejected_order_count=diagnostics[0],
        benchmark_rejected_order_count=diagnostics[1],
        strategy_unresolved_position_count=diagnostics[2],
        benchmark_unresolved_position_count=diagnostics[3],
    )


def _blocks(
    *,
    strategy: BacktestResult,
    benchmark: BacktestResult,
) -> tuple[LowVolatilityForwardBlockResult, ...]:
    if (
        len(strategy.snapshots) != 126
        or len(benchmark.snapshots) != 126
    ):
        raise ValueError(
            "low-volatility forward evaluation requires exactly 126 sessions"
        )
    blocks: list[LowVolatilityForwardBlockResult] = []
    for start in range(0, 126, 21):
        end = start + 20
        strategy_start = (
            strategy.initial_cash
            if start == 0
            else strategy.snapshots[start - 1].equity
        )
        benchmark_start = (
            benchmark.initial_cash
            if start == 0
            else benchmark.snapshots[start - 1].equity
        )
        strategy_end = strategy.snapshots[end].equity
        benchmark_end = benchmark.snapshots[end].equity
        blocks.append(
            LowVolatilityForwardBlockResult(
                sequence=len(blocks) + 1,
                start_date=strategy.snapshots[start].session_date,
                end_date=strategy.snapshots[end].session_date,
                session_count=21,
                strategy_starting_equity=strategy_start,
                strategy_ending_equity=strategy_end,
                benchmark_starting_equity=benchmark_start,
                benchmark_ending_equity=benchmark_end,
                strategy_return=(
                    strategy_end / strategy_start - Decimal("1")
                ),
                benchmark_return=(
                    benchmark_end / benchmark_start - Decimal("1")
                ),
            )
        )
    return tuple(blocks)


def _diagnostics(
    *,
    strategy: BacktestResult,
    benchmark: BacktestResult,
) -> tuple[int, int, int, int]:
    return (
        sum(
            value.state is ExecutionState.REJECTED
            for value in strategy.reports
        ),
        sum(
            value.state is ExecutionState.REJECTED
            for value in benchmark.reports
        ),
        len(strategy.snapshots[-1].positions),
        len(benchmark.snapshots[-1].positions),
    )
