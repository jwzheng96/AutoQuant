from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from itertools import pairwise

from autoquant.backtest.engine import BacktestEngine
from autoquant.backtest.ledger import ExecutionModel
from autoquant.backtest.models import (
    AccountSnapshot,
    BacktestResult,
    ExecutionState,
    MarketState,
    OrderIntent,
    OrderSide,
)
from autoquant.backtest.runner import (
    DailyDatasetReaderPort,
    ManifestControlPort,
    ManifestMarketCompiler,
    _baseline_quantity,
    _order,
)
from autoquant.backtest.validation import (
    MarketCompilerPort,
    compile_common_calendar_markets,
)
from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
)

PORTFOLIO_VALIDATION_VERSION = (
    "cross-sectional-momentum-walk-forward-v1"
)
PORTFOLIO_OBJECTIVE_VERSION = (
    "return-minus-drawdown-turnover-v1"
)
PORTFOLIO_EVIDENCE_POLICY_VERSION = (
    "portfolio-validation-evidence-gates-v1"
)


@dataclass(frozen=True, slots=True, order=True)
class CrossSectionalMomentumParameters:
    lookback_sessions: int
    rebalance_sessions: int
    selection_count: int

    def __post_init__(self) -> None:
        if not 20 <= self.lookback_sessions <= 252:
            raise ValueError(
                "lookback_sessions must be between 20 and 252"
            )
        if not 5 <= self.rebalance_sessions <= 63:
            raise ValueError(
                "rebalance_sessions must be between 5 and 63"
            )
        if not 1 <= self.selection_count <= 10:
            raise ValueError(
                "selection_count must be between 1 and 10"
            )

    @property
    def strategy_id(self) -> str:
        return (
            "cross_sectional_momentum_v1:"
            f"lookback={self.lookback_sessions}:"
            f"rebalance={self.rebalance_sessions}:"
            f"select={self.selection_count}"
        )

    def payload(self) -> dict[str, int]:
        return {
            "lookback_sessions": self.lookback_sessions,
            "rebalance_sessions": self.rebalance_sessions,
            "selection_count": self.selection_count,
        }


@dataclass(frozen=True, slots=True)
class PortfolioWalkForwardConfig:
    initial_cash: Decimal = Decimal("1000000")
    gross_allocation: Decimal = Decimal("0.29")
    maximum_order_notional: Decimal = Decimal("100000")
    slippage_bps: Decimal = Decimal("5")
    train_sessions: int = 252
    test_sessions: int = 21
    embargo_sessions: int = 1
    candidates: tuple[CrossSectionalMomentumParameters, ...] = (
        CrossSectionalMomentumParameters(20, 5, 3),
        CrossSectionalMomentumParameters(60, 10, 3),
        CrossSectionalMomentumParameters(120, 20, 3),
    )

    def __post_init__(self) -> None:
        for name, value in (
            ("initial_cash", self.initial_cash),
            ("gross_allocation", self.gross_allocation),
            ("maximum_order_notional", self.maximum_order_notional),
            ("slippage_bps", self.slippage_bps),
        ):
            if not isinstance(value, Decimal) or not value.is_finite():
                raise ValueError(f"{name} must be a finite Decimal")
        if not Decimal("10000") <= self.initial_cash <= Decimal(
            "1000000000"
        ):
            raise ValueError("initial_cash is outside supported bounds")
        if not Decimal("0") < self.gross_allocation <= Decimal("0.80"):
            raise ValueError(
                "gross_allocation must be in (0, 0.80]"
            )
        if self.maximum_order_notional <= 0:
            raise ValueError(
                "maximum_order_notional must be positive"
            )
        if not Decimal("0") <= self.slippage_bps <= Decimal("100"):
            raise ValueError("slippage_bps must be between 0 and 100")
        if not 126 <= self.train_sessions <= 750:
            raise ValueError(
                "train_sessions must be between 126 and 750"
            )
        if not 20 <= self.test_sessions <= 126:
            raise ValueError(
                "test_sessions must be between 20 and 126"
            )
        if not 1 <= self.embargo_sessions <= 20:
            raise ValueError(
                "embargo_sessions must be between 1 and 20"
            )
        candidates = tuple(sorted(self.candidates))
        if (
            not candidates
            or len(candidates) > 12
            or len(set(candidates)) != len(candidates)
        ):
            raise ValueError(
                "portfolio candidates must contain 1-12 unique values"
            )
        if max(
            value.lookback_sessions for value in candidates
        ) >= self.train_sessions:
            raise ValueError(
                "candidate lookback must be smaller than train_sessions"
            )
        if any(
            (
                self.gross_allocation / value.selection_count
                > Decimal("0.20")
                or self.initial_cash
                * self.gross_allocation
                / value.selection_count
                * (
                    Decimal("1")
                    + self.slippage_bps / Decimal("10000")
                )
                > self.maximum_order_notional
            )
            for value in candidates
        ):
            raise ValueError(
                "candidate per-position allocation exceeds risk limits"
            )
        object.__setattr__(self, "candidates", candidates)


@dataclass(frozen=True, slots=True)
class PortfolioWalkForwardFold:
    sequence: int
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    selected: CrossSectionalMomentumParameters
    selection_score: Decimal
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
        ):
            raise ValueError("portfolio fold intervals are invalid")
        if (
            self.training_result.strategy_id
            != self.selected.strategy_id
            or self.test_result.strategy_id
            != self.selected.strategy_id
            or self.benchmark_result.strategy_id
            != "equal_weight_buy_hold_v1"
        ):
            raise ValueError(
                "portfolio fold strategy identity is invalid"
            )
        results = (
            self.training_result,
            self.test_result,
            self.benchmark_result,
        )
        if (
            len({value.manifest_hash for value in results}) != 1
            or len({value.as_of for value in results}) != 1
            or not self.training_result.snapshots
            or not self.test_result.snapshots
            or not self.benchmark_result.snapshots
            or self.training_result.snapshots[0].session_date
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
                "portfolio fold results do not cover declared intervals"
            )
        if (
            not isinstance(self.selection_score, Decimal)
            or not self.selection_score.is_finite()
        ):
            raise ValueError(
                "portfolio selection score must be finite"
            )
        object.__setattr__(
            self,
            "fold_hash",
            _canonical_hash(
                {
                    "benchmark_result_hash": (
                        self.benchmark_result.result_hash
                    ),
                    "selected": self.selected.payload(),
                    "selection_score": _decimal_text(
                        self.selection_score
                    ),
                    "sequence": self.sequence,
                    "test_end": self.test_end.isoformat(),
                    "test_result_hash": self.test_result.result_hash,
                    "test_start": self.test_start.isoformat(),
                    "train_end": self.train_end.isoformat(),
                    "train_start": self.train_start.isoformat(),
                    "training_result_hash": (
                        self.training_result.result_hash
                    ),
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class PortfolioWalkForwardResult:
    manifest_hash: str
    instruments: tuple[str, ...]
    as_of: datetime
    config: PortfolioWalkForwardConfig
    folds: tuple[PortfolioWalkForwardFold, ...]
    compounded_oos_return: Decimal
    benchmark_compounded_oos_return: Decimal
    excess_oos_return: Decimal
    profitable_fold_rate: Decimal
    worst_oos_drawdown: Decimal
    mean_training_return: Decimal
    selection_optimism: Decimal
    validation_version: str = PORTFOLIO_VALIDATION_VERSION
    objective_version: str = PORTFOLIO_OBJECTIVE_VERSION
    result_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.manifest_hash,
            name="portfolio validation manifest hash",
        )
        instruments = tuple(sorted(self.instruments))
        folds = tuple(self.folds)
        if (
            len(instruments) < 3
            or len(set(instruments)) != len(instruments)
            or not folds
            or tuple(value.sequence for value in folds)
            != tuple(range(1, len(folds) + 1))
            or any(
                current.test_end >= following.test_start
                for current, following in pairwise(folds)
            )
        ):
            raise ValueError(
                "portfolio validation identity or folds are invalid"
            )
        object.__setattr__(self, "instruments", instruments)
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
        )
        if any(actual != value for actual, value in expected):
            raise ValueError(
                "portfolio validation aggregate metrics are inconsistent"
            )
        mean_test = sum(test_returns, Decimal("0")) / Decimal(
            len(folds)
        )
        if self.selection_optimism != (
            self.mean_training_return - mean_test
        ):
            raise ValueError(
                "portfolio selection optimism is inconsistent"
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
                    "instruments": list(instruments),
                    "manifest_hash": self.manifest_hash,
                    "objective_version": self.objective_version,
                    "profitable_fold_rate": _decimal_text(
                        self.profitable_fold_rate
                    ),
                    "selection_optimism": _decimal_text(
                        self.selection_optimism
                    ),
                    "validation_version": self.validation_version,
                    "worst_oos_drawdown": _decimal_text(
                        self.worst_oos_drawdown
                    ),
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class PortfolioValidationEvidencePolicy:
    minimum_folds: int = 6
    minimum_oos_sessions: int = 126
    minimum_profitable_fold_rate: Decimal = Decimal("0.50")
    maximum_oos_drawdown: Decimal = Decimal("0.12")
    maximum_selection_optimism: Decimal = Decimal("0.10")
    version: str = PORTFOLIO_EVIDENCE_POLICY_VERSION
    policy_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if self.minimum_folds < 3 or self.minimum_oos_sessions < 60:
            raise ValueError(
                "portfolio evidence sample requirements are invalid"
            )
        for name, value in (
            (
                "minimum_profitable_fold_rate",
                self.minimum_profitable_fold_rate,
            ),
            ("maximum_oos_drawdown", self.maximum_oos_drawdown),
            (
                "maximum_selection_optimism",
                self.maximum_selection_optimism,
            ),
        ):
            if (
                not isinstance(value, Decimal)
                or not value.is_finite()
                or not Decimal("0") <= value <= Decimal("1")
            ):
                raise ValueError(f"{name} must be between zero and one")
        object.__setattr__(
            self,
            "policy_hash",
            _canonical_hash(
                {
                    "maximum_oos_drawdown": _decimal_text(
                        self.maximum_oos_drawdown
                    ),
                    "maximum_selection_optimism": _decimal_text(
                        self.maximum_selection_optimism
                    ),
                    "minimum_folds": self.minimum_folds,
                    "minimum_oos_sessions": self.minimum_oos_sessions,
                    "minimum_profitable_fold_rate": _decimal_text(
                        self.minimum_profitable_fold_rate
                    ),
                    "version": self.version,
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class PortfolioValidationEvidence:
    result_hash: str
    policy_hash: str
    fold_count: int
    oos_sessions: int
    rejected_order_count: int
    evidence_status: str
    gate_failures: tuple[str, ...]
    assessment_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.result_hash,
            name="portfolio evidence result hash",
        )
        _require_lowercase_sha256(
            self.policy_hash,
            name="portfolio evidence policy hash",
        )
        if (
            self.fold_count < 1
            or self.oos_sessions < 1
            or self.rejected_order_count < 0
            or self.evidence_status
            not in {"research_candidate", "insufficient", "rejected"}
        ):
            raise ValueError(
                "portfolio evidence counts or status are invalid"
            )
        failures = tuple(sorted(self.gate_failures))
        if len(set(failures)) != len(failures):
            raise ValueError(
                "portfolio evidence failures must be unique"
            )
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
                    "rejected_order_count": (
                        self.rejected_order_count
                    ),
                    "result_hash": self.result_hash,
                }
            ),
        )


def assess_portfolio_validation(
    result: PortfolioWalkForwardResult,
    *,
    policy: PortfolioValidationEvidencePolicy | None = None,
) -> PortfolioValidationEvidence:
    active_policy = policy or PortfolioValidationEvidencePolicy()
    fold_count = len(result.folds)
    oos_sessions = sum(
        len(value.test_result.snapshots) for value in result.folds
    )
    rejected_order_count = sum(
        report.state is ExecutionState.REJECTED
        for fold in result.folds
        for run in (
            fold.training_result,
            fold.test_result,
            fold.benchmark_result,
        )
        for report in run.reports
    )
    failures: list[str] = []
    if fold_count < active_policy.minimum_folds:
        failures.append("minimum_fold_count")
    if oos_sessions < active_policy.minimum_oos_sessions:
        failures.append("minimum_oos_sessions")
    if result.compounded_oos_return <= 0:
        failures.append("nonpositive_oos_return")
    if result.excess_oos_return <= 0:
        failures.append("nonpositive_excess_return")
    if (
        result.profitable_fold_rate
        < active_policy.minimum_profitable_fold_rate
    ):
        failures.append("profitable_fold_rate")
    if result.worst_oos_drawdown > active_policy.maximum_oos_drawdown:
        failures.append("oos_drawdown_limit")
    if (
        result.selection_optimism
        > active_policy.maximum_selection_optimism
    ):
        failures.append("selection_optimism")
    if rejected_order_count:
        failures.append("execution_rejections")
    sample_failures = {
        "minimum_fold_count",
        "minimum_oos_sessions",
    }
    status = (
        "research_candidate"
        if not failures
        else "insufficient"
        if set(failures).issubset(sample_failures)
        else "rejected"
    )
    return PortfolioValidationEvidence(
        result_hash=result.result_hash,
        policy_hash=active_policy.policy_hash,
        fold_count=fold_count,
        oos_sessions=oos_sessions,
        rejected_order_count=rejected_order_count,
        evidence_status=status,
        gate_failures=tuple(failures),
    )


class PortfolioWalkForwardValidator:
    """Nested portfolio validation on one immutable common calendar."""

    def __init__(
        self,
        *,
        control_repository: ManifestControlPort,
        dataset_reader: DailyDatasetReaderPort,
        compiler: MarketCompilerPort | None = None,
    ) -> None:
        self._control = control_repository
        self._reader = dataset_reader
        self._compiler = compiler or ManifestMarketCompiler()

    async def run(
        self,
        *,
        manifest_hash: str,
        config: PortfolioWalkForwardConfig,
    ) -> PortfolioWalkForwardResult:
        manifest = await self._control.read_manifest(manifest_hash)
        if len(manifest.instruments) < 3:
            raise ValueError(
                "portfolio validation requires at least three instruments"
            )
        if any(
            value.selection_count > len(manifest.instruments)
            for value in config.candidates
        ):
            raise ValueError(
                "portfolio candidate selects more instruments than available"
            )
        dataset = await self._reader.query(
            manifest.manifest_hash,
            manifest.as_of,
        )
        markets = compile_common_calendar_markets(
            instruments=manifest.instruments,
            dataset=dataset,
            compiler=self._compiler,
        )
        sessions = _market_sessions(markets)
        minimum = (
            config.train_sessions
            + config.embargo_sessions
            + config.test_sessions
        )
        if len(sessions) < minimum:
            raise ValueError(
                "portfolio walk-forward history is insufficient"
            )
        folds: list[PortfolioWalkForwardFold] = []
        test_start_index = (
            config.train_sessions + config.embargo_sessions
        )
        while test_start_index + config.test_sessions <= len(
            sessions
        ):
            train_end_index = (
                test_start_index - config.embargo_sessions
            )
            train_start_index = (
                train_end_index - config.train_sessions
            )
            train_sessions = sessions[
                train_start_index:train_end_index
            ]
            test_sessions = sessions[
                test_start_index : test_start_index
                + config.test_sessions
            ]
            selected, score, training = _select_candidate(
                manifest_hash=manifest.manifest_hash,
                as_of=manifest.as_of,
                sessions=train_sessions,
                config=config,
            )
            history_start = max(
                0,
                test_start_index
                - selected.lookback_sessions
                - 1,
            )
            test_with_history = sessions[
                history_start : test_start_index
                + config.test_sessions
            ]
            test = _run_cross_sectional(
                manifest_hash=manifest.manifest_hash,
                as_of=manifest.as_of,
                sessions=test_with_history,
                parameters=selected,
                config=config,
                trade_start=test_sessions[0][0].bar.session_date,
                trade_end=test_sessions[-1][0].bar.session_date,
            )
            benchmark = _run_equal_weight_buy_hold(
                manifest_hash=manifest.manifest_hash,
                as_of=manifest.as_of,
                sessions=test_sessions,
                config=config,
            )
            folds.append(
                PortfolioWalkForwardFold(
                    sequence=len(folds) + 1,
                    train_start=train_sessions[0][0].bar.session_date,
                    train_end=train_sessions[-1][0].bar.session_date,
                    test_start=test_sessions[0][0].bar.session_date,
                    test_end=test_sessions[-1][0].bar.session_date,
                    selected=selected,
                    selection_score=score,
                    training_result=training,
                    test_result=test,
                    benchmark_result=benchmark,
                )
            )
            test_start_index += config.test_sessions
        return _result(
            manifest_hash=manifest.manifest_hash,
            instruments=manifest.instruments,
            as_of=manifest.as_of,
            config=config,
            folds=tuple(folds),
        )


def _market_sessions(
    markets: dict[str, tuple[MarketState, ...]],
) -> tuple[tuple[MarketState, ...], ...]:
    instruments = tuple(sorted(markets))
    count = len(markets[instruments[0]])
    values = tuple(
        tuple(markets[instrument][index] for instrument in instruments)
        for index in range(count)
    )
    if any(
        len({market.bar.session_date for market in session}) != 1
        for session in values
    ):
        raise ValueError("portfolio market sessions are not aligned")
    return values


def _select_candidate(
    *,
    manifest_hash: str,
    as_of: datetime,
    sessions: tuple[tuple[MarketState, ...], ...],
    config: PortfolioWalkForwardConfig,
) -> tuple[
    CrossSectionalMomentumParameters,
    Decimal,
    BacktestResult,
]:
    candidates: list[
        tuple[
            Decimal,
            CrossSectionalMomentumParameters,
            BacktestResult,
        ]
    ] = []
    for parameters in config.candidates:
        result = _run_cross_sectional(
            manifest_hash=manifest_hash,
            as_of=as_of,
            sessions=sessions,
            parameters=parameters,
            config=config,
            trade_start=sessions[0][0].bar.session_date,
            trade_end=sessions[-1][0].bar.session_date,
        )
        score = (
            result.total_return
            - result.max_drawdown
            - result.turnover * Decimal("0.001")
        )
        candidates.append((score, parameters, result))
    score, parameters, result = max(
        candidates,
        key=lambda value: (
            value[0],
            -value[1].lookback_sessions,
            -value[1].rebalance_sessions,
            -value[1].selection_count,
        ),
    )
    return parameters, score, result


def _run_cross_sectional(
    *,
    manifest_hash: str,
    as_of: datetime,
    sessions: tuple[tuple[MarketState, ...], ...],
    parameters: CrossSectionalMomentumParameters,
    config: PortfolioWalkForwardConfig,
    trade_start: date,
    trade_end: date,
) -> BacktestResult:
    start_index, end_index = _trade_interval_indices(
        sessions=sessions,
        trade_start=trade_start,
        trade_end=trade_end,
    )
    market_sessions = sessions[start_index : end_index + 1]
    policy = _CrossSectionalOrderPolicy(
        all_sessions=sessions,
        start_index=start_index,
        trade_session_count=len(market_sessions),
        parameters=parameters,
        config=config,
    )
    return BacktestEngine(
        execution=ExecutionModel(
            slippage_bps=config.slippage_bps
        )
    ).run_dynamic(
        strategy_id=parameters.strategy_id,
        manifest_hash=manifest_hash,
        as_of=as_of,
        initial_cash=config.initial_cash,
        market_sessions=market_sessions,
        order_factory=policy,
    )


class _CrossSectionalOrderPolicy:
    def __init__(
        self,
        *,
        all_sessions: tuple[tuple[MarketState, ...], ...],
        start_index: int,
        trade_session_count: int,
        parameters: CrossSectionalMomentumParameters,
        config: PortfolioWalkForwardConfig,
    ) -> None:
        self._sessions = all_sessions
        self._start_index = start_index
        self._trade_session_count = trade_session_count
        self._parameters = parameters
        self._config = config
        self._last_rebalance: int | None = None
        self._order_sequence = 0

    def __call__(
        self,
        trade_index: int,
        markets: tuple[MarketState, ...],
        previous: AccountSnapshot | None,
    ) -> tuple[OrderIntent, ...]:
        absolute_index = self._start_index + trade_index
        session_date = markets[0].bar.session_date
        holdings = (
            {}
            if previous is None
            else {
                value.instrument: value.total_quantity
                for value in previous.positions
            }
        )
        if trade_index == self._trade_session_count - 1:
            return tuple(
                self._order(
                    instrument=instrument,
                    side=OrderSide.SELL,
                    quantity=quantity,
                    session_date=session_date,
                    suffix="forced-exit",
                )
                for instrument, quantity in sorted(holdings.items())
            )
        can_signal = (
            absolute_index > self._parameters.lookback_sessions
        )
        rebalance = can_signal and (
            self._last_rebalance is None
            or trade_index - self._last_rebalance
            >= self._parameters.rebalance_sessions
        )
        if not rebalance:
            return ()
        self._last_rebalance = trade_index
        selected = self._selected(absolute_index)
        orders: list[OrderIntent] = []
        for instrument in sorted(set(holdings) - selected):
            orders.append(
                self._order(
                    instrument=instrument,
                    side=OrderSide.SELL,
                    quantity=holdings[instrument],
                    session_date=session_date,
                    suffix="exit",
                )
            )
        market_by_instrument = {
            value.bar.instrument: value for value in markets
        }
        for instrument in sorted(selected - set(holdings)):
            market = market_by_instrument[instrument]
            quantity = _baseline_quantity(
                initial_cash=self._config.initial_cash,
                allocation=(
                    self._config.gross_allocation
                    / self._parameters.selection_count
                ),
                reference_price=market.bar.pre_close,
                slippage_bps=self._config.slippage_bps,
                buy_minimum=market.rules.buy_minimum,
                buy_step=market.rules.buy_step,
                maximum=market.rules.max_order_quantity,
            )
            orders.append(
                self._order(
                    instrument=instrument,
                    side=OrderSide.BUY,
                    quantity=quantity,
                    session_date=session_date,
                    suffix="entry",
                )
            )
        return tuple(orders)

    def _selected(self, absolute_index: int) -> set[str]:
        previous = self._sessions[absolute_index - 1]
        lookback = self._sessions[
            absolute_index
            - self._parameters.lookback_sessions
            - 1
        ]
        previous_by_instrument = {
            value.bar.instrument: value for value in previous
        }
        lookback_by_instrument = {
            value.bar.instrument: value for value in lookback
        }
        scores = tuple(
            sorted(
                (
                    (
                        previous_by_instrument[
                            instrument
                        ].bar.close_price
                        / lookback_by_instrument[
                            instrument
                        ].bar.close_price
                        - Decimal("1"),
                        instrument,
                    )
                    for instrument in previous_by_instrument
                ),
                key=lambda value: (-value[0], value[1]),
            )
        )
        return {
            instrument
            for score, instrument in scores[
                : self._parameters.selection_count
            ]
            if score > 0
        }

    def _order(
        self,
        *,
        instrument: str,
        side: OrderSide,
        quantity: int,
        session_date: date,
        suffix: str,
    ) -> OrderIntent:
        self._order_sequence += 1
        return _order(
            order_id=(
                f"portfolio-{self._order_sequence:05d}-{suffix}"
            ),
            instrument=instrument,
            side=side,
            quantity=quantity,
            session_date=session_date,
        )


def _trade_interval_indices(
    *,
    sessions: tuple[tuple[MarketState, ...], ...],
    trade_start: date,
    trade_end: date,
) -> tuple[int, int]:
    if trade_start > trade_end:
        raise ValueError("portfolio trade interval is invalid")
    dates = tuple(value[0].bar.session_date for value in sessions)
    try:
        start_index = dates.index(trade_start)
        end_index = dates.index(trade_end)
    except ValueError:
        raise ValueError(
            "portfolio trade interval is outside market sessions"
        ) from None
    return start_index, end_index


def _run_equal_weight_buy_hold(
    *,
    manifest_hash: str,
    as_of: datetime,
    sessions: tuple[tuple[MarketState, ...], ...],
    config: PortfolioWalkForwardConfig,
) -> BacktestResult:
    instruments = tuple(
        value.bar.instrument for value in sessions[0]
    )
    allocation = config.gross_allocation / Decimal(len(instruments))
    quantities = {
        market.bar.instrument: _baseline_quantity(
            initial_cash=config.initial_cash,
            allocation=allocation,
            reference_price=market.bar.pre_close,
            slippage_bps=config.slippage_bps,
            buy_minimum=market.rules.buy_minimum,
            buy_step=market.rules.buy_step,
            maximum=market.rules.max_order_quantity,
        )
        for market in sessions[0]
    }
    def orders(
        index: int,
        markets: tuple[MarketState, ...],
        previous: AccountSnapshot | None,
    ) -> tuple[OrderIntent, ...]:
        session_date = markets[0].bar.session_date
        if index == 0:
            return tuple(
                _order(
                    order_id=f"benchmark-entry-{instrument}",
                    instrument=instrument,
                    side=OrderSide.BUY,
                    quantity=quantities[instrument],
                    session_date=session_date,
                )
                for instrument in instruments
            )
        if index == len(sessions) - 1 and previous is not None:
            return tuple(
                _order(
                    order_id=f"benchmark-exit-{position.instrument}",
                    instrument=position.instrument,
                    side=OrderSide.SELL,
                    quantity=position.total_quantity,
                    session_date=session_date,
                )
                for position in previous.positions
            )
        return ()

    return BacktestEngine(
        execution=ExecutionModel(
            slippage_bps=config.slippage_bps
        )
    ).run_dynamic(
        strategy_id="equal_weight_buy_hold_v1",
        manifest_hash=manifest_hash,
        as_of=as_of,
        initial_cash=config.initial_cash,
        market_sessions=sessions,
        order_factory=orders,
    )


def _result(
    *,
    manifest_hash: str,
    instruments: tuple[str, ...],
    as_of: datetime,
    config: PortfolioWalkForwardConfig,
    folds: tuple[PortfolioWalkForwardFold, ...],
) -> PortfolioWalkForwardResult:
    if not folds:
        raise ValueError("portfolio validation produced no folds")
    test_returns = tuple(
        value.test_result.total_return for value in folds
    )
    benchmark_returns = tuple(
        value.benchmark_result.total_return for value in folds
    )
    training_returns = tuple(
        value.training_result.total_return for value in folds
    )
    compounded = _compound(test_returns)
    benchmark = _compound(benchmark_returns)
    mean_test = sum(test_returns, Decimal("0")) / Decimal(
        len(folds)
    )
    mean_training = sum(
        training_returns,
        Decimal("0"),
    ) / Decimal(len(folds))
    return PortfolioWalkForwardResult(
        manifest_hash=manifest_hash,
        instruments=instruments,
        as_of=as_of,
        config=config,
        folds=folds,
        compounded_oos_return=compounded,
        benchmark_compounded_oos_return=benchmark,
        excess_oos_return=compounded - benchmark,
        profitable_fold_rate=Decimal(
            sum(value > 0 for value in test_returns)
        )
        / Decimal(len(folds)),
        worst_oos_drawdown=max(
            value.test_result.max_drawdown for value in folds
        ),
        mean_training_return=mean_training,
        selection_optimism=mean_training - mean_test,
    )


def _compound(values: tuple[Decimal, ...]) -> Decimal:
    result = Decimal("1")
    for value in values:
        result *= Decimal("1") + value
    return result - Decimal("1")
