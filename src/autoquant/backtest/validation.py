from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from itertools import pairwise
from typing import Protocol

from autoquant.backtest.engine import BacktestEngine
from autoquant.backtest.ledger import ExecutionModel
from autoquant.backtest.models import (
    BacktestResult,
    BacktestSession,
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
from autoquant.data.daily_ingestion import ValidatedDailyDataset
from autoquant.data.models import _canonical_hash, _decimal_text, _require_lowercase_sha256

VALIDATION_VERSION = "rolling-walk-forward-v2"
SELECTION_OBJECTIVE_VERSION = "return-minus-drawdown-turnover-v1"


class MarketCompilerPort(Protocol):
    def compile(
        self, instrument: str, dataset: ValidatedDailyDataset
    ) -> tuple[MarketState, ...]: ...


@dataclass(frozen=True, slots=True, order=True)
class SmaParameters:
    fast_sessions: int
    slow_sessions: int

    def __post_init__(self) -> None:
        if not 2 <= self.fast_sessions <= 60:
            raise ValueError("fast_sessions must be between 2 and 60")
        if not 5 <= self.slow_sessions <= 250:
            raise ValueError("slow_sessions must be between 5 and 250")
        if self.fast_sessions >= self.slow_sessions:
            raise ValueError("fast_sessions must be smaller than slow_sessions")


@dataclass(frozen=True, slots=True)
class WalkForwardConfig:
    initial_cash: Decimal = Decimal("1000000")
    allocation: Decimal = Decimal("0.95")
    slippage_bps: Decimal = Decimal("5")
    train_sessions: int = 120
    test_sessions: int = 40
    embargo_sessions: int = 1
    candidates: tuple[SmaParameters, ...] = (
        SmaParameters(5, 20),
        SmaParameters(10, 30),
        SmaParameters(20, 60),
    )

    def __post_init__(self) -> None:
        for name, value in (
            ("initial_cash", self.initial_cash),
            ("allocation", self.allocation),
            ("slippage_bps", self.slippage_bps),
        ):
            if not isinstance(value, Decimal) or not value.is_finite():
                raise ValueError(f"{name} must be a finite Decimal")
        if self.initial_cash < 10_000 or self.initial_cash > 1_000_000_000:
            raise ValueError("initial_cash must be between 10000 and 1000000000")
        if self.allocation <= 0 or self.allocation > 1:
            raise ValueError("allocation must be between zero and one")
        if self.slippage_bps < 0 or self.slippage_bps > 100:
            raise ValueError("slippage_bps must be between 0 and 100")
        if not 60 <= self.train_sessions <= 750:
            raise ValueError("train_sessions must be between 60 and 750")
        if not 20 <= self.test_sessions <= 250:
            raise ValueError("test_sessions must be between 20 and 250")
        if not 1 <= self.embargo_sessions <= 20:
            raise ValueError("embargo_sessions must be between 1 and 20")
        candidates = tuple(self.candidates)
        object.__setattr__(self, "candidates", candidates)
        if not candidates or len(candidates) > 25 or len(set(candidates)) != len(candidates):
            raise ValueError("candidates must contain 1-25 unique parameter sets")
        if max(candidate.slow_sessions for candidate in candidates) >= self.train_sessions:
            raise ValueError("every slow window must be smaller than train_sessions")


@dataclass(frozen=True, slots=True)
class WalkForwardFold:
    sequence: int
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    selected: SmaParameters
    selection_score: Decimal
    training_result: BacktestResult
    test_result: BacktestResult
    benchmark_result: BacktestResult | None = None
    fold_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if self.sequence < 1:
            raise ValueError("fold sequence must be positive")
        if not self.train_start <= self.train_end < self.test_start <= self.test_end:
            raise ValueError("fold intervals must be ordered and disjoint")
        expected_strategy = (
            f"sma_cross_v1:fast={self.selected.fast_sessions}:"
            f"slow={self.selected.slow_sessions}"
        )
        if (
            self.training_result.strategy_id != expected_strategy
            or self.test_result.strategy_id != expected_strategy
        ):
            raise ValueError("fold strategy does not match selected parameters")
        if (
            self.training_result.manifest_hash != self.test_result.manifest_hash
            or self.training_result.as_of != self.test_result.as_of
        ):
            raise ValueError("fold results must share one manifest cutoff")
        if (
            not self.training_result.snapshots
            or self.training_result.snapshots[0].session_date != self.train_start
            or self.training_result.snapshots[-1].session_date != self.train_end
        ):
            raise ValueError("training snapshots must cover the declared interval")
        if (
            not self.test_result.snapshots
            or self.test_result.snapshots[0].session_date != self.test_start
            or self.test_result.snapshots[-1].session_date != self.test_end
        ):
            raise ValueError("test snapshots must cover the declared interval")
        if self.benchmark_result is not None:
            if self.benchmark_result.strategy_id != "buy_hold_benchmark_v1":
                raise ValueError("fold benchmark has an unexpected strategy")
            if (
                self.benchmark_result.manifest_hash
                != self.training_result.manifest_hash
                or self.benchmark_result.as_of != self.training_result.as_of
                or not self.benchmark_result.snapshots
                or self.benchmark_result.snapshots[0].session_date != self.test_start
                or self.benchmark_result.snapshots[-1].session_date != self.test_end
            ):
                raise ValueError("fold benchmark must cover the declared test interval")
        fold_payload: dict[str, object] = {
            "selected": {
                "fast_sessions": self.selected.fast_sessions,
                "slow_sessions": self.selected.slow_sessions,
            },
            "selection_score": _decimal_text(self.selection_score),
            "sequence": self.sequence,
            "test_end": self.test_end.isoformat(),
            "test_result_hash": self.test_result.result_hash,
            "test_start": self.test_start.isoformat(),
            "train_end": self.train_end.isoformat(),
            "train_start": self.train_start.isoformat(),
            "training_result_hash": self.training_result.result_hash,
        }
        if self.benchmark_result is not None:
            fold_payload["benchmark_result_hash"] = self.benchmark_result.result_hash
        object.__setattr__(
            self,
            "fold_hash",
            _canonical_hash(fold_payload),
        )


@dataclass(frozen=True, slots=True)
class WalkForwardResult:
    manifest_hash: str
    instrument: str
    as_of: datetime
    config: WalkForwardConfig
    folds: tuple[WalkForwardFold, ...]
    compounded_oos_return: Decimal
    mean_oos_return: Decimal
    worst_oos_drawdown: Decimal
    profitable_fold_rate: Decimal
    mean_training_return: Decimal
    selection_optimism: Decimal
    benchmark_compounded_oos_return: Decimal | None = None
    excess_oos_return: Decimal | None = None
    validation_version: str = VALIDATION_VERSION
    objective_version: str = SELECTION_OBJECTIVE_VERSION
    result_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(self.manifest_hash, name="manifest_hash")
        folds = tuple(self.folds)
        object.__setattr__(self, "folds", folds)
        if not folds:
            raise ValueError("walk-forward result requires at least one fold")
        if tuple(fold.sequence for fold in folds) != tuple(range(1, len(folds) + 1)):
            raise ValueError("fold sequences must be contiguous")
        if any(
            current.test_end >= following.test_start
            for current, following in pairwise(folds)
        ):
            raise ValueError("test folds must be non-overlapping and ordered")
        if any(
            fold.training_result.manifest_hash != self.manifest_hash
            or fold.test_result.manifest_hash != self.manifest_hash
            or fold.training_result.as_of != self.as_of
            or fold.test_result.as_of != self.as_of
            for fold in folds
        ):
            raise ValueError("fold results must match the validation manifest cutoff")
        test_returns = tuple(fold.test_result.total_return for fold in folds)
        training_returns = tuple(
            fold.training_result.total_return for fold in folds
        )
        compounded_factor = Decimal("1")
        for value in test_returns:
            compounded_factor *= Decimal("1") + value
        expected_values = (
            (self.compounded_oos_return, compounded_factor - Decimal("1")),
            (
                self.mean_oos_return,
                sum(test_returns, Decimal("0")) / len(test_returns),
            ),
            (
                self.worst_oos_drawdown,
                max(fold.test_result.max_drawdown for fold in folds),
            ),
            (
                self.profitable_fold_rate,
                Decimal(sum(value > 0 for value in test_returns)) / len(test_returns),
            ),
            (
                self.mean_training_return,
                sum(training_returns, Decimal("0")) / len(training_returns),
            ),
        )
        if any(actual != expected for actual, expected in expected_values):
            raise ValueError("validation aggregate metrics do not match folds")
        if self.selection_optimism != self.mean_training_return - self.mean_oos_return:
            raise ValueError("selection_optimism does not match fold means")
        benchmark_results = tuple(
            fold.benchmark_result
            for fold in folds
            if fold.benchmark_result is not None
        )
        if benchmark_results and len(benchmark_results) != len(folds):
            raise ValueError("every fold must either include or omit a benchmark")
        if benchmark_results:
            benchmark_factor = Decimal("1")
            for benchmark in benchmark_results:
                benchmark_factor *= Decimal("1") + benchmark.total_return
            expected_benchmark = benchmark_factor - Decimal("1")
            if self.benchmark_compounded_oos_return != expected_benchmark:
                raise ValueError("benchmark aggregate does not match folds")
            if self.excess_oos_return != self.compounded_oos_return - expected_benchmark:
                raise ValueError("excess_oos_return does not match the benchmark")
        elif (
            self.benchmark_compounded_oos_return is not None
            or self.excess_oos_return is not None
        ):
            raise ValueError("benchmark aggregates require fold benchmarks")
        payload = {
            "as_of": self.as_of.isoformat(timespec="microseconds"),
            "compounded_oos_return": _decimal_text(self.compounded_oos_return),
            "config": _config_payload(self.config),
            "fold_hashes": [fold.fold_hash for fold in folds],
            "instrument": self.instrument,
            "manifest_hash": self.manifest_hash,
            "mean_oos_return": _decimal_text(self.mean_oos_return),
            "mean_training_return": _decimal_text(self.mean_training_return),
            "objective_version": self.objective_version,
            "profitable_fold_rate": _decimal_text(self.profitable_fold_rate),
            "selection_optimism": _decimal_text(self.selection_optimism),
            "validation_version": self.validation_version,
            "worst_oos_drawdown": _decimal_text(self.worst_oos_drawdown),
        }
        if self.benchmark_compounded_oos_return is not None:
            if self.excess_oos_return is None:
                raise ValueError("benchmark aggregate requires excess_oos_return")
            payload["benchmark_compounded_oos_return"] = _decimal_text(
                self.benchmark_compounded_oos_return
            )
            payload["excess_oos_return"] = _decimal_text(self.excess_oos_return)
        object.__setattr__(self, "result_hash", _canonical_hash(payload))


class WalkForwardValidator:
    """Select parameters only in rolling training windows and score untouched tests."""

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
        instrument: str,
        config: WalkForwardConfig,
    ) -> WalkForwardResult:
        manifest = await self._control.read_manifest(manifest_hash)
        if instrument not in manifest.instruments:
            raise ValueError("instrument is not present in the selected manifest")
        dataset = await self._reader.query(manifest.manifest_hash, manifest.as_of)
        markets = self._compiler.compile(instrument, dataset)
        minimum = config.train_sessions + config.embargo_sessions + config.test_sessions
        if len(markets) < minimum:
            raise ValueError(
                f"walk-forward validation requires at least {minimum} trading sessions"
            )

        folds: list[WalkForwardFold] = []
        test_start_index = config.train_sessions + config.embargo_sessions
        while test_start_index + config.test_sessions <= len(markets):
            train_end_index = test_start_index - config.embargo_sessions
            train_start_index = train_end_index - config.train_sessions
            train_markets = markets[train_start_index:train_end_index]
            test_markets = markets[
                test_start_index : test_start_index + config.test_sessions
            ]
            selected, score, training = self._select(
                manifest_hash=manifest.manifest_hash,
                as_of=manifest.as_of,
                markets=train_markets,
                config=config,
            )
            history_start = max(0, test_start_index - selected.slow_sessions)
            test_with_history = markets[
                history_start : test_start_index + config.test_sessions
            ]
            test = _run_sma(
                manifest_hash=manifest.manifest_hash,
                as_of=manifest.as_of,
                markets=test_with_history,
                parameters=selected,
                config=config,
                trade_start=test_markets[0].bar.session_date,
                trade_end=test_markets[-1].bar.session_date,
            )
            benchmark = _run_buy_hold(
                manifest_hash=manifest.manifest_hash,
                as_of=manifest.as_of,
                markets=test_markets,
                config=config,
            )
            folds.append(
                WalkForwardFold(
                    sequence=len(folds) + 1,
                    train_start=train_markets[0].bar.session_date,
                    train_end=train_markets[-1].bar.session_date,
                    test_start=test_markets[0].bar.session_date,
                    test_end=test_markets[-1].bar.session_date,
                    selected=selected,
                    selection_score=score,
                    training_result=training,
                    test_result=test,
                    benchmark_result=benchmark,
                )
            )
            test_start_index += config.test_sessions

        test_returns = tuple(fold.test_result.total_return for fold in folds)
        training_returns = tuple(
            fold.training_result.total_return for fold in folds
        )
        mean_test = sum(test_returns, Decimal("0")) / len(test_returns)
        mean_training = sum(training_returns, Decimal("0")) / len(training_returns)
        compounded_factor = Decimal("1")
        for value in test_returns:
            compounded_factor *= Decimal("1") + value
        benchmark_factor = Decimal("1")
        for fold in folds:
            if fold.benchmark_result is None:
                raise RuntimeError("validator produced a fold without a benchmark")
            benchmark_factor *= Decimal("1") + fold.benchmark_result.total_return
        compounded = compounded_factor - Decimal("1")
        benchmark_compounded = benchmark_factor - Decimal("1")
        return WalkForwardResult(
            manifest_hash=manifest.manifest_hash,
            instrument=instrument,
            as_of=manifest.as_of,
            config=config,
            folds=tuple(folds),
            compounded_oos_return=compounded,
            mean_oos_return=mean_test,
            worst_oos_drawdown=max(
                fold.test_result.max_drawdown for fold in folds
            ),
            profitable_fold_rate=(
                Decimal(sum(value > 0 for value in test_returns)) / len(test_returns)
            ),
            mean_training_return=mean_training,
            selection_optimism=mean_training - mean_test,
            benchmark_compounded_oos_return=benchmark_compounded,
            excess_oos_return=compounded - benchmark_compounded,
        )

    @staticmethod
    def _select(
        *,
        manifest_hash: str,
        as_of: datetime,
        markets: tuple[MarketState, ...],
        config: WalkForwardConfig,
    ) -> tuple[SmaParameters, Decimal, BacktestResult]:
        candidates: list[tuple[Decimal, SmaParameters, BacktestResult]] = []
        for parameters in config.candidates:
            result = _run_sma(
                manifest_hash=manifest_hash,
                as_of=as_of,
                markets=markets,
                parameters=parameters,
                config=config,
                trade_start=markets[0].bar.session_date,
                trade_end=markets[-1].bar.session_date,
            )
            score = (
                result.total_return
                - result.max_drawdown
                - result.turnover * Decimal("0.001")
            )
            candidates.append((score, parameters, result))
        score, parameters, result = max(
            candidates,
            key=lambda item: (item[0], -item[1].fast_sessions, -item[1].slow_sessions),
        )
        return parameters, score, result


def _run_sma(
    *,
    manifest_hash: str,
    as_of: datetime,
    markets: tuple[MarketState, ...],
    parameters: SmaParameters,
    config: WalkForwardConfig,
    trade_start: date,
    trade_end: date,
) -> BacktestResult:
    sessions = _sma_sessions(
        markets=markets,
        parameters=parameters,
        config=config,
        trade_start=trade_start,
        trade_end=trade_end,
    )
    return BacktestEngine(
        execution=ExecutionModel(slippage_bps=config.slippage_bps)
    ).run(
        strategy_id=(
            f"sma_cross_v1:fast={parameters.fast_sessions}:"
            f"slow={parameters.slow_sessions}"
        ),
        manifest_hash=manifest_hash,
        as_of=as_of,
        initial_cash=config.initial_cash,
        sessions=sessions,
    )


def _run_buy_hold(
    *,
    manifest_hash: str,
    as_of: datetime,
    markets: tuple[MarketState, ...],
    config: WalkForwardConfig,
) -> BacktestResult:
    if len(markets) < 2:
        raise ValueError("benchmark requires at least two sessions")
    quantity = _baseline_quantity(
        initial_cash=config.initial_cash,
        allocation=config.allocation,
        reference_price=markets[0].bar.pre_close,
        slippage_bps=config.slippage_bps,
        buy_minimum=markets[0].rules.buy_minimum,
        buy_step=markets[0].rules.buy_step,
        maximum=markets[0].rules.max_order_quantity,
    )
    sessions = tuple(
        BacktestSession(
            session_date=market.bar.session_date,
            markets=(market,),
            orders=(
                (
                    _order(
                        order_id="benchmark-entry",
                        instrument=market.bar.instrument,
                        side=OrderSide.BUY,
                        quantity=quantity,
                        session_date=market.bar.session_date,
                    ),
                )
                if index == 0
                else (
                    _order(
                        order_id="benchmark-exit",
                        instrument=market.bar.instrument,
                        side=OrderSide.SELL,
                        quantity=quantity,
                        session_date=market.bar.session_date,
                    ),
                )
                if index == len(markets) - 1
                else ()
            ),
        )
        for index, market in enumerate(markets)
    )
    return BacktestEngine(
        execution=ExecutionModel(slippage_bps=config.slippage_bps)
    ).run(
        strategy_id="buy_hold_benchmark_v1",
        manifest_hash=manifest_hash,
        as_of=as_of,
        initial_cash=config.initial_cash,
        sessions=sessions,
    )
def _sma_sessions(
    *,
    markets: tuple[MarketState, ...],
    parameters: SmaParameters,
    config: WalkForwardConfig,
    trade_start: date,
    trade_end: date,
) -> tuple[BacktestSession, ...]:
    if trade_start > trade_end:
        raise ValueError("trade_start cannot follow trade_end")
    compiled: list[BacktestSession] = []
    target_invested = False
    active_quantity: int | None = None
    order_sequence = 0
    for index, market in enumerate(markets):
        session_date = market.bar.session_date
        if session_date < trade_start:
            continue
        if session_date > trade_end:
            break
        history = markets[:index]
        orders: list[OrderIntent] = []
        if len(history) >= parameters.slow_sessions:
            fast = sum(
                (item.bar.close_price for item in history[-parameters.fast_sessions :]),
                Decimal("0"),
            ) / parameters.fast_sessions
            slow = sum(
                (item.bar.close_price for item in history[-parameters.slow_sessions :]),
                Decimal("0"),
            ) / parameters.slow_sessions
            signal = fast > slow
            if signal and not target_invested:
                active_quantity = _baseline_quantity(
                    initial_cash=config.initial_cash,
                    allocation=config.allocation,
                    reference_price=market.bar.pre_close,
                    slippage_bps=config.slippage_bps,
                    buy_minimum=market.rules.buy_minimum,
                    buy_step=market.rules.buy_step,
                    maximum=market.rules.max_order_quantity,
                )
                order_sequence += 1
                orders.append(
                    _order(
                        order_id=f"sma-{order_sequence:04d}-entry",
                        instrument=market.bar.instrument,
                        side=OrderSide.BUY,
                        quantity=active_quantity,
                        session_date=session_date,
                    )
                )
                target_invested = True
            elif not signal and target_invested and active_quantity is not None:
                order_sequence += 1
                orders.append(
                    _order(
                        order_id=f"sma-{order_sequence:04d}-exit",
                        instrument=market.bar.instrument,
                        side=OrderSide.SELL,
                        quantity=active_quantity,
                        session_date=session_date,
                    )
                )
                target_invested = False
                active_quantity = None
        compiled.append(
            BacktestSession(
                session_date=session_date,
                markets=(market,),
                orders=tuple(orders),
            )
        )
    if not compiled:
        raise ValueError("trade interval contains no sessions")
    if target_invested and active_quantity is not None:
        final = compiled[-1]
        order_sequence += 1
        forced_exit = _order(
            order_id=f"sma-{order_sequence:04d}-forced-exit",
            instrument=final.markets[0].bar.instrument,
            side=OrderSide.SELL,
            quantity=active_quantity,
            session_date=final.session_date,
        )
        compiled[-1] = BacktestSession(
            session_date=final.session_date,
            markets=final.markets,
            orders=(*final.orders, forced_exit),
        )
    return tuple(compiled)


def _config_payload(config: WalkForwardConfig) -> dict[str, object]:
    return {
        "allocation": _decimal_text(config.allocation),
        "candidates": [
            {
                "fast_sessions": candidate.fast_sessions,
                "slow_sessions": candidate.slow_sessions,
            }
            for candidate in config.candidates
        ],
        "embargo_sessions": config.embargo_sessions,
        "initial_cash": _decimal_text(config.initial_cash),
        "slippage_bps": _decimal_text(config.slippage_bps),
        "test_sessions": config.test_sessions,
        "train_sessions": config.train_sessions,
    }
