from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal

from autoquant.backtest.portfolio_validation import (
    CrossSectionalMomentumParameters,
    PortfolioWalkForwardFold,
    PortfolioWalkForwardResult,
    assess_portfolio_validation,
)

PORTFOLIO_DIAGNOSTIC_VERSION = "portfolio-oos-diagnostic-v1"


@dataclass(frozen=True, slots=True)
class MomentumSelectionFrequency:
    parameters: CrossSectionalMomentumParameters
    count: int
    share: Decimal

    def __post_init__(self) -> None:
        if (
            self.count < 1
            or not isinstance(self.share, Decimal)
            or not self.share.is_finite()
            or not Decimal("0") < self.share <= Decimal("1")
        ):
            raise ValueError(
                "momentum selection frequency is invalid"
            )

    def payload(self) -> dict[str, object]:
        return {
            "count": self.count,
            "parameters": self.parameters.payload(),
            "share": _decimal_text(self.share),
        }


@dataclass(frozen=True, slots=True)
class PortfolioValidationDiagnostics:
    result_hash: str
    fold_count: int
    positive_excess_fold_rate: Decimal
    median_fold_excess_return: Decimal
    mean_positive_fold_excess: Decimal
    mean_nonpositive_fold_excess: Decimal
    first_half_excess_return: Decimal
    second_half_excess_return: Decimal
    mean_strategy_turnover: Decimal
    mean_benchmark_turnover: Decimal
    strategy_fee_rate: Decimal
    benchmark_fee_rate: Decimal
    selection_frequencies: tuple[MomentumSelectionFrequency, ...]
    maximum_selection_share: Decimal
    assessment_gate_failures: tuple[str, ...]
    diagnostic_codes: tuple[str, ...]
    version: str = PORTFOLIO_DIAGNOSTIC_VERSION
    diagnostic_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            len(self.result_hash) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.result_hash
            )
            or self.fold_count < 1
            or not self.selection_frequencies
            or sum(
                value.count for value in self.selection_frequencies
            )
            != self.fold_count
        ):
            raise ValueError(
                "portfolio diagnostic identity is invalid"
            )
        for name, value in (
            (
                "positive_excess_fold_rate",
                self.positive_excess_fold_rate,
            ),
            (
                "median_fold_excess_return",
                self.median_fold_excess_return,
            ),
            (
                "mean_positive_fold_excess",
                self.mean_positive_fold_excess,
            ),
            (
                "mean_nonpositive_fold_excess",
                self.mean_nonpositive_fold_excess,
            ),
            (
                "first_half_excess_return",
                self.first_half_excess_return,
            ),
            (
                "second_half_excess_return",
                self.second_half_excess_return,
            ),
            ("mean_strategy_turnover", self.mean_strategy_turnover),
            ("mean_benchmark_turnover", self.mean_benchmark_turnover),
            ("strategy_fee_rate", self.strategy_fee_rate),
            ("benchmark_fee_rate", self.benchmark_fee_rate),
            (
                "maximum_selection_share",
                self.maximum_selection_share,
            ),
        ):
            if not isinstance(value, Decimal) or not value.is_finite():
                raise ValueError(f"{name} must be a finite Decimal")
        if not (
            Decimal("0")
            <= self.positive_excess_fold_rate
            <= Decimal("1")
            and Decimal("0")
            < self.maximum_selection_share
            <= Decimal("1")
            and self.strategy_fee_rate >= 0
            and self.benchmark_fee_rate >= 0
        ):
            raise ValueError(
                "portfolio diagnostic rates are invalid"
            )
        frequencies = tuple(
            sorted(
                self.selection_frequencies,
                key=lambda value: value.parameters,
            )
        )
        failures = tuple(sorted(self.assessment_gate_failures))
        codes = tuple(sorted(self.diagnostic_codes))
        if (
            len({value.parameters for value in frequencies})
            != len(frequencies)
            or len(set(failures)) != len(failures)
            or len(set(codes)) != len(codes)
        ):
            raise ValueError(
                "portfolio diagnostic values must be unique"
            )
        object.__setattr__(
            self,
            "selection_frequencies",
            frequencies,
        )
        object.__setattr__(
            self,
            "assessment_gate_failures",
            failures,
        )
        object.__setattr__(self, "diagnostic_codes", codes)
        object.__setattr__(
            self,
            "diagnostic_hash",
            _canonical_hash(self.payload(include_hash=False)),
        )

    def payload(
        self,
        *,
        include_hash: bool = True,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "assessment_gate_failures": list(
                self.assessment_gate_failures
            ),
            "benchmark_fee_rate": _decimal_text(
                self.benchmark_fee_rate
            ),
            "diagnostic_codes": list(self.diagnostic_codes),
            "first_half_excess_return": _decimal_text(
                self.first_half_excess_return
            ),
            "fold_count": self.fold_count,
            "maximum_selection_share": _decimal_text(
                self.maximum_selection_share
            ),
            "mean_benchmark_turnover": _decimal_text(
                self.mean_benchmark_turnover
            ),
            "mean_nonpositive_fold_excess": _decimal_text(
                self.mean_nonpositive_fold_excess
            ),
            "mean_positive_fold_excess": _decimal_text(
                self.mean_positive_fold_excess
            ),
            "mean_strategy_turnover": _decimal_text(
                self.mean_strategy_turnover
            ),
            "median_fold_excess_return": _decimal_text(
                self.median_fold_excess_return
            ),
            "positive_excess_fold_rate": _decimal_text(
                self.positive_excess_fold_rate
            ),
            "result_hash": self.result_hash,
            "second_half_excess_return": _decimal_text(
                self.second_half_excess_return
            ),
            "selection_frequencies": [
                value.payload()
                for value in self.selection_frequencies
            ],
            "strategy_fee_rate": _decimal_text(
                self.strategy_fee_rate
            ),
            "version": self.version,
        }
        if include_hash:
            payload["diagnostic_hash"] = self.diagnostic_hash
        return payload


def diagnose_portfolio_validation(
    result: PortfolioWalkForwardResult,
) -> PortfolioValidationDiagnostics:
    fold_count = len(result.folds)
    fold_excess = tuple(
        fold.test_result.total_return
        - fold.benchmark_result.total_return
        for fold in result.folds
    )
    split = (fold_count + 1) // 2
    first = result.folds[:split]
    second = result.folds[split:]
    counts = Counter(fold.selected for fold in result.folds)
    frequencies = tuple(
        MomentumSelectionFrequency(
            parameters=parameters,
            count=count,
            share=Decimal(count) / Decimal(fold_count),
        )
        for parameters, count in counts.items()
    )
    maximum_share = max(value.share for value in frequencies)
    first_excess = _segment_excess(first)
    second_excess = (
        first_excess if not second else _segment_excess(second)
    )
    positive_rate = (
        Decimal(sum(value > 0 for value in fold_excess))
        / Decimal(fold_count)
    )
    positive_excess = tuple(
        value for value in fold_excess if value > 0
    )
    nonpositive_excess = tuple(
        value for value in fold_excess if value <= 0
    )
    assessment = assess_portfolio_validation(result)
    codes: list[str] = []
    if len(result.instruments) < 20:
        codes.append("small_universe")
    if positive_rate < Decimal("0.50"):
        codes.append("benchmark_dominates_majority_of_folds")
    if (
        result.excess_oos_return <= 0
        and positive_rate >= Decimal("0.50")
    ):
        codes.append("loss_severity_dominates_hit_rate")
    if second_excess <= 0:
        codes.append("late_sample_underperformance")
    if second and second_excess < first_excess:
        codes.append("relative_performance_decay")
    if maximum_share < Decimal("0.50"):
        codes.append("parameter_selection_unstable")
    return PortfolioValidationDiagnostics(
        result_hash=result.result_hash,
        fold_count=fold_count,
        positive_excess_fold_rate=positive_rate,
        median_fold_excess_return=_median(fold_excess),
        mean_positive_fold_excess=(
            Decimal("0")
            if not positive_excess
            else _mean(positive_excess)
        ),
        mean_nonpositive_fold_excess=(
            Decimal("0")
            if not nonpositive_excess
            else _mean(nonpositive_excess)
        ),
        first_half_excess_return=first_excess,
        second_half_excess_return=second_excess,
        mean_strategy_turnover=_mean(
            tuple(
                fold.test_result.turnover for fold in result.folds
            )
        ),
        mean_benchmark_turnover=_mean(
            tuple(
                fold.benchmark_result.turnover
                for fold in result.folds
            )
        ),
        strategy_fee_rate=_fee_rate(
            result,
            benchmark=False,
        ),
        benchmark_fee_rate=_fee_rate(
            result,
            benchmark=True,
        ),
        selection_frequencies=frequencies,
        maximum_selection_share=maximum_share,
        assessment_gate_failures=assessment.gate_failures,
        diagnostic_codes=tuple(codes),
    )


def _segment_excess(
    folds: tuple[PortfolioWalkForwardFold, ...],
) -> Decimal:
    strategy = _compound(
        tuple(
            fold.test_result.total_return for fold in folds
        )
    )
    benchmark = _compound(
        tuple(
            fold.benchmark_result.total_return for fold in folds
        )
    )
    return strategy - benchmark


def _fee_rate(
    result: PortfolioWalkForwardResult,
    *,
    benchmark: bool,
) -> Decimal:
    total = sum(
        tuple(
            (
                fold.benchmark_result.total_fees
                if benchmark
                else fold.test_result.total_fees
            )
            for fold in result.folds
        ),
        Decimal("0"),
    )
    capital = sum(
        tuple(
            (
                fold.benchmark_result.initial_cash
                if benchmark
                else fold.test_result.initial_cash
            )
            for fold in result.folds
        ),
        Decimal("0"),
    )
    return Decimal("0") if capital == 0 else total / capital


def _compound(values: tuple[Decimal, ...]) -> Decimal:
    compounded = Decimal("1")
    for value in values:
        compounded *= Decimal("1") + value
    return compounded - Decimal("1")


def _mean(values: tuple[Decimal, ...]) -> Decimal:
    return sum(values, Decimal("0")) / Decimal(len(values))


def _median(values: tuple[Decimal, ...]) -> Decimal:
    ordered = tuple(sorted(values))
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (
        ordered[middle - 1] + ordered[middle]
    ) / Decimal("2")


def _decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == 0:
        return "0"
    return format(normalized, "f")


def _canonical_hash(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
