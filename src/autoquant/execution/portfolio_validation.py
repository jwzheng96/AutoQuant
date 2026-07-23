from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, localcontext
from itertools import combinations
from uuid import UUID

from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
    _require_nonblank,
)

PORTFOLIO_OOS_POLICY_VERSION = "portfolio-oos-gates-v1"


@dataclass(frozen=True, slots=True)
class PortfolioOosFold:
    sequence: int
    test_start: date
    test_end: date
    total_return: Decimal
    max_drawdown: Decimal

    def __post_init__(self) -> None:
        if (
            not isinstance(self.sequence, int)
            or isinstance(self.sequence, bool)
            or self.sequence < 1
            or self.test_start > self.test_end
        ):
            raise ValueError("portfolio OOS fold identity is invalid")
        _finite_decimal(
            self.total_return,
            name="portfolio fold total_return",
        )
        _finite_decimal(
            self.max_drawdown,
            name="portfolio fold max_drawdown",
        )
        if self.total_return < Decimal("-1"):
            raise ValueError("portfolio fold return cannot be below -1")
        if not Decimal("0") <= self.max_drawdown <= Decimal("1"):
            raise ValueError(
                "portfolio fold drawdown must be between zero and one"
            )

    def payload(self) -> dict[str, object]:
        return {
            "max_drawdown": _decimal_text(self.max_drawdown),
            "sequence": self.sequence,
            "test_end": self.test_end.isoformat(),
            "test_start": self.test_start.isoformat(),
            "total_return": _decimal_text(self.total_return),
        }


@dataclass(frozen=True, slots=True)
class PortfolioOosComponentEvidence:
    experiment_id: UUID
    validation_result_hash: str
    instrument: str
    allocation: Decimal
    folds: tuple[PortfolioOosFold, ...]
    evidence_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.validation_result_hash,
            name="portfolio validation result hash",
        )
        _require_nonblank(
            self.instrument,
            name="portfolio validation instrument",
        )
        _finite_decimal(
            self.allocation,
            name="portfolio validation allocation",
        )
        if not Decimal("0") < self.allocation <= Decimal("1"):
            raise ValueError(
                "portfolio validation allocation must be in (0, 1]"
            )
        folds = tuple(self.folds)
        if (
            not folds
            or tuple(value.sequence for value in folds)
            != tuple(range(1, len(folds) + 1))
        ):
            raise ValueError(
                "portfolio validation folds must be contiguous"
            )
        object.__setattr__(self, "folds", folds)
        object.__setattr__(
            self,
            "evidence_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "allocation": _decimal_text(self.allocation),
            "experiment_id": str(self.experiment_id),
            "folds": [value.payload() for value in self.folds],
            "instrument": self.instrument,
            "validation_result_hash": self.validation_result_hash,
        }


@dataclass(frozen=True, slots=True)
class PortfolioOosPolicy:
    minimum_folds: int = 6
    minimum_profitable_fold_rate: Decimal = Decimal("0.5")
    maximum_drawdown: Decimal = Decimal("0.12")
    maximum_pairwise_correlation: Decimal = Decimal("0.85")
    maximum_component_contribution: Decimal = Decimal("0.65")
    version: str = PORTFOLIO_OOS_POLICY_VERSION
    policy_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.minimum_folds, int)
            or isinstance(self.minimum_folds, bool)
            or self.minimum_folds < 3
        ):
            raise ValueError("portfolio OOS minimum_folds must be at least 3")
        for name, value in (
            (
                "minimum_profitable_fold_rate",
                self.minimum_profitable_fold_rate,
            ),
            ("maximum_drawdown", self.maximum_drawdown),
            (
                "maximum_pairwise_correlation",
                self.maximum_pairwise_correlation,
            ),
            (
                "maximum_component_contribution",
                self.maximum_component_contribution,
            ),
        ):
            _finite_decimal(value, name=name)
            if not Decimal("0") <= value <= Decimal("1"):
                raise ValueError(f"{name} must be between zero and one")
        _require_nonblank(self.version, name="portfolio OOS policy version")
        object.__setattr__(
            self,
            "policy_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "maximum_component_contribution": _decimal_text(
                self.maximum_component_contribution
            ),
            "maximum_drawdown": _decimal_text(self.maximum_drawdown),
            "maximum_pairwise_correlation": _decimal_text(
                self.maximum_pairwise_correlation
            ),
            "minimum_folds": self.minimum_folds,
            "minimum_profitable_fold_rate": _decimal_text(
                self.minimum_profitable_fold_rate
            ),
            "version": self.version,
        }


@dataclass(frozen=True, slots=True)
class PortfolioOosAssessment:
    policy_hash: str
    component_evidence: tuple[tuple[str, str, str], ...]
    fold_count: int
    compounded_return: Decimal
    profitable_fold_rate: Decimal
    maximum_drawdown: Decimal
    maximum_pairwise_correlation: Decimal | None
    maximum_component_contribution: Decimal
    gate_failures: tuple[str, ...]
    version: str = "portfolio-oos-assessment-v1"
    assessment_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.policy_hash,
            name="portfolio OOS policy hash",
        )
        evidence = tuple(sorted(self.component_evidence))
        if (
            len(evidence) < 3
            or len({value[0] for value in evidence}) != len(evidence)
        ):
            raise ValueError(
                "portfolio assessment requires unique component instruments"
            )
        for instrument, result_hash, evidence_hash in evidence:
            _require_nonblank(
                instrument,
                name="portfolio assessment instrument",
            )
            _require_lowercase_sha256(
                result_hash,
                name="portfolio assessment validation hash",
            )
            _require_lowercase_sha256(
                evidence_hash,
                name="portfolio assessment evidence hash",
            )
        if self.fold_count < 1:
            raise ValueError(
                "portfolio assessment fold_count must be positive"
            )
        for name, value in (
            ("compounded_return", self.compounded_return),
            ("profitable_fold_rate", self.profitable_fold_rate),
            ("maximum_drawdown", self.maximum_drawdown),
            (
                "maximum_component_contribution",
                self.maximum_component_contribution,
            ),
        ):
            _finite_decimal(value, name=name)
        if self.maximum_pairwise_correlation is not None:
            _finite_decimal(
                self.maximum_pairwise_correlation,
                name="maximum_pairwise_correlation",
            )
            if not Decimal("-1") <= (
                self.maximum_pairwise_correlation
            ) <= Decimal("1"):
                raise ValueError(
                    "portfolio correlation must be between -1 and one"
                )
        if (
            not Decimal("0") <= self.profitable_fold_rate <= Decimal("1")
            or not Decimal("0") <= self.maximum_drawdown <= Decimal("1")
            or not Decimal("0")
            <= self.maximum_component_contribution
            <= Decimal("1")
        ):
            raise ValueError(
                "portfolio assessment rates must be between zero and one"
            )
        failures = tuple(sorted(self.gate_failures))
        if len(set(failures)) != len(failures) or any(
            not value.strip() for value in failures
        ):
            raise ValueError(
                "portfolio assessment failures must be unique and nonblank"
            )
        _require_nonblank(self.version, name="portfolio assessment version")
        object.__setattr__(self, "component_evidence", evidence)
        object.__setattr__(self, "gate_failures", failures)
        object.__setattr__(
            self,
            "assessment_hash",
            _canonical_hash(self.payload()),
        )

    @property
    def passed(self) -> bool:
        return not self.gate_failures

    @property
    def instruments(self) -> tuple[str, ...]:
        return tuple(value[0] for value in self.component_evidence)

    def payload(self) -> dict[str, object]:
        return {
            "component_evidence": [
                {
                    "evidence_hash": evidence_hash,
                    "instrument": instrument,
                    "validation_result_hash": result_hash,
                }
                for instrument, result_hash, evidence_hash in (
                    self.component_evidence
                )
            ],
            "compounded_return": _decimal_text(
                self.compounded_return
            ),
            "fold_count": self.fold_count,
            "gate_failures": list(self.gate_failures),
            "maximum_component_contribution": _decimal_text(
                self.maximum_component_contribution
            ),
            "maximum_drawdown": _decimal_text(self.maximum_drawdown),
            "maximum_pairwise_correlation": (
                None
                if self.maximum_pairwise_correlation is None
                else _decimal_text(self.maximum_pairwise_correlation)
            ),
            "policy_hash": self.policy_hash,
            "profitable_fold_rate": _decimal_text(
                self.profitable_fold_rate
            ),
            "version": self.version,
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> PortfolioOosAssessment:
        raw_evidence = payload.get("component_evidence")
        raw_failures = payload.get("gate_failures")
        if not isinstance(raw_evidence, list) or not isinstance(
            raw_failures,
            list,
        ):
            raise TypeError(
                "portfolio assessment arrays are invalid"
            )
        evidence: list[tuple[str, str, str]] = []
        for raw in raw_evidence:
            if not isinstance(raw, dict):
                raise TypeError(
                    "portfolio assessment component is invalid"
                )
            evidence.append(
                (
                    str(raw["instrument"]),
                    str(raw["validation_result_hash"]),
                    str(raw["evidence_hash"]),
                )
            )
        correlation = payload.get("maximum_pairwise_correlation")
        assessment = cls(
            policy_hash=str(payload["policy_hash"]),
            component_evidence=tuple(evidence),
            fold_count=int(str(payload["fold_count"])),
            compounded_return=Decimal(
                str(payload["compounded_return"])
            ),
            profitable_fold_rate=Decimal(
                str(payload["profitable_fold_rate"])
            ),
            maximum_drawdown=Decimal(
                str(payload["maximum_drawdown"])
            ),
            maximum_pairwise_correlation=(
                None
                if correlation is None
                else Decimal(str(correlation))
            ),
            maximum_component_contribution=Decimal(
                str(payload["maximum_component_contribution"])
            ),
            gate_failures=tuple(str(value) for value in raw_failures),
            version=str(payload["version"]),
        )
        if assessment.payload() != payload:
            raise ValueError(
                "portfolio assessment payload is not canonical"
            )
        return assessment


def assess_portfolio_oos(
    components: tuple[PortfolioOosComponentEvidence, ...],
    *,
    policy: PortfolioOosPolicy | None = None,
) -> PortfolioOosAssessment:
    active_policy = policy or PortfolioOosPolicy()
    values = tuple(sorted(components, key=lambda value: value.instrument))
    if (
        len(values) < 3
        or len({value.instrument for value in values}) != len(values)
        or sum(
            (value.allocation for value in values),
            Decimal("0"),
        )
        > Decimal("1")
    ):
        raise ValueError(
            "portfolio OOS assessment requires unique bounded components"
        )
    if any(
        fold.total_return < -component.allocation
        for component in values
        for fold in component.folds
    ):
        raise ValueError(
            "portfolio component loss exceeds its bounded allocation"
        )
    fold_count = min(len(value.folds) for value in values)
    intervals = tuple(
        (
            value.sequence,
            value.test_start,
            value.test_end,
        )
        for value in values[0].folds[:fold_count]
    )
    aligned = all(
        len(component.folds) == fold_count
        and
        tuple(
            (
                fold.sequence,
                fold.test_start,
                fold.test_end,
            )
            for fold in component.folds[:fold_count]
        )
        == intervals
        for component in values
    )
    portfolio_returns = tuple(
        sum(
            (
                component.folds[index].total_return
                for component in values
            ),
            Decimal("0"),
        )
        for index in range(fold_count)
    )
    compounded_return = _compound(portfolio_returns)
    profitable_fold_rate = Decimal(
        sum(value > 0 for value in portfolio_returns)
    ) / Decimal(fold_count)
    fold_path_drawdown = _path_drawdown(portfolio_returns)
    conservative_within_fold_drawdown = max(
        (
            sum(
                (
                    component.folds[index].max_drawdown
                    for component in values
                ),
                Decimal("0"),
            )
            for index in range(fold_count)
        ),
        default=Decimal("0"),
    )
    maximum_drawdown = max(
        fold_path_drawdown,
        conservative_within_fold_drawdown,
    )
    correlations = tuple(
        _correlation(
            tuple(fold.total_return for fold in left.folds),
            tuple(fold.total_return for fold in right.folds),
        )
        for left, right in combinations(values, 2)
    )
    defined_correlations = tuple(
        value for value in correlations if value is not None
    )
    maximum_correlation = (
        None
        if len(defined_correlations) != len(correlations)
        else max(defined_correlations)
    )
    component_returns = tuple(
        abs(
            _compound(
                tuple(fold.total_return for fold in component.folds)
            )
        )
        for component in values
    )
    contribution_total = sum(component_returns, Decimal("0"))
    maximum_contribution = (
        Decimal("1")
        if contribution_total == 0
        else max(component_returns) / contribution_total
    )
    failures: list[str] = []
    if not aligned:
        failures.append("fold_intervals_misaligned")
    if fold_count < active_policy.minimum_folds:
        failures.append("minimum_fold_count")
    if compounded_return <= 0:
        failures.append("nonpositive_portfolio_return")
    if (
        profitable_fold_rate
        < active_policy.minimum_profitable_fold_rate
    ):
        failures.append("profitable_fold_rate")
    if maximum_drawdown > active_policy.maximum_drawdown:
        failures.append("portfolio_drawdown_limit")
    if maximum_correlation is None:
        failures.append("pairwise_correlation_unavailable")
    elif (
        maximum_correlation
        > active_policy.maximum_pairwise_correlation
    ):
        failures.append("pairwise_correlation_limit")
    if (
        maximum_contribution
        > active_policy.maximum_component_contribution
    ):
        failures.append("component_contribution_concentration")
    return PortfolioOosAssessment(
        policy_hash=active_policy.policy_hash,
        component_evidence=tuple(
            (
                value.instrument,
                value.validation_result_hash,
                value.evidence_hash,
            )
            for value in values
        ),
        fold_count=fold_count,
        compounded_return=compounded_return,
        profitable_fold_rate=profitable_fold_rate,
        maximum_drawdown=maximum_drawdown,
        maximum_pairwise_correlation=maximum_correlation,
        maximum_component_contribution=maximum_contribution,
        gate_failures=tuple(failures),
    )


def _compound(values: tuple[Decimal, ...]) -> Decimal:
    factor = Decimal("1")
    for value in values:
        factor *= Decimal("1") + value
    return factor - Decimal("1")


def _path_drawdown(values: tuple[Decimal, ...]) -> Decimal:
    equity = Decimal("1")
    peak = equity
    maximum = Decimal("0")
    for value in values:
        equity *= Decimal("1") + value
        peak = max(peak, equity)
        if peak > 0:
            maximum = max(maximum, (peak - equity) / peak)
    return maximum


def _correlation(
    left: tuple[Decimal, ...],
    right: tuple[Decimal, ...],
) -> Decimal | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    with localcontext() as context:
        context.prec = 34
        count = Decimal(len(left))
        left_mean = sum(left, Decimal("0")) / count
        right_mean = sum(right, Decimal("0")) / count
        covariance = sum(
            (
                (left_value - left_mean)
                * (right_value - right_mean)
                for left_value, right_value in zip(
                    left,
                    right,
                    strict=True,
                )
            ),
            Decimal("0"),
        )
        left_variance = sum(
            (
                (value - left_mean) ** 2
                for value in left
            ),
            Decimal("0"),
        )
        right_variance = sum(
            (
                (value - right_mean) ** 2
                for value in right
            ),
            Decimal("0"),
        )
        denominator = (left_variance * right_variance).sqrt()
        if denominator == 0:
            return None
        result = covariance / denominator
        return min(Decimal("1"), max(Decimal("-1"), +result))


def _finite_decimal(value: Decimal, *, name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")
