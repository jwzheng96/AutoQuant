from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal
from uuid import uuid4

from autoquant.execution.portfolio_validation import (
    PortfolioOosAssessment,
    PortfolioOosComponentEvidence,
    PortfolioOosFold,
    assess_portfolio_oos,
)

INSTRUMENTS = (
    "000001.XSHE",
    "600000.XSHG",
    "600519.XSHG",
)
RETURNS = (
    ("0.010", "0.020", "-0.005", "0.015", "0.003", "0.012"),
    ("0.008", "-0.003", "0.018", "0.004", "0.014", "0.006"),
    ("-0.002", "0.011", "0.005", "0.017", "0.007", "0.009"),
)


def _components() -> tuple[PortfolioOosComponentEvidence, ...]:
    return tuple(
        PortfolioOosComponentEvidence(
            experiment_id=uuid4(),
            validation_result_hash=f"{index + 1:x}" * 64,
            instrument=instrument,
            allocation=Decimal("0.20"),
            folds=tuple(
                PortfolioOosFold(
                    sequence=sequence,
                    test_start=date(2025, sequence, 1),
                    test_end=date(2025, sequence, 20),
                    total_return=Decimal(value),
                    max_drawdown=Decimal("0.01"),
                )
                for sequence, value in enumerate(
                    component_returns,
                    start=1,
                )
            ),
        )
        for index, (instrument, component_returns) in enumerate(
            zip(INSTRUMENTS, RETURNS, strict=True)
        )
    )


def test_portfolio_oos_assessment_passes_diversified_aligned_folds() -> None:
    assessment = assess_portfolio_oos(_components())

    assert assessment.passed
    assert assessment.fold_count == 6
    assert assessment.compounded_return > 0
    assert assessment.maximum_drawdown <= Decimal("0.12")
    assert assessment.maximum_pairwise_correlation is not None
    assert PortfolioOosAssessment.from_payload(
        assessment.payload()
    ) == assessment


def test_portfolio_oos_assessment_rejects_identical_return_streams() -> None:
    components = _components()
    identical = tuple(
        replace(
            component,
            folds=components[0].folds,
        )
        for component in components
    )

    assessment = assess_portfolio_oos(identical)

    assert not assessment.passed
    assert "pairwise_correlation_limit" in assessment.gate_failures


def test_portfolio_oos_assessment_rejects_misaligned_test_intervals() -> None:
    components = _components()
    shifted_fold = replace(
        components[2].folds[-1],
        test_end=date(2025, 6, 21),
    )
    misaligned = (
        components[0],
        components[1],
        replace(
            components[2],
            folds=(*components[2].folds[:-1], shifted_fold),
        ),
    )

    assessment = assess_portfolio_oos(misaligned)

    assert not assessment.passed
    assert "fold_intervals_misaligned" in assessment.gate_failures


def test_portfolio_oos_assessment_rejects_unproven_correlation() -> None:
    components = tuple(
        replace(
            component,
            folds=tuple(
                replace(fold, total_return=Decimal("0.01"))
                for fold in component.folds
            ),
        )
        for component in _components()
    )

    assessment = assess_portfolio_oos(components)

    assert not assessment.passed
    assert "pairwise_correlation_unavailable" in assessment.gate_failures
