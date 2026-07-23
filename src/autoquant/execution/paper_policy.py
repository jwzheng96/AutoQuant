from __future__ import annotations

from autoquant.risk.models import RiskPolicy


def default_paper_policy(
    instruments: tuple[str, ...],
) -> RiskPolicy:
    """One versioned conservative policy factory shared by approval and runtime."""

    return RiskPolicy(
        allowed_instruments=instruments,
        version="paper-pretrade-risk-v1",
    )
