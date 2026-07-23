from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.paper_deployment import (
    PostgresPaperDeploymentRegistry,
)


def _registry() -> tuple[
    PostgresPaperDeploymentRegistry,
    AsyncMock,
    AsyncMock,
]:
    singles = AsyncMock()
    portfolios = AsyncMock()
    return (
        PostgresPaperDeploymentRegistry(
            singles=cast(Any, singles),
            portfolios=cast(Any, portfolios),
        ),
        singles,
        portfolios,
    )


@pytest.mark.asyncio
async def test_deployment_reader_returns_the_only_active_kind() -> None:
    registry, singles, portfolios = _registry()
    portfolio = SimpleNamespace(registration_hash="a" * 64)
    singles.active.return_value = None
    portfolios.active.return_value = portfolio

    assert (
        await registry.active(
            account_id="paper-main",
            strategy_id="validated-sma-paper",
        )
        is portfolio
    )


@pytest.mark.asyncio
async def test_deployment_reader_fails_closed_on_dual_activation() -> None:
    registry, singles, portfolios = _registry()
    singles.active.return_value = SimpleNamespace(
        registration_hash="a" * 64
    )
    portfolios.active.return_value = SimpleNamespace(
        registration_hash="b" * 64
    )

    with pytest.raises(
        PersistenceUnavailableError,
        match="both active",
    ):
        await registry.active(
            account_id="paper-main",
            strategy_id="validated-sma-paper",
        )


@pytest.mark.asyncio
async def test_deployment_registry_closes_both_stores() -> None:
    registry, singles, portfolios = _registry()

    await registry.close()

    singles.close.assert_awaited_once_with()
    portfolios.close.assert_awaited_once_with()
