from __future__ import annotations

from typing import Protocol, TypeAlias

from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.strategy_portfolio_store import (
    PostgresPaperPortfolioRegistry,
)
from autoquant.execution.strategy_registry_store import (
    PostgresPaperStrategyRegistry,
)
from autoquant.execution.validated_sma import ValidatedSmaRegistration
from autoquant.execution.validated_sma_portfolio import (
    ValidatedSmaPortfolioRegistration,
)

PaperDeployment: TypeAlias = (
    ValidatedSmaRegistration | ValidatedSmaPortfolioRegistration
)


class PaperDeploymentReader(Protocol):
    async def active(
        self,
        *,
        account_id: str,
        strategy_id: str,
    ) -> PaperDeployment | None: ...


class PostgresPaperDeploymentRegistry:
    """Read exactly one active single or portfolio paper deployment."""

    def __init__(
        self,
        *,
        singles: PostgresPaperStrategyRegistry,
        portfolios: PostgresPaperPortfolioRegistry,
    ) -> None:
        self.singles = singles
        self.portfolios = portfolios

    @classmethod
    def connect(
        cls,
        *,
        dsn: str,
        schema: str = "public",
    ) -> PostgresPaperDeploymentRegistry:
        return cls(
            singles=PostgresPaperStrategyRegistry.connect(
                dsn=dsn,
                schema=schema,
            ),
            portfolios=PostgresPaperPortfolioRegistry.connect(
                dsn=dsn,
                schema=schema,
            ),
        )

    async def close(self) -> None:
        await self.singles.close()
        await self.portfolios.close()

    async def active(
        self,
        *,
        account_id: str,
        strategy_id: str,
    ) -> PaperDeployment | None:
        single = await self.singles.active(
            account_id=account_id,
            strategy_id=strategy_id,
        )
        portfolio = await self.portfolios.active(
            account_id=account_id,
            strategy_id=strategy_id,
        )
        if single is not None and portfolio is not None:
            raise PersistenceUnavailableError(
                "single and portfolio paper deployments are both active"
            )
        return portfolio if portfolio is not None else single
