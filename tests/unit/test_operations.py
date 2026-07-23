from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from autoquant.config import AppSettings, RuntimeEnvironment
from autoquant.errors import MissingCapabilityError
from autoquant.operations import (
    approve_paper_sma_strategy,
    revoke_paper_strategy,
)

NOW = datetime(2026, 7, 23, 8, tzinfo=UTC)


def _settings() -> AppSettings:
    return AppSettings(
        _env_file=None,
        environment=RuntimeEnvironment.PAPER,
        postgres_dsn="postgresql+asyncpg://configured",
        clickhouse_dsn="clickhouse://configured",
    )


@pytest.mark.asyncio
async def test_approval_stops_before_research_when_kill_switch_is_inactive() -> None:
    market = MagicMock()
    market.client.close = AsyncMock()
    control = MagicMock()
    control.close = AsyncMock()
    execution_controls = MagicMock()
    execution_controls.replay = AsyncMock(
        return_value=MagicMock(active=False)
    )
    execution_controls.close = AsyncMock()
    validations = MagicMock()
    validations.detail = AsyncMock()
    validations.close = AsyncMock()
    registry = MagicMock()
    registry.close = AsyncMock()
    frozen_datetime = MagicMock()
    frozen_datetime.now.return_value = NOW
    with (
        patch("autoquant.operations.datetime", frozen_datetime),
        patch(
            "autoquant.operations.ClickHouseDailyRepository.connect",
            new=AsyncMock(return_value=market),
        ),
        patch(
            "autoquant.operations.PostgresControlRepository.connect",
            return_value=control,
        ),
        patch(
            "autoquant.operations.PostgresExecutionControlRepository.connect",
            return_value=execution_controls,
        ),
        patch(
            "autoquant.operations.PostgresValidationRepository.connect",
            return_value=validations,
        ),
        patch(
            "autoquant.operations.PostgresPaperStrategyRegistry.connect",
            return_value=registry,
        ),
    ):
        with pytest.raises(MissingCapabilityError, match="kill switch"):
            await approve_paper_sma_strategy(
                _settings(),
                experiment_id=uuid4(),
                signal_manifest_hash="a" * 64,
                reference_session_date=date(2026, 7, 23),
                approved_by="operator",
            )

    validations.detail.assert_not_awaited()
    registry.approve.assert_not_called()
    execution_controls.close.assert_awaited_once()
    market.client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_revocation_stops_when_kill_switch_is_inactive() -> None:
    controls = MagicMock()
    controls.replay = AsyncMock(return_value=MagicMock(active=False))
    controls.close = AsyncMock()
    registry = MagicMock()
    registry.revoke = AsyncMock()
    registry.close = AsyncMock()
    frozen_datetime = MagicMock()
    frozen_datetime.now.return_value = NOW
    with (
        patch("autoquant.operations.datetime", frozen_datetime),
        patch(
            "autoquant.operations.PostgresExecutionControlRepository.connect",
            return_value=controls,
        ),
        patch(
            "autoquant.operations.PostgresPaperStrategyRegistry.connect",
            return_value=registry,
        ),
    ):
        with pytest.raises(MissingCapabilityError, match="kill switch"):
            await revoke_paper_strategy(
                _settings(),
                revoked_by="operator",
                reason="scheduled_research_refresh",
            )

    registry.revoke.assert_not_awaited()
    registry.close.assert_awaited_once()
    controls.close.assert_awaited_once()
