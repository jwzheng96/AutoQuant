from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from autoquant.config import AppSettings, RuntimeEnvironment
from autoquant.data.daily_ingestion import ValidatedDailyDataset
from autoquant.data.daily_models import DailyCoverageEvidence
from autoquant.errors import (
    MissingCapabilityError,
    PersistenceUnavailableError,
)
from autoquant.operations import (
    _month_intervals,
    _RecyclingDailyDatasetReader,
    _validate_campaign_dataset,
    approve_paper_sma_strategy,
    revoke_paper_strategy,
)

NOW = datetime(2026, 7, 23, 8, tzinfo=UTC)


def test_universe_backfill_months_are_bounded_and_exact() -> None:
    values = _month_intervals(
        date(2025, 12, 1),
        date(2026, 2, 1),
    )

    assert values == (
        (date(2025, 12, 1), date(2025, 12, 31)),
        (date(2026, 1, 1), date(2026, 1, 31)),
        (date(2026, 2, 1), date(2026, 2, 28)),
    )
    with pytest.raises(ValueError, match="12 months"):
        _month_intervals(
            date(2025, 1, 1),
            date(2026, 1, 1),
        )
    with pytest.raises(ValueError, match="first days"):
        _month_intervals(
            date(2026, 1, 2),
            date(2026, 2, 1),
        )


def test_validation_campaign_dataset_requires_aligned_history() -> None:
    instruments = (
        "000001.XSHE",
        "600000.XSHG",
        "600519.XSHG",
    )
    bars = tuple(
        SimpleNamespace(
            instrument=instrument,
            session_date=date(2025, 1, session),
            pre_close=Decimal("10"),
            high_price=Decimal("11"),
        )
        for instrument in instruments
        for session in range(1, 7)
    )
    factors = tuple(
        SimpleNamespace(
            instrument=value.instrument,
            session_date=value.session_date,
        )
        for value in bars
    )
    dataset = ValidatedDailyDataset(
        bars=bars,  # type: ignore[arg-type]
        factors=factors,  # type: ignore[arg-type]
        coverage=DailyCoverageEvidence((), (), (), ()),
    )
    compiler = MagicMock()
    compiler.compile.side_effect = lambda instrument, candidate_dataset: tuple(
        SimpleNamespace(
            bar=value,
            rules=SimpleNamespace(buy_minimum=100),
        )
        for value in candidate_dataset.bars
        if value.instrument == instrument
    )

    _validate_campaign_dataset(
        dataset=dataset,
        instruments=instruments,
        minimum_sessions=6,
        initial_cash=Decimal("1000000"),
        allocation=Decimal("0.20"),
        slippage_bps=Decimal("5"),
        maximum_order_notional=Decimal("100000"),
        compiler=cast(Any, compiler),
    )

    with pytest.raises(ValueError, match="affordable"):
        _validate_campaign_dataset(
            dataset=dataset,
            instruments=instruments,
            minimum_sessions=6,
            initial_cash=Decimal("1000000"),
            allocation=Decimal("0.20"),
            slippage_bps=Decimal("5"),
            maximum_order_notional=Decimal("100"),
            compiler=cast(Any, compiler),
        )

    with pytest.raises(ValueError, match="common-calendar"):
        _validate_campaign_dataset(
            dataset=ValidatedDailyDataset(
                bars=bars[:-1],  # type: ignore[arg-type]
                factors=factors[:-1],  # type: ignore[arg-type]
                coverage=dataset.coverage,
            ),
            instruments=instruments,
            minimum_sessions=6,
            initial_cash=Decimal("1000000"),
            allocation=Decimal("0.20"),
            slippage_bps=Decimal("5"),
            maximum_order_notional=Decimal("100000"),
            compiler=cast(Any, compiler),
        )


def _settings() -> AppSettings:
    return AppSettings(
        _env_file=None,
        environment=RuntimeEnvironment.PAPER,
        postgres_dsn="postgresql+asyncpg://configured",
        clickhouse_dsn="clickhouse://configured",
    )


@pytest.mark.asyncio
async def test_daily_dataset_reader_recycles_clickhouse_connections() -> None:
    market_one = MagicMock()
    market_one.client.close = AsyncMock()
    market_two = MagicMock()
    market_two.client.close = AsyncMock()
    reader_one = MagicMock()
    reader_one.query = AsyncMock(side_effect=["one", "two"])
    reader_two = MagicMock()
    reader_two.query = AsyncMock(return_value="three")
    with (
        patch(
            "autoquant.operations.ClickHouseDailyRepository.connect",
            new=AsyncMock(side_effect=[market_one, market_two]),
        ) as connect,
        patch(
            "autoquant.operations.ValidatedDailyDatasetReader",
            side_effect=[reader_one, reader_two],
        ),
    ):
        reader = _RecyclingDailyDatasetReader(
            clickhouse_dsn="clickhouse://configured",
            control_repository=MagicMock(),
            recycle_after=2,
        )

        assert await reader.query("a" * 64, NOW) == "one"
        assert await reader.query("b" * 64, NOW) == "two"
        assert await reader.query("c" * 64, NOW) == "three"
        await reader.close()

    assert connect.await_count == 2
    market_one.client.close.assert_awaited_once()
    market_two.client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_daily_dataset_reader_drops_failed_clickhouse_connection() -> None:
    market_one = MagicMock()
    market_one.client.close = AsyncMock()
    market_two = MagicMock()
    market_two.client.close = AsyncMock()
    reader_one = MagicMock()
    reader_one.query = AsyncMock(
        side_effect=PersistenceUnavailableError("malformed response")
    )
    reader_two = MagicMock()
    reader_two.query = AsyncMock(return_value="recovered")
    with (
        patch(
            "autoquant.operations.ClickHouseDailyRepository.connect",
            new=AsyncMock(side_effect=[market_one, market_two]),
        ) as connect,
        patch(
            "autoquant.operations.ValidatedDailyDatasetReader",
            side_effect=[reader_one, reader_two],
        ),
    ):
        reader = _RecyclingDailyDatasetReader(
            clickhouse_dsn="clickhouse://configured",
            control_repository=MagicMock(),
        )

        with pytest.raises(
            PersistenceUnavailableError,
            match="malformed response",
        ):
            await reader.query("a" * 64, NOW)
        assert await reader.query("a" * 64, NOW) == "recovered"
        await reader.close()

    assert connect.await_count == 2
    market_one.client.close.assert_awaited_once()
    market_two.client.close.assert_awaited_once()


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
    registry.active = AsyncMock()
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
            "autoquant.operations.PostgresPaperDeploymentRegistry.connect",
            return_value=registry,
        ),
    ):
        with pytest.raises(MissingCapabilityError, match="kill switch"):
            await revoke_paper_strategy(
                _settings(),
                revoked_by="operator",
                reason="scheduled_research_refresh",
            )

    registry.active.assert_not_awaited()
    registry.close.assert_awaited_once()
    controls.close.assert_awaited_once()
