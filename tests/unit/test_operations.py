from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
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
    _next_low_volatility_forward_session,
    _RecyclingDailyDatasetReader,
    _validate_campaign_dataset,
    approve_paper_sma_strategy,
    create_compliance_approval,
    revoke_paper_strategy,
    run_low_volatility_forward_window,
    run_research_data_campaign,
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


def test_forward_cycle_selects_earliest_missing_required_session() -> None:
    open_dates = tuple(date(2026, 7, day) for day in (23, 24, 27, 28))

    assert _next_low_volatility_forward_session(
        open_dates=open_dates,
        bound_dates=(
            date(2026, 7, 23),
            date(2026, 7, 27),
        ),
        minimum_sessions=3,
    ) == date(2026, 7, 24)
    assert (
        _next_low_volatility_forward_session(
            open_dates=open_dates,
            bound_dates=open_dates[:3],
            minimum_sessions=3,
        )
        is None
    )


def test_forward_cycle_rejects_ambiguous_session_order() -> None:
    with pytest.raises(ValueError, match="session inputs"):
        _next_low_volatility_forward_session(
            open_dates=(
                date(2026, 7, 24),
                date(2026, 7, 23),
            ),
            bound_dates=(),
            minimum_sessions=126,
        )


@pytest.mark.asyncio
async def test_forward_window_repeats_only_batch_progress_until_frozen() -> None:
    cycle = AsyncMock(
        side_effect=(
            {"status": "batch_progress", "item_counts": {"completed": 25}},
            {"status": "batch_progress", "item_counts": {"completed": 50}},
            {
                "status": "session_frozen",
                "binding_hash": "a" * 64,
                "completed_required_sessions": 2,
            },
        )
    )
    sleeper = AsyncMock()
    with (
        patch(
            "autoquant.operations.run_low_volatility_forward_cycle",
            new=cycle,
        ),
        patch("autoquant.operations.asyncio.sleep", new=sleeper),
    ):
        result = await run_low_volatility_forward_window(
            _settings(),
            forward_spec_hash="b" * 64,
            requested_by="forward-collector",
            max_cycles=20,
            max_items=25,
            pause_seconds=Decimal("1.25"),
            interval_seconds=Decimal("5"),
        )

    assert result["status"] == "session_frozen"
    assert result["window_cycles"] == 3
    assert result["window_exhausted"] is False
    assert result["cycle_statuses"] == [
        "batch_progress",
        "batch_progress",
        "session_frozen",
    ]
    assert cycle.await_count == 3
    assert sleeper.await_count == 2


@pytest.mark.asyncio
async def test_forward_window_stops_waiting_without_sleep_or_vendor_loop() -> None:
    cycle = AsyncMock(
        return_value={
            "status": "waiting_for_completed_session",
            "completed_required_sessions": 1,
        }
    )
    sleeper = AsyncMock()
    with (
        patch(
            "autoquant.operations.run_low_volatility_forward_cycle",
            new=cycle,
        ),
        patch("autoquant.operations.asyncio.sleep", new=sleeper),
    ):
        result = await run_low_volatility_forward_window(
            _settings(),
            forward_spec_hash="b" * 64,
            requested_by="forward-collector",
            max_cycles=20,
            max_items=25,
            pause_seconds=Decimal("1.25"),
            interval_seconds=Decimal("5"),
        )

    assert result["status"] == "waiting_for_completed_session"
    assert result["window_cycles"] == 1
    cycle.assert_awaited_once()
    sleeper.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "terminal_status",
    (
        "collector_busy",
        "waiting_for_data_availability",
        "retry_authorization_required",
    ),
)
async def test_forward_window_stops_on_visibility_or_retry_gate(
    terminal_status: str,
) -> None:
    cycle = AsyncMock(
        return_value={
            "status": terminal_status,
            "completed_required_sessions": 1,
        }
    )
    sleeper = AsyncMock()
    with (
        patch(
            "autoquant.operations.run_low_volatility_forward_cycle",
            new=cycle,
        ),
        patch("autoquant.operations.asyncio.sleep", new=sleeper),
    ):
        result = await run_low_volatility_forward_window(
            _settings(),
            forward_spec_hash="b" * 64,
            requested_by="forward-collector",
            max_cycles=20,
            max_items=25,
            pause_seconds=Decimal("1.25"),
            interval_seconds=Decimal("5"),
        )

    assert result["status"] == terminal_status
    assert result["window_cycles"] == 1
    cycle.assert_awaited_once()
    sleeper.assert_not_awaited()


@pytest.mark.asyncio
async def test_forward_window_exhaustion_is_explicitly_non_successful() -> None:
    cycle = AsyncMock(return_value={"status": "batch_progress"})
    with patch(
        "autoquant.operations.run_low_volatility_forward_cycle",
        new=cycle,
    ):
        result = await run_low_volatility_forward_window(
            _settings(),
            forward_spec_hash="b" * 64,
            requested_by="forward-collector",
            max_cycles=2,
            max_items=25,
            pause_seconds=Decimal("1.25"),
            interval_seconds=Decimal("0"),
        )

    assert result["status"] == "window_exhausted"
    assert result["last_cycle_status"] == "batch_progress"
    assert result["window_exhausted"] is True
    assert cycle.await_count == 2


@pytest.mark.asyncio
async def test_forward_window_rejects_unbounded_or_nonfinite_timing() -> None:
    with pytest.raises(ValueError, match="interval_seconds"):
        await run_low_volatility_forward_window(
            _settings(),
            forward_spec_hash="b" * 64,
            requested_by="forward-collector",
            max_cycles=20,
            max_items=25,
            pause_seconds=Decimal("1.25"),
            interval_seconds=Decimal("NaN"),
        )


@pytest.mark.asyncio
async def test_research_campaign_busy_worker_does_not_open_vendor_or_market() -> None:
    status = SimpleNamespace(
        spec=SimpleNamespace(
            campaign_hash="a" * 64,
            campaign_key="integration-worker-lock-v1",
            policy_hash="b" * 64,
            snapshot_hashes=("c" * 64,),
            instruments=("000001.XSHE",),
            start_date=date(2026, 7, 23),
            end_date=date(2026, 7, 23),
        ),
        created_at=NOW,
        status="queued",
        items=(
            SimpleNamespace(
                state="queued",
                error_code=None,
            ),
        ),
        manifest=None,
    )
    repository = MagicMock()
    repository.try_acquire_worker_lock = AsyncMock(return_value=False)
    repository.status = AsyncMock(return_value=status)
    repository.close = AsyncMock()
    control = MagicMock()
    control.close = AsyncMock()
    source = MagicMock()
    with (
        patch(
            "autoquant.operations.PostgresResearchDataCampaignRepository.connect",
            return_value=repository,
        ),
        patch(
            "autoquant.operations.PostgresControlRepository.connect",
            return_value=control,
        ),
        patch("autoquant.operations.tushare_source", new=source),
        patch(
            "autoquant.operations.ClickHouseDailyRepository.connect",
            new=AsyncMock(),
        ) as market_connect,
    ):
        result = await run_research_data_campaign(
            _settings(),
            campaign_hash="a" * 64,
            max_items=25,
            pause_seconds=Decimal("1.25"),
        )

    assert result["status"] == "queued"
    assert result["batch"]["worker_busy"] is True
    assert result["batch"]["processed_count"] == 0
    source.assert_not_called()
    market_connect.assert_not_awaited()
    repository.close.assert_awaited_once()
    control.close.assert_awaited_once()


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
async def test_compliance_approval_enforces_separation_before_write() -> None:
    registry = MagicMock()
    registry.active = AsyncMock(
        return_value=MagicMock(
            approved_by="strategy-operator",
            registration_hash="a" * 64,
        )
    )
    registry.close = AsyncMock()
    execution_controls = MagicMock()
    execution_controls.replay = AsyncMock()
    execution_controls.close = AsyncMock()
    approvals = MagicMock()
    approvals.approve = AsyncMock()
    approvals.close = AsyncMock()
    control = MagicMock()
    control.close = AsyncMock()
    with (
        patch(
            "autoquant.operations.PostgresPaperDeploymentRegistry.connect",
            return_value=registry,
        ),
        patch(
            "autoquant.operations.PostgresExecutionControlRepository.connect",
            return_value=execution_controls,
        ),
        patch(
            "autoquant.operations.PostgresComplianceApprovalRepository.connect",
            return_value=approvals,
        ),
        patch(
            "autoquant.operations.PostgresControlRepository.connect",
            return_value=control,
        ),
    ):
        with pytest.raises(ValueError, match="must differ"):
            await create_compliance_approval(
                _settings(),
                external_artifact_hash="b" * 64,
                approval_reference="GRC/AQ/2026-0001",
                approved_by="strategy-operator",
                valid_until=NOW + timedelta(days=7),
            )

    execution_controls.replay.assert_not_awaited()
    approvals.approve.assert_not_awaited()
    control.close.assert_awaited_once()
    approvals.close.assert_awaited_once()
    execution_controls.close.assert_awaited_once()
    registry.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_daily_dataset_reader_recycles_clickhouse_connections() -> None:
    market_one = MagicMock()
    market_one.client.close = AsyncMock()
    market_one.client.command = AsyncMock()
    market_two = MagicMock()
    market_two.client.close = AsyncMock()
    market_two.client.command = AsyncMock()
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
    market_one.client.command.assert_awaited_once_with("SYSTEM JEMALLOC PURGE")
    market_two.client.command.assert_awaited_once_with("SYSTEM JEMALLOC PURGE")
    market_two.client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_daily_dataset_reader_drops_failed_clickhouse_connection() -> None:
    market_one = MagicMock()
    market_one.client.close = AsyncMock()
    market_one.client.command = AsyncMock()
    market_two = MagicMock()
    market_two.client.close = AsyncMock()
    market_two.client.command = AsyncMock()
    reader_one = MagicMock()
    reader_one.query = AsyncMock(side_effect=PersistenceUnavailableError("malformed response"))
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
    market_one.client.command.assert_awaited_once_with("SYSTEM JEMALLOC PURGE")
    market_two.client.command.assert_awaited_once_with("SYSTEM JEMALLOC PURGE")
    market_two.client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_approval_stops_before_research_when_kill_switch_is_inactive() -> None:
    market = MagicMock()
    market.client.close = AsyncMock()
    control = MagicMock()
    control.close = AsyncMock()
    execution_controls = MagicMock()
    execution_controls.replay = AsyncMock(return_value=MagicMock(active=False))
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
