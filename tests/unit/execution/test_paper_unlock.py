from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import SecretStr

from autoquant.data.daily_models import TradingSession
from autoquant.errors import MissingCapabilityError, PersistenceUnavailableError
from autoquant.execution.control import KillSwitchControl, KillSwitchReason
from autoquant.execution.paper_scheduler_lease_store import (
    scheduler_lease_token_hash,
)
from autoquant.execution.paper_unlock import PaperRuntimeUnlockEvidence
from autoquant.execution.paper_unlock_service import PaperRuntimeUnlockService
from autoquant.execution.quote_book import ContinuousQuoteBook
from autoquant.risk.models import MarketQuote

NOW = datetime(2026, 7, 23, 1, 30, tzinfo=UTC)
SESSION_DATE = date(2026, 7, 23)
INSTRUMENT = "600000.XSHG"
TOKEN = SecretStr("x" * 32)
ZERO_HASH = "0" * 64


def _control(*, active: bool, version: int = 3) -> KillSwitchControl:
    return KillSwitchControl(
        account_id="paper-main",
        active=active,
        version=version,
        reason=(
            KillSwitchReason.MANUAL
            if active
            else KillSwitchReason.RESET_APPROVED
        ),
        changed_at=NOW - timedelta(minutes=1),
        changed_by="test",
        last_event_hash=ZERO_HASH,
    )


def _quote_snapshot(*, received_at: datetime = NOW):
    book = ContinuousQuoteBook(source="qmt")
    book.reset(
        quotes=(
            MarketQuote(
                instrument=INSTRUMENT,
                as_of=received_at - timedelta(milliseconds=10),
                last_price=Decimal("10"),
                bid_price=Decimal("9.99"),
                ask_price=Decimal("10.01"),
                market_open=True,
            ),
        ),
        source_sequence=1,
        received_at=received_at,
        reset_id="paper-unlock-test",
    )
    return book.snapshot(
        instruments=(INSTRUMENT,),
        now=received_at,
        max_age=timedelta(seconds=1),
        require_market_open=True,
    )


class FakeControls:
    def __init__(self) -> None:
        self.active = _control(active=True)
        self.reset_paper_runtime = AsyncMock(
            return_value=_control(active=False, version=4)
        )

    @asynccontextmanager
    async def coordination_lock(
        self,
        *,
        account_id: str,
    ) -> AsyncIterator[None]:
        assert account_id == "paper-main"
        yield

    async def replay(self, *, account_id: str) -> KillSwitchControl:
        assert account_id == "paper-main"
        return self.active

    async def get(self, *, account_id: str) -> KillSwitchControl:
        assert account_id == "paper-main"
        return self.active


def _service(
    *,
    times: tuple[datetime, ...] = (NOW, NOW, NOW),
) -> tuple[PaperRuntimeUnlockService, dict[str, object]]:
    controls = FakeControls()
    executions = AsyncMock()
    executions.account_histories.return_value = ()
    executions.save_reconciliation.return_value = None
    broker = AsyncMock()
    broker.account_histories.return_value = ()
    sessions = AsyncMock()
    sessions.replay.return_value = SimpleNamespace(state_hash="a" * 64)
    sessions.observe.return_value = SimpleNamespace(state_hash="b" * 64)
    strategies = AsyncMock()
    strategies.active.return_value = SimpleNamespace(
        account_id="paper-main",
        strategy_id="validated-sma-paper",
        execution_mode="paper",
        registration_hash="c" * 64,
        instrument=INSTRUMENT,
        instruments=(INSTRUMENT,),
    )
    leases = AsyncMock()
    leases.verify_owner.return_value = SimpleNamespace(
        holder_id="paper-node-01",
        generation=7,
    )
    unlocks = AsyncMock()
    calendar = AsyncMock()
    calendar.return_value = TradingSession(
        source="tushare",
        session_date=SESSION_DATE,
        is_open=True,
        available_at=NOW - timedelta(days=1),
        response_hash="d" * 64,
    )
    quotes = AsyncMock(return_value=_quote_snapshot())
    time_values = iter(times)
    dependencies: dict[str, object] = {
        "controls": controls,
        "executions": executions,
        "broker": broker,
        "sessions": sessions,
        "strategies": strategies,
        "leases": leases,
        "unlocks": unlocks,
        "calendar": calendar,
        "quotes": quotes,
    }
    return (
        PaperRuntimeUnlockService(
            account_id="paper-main",
            strategy_id="validated-sma-paper",
            initial_cash=Decimal("1000000"),
            holder_id="paper-node-01",
            lease_token=TOKEN,
            controls=controls,  # type: ignore[arg-type]
            executions=executions,
            broker=broker,
            sessions=sessions,
            strategies=strategies,
            leases=leases,
            unlocks=unlocks,
            calendar=calendar,
            quotes=quotes,
            now=lambda: next(time_values),
        ),
        dependencies,
    )


def test_unlock_evidence_hashes_every_runtime_fence() -> None:
    evidence = PaperRuntimeUnlockEvidence(
        account_id="paper-main",
        strategy_id="validated-sma-paper",
        session_date=SESSION_DATE,
        evaluated_at=NOW,
        registration_hash="a" * 64,
        calendar_hash="b" * 64,
        session_state_hash="c" * 64,
        quote_evidence_hash="d" * 64,
        reconciliation_report_hash="e" * 64,
        lease_holder_id="paper-node-01",
        lease_token_hash=scheduler_lease_token_hash(TOKEN),
        lease_generation=3,
    )

    assert evidence.evidence_hash == PaperRuntimeUnlockEvidence(
        **{
            field: getattr(evidence, field)
            for field in (
                "account_id",
                "strategy_id",
                "session_date",
                "evaluated_at",
                "registration_hash",
                "calendar_hash",
                "session_state_hash",
                "quote_evidence_hash",
                "reconciliation_report_hash",
                "lease_holder_id",
                "lease_token_hash",
                "lease_generation",
            )
        }
    ).evidence_hash


@pytest.mark.asyncio
async def test_unlock_collects_and_persists_evidence_before_atomic_reset() -> None:
    service, dependencies = _service()

    result = await service.unlock(actor="operator")

    assert not result.control.active
    assert result.evidence.registration_hash == "c" * 64
    assert result.evidence.session_state_hash == "b" * 64
    unlocks = dependencies["unlocks"]
    assert isinstance(unlocks, AsyncMock)
    unlocks.append.assert_awaited_once_with(result.evidence)
    controls = dependencies["controls"]
    assert isinstance(controls, FakeControls)
    controls.reset_paper_runtime.assert_awaited_once()
    reset_call = controls.reset_paper_runtime.await_args.kwargs
    assert reset_call["evidence"] == result.evidence
    assert reset_call["expected_version"] == 3
    assert reset_call["lease_token"].get_secret_value() == "x" * 32


@pytest.mark.asyncio
async def test_unlock_refuses_stale_quote_before_reconciliation() -> None:
    service, dependencies = _service(
        times=(NOW, NOW + timedelta(seconds=3)),
    )

    with pytest.raises(PersistenceUnavailableError, match="quote evidence"):
        await service.unlock(actor="operator")

    executions = dependencies["executions"]
    assert isinstance(executions, AsyncMock)
    executions.save_reconciliation.assert_not_awaited()


@pytest.mark.asyncio
async def test_unlock_refuses_non_continuous_phase() -> None:
    pre_open = datetime(2026, 7, 23, 1, 0, tzinfo=UTC)
    service, dependencies = _service(times=(pre_open,))
    calendar = dependencies["calendar"]
    assert isinstance(calendar, AsyncMock)
    calendar.return_value = TradingSession(
        source="tushare",
        session_date=SESSION_DATE,
        is_open=True,
        available_at=pre_open - timedelta(days=1),
        response_hash="d" * 64,
    )

    with pytest.raises(MissingCapabilityError, match="continuous trading"):
        await service.unlock(actor="operator")


@pytest.mark.asyncio
async def test_unlock_refuses_divergent_execution_histories() -> None:
    service, dependencies = _service()
    executions = dependencies["executions"]
    assert isinstance(executions, AsyncMock)
    executions.account_histories.return_value = ("internal-only",)

    with pytest.raises(PersistenceUnavailableError, match="do not converge"):
        await service.unlock(actor="operator")


@pytest.mark.asyncio
async def test_unlock_refuses_missing_strategy_before_quote_request() -> None:
    service, dependencies = _service()
    strategies = dependencies["strategies"]
    assert isinstance(strategies, AsyncMock)
    strategies.active.return_value = None

    with pytest.raises(MissingCapabilityError, match="approved strategy"):
        await service.unlock(actor="operator")

    quotes = dependencies["quotes"]
    assert isinstance(quotes, AsyncMock)
    quotes.assert_not_awaited()
