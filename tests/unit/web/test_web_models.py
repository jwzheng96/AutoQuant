from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from autoquant.web.models import (
    DailyIngestionJobRequest,
    LowVolatilityForwardProgressView,
    OperatorJob,
    OperatorJobState,
    PortfolioWalkForwardJobRequest,
)

MANIFEST_HASH = "a" * 64


@pytest.mark.parametrize(
    "campaign_status",
    ("partially_completed", "awaiting_finalization"),
)
def test_forward_progress_accepts_nonterminal_campaign_states(
    campaign_status: str,
) -> None:
    progress = LowVolatilityForwardProgressView(
        spec_hash=MANIFEST_HASH,
        forward_start_date=date(2026, 7, 23),
        safe_cutoff_date=date(2026, 7, 24),
        minimum_forward_sessions=126,
        minimum_paper_sessions=60,
        observed_open_sessions=2,
        completed_sessions=0,
        completed_required_sessions=0,
        remaining_required_sessions=126,
        missing_session_dates=(date(2026, 7, 23), date(2026, 7, 24)),
        calendar_conflict_dates=(),
        collection_campaign_hash="b" * 64,
        collection_campaign_status=campaign_status,
        collection_queued_items=275 if campaign_status == "partially_completed" else 0,
        collection_completed_items=25 if campaign_status == "partially_completed" else 300,
        status="backfill_required",
        sessions=(),
    )

    assert progress.collection_campaign_status == campaign_status
    assert progress.live_trading_locked is True


def test_daily_ingestion_request_normalizes_and_limits_scope() -> None:
    request = DailyIngestionJobRequest(
        instruments=("000001.xshe", "600000.XSHG"),
        start=date(2025, 1, 1),
        end=date(2025, 12, 31),
        idempotency_key="operator-2025-backfill-0001",
    )

    assert request.instruments == ("000001.XSHE", "600000.XSHG")


@pytest.mark.parametrize(
    "payload",
    [
        {
            "instruments": ("bad",),
            "start": date(2025, 1, 1),
            "end": date(2025, 1, 2),
            "idempotency_key": "operator-invalid-0001",
        },
        {
            "instruments": ("000001.XSHE", "000001.XSHE"),
            "start": date(2025, 1, 1),
            "end": date(2025, 1, 2),
            "idempotency_key": "operator-duplicate-0001",
        },
        {
            "instruments": ("000001.XSHE",),
            "start": date(2025, 1, 1),
            "end": date(2026, 1, 2),
            "idempotency_key": "operator-too-wide-0001",
        },
        {
            "instruments": ("000001.XSHE",),
            "start": date(2025, 2, 1),
            "end": date(2025, 1, 1),
            "idempotency_key": "operator-backwards-0001",
        },
    ],
)
def test_daily_ingestion_request_rejects_unsafe_scope(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        DailyIngestionJobRequest.model_validate(payload)


def test_operator_job_requires_aware_timestamps() -> None:
    request = DailyIngestionJobRequest(
        instruments=("000001.XSHE",),
        start=date(2025, 1, 1),
        end=date(2025, 1, 2),
        idempotency_key="operator-aware-time-0001",
    )

    job = OperatorJob(
        job_id=uuid4(),
        state=OperatorJobState.QUEUED,
        request=request,
        requested_by="operator",
        created_at=datetime(2025, 1, 1, tzinfo=UTC),
    )

    assert job.created_at.tzinfo is UTC
    with pytest.raises(ValidationError, match="timezone-aware"):
        OperatorJob(
            job_id=uuid4(),
            state=OperatorJobState.QUEUED,
            request=request,
            requested_by="operator",
            created_at=datetime(2025, 1, 1),
        )


def test_portfolio_validation_request_has_execution_headroom() -> None:
    request = PortfolioWalkForwardJobRequest(
        manifest_hash=MANIFEST_HASH.upper(),
        idempotency_key="portfolio-validation-0001",
    )

    assert request.manifest_hash == MANIFEST_HASH
    assert request.gross_allocation == Decimal("0.29")
    assert (
        request.initial_cash
        * request.gross_allocation
        / request.candidates[0].selection_count
        * (
            Decimal("1")
            + request.slippage_bps / Decimal("10000")
        )
        < request.maximum_order_notional
    )


def test_portfolio_validation_request_counts_slippage_in_order_cap() -> None:
    with pytest.raises(
        ValidationError,
        match="allocation exceeds risk limits",
    ):
        PortfolioWalkForwardJobRequest(
            manifest_hash=MANIFEST_HASH,
            gross_allocation=Decimal("0.30"),
            idempotency_key="portfolio-validation-0002",
        )
