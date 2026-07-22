from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from autoquant.config import AppSettings
from autoquant.errors import PersistenceUnavailableError
from autoquant.web.models import DailyIngestionJobRequest, OperatorJob, OperatorJobState
from autoquant.web.service import ConsoleService

NOW = datetime(2025, 1, 3, tzinfo=UTC)


def _request() -> DailyIngestionJobRequest:
    return DailyIngestionJobRequest(
        instruments=("000001.XSHE",),
        start=date(2025, 1, 1),
        end=date(2025, 1, 2),
        idempotency_key="operator-service-test-0001",
    )

def _job(state: OperatorJobState = OperatorJobState.RUNNING) -> OperatorJob:
    return OperatorJob(
        job_id=uuid4(),
        state=state,
        request=_request(),
        requested_by="operator",
        created_at=NOW,
        started_at=NOW if state is not OperatorJobState.QUEUED else None,
    )


def _service(
    *, operator: MagicMock, control: MagicMock, runner: AsyncMock
) -> ConsoleService:
    market = MagicMock()
    market.client = MagicMock()
    return ConsoleService(
        settings=AppSettings(_env_file=None, tushare_token="configured-token"),
        operator_repository=operator,
        control_repository=control,
        market_repository=market,
        ingestion_runner=runner,
        now=lambda: NOW,
        poll_interval=0.01,
    )


@pytest.mark.asyncio
async def test_worker_completes_job_and_audits_only_safe_result_fields() -> None:
    operator = MagicMock()
    operator.complete_job = AsyncMock()
    operator.fail_job = AsyncMock()
    control = MagicMock()
    control.append_audit_event = AsyncMock(return_value="a" * 64)
    runner = AsyncMock(
        return_value={
            "status": "completed",
            "persisted_bars": 2,
            "persisted_factors": 2,
            "manifest_hash": "b" * 64,
            "quality_hash": "c" * 64,
            "secret": "must-not-be-audited",
        }
    )
    service = _service(operator=operator, control=control, runner=runner)
    job = _job()

    await service._run_job(job)

    operator.complete_job.assert_awaited_once()
    operator.fail_job.assert_not_awaited()
    completion_payload = control.append_audit_event.await_args_list[-1].args[2]
    assert completion_payload["job_id"] == str(job.job_id)
    assert "secret" not in completion_payload


@pytest.mark.asyncio
async def test_worker_fails_closed_on_ingestion_error() -> None:
    operator = MagicMock()
    operator.complete_job = AsyncMock()
    operator.fail_job = AsyncMock()
    control = MagicMock()
    control.append_audit_event = AsyncMock(return_value="a" * 64)
    runner = AsyncMock(side_effect=PersistenceUnavailableError("vendor unavailable"))
    service = _service(operator=operator, control=control, runner=runner)

    await service._run_job(_job())

    operator.complete_job.assert_not_awaited()
    assert operator.fail_job.await_args.kwargs["error_code"] == "ingestion_failed"
    audit_payload = control.append_audit_event.await_args_list[-1].args[2]
    assert audit_payload["error_code"] == "ingestion_failed"
    assert "vendor unavailable" not in str(audit_payload)


@pytest.mark.asyncio
async def test_job_creation_rejects_queue_when_audit_is_unavailable() -> None:
    queued = _job(OperatorJobState.QUEUED)
    operator = MagicMock()
    operator.create_job = AsyncMock(return_value=queued)
    operator.reject_queued_job = AsyncMock()
    control = MagicMock()
    control.append_audit_event = AsyncMock(
        side_effect=PersistenceUnavailableError("audit unavailable")
    )
    service = _service(operator=operator, control=control, runner=AsyncMock())

    with pytest.raises(PersistenceUnavailableError, match="audit is unavailable"):
        await service.create_daily_job(_request(), requested_by="operator")

    operator.reject_queued_job.assert_awaited_once_with(
        queued.job_id,
        error_code="audit_unavailable",
        now=NOW,
    )
