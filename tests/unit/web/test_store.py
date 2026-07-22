from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast
from uuid import uuid4

from sqlalchemy.engine import RowMapping

from autoquant.web.models import OperatorJobState
from autoquant.web.store import PostgresOperatorRepository


def test_operator_job_row_mapping_does_not_expose_credentials() -> None:
    job_id = uuid4()
    row = cast(
        RowMapping,
        {
            "job_id": job_id,
            "job_type": "daily_ingestion",
            "state": "completed",
            "request_payload": {
                "instruments": ["000001.XSHE"],
                "start": "2025-01-01",
                "end": "2025-01-02",
                "idempotency_key": "operator-row-mapping-0001",
            },
            "requested_by": "operator",
            "created_at": datetime(2025, 1, 1, tzinfo=UTC),
            "started_at": datetime(2025, 1, 1, tzinfo=UTC),
            "completed_at": datetime(2025, 1, 1, tzinfo=UTC),
            "result_payload": {"status": "completed", "persisted_bars": 2},
            "error_code": None,
        },
    )

    job = PostgresOperatorRepository._job_from_row(row)

    assert job.job_id == job_id
    assert job.state is OperatorJobState.COMPLETED
    assert job.request.start == date(2025, 1, 1)
    assert job.result == {"status": "completed", "persisted_bars": 2}


def test_operator_console_migration_has_persistent_queue_constraints() -> None:
    migration = Path("migrations/postgres/002_operator_console.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS operator_jobs" in migration
    assert "idempotency_key text NOT NULL UNIQUE" in migration
    assert "FOR UPDATE" not in migration
    assert "VALUES ('postgres', 2)" in migration
