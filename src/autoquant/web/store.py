from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.errors import PersistenceUnavailableError
from autoquant.web.models import (
    ControlSummary,
    DailyIngestionJobRequest,
    OperatorJob,
    OperatorJobState,
)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_JOB_COLUMNS = """
job_id, job_type, state, request_payload, requested_by, created_at,
started_at, completed_at, result_payload, error_code
"""
_QUALIFIED_JOB_COLUMNS = """
jobs.job_id, jobs.job_type, jobs.state, jobs.request_payload, jobs.requested_by,
jobs.created_at, jobs.started_at, jobs.completed_at, jobs.result_payload, jobs.error_code
"""


class PostgresOperatorRepository:
    """Persistent queue and read model for local operator actions."""

    def __init__(self, *, engine: AsyncEngine, schema: str = "public") -> None:
        if _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError("schema must be a safe PostgreSQL identifier")
        self._engine = engine
        self._schema = schema

    @classmethod
    def connect(cls, *, dsn: str, schema: str = "public") -> PostgresOperatorRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError("PostgreSQL operator connection failed") from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def create_job(
        self,
        request: DailyIngestionJobRequest,
        *,
        requested_by: str,
        now: datetime,
    ) -> OperatorJob:
        created_at = _aware_utc(now)
        job_id = uuid4()
        payload = request.model_dump(mode="json")
        parameters: dict[str, object] = {
            "job_id": job_id,
            "idempotency_key": request.idempotency_key,
            "job_type": "daily_ingestion",
            "state": OperatorJobState.QUEUED.value,
            "request_payload": json.dumps(payload, separators=(",", ":"), sort_keys=True),
            "requested_by": requested_by,
            "created_at": created_at,
        }
        sql = text(
            f"""
            INSERT INTO {self._schema}.operator_jobs
                (job_id, idempotency_key, job_type, state, request_payload,
                 requested_by, created_at)
            VALUES
                (:job_id, :idempotency_key, :job_type, :state,
                 CAST(:request_payload AS jsonb), :requested_by, :created_at)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING {_JOB_COLUMNS}
            """
        )
        try:
            async with self._engine.begin() as connection:
                row = (await connection.execute(sql, parameters)).mappings().one_or_none()
                if row is None:
                    row = (
                        (
                            await connection.execute(
                                text(
                                    f"SELECT {_JOB_COLUMNS} FROM {self._schema}.operator_jobs "
                                    "WHERE idempotency_key = :idempotency_key"
                                ),
                                {"idempotency_key": request.idempotency_key},
                            )
                        )
                        .mappings()
                        .one()
                    )
        except Exception:
            raise PersistenceUnavailableError("Operator job creation failed") from None
        job = self._job_from_row(row)
        if job.request != request or job.requested_by != requested_by:
            raise ValueError("idempotency key already belongs to another request")
        return job

    async def list_jobs(self, *, limit: int = 50) -> tuple[OperatorJob, ...]:
        if limit < 1 or limit > 200:
            raise ValueError("limit must be between 1 and 200")
        try:
            async with self._engine.connect() as connection:
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"SELECT {_JOB_COLUMNS} FROM {self._schema}.operator_jobs "
                                "ORDER BY created_at DESC, job_id DESC LIMIT :limit"
                            ),
                            {"limit": limit},
                        )
                    )
                    .mappings()
                    .all()
                )
        except Exception:
            raise PersistenceUnavailableError("Operator job listing failed") from None
        return tuple(self._job_from_row(row) for row in rows)

    async def claim_next_job(self, *, now: datetime) -> OperatorJob | None:
        started_at = _aware_utc(now)
        sql = text(
            f"""
            WITH next_job AS (
                SELECT job_id
                FROM {self._schema}.operator_jobs
                WHERE state = 'queued'
                ORDER BY created_at, job_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE {self._schema}.operator_jobs AS jobs
            SET state = 'running', started_at = :started_at
            FROM next_job
            WHERE jobs.job_id = next_job.job_id
            RETURNING {_QUALIFIED_JOB_COLUMNS}
            """
        )
        try:
            async with self._engine.begin() as connection:
                row = (
                    (await connection.execute(sql, {"started_at": started_at}))
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError("Operator job claim failed") from None
        return None if row is None else self._job_from_row(row)

    async def complete_job(
        self,
        job_id: UUID,
        *,
        result: Mapping[str, object],
        now: datetime,
    ) -> OperatorJob:
        return await self._finish_job(
            job_id,
            state=OperatorJobState.COMPLETED,
            result=result,
            error_code=None,
            now=now,
        )

    async def fail_job(
        self,
        job_id: UUID,
        *,
        error_code: str,
        now: datetime,
    ) -> OperatorJob:
        if not error_code or len(error_code) > 80:
            raise ValueError("error_code must be 1-80 characters")
        return await self._finish_job(
            job_id,
            state=OperatorJobState.FAILED,
            result=None,
            error_code=error_code,
            now=now,
        )

    async def reject_queued_job(
        self,
        job_id: UUID,
        *,
        error_code: str,
        now: datetime,
    ) -> OperatorJob:
        if not error_code or len(error_code) > 80:
            raise ValueError("error_code must be 1-80 characters")
        sql = text(
            f"""
            UPDATE {self._schema}.operator_jobs
            SET state = 'failed', completed_at = :completed_at,
                error_code = :error_code
            WHERE job_id = :job_id AND state = 'queued'
            RETURNING {_JOB_COLUMNS}
            """
        )
        try:
            async with self._engine.begin() as connection:
                row = (
                    (
                        await connection.execute(
                            sql,
                            {
                                "job_id": job_id,
                                "completed_at": _aware_utc(now),
                                "error_code": error_code,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError("Queued operator job rejection failed") from None
        if row is None:
            raise PersistenceUnavailableError("Operator job is not queued")
        return self._job_from_row(row)

    async def interrupt_running_jobs(self, *, now: datetime) -> int:
        try:
            async with self._engine.begin() as connection:
                result = await connection.execute(
                    text(
                        f"""
                        UPDATE {self._schema}.operator_jobs
                        SET state = 'interrupted', completed_at = :completed_at,
                            error_code = 'worker_restarted'
                        WHERE state = 'running'
                        """
                    ),
                    {"completed_at": _aware_utc(now)},
                )
        except Exception:
            raise PersistenceUnavailableError("Operator job recovery failed") from None
        return int(result.rowcount or 0)

    async def control_summary(self) -> ControlSummary:
        sql = text(
            f"""
            SELECT
              (SELECT count(*) FROM {self._schema}.dataset_manifests) AS manifests,
              (SELECT count(*) FROM {self._schema}.quality_reports) AS quality_reports,
              (SELECT count(*) FROM {self._schema}.audit_events) AS audit_events,
              (SELECT count(*) FROM {self._schema}.ingestion_checkpoints) AS checkpoints
            """
        )
        try:
            async with self._engine.connect() as connection:
                row = (await connection.execute(sql)).mappings().one()
        except Exception:
            raise PersistenceUnavailableError("Operator control summary failed") from None
        return ControlSummary(
            manifests=int(row["manifests"]),
            quality_reports=int(row["quality_reports"]),
            audit_events=int(row["audit_events"]),
            checkpoints=int(row["checkpoints"]),
        )

    async def _finish_job(
        self,
        job_id: UUID,
        *,
        state: OperatorJobState,
        result: Mapping[str, object] | None,
        error_code: str | None,
        now: datetime,
    ) -> OperatorJob:
        result_payload = None
        if result is not None:
            result_payload = json.dumps(dict(result), separators=(",", ":"), sort_keys=True)
        sql = text(
            f"""
            UPDATE {self._schema}.operator_jobs
            SET state = :state, completed_at = :completed_at,
                result_payload = CAST(:result_payload AS jsonb), error_code = :error_code
            WHERE job_id = :job_id AND state = 'running'
            RETURNING {_JOB_COLUMNS}
            """
        )
        try:
            async with self._engine.begin() as connection:
                row = (
                    (
                        await connection.execute(
                            sql,
                            {
                                "job_id": job_id,
                                "state": state.value,
                                "completed_at": _aware_utc(now),
                                "result_payload": result_payload,
                                "error_code": error_code,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError("Operator job completion failed") from None
        if row is None:
            raise PersistenceUnavailableError("Operator job is not running")
        return self._job_from_row(row)

    @staticmethod
    def _job_from_row(row: RowMapping) -> OperatorJob:
        request_payload = _json_object(row["request_payload"])
        result_value = row["result_payload"]
        return OperatorJob(
            job_id=row["job_id"],
            job_type=str(row["job_type"]),
            state=OperatorJobState(str(row["state"])),
            request=DailyIngestionJobRequest.model_validate(request_payload),
            requested_by=str(row["requested_by"]),
            created_at=row["created_at"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            result=None if result_value is None else _json_object(result_value),
            error_code=None if row["error_code"] is None else str(row["error_code"]),
        )


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _json_object(value: Any) -> dict[str, object]:
    parsed = json.loads(value) if isinstance(value, str) else value
    if not isinstance(parsed, dict):
        raise ValueError("stored JSON payload must be an object")
    return {str(key): item for key, item in parsed.items()}
