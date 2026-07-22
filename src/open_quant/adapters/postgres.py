from __future__ import annotations

import hashlib
import json
import re
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import datetime
from typing import TypeAlias

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    create_async_engine,
)

from open_quant.clock import to_utc
from open_quant.data.models import DatasetManifest, SourceEvidence
from open_quant.data.quality import QualityIssue, QualityReport, QualitySeverity
from open_quant.errors import PersistenceUnavailableError

JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_ZERO_HASH = "0" * 64
_AUTH_WORDS = ("auth", "authorization", "token", "password", "credential", "header")


def _require_nonblank(value: str, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} cannot be empty")
    return value


def _require_hash(value: str, *, name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 hash")
    return value


def _json_value(value: object) -> JsonValue:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise ValueError("payload must contain only JSON-safe values")
        return value
    if isinstance(value, Mapping):
        result: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("payload must contain only JSON-safe values")
            result[key] = _json_value(item)
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_value(item) for item in value]
    raise TypeError("payload must contain only JSON-safe values")


def _json_object(value: object) -> dict[str, JsonValue]:
    normalized = _json_value(value)
    if not isinstance(normalized, dict):
        raise TypeError("payload must be a JSON-safe object")
    return normalized


def _canonical_bytes(value: object) -> bytes:
    try:
        normalized = _json_value(value)
        return json.dumps(
            normalized,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValueError("payload must contain only JSON-safe values") from None


def audit_event_hash(
    *,
    previous_hash: str,
    event_type: str,
    occurred_at: datetime,
    payload: object,
) -> str:
    previous = _require_hash(previous_hash, name="previous_hash")
    event = _require_nonblank(event_type, name="event_type")
    occurred = to_utc(occurred_at, name="occurred_at")
    canonical = {
        "event_type": event,
        "occurred_at": occurred.isoformat(timespec="microseconds"),
        "payload": _json_object(payload),
        "previous_hash": previous,
    }
    return hashlib.sha256(_canonical_bytes(canonical)).hexdigest()


def source_evidence_parameters(evidence: SourceEvidence) -> dict[str, object]:
    if not isinstance(evidence, SourceEvidence):
        raise TypeError("evidence must be SourceEvidence")
    method_lower = evidence.method.casefold()
    body_prefix = evidence.response_body[:4096].lower()
    if any(word in method_lower for word in _AUTH_WORDS) or any(
        marker in body_prefix
        for marker in (b"authorization:", b"password:", b"token:", b"cookie:")
    ):
        raise ValueError("authentication material cannot be persisted as source evidence")
    return {
        "evidence_hash": evidence.response_hash,
        "source": evidence.source,
        "method": evidence.method,
        "requested_at": evidence.requested_at,
        "response_body": evidence.response_body,
    }


def quality_report_payload(report: QualityReport) -> dict[str, JsonValue]:
    if not isinstance(report, QualityReport):
        raise TypeError("report must be QualityReport")
    return {
        "as_of": None if report.as_of is None else report.as_of.isoformat(timespec="microseconds"),
        "end": report.end.isoformat(timespec="microseconds"),
        "issues": [
            {
                "code": issue.code,
                "event_time": issue.event_time.isoformat(timespec="microseconds"),
                "instrument": issue.instrument,
                "message": issue.message,
                "severity": issue.severity.value,
            }
            for issue in report.issues
        ],
        "passed": report.passed,
        "production_complete": report.production_complete,
        "requested_instruments": list(report.requested_instruments),
        "start": report.start.isoformat(timespec="microseconds"),
    }


def manifest_payload(manifest: DatasetManifest) -> dict[str, JsonValue]:
    if not isinstance(manifest, DatasetManifest):
        raise TypeError("manifest must be DatasetManifest")
    return {
        "as_of": manifest.as_of.isoformat(timespec="microseconds"),
        "end_time": manifest.end_time.isoformat(timespec="microseconds"),
        "instruments": list(manifest.instruments),
        "production_complete": manifest.production_complete,
        "quality_report_hash": manifest.quality_report_hash,
        "record_hashes": list(manifest.record_hashes),
        "row_count": manifest.row_count,
        "source": manifest.source,
        "start_time": manifest.start_time.isoformat(timespec="microseconds"),
    }


class PostgresControlTransaction:
    def __init__(self, connection: AsyncConnection) -> None:
        self._connection = connection

    async def save_source_evidence(self, evidence: SourceEvidence) -> None:
        parameters = source_evidence_parameters(evidence)
        await self._execute(
            """
            INSERT INTO source_evidence
                (evidence_hash, source, method, requested_at, response_body)
            VALUES
                (:evidence_hash, :source, :method, :requested_at, :response_body)
            ON CONFLICT (evidence_hash) DO NOTHING
            """,
            parameters,
        )
        row = await self._one(
            "SELECT source, method, requested_at, response_body FROM source_evidence "
            "WHERE evidence_hash = :evidence_hash",
            {"evidence_hash": evidence.response_hash},
        )
        stored = self._source_evidence_from_row(evidence.response_hash, row)
        if stored != evidence:
            raise ValueError("source evidence hash conflicts with stored content")

    async def save_quality_report(self, report: QualityReport) -> None:
        payload = quality_report_payload(report)
        expected_hash = hashlib.sha256(_canonical_bytes(payload)).hexdigest()
        if expected_hash != report.report_hash:
            raise ValueError("quality report hash does not match payload")
        parameters: dict[str, object] = {
            "report_hash": report.report_hash,
            "passed": report.passed,
            "production_complete": report.production_complete,
            "payload": json.dumps(payload, separators=(",", ":"), sort_keys=True),
        }
        await self._execute(
            """
            INSERT INTO quality_reports
                (report_hash, passed, production_complete, payload)
            VALUES
                (:report_hash, :passed, :production_complete, CAST(:payload AS jsonb))
            ON CONFLICT (report_hash) DO NOTHING
            """,
            parameters,
        )
        row = await self._one(
            "SELECT passed, production_complete, payload FROM quality_reports "
            "WHERE report_hash = :report_hash",
            {"report_hash": report.report_hash},
        )
        if (
            row["passed"] != report.passed
            or row["production_complete"] != report.production_complete
            or _json_value(row["payload"]) != payload
        ):
            raise ValueError("quality report hash conflicts with stored content")

    async def save_manifest(self, manifest: DatasetManifest) -> None:
        quality = await self._one(
            "SELECT passed, production_complete FROM quality_reports "
            "WHERE report_hash = :report_hash",
            {"report_hash": manifest.quality_report_hash},
        )
        if manifest.production_complete and not (
            quality["passed"] is True and quality["production_complete"] is True
        ):
            raise ValueError("production manifest requires passing complete quality")
        payload = manifest_payload(manifest)
        expected_hash = hashlib.sha256(_canonical_bytes(payload)).hexdigest()
        if expected_hash != manifest.manifest_hash:
            raise ValueError("manifest hash does not match payload")
        parameters: dict[str, object] = {
            "manifest_hash": manifest.manifest_hash,
            "source": manifest.source,
            "start_time": manifest.start_time,
            "end_time": manifest.end_time,
            "as_of": manifest.as_of,
            "quality_report_hash": manifest.quality_report_hash,
            "row_count": manifest.row_count,
            "production_complete": manifest.production_complete,
            "payload": json.dumps(payload, separators=(",", ":"), sort_keys=True),
        }
        await self._execute(
            """
            INSERT INTO dataset_manifests
                (manifest_hash, source, start_time, end_time, as_of,
                 quality_report_hash, row_count, production_complete, payload)
            VALUES
                (:manifest_hash, :source, :start_time, :end_time, :as_of,
                 :quality_report_hash, :row_count, :production_complete,
                 CAST(:payload AS jsonb))
            ON CONFLICT (manifest_hash) DO NOTHING
            """,
            parameters,
        )
        row = await self._one(
            "SELECT payload FROM dataset_manifests WHERE manifest_hash = :manifest_hash",
            {"manifest_hash": manifest.manifest_hash},
        )
        if _json_value(row["payload"]) != payload:
            raise ValueError("manifest hash conflicts with stored content")

    async def advance_checkpoint(
        self,
        source: str,
        stream: str,
        instrument: str,
        event_time: datetime,
        content_hash: str,
    ) -> None:
        parameters: dict[str, object] = {
            "source": _require_nonblank(source, name="source"),
            "stream": _require_nonblank(stream, name="stream"),
            "instrument": _require_nonblank(instrument, name="instrument"),
            "event_time": to_utc(event_time, name="event_time"),
            "content_hash": _require_hash(content_hash, name="content_hash"),
        }
        existing = await self._optional_one(
            "SELECT event_time, content_hash FROM ingestion_checkpoints "
            "WHERE source = :source AND stream = :stream AND instrument = :instrument "
            "FOR UPDATE",
            parameters,
        )
        if existing is not None:
            stored_time = to_utc(existing["event_time"], name="stored event_time")
            requested_time = parameters["event_time"]
            if not isinstance(requested_time, datetime):
                raise TypeError("event_time must be a datetime")
            if requested_time < stored_time:
                raise ValueError("checkpoint cannot move backward")
            if requested_time == stored_time and existing["content_hash"] != content_hash:
                raise ValueError("checkpoint same timestamp has different content hash")
        await self._execute(
            """
            INSERT INTO ingestion_checkpoints
                (source, stream, instrument, event_time, content_hash)
            VALUES (:source, :stream, :instrument, :event_time, :content_hash)
            ON CONFLICT (source, stream, instrument) DO UPDATE SET
                event_time = EXCLUDED.event_time,
                content_hash = EXCLUDED.content_hash,
                updated_at = clock_timestamp()
            """,
            parameters,
        )

    async def append_audit_event(
        self,
        event_type: str,
        occurred_at: datetime,
        payload: object,
    ) -> str:
        event = _require_nonblank(event_type, name="event_type")
        occurred = to_utc(occurred_at, name="occurred_at")
        normalized_payload = _json_object(payload)
        await self._execute(
            "SELECT pg_advisory_xact_lock(hashtext('open_quant.audit_events'))",
            {},
        )
        last = await self._optional_one(
            "SELECT event_hash FROM audit_events ORDER BY sequence DESC LIMIT 1",
            {},
        )
        previous_hash = _ZERO_HASH if last is None else str(last["event_hash"])
        event_hash = audit_event_hash(
            previous_hash=previous_hash,
            event_type=event,
            occurred_at=occurred,
            payload=normalized_payload,
        )
        await self._execute(
            """
            INSERT INTO audit_events
                (event_type, occurred_at, payload, previous_hash, event_hash)
            VALUES
                (:event_type, :occurred_at, CAST(:payload AS jsonb),
                 :previous_hash, :event_hash)
            """,
            {
                "event_type": event,
                "occurred_at": occurred,
                "payload": json.dumps(normalized_payload, separators=(",", ":"), sort_keys=True),
                "previous_hash": previous_hash,
                "event_hash": event_hash,
            },
        )
        return event_hash

    async def read_source_evidence(self, evidence_hash: str) -> SourceEvidence:
        normalized_hash = _require_hash(evidence_hash, name="evidence_hash")
        row = await self._one(
            "SELECT source, method, requested_at, response_body FROM source_evidence "
            "WHERE evidence_hash = :evidence_hash",
            {"evidence_hash": normalized_hash},
        )
        return self._source_evidence_from_row(normalized_hash, row)

    async def read_manifest(self, manifest_hash: str) -> DatasetManifest:
        normalized_hash = _require_hash(manifest_hash, name="manifest_hash")
        row = await self._one(
            "SELECT payload FROM dataset_manifests WHERE manifest_hash = :manifest_hash",
            {"manifest_hash": normalized_hash},
        )
        payload = _json_object(row["payload"])
        try:
            manifest = DatasetManifest(
                source=str(payload["source"]),
                instruments=tuple(str(item) for item in payload["instruments"]),  # type: ignore[union-attr]
                start_time=datetime.fromisoformat(str(payload["start_time"])),
                end_time=datetime.fromisoformat(str(payload["end_time"])),
                as_of=datetime.fromisoformat(str(payload["as_of"])),
                record_hashes=tuple(str(item) for item in payload["record_hashes"]),  # type: ignore[union-attr]
                quality_report_hash=str(payload["quality_report_hash"]),
                production_complete=bool(payload["production_complete"]),
                row_count=int(str(payload["row_count"])),
            )
        except (KeyError, TypeError, ValueError):
            raise PersistenceUnavailableError("PostgreSQL stored malformed manifest") from None
        if manifest.manifest_hash != normalized_hash:
            raise PersistenceUnavailableError("PostgreSQL manifest hash verification failed")
        return manifest

    async def read_quality_report(self, report_hash: str) -> QualityReport:
        normalized_hash = _require_hash(report_hash, name="report_hash")
        row = await self._one(
            "SELECT passed, production_complete, payload FROM quality_reports "
            "WHERE report_hash = :report_hash",
            {"report_hash": normalized_hash},
        )
        payload = _json_object(row["payload"])
        try:
            raw_issues = payload["issues"]
            if not isinstance(raw_issues, list):
                raise TypeError("issues must be a list")
            issues = tuple(
                QualityIssue(
                    severity=QualitySeverity(str(item["severity"])),
                    code=str(item["code"]),
                    instrument=str(item["instrument"]),
                    event_time=datetime.fromisoformat(str(item["event_time"])),
                    message=str(item["message"]),
                )
                for item in raw_issues
                if isinstance(item, dict)
            )
            requested = payload["requested_instruments"]
            if not isinstance(requested, list) or len(issues) != len(raw_issues):
                raise TypeError("malformed quality report sequences")
            raw_as_of = payload["as_of"]
            report = QualityReport(
                requested_instruments=tuple(str(item) for item in requested),
                start=datetime.fromisoformat(str(payload["start"])),
                end=datetime.fromisoformat(str(payload["end"])),
                as_of=(
                    None
                    if raw_as_of is None
                    else datetime.fromisoformat(str(raw_as_of))
                ),
                issues=issues,
                production_complete=bool(payload["production_complete"]),
            )
        except (KeyError, TypeError, ValueError):
            raise PersistenceUnavailableError(
                "PostgreSQL stored malformed quality report"
            ) from None
        if (
            report.report_hash != normalized_hash
            or report.passed != row["passed"]
            or report.production_complete != row["production_complete"]
        ):
            raise PersistenceUnavailableError(
                "PostgreSQL quality report hash verification failed"
            )
        return report

    async def _execute(self, sql: str, parameters: Mapping[str, object]) -> None:
        try:
            await self._connection.execute(text(sql), dict(parameters))
        except Exception:
            raise PersistenceUnavailableError("PostgreSQL write failed") from None

    async def _one(self, sql: str, parameters: Mapping[str, object]) -> RowMapping:
        row = await self._optional_one(sql, parameters)
        if row is None:
            raise PersistenceUnavailableError("PostgreSQL required record is unavailable")
        return row

    async def _optional_one(
        self, sql: str, parameters: Mapping[str, object]
    ) -> RowMapping | None:
        try:
            result = await self._connection.execute(text(sql), dict(parameters))
            return result.mappings().one_or_none()
        except PersistenceUnavailableError:
            raise
        except Exception:
            raise PersistenceUnavailableError("PostgreSQL query failed") from None

    @staticmethod
    def _source_evidence_from_row(evidence_hash: str, row: RowMapping) -> SourceEvidence:
        try:
            return SourceEvidence(
                source=row["source"],
                method=row["method"],
                requested_at=row["requested_at"],
                response_body=bytes(row["response_body"]),
                response_hash=evidence_hash,
            )
        except (KeyError, TypeError, ValueError):
            raise PersistenceUnavailableError(
                "PostgreSQL stored malformed source evidence"
            ) from None


class PostgresControlRepository:
    def __init__(self, *, engine: AsyncEngine, schema: str = "public") -> None:
        if not isinstance(schema, str) or _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError("schema must be a safe PostgreSQL identifier")
        self._engine = engine
        self._schema = schema

    @classmethod
    def connect(
        cls, *, dsn: str, schema: str = "public"
    ) -> PostgresControlRepository:
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("dsn cannot be empty")
        if not isinstance(schema, str) or _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError("schema must be a safe PostgreSQL identifier")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError("PostgreSQL connection setup failed") from None
        return cls(engine=engine, schema=schema)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[PostgresControlTransaction]:
        try:
            async with self._engine.begin() as connection:
                await connection.exec_driver_sql(f'SET LOCAL search_path TO "{self._schema}"')
                yield PostgresControlTransaction(connection)
        except (TypeError, ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError("PostgreSQL transaction failed") from None

    async def initialize(self, migration: str) -> None:
        _require_nonblank(migration, name="migration")
        try:
            async with self._engine.begin() as connection:
                await connection.exec_driver_sql(
                    f'CREATE SCHEMA IF NOT EXISTS "{self._schema}"'
                )
                await connection.exec_driver_sql(f'SET LOCAL search_path TO "{self._schema}"')
                await connection.exec_driver_sql(migration)
        except Exception:
            raise PersistenceUnavailableError("PostgreSQL migration failed") from None

    async def drop_test_schema(self) -> None:
        if not self._schema.startswith("oq_test_"):
            raise ValueError("only isolated test schemas may be dropped")
        try:
            async with self._engine.begin() as connection:
                await connection.exec_driver_sql(f'DROP SCHEMA "{self._schema}" CASCADE')
        except Exception:
            raise PersistenceUnavailableError("PostgreSQL test cleanup failed") from None

    async def close(self) -> None:
        try:
            await self._engine.dispose()
        except Exception:
            raise PersistenceUnavailableError("PostgreSQL close failed") from None

    async def check_connection(self) -> None:
        required = (
            "schema_versions",
            "ingestion_checkpoints",
            "source_evidence",
            "quality_reports",
            "dataset_manifests",
            "audit_events",
        )
        try:
            async with self._engine.connect() as connection:
                await connection.exec_driver_sql(f'SET search_path TO "{self._schema}"')
                result = await connection.execute(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = :schema AND table_name = ANY(:tables)"
                    ),
                    {"schema": self._schema, "tables": list(required)},
                )
                found = {str(row[0]) for row in result}
                version_result = await connection.execute(
                    text(
                        "SELECT version FROM schema_versions "
                        "WHERE component = 'postgres'"
                    )
                )
                version = version_result.scalar_one_or_none()
        except Exception:
            raise PersistenceUnavailableError("PostgreSQL connection check failed") from None
        if found != set(required):
            raise PersistenceUnavailableError("PostgreSQL phase-1 schema is unavailable")
        if version != 1:
            raise PersistenceUnavailableError(
                "PostgreSQL phase-1 schema version is unavailable"
            )

    async def save_source_evidence(self, evidence: SourceEvidence) -> None:
        async with self.transaction() as transaction:
            await transaction.save_source_evidence(evidence)

    async def save_quality_report(self, report: QualityReport) -> None:
        async with self.transaction() as transaction:
            await transaction.save_quality_report(report)

    async def save_manifest(self, manifest: DatasetManifest) -> None:
        async with self.transaction() as transaction:
            await transaction.save_manifest(manifest)

    async def advance_checkpoint(
        self,
        source: str,
        stream: str,
        instrument: str,
        event_time: datetime,
        content_hash: str,
    ) -> None:
        async with self.transaction() as transaction:
            await transaction.advance_checkpoint(
                source, stream, instrument, event_time, content_hash
            )

    async def append_audit_event(
        self, event_type: str, occurred_at: datetime, payload: object
    ) -> str:
        async with self.transaction() as transaction:
            return await transaction.append_audit_event(event_type, occurred_at, payload)

    async def read_source_evidence(self, evidence_hash: str) -> SourceEvidence:
        async with self.transaction() as transaction:
            return await transaction.read_source_evidence(evidence_hash)

    async def read_manifest(self, manifest_hash: str) -> DatasetManifest:
        async with self.transaction() as transaction:
            return await transaction.read_manifest(manifest_hash)

    async def read_quality_report(self, report_hash: str) -> QualityReport:
        async with self.transaction() as transaction:
            return await transaction.read_quality_report(report_hash)
