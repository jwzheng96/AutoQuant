from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _datetime_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.qmt_preflight import QmtClockAttestation
from autoquant.execution.qmt_readonly import QmtReadOnlyBaseline
from autoquant.execution.qmt_session_store import QmtSessionLease

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_HOLDER_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")


@dataclass(frozen=True, slots=True)
class QmtReadOnlyAcceptanceEvidence:
    logical_account_id: str
    observed_at: datetime
    baseline_evidence_hash: str
    account_snapshot_hash: str
    package_manifest_hash: str
    position_count: int
    order_count: int
    trade_count: int
    callback_cursor: int
    lease_session_id: int
    lease_holder_id: str
    lease_token_hash: str
    lease_generation: int
    clock_attestation: QmtClockAttestation | None = None
    evidence_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(
            self.logical_account_id,
            name="logical_account_id",
        )
        if len(self.logical_account_id) > 128:
            raise ValueError(
                "logical_account_id cannot exceed 128 characters"
            )
        object.__setattr__(
            self,
            "observed_at",
            to_utc(self.observed_at, name="QMT acceptance observed_at"),
        )
        for hash_name, hash_value in (
            ("baseline_evidence_hash", self.baseline_evidence_hash),
            ("account_snapshot_hash", self.account_snapshot_hash),
            ("package_manifest_hash", self.package_manifest_hash),
            ("lease_token_hash", self.lease_token_hash),
        ):
            _require_lowercase_sha256(hash_value, name=hash_name)
        for count_name, count_value in (
            ("position_count", self.position_count),
            ("order_count", self.order_count),
            ("trade_count", self.trade_count),
            ("callback_cursor", self.callback_cursor),
        ):
            if (
                not isinstance(count_value, int)
                or isinstance(count_value, bool)
                or count_value < 0
            ):
                raise ValueError(f"{count_name} must be nonnegative")
        for positive_name, positive_value in (
            ("lease_session_id", self.lease_session_id),
            ("lease_generation", self.lease_generation),
        ):
            if (
                not isinstance(positive_value, int)
                or isinstance(positive_value, bool)
                or positive_value < 1
            ):
                raise ValueError(f"{positive_name} must be positive")
        if _HOLDER_ID.fullmatch(self.lease_holder_id) is None:
            raise ValueError(
                "lease_holder_id must be a safe identifier"
            )
        if self.clock_attestation is not None:
            if (
                not self.clock_attestation.trusted
                or self.clock_attestation.request_started_at
                < self.observed_at
                or self.clock_attestation.request_started_at
                > self.observed_at + timedelta(seconds=5)
            ):
                raise ValueError(
                    "QMT acceptance requires a fresh trusted clock attestation"
                )
        object.__setattr__(
            self,
            "evidence_hash",
            _canonical_hash(self.payload()),
        )

    @classmethod
    def from_baseline(
        cls,
        *,
        baseline: QmtReadOnlyBaseline,
        package_manifest_hash: str,
        lease: QmtSessionLease,
        clock_attestation: QmtClockAttestation | None = None,
    ) -> QmtReadOnlyAcceptanceEvidence:
        return cls(
            logical_account_id=baseline.logical_account_id,
            observed_at=baseline.query_completed_at,
            baseline_evidence_hash=baseline.evidence_hash,
            account_snapshot_hash=baseline.account_snapshot.snapshot_hash,
            package_manifest_hash=package_manifest_hash,
            position_count=len(baseline.positions),
            order_count=len(baseline.orders),
            trade_count=len(baseline.trades),
            callback_cursor=baseline.callback_cursor,
            lease_session_id=lease.session_id,
            lease_holder_id=lease.holder_id,
            lease_token_hash=lease.token_hash,
            lease_generation=lease.generation,
            clock_attestation=clock_attestation,
        )

    def payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "account_snapshot_hash": self.account_snapshot_hash,
            "baseline_evidence_hash": self.baseline_evidence_hash,
            "callback_cursor": self.callback_cursor,
            "lease_generation": self.lease_generation,
            "lease_holder_id": self.lease_holder_id,
            "lease_session_id": self.lease_session_id,
            "lease_token_hash": self.lease_token_hash,
            "logical_account_id": self.logical_account_id,
            "observed_at": _datetime_text(self.observed_at),
            "order_count": self.order_count,
            "package_manifest_hash": self.package_manifest_hash,
            "position_count": self.position_count,
            "trade_count": self.trade_count,
            "version": (
                "qmt-readonly-acceptance-v1"
                if self.clock_attestation is None
                else "qmt-readonly-acceptance-v2"
            ),
        }
        if self.clock_attestation is not None:
            payload["clock_attestation"] = (
                self.clock_attestation.payload()
            )
            payload["clock_attestation_hash"] = (
                self.clock_attestation.attestation_hash
            )
        return payload


class PostgresQmtReadOnlyAcceptanceRepository:
    """Persist redacted QMT acceptance evidence under active safety fences."""

    def __init__(
        self,
        *,
        engine: AsyncEngine,
        schema: str = "public",
    ) -> None:
        if _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError(
                "schema must be a safe PostgreSQL identifier"
            )
        self._engine = engine
        self._schema = schema

    @classmethod
    def connect(
        cls,
        *,
        dsn: str,
        schema: str = "public",
    ) -> PostgresQmtReadOnlyAcceptanceRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "PostgreSQL QMT acceptance connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def check_connection(self) -> None:
        try:
            async with self._engine.connect() as connection:
                table = await connection.scalar(
                    text("SELECT to_regclass(:table_name)"),
                    {
                        "table_name": (
                            f"{self._schema}."
                            "qmt_readonly_acceptance_evidence"
                        )
                    },
                )
                version = await connection.scalar(
                    text(
                        f"""
                        SELECT version
                        FROM {self._schema}.schema_versions
                        WHERE component = 'postgres'
                        """
                    )
                )
        except Exception:
            raise PersistenceUnavailableError(
                "QMT acceptance schema check failed"
            ) from None
        if table is None or not isinstance(version, int) or version < 52:
            raise PersistenceUnavailableError(
                "QMT acceptance schema v52 is unavailable"
            )

    async def append(
        self,
        evidence: QmtReadOnlyAcceptanceEvidence,
        *,
        now: datetime,
    ) -> QmtReadOnlyAcceptanceEvidence:
        if not isinstance(evidence, QmtReadOnlyAcceptanceEvidence):
            raise TypeError(
                "evidence must be QmtReadOnlyAcceptanceEvidence"
            )
        if evidence.clock_attestation is None:
            raise ValueError(
                "new QMT acceptance requires a trusted clock attestation"
            )
        instant = to_utc(now, name="QMT acceptance persistence time")
        if instant < evidence.observed_at:
            raise ValueError(
                "QMT acceptance cannot be persisted before observation"
            )
        parameters = {
            **evidence.payload(),
            "clock_attestation_hash": (
                None
                if evidence.clock_attestation is None
                else evidence.clock_attestation.attestation_hash
            ),
            "clock_attestation_payload": (
                None
                if evidence.clock_attestation is None
                else json.dumps(
                    evidence.clock_attestation.payload(),
                    sort_keys=True,
                    separators=(",", ":"),
                )
            ),
            "evidence_hash": evidence.evidence_hash,
            "observed_at": evidence.observed_at,
            "evidence_payload": json.dumps(
                evidence.payload(),
                sort_keys=True,
                separators=(",", ":"),
            ),
        }
        try:
            async with self._engine.begin() as connection:
                kill_switch_active = await connection.scalar(
                    text(
                        f"""
                        SELECT active
                        FROM {self._schema}.execution_control_state
                        WHERE account_id = :logical_account_id
                        FOR SHARE
                        """
                    ),
                    {
                        "logical_account_id": (
                            evidence.logical_account_id
                        )
                    },
                )
                lease = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT holder_id, token_hash, generation,
                                       acquired_at, expires_at, released_at
                                FROM {self._schema}.qmt_session_leases
                                WHERE session_id = :lease_session_id
                                FOR SHARE
                                """
                            ),
                            {
                                "lease_session_id": (
                                    evidence.lease_session_id
                                )
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if kill_switch_active is not True:
                    raise ValueError(
                        "QMT acceptance requires an active kill switch"
                    )
                if (
                    lease is None
                    or str(lease["holder_id"])
                    != evidence.lease_holder_id
                    or str(lease["token_hash"])
                    != evidence.lease_token_hash
                    or int(lease["generation"])
                    != evidence.lease_generation
                    or lease["released_at"] is not None
                    or to_utc(lease["acquired_at"])
                    > evidence.observed_at
                    or to_utc(lease["expires_at"]) <= instant
                ):
                    raise ValueError(
                        "QMT session lease changed before evidence persistence"
                    )
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.qmt_readonly_acceptance_evidence
                            (evidence_hash, logical_account_id, observed_at,
                             baseline_evidence_hash, account_snapshot_hash,
                             package_manifest_hash, position_count,
                             order_count, trade_count, callback_cursor,
                             lease_session_id, lease_holder_id,
                             lease_token_hash, lease_generation,
                             clock_attestation_hash,
                             clock_attestation_payload,
                             evidence_payload)
                        VALUES
                            (:evidence_hash, :logical_account_id,
                             :observed_at, :baseline_evidence_hash,
                             :account_snapshot_hash,
                             :package_manifest_hash, :position_count,
                             :order_count, :trade_count, :callback_cursor,
                             :lease_session_id, :lease_holder_id,
                             :lease_token_hash, :lease_generation,
                             :clock_attestation_hash,
                             CAST(:clock_attestation_payload AS jsonb),
                             CAST(:evidence_payload AS jsonb))
                        ON CONFLICT (evidence_hash) DO NOTHING
                        """
                    ),
                    parameters,
                )
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.qmt_readonly_acceptance_evidence
                                WHERE evidence_hash = :evidence_hash
                                """
                            ),
                            {
                                "evidence_hash": evidence.evidence_hash
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
        except (ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "QMT acceptance evidence persistence failed"
            ) from None
        stored = qmt_readonly_acceptance_from_row(row)
        if stored != evidence:
            raise PersistenceUnavailableError(
                "Stored QMT acceptance evidence does not match"
            )
        return stored

    async def latest(
        self,
        *,
        logical_account_id: str,
    ) -> QmtReadOnlyAcceptanceEvidence | None:
        _require_nonblank(
            logical_account_id,
            name="logical_account_id",
        )
        try:
            async with self._engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.qmt_readonly_acceptance_evidence
                                WHERE logical_account_id = :logical_account_id
                                ORDER BY observed_at DESC, evidence_hash DESC
                                LIMIT 1
                                """
                            ),
                            {
                                "logical_account_id": logical_account_id
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "QMT acceptance evidence read failed"
            ) from None
        return (
            None
            if row is None
            else qmt_readonly_acceptance_from_row(row)
        )


def qmt_readonly_acceptance_from_row(
    row: RowMapping,
) -> QmtReadOnlyAcceptanceEvidence:
    try:
        raw_payload = row["evidence_payload"]
        payload = (
            json.loads(raw_payload)
            if isinstance(raw_payload, str)
            else dict(raw_payload)
        )
        raw_clock_payload = row["clock_attestation_payload"]
        clock_payload = (
            None
            if raw_clock_payload is None
            else (
                json.loads(raw_clock_payload)
                if isinstance(raw_clock_payload, str)
                else dict(raw_clock_payload)
            )
        )
        clock_attestation = (
            None
            if clock_payload is None
            else QmtClockAttestation.from_payload(clock_payload)
        )
        evidence = QmtReadOnlyAcceptanceEvidence(
            logical_account_id=str(row["logical_account_id"]),
            observed_at=to_utc(row["observed_at"]),
            baseline_evidence_hash=str(
                row["baseline_evidence_hash"]
            ),
            account_snapshot_hash=str(row["account_snapshot_hash"]),
            package_manifest_hash=str(row["package_manifest_hash"]),
            position_count=int(row["position_count"]),
            order_count=int(row["order_count"]),
            trade_count=int(row["trade_count"]),
            callback_cursor=int(row["callback_cursor"]),
            lease_session_id=int(row["lease_session_id"]),
            lease_holder_id=str(row["lease_holder_id"]),
            lease_token_hash=str(row["lease_token_hash"]),
            lease_generation=int(row["lease_generation"]),
            clock_attestation=clock_attestation,
        )
        if (
            payload != evidence.payload()
            or _canonical_hash(payload) != evidence.evidence_hash
            or str(row["evidence_hash"]) != evidence.evidence_hash
            or (
                clock_attestation is None
                and row["clock_attestation_hash"] is not None
            )
            or (
                clock_attestation is not None
                and str(row["clock_attestation_hash"])
                != clock_attestation.attestation_hash
            )
        ):
            raise ValueError(
                "QMT acceptance evidence payload is inconsistent"
            )
        return evidence
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "Stored QMT acceptance evidence failed integrity verification"
        ) from None
