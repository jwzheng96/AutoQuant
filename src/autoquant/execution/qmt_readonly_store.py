from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime

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
        )

    def payload(self) -> dict[str, object]:
        return {
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
            "version": "qmt-readonly-acceptance-v1",
        }


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
        if table is None or not isinstance(version, int) or version < 17:
            raise PersistenceUnavailableError(
                "QMT acceptance schema v17 is unavailable"
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
        instant = to_utc(now, name="QMT acceptance persistence time")
        if instant < evidence.observed_at:
            raise ValueError(
                "QMT acceptance cannot be persisted before observation"
            )
        parameters = {
            **evidence.payload(),
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
                             evidence_payload)
                        VALUES
                            (:evidence_hash, :logical_account_id,
                             :observed_at, :baseline_evidence_hash,
                             :account_snapshot_hash,
                             :package_manifest_hash, :position_count,
                             :order_count, :trade_count, :callback_cursor,
                             :lease_session_id, :lease_holder_id,
                             :lease_token_hash, :lease_generation,
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
        stored = _from_row(row)
        if stored != evidence:
            raise PersistenceUnavailableError(
                "Stored QMT acceptance evidence does not match"
            )
        return stored


def _from_row(row: RowMapping) -> QmtReadOnlyAcceptanceEvidence:
    try:
        raw_payload = row["evidence_payload"]
        payload = (
            json.loads(raw_payload)
            if isinstance(raw_payload, str)
            else dict(raw_payload)
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
        )
        if (
            payload != evidence.payload()
            or _canonical_hash(payload) != evidence.evidence_hash
            or str(row["evidence_hash"]) != evidence.evidence_hash
        ):
            raise ValueError(
                "QMT acceptance evidence payload is inconsistent"
            )
        return evidence
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "Stored QMT acceptance evidence failed integrity verification"
        ) from None
