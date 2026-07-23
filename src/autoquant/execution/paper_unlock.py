from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.clock import to_shanghai, to_utc
from autoquant.data.models import (
    _canonical_hash,
    _datetime_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.errors import PersistenceUnavailableError

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_HOLDER_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")


@dataclass(frozen=True, slots=True)
class PaperRuntimeUnlockEvidence:
    account_id: str
    strategy_id: str
    session_date: date
    evaluated_at: datetime
    registration_hash: str
    calendar_hash: str
    session_state_hash: str
    quote_evidence_hash: str
    reconciliation_report_hash: str
    lease_holder_id: str
    lease_token_hash: str
    lease_generation: int
    evidence_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for name, value in (
            ("account_id", self.account_id),
            ("strategy_id", self.strategy_id),
        ):
            _require_nonblank(value, name=name)
            if len(value) > 128:
                raise ValueError(f"{name} cannot exceed 128 characters")
        if _HOLDER_ID.fullmatch(self.lease_holder_id) is None:
            raise ValueError("lease_holder_id must be a safe identifier")
        for name, value in (
            ("registration_hash", self.registration_hash),
            ("calendar_hash", self.calendar_hash),
            ("session_state_hash", self.session_state_hash),
            ("quote_evidence_hash", self.quote_evidence_hash),
            ("reconciliation_report_hash", self.reconciliation_report_hash),
            ("lease_token_hash", self.lease_token_hash),
        ):
            _require_lowercase_sha256(value, name=name)
        if (
            not isinstance(self.lease_generation, int)
            or isinstance(self.lease_generation, bool)
            or self.lease_generation < 1
        ):
            raise ValueError("lease_generation must be positive")
        evaluated_at = to_utc(
            self.evaluated_at,
            name="paper runtime unlock time",
        )
        if to_shanghai(evaluated_at).date() != self.session_date:
            raise ValueError("paper runtime unlock evidence belongs to another session")
        object.__setattr__(self, "evaluated_at", evaluated_at)
        object.__setattr__(
            self,
            "evidence_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "account_id": self.account_id,
            "calendar_hash": self.calendar_hash,
            "evaluated_at": _datetime_text(self.evaluated_at),
            "lease_generation": self.lease_generation,
            "lease_holder_id": self.lease_holder_id,
            "lease_token_hash": self.lease_token_hash,
            "quote_evidence_hash": self.quote_evidence_hash,
            "reconciliation_report_hash": self.reconciliation_report_hash,
            "registration_hash": self.registration_hash,
            "session_date": self.session_date.isoformat(),
            "session_state_hash": self.session_state_hash,
            "strategy_id": self.strategy_id,
            "version": "paper-runtime-unlock-v1",
        }


class PostgresPaperRuntimeUnlockRepository:
    """Append-only evidence used by the atomic paper-runtime reset fence."""

    def __init__(self, *, engine: AsyncEngine, schema: str = "public") -> None:
        if _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError("schema must be a safe PostgreSQL identifier")
        self._engine = engine
        self._schema = schema

    @classmethod
    def connect(
        cls,
        *,
        dsn: str,
        schema: str = "public",
    ) -> PostgresPaperRuntimeUnlockRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "PostgreSQL paper unlock connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def check_connection(self) -> None:
        try:
            async with self._engine.connect() as connection:
                table = await connection.scalar(
                    text("SELECT to_regclass(:table_name)"),
                    {"table_name": f"{self._schema}.paper_runtime_unlock_evidence"},
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
                "Paper runtime unlock schema check failed"
            ) from None
        if table is None or not isinstance(version, int) or version < 16:
            raise PersistenceUnavailableError(
                "Paper runtime unlock schema v16 is unavailable"
            )

    async def append(
        self,
        evidence: PaperRuntimeUnlockEvidence,
    ) -> PaperRuntimeUnlockEvidence:
        if not isinstance(evidence, PaperRuntimeUnlockEvidence):
            raise TypeError("evidence must be PaperRuntimeUnlockEvidence")
        parameters = {
            **evidence.payload(),
            "evidence_hash": evidence.evidence_hash,
            "session_date": evidence.session_date,
            "evaluated_at": evidence.evaluated_at,
            "evidence_payload": json.dumps(
                evidence.payload(),
                sort_keys=True,
                separators=(",", ":"),
            ),
        }
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.paper_runtime_unlock_evidence
                            (evidence_hash, account_id, strategy_id, session_date,
                             evaluated_at, registration_hash, calendar_hash,
                             session_state_hash, quote_evidence_hash,
                             reconciliation_report_hash, lease_holder_id,
                             lease_token_hash, lease_generation, evidence_payload)
                        VALUES
                            (:evidence_hash, :account_id, :strategy_id, :session_date,
                             :evaluated_at, :registration_hash, :calendar_hash,
                             :session_state_hash, :quote_evidence_hash,
                             :reconciliation_report_hash, :lease_holder_id,
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
                                FROM {self._schema}.paper_runtime_unlock_evidence
                                WHERE evidence_hash = :evidence_hash
                                """
                            ),
                            {"evidence_hash": evidence.evidence_hash},
                        )
                    )
                    .mappings()
                    .one()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "Paper runtime unlock evidence persistence failed"
            ) from None
        stored = _from_row(row)
        if stored != evidence:
            raise PersistenceUnavailableError(
                "Stored paper runtime unlock evidence does not match"
            )
        return stored

    async def get(self, *, evidence_hash: str) -> PaperRuntimeUnlockEvidence:
        _require_lowercase_sha256(evidence_hash, name="evidence_hash")
        try:
            async with self._engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.paper_runtime_unlock_evidence
                                WHERE evidence_hash = :evidence_hash
                                """
                            ),
                            {"evidence_hash": evidence_hash},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "Paper runtime unlock evidence read failed"
            ) from None
        if row is None:
            raise LookupError("paper runtime unlock evidence not found")
        return _from_row(row)


def _from_row(row: RowMapping) -> PaperRuntimeUnlockEvidence:
    try:
        raw_payload = row["evidence_payload"]
        payload = (
            json.loads(raw_payload)
            if isinstance(raw_payload, str)
            else dict(raw_payload)
        )
        evidence = PaperRuntimeUnlockEvidence(
            account_id=str(row["account_id"]),
            strategy_id=str(row["strategy_id"]),
            session_date=row["session_date"],
            evaluated_at=to_utc(row["evaluated_at"]),
            registration_hash=str(row["registration_hash"]),
            calendar_hash=str(row["calendar_hash"]),
            session_state_hash=str(row["session_state_hash"]),
            quote_evidence_hash=str(row["quote_evidence_hash"]),
            reconciliation_report_hash=str(
                row["reconciliation_report_hash"]
            ),
            lease_holder_id=str(row["lease_holder_id"]),
            lease_token_hash=str(row["lease_token_hash"]),
            lease_generation=int(row["lease_generation"]),
        )
        if (
            evidence.evidence_hash != str(row["evidence_hash"])
            or evidence.payload() != payload
        ):
            raise ValueError("paper unlock evidence hash mismatch")
        return evidence
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "Stored paper runtime unlock evidence is malformed"
        ) from None
