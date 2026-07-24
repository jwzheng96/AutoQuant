from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.backtest.low_volatility_forward import (
    LowVolatilityForwardSessionBinding,
)
from autoquant.data.models import _require_lowercase_sha256
from autoquant.errors import PersistenceUnavailableError

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


@dataclass(frozen=True, slots=True)
class LowVolatilityForwardSessionRecord:
    binding: LowVolatilityForwardSessionBinding
    requested_by: str
    completed_at: datetime
    live_trading_locked: bool = True

    def __post_init__(self) -> None:
        if (
            not self.requested_by.strip()
            or self.requested_by != self.requested_by.strip()
            or len(self.requested_by) > 128
            or self.completed_at.tzinfo is None
            or self.completed_at.utcoffset() is None
            or not self.live_trading_locked
        ):
            raise ValueError("forward session record is inconsistent")
        object.__setattr__(
            self,
            "completed_at",
            self.completed_at.astimezone(UTC),
        )


class PostgresLowVolatilityForwardSessionRepository:
    def __init__(
        self,
        *,
        engine: AsyncEngine,
        schema: str = "public",
    ) -> None:
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
    ) -> PostgresLowVolatilityForwardSessionRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(
                dsn,
                pool_pre_ping=True,
            )
        except Exception:
            raise PersistenceUnavailableError("forward session connection failed") from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def freeze(
        self,
        binding: LowVolatilityForwardSessionBinding,
        *,
        requested_by: str,
        completed_at: datetime,
    ) -> LowVolatilityForwardSessionRecord:
        requested = LowVolatilityForwardSessionRecord(
            binding=binding,
            requested_by=requested_by,
            completed_at=completed_at,
        )
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:identity))"),
                    {
                        "identity": (
                            "low-volatility-forward-session:"
                            f"{binding.forward_spec_hash}:"
                            f"{binding.session_date.isoformat()}"
                        )
                    },
                )
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO
                            {self._schema}.low_volatility_forward_sessions
                            (binding_hash, forward_spec_hash,
                             session_date, snapshot_hash,
                             snapshot_reference_date,
                             dataset_manifest_hash, policy_hash,
                             calendar_content_hash,
                             instrument_count, binding_version,
                             requested_by, completed_at,
                             live_trading_locked, payload)
                        VALUES
                            (:binding_hash, :forward_spec_hash,
                             :session_date, :snapshot_hash,
                             :snapshot_reference_date,
                             :dataset_manifest_hash, :policy_hash,
                             :calendar_content_hash,
                             :instrument_count, :binding_version,
                             :requested_by, :completed_at, true,
                             CAST(:payload AS jsonb))
                        ON CONFLICT (forward_spec_hash, session_date)
                        DO NOTHING
                        """
                    ),
                    {
                        "binding_hash": binding.binding_hash,
                        "binding_version": binding.version,
                        "calendar_content_hash": (binding.calendar_content_hash),
                        "completed_at": requested.completed_at,
                        "dataset_manifest_hash": (binding.dataset_manifest_hash),
                        "forward_spec_hash": (binding.forward_spec_hash),
                        "instrument_count": len(binding.instruments),
                        "payload": _json(binding.payload()),
                        "policy_hash": binding.policy_hash,
                        "requested_by": requested.requested_by,
                        "session_date": binding.session_date,
                        "snapshot_hash": binding.snapshot_hash,
                        "snapshot_reference_date": (binding.snapshot_reference_date),
                    },
                )
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.low_volatility_forward_sessions
                                WHERE forward_spec_hash =
                                        :forward_spec_hash
                                  AND session_date = :session_date
                                """
                            ),
                            {
                                "forward_spec_hash": (binding.forward_spec_hash),
                                "session_date": (binding.session_date),
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
            stored = _record(row)
            if stored.binding != binding:
                raise ValueError("a different forward session is frozen")
            return stored
        except ValueError:
            raise
        except Exception:
            raise PersistenceUnavailableError("forward session freeze failed") from None

    async def read(
        self,
        binding_hash: str,
    ) -> LowVolatilityForwardSessionRecord:
        _require_lowercase_sha256(
            binding_hash,
            name="forward session binding hash",
        )
        try:
            async with self._engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.low_volatility_forward_sessions
                                WHERE binding_hash = :binding_hash
                                """
                            ),
                            {"binding_hash": binding_hash},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
            if row is None:
                raise LookupError("forward session binding does not exist")
            return _record(row)
        except (LookupError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError("forward session lookup failed") from None

    async def read_for_session(
        self,
        *,
        forward_spec_hash: str,
        session_date: date,
    ) -> LowVolatilityForwardSessionRecord | None:
        _require_lowercase_sha256(
            forward_spec_hash,
            name="forward session spec hash",
        )
        try:
            async with self._engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.low_volatility_forward_sessions
                                WHERE forward_spec_hash =
                                        :forward_spec_hash
                                  AND session_date = :session_date
                                """
                            ),
                            {
                                "forward_spec_hash": (forward_spec_hash),
                                "session_date": session_date,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
            return None if row is None else _record(row)
        except PersistenceUnavailableError:
            raise
        except Exception:
            raise PersistenceUnavailableError("forward session lookup failed") from None

    async def list_for_spec(
        self,
        *,
        forward_spec_hash: str,
        limit: int = 1000,
    ) -> tuple[LowVolatilityForwardSessionRecord, ...]:
        _require_lowercase_sha256(
            forward_spec_hash,
            name="forward session spec hash",
        )
        if limit < 1 or limit > 1000:
            raise ValueError("forward session limit must be between 1 and 1000")
        try:
            async with self._engine.connect() as connection:
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.low_volatility_forward_sessions
                                WHERE forward_spec_hash =
                                        :forward_spec_hash
                                ORDER BY session_date ASC
                                LIMIT :limit
                                """
                            ),
                            {
                                "forward_spec_hash": (forward_spec_hash),
                                "limit": limit,
                            },
                        )
                    )
                    .mappings()
                    .all()
                )
            return tuple(_record(row) for row in rows)
        except PersistenceUnavailableError:
            raise
        except Exception:
            raise PersistenceUnavailableError("forward session listing failed") from None


def _record(
    row: RowMapping,
) -> LowVolatilityForwardSessionRecord:
    try:
        raw = row["payload"]
        payload = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(payload, dict):
            raise TypeError("forward session payload is not an object")
        binding = LowVolatilityForwardSessionBinding.from_payload(
            {str(key): value for key, value in payload.items()}
        )
        if (
            binding.binding_hash != str(row["binding_hash"])
            or binding.forward_spec_hash != str(row["forward_spec_hash"])
            or binding.session_date != row["session_date"]
            or binding.snapshot_hash != str(row["snapshot_hash"])
            or binding.snapshot_reference_date != row["snapshot_reference_date"]
            or binding.dataset_manifest_hash != str(row["dataset_manifest_hash"])
            or binding.policy_hash != str(row["policy_hash"])
            or binding.calendar_content_hash != str(row["calendar_content_hash"])
            or len(binding.instruments) != int(row["instrument_count"])
            or binding.version != str(row["binding_version"])
            or row["live_trading_locked"] is not True
        ):
            raise ValueError("stored forward session metadata mismatch")
        return LowVolatilityForwardSessionRecord(
            binding=binding,
            requested_by=str(row["requested_by"]),
            completed_at=row["completed_at"],
        )
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError("stored forward session failed integrity") from None


def _json(payload: object) -> str:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
