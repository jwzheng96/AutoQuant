from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from autoquant.clock import to_utc
from autoquant.data.models import _canonical_hash, _require_lowercase_sha256
from autoquant.errors import (
    PersistenceUnavailableError,
    QmtSessionConflictError,
    QmtSessionLeaseLostError,
)
from autoquant.execution.models import ZERO_HASH

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_HOLDER_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")
_MAX_TTL = timedelta(minutes=5)


class QmtSessionLeaseAction(StrEnum):
    ACQUIRE = "acquire"
    RELEASE = "release"


@dataclass(frozen=True, slots=True)
class QmtSessionLease:
    session_id: int
    holder_id: str
    token_hash: str
    acquired_at: datetime
    heartbeat_at: datetime
    expires_at: datetime
    released_at: datetime | None
    generation: int
    version: int
    event_sequence: int
    last_event_hash: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.session_id, int)
            or isinstance(self.session_id, bool)
            or not 1 <= self.session_id <= 2_147_483_647
        ):
            raise ValueError("session_id must be a positive 32-bit integer")
        if _HOLDER_ID.fullmatch(self.holder_id) is None:
            raise ValueError("holder_id must be a safe 1-64 character identifier")
        _require_lowercase_sha256(self.token_hash, name="token_hash")
        acquired_at = to_utc(self.acquired_at, name="acquired_at")
        heartbeat_at = to_utc(self.heartbeat_at, name="heartbeat_at")
        expires_at = to_utc(self.expires_at, name="expires_at")
        released_at = (
            None if self.released_at is None else to_utc(self.released_at, name="released_at")
        )
        if heartbeat_at < acquired_at:
            raise ValueError("heartbeat cannot precede acquisition")
        if released_at is None and expires_at <= heartbeat_at:
            raise ValueError("active lease expiry must follow its heartbeat")
        if released_at is not None and released_at < acquired_at:
            raise ValueError("release cannot precede acquisition")
        for name, value in (
            ("generation", self.generation),
            ("version", self.version),
            ("event_sequence", self.event_sequence),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        _require_lowercase_sha256(self.last_event_hash, name="last_event_hash")
        object.__setattr__(self, "acquired_at", acquired_at)
        object.__setattr__(self, "heartbeat_at", heartbeat_at)
        object.__setattr__(self, "expires_at", expires_at)
        object.__setattr__(self, "released_at", released_at)

    def active_at(self, now: datetime) -> bool:
        instant = to_utc(now, name="lease observation time")
        return self.released_at is None and self.expires_at > instant


def qmt_session_token_hash(token: SecretStr) -> str:
    raw = token.get_secret_value()
    if len(raw) < 32:
        raise ValueError("QMT lease token must contain at least 32 characters")
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _validate_request(
    *,
    session_id: int,
    holder_id: str,
    token: SecretStr,
    now: datetime,
    ttl: timedelta,
) -> tuple[str, datetime]:
    if (
        not isinstance(session_id, int)
        or isinstance(session_id, bool)
        or not 1 <= session_id <= 2_147_483_647
    ):
        raise ValueError("session_id must be a positive 32-bit integer")
    if _HOLDER_ID.fullmatch(holder_id) is None:
        raise ValueError("holder_id must be a safe 1-64 character identifier")
    if not timedelta(0) < ttl <= _MAX_TTL:
        raise ValueError("QMT lease ttl must be positive and no more than five minutes")
    return qmt_session_token_hash(token), to_utc(now, name="lease time")


def _event_payload(
    *,
    session_id: int,
    sequence: int,
    generation: int,
    action: QmtSessionLeaseAction,
    holder_id: str,
    token_hash: str,
    occurred_at: datetime,
    expires_at: datetime,
    previous_hash: str,
) -> dict[str, object]:
    return {
        "action": action.value,
        "expires_at": expires_at.isoformat(),
        "generation": generation,
        "holder_id": holder_id,
        "occurred_at": occurred_at.isoformat(),
        "previous_hash": previous_hash,
        "sequence": sequence,
        "session_id": session_id,
        "token_hash": token_hash,
    }


class PostgresQmtSessionLeaseRepository:
    """Durable, fenced QMT session ownership without retaining raw lease tokens."""

    def __init__(self, *, engine: AsyncEngine, schema: str = "public") -> None:
        if _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError("schema must be a safe PostgreSQL identifier")
        self._engine = engine
        self._schema = schema

    @classmethod
    def connect(cls, *, dsn: str, schema: str = "public") -> PostgresQmtSessionLeaseRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "PostgreSQL QMT session lease connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def active_session_ids(self, *, now: datetime) -> tuple[int, ...]:
        instant = to_utc(now, name="lease observation time")
        try:
            async with self._engine.connect() as connection:
                values = (
                    await connection.scalars(
                        text(
                            f"SELECT session_id FROM {self._schema}.qmt_session_leases "
                            "WHERE released_at IS NULL AND expires_at > :now "
                            "ORDER BY session_id"
                        ),
                        {"now": instant},
                    )
                ).all()
        except Exception:
            raise PersistenceUnavailableError("QMT session lease read failed") from None
        return tuple(int(value) for value in values)

    async def verify_owner(
        self,
        *,
        session_id: int,
        holder_id: str,
        token: SecretStr,
        now: datetime,
    ) -> QmtSessionLease:
        token_hash, instant = _validate_request(
            session_id=session_id,
            holder_id=holder_id,
            token=token,
            now=now,
            ttl=timedelta(seconds=1),
        )
        try:
            async with self._engine.connect() as connection:
                current = await self._select(
                    connection,
                    session_id,
                    for_update=False,
                )
        except Exception:
            raise PersistenceUnavailableError(
                "QMT session lease ownership read failed"
            ) from None
        self._require_owner(
            current,
            token_hash=token_hash,
            holder_id=holder_id,
            now=instant,
        )
        if current is None:
            raise QmtSessionLeaseLostError(
                "QMT session lease is unavailable"
            )
        return current

    async def acquire(
        self,
        *,
        session_id: int,
        holder_id: str,
        token: SecretStr,
        now: datetime,
        ttl: timedelta,
    ) -> QmtSessionLease:
        token_hash, instant = _validate_request(
            session_id=session_id,
            holder_id=holder_id,
            token=token,
            now=now,
            ttl=ttl,
        )
        try:
            async with self._engine.begin() as connection:
                await self._lock(connection, session_id)
                current = await self._select(
                    connection,
                    session_id,
                    for_update=True,
                )
                if current is not None and current.active_at(instant):
                    if current.holder_id != holder_id or current.token_hash != token_hash:
                        raise QmtSessionConflictError("QMT session identifier has an active lease")
                    return await self._renew_locked(
                        connection,
                        current=current,
                        token_hash=token_hash,
                        holder_id=holder_id,
                        now=instant,
                        ttl=ttl,
                    )
                previous_hash = ZERO_HASH if current is None else current.last_event_hash
                sequence = 1 if current is None else current.event_sequence + 1
                generation = 1 if current is None else current.generation + 1
                version = 1 if current is None else current.version + 1
                expires_at = instant + ttl
                payload = _event_payload(
                    session_id=session_id,
                    sequence=sequence,
                    generation=generation,
                    action=QmtSessionLeaseAction.ACQUIRE,
                    holder_id=holder_id,
                    token_hash=token_hash,
                    occurred_at=instant,
                    expires_at=expires_at,
                    previous_hash=previous_hash,
                )
                event_hash = _canonical_hash(payload)
                lease = QmtSessionLease(
                    session_id=session_id,
                    holder_id=holder_id,
                    token_hash=token_hash,
                    acquired_at=instant,
                    heartbeat_at=instant,
                    expires_at=expires_at,
                    released_at=None,
                    generation=generation,
                    version=version,
                    event_sequence=sequence,
                    last_event_hash=event_hash,
                )
                await self._write_state(connection, lease=lease, exists=current is not None)
                await self._insert_event(
                    connection, lease=lease, action=QmtSessionLeaseAction.ACQUIRE, payload=payload
                )
                return lease
        except (QmtSessionConflictError, ValueError):
            raise
        except Exception:
            raise PersistenceUnavailableError("QMT session lease acquisition failed") from None

    async def renew(
        self,
        *,
        session_id: int,
        holder_id: str,
        token: SecretStr,
        now: datetime,
        ttl: timedelta,
    ) -> QmtSessionLease:
        token_hash, instant = _validate_request(
            session_id=session_id,
            holder_id=holder_id,
            token=token,
            now=now,
            ttl=ttl,
        )
        try:
            async with self._engine.begin() as connection:
                await self._lock(connection, session_id)
                current = await self._select(
                    connection,
                    session_id,
                    for_update=True,
                )
                return await self._renew_locked(
                    connection,
                    current=current,
                    token_hash=token_hash,
                    holder_id=holder_id,
                    now=instant,
                    ttl=ttl,
                )
        except (QmtSessionLeaseLostError, ValueError):
            raise
        except Exception:
            raise PersistenceUnavailableError("QMT session lease renewal failed") from None

    async def release(
        self,
        *,
        session_id: int,
        holder_id: str,
        token: SecretStr,
        now: datetime,
    ) -> QmtSessionLease:
        token_hash, instant = _validate_request(
            session_id=session_id,
            holder_id=holder_id,
            token=token,
            now=now,
            ttl=timedelta(seconds=1),
        )
        try:
            async with self._engine.begin() as connection:
                await self._lock(connection, session_id)
                current = await self._select(
                    connection,
                    session_id,
                    for_update=True,
                )
                self._require_owner(
                    current,
                    token_hash=token_hash,
                    holder_id=holder_id,
                    now=instant,
                )
                if current is None:
                    raise QmtSessionLeaseLostError("QMT session lease is unavailable")
                if instant < current.heartbeat_at:
                    raise ValueError("QMT session lease time cannot move backwards")
                sequence = current.event_sequence + 1
                payload = _event_payload(
                    session_id=session_id,
                    sequence=sequence,
                    generation=current.generation,
                    action=QmtSessionLeaseAction.RELEASE,
                    holder_id=holder_id,
                    token_hash=token_hash,
                    occurred_at=instant,
                    expires_at=instant,
                    previous_hash=current.last_event_hash,
                )
                event_hash = _canonical_hash(payload)
                released = QmtSessionLease(
                    session_id=current.session_id,
                    holder_id=current.holder_id,
                    token_hash=current.token_hash,
                    acquired_at=current.acquired_at,
                    heartbeat_at=instant,
                    expires_at=instant,
                    released_at=instant,
                    generation=current.generation,
                    version=current.version + 1,
                    event_sequence=sequence,
                    last_event_hash=event_hash,
                )
                await self._write_state(connection, lease=released, exists=True)
                await self._insert_event(
                    connection,
                    lease=released,
                    action=QmtSessionLeaseAction.RELEASE,
                    payload=payload,
                )
                return released
        except (QmtSessionLeaseLostError, ValueError):
            raise
        except Exception:
            raise PersistenceUnavailableError("QMT session lease release failed") from None

    async def _renew_locked(
        self,
        connection: AsyncConnection,
        *,
        current: QmtSessionLease | None,
        token_hash: str,
        holder_id: str,
        now: datetime,
        ttl: timedelta,
    ) -> QmtSessionLease:
        self._require_owner(
            current,
            token_hash=token_hash,
            holder_id=holder_id,
            now=now,
        )
        if current is None:
            raise QmtSessionLeaseLostError("QMT session lease is unavailable")
        if now < current.heartbeat_at:
            raise ValueError("QMT session lease time cannot move backwards")
        renewed = QmtSessionLease(
            session_id=current.session_id,
            holder_id=current.holder_id,
            token_hash=current.token_hash,
            acquired_at=current.acquired_at,
            heartbeat_at=now,
            expires_at=now + ttl,
            released_at=None,
            generation=current.generation,
            version=current.version + 1,
            event_sequence=current.event_sequence,
            last_event_hash=current.last_event_hash,
        )
        await self._write_state(connection, lease=renewed, exists=True)
        return renewed

    @staticmethod
    def _require_owner(
        current: QmtSessionLease | None,
        *,
        token_hash: str,
        holder_id: str,
        now: datetime,
    ) -> None:
        if (
            current is None
            or not current.active_at(now)
            or current.released_at is not None
            or current.holder_id != holder_id
            or current.token_hash != token_hash
        ):
            raise QmtSessionLeaseLostError("QMT session lease ownership was lost")

    async def _lock(self, connection: AsyncConnection, session_id: int) -> None:
        await connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": f"autoquant:qmt-session:{session_id}"},
        )

    async def _select(
        self,
        connection: AsyncConnection,
        session_id: int,
        *,
        for_update: bool,
    ) -> QmtSessionLease | None:
        suffix = " FOR UPDATE" if for_update else ""
        row = (
            (
                await connection.execute(
                    text(
                        f"SELECT session_id, holder_id, token_hash, acquired_at, "
                        "heartbeat_at, expires_at, released_at, generation, version, "
                        f"event_sequence, last_event_hash FROM {self._schema}.qmt_session_leases "
                        f"WHERE session_id = :session_id{suffix}"
                    ),
                    {"session_id": session_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else _lease_from_row(row)

    async def _write_state(
        self,
        connection: AsyncConnection,
        *,
        lease: QmtSessionLease,
        exists: bool,
    ) -> None:
        parameters = {
            "session_id": lease.session_id,
            "holder_id": lease.holder_id,
            "token_hash": lease.token_hash,
            "acquired_at": lease.acquired_at,
            "heartbeat_at": lease.heartbeat_at,
            "expires_at": lease.expires_at,
            "released_at": lease.released_at,
            "generation": lease.generation,
            "version": lease.version,
            "event_sequence": lease.event_sequence,
            "last_event_hash": lease.last_event_hash,
        }
        if exists:
            await connection.execute(
                text(
                    f"UPDATE {self._schema}.qmt_session_leases SET "
                    "holder_id=:holder_id, token_hash=:token_hash, acquired_at=:acquired_at, "
                    "heartbeat_at=:heartbeat_at, expires_at=:expires_at, "
                    "released_at=:released_at, generation=:generation, version=:version, "
                    "event_sequence=:event_sequence, last_event_hash=:last_event_hash, "
                    "updated_at=clock_timestamp() WHERE session_id=:session_id"
                ),
                parameters,
            )
        else:
            await connection.execute(
                text(
                    f"INSERT INTO {self._schema}.qmt_session_leases "
                    "(session_id, holder_id, token_hash, acquired_at, heartbeat_at, "
                    "expires_at, released_at, generation, version, event_sequence, "
                    "last_event_hash) VALUES (:session_id, :holder_id, :token_hash, "
                    ":acquired_at, :heartbeat_at, :expires_at, :released_at, :generation, "
                    ":version, :event_sequence, :last_event_hash)"
                ),
                parameters,
            )

    async def _insert_event(
        self,
        connection: AsyncConnection,
        *,
        lease: QmtSessionLease,
        action: QmtSessionLeaseAction,
        payload: dict[str, object],
    ) -> None:
        await connection.execute(
            text(
                f"INSERT INTO {self._schema}.qmt_session_lease_events "
                "(event_hash, session_id, sequence, generation, action, holder_id, "
                "token_hash, occurred_at, expires_at, previous_hash, event_payload) "
                "VALUES (:event_hash, :session_id, :sequence, :generation, :action, "
                ":holder_id, :token_hash, :occurred_at, :expires_at, :previous_hash, "
                "CAST(:event_payload AS jsonb))"
            ),
            {
                "event_hash": lease.last_event_hash,
                "session_id": lease.session_id,
                "sequence": lease.event_sequence,
                "generation": lease.generation,
                "action": action.value,
                "holder_id": lease.holder_id,
                "token_hash": lease.token_hash,
                "occurred_at": (
                    lease.acquired_at
                    if action is QmtSessionLeaseAction.ACQUIRE
                    else lease.released_at
                ),
                "expires_at": lease.expires_at,
                "previous_hash": payload["previous_hash"],
                "event_payload": json.dumps(payload, sort_keys=True, separators=(",", ":")),
            },
        )


def _lease_from_row(row: RowMapping) -> QmtSessionLease:
    return QmtSessionLease(
        session_id=int(row["session_id"]),
        holder_id=str(row["holder_id"]),
        token_hash=str(row["token_hash"]),
        acquired_at=row["acquired_at"],
        heartbeat_at=row["heartbeat_at"],
        expires_at=row["expires_at"],
        released_at=row["released_at"],
        generation=int(row["generation"]),
        version=int(row["version"]),
        event_sequence=int(row["event_sequence"]),
        last_event_hash=str(row["last_event_hash"]),
    )
