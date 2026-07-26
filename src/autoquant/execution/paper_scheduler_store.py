from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from autoquant.clock import to_utc
from autoquant.data.models import _canonical_hash, _require_lowercase_sha256
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.models import ZERO_HASH
from autoquant.execution.paper_scheduler import (
    PaperSchedulerCycle,
    scheduler_cycle_payload,
)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


@dataclass(frozen=True, slots=True)
class PaperSchedulerEvent:
    account_id: str
    sequence: int
    previous_hash: str
    cycle_hash: str
    evaluated_at: datetime
    event_hash: str

    def __post_init__(self) -> None:
        if not self.account_id.strip():
            raise ValueError("account_id cannot be empty")
        if (
            not isinstance(self.sequence, int)
            or isinstance(self.sequence, bool)
            or self.sequence < 1
        ):
            raise ValueError("scheduler event sequence must be positive")
        _require_lowercase_sha256(self.previous_hash, name="previous_hash")
        _require_lowercase_sha256(self.cycle_hash, name="cycle_hash")
        _require_lowercase_sha256(self.event_hash, name="event_hash")
        object.__setattr__(
            self,
            "evaluated_at",
            to_utc(self.evaluated_at, name="scheduler event evaluated_at"),
        )


@dataclass(frozen=True, slots=True)
class PaperSchedulerRecovery:
    account_id: str
    event_count: int
    latest_evaluated_at: datetime | None
    latest_cycle_hash: str | None
    recovery_verified: bool


class PaperSchedulerHealthState(StrEnum):
    HEALTHY = "healthy"
    STARTING = "starting"
    STOPPED = "stopped"
    STALE = "stale"
    FAILED = "failed"
    IDENTITY_MISMATCH = "identity_mismatch"


@dataclass(frozen=True, slots=True)
class PaperSchedulerRuntimeHealth:
    account_id: str
    strategy_id: str
    state: PaperSchedulerHealthState
    checked_at: datetime
    maximum_silence: timedelta
    lease_active: bool
    lease_holder_id: str | None
    latest_event_persisted_at: datetime | None
    latest_event_age: timedelta | None
    latest_status: str | None
    latest_phase: str | None
    latest_error_code: str | None

    def __post_init__(self) -> None:
        for value, name in (
            (self.account_id, "scheduler health account_id"),
            (self.strategy_id, "scheduler health strategy_id"),
        ):
            if not value or value != value.strip():
                raise ValueError(f"{name} is invalid")
        if not isinstance(self.state, PaperSchedulerHealthState):
            raise TypeError("scheduler health state is invalid")
        checked_at = to_utc(self.checked_at, name="scheduler health checked_at")
        if self.maximum_silence <= timedelta(0):
            raise ValueError("scheduler maximum silence must be positive")
        if type(self.lease_active) is not bool:
            raise TypeError("scheduler lease_active must be bool")
        if self.lease_active and self.lease_holder_id is None:
            raise ValueError("active scheduler lease requires a holder")
        event_at = (
            None
            if self.latest_event_persisted_at is None
            else to_utc(
                self.latest_event_persisted_at,
                name="scheduler event persisted_at",
            )
        )
        if (event_at is None) != (self.latest_event_age is None):
            raise ValueError("scheduler event freshness evidence is incomplete")
        if self.latest_event_age is not None and self.latest_event_age < timedelta(0):
            raise ValueError("scheduler event age cannot be negative")
        if (self.latest_status is None) != (self.latest_phase is None):
            raise ValueError("scheduler latest cycle identity is incomplete")
        if self.state is PaperSchedulerHealthState.HEALTHY and (
            not self.lease_active
            or event_at is None
            or self.latest_event_age is None
            or self.latest_event_age > self.maximum_silence
            or self.latest_status == "failed"
            or self.latest_error_code is not None
        ):
            raise ValueError("healthy scheduler state requires fresh successful evidence")
        object.__setattr__(self, "checked_at", checked_at)
        object.__setattr__(self, "latest_event_persisted_at", event_at)

    @property
    def healthy(self) -> bool:
        return self.state is PaperSchedulerHealthState.HEALTHY

    @property
    def event_fresh(self) -> bool:
        return self.latest_event_age is not None and self.latest_event_age <= self.maximum_silence


class PostgresPaperSchedulerRepository:
    """Append-only, hash-chained scheduler-cycle evidence."""

    def __init__(self, *, engine: AsyncEngine, schema: str = "public") -> None:
        if _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError("schema must be a safe PostgreSQL identifier")
        self._engine = engine
        self._schema = schema

    @classmethod
    def connect(cls, *, dsn: str, schema: str = "public") -> PostgresPaperSchedulerRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "PostgreSQL paper scheduler connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def append(self, cycle: PaperSchedulerCycle) -> PaperSchedulerEvent:
        if not isinstance(cycle, PaperSchedulerCycle):
            raise TypeError("cycle must be PaperSchedulerCycle")
        payload = scheduler_cycle_payload(cycle)
        if _canonical_hash(payload) != cycle.cycle_hash:
            raise ValueError("scheduler cycle hash does not match its payload")
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                    {"key": f"autoquant:paper-scheduler:{cycle.account_id}"},
                )
                duplicate = await self._select_by_cycle_hash(
                    connection,
                    account_id=cycle.account_id,
                    cycle_hash=cycle.cycle_hash,
                )
                if duplicate is not None:
                    return duplicate
                state = await self._select_state(connection, cycle.account_id)
                if state is None:
                    sequence = 1
                    previous_hash = ZERO_HASH
                else:
                    if cycle.evaluated_at < state["last_evaluated_at"]:
                        raise ValueError("scheduler cycle time cannot move backwards")
                    sequence = int(state["last_sequence"]) + 1
                    previous_hash = str(state["last_event_hash"])
                event_hash = _canonical_hash(
                    {
                        "cycle_hash": cycle.cycle_hash,
                        "previous_hash": previous_hash,
                        "sequence": sequence,
                    }
                )
                event = PaperSchedulerEvent(
                    account_id=cycle.account_id,
                    sequence=sequence,
                    previous_hash=previous_hash,
                    cycle_hash=cycle.cycle_hash,
                    evaluated_at=cycle.evaluated_at,
                    event_hash=event_hash,
                )
                await self._insert_event(
                    connection,
                    cycle=cycle,
                    event=event,
                    payload=payload,
                )
                await self._write_state(
                    connection,
                    event=event,
                    exists=state is not None,
                )
                return event
        except ValueError:
            raise
        except Exception:
            raise PersistenceUnavailableError("Paper scheduler cycle persistence failed") from None

    async def replay(self, *, account_id: str) -> PaperSchedulerRecovery:
        if not account_id.strip():
            raise ValueError("account_id cannot be empty")
        try:
            async with self._engine.connect() as connection:
                state = await self._select_state(connection, account_id)
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"SELECT event_hash, account_id, sequence, "
                                "previous_hash, cycle_hash, "
                                "evaluated_at, strategy_id, session_date, phase, status, "
                                "control_state_hash, calendar_hash, mark_evidence_hash, "
                                f"quote_evidence_hash, error_code, cycle_payload FROM "
                                f"{self._schema}.paper_scheduler_events "
                                "WHERE account_id=:account_id ORDER BY sequence"
                            ),
                            {"account_id": account_id},
                        )
                    )
                    .mappings()
                    .all()
                )
        except Exception:
            raise PersistenceUnavailableError("Paper scheduler recovery read failed") from None
        if state is None:
            if rows:
                raise PersistenceUnavailableError(
                    "Paper scheduler events exist without materialized state"
                )
            return PaperSchedulerRecovery(
                account_id=account_id,
                event_count=0,
                latest_evaluated_at=None,
                latest_cycle_hash=None,
                recovery_verified=True,
            )
        previous_hash = ZERO_HASH
        latest_at: datetime | None = None
        latest_cycle_hash: str | None = None
        for expected_sequence, row in enumerate(rows, start=1):
            payload = dict(row["cycle_payload"])
            cycle_hash = _canonical_hash(payload)
            event_hash = _canonical_hash(
                {
                    "cycle_hash": cycle_hash,
                    "previous_hash": previous_hash,
                    "sequence": expected_sequence,
                }
            )
            if (
                int(row["sequence"]) != expected_sequence
                or str(row["previous_hash"]) != previous_hash
                or str(row["cycle_hash"]) != cycle_hash
                or str(row["event_hash"]) != event_hash
                or not _columns_match_payload(row, payload)
            ):
                raise PersistenceUnavailableError(
                    "Paper scheduler event failed integrity verification"
                )
            previous_hash = event_hash
            latest_cycle_hash = cycle_hash
            latest_at = to_utc(row["evaluated_at"], name="scheduler evaluated_at")
        if (
            len(rows) != int(state["last_sequence"])
            or previous_hash != str(state["last_event_hash"])
            or latest_cycle_hash != str(state["last_cycle_hash"])
            or latest_at != to_utc(state["last_evaluated_at"], name="scheduler state evaluated_at")
        ):
            raise PersistenceUnavailableError("Paper scheduler state does not match event replay")
        return PaperSchedulerRecovery(
            account_id=account_id,
            event_count=len(rows),
            latest_evaluated_at=latest_at,
            latest_cycle_hash=latest_cycle_hash,
            recovery_verified=True,
        )

    async def runtime_health(
        self,
        *,
        account_id: str,
        strategy_id: str,
        maximum_silence: timedelta,
    ) -> PaperSchedulerRuntimeHealth:
        if not account_id or account_id != account_id.strip():
            raise ValueError("scheduler health account_id is invalid")
        if not strategy_id or strategy_id != strategy_id.strip():
            raise ValueError("scheduler health strategy_id is invalid")
        if maximum_silence <= timedelta(0) or maximum_silence > timedelta(minutes=10):
            raise ValueError("scheduler maximum silence must be positive and at most ten minutes")
        try:
            async with self._engine.connect() as connection:
                await connection.exec_driver_sql(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                checked_at = await connection.scalar(text("SELECT clock_timestamp()"))
                event = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT strategy_id, status, phase, error_code,
                                       created_at
                                FROM {self._schema}.paper_scheduler_events
                                WHERE account_id = :account_id
                                ORDER BY sequence DESC
                                LIMIT 1
                                """
                            ),
                            {"account_id": account_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                lease = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT strategy_id, holder_id, released_at,
                                       expires_at
                                FROM {self._schema}.paper_scheduler_leases
                                WHERE account_id = :account_id
                                """
                            ),
                            {"account_id": account_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                await connection.rollback()
        except Exception:
            raise PersistenceUnavailableError(
                "Paper scheduler runtime health read failed"
            ) from None
        if not isinstance(checked_at, datetime):
            raise PersistenceUnavailableError("Paper scheduler database clock is unavailable")
        database_now = to_utc(checked_at, name="scheduler database time")
        lease_active = bool(
            lease is not None
            and lease["released_at"] is None
            and lease["expires_at"] > database_now
        )
        lease_holder_id = str(lease["holder_id"]) if lease_active and lease is not None else None
        event_at = (
            None
            if event is None
            else to_utc(
                event["created_at"],
                name="scheduler event persisted_at",
            )
        )
        event_age = None if event_at is None else database_now - event_at
        event_strategy = None if event is None else str(event["strategy_id"])
        lease_strategy = None if lease is None else str(lease["strategy_id"])
        state = _scheduler_health_state(
            strategy_id=strategy_id,
            lease_active=lease_active,
            lease_strategy_id=lease_strategy,
            event_strategy_id=event_strategy,
            event_age=event_age,
            maximum_silence=maximum_silence,
            latest_status=(None if event is None else str(event["status"])),
            latest_error_code=(
                None if event is None or event["error_code"] is None else str(event["error_code"])
            ),
        )
        return PaperSchedulerRuntimeHealth(
            account_id=account_id,
            strategy_id=strategy_id,
            state=state,
            checked_at=database_now,
            maximum_silence=maximum_silence,
            lease_active=lease_active,
            lease_holder_id=lease_holder_id,
            latest_event_persisted_at=event_at,
            latest_event_age=event_age,
            latest_status=None if event is None else str(event["status"]),
            latest_phase=None if event is None else str(event["phase"]),
            latest_error_code=(
                None if event is None or event["error_code"] is None else str(event["error_code"])
            ),
        )

    async def _select_state(
        self, connection: AsyncConnection, account_id: str
    ) -> RowMapping | None:
        return (
            (
                await connection.execute(
                    text(
                        f"SELECT account_id, last_sequence, last_event_hash, "
                        f"last_cycle_hash, last_evaluated_at FROM "
                        f"{self._schema}.paper_scheduler_state "
                        "WHERE account_id=:account_id FOR UPDATE"
                    ),
                    {"account_id": account_id},
                )
            )
            .mappings()
            .one_or_none()
        )

    async def _select_by_cycle_hash(
        self,
        connection: AsyncConnection,
        *,
        account_id: str,
        cycle_hash: str,
    ) -> PaperSchedulerEvent | None:
        row = (
            (
                await connection.execute(
                    text(
                        f"SELECT account_id, sequence, previous_hash, cycle_hash, "
                        f"evaluated_at, event_hash FROM "
                        f"{self._schema}.paper_scheduler_events "
                        "WHERE account_id=:account_id AND cycle_hash=:cycle_hash"
                    ),
                    {"account_id": account_id, "cycle_hash": cycle_hash},
                )
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else _event_from_row(row)

    async def _insert_event(
        self,
        connection: AsyncConnection,
        *,
        cycle: PaperSchedulerCycle,
        event: PaperSchedulerEvent,
        payload: dict[str, object],
    ) -> None:
        await connection.execute(
            text(
                f"INSERT INTO {self._schema}.paper_scheduler_events "
                "(event_hash, account_id, sequence, previous_hash, cycle_hash, "
                "strategy_id, session_date, evaluated_at, phase, status, "
                "control_state_hash, calendar_hash, mark_evidence_hash, "
                "quote_evidence_hash, error_code, cycle_payload) VALUES "
                "(:event_hash, :account_id, :sequence, :previous_hash, :cycle_hash, "
                ":strategy_id, :session_date, :evaluated_at, :phase, :status, "
                ":control_state_hash, :calendar_hash, :mark_evidence_hash, "
                ":quote_evidence_hash, :error_code, CAST(:cycle_payload AS jsonb))"
            ),
            {
                "event_hash": event.event_hash,
                "account_id": cycle.account_id,
                "sequence": event.sequence,
                "previous_hash": event.previous_hash,
                "cycle_hash": cycle.cycle_hash,
                "strategy_id": cycle.strategy_id,
                "session_date": cycle.session_date,
                "evaluated_at": cycle.evaluated_at,
                "phase": cycle.phase.value,
                "status": cycle.status.value,
                "control_state_hash": cycle.control.state_hash,
                "calendar_hash": cycle.calendar_hash,
                "mark_evidence_hash": cycle.mark_evidence_hash,
                "quote_evidence_hash": cycle.quote_evidence_hash,
                "error_code": cycle.error_code,
                "cycle_payload": json.dumps(payload, sort_keys=True, separators=(",", ":")),
            },
        )

    async def _write_state(
        self,
        connection: AsyncConnection,
        *,
        event: PaperSchedulerEvent,
        exists: bool,
    ) -> None:
        parameters = {
            "account_id": event.account_id,
            "last_sequence": event.sequence,
            "last_event_hash": event.event_hash,
            "last_cycle_hash": event.cycle_hash,
            "last_evaluated_at": event.evaluated_at,
        }
        if exists:
            await connection.execute(
                text(
                    f"UPDATE {self._schema}.paper_scheduler_state SET "
                    "last_sequence=:last_sequence, last_event_hash=:last_event_hash, "
                    "last_cycle_hash=:last_cycle_hash, "
                    "last_evaluated_at=:last_evaluated_at, "
                    "updated_at=clock_timestamp() WHERE account_id=:account_id"
                ),
                parameters,
            )
        else:
            await connection.execute(
                text(
                    f"INSERT INTO {self._schema}.paper_scheduler_state "
                    "(account_id, last_sequence, last_event_hash, last_cycle_hash, "
                    "last_evaluated_at) VALUES (:account_id, :last_sequence, "
                    ":last_event_hash, :last_cycle_hash, :last_evaluated_at)"
                ),
                parameters,
            )


def _event_from_row(row: RowMapping) -> PaperSchedulerEvent:
    return PaperSchedulerEvent(
        account_id=str(row["account_id"]),
        sequence=int(row["sequence"]),
        previous_hash=str(row["previous_hash"]),
        cycle_hash=str(row["cycle_hash"]),
        evaluated_at=row["evaluated_at"],
        event_hash=str(row["event_hash"]),
    )


def _scheduler_health_state(
    *,
    strategy_id: str,
    lease_active: bool,
    lease_strategy_id: str | None,
    event_strategy_id: str | None,
    event_age: timedelta | None,
    maximum_silence: timedelta,
    latest_status: str | None,
    latest_error_code: str | None,
) -> PaperSchedulerHealthState:
    if lease_active and lease_strategy_id != strategy_id:
        return PaperSchedulerHealthState.IDENTITY_MISMATCH
    if event_strategy_id is not None and event_strategy_id != strategy_id:
        return PaperSchedulerHealthState.IDENTITY_MISMATCH
    if not lease_active:
        return PaperSchedulerHealthState.STOPPED
    if event_age is None:
        return PaperSchedulerHealthState.STARTING
    if event_age < timedelta(0) or event_age > maximum_silence:
        return PaperSchedulerHealthState.STALE
    if latest_status == "failed" or latest_error_code is not None:
        return PaperSchedulerHealthState.FAILED
    return PaperSchedulerHealthState.HEALTHY


def _columns_match_payload(row: RowMapping, payload: dict[str, object]) -> bool:
    return (
        str(row["account_id"]) == payload.get("account_id")
        and str(row["strategy_id"]) == payload.get("strategy_id")
        and row["session_date"].isoformat() == payload.get("session_date")
        and to_utc(row["evaluated_at"]).isoformat(timespec="microseconds")
        == payload.get("evaluated_at")
        and str(row["phase"]) == payload.get("phase")
        and str(row["status"]) == payload.get("status")
        and str(row["control_state_hash"]) == payload.get("control_state_hash")
        and row["calendar_hash"] == payload.get("calendar_hash")
        and row["mark_evidence_hash"] == payload.get("mark_evidence_hash")
        and row["quote_evidence_hash"] == payload.get("quote_evidence_hash")
        and row["error_code"] == payload.get("error_code")
    )
