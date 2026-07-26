from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    create_async_engine,
)

from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _datetime_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.models import ZERO_HASH

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_BASELINE_MAX_AGE = timedelta(hours=24)


class QmtRecoveryDrillKind(StrEnum):
    DISCONNECT = "disconnect_recovery"
    MINIQMT_RESTART = "miniqmt_restart_recovery"


class QmtRecoveryDrillAction(StrEnum):
    START = "start"
    COMPLETE = "complete"


@dataclass(frozen=True, slots=True)
class QmtRecoveryDrillEvent:
    drill_id: UUID
    sequence: int
    account_id: str
    kind: QmtRecoveryDrillKind
    action: QmtRecoveryDrillAction
    actor: str
    occurred_at: datetime
    expires_at: datetime
    baseline_qmt_evidence_hash: str
    recovery_qmt_evidence_hash: str | None
    failure_control_event_hash: str | None
    previous_hash: str
    event_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.drill_id, UUID):
            raise TypeError("QMT recovery drill_id is invalid")
        _require_nonblank(self.account_id, name="drill account_id")
        _require_nonblank(self.actor, name="drill actor")
        if not isinstance(self.kind, QmtRecoveryDrillKind):
            raise TypeError("QMT recovery drill kind is invalid")
        if not isinstance(self.action, QmtRecoveryDrillAction):
            raise TypeError("QMT recovery drill action is invalid")
        occurred = to_utc(
            self.occurred_at,
            name="QMT recovery drill time",
        )
        expires = to_utc(
            self.expires_at,
            name="QMT recovery drill expiry",
        )
        if expires <= occurred:
            raise ValueError("QMT recovery drill expiry must be in the future")
        _require_lowercase_sha256(
            self.baseline_qmt_evidence_hash,
            name="baseline_qmt_evidence_hash",
        )
        _require_lowercase_sha256(self.previous_hash, name="previous_hash")
        for name, value in (
            (
                "recovery_qmt_evidence_hash",
                self.recovery_qmt_evidence_hash,
            ),
            (
                "failure_control_event_hash",
                self.failure_control_event_hash,
            ),
        ):
            if value is not None:
                _require_lowercase_sha256(value, name=name)
        if self.action is QmtRecoveryDrillAction.START:
            if (
                self.sequence != 1
                or self.previous_hash != ZERO_HASH
                or self.recovery_qmt_evidence_hash is not None
                or self.failure_control_event_hash is not None
            ):
                raise ValueError("QMT recovery drill start evidence is invalid")
        elif (
            self.sequence != 2
            or self.previous_hash == ZERO_HASH
            or self.recovery_qmt_evidence_hash is None
            or self.failure_control_event_hash is None
            or self.recovery_qmt_evidence_hash
            == self.baseline_qmt_evidence_hash
        ):
            raise ValueError("QMT recovery drill completion evidence is invalid")
        object.__setattr__(self, "occurred_at", occurred)
        object.__setattr__(self, "expires_at", expires)
        object.__setattr__(
            self,
            "event_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "account_id": self.account_id,
            "action": self.action.value,
            "actor": self.actor,
            "baseline_qmt_evidence_hash": (
                self.baseline_qmt_evidence_hash
            ),
            "drill_id": str(self.drill_id),
            "expires_at": _datetime_text(self.expires_at),
            "failure_control_event_hash": (
                self.failure_control_event_hash
            ),
            "kind": self.kind.value,
            "occurred_at": _datetime_text(self.occurred_at),
            "previous_hash": self.previous_hash,
            "recovery_qmt_evidence_hash": (
                self.recovery_qmt_evidence_hash
            ),
            "sequence": self.sequence,
            "version": "qmt-recovery-drill-event-v1",
        }


class PostgresQmtRecoveryDrillRepository:
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
    ) -> PostgresQmtRecoveryDrillRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "PostgreSQL QMT recovery drill connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def start(
        self,
        *,
        account_id: str,
        kind: QmtRecoveryDrillKind,
        actor: str,
        now: datetime,
        ttl: timedelta = timedelta(minutes=30),
    ) -> QmtRecoveryDrillEvent:
        _require_nonblank(account_id, name="drill account_id")
        _require_nonblank(actor, name="drill actor")
        if not isinstance(kind, QmtRecoveryDrillKind):
            raise TypeError("QMT recovery drill kind is invalid")
        instant = to_utc(now, name="QMT recovery drill start time")
        if not timedelta(minutes=5) <= ttl <= timedelta(hours=1):
            raise ValueError("QMT recovery drill ttl must be 5-60 minutes")
        try:
            async with self._engine.begin() as connection:
                await _lock(
                    connection,
                    f"qmt-recovery-drill:{account_id}:{kind.value}",
                )
                await _require_active_control(
                    connection,
                    schema=self._schema,
                    account_id=account_id,
                )
                active = await connection.scalar(
                    text(
                        f"""
                        SELECT count(*)
                        FROM {self._schema}.qmt_recovery_drill_events start_event
                        WHERE start_event.account_id = :account_id
                          AND start_event.kind = :kind
                          AND start_event.action = 'start'
                          AND start_event.expires_at > :now
                          AND NOT EXISTS (
                              SELECT 1
                              FROM {self._schema}.qmt_recovery_drill_events completed
                              WHERE completed.drill_id = start_event.drill_id
                                AND completed.action = 'complete'
                          )
                        """
                    ),
                    {
                        "account_id": account_id,
                        "kind": kind.value,
                        "now": instant,
                    },
                )
                if int(active or 0):
                    raise ValueError(
                        "an unexpired QMT recovery drill is already active"
                    )
                baseline = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT evidence_hash, observed_at
                                FROM {self._schema}.qmt_readonly_acceptance_evidence
                                WHERE logical_account_id = :account_id
                                  AND observed_at <= :now
                                  AND clock_attestation_hash IS NOT NULL
                                  AND clock_attestation_payload
                                      ->>'trusted' = 'true'
                                ORDER BY observed_at DESC, evidence_hash DESC
                                LIMIT 1
                                """
                            ),
                            {
                                "account_id": account_id,
                                "now": instant,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if (
                    baseline is None
                    or instant - baseline["observed_at"]
                    > _BASELINE_MAX_AGE
                ):
                    raise ValueError(
                        "QMT recovery drill requires fresh baseline acceptance"
                    )
                event = QmtRecoveryDrillEvent(
                    drill_id=uuid4(),
                    sequence=1,
                    account_id=account_id,
                    kind=kind,
                    action=QmtRecoveryDrillAction.START,
                    actor=actor,
                    occurred_at=instant,
                    expires_at=instant + ttl,
                    baseline_qmt_evidence_hash=str(
                        baseline["evidence_hash"]
                    ),
                    recovery_qmt_evidence_hash=None,
                    failure_control_event_hash=None,
                    previous_hash=ZERO_HASH,
                )
                await _insert(
                    connection,
                    schema=self._schema,
                    event=event,
                )
                return event
        except (TypeError, ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "QMT recovery drill start failed"
            ) from None

    async def complete(
        self,
        *,
        drill_id: UUID,
        actor: str,
        now: datetime,
    ) -> QmtRecoveryDrillEvent:
        if not isinstance(drill_id, UUID):
            raise TypeError("QMT recovery drill_id is invalid")
        _require_nonblank(actor, name="drill actor")
        instant = to_utc(now, name="QMT recovery drill completion time")
        try:
            async with self._engine.begin() as connection:
                await _lock(
                    connection,
                    f"qmt-recovery-drill-id:{drill_id}",
                )
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.qmt_recovery_drill_events
                                WHERE drill_id = :drill_id
                                ORDER BY sequence
                                """
                            ),
                            {"drill_id": drill_id},
                        )
                    )
                    .mappings()
                    .all()
                )
                if not rows:
                    raise LookupError("QMT recovery drill not found")
                start = _from_row(rows[0])
                if len(rows) > 1:
                    completed = _from_row(rows[-1])
                    if completed.action is QmtRecoveryDrillAction.COMPLETE:
                        return completed
                    raise PersistenceUnavailableError(
                        "QMT recovery drill chain is invalid"
                    )
                if not start.occurred_at < instant < start.expires_at:
                    raise ValueError(
                        "QMT recovery drill is expired or time is invalid"
                    )
                await _require_active_control(
                    connection,
                    schema=self._schema,
                    account_id=start.account_id,
                )
                failure = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT event_hash, occurred_at
                                FROM {self._schema}.execution_control_events
                                WHERE account_id = :account_id
                                  AND action = 'activate'
                                  AND reason IN (
                                      'dependency_unavailable',
                                      'recovery_failed'
                                  )
                                  AND occurred_at > :started_at
                                  AND occurred_at < :now
                                ORDER BY occurred_at DESC, sequence DESC
                                LIMIT 1
                                """
                            ),
                            {
                                "account_id": start.account_id,
                                "started_at": start.occurred_at,
                                "now": instant,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if failure is None:
                    raise ValueError(
                        "QMT recovery drill has no observed fail-closed event"
                    )
                recovery = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT evidence_hash, observed_at
                                FROM {self._schema}.qmt_readonly_acceptance_evidence
                                WHERE logical_account_id = :account_id
                                  AND observed_at > :failure_at
                                  AND observed_at < :now
                                  AND clock_attestation_hash IS NOT NULL
                                  AND clock_attestation_payload
                                      ->>'trusted' = 'true'
                                ORDER BY observed_at DESC, evidence_hash DESC
                                LIMIT 1
                                """
                            ),
                            {
                                "account_id": start.account_id,
                                "failure_at": failure["occurred_at"],
                                "now": instant,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if recovery is None:
                    raise ValueError(
                        "QMT recovery drill requires post-failure acceptance"
                    )
                event = QmtRecoveryDrillEvent(
                    drill_id=start.drill_id,
                    sequence=2,
                    account_id=start.account_id,
                    kind=start.kind,
                    action=QmtRecoveryDrillAction.COMPLETE,
                    actor=actor,
                    occurred_at=instant,
                    expires_at=start.expires_at,
                    baseline_qmt_evidence_hash=(
                        start.baseline_qmt_evidence_hash
                    ),
                    recovery_qmt_evidence_hash=str(
                        recovery["evidence_hash"]
                    ),
                    failure_control_event_hash=str(
                        failure["event_hash"]
                    ),
                    previous_hash=start.event_hash,
                )
                await _insert(
                    connection,
                    schema=self._schema,
                    event=event,
                )
                return event
        except (
            LookupError,
            TypeError,
            ValueError,
            PersistenceUnavailableError,
        ):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "QMT recovery drill completion failed"
            ) from None


async def _lock(connection: AsyncConnection, key: str) -> None:
    await connection.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": key},
    )


async def _require_active_control(
    connection: AsyncConnection,
    *,
    schema: str,
    account_id: str,
) -> None:
    active = await connection.scalar(
        text(
            f"""
            SELECT active
            FROM {schema}.execution_control_state
            WHERE account_id = :account_id
            """
        ),
        {"account_id": account_id},
    )
    if active is not True:
        raise ValueError(
            "QMT recovery drill requires the kill switch to remain active"
        )


async def _insert(
    connection: AsyncConnection,
    *,
    schema: str,
    event: QmtRecoveryDrillEvent,
) -> None:
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema}.qmt_recovery_drill_events
                (event_hash, drill_id, sequence, account_id, kind, action,
                 actor, occurred_at, expires_at,
                 baseline_qmt_evidence_hash, recovery_qmt_evidence_hash,
                 failure_control_event_hash, previous_hash, event_payload)
            VALUES
                (:event_hash, :drill_id, :sequence, :account_id, :kind,
                 :action, :actor, :occurred_at, :expires_at,
                 :baseline_qmt_evidence_hash,
                 :recovery_qmt_evidence_hash,
                 :failure_control_event_hash, :previous_hash,
                 CAST(:event_payload AS jsonb))
            """
        ),
        {
            "event_hash": event.event_hash,
            "drill_id": event.drill_id,
            "sequence": event.sequence,
            "account_id": event.account_id,
            "kind": event.kind.value,
            "action": event.action.value,
            "actor": event.actor,
            "occurred_at": event.occurred_at,
            "expires_at": event.expires_at,
            "baseline_qmt_evidence_hash": (
                event.baseline_qmt_evidence_hash
            ),
            "recovery_qmt_evidence_hash": (
                event.recovery_qmt_evidence_hash
            ),
            "failure_control_event_hash": (
                event.failure_control_event_hash
            ),
            "previous_hash": event.previous_hash,
            "event_payload": json.dumps(
                event.payload(),
                sort_keys=True,
                separators=(",", ":"),
            ),
        },
    )


def _from_row(row: RowMapping) -> QmtRecoveryDrillEvent:
    try:
        event = QmtRecoveryDrillEvent(
            drill_id=row["drill_id"],
            sequence=int(row["sequence"]),
            account_id=str(row["account_id"]),
            kind=QmtRecoveryDrillKind(str(row["kind"])),
            action=QmtRecoveryDrillAction(str(row["action"])),
            actor=str(row["actor"]),
            occurred_at=row["occurred_at"],
            expires_at=row["expires_at"],
            baseline_qmt_evidence_hash=str(
                row["baseline_qmt_evidence_hash"]
            ),
            recovery_qmt_evidence_hash=(
                None
                if row["recovery_qmt_evidence_hash"] is None
                else str(row["recovery_qmt_evidence_hash"])
            ),
            failure_control_event_hash=(
                None
                if row["failure_control_event_hash"] is None
                else str(row["failure_control_event_hash"])
            ),
            previous_hash=str(row["previous_hash"]),
        )
        if (
            event.event_hash != str(row["event_hash"])
            or event.payload() != row["event_payload"]
        ):
            raise ValueError("QMT recovery drill event integrity failed")
        return event
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "Stored QMT recovery drill event is invalid"
        ) from None
