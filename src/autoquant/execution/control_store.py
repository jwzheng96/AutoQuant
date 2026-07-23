from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from time import monotonic

from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from autoquant.clock import to_utc
from autoquant.data.models import _canonical_hash
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.control import (
    KillSwitchAction,
    KillSwitchCommand,
    KillSwitchControl,
    KillSwitchEvent,
    KillSwitchReason,
    apply_kill_switch_command,
    command_payload,
    control_payload,
)
from autoquant.execution.paper_scheduler_lease_store import (
    scheduler_lease_token_hash,
)
from autoquant.execution.paper_unlock import PaperRuntimeUnlockEvidence

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_STATE_COLUMNS = """
account_id, active, version, reason, changed_at, changed_by,
last_event_hash, state_hash, state_payload
"""


class PostgresExecutionControlRepository:
    """Durable fail-closed kill switch with an immutable command chain."""

    def __init__(self, *, engine: AsyncEngine, schema: str = "public") -> None:
        if _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError("schema must be a safe PostgreSQL identifier")
        self._engine = engine
        self._schema = schema

    @classmethod
    def connect(cls, *, dsn: str, schema: str = "public") -> PostgresExecutionControlRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "PostgreSQL execution control connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    @asynccontextmanager
    async def coordination_lock(
        self, *, account_id: str, timeout: timedelta = timedelta(seconds=5)
    ) -> AsyncIterator[None]:
        """Serialize a full account cycle across processes without queueing pool slots."""
        if not account_id.strip():
            raise ValueError("account_id cannot be empty")
        if timeout <= timedelta(0):
            raise ValueError("coordination lock timeout must be positive")
        deadline = monotonic() + timeout.total_seconds()
        while True:
            connection: AsyncConnection | None = None
            try:
                connection = await self._engine.connect()
                acquired = bool(
                    await connection.scalar(
                        text("SELECT pg_try_advisory_lock(hashtextextended(:lock_key, 0))"),
                        {"lock_key": f"autoquant:paper-cycle:{account_id}"},
                    )
                )
            except Exception:
                if connection is not None:
                    try:
                        await connection.close()
                    except Exception:
                        pass
                raise PersistenceUnavailableError(
                    "Paper coordination lock acquisition failed"
                ) from None
            if connection is None:
                raise PersistenceUnavailableError(
                    "Paper coordination lock connection is unavailable"
                )
            if acquired:
                try:
                    yield
                finally:
                    released = False
                    try:
                        released = bool(
                            await connection.scalar(
                                text("SELECT pg_advisory_unlock(hashtextextended(:lock_key, 0))"),
                                {"lock_key": f"autoquant:paper-cycle:{account_id}"},
                            )
                        )
                        if not released:
                            await connection.invalidate()
                    except Exception:
                        await connection.invalidate()
                        raise PersistenceUnavailableError(
                            "Paper coordination lock release failed"
                        ) from None
                    finally:
                        await connection.close()
                    if not released:
                        raise PersistenceUnavailableError(
                            "Paper coordination lock ownership was lost"
                        )
                return
            await connection.close()
            if monotonic() >= deadline:
                raise PersistenceUnavailableError("Paper coordination lock acquisition timed out")
            await asyncio.sleep(0.01)

    async def ensure_fail_closed(self, *, account_id: str, now: datetime) -> KillSwitchControl:
        occurred_at = to_utc(now, name="kill switch initialization time")
        command = KillSwitchCommand(
            command_id=f"initialize-kill-switch:{account_id}",
            account_id=account_id,
            action=KillSwitchAction.INITIALIZE,
            reason=KillSwitchReason.INITIALIZING,
            actor="system",
            occurred_at=occurred_at,
        )
        try:
            async with self._engine.begin() as connection:
                await _lock(connection, account_id)
                row = await self._select_state(connection, account_id, for_update=True)
                if row is not None:
                    return _state_from_row(row)
                state, event = apply_kill_switch_command(None, command)
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.execution_control_state
                            (account_id, active, version, reason, changed_at,
                             changed_by, last_event_hash, state_hash, state_payload)
                        VALUES
                            (:account_id, :active, :version, :reason, :changed_at,
                             :changed_by, :last_event_hash, :state_hash,
                             CAST(:state_payload AS jsonb))
                        """
                    ),
                    _state_parameters(state),
                )
                await self._insert_event(connection, state, event)
                return state
        except (ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError("Kill switch initialization failed") from None

    async def get(self, *, account_id: str) -> KillSwitchControl:
        try:
            async with self._engine.connect() as connection:
                row = await self._select_state(connection, account_id, for_update=False)
        except Exception:
            raise PersistenceUnavailableError("Kill switch read failed") from None
        if row is None:
            raise LookupError("kill switch is not initialized")
        return _state_from_row(row)

    async def activate(
        self,
        *,
        account_id: str,
        command_id: str,
        reason: KillSwitchReason,
        actor: str,
        now: datetime,
        evidence_hash: str | None = None,
    ) -> KillSwitchControl:
        if reason in {KillSwitchReason.INITIALIZING, KillSwitchReason.RESET_APPROVED}:
            raise ValueError("activation reason is invalid")
        await self.ensure_fail_closed(account_id=account_id, now=now)
        command = KillSwitchCommand(
            command_id=command_id,
            account_id=account_id,
            action=KillSwitchAction.ACTIVATE,
            reason=reason,
            actor=actor,
            occurred_at=now,
            evidence_hash=evidence_hash,
        )
        return await self._mutate(command=command)

    async def reset(
        self,
        *,
        account_id: str,
        command_id: str,
        actor: str,
        now: datetime,
        expected_version: int,
        reconciliation_report_hash: str,
        recovery_verified: bool,
        max_reconciliation_age: timedelta = timedelta(seconds=10),
    ) -> KillSwitchControl:
        if type(recovery_verified) is not bool or not recovery_verified:
            raise ValueError("verified execution recovery is required for reset")
        if expected_version < 1:
            raise ValueError("expected_version must be positive")
        if max_reconciliation_age <= timedelta(0):
            raise ValueError("max_reconciliation_age must be positive")
        command = KillSwitchCommand(
            command_id=command_id,
            account_id=account_id,
            action=KillSwitchAction.RESET,
            reason=KillSwitchReason.RESET_APPROVED,
            actor=actor,
            occurred_at=now,
            evidence_hash=reconciliation_report_hash,
        )
        return await self._mutate(
            command=command,
            expected_version=expected_version,
            max_reconciliation_age=max_reconciliation_age,
        )

    async def reset_paper_runtime(
        self,
        *,
        evidence: PaperRuntimeUnlockEvidence,
        lease_token: SecretStr,
        command_id: str,
        actor: str,
        now: datetime,
        expected_version: int,
        max_evidence_age: timedelta = timedelta(seconds=3),
    ) -> KillSwitchControl:
        if not isinstance(evidence, PaperRuntimeUnlockEvidence):
            raise TypeError("evidence must be PaperRuntimeUnlockEvidence")
        if scheduler_lease_token_hash(lease_token) != evidence.lease_token_hash:
            raise ValueError("paper runtime lease token does not match evidence")
        if max_evidence_age <= timedelta(0):
            raise ValueError("paper runtime evidence age must be positive")
        command = KillSwitchCommand(
            command_id=command_id,
            account_id=evidence.account_id,
            action=KillSwitchAction.RESET,
            reason=KillSwitchReason.RESET_APPROVED,
            actor=actor,
            occurred_at=now,
            evidence_hash=evidence.evidence_hash,
        )
        return await self._mutate(
            command=command,
            expected_version=expected_version,
            max_reconciliation_age=max_evidence_age,
            runtime_evidence=evidence,
        )

    async def replay(self, *, account_id: str) -> KillSwitchControl:
        current = await self.get(account_id=account_id)
        try:
            async with self._engine.connect() as connection:
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT event_hash, sequence, command_hash,
                                       previous_hash, transition_state_hash,
                                       command_payload
                                FROM {self._schema}.execution_control_events
                                WHERE account_id = :account_id
                                ORDER BY sequence
                                """
                            ),
                            {"account_id": account_id},
                        )
                    )
                    .mappings()
                    .all()
                )
        except Exception:
            raise PersistenceUnavailableError("Kill switch replay read failed") from None
        replayed: KillSwitchControl | None = None
        for row in rows:
            command = _command_from_payload(row["command_payload"])
            replayed, event = apply_kill_switch_command(replayed, command)
            if (
                event.event_hash != str(row["event_hash"])
                or event.sequence != int(row["sequence"])
                or command.command_hash != str(row["command_hash"])
                or event.previous_hash != str(row["previous_hash"])
                or event.transition_state_hash != str(row["transition_state_hash"])
            ):
                raise PersistenceUnavailableError("Kill switch event failed integrity verification")
        if replayed is None or replayed != current:
            raise PersistenceUnavailableError("Kill switch state does not match event replay")
        return replayed

    async def _mutate(
        self,
        *,
        command: KillSwitchCommand,
        expected_version: int | None = None,
        max_reconciliation_age: timedelta | None = None,
        runtime_evidence: PaperRuntimeUnlockEvidence | None = None,
    ) -> KillSwitchControl:
        try:
            async with self._engine.begin() as connection:
                await _lock(connection, command.account_id)
                row = await self._select_state(connection, command.account_id, for_update=True)
                if row is None:
                    raise LookupError("kill switch is not initialized")
                current = _state_from_row(row)
                prior = (
                    (
                        await connection.execute(
                            text(
                                f"SELECT command_hash FROM {self._schema}.execution_control_events "
                                "WHERE command_id = :command_id"
                            ),
                            {"command_id": command.command_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if prior is not None:
                    if str(prior["command_hash"]) != command.command_hash:
                        raise ValueError("command_id already belongs to another command")
                    return current
                if expected_version is not None and current.version != expected_version:
                    raise ValueError("kill switch version changed before reset")
                if command.action is KillSwitchAction.RESET:
                    if not current.active:
                        raise ValueError("kill switch is already inactive")
                    if max_reconciliation_age is None:
                        raise ValueError("reset reconciliation age is missing")
                    if runtime_evidence is None:
                        await self._verify_reset_evidence(
                            connection,
                            command=command,
                            max_age=max_reconciliation_age,
                        )
                    else:
                        await self._verify_paper_runtime_reset(
                            connection,
                            command=command,
                            evidence=runtime_evidence,
                            max_age=max_reconciliation_age,
                        )
                state, event = apply_kill_switch_command(current, command)
                await self._insert_event(connection, state, event)
                result = await connection.execute(
                    text(
                        f"""
                        UPDATE {self._schema}.execution_control_state
                        SET active = :active,
                            version = :version,
                            reason = :reason,
                            changed_at = :changed_at,
                            changed_by = :changed_by,
                            last_event_hash = :last_event_hash,
                            state_hash = :state_hash,
                            state_payload = CAST(:state_payload AS jsonb)
                        WHERE account_id = :account_id
                          AND state_hash = :previous_state_hash
                        """
                    ),
                    {
                        **_state_parameters(state),
                        "previous_state_hash": current.state_hash,
                    },
                )
                if result.rowcount != 1:
                    raise PersistenceUnavailableError("Kill switch optimistic update failed")
                return state
        except (LookupError, ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError("Kill switch update failed") from None

    async def _verify_reset_evidence(
        self,
        connection: AsyncConnection,
        *,
        command: KillSwitchCommand,
        max_age: timedelta,
    ) -> None:
        row = (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT account_id, evaluated_at, reconciled
                        FROM {self._schema}.execution_reconciliation_reports
                        WHERE report_hash = :report_hash
                        """
                    ),
                    {"report_hash": command.evidence_hash},
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None or not bool(row["reconciled"]):
            raise ValueError("reset requires a passing persisted reconciliation")
        evaluated_at = to_utc(row["evaluated_at"], name="reconciliation time")
        if str(row["account_id"]) != command.account_id:
            raise ValueError("reset reconciliation belongs to another account")
        if command.occurred_at < evaluated_at or command.occurred_at - evaluated_at > max_age:
            raise ValueError("reset reconciliation is stale or from the future")

    async def _verify_paper_runtime_reset(
        self,
        connection: AsyncConnection,
        *,
        command: KillSwitchCommand,
        evidence: PaperRuntimeUnlockEvidence,
        max_age: timedelta,
    ) -> None:
        if command.evidence_hash != evidence.evidence_hash:
            raise ValueError("paper runtime reset evidence hash does not match")
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
            .one_or_none()
        )
        if row is None:
            raise ValueError("paper runtime reset evidence is not persisted")
        payload = dict(row["evidence_payload"])
        if (
            _canonical_hash(payload) != evidence.evidence_hash
            or payload != evidence.payload()
            or str(row["account_id"]) != evidence.account_id
            or str(row["strategy_id"]) != evidence.strategy_id
            or row["session_date"] != evidence.session_date
            or str(row["registration_hash"]) != evidence.registration_hash
            or str(row["calendar_hash"]) != evidence.calendar_hash
            or str(row["session_state_hash"]) != evidence.session_state_hash
            or str(row["quote_evidence_hash"]) != evidence.quote_evidence_hash
            or str(row["reconciliation_report_hash"])
            != evidence.reconciliation_report_hash
            or str(row["lease_holder_id"]) != evidence.lease_holder_id
            or str(row["lease_token_hash"]) != evidence.lease_token_hash
            or int(row["lease_generation"]) != evidence.lease_generation
        ):
            raise ValueError("paper runtime reset evidence is inconsistent")
        evaluated_at = to_utc(
            row["evaluated_at"],
            name="paper runtime evidence time",
        )
        if (
            evaluated_at != evidence.evaluated_at
            or command.occurred_at < evaluated_at
            or command.occurred_at - evaluated_at > max_age
        ):
            raise ValueError("paper runtime reset evidence is stale or from the future")

        reconciliation = (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT account_id, evaluated_at, reconciled
                        FROM {self._schema}.execution_reconciliation_reports
                        WHERE report_hash = :report_hash
                        """
                    ),
                    {"report_hash": evidence.reconciliation_report_hash},
                )
            )
            .mappings()
            .one_or_none()
        )
        if (
            reconciliation is None
            or str(reconciliation["account_id"]) != evidence.account_id
            or not bool(reconciliation["reconciled"])
            or to_utc(reconciliation["evaluated_at"]) != evaluated_at
        ):
            raise ValueError("paper runtime reset reconciliation is not current")

        session_state_hash = await connection.scalar(
            text(
                f"""
                SELECT state_hash
                FROM {self._schema}.paper_session_risk_state
                WHERE account_id = :account_id
                  AND session_date = :session_date
                """
            ),
            {
                "account_id": evidence.account_id,
                "session_date": evidence.session_date,
            },
        )
        if str(session_state_hash) != evidence.session_state_hash:
            raise ValueError("paper runtime session state changed before reset")

        activations = (
            (
                await connection.execute(
                    text(
                        f"""
                        WITH single_latest AS (
                            SELECT action, registration_hash
                            FROM {self._schema}.paper_strategy_activation_events
                            WHERE account_id = :account_id
                              AND strategy_id = :strategy_id
                            ORDER BY sequence DESC
                            LIMIT 1
                        ),
                        portfolio_latest AS (
                            SELECT action, registration_hash
                            FROM {self._schema}.paper_portfolio_activation_events
                            WHERE account_id = :account_id
                              AND strategy_id = :strategy_id
                            ORDER BY sequence DESC
                            LIMIT 1
                        )
                        SELECT 'single' AS deployment_kind,
                               action, registration_hash
                        FROM single_latest
                        UNION ALL
                        SELECT 'portfolio' AS deployment_kind,
                               action, registration_hash
                        FROM portfolio_latest
                        """
                    ),
                    {
                        "account_id": evidence.account_id,
                        "strategy_id": evidence.strategy_id,
                    },
                )
            )
            .mappings()
            .all()
        )
        active_approvals = tuple(
            row
            for row in activations
            if str(row["action"]) == "approve"
        )
        if (
            len(active_approvals) != 1
            or str(active_approvals[0]["registration_hash"])
            != evidence.registration_hash
        ):
            raise ValueError(
                "paper runtime deployment approval changed before reset"
            )

        lease = (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT strategy_id, holder_id, token_hash, generation,
                               acquired_at, expires_at, released_at
                        FROM {self._schema}.paper_scheduler_leases
                        WHERE account_id = :account_id
                        FOR SHARE
                        """
                    ),
                    {"account_id": evidence.account_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if (
            lease is None
            or str(lease["strategy_id"]) != evidence.strategy_id
            or str(lease["holder_id"]) != evidence.lease_holder_id
            or str(lease["token_hash"]) != evidence.lease_token_hash
            or int(lease["generation"]) != evidence.lease_generation
            or lease["released_at"] is not None
            or to_utc(lease["acquired_at"]) > evidence.evaluated_at
            or to_utc(lease["expires_at"]) <= command.occurred_at
        ):
            raise ValueError("paper scheduler lease changed before reset")

    async def _select_state(
        self, connection: AsyncConnection, account_id: str, *, for_update: bool
    ) -> RowMapping | None:
        suffix = " FOR UPDATE" if for_update else ""
        return (
            (
                await connection.execute(
                    text(
                        f"SELECT {_STATE_COLUMNS} "
                        f"FROM {self._schema}.execution_control_state "
                        f"WHERE account_id = :account_id{suffix}"
                    ),
                    {"account_id": account_id},
                )
            )
            .mappings()
            .one_or_none()
        )

    async def _insert_event(
        self,
        connection: AsyncConnection,
        state: KillSwitchControl,
        event: KillSwitchEvent,
    ) -> None:
        command = event.command
        await connection.execute(
            text(
                f"""
                INSERT INTO {self._schema}.execution_control_events
                    (event_hash, command_id, command_hash, account_id,
                     sequence, action, reason, actor, occurred_at,
                     evidence_hash, previous_hash, transition_state_hash,
                     command_payload)
                VALUES
                    (:event_hash, :command_id, :command_hash, :account_id,
                     :sequence, :action, :reason, :actor, :occurred_at,
                     :evidence_hash, :previous_hash, :transition_state_hash,
                     CAST(:command_payload AS jsonb))
                """
            ),
            {
                "event_hash": event.event_hash,
                "command_id": command.command_id,
                "command_hash": command.command_hash,
                "account_id": state.account_id,
                "sequence": event.sequence,
                "action": command.action.value,
                "reason": command.reason.value,
                "actor": command.actor,
                "occurred_at": command.occurred_at,
                "evidence_hash": command.evidence_hash,
                "previous_hash": event.previous_hash,
                "transition_state_hash": event.transition_state_hash,
                "command_payload": _json(command_payload(command)),
            },
        )


async def _lock(connection: AsyncConnection, account_id: str) -> None:
    await connection.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
        {"lock_key": f"autoquant:execution-control:{account_id}"},
    )


def _state_from_row(row: RowMapping) -> KillSwitchControl:
    try:
        payload = _object(row["state_payload"])
        state = KillSwitchControl(
            account_id=str(payload["account_id"]),
            active=bool(payload["active"]),
            version=int(str(payload["version"])),
            reason=KillSwitchReason(str(payload["reason"])),
            changed_at=_datetime(payload["changed_at"]),
            changed_by=str(payload["changed_by"]),
            last_event_hash=str(payload["last_event_hash"]),
        )
        if (
            state.account_id != str(row["account_id"])
            or state.active is not bool(row["active"])
            or state.version != int(row["version"])
            or state.reason.value != str(row["reason"])
            or state.changed_at != _datetime(row["changed_at"])
            or state.changed_by != str(row["changed_by"])
            or state.last_event_hash != str(row["last_event_hash"])
            or state.state_hash != str(row["state_hash"])
        ):
            raise ValueError("kill switch columns do not match payload")
        return state
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "Stored kill switch state failed integrity verification"
        ) from None


def _command_from_payload(raw: object) -> KillSwitchCommand:
    try:
        payload = _object(raw)
        command = KillSwitchCommand(
            command_id=str(payload["command_id"]),
            account_id=str(payload["account_id"]),
            action=KillSwitchAction(str(payload["action"])),
            reason=KillSwitchReason(str(payload["reason"])),
            actor=str(payload["actor"]),
            occurred_at=_datetime(payload["occurred_at"]),
            evidence_hash=(
                None if payload["evidence_hash"] is None else str(payload["evidence_hash"])
            ),
        )
        if _canonical_hash(payload) != command.command_hash:
            raise ValueError("kill switch command hash mismatch")
        return command
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "Stored kill switch command failed integrity verification"
        ) from None


def _state_parameters(state: KillSwitchControl) -> dict[str, object]:
    return {
        "account_id": state.account_id,
        "active": state.active,
        "version": state.version,
        "reason": state.reason.value,
        "changed_at": state.changed_at,
        "changed_by": state.changed_by,
        "last_event_hash": state.last_event_hash,
        "state_hash": state.state_hash,
        "state_payload": _json(control_payload(state)),
    }


def _object(raw: object) -> dict[str, object]:
    value = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(value, dict):
        raise TypeError("stored payload must be an object")
    return value


def _datetime(raw: object) -> datetime:
    if isinstance(raw, datetime):
        return to_utc(raw)
    if isinstance(raw, str):
        return to_utc(datetime.fromisoformat(raw))
    raise TypeError("stored timestamp is invalid")


def _json(value: object) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)
