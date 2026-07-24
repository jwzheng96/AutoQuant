from __future__ import annotations

import json
import re
from datetime import timedelta

from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.clock import SHANGHAI
from autoquant.data.models import _require_nonblank
from autoquant.errors import (
    BrokerStateUnknownError,
    PersistenceUnavailableError,
    QmtSessionLeaseLostError,
)
from autoquant.execution.models import ZERO_HASH
from autoquant.execution.qmt_callback_inbox import (
    QmtCallbackInboxEvent,
    QmtSanitizedCallback,
    replay_qmt_callback_inbox,
)
from autoquant.execution.qmt_gateway import QmtCallbackKind
from autoquant.execution.qmt_session_store import qmt_session_token_hash

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_HOLDER_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")
_MAXIMUM_CALLBACK_PERSISTENCE_AGE = timedelta(seconds=5)


class PostgresQmtCallbackInbox:
    """Lease-fenced append-only callback evidence; it never invokes a broker."""

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
    ) -> PostgresQmtCallbackInbox:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError("QMT callback inbox connection failed") from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def check_connection(self) -> None:
        try:
            async with self._engine.connect() as connection:
                table_exists = await connection.scalar(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.tables
                            WHERE table_schema = :schema
                              AND table_name = 'qmt_callback_inbox_events'
                        )
                        """
                    ),
                    {"schema": self._schema},
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
            raise PersistenceUnavailableError("QMT callback inbox schema check failed") from None
        if table_exists is not True or not isinstance(version, int) or version < 40:
            raise PersistenceUnavailableError("QMT callback inbox schema v40 is unavailable")

    async def append(
        self,
        callback: QmtSanitizedCallback,
        *,
        gateway_holder_id: str,
        qmt_session_id: int,
        qmt_lease_generation: int,
        lease_token: SecretStr,
    ) -> QmtCallbackInboxEvent:
        if not isinstance(callback, QmtSanitizedCallback):
            raise TypeError("callback must be QmtSanitizedCallback")
        _validate_scope(
            account_id=callback.account_id,
            gateway_holder_id=gateway_holder_id,
            qmt_session_id=qmt_session_id,
            qmt_lease_generation=qmt_lease_generation,
        )
        token_hash = _token_hash(lease_token)
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:identity))"),
                    {
                        "identity": (
                            f"qmt-callback:{callback.account_id}:"
                            f"{gateway_holder_id}:{qmt_session_id}:"
                            f"{qmt_lease_generation}"
                        )
                    },
                )
                lease_row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *, clock_timestamp() AS observed_at
                                FROM {self._schema}.qmt_session_leases
                                WHERE session_id = :qmt_session_id
                                FOR SHARE
                                """
                            ),
                            {"qmt_session_id": qmt_session_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if not _lease_matches(
                    lease_row,
                    callback=callback,
                    gateway_holder_id=gateway_holder_id,
                    qmt_lease_generation=qmt_lease_generation,
                    token_hash=token_hash,
                ):
                    raise QmtSessionLeaseLostError(
                        "QMT callback inbox requires its matching active daily bearer lease"
                    )
                assert lease_row is not None
                observed_at = lease_row["observed_at"]
                existing_row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.qmt_callback_inbox_events
                                WHERE account_id = :account_id
                                  AND gateway_holder_id =
                                      :gateway_holder_id
                                  AND qmt_session_id = :qmt_session_id
                                  AND qmt_lease_generation =
                                      :qmt_lease_generation
                                  AND local_sequence = :local_sequence
                                """
                            ),
                            _scope_parameters(
                                callback,
                                gateway_holder_id=gateway_holder_id,
                                qmt_session_id=qmt_session_id,
                                qmt_lease_generation=qmt_lease_generation,
                            ),
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if existing_row is not None:
                    existing = _event_from_row(existing_row)
                    if existing.callback != callback:
                        raise BrokerStateUnknownError(
                            "QMT callback sequence conflicts with durable evidence"
                        )
                    return existing
                if (
                    callback.received_at > observed_at
                    or observed_at - callback.received_at > _MAXIMUM_CALLBACK_PERSISTENCE_AGE
                ):
                    raise BrokerStateUnknownError(
                        "QMT callback inbox requires persistence within five seconds"
                    )
                last_row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.qmt_callback_inbox_events
                                WHERE account_id = :account_id
                                  AND gateway_holder_id =
                                      :gateway_holder_id
                                  AND qmt_session_id = :qmt_session_id
                                  AND qmt_lease_generation =
                                      :qmt_lease_generation
                                ORDER BY local_sequence DESC
                                LIMIT 1
                                """
                            ),
                            _scope_parameters(
                                callback,
                                gateway_holder_id=gateway_holder_id,
                                qmt_session_id=qmt_session_id,
                                qmt_lease_generation=qmt_lease_generation,
                            ),
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                previous = None if last_row is None else _event_from_row(last_row)
                expected_sequence = 1 if previous is None else previous.callback.local_sequence + 1
                if callback.local_sequence != expected_sequence:
                    raise BrokerStateUnknownError(
                        "QMT callback inbox sequence has a gap or time regression"
                    )
                event = QmtCallbackInboxEvent(
                    callback=callback,
                    gateway_holder_id=gateway_holder_id,
                    qmt_session_id=qmt_session_id,
                    qmt_lease_generation=qmt_lease_generation,
                    previous_hash=(ZERO_HASH if previous is None else previous.event_hash),
                )
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.qmt_callback_inbox_events
                            (event_hash, account_id, gateway_holder_id,
                             qmt_session_id, qmt_lease_generation,
                             broker_session_date, local_sequence,
                             kind, received_at, previous_hash,
                             callback_payload_hash, redacted_payload,
                             broker_mutation_allowed,
                             event_version, event_payload)
                        VALUES
                            (:event_hash, :account_id, :gateway_holder_id,
                             :qmt_session_id, :qmt_lease_generation,
                             :broker_session_date, :local_sequence,
                             :kind, :received_at, :previous_hash,
                             :callback_payload_hash,
                             CAST(:redacted_payload AS jsonb), false,
                             :event_version,
                             CAST(:event_payload AS jsonb))
                        """
                    ),
                    _event_parameters(event),
                )
                stored_row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.qmt_callback_inbox_events
                                WHERE event_hash = :event_hash
                                """
                            ),
                            {"event_hash": event.event_hash},
                        )
                    )
                    .mappings()
                    .one()
                )
            stored = _event_from_row(stored_row)
            if stored != event:
                raise PersistenceUnavailableError(
                    "stored QMT callback failed integrity verification"
                )
            return stored
        except (
            TypeError,
            ValueError,
            BrokerStateUnknownError,
            QmtSessionLeaseLostError,
        ):
            raise
        except PersistenceUnavailableError:
            raise
        except Exception:
            raise PersistenceUnavailableError("QMT callback inbox persistence failed") from None

    async def replay_current(
        self,
        *,
        account_id: str,
        gateway_holder_id: str,
        qmt_session_id: int,
        qmt_lease_generation: int,
        lease_token: SecretStr,
    ) -> tuple[QmtCallbackInboxEvent, ...]:
        _validate_scope(
            account_id=account_id,
            gateway_holder_id=gateway_holder_id,
            qmt_session_id=qmt_session_id,
            qmt_lease_generation=qmt_lease_generation,
        )
        token_hash = _token_hash(lease_token)
        try:
            async with self._engine.begin() as connection:
                lease_row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *, clock_timestamp() AS observed_at
                                FROM {self._schema}.qmt_session_leases
                                WHERE session_id = :qmt_session_id
                                FOR SHARE
                                """
                            ),
                            {"qmt_session_id": qmt_session_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if not _scope_lease_matches(
                    lease_row,
                    gateway_holder_id=gateway_holder_id,
                    qmt_lease_generation=qmt_lease_generation,
                    token_hash=token_hash,
                ):
                    raise QmtSessionLeaseLostError(
                        "QMT callback replay requires its matching active daily bearer lease"
                    )
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.qmt_callback_inbox_events
                                WHERE account_id = :account_id
                                  AND gateway_holder_id =
                                      :gateway_holder_id
                                  AND qmt_session_id = :qmt_session_id
                                  AND qmt_lease_generation =
                                      :qmt_lease_generation
                                ORDER BY local_sequence
                                """
                            ),
                            {
                                "account_id": account_id,
                                "gateway_holder_id": gateway_holder_id,
                                "qmt_lease_generation": qmt_lease_generation,
                                "qmt_session_id": qmt_session_id,
                            },
                        )
                    )
                    .mappings()
                    .all()
                )
            return replay_qmt_callback_inbox(tuple(_event_from_row(row) for row in rows))
        except (
            TypeError,
            ValueError,
            BrokerStateUnknownError,
            QmtSessionLeaseLostError,
        ):
            raise
        except PersistenceUnavailableError:
            raise
        except Exception:
            raise PersistenceUnavailableError("QMT callback inbox replay failed") from None


def _token_hash(lease_token: SecretStr) -> str:
    if not isinstance(lease_token, SecretStr):
        raise TypeError("lease_token must be SecretStr")
    return qmt_session_token_hash(lease_token)


def _validate_scope(
    *,
    account_id: str,
    gateway_holder_id: str,
    qmt_session_id: int,
    qmt_lease_generation: int,
) -> None:
    _require_nonblank(account_id, name="QMT callback logical account_id")
    if account_id != account_id.strip() or len(account_id) > 128:
        raise ValueError(
            "QMT callback logical account_id must be trimmed and at most 128 characters"
        )
    if _HOLDER_ID.fullmatch(gateway_holder_id) is None:
        raise ValueError("QMT callback gateway_holder_id must be a safe 1-64 character identifier")
    if (
        not isinstance(qmt_session_id, int)
        or isinstance(qmt_session_id, bool)
        or not 1 <= qmt_session_id <= 2_147_483_647
    ):
        raise ValueError("QMT callback qmt_session_id must be a positive 32-bit integer")
    if (
        not isinstance(qmt_lease_generation, int)
        or isinstance(qmt_lease_generation, bool)
        or qmt_lease_generation < 1
    ):
        raise ValueError("QMT callback qmt_lease_generation must be positive")


def _scope_parameters(
    callback: QmtSanitizedCallback,
    *,
    gateway_holder_id: str,
    qmt_session_id: int,
    qmt_lease_generation: int,
) -> dict[str, object]:
    return {
        "account_id": callback.account_id,
        "gateway_holder_id": gateway_holder_id,
        "local_sequence": callback.local_sequence,
        "qmt_lease_generation": qmt_lease_generation,
        "qmt_session_id": qmt_session_id,
    }


def _event_parameters(event: QmtCallbackInboxEvent) -> dict[str, object]:
    callback = event.callback
    return {
        "account_id": callback.account_id,
        "broker_session_date": callback.broker_session_date,
        "callback_payload_hash": callback.payload_hash,
        "event_hash": event.event_hash,
        "event_payload": _json(event.payload()),
        "event_version": event.version,
        "gateway_holder_id": event.gateway_holder_id,
        "kind": callback.kind.value,
        "local_sequence": callback.local_sequence,
        "previous_hash": event.previous_hash,
        "qmt_lease_generation": event.qmt_lease_generation,
        "qmt_session_id": event.qmt_session_id,
        "received_at": callback.received_at,
        "redacted_payload": _json(dict(callback.redacted_payload)),
    }


def _event_from_row(row: RowMapping) -> QmtCallbackInboxEvent:
    callback = QmtSanitizedCallback(
        account_id=str(row["account_id"]),
        local_sequence=int(row["local_sequence"]),
        kind=QmtCallbackKind(str(row["kind"])),
        received_at=row["received_at"],
        broker_session_date=row["broker_session_date"],
        redacted_payload=dict(row["redacted_payload"]),
    )
    event = QmtCallbackInboxEvent(
        callback=callback,
        gateway_holder_id=str(row["gateway_holder_id"]),
        qmt_session_id=int(row["qmt_session_id"]),
        qmt_lease_generation=int(row["qmt_lease_generation"]),
        previous_hash=str(row["previous_hash"]),
        version=str(row["event_version"]),
    )
    if (
        str(row["callback_payload_hash"]) != callback.payload_hash
        or str(row["event_hash"]) != event.event_hash
        or row["broker_mutation_allowed"] is not False
        or dict(row["event_payload"]) != event.payload()
    ):
        raise PersistenceUnavailableError("QMT callback inbox row failed integrity verification")
    return event


def _lease_matches(
    row: RowMapping | None,
    *,
    callback: QmtSanitizedCallback,
    gateway_holder_id: str,
    qmt_lease_generation: int,
    token_hash: str,
) -> bool:
    return bool(
        _scope_lease_matches(
            row,
            gateway_holder_id=gateway_holder_id,
            qmt_lease_generation=qmt_lease_generation,
            token_hash=token_hash,
        )
        and row is not None
        and callback.broker_session_date == row["observed_at"].astimezone(SHANGHAI).date()
        and callback.received_at >= row["acquired_at"]
    )


def _scope_lease_matches(
    row: RowMapping | None,
    *,
    gateway_holder_id: str,
    qmt_lease_generation: int,
    token_hash: str,
) -> bool:
    return bool(
        row is not None
        and str(row["holder_id"]) == gateway_holder_id
        and str(row["token_hash"]) == token_hash
        and int(row["generation"]) == qmt_lease_generation
        and row["released_at"] is None
        and row["expires_at"] > row["observed_at"]
        and row["acquired_at"].astimezone(SHANGHAI).date()
        == row["observed_at"].astimezone(SHANGHAI).date()
    )


def _json(payload: dict[str, object]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))
