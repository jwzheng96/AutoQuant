from __future__ import annotations

import json
import re
from datetime import datetime

from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.data.models import _canonical_hash, _require_nonblank
from autoquant.errors import (
    BrokerStateUnknownError,
    PersistenceUnavailableError,
    QmtSessionLeaseLostError,
)
from autoquant.execution.qmt_canary_contract import (
    QmtCanaryOrderCandidate,
    QmtOrderCorrelation,
    QmtOrderCorrelationBook,
)
from autoquant.execution.qmt_session_store import qmt_session_token_hash

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class PostgresQmtCanaryOrderLedger:
    """Append-only candidate and identity ledger; it cannot mutate a broker."""

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
    ) -> PostgresQmtCanaryOrderLedger:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError("QMT canary ledger connection failed") from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def check_connection(self) -> None:
        required = {
            "qmt_canary_order_candidates",
            "qmt_order_correlation_reservations",
            "qmt_order_correlation_bindings",
        }
        try:
            async with self._engine.connect() as connection:
                rows = (
                    await connection.scalars(
                        text(
                            """
                            SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = :schema
                              AND table_name = ANY(:tables)
                            """
                        ),
                        {"schema": self._schema, "tables": sorted(required)},
                    )
                ).all()
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
            raise PersistenceUnavailableError("QMT canary ledger schema check failed") from None
        if set(map(str, rows)) != required or not isinstance(version, int) or version < 37:
            raise PersistenceUnavailableError("QMT canary ledger schema v37 is unavailable")

    async def reserve(
        self,
        candidate: QmtCanaryOrderCandidate,
        *,
        lease_token: SecretStr,
        async_request_id: int,
        reserved_at: datetime,
    ) -> QmtOrderCorrelation:
        if not isinstance(candidate, QmtCanaryOrderCandidate):
            raise TypeError("candidate must be QmtCanaryOrderCandidate")
        token_hash = _lease_token_hash(lease_token)
        candidate.require_current(now=reserved_at)
        proposed = QmtOrderCorrelation(
            candidate_hash=candidate.candidate_hash,
            client_order_id=candidate.decision.order.client_order_id,
            async_request_id=async_request_id,
            reserved_at=reserved_at,
        )
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:identity))"),
                    {
                        "identity": (
                            f"qmt-canary:{candidate.account_id}:"
                            f"{candidate.gateway_holder_id}:{candidate.qmt_session_id}:"
                            f"{candidate.qmt_lease_generation}"
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
                            _candidate_parameters(candidate),
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if not _active_lease_matches(
                    lease_row,
                    candidate=candidate,
                    token_hash=token_hash,
                ):
                    raise QmtSessionLeaseLostError(
                        "QMT candidate requires its matching active bearer lease"
                    )
                assert lease_row is not None
                candidate.require_current(now=lease_row["observed_at"])
                candidate_rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.qmt_canary_order_candidates
                                WHERE candidate_hash = :candidate_hash
                                   OR client_order_id = :client_order_id
                                   OR risk_decision_hash = :risk_decision_hash
                                """
                            ),
                            _candidate_parameters(candidate),
                        )
                    )
                    .mappings()
                    .all()
                )
                if any(not _candidate_row_matches(row, candidate) for row in candidate_rows):
                    raise ValueError("QMT canary candidate identity conflicts")
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.qmt_canary_order_candidates
                            (candidate_hash, account_id, strategy_id,
                             gateway_holder_id, qmt_session_id,
                             qmt_lease_generation,
                             client_order_id, risk_decision_hash,
                             created_at, valid_until,
                             broker_mutation_allowed,
                             candidate_version, payload)
                        VALUES
                            (:candidate_hash, :account_id, :strategy_id,
                             :gateway_holder_id, :qmt_session_id,
                             :qmt_lease_generation,
                             :client_order_id, :risk_decision_hash,
                             :created_at, :valid_until, false,
                             :candidate_version, CAST(:payload AS jsonb))
                        ON CONFLICT (candidate_hash) DO NOTHING
                        """
                    ),
                    _candidate_parameters(candidate),
                )
                reservation_rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.qmt_order_correlation_reservations
                                WHERE candidate_hash = :candidate_hash
                                   OR client_order_id = :client_order_id
                                   OR (
                                       gateway_holder_id = :gateway_holder_id
                                       AND qmt_session_id = :qmt_session_id
                                       AND qmt_lease_generation =
                                           :qmt_lease_generation
                                       AND async_request_id = :async_request_id
                                   )
                                """
                            ),
                            _reservation_parameters(candidate, proposed),
                        )
                    )
                    .mappings()
                    .all()
                )
                if any(_reservation_from_row(row) != proposed for row in reservation_rows):
                    raise ValueError("QMT correlation reservation identity conflicts")
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO
                            {self._schema}.qmt_order_correlation_reservations
                            (correlation_hash, candidate_hash,
                             gateway_holder_id, qmt_session_id,
                             qmt_lease_generation, client_order_id,
                             async_request_id,
                             reserved_at, payload)
                        VALUES
                            (:correlation_hash, :candidate_hash,
                             :gateway_holder_id, :qmt_session_id,
                             :qmt_lease_generation, :client_order_id,
                             :async_request_id,
                             :reserved_at, CAST(:payload AS jsonb))
                        ON CONFLICT (correlation_hash) DO NOTHING
                        """
                    ),
                    _reservation_parameters(candidate, proposed),
                )
                stored_candidate = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.qmt_canary_order_candidates
                                WHERE candidate_hash = :candidate_hash
                                """
                            ),
                            {"candidate_hash": candidate.candidate_hash},
                        )
                    )
                    .mappings()
                    .one()
                )
                stored_reservation = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.qmt_order_correlation_reservations
                                WHERE candidate_hash = :candidate_hash
                                """
                            ),
                            {"candidate_hash": candidate.candidate_hash},
                        )
                    )
                    .mappings()
                    .one()
                )
            if not _candidate_row_matches(stored_candidate, candidate):
                raise PersistenceUnavailableError(
                    "stored QMT canary candidate failed integrity verification"
                )
            stored = _reservation_from_row(stored_reservation)
            if stored != proposed:
                raise PersistenceUnavailableError(
                    "stored QMT correlation reservation failed integrity verification"
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
            raise PersistenceUnavailableError(
                "QMT correlation reservation persistence failed"
            ) from None

    async def bind(
        self,
        *,
        gateway_holder_id: str,
        qmt_session_id: int,
        qmt_lease_generation: int,
        lease_token: SecretStr,
        async_request_id: int,
        broker_order_id: str,
        bound_at: datetime,
    ) -> QmtOrderCorrelation:
        _require_scope(
            gateway_holder_id=gateway_holder_id,
            qmt_session_id=qmt_session_id,
            qmt_lease_generation=qmt_lease_generation,
        )
        token_hash = _lease_token_hash(lease_token)
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:identity))"),
                    {
                        "identity": (
                            f"qmt-bind:{gateway_holder_id}:{qmt_session_id}:"
                            f"{qmt_lease_generation}:{async_request_id}"
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
                if not _active_scope_lease_matches(
                    lease_row,
                    gateway_holder_id=gateway_holder_id,
                    qmt_lease_generation=qmt_lease_generation,
                    token_hash=token_hash,
                ):
                    raise QmtSessionLeaseLostError(
                        "QMT order binding requires its matching active bearer lease"
                    )
                reservation_row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.qmt_order_correlation_reservations
                                WHERE gateway_holder_id = :gateway_holder_id
                                  AND qmt_session_id = :qmt_session_id
                                  AND qmt_lease_generation =
                                      :qmt_lease_generation
                                  AND async_request_id = :async_request_id
                                """
                            ),
                            {
                                "async_request_id": async_request_id,
                                "gateway_holder_id": gateway_holder_id,
                                "qmt_lease_generation": qmt_lease_generation,
                                "qmt_session_id": qmt_session_id,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if reservation_row is None:
                    raise BrokerStateUnknownError("QMT async response has no durable reservation")
                reservation = _reservation_from_row(reservation_row)
                proposed = QmtOrderCorrelation(
                    candidate_hash=reservation.candidate_hash,
                    client_order_id=reservation.client_order_id,
                    async_request_id=reservation.async_request_id,
                    reserved_at=reservation.reserved_at,
                    broker_order_id=broker_order_id,
                    bound_at=bound_at,
                )
                binding_rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT b.*, r.client_order_id, r.reserved_at
                                FROM
                                    {self._schema}.qmt_order_correlation_bindings b
                                JOIN
                                    {self._schema}.qmt_order_correlation_reservations r
                                  ON r.candidate_hash = b.candidate_hash
                                WHERE b.candidate_hash = :candidate_hash
                                   OR (
                                       b.gateway_holder_id =
                                           :gateway_holder_id
                                       AND b.qmt_session_id =
                                           :qmt_session_id
                                       AND b.qmt_lease_generation =
                                           :qmt_lease_generation
                                       AND (
                                           b.async_request_id =
                                               :async_request_id
                                           OR b.broker_order_id =
                                               :broker_order_id
                                       )
                                   )
                                """
                            ),
                            {
                                **_correlation_parameters(proposed),
                                "gateway_holder_id": gateway_holder_id,
                                "qmt_lease_generation": qmt_lease_generation,
                                "qmt_session_id": qmt_session_id,
                            },
                        )
                    )
                    .mappings()
                    .all()
                )
                if any(_binding_from_row(row) != proposed for row in binding_rows):
                    raise BrokerStateUnknownError(
                        "QMT durable order binding conflicts with broker response"
                    )
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO
                            {self._schema}.qmt_order_correlation_bindings
                            (correlation_hash, candidate_hash,
                             gateway_holder_id, qmt_session_id,
                             qmt_lease_generation, async_request_id,
                             broker_order_id,
                             bound_at, payload)
                        VALUES
                            (:correlation_hash, :candidate_hash,
                             :gateway_holder_id, :qmt_session_id,
                             :qmt_lease_generation, :async_request_id,
                             :broker_order_id,
                             :bound_at, CAST(:payload AS jsonb))
                        ON CONFLICT (correlation_hash) DO NOTHING
                        """
                    ),
                    {
                        **_correlation_parameters(proposed),
                        "gateway_holder_id": gateway_holder_id,
                        "qmt_lease_generation": qmt_lease_generation,
                        "qmt_session_id": qmt_session_id,
                    },
                )
                stored_row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT b.*, r.client_order_id, r.reserved_at
                                FROM
                                    {self._schema}.qmt_order_correlation_bindings b
                                JOIN
                                    {self._schema}.qmt_order_correlation_reservations r
                                  ON r.candidate_hash = b.candidate_hash
                                WHERE b.candidate_hash = :candidate_hash
                                """
                            ),
                            {"candidate_hash": proposed.candidate_hash},
                        )
                    )
                    .mappings()
                    .one()
                )
            stored = _binding_from_row(stored_row)
            if stored != proposed:
                raise PersistenceUnavailableError(
                    "stored QMT order binding failed integrity verification"
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
            raise PersistenceUnavailableError("QMT order binding persistence failed") from None

    async def restore_book(
        self,
        *,
        account_id: str,
        gateway_holder_id: str,
        qmt_session_id: int,
        qmt_lease_generation: int,
        lease_token: SecretStr,
    ) -> QmtOrderCorrelationBook:
        _require_nonblank(account_id, name="QMT canary account_id")
        _require_scope(
            gateway_holder_id=gateway_holder_id,
            qmt_session_id=qmt_session_id,
            qmt_lease_generation=qmt_lease_generation,
        )
        token_hash = _lease_token_hash(lease_token)
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
                if not _active_scope_lease_matches(
                    lease_row,
                    gateway_holder_id=gateway_holder_id,
                    qmt_lease_generation=qmt_lease_generation,
                    token_hash=token_hash,
                ):
                    raise QmtSessionLeaseLostError(
                        "QMT correlation recovery requires its matching active bearer lease"
                    )
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT
                                    r.correlation_hash AS reservation_hash,
                                    r.candidate_hash, r.client_order_id,
                                    r.async_request_id, r.reserved_at,
                                    r.payload AS reservation_payload,
                                    c.account_id,
                                    c.gateway_holder_id,
                                    c.qmt_session_id,
                                    c.qmt_lease_generation,
                                    c.broker_mutation_allowed,
                                    c.payload AS candidate_payload,
                                    b.correlation_hash AS binding_hash,
                                    b.broker_order_id, b.bound_at,
                                    b.payload AS binding_payload
                                FROM
                                    {self._schema}.qmt_order_correlation_reservations r
                                JOIN
                                    {self._schema}.qmt_canary_order_candidates c
                                  ON c.candidate_hash = r.candidate_hash
                                LEFT JOIN
                                    {self._schema}.qmt_order_correlation_bindings b
                                  ON b.candidate_hash = r.candidate_hash
                                WHERE c.account_id = :account_id
                                  AND c.gateway_holder_id = :gateway_holder_id
                                  AND c.qmt_session_id = :qmt_session_id
                                  AND c.qmt_lease_generation =
                                      :qmt_lease_generation
                                ORDER BY r.reserved_at, r.async_request_id
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
            correlations = tuple(_restored_from_row(row) for row in rows)
            return QmtOrderCorrelationBook.restore(correlations)
        except (
            TypeError,
            ValueError,
            BrokerStateUnknownError,
            QmtSessionLeaseLostError,
        ):
            raise
        except Exception:
            raise PersistenceUnavailableError("QMT order correlation recovery failed") from None


def _candidate_parameters(candidate: QmtCanaryOrderCandidate) -> dict[str, object]:
    payload = candidate.payload()
    return {
        "account_id": candidate.account_id,
        "candidate_hash": candidate.candidate_hash,
        "candidate_version": candidate.version,
        "client_order_id": candidate.decision.order.client_order_id,
        "created_at": candidate.created_at,
        "gateway_holder_id": candidate.gateway_holder_id,
        "payload": _json(payload),
        "qmt_lease_generation": candidate.qmt_lease_generation,
        "qmt_session_id": candidate.qmt_session_id,
        "risk_decision_hash": candidate.decision.decision_hash,
        "strategy_id": candidate.strategy_id,
        "valid_until": candidate.valid_until,
    }


def _candidate_row_matches(
    row: RowMapping,
    candidate: QmtCanaryOrderCandidate,
) -> bool:
    return (
        str(row["candidate_hash"]) == candidate.candidate_hash
        and str(row["account_id"]) == candidate.account_id
        and str(row["strategy_id"]) == candidate.strategy_id
        and str(row["gateway_holder_id"]) == candidate.gateway_holder_id
        and int(row["qmt_session_id"]) == candidate.qmt_session_id
        and int(row["qmt_lease_generation"]) == candidate.qmt_lease_generation
        and str(row["qmt_lease_action"]) == "acquire"
        and str(row["client_order_id"]) == candidate.decision.order.client_order_id
        and str(row["risk_decision_hash"]) == candidate.decision.decision_hash
        and row["created_at"] == candidate.created_at
        and row["valid_until"] == candidate.valid_until
        and row["broker_mutation_allowed"] is False
        and str(row["candidate_version"]) == candidate.version
        and dict(row["payload"]) == candidate.payload()
    )


def _active_lease_matches(
    row: RowMapping | None,
    *,
    candidate: QmtCanaryOrderCandidate,
    token_hash: str,
) -> bool:
    return bool(
        row is not None
        and int(row["session_id"]) == candidate.qmt_session_id
        and str(row["holder_id"]) == candidate.gateway_holder_id
        and str(row["token_hash"]) == token_hash
        and int(row["generation"]) == candidate.qmt_lease_generation
        and row["released_at"] is None
        and row["acquired_at"] <= candidate.created_at
        and row["expires_at"] > row["observed_at"]
    )


def _active_scope_lease_matches(
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
    )


def _lease_token_hash(lease_token: SecretStr) -> str:
    if not isinstance(lease_token, SecretStr):
        raise TypeError("lease_token must be SecretStr")
    return qmt_session_token_hash(lease_token)


def _correlation_parameters(
    correlation: QmtOrderCorrelation,
) -> dict[str, object]:
    return {
        **correlation.payload(),
        "bound_at": correlation.bound_at,
        "correlation_hash": correlation.correlation_hash,
        "payload": _json(correlation.payload()),
        "reserved_at": correlation.reserved_at,
    }


def _reservation_parameters(
    candidate: QmtCanaryOrderCandidate,
    correlation: QmtOrderCorrelation,
) -> dict[str, object]:
    return {
        **_correlation_parameters(correlation),
        "gateway_holder_id": candidate.gateway_holder_id,
        "qmt_lease_generation": candidate.qmt_lease_generation,
        "qmt_session_id": candidate.qmt_session_id,
    }


def _require_scope(
    *,
    gateway_holder_id: str,
    qmt_session_id: int,
    qmt_lease_generation: int,
) -> None:
    _require_nonblank(gateway_holder_id, name="QMT gateway_holder_id")
    for value, name in (
        (qmt_session_id, "qmt_session_id"),
        (qmt_lease_generation, "qmt_lease_generation"),
    ):
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            raise ValueError(f"{name} must be positive")


def _reservation_from_row(row: RowMapping) -> QmtOrderCorrelation:
    value = QmtOrderCorrelation(
        candidate_hash=str(row["candidate_hash"]),
        client_order_id=str(row["client_order_id"]),
        async_request_id=int(row["async_request_id"]),
        reserved_at=row["reserved_at"],
    )
    if (
        str(row["correlation_hash"]) != value.correlation_hash
        or dict(row["payload"]) != value.payload()
    ):
        raise PersistenceUnavailableError("QMT reservation row failed integrity verification")
    return value


def _binding_from_row(row: RowMapping) -> QmtOrderCorrelation:
    value = QmtOrderCorrelation(
        candidate_hash=str(row["candidate_hash"]),
        client_order_id=str(row["client_order_id"]),
        async_request_id=int(row["async_request_id"]),
        reserved_at=row["reserved_at"],
        broker_order_id=str(row["broker_order_id"]),
        bound_at=row["bound_at"],
    )
    if (
        str(row["correlation_hash"]) != value.correlation_hash
        or dict(row["payload"]) != value.payload()
    ):
        raise PersistenceUnavailableError("QMT binding row failed integrity verification")
    return value


def _restored_from_row(row: RowMapping) -> QmtOrderCorrelation:
    candidate_payload = dict(row["candidate_payload"])
    if (
        _canonical_hash(candidate_payload) != str(row["candidate_hash"])
        or row["broker_mutation_allowed"] is not False
        or candidate_payload.get("broker_mutation_allowed") is not False
        or candidate_payload.get("account_id") != str(row["account_id"])
        or candidate_payload.get("gateway_holder_id") != str(row["gateway_holder_id"])
        or candidate_payload.get("qmt_session_id") != int(row["qmt_session_id"])
        or candidate_payload.get("qmt_lease_generation") != int(row["qmt_lease_generation"])
    ):
        raise PersistenceUnavailableError("restored QMT candidate failed integrity verification")
    reservation = QmtOrderCorrelation(
        candidate_hash=str(row["candidate_hash"]),
        client_order_id=str(row["client_order_id"]),
        async_request_id=int(row["async_request_id"]),
        reserved_at=row["reserved_at"],
    )
    if (
        str(row["reservation_hash"]) != reservation.correlation_hash
        or dict(row["reservation_payload"]) != reservation.payload()
    ):
        raise PersistenceUnavailableError("restored QMT reservation failed integrity verification")
    if row["binding_hash"] is None:
        if (
            row["broker_order_id"] is not None
            or row["bound_at"] is not None
            or row["binding_payload"] is not None
        ):
            raise PersistenceUnavailableError("restored QMT binding is structurally incomplete")
        return reservation
    binding = QmtOrderCorrelation(
        candidate_hash=reservation.candidate_hash,
        client_order_id=reservation.client_order_id,
        async_request_id=reservation.async_request_id,
        reserved_at=reservation.reserved_at,
        broker_order_id=str(row["broker_order_id"]),
        bound_at=row["bound_at"],
    )
    if (
        str(row["binding_hash"]) != binding.correlation_hash
        or dict(row["binding_payload"]) != binding.payload()
    ):
        raise PersistenceUnavailableError("restored QMT binding failed integrity verification")
    return binding


def _json(payload: dict[str, object]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))
