from __future__ import annotations

import json
import re
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from autoquant.clock import to_utc
from autoquant.data.models import _canonical_hash, _decimal_text
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.session_risk import (
    PaperSessionRiskState,
    SessionRiskObservation,
    apply_session_observation,
    observation_payload,
    state_payload,
)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_STATE_COLUMNS = """
account_id, session_date, day_start_equity, peak_equity,
cumulative_turnover, as_of, latest_observation_hash, latest_snapshot_hash,
turnover_evidence_hash, version, last_event_hash, state_hash, state_payload,
updated_at
"""


class PostgresPaperSessionRiskRepository:
    """Hash-chained daily risk metrics derived from persisted account evidence."""

    def __init__(self, *, engine: AsyncEngine, schema: str = "public") -> None:
        if _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError("schema must be a safe PostgreSQL identifier")
        self._engine = engine
        self._schema = schema

    @classmethod
    def connect(cls, *, dsn: str, schema: str = "public") -> PostgresPaperSessionRiskRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "PostgreSQL paper session risk connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def initialize(self, observation: SessionRiskObservation) -> PaperSessionRiskState:
        return await self._mutate(observation=observation, initialize=True)

    async def observe(self, observation: SessionRiskObservation) -> PaperSessionRiskState:
        return await self._mutate(observation=observation, initialize=False)

    async def get(self, *, account_id: str, session_date: date) -> PaperSessionRiskState:
        try:
            async with self._engine.connect() as connection:
                row = await self._select_state(
                    connection,
                    account_id=account_id,
                    session_date=session_date,
                    for_update=False,
                )
        except Exception:
            raise PersistenceUnavailableError("Paper session risk state read failed") from None
        if row is None:
            raise LookupError("paper session risk state is not initialized")
        return _state_from_row(row)

    async def replay(self, *, account_id: str, session_date: date) -> PaperSessionRiskState:
        current = await self.get(
            account_id=account_id,
            session_date=session_date,
        )
        try:
            async with self._engine.connect() as connection:
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT event_hash, sequence, previous_hash,
                                       observation_hash, transition_state_hash,
                                       observation_payload
                                FROM {self._schema}.paper_session_risk_events
                                WHERE account_id = :account_id
                                  AND session_date = :session_date
                                ORDER BY sequence
                                """
                            ),
                            {
                                "account_id": account_id,
                                "session_date": session_date,
                            },
                        )
                    )
                    .mappings()
                    .all()
                )
        except Exception:
            raise PersistenceUnavailableError("Paper session risk replay read failed") from None
        replayed: PaperSessionRiskState | None = None
        for row in rows:
            observation = _observation_from_payload(row["observation_payload"])
            replayed, event = apply_session_observation(replayed, observation)
            if (
                event.event_hash != str(row["event_hash"])
                or event.sequence != int(row["sequence"])
                or event.previous_hash != str(row["previous_hash"])
                or observation.observation_hash != str(row["observation_hash"])
                or event.transition_state_hash != str(row["transition_state_hash"])
            ):
                raise PersistenceUnavailableError(
                    "Paper session risk event failed integrity verification"
                )
        if replayed is None or replayed != current:
            raise PersistenceUnavailableError(
                "Paper session risk state does not match event replay"
            )
        return replayed

    async def _mutate(
        self,
        *,
        observation: SessionRiskObservation,
        initialize: bool,
    ) -> PaperSessionRiskState:
        try:
            async with self._engine.begin() as connection:
                await _lock(
                    connection,
                    account_id=observation.account_id,
                    session_date=observation.session_date,
                )
                await self._verify_snapshot(connection, observation)
                row = await self._select_state(
                    connection,
                    account_id=observation.account_id,
                    session_date=observation.session_date,
                    for_update=True,
                )
                current = None if row is None else _state_from_row(row)
                if current is not None and (
                    current.latest_observation_hash == observation.observation_hash
                ):
                    return current
                if initialize and current is not None:
                    raise ValueError("paper session risk state is already initialized")
                if not initialize and current is None:
                    raise LookupError("paper session risk state is not initialized")
                state, event = apply_session_observation(current, observation)
                if current is None:
                    await connection.execute(
                        text(
                            f"""
                            INSERT INTO {self._schema}.paper_session_risk_state
                                (account_id, session_date, day_start_equity,
                                 peak_equity, cumulative_turnover, as_of,
                                 latest_observation_hash, latest_snapshot_hash,
                                 turnover_evidence_hash, version, last_event_hash,
                                 state_hash, state_payload, updated_at)
                            VALUES
                                (:account_id, :session_date, :day_start_equity,
                                 :peak_equity, :cumulative_turnover, :as_of,
                                 :latest_observation_hash, :latest_snapshot_hash,
                                 :turnover_evidence_hash, :version, :last_event_hash,
                                 :state_hash, CAST(:state_payload AS jsonb), :updated_at)
                            """
                        ),
                        _state_parameters(state),
                    )
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.paper_session_risk_events
                            (event_hash, account_id, session_date, sequence,
                             previous_hash, observation_hash,
                             transition_state_hash, observation_payload)
                        VALUES
                            (:event_hash, :account_id, :session_date, :sequence,
                             :previous_hash, :observation_hash,
                             :transition_state_hash,
                             CAST(:observation_payload AS jsonb))
                        """
                    ),
                    {
                        "event_hash": event.event_hash,
                        "account_id": observation.account_id,
                        "session_date": observation.session_date,
                        "sequence": event.sequence,
                        "previous_hash": event.previous_hash,
                        "observation_hash": observation.observation_hash,
                        "transition_state_hash": event.transition_state_hash,
                        "observation_payload": _json(observation_payload(observation)),
                    },
                )
                if current is not None:
                    result = await connection.execute(
                        text(
                            f"""
                            UPDATE {self._schema}.paper_session_risk_state
                            SET peak_equity = :peak_equity,
                                cumulative_turnover = :cumulative_turnover,
                                as_of = :as_of,
                                latest_observation_hash = :latest_observation_hash,
                                latest_snapshot_hash = :latest_snapshot_hash,
                                turnover_evidence_hash = :turnover_evidence_hash,
                                version = :version,
                                last_event_hash = :last_event_hash,
                                state_hash = :state_hash,
                                state_payload = CAST(:state_payload AS jsonb),
                                updated_at = :updated_at
                            WHERE account_id = :account_id
                              AND session_date = :session_date
                              AND state_hash = :previous_state_hash
                            """
                        ),
                        {
                            **_state_parameters(state),
                            "previous_state_hash": current.state_hash,
                        },
                    )
                    if result.rowcount != 1:
                        raise PersistenceUnavailableError(
                            "Paper session risk optimistic update failed"
                        )
                stored = await self._select_state(
                    connection,
                    account_id=observation.account_id,
                    session_date=observation.session_date,
                    for_update=False,
                )
                if stored is None or _state_from_row(stored) != state:
                    raise PersistenceUnavailableError(
                        "Stored paper session risk state failed verification"
                    )
                return state
        except (LookupError, ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError("Paper session risk update failed") from None

    async def _verify_snapshot(
        self,
        connection: AsyncConnection,
        observation: SessionRiskObservation,
    ) -> None:
        row = (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT account_id, as_of, payload
                        FROM {self._schema}.execution_account_snapshots
                        WHERE snapshot_hash = :snapshot_hash
                        """
                    ),
                    {"snapshot_hash": observation.snapshot_hash},
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise ValueError("session observation requires a persisted account snapshot")
        payload = _object(row["payload"])
        if (
            _canonical_hash(payload) != observation.snapshot_hash
            or str(row["account_id"]) != observation.account_id
            or to_utc(row["as_of"]) != observation.as_of
            or str(payload.get("equity")) != _decimal_text(observation.equity)
        ):
            raise PersistenceUnavailableError(
                "Session observation snapshot failed integrity verification"
            )

    async def _select_state(
        self,
        connection: AsyncConnection,
        *,
        account_id: str,
        session_date: date,
        for_update: bool,
    ) -> RowMapping | None:
        suffix = " FOR UPDATE" if for_update else ""
        return (
            (
                await connection.execute(
                    text(
                        f"SELECT {_STATE_COLUMNS} "
                        f"FROM {self._schema}.paper_session_risk_state "
                        "WHERE account_id = :account_id "
                        f"AND session_date = :session_date{suffix}"
                    ),
                    {
                        "account_id": account_id,
                        "session_date": session_date,
                    },
                )
            )
            .mappings()
            .one_or_none()
        )


async def _lock(connection: AsyncConnection, *, account_id: str, session_date: date) -> None:
    await connection.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
        {"lock_key": f"autoquant:paper-session:{account_id}:{session_date.isoformat()}"},
    )


def _state_from_row(row: RowMapping) -> PaperSessionRiskState:
    try:
        payload = _object(row["state_payload"])
        state = PaperSessionRiskState(
            account_id=str(payload["account_id"]),
            session_date=date.fromisoformat(str(payload["session_date"])),
            day_start_equity=Decimal(str(payload["day_start_equity"])),
            peak_equity=Decimal(str(payload["peak_equity"])),
            cumulative_turnover=Decimal(str(payload["cumulative_turnover"])),
            as_of=_datetime(payload["as_of"]),
            latest_observation_hash=str(payload["latest_observation_hash"]),
            latest_snapshot_hash=str(payload["latest_snapshot_hash"]),
            turnover_evidence_hash=str(payload["turnover_evidence_hash"]),
            version=int(str(payload["version"])),
            last_event_hash=str(payload["last_event_hash"]),
        )
        if (
            state.account_id != str(row["account_id"])
            or state.session_date != row["session_date"]
            or state.day_start_equity != Decimal(str(row["day_start_equity"]))
            or state.peak_equity != Decimal(str(row["peak_equity"]))
            or state.cumulative_turnover != Decimal(str(row["cumulative_turnover"]))
            or state.as_of != _datetime(row["as_of"])
            or state.latest_observation_hash != str(row["latest_observation_hash"])
            or state.latest_snapshot_hash != str(row["latest_snapshot_hash"])
            or state.turnover_evidence_hash != str(row["turnover_evidence_hash"])
            or state.version != int(row["version"])
            or state.last_event_hash != str(row["last_event_hash"])
            or state.state_hash != str(row["state_hash"])
            or state.as_of != _datetime(row["updated_at"])
        ):
            raise ValueError("paper session risk columns do not match payload")
        return state
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "Stored paper session risk state failed integrity verification"
        ) from None


def _observation_from_payload(raw: object) -> SessionRiskObservation:
    try:
        payload = _object(raw)
        observation = SessionRiskObservation(
            account_id=str(payload["account_id"]),
            session_date=date.fromisoformat(str(payload["session_date"])),
            as_of=_datetime(payload["as_of"]),
            equity=Decimal(str(payload["equity"])),
            cumulative_turnover=Decimal(str(payload["cumulative_turnover"])),
            snapshot_hash=str(payload["snapshot_hash"]),
            turnover_evidence_hash=str(payload["turnover_evidence_hash"]),
        )
        if _canonical_hash(payload) != observation.observation_hash:
            raise ValueError("session observation payload hash mismatch")
        return observation
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "Stored session observation failed integrity verification"
        ) from None


def _state_parameters(state: PaperSessionRiskState) -> dict[str, object]:
    return {
        "account_id": state.account_id,
        "session_date": state.session_date,
        "day_start_equity": _decimal_text(state.day_start_equity),
        "peak_equity": _decimal_text(state.peak_equity),
        "cumulative_turnover": _decimal_text(state.cumulative_turnover),
        "as_of": state.as_of,
        "latest_observation_hash": state.latest_observation_hash,
        "latest_snapshot_hash": state.latest_snapshot_hash,
        "turnover_evidence_hash": state.turnover_evidence_hash,
        "version": state.version,
        "last_event_hash": state.last_event_hash,
        "state_hash": state.state_hash,
        "state_payload": _json(state_payload(state)),
        "updated_at": state.as_of,
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
