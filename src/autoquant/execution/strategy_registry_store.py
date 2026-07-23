from __future__ import annotations

import json
import re
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from autoquant.backtest.validation import SmaParameters
from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _datetime_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.models import ZERO_HASH
from autoquant.execution.validated_sma import (
    ValidatedSmaRegistration,
    select_deployment_parameters,
)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class StrategyActivationAction(StrEnum):
    APPROVE = "approve"
    REVOKE = "revoke"


def _event_payload(
    *,
    account_id: str,
    strategy_id: str,
    sequence: int,
    action: StrategyActivationAction,
    registration_hash: str | None,
    actor: str,
    reason: str,
    occurred_at: datetime,
    previous_hash: str,
) -> dict[str, object]:
    return {
        "account_id": account_id,
        "action": action.value,
        "actor": actor,
        "occurred_at": _datetime_text(occurred_at),
        "previous_hash": previous_hash,
        "reason": reason,
        "registration_hash": registration_hash,
        "sequence": sequence,
        "strategy_id": strategy_id,
    }


class PostgresPaperStrategyRegistry:
    """Append-only approval/revocation chain for paper-only strategy artifacts."""

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
    ) -> PostgresPaperStrategyRegistry:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "PostgreSQL paper strategy registry connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def approve(
        self,
        registration: ValidatedSmaRegistration,
    ) -> ValidatedSmaRegistration:
        if (
            not isinstance(registration, ValidatedSmaRegistration)
            or registration.execution_mode != "paper"
        ):
            raise TypeError("registry only accepts paper strategy registrations")
        try:
            async with self._engine.begin() as connection:
                await self._lock(
                    connection,
                    account_id=registration.account_id,
                    strategy_id=registration.strategy_id,
                )
                await self._verify_research_candidate(
                    connection,
                    registration=registration,
                )
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.paper_strategy_registrations
                            (registration_hash, account_id, strategy_id,
                             strategy_version, experiment_id,
                             validation_result_hash, validation_manifest_hash,
                             signal_manifest_hash, signal_manifest_as_of,
                             instrument, selected_fast, selected_slow,
                             allocation, slippage_bps, risk_policy_hash,
                             rule_version, approved_by, approved_at,
                             execution_mode, parameter_selection_version,
                             signal_policy_version, artifact_payload)
                        VALUES
                            (:registration_hash, :account_id, :strategy_id,
                             :strategy_version, :experiment_id,
                             :validation_result_hash, :validation_manifest_hash,
                             :signal_manifest_hash, :signal_manifest_as_of,
                             :instrument, :selected_fast, :selected_slow,
                             :allocation, :slippage_bps, :risk_policy_hash,
                             :rule_version, :approved_by, :approved_at,
                             'paper', :parameter_selection_version,
                             :signal_policy_version,
                             CAST(:artifact_payload AS jsonb))
                        ON CONFLICT (registration_hash) DO NOTHING
                        """
                    ),
                    self._registration_parameters(registration),
                )
                stored_row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.paper_strategy_registrations
                                WHERE registration_hash = :registration_hash
                                """
                            ),
                            {"registration_hash": registration.registration_hash},
                        )
                    )
                    .mappings()
                    .one()
                )
                stored = self._registration_from_row(stored_row)
                if stored != registration:
                    raise PersistenceUnavailableError(
                        "stored strategy registration failed integrity verification"
                    )
                latest = await self._latest_event(
                    connection,
                    account_id=registration.account_id,
                    strategy_id=registration.strategy_id,
                )
                if latest is not None and str(latest["action"]) == "approve":
                    if (
                        str(latest["registration_hash"])
                        == registration.registration_hash
                    ):
                        return registration
                    raise ValueError(
                        "active paper strategy must be revoked before replacement"
                    )
                await self._append_event(
                    connection,
                    account_id=registration.account_id,
                    strategy_id=registration.strategy_id,
                    action=StrategyActivationAction.APPROVE,
                    registration_hash=registration.registration_hash,
                    actor=registration.approved_by,
                    reason="oos_candidate_approved_for_paper",
                    occurred_at=registration.approved_at,
                    latest=latest,
                )
                return registration
        except (TypeError, ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "paper strategy approval persistence failed"
            ) from None

    async def revoke(
        self,
        *,
        account_id: str,
        strategy_id: str,
        revoked_by: str,
        reason: str,
        revoked_at: datetime,
    ) -> None:
        for name, value in (
            ("account_id", account_id),
            ("strategy_id", strategy_id),
            ("revoked_by", revoked_by),
            ("reason", reason),
        ):
            _require_nonblank(value, name=name)
            if len(value) > 128:
                raise ValueError(f"{name} cannot exceed 128 characters")
        instant = to_utc(revoked_at, name="strategy revocation time")
        try:
            async with self._engine.begin() as connection:
                await self._lock(
                    connection,
                    account_id=account_id,
                    strategy_id=strategy_id,
                )
                latest = await self._latest_event(
                    connection,
                    account_id=account_id,
                    strategy_id=strategy_id,
                )
                if latest is None or str(latest["action"]) != "approve":
                    raise LookupError("paper strategy is not active")
                if instant < latest["occurred_at"]:
                    raise ValueError("revocation cannot precede approval")
                await self._append_event(
                    connection,
                    account_id=account_id,
                    strategy_id=strategy_id,
                    action=StrategyActivationAction.REVOKE,
                    registration_hash=None,
                    actor=revoked_by,
                    reason=reason,
                    occurred_at=instant,
                    latest=latest,
                )
        except (LookupError, TypeError, ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "paper strategy revocation persistence failed"
            ) from None

    async def active(
        self,
        *,
        account_id: str,
        strategy_id: str,
    ) -> ValidatedSmaRegistration | None:
        _require_nonblank(account_id, name="account_id")
        _require_nonblank(strategy_id, name="strategy_id")
        try:
            async with self._engine.connect() as connection:
                events = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.paper_strategy_activation_events
                                WHERE account_id = :account_id
                                  AND strategy_id = :strategy_id
                                ORDER BY sequence
                                """
                            ),
                            {
                                "account_id": account_id,
                                "strategy_id": strategy_id,
                            },
                        )
                    )
                    .mappings()
                    .all()
                )
                self._verify_events(
                    events,
                    account_id=account_id,
                    strategy_id=strategy_id,
                )
                if not events or str(events[-1]["action"]) == "revoke":
                    return None
                registration_hash = str(events[-1]["registration_hash"])
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.paper_strategy_registrations
                                WHERE registration_hash = :registration_hash
                                """
                            ),
                            {"registration_hash": registration_hash},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise PersistenceUnavailableError(
                        "active paper registration artifact is missing"
                    )
                registration = self._registration_from_row(row)
                if (
                    registration.account_id != account_id
                    or registration.strategy_id != strategy_id
                    or registration.registration_hash != registration_hash
                ):
                    raise PersistenceUnavailableError(
                        "active paper registration does not match its event"
                    )
                return registration
        except PersistenceUnavailableError:
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "paper strategy registry read failed"
            ) from None

    async def _append_event(
        self,
        connection: AsyncConnection,
        *,
        account_id: str,
        strategy_id: str,
        action: StrategyActivationAction,
        registration_hash: str | None,
        actor: str,
        reason: str,
        occurred_at: datetime,
        latest: RowMapping | None,
    ) -> None:
        sequence = 1 if latest is None else int(latest["sequence"]) + 1
        previous_hash = ZERO_HASH if latest is None else str(latest["event_hash"])
        payload = _event_payload(
            account_id=account_id,
            strategy_id=strategy_id,
            sequence=sequence,
            action=action,
            registration_hash=registration_hash,
            actor=actor,
            reason=reason,
            occurred_at=occurred_at,
            previous_hash=previous_hash,
        )
        event_hash = _canonical_hash(payload)
        await connection.execute(
            text(
                f"""
                INSERT INTO {self._schema}.paper_strategy_activation_events
                    (event_hash, account_id, strategy_id, sequence, action,
                     registration_hash, actor, reason, occurred_at,
                     previous_hash, event_payload)
                VALUES
                    (:event_hash, :account_id, :strategy_id, :sequence, :action,
                     :registration_hash, :actor, :reason, :occurred_at,
                     :previous_hash, CAST(:event_payload AS jsonb))
                """
            ),
            {
                **payload,
                "event_hash": event_hash,
                "occurred_at": occurred_at,
                "event_payload": json.dumps(
                    payload,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            },
        )

    async def _verify_research_candidate(
        self,
        connection: AsyncConnection,
        *,
        registration: ValidatedSmaRegistration,
    ) -> None:
        experiment = (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT state, result_hash, manifest_hash,
                               request_payload, summary_payload
                        FROM {self._schema}.validation_experiments
                        WHERE experiment_id = :experiment_id
                        """
                    ),
                    {"experiment_id": registration.experiment_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if experiment is None:
            raise ValueError("strategy registration experiment does not exist")
        raw_request = experiment["request_payload"]
        raw_summary = experiment["summary_payload"]
        request = (
            json.loads(raw_request)
            if isinstance(raw_request, str)
            else dict(raw_request)
        )
        summary = (
            json.loads(raw_summary)
            if isinstance(raw_summary, str)
            else dict(raw_summary)
        )
        fold_rows = (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT selected_fast, selected_slow
                        FROM {self._schema}.validation_folds
                        WHERE experiment_id = :experiment_id
                        ORDER BY sequence
                        """
                    ),
                    {"experiment_id": registration.experiment_id},
                )
            )
            .mappings()
            .all()
        )
        selected = select_deployment_parameters(
            tuple(
                SmaParameters(
                    int(row["selected_fast"]),
                    int(row["selected_slow"]),
                )
                for row in fold_rows
            )
        )
        if (
            str(experiment["state"]) != "completed"
            or str(experiment["result_hash"])
            != registration.validation_result_hash
            or str(experiment["manifest_hash"])
            != registration.validation_manifest_hash
            or summary.get("evidence_status") != "research_candidate"
            or tuple(summary.get("gate_failures", ()))
            or request.get("instrument") != registration.instrument
            or Decimal(str(request.get("allocation"))) != registration.allocation
            or Decimal(str(request.get("slippage_bps")))
            != registration.slippage_bps
            or (
                selected.fast_sessions,
                selected.slow_sessions,
            )
            != (
                registration.fast_sessions,
                registration.slow_sessions,
            )
        ):
            raise ValueError(
                "strategy registration does not match a gate-passing research candidate"
            )

    async def _latest_event(
        self,
        connection: AsyncConnection,
        *,
        account_id: str,
        strategy_id: str,
    ) -> RowMapping | None:
        return (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT *
                        FROM {self._schema}.paper_strategy_activation_events
                        WHERE account_id = :account_id
                          AND strategy_id = :strategy_id
                        ORDER BY sequence DESC
                        LIMIT 1
                        FOR UPDATE
                        """
                    ),
                    {
                        "account_id": account_id,
                        "strategy_id": strategy_id,
                    },
                )
            )
            .mappings()
            .one_or_none()
        )

    async def _lock(
        self,
        connection: AsyncConnection,
        *,
        account_id: str,
        strategy_id: str,
    ) -> None:
        await connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": f"paper-strategy:{account_id}:{strategy_id}"},
        )

    @staticmethod
    def _registration_parameters(
        value: ValidatedSmaRegistration,
    ) -> dict[str, object]:
        return {
            **value.artifact_payload(),
            "registration_hash": value.registration_hash,
            "artifact_payload": json.dumps(
                value.artifact_payload(),
                separators=(",", ":"),
                sort_keys=True,
            ),
            "allocation": value.allocation,
            "approved_at": value.approved_at,
            "experiment_id": value.experiment_id,
            "selected_fast": value.fast_sessions,
            "selected_slow": value.slow_sessions,
            "signal_manifest_as_of": value.signal_manifest_as_of,
            "slippage_bps": value.slippage_bps,
        }

    @staticmethod
    def _registration_from_row(row: RowMapping) -> ValidatedSmaRegistration:
        try:
            value = ValidatedSmaRegistration(
                account_id=str(row["account_id"]),
                strategy_id=str(row["strategy_id"]),
                strategy_version=str(row["strategy_version"]),
                experiment_id=UUID(str(row["experiment_id"])),
                validation_result_hash=str(row["validation_result_hash"]),
                validation_manifest_hash=str(row["validation_manifest_hash"]),
                signal_manifest_hash=str(row["signal_manifest_hash"]),
                signal_manifest_as_of=row["signal_manifest_as_of"],
                instrument=str(row["instrument"]),
                fast_sessions=int(row["selected_fast"]),
                slow_sessions=int(row["selected_slow"]),
                allocation=row["allocation"],
                slippage_bps=row["slippage_bps"],
                risk_policy_hash=str(row["risk_policy_hash"]),
                rule_version=str(row["rule_version"]),
                approved_by=str(row["approved_by"]),
                approved_at=row["approved_at"],
                execution_mode=str(row["execution_mode"]),
                parameter_selection_version=str(
                    row["parameter_selection_version"]
                ),
                signal_policy_version=str(row["signal_policy_version"]),
            )
            payload = (
                json.loads(row["artifact_payload"])
                if isinstance(row["artifact_payload"], str)
                else dict(row["artifact_payload"])
            )
            if (
                value.registration_hash != str(row["registration_hash"])
                or value.artifact_payload() != payload
            ):
                raise ValueError("strategy registration hash mismatch")
            return value
        except (KeyError, TypeError, ValueError):
            raise PersistenceUnavailableError(
                "stored paper strategy registration is malformed"
            ) from None

    @staticmethod
    def _verify_events(
        events: Sequence[RowMapping],
        *,
        account_id: str,
        strategy_id: str,
    ) -> None:
        previous_hash = ZERO_HASH
        expected_sequence = 1
        active = False
        for row in events:
            action = StrategyActivationAction(str(row["action"]))
            occurred_at = to_utc(row["occurred_at"])
            registration_hash = (
                None
                if row["registration_hash"] is None
                else str(row["registration_hash"])
            )
            if (
                str(row["account_id"]) != account_id
                or str(row["strategy_id"]) != strategy_id
                or int(row["sequence"]) != expected_sequence
                or str(row["previous_hash"]) != previous_hash
                or (action is StrategyActivationAction.APPROVE) == active
            ):
                raise PersistenceUnavailableError(
                    "paper strategy activation chain is inconsistent"
                )
            if action is StrategyActivationAction.APPROVE:
                if registration_hash is None:
                    raise PersistenceUnavailableError(
                        "approval event is missing a registration"
                    )
                active = True
            else:
                if registration_hash is not None:
                    raise PersistenceUnavailableError(
                        "revocation event must not contain a registration"
                    )
                active = False
            payload = _event_payload(
                account_id=account_id,
                strategy_id=strategy_id,
                sequence=expected_sequence,
                action=action,
                registration_hash=registration_hash,
                actor=str(row["actor"]),
                reason=str(row["reason"]),
                occurred_at=occurred_at,
                previous_hash=previous_hash,
            )
            stored_payload = (
                json.loads(row["event_payload"])
                if isinstance(row["event_payload"], str)
                else dict(row["event_payload"])
            )
            event_hash = _canonical_hash(payload)
            _require_lowercase_sha256(str(row["event_hash"]), name="event_hash")
            if (
                stored_payload != payload
                or str(row["event_hash"]) != event_hash
            ):
                raise PersistenceUnavailableError(
                    "paper strategy activation event failed integrity verification"
                )
            previous_hash = event_hash
            expected_sequence += 1
