from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    create_async_engine,
)

from autoquant.backtest.validation import SmaParameters
from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _datetime_text,
    _require_nonblank,
)
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.models import ZERO_HASH
from autoquant.execution.validated_sma import (
    ValidatedSmaRegistration,
    select_deployment_parameters,
)
from autoquant.execution.validated_sma_portfolio import (
    ValidatedSmaPortfolioRegistration,
)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class PortfolioActivationAction(StrEnum):
    APPROVE = "approve"
    REVOKE = "revoke"


class PostgresPaperPortfolioRegistry:
    """Append-only registry for independently validated paper components."""

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
    ) -> PostgresPaperPortfolioRegistry:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "PostgreSQL paper portfolio registry connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def approve(
        self,
        registration: ValidatedSmaPortfolioRegistration,
    ) -> ValidatedSmaPortfolioRegistration:
        if not isinstance(
            registration,
            ValidatedSmaPortfolioRegistration,
        ):
            raise TypeError(
                "portfolio registry requires a validated portfolio"
            )
        try:
            async with self._engine.begin() as connection:
                await self._lock(
                    connection,
                    account_id=registration.account_id,
                    strategy_id=registration.strategy_id,
                )
                await self._require_single_strategy_inactive(
                    connection,
                    account_id=registration.account_id,
                    strategy_id=registration.strategy_id,
                )
                await self._verify_valuation_manifest(
                    connection,
                    registration=registration,
                )
                for component in registration.components:
                    await self._verify_component_candidate(
                        connection,
                        component=component,
                    )
                await self._insert_registration(
                    connection,
                    registration=registration,
                )
                latest = await self._latest_event(
                    connection,
                    account_id=registration.account_id,
                    strategy_id=registration.strategy_id,
                )
                if (
                    latest is not None
                    and str(latest["action"]) == "approve"
                ):
                    if (
                        str(latest["registration_hash"])
                        == registration.registration_hash
                    ):
                        return registration
                    raise ValueError(
                        "active portfolio must be revoked before replacement"
                    )
                await self._append_event(
                    connection,
                    registration=registration,
                    action=PortfolioActivationAction.APPROVE,
                    actor=registration.approved_by,
                    reason="oos_components_approved_for_paper",
                    occurred_at=registration.approved_at,
                    latest=latest,
                )
                stored = await self._read_registration(
                    connection,
                    registration_hash=registration.registration_hash,
                )
                if stored != registration:
                    raise PersistenceUnavailableError(
                        "stored portfolio failed integrity verification"
                    )
                return stored
        except (TypeError, ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "paper portfolio approval persistence failed"
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
        instant = to_utc(
            revoked_at,
            name="portfolio revocation time",
        )
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
                if (
                    latest is None
                    or str(latest["action"]) != "approve"
                ):
                    raise LookupError("paper portfolio is not active")
                if instant < latest["occurred_at"]:
                    raise ValueError(
                        "portfolio revocation cannot precede approval"
                    )
                registration = await self._read_registration(
                    connection,
                    registration_hash=str(
                        latest["registration_hash"]
                    ),
                )
                await self._append_event(
                    connection,
                    registration=registration,
                    action=PortfolioActivationAction.REVOKE,
                    actor=revoked_by,
                    reason=reason,
                    occurred_at=instant,
                    latest=latest,
                )
        except (
            LookupError,
            TypeError,
            ValueError,
            PersistenceUnavailableError,
        ):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "paper portfolio revocation persistence failed"
            ) from None

    async def active(
        self,
        *,
        account_id: str,
        strategy_id: str,
    ) -> ValidatedSmaPortfolioRegistration | None:
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
                                FROM {self._schema}.paper_portfolio_activation_events
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
                self._verify_event_chain(
                    events,
                    account_id=account_id,
                    strategy_id=strategy_id,
                )
                if (
                    not events
                    or str(events[-1]["action"]) == "revoke"
                ):
                    return None
                registration = await self._read_registration(
                    connection,
                    registration_hash=str(
                        events[-1]["registration_hash"]
                    ),
                )
                if (
                    registration.account_id != account_id
                    or registration.strategy_id != strategy_id
                ):
                    raise PersistenceUnavailableError(
                        "active portfolio does not match its event"
                    )
                return registration
        except PersistenceUnavailableError:
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "paper portfolio registry read failed"
            ) from None

    async def _insert_registration(
        self,
        connection: AsyncConnection,
        *,
        registration: ValidatedSmaPortfolioRegistration,
    ) -> None:
        payload = registration.artifact_payload()
        await connection.execute(
            text(
                f"""
                INSERT INTO {self._schema}.paper_portfolio_registrations
                    (registration_hash, account_id, strategy_id,
                     strategy_version, valuation_manifest_hash,
                     valuation_manifest_as_of, component_count,
                     total_allocation, risk_policy_hash, approved_by,
                     approved_at, execution_mode, portfolio_version,
                     artifact_payload)
                VALUES
                    (:registration_hash, :account_id, :strategy_id,
                     :strategy_version, :valuation_manifest_hash,
                     :valuation_manifest_as_of, :component_count,
                     :total_allocation, :risk_policy_hash, :approved_by,
                     :approved_at, 'paper', :portfolio_version,
                     CAST(:artifact_payload AS jsonb))
                ON CONFLICT (registration_hash) DO NOTHING
                """
            ),
            {
                **payload,
                "registration_hash": registration.registration_hash,
                "valuation_manifest_as_of": (
                    registration.valuation_manifest_as_of
                ),
                "component_count": len(registration.components),
                "total_allocation": registration.total_allocation,
                "approved_at": registration.approved_at,
                "artifact_payload": _json(payload),
            },
        )
        for component in registration.components:
            await connection.execute(
                text(
                    f"""
                    INSERT INTO {self._schema}.paper_portfolio_components
                        (component_hash, portfolio_registration_hash,
                         instrument, experiment_id,
                         validation_result_hash,
                         validation_manifest_hash, signal_manifest_hash,
                         signal_manifest_as_of, selected_fast,
                         selected_slow, allocation, slippage_bps,
                         rule_version, component_payload)
                    VALUES
                        (:component_hash, :portfolio_registration_hash,
                         :instrument, :experiment_id,
                         :validation_result_hash,
                         :validation_manifest_hash, :signal_manifest_hash,
                         :signal_manifest_as_of, :selected_fast,
                         :selected_slow, :allocation, :slippage_bps,
                         :rule_version, CAST(:component_payload AS jsonb))
                    ON CONFLICT (component_hash) DO NOTHING
                    """
                ),
                {
                    "component_hash": component.registration_hash,
                    "portfolio_registration_hash": (
                        registration.registration_hash
                    ),
                    "instrument": component.instrument,
                    "experiment_id": component.experiment_id,
                    "validation_result_hash": (
                        component.validation_result_hash
                    ),
                    "validation_manifest_hash": (
                        component.validation_manifest_hash
                    ),
                    "signal_manifest_hash": (
                        component.signal_manifest_hash
                    ),
                    "signal_manifest_as_of": (
                        component.signal_manifest_as_of
                    ),
                    "selected_fast": component.fast_sessions,
                    "selected_slow": component.slow_sessions,
                    "allocation": component.allocation,
                    "slippage_bps": component.slippage_bps,
                    "rule_version": component.rule_version,
                    "component_payload": _json(
                        component.artifact_payload()
                    ),
                },
            )

    async def _read_registration(
        self,
        connection: AsyncConnection,
        *,
        registration_hash: str,
    ) -> ValidatedSmaPortfolioRegistration:
        parent = (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT *
                        FROM {self._schema}.paper_portfolio_registrations
                        WHERE registration_hash = :registration_hash
                        """
                    ),
                    {"registration_hash": registration_hash},
                )
            )
            .mappings()
            .one_or_none()
        )
        if parent is None:
            raise PersistenceUnavailableError(
                "portfolio registration artifact is missing"
            )
        components = (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT *
                        FROM {self._schema}.paper_portfolio_components
                        WHERE portfolio_registration_hash = :registration_hash
                        ORDER BY instrument
                        """
                    ),
                    {"registration_hash": registration_hash},
                )
            )
            .mappings()
            .all()
        )
        try:
            component_values = tuple(
                ValidatedSmaRegistration(
                    account_id=str(parent["account_id"]),
                    strategy_id=str(parent["strategy_id"]),
                    strategy_version=str(
                        _object(row["component_payload"])[
                            "strategy_version"
                        ]
                    ),
                    experiment_id=UUID(str(row["experiment_id"])),
                    validation_result_hash=str(
                        row["validation_result_hash"]
                    ),
                    validation_manifest_hash=str(
                        row["validation_manifest_hash"]
                    ),
                    signal_manifest_hash=str(
                        row["signal_manifest_hash"]
                    ),
                    signal_manifest_as_of=to_utc(
                        row["signal_manifest_as_of"]
                    ),
                    instrument=str(row["instrument"]),
                    fast_sessions=int(row["selected_fast"]),
                    slow_sessions=int(row["selected_slow"]),
                    allocation=Decimal(str(row["allocation"])),
                    slippage_bps=Decimal(str(row["slippage_bps"])),
                    risk_policy_hash=str(parent["risk_policy_hash"]),
                    rule_version=str(row["rule_version"]),
                    approved_by=str(parent["approved_by"]),
                    approved_at=to_utc(parent["approved_at"]),
                )
                for row in components
            )
            registration = ValidatedSmaPortfolioRegistration(
                account_id=str(parent["account_id"]),
                strategy_id=str(parent["strategy_id"]),
                strategy_version=str(parent["strategy_version"]),
                components=component_values,
                valuation_manifest_hash=str(
                    parent["valuation_manifest_hash"]
                ),
                valuation_manifest_as_of=to_utc(
                    parent["valuation_manifest_as_of"]
                ),
                risk_policy_hash=str(parent["risk_policy_hash"]),
                approved_by=str(parent["approved_by"]),
                approved_at=to_utc(parent["approved_at"]),
                execution_mode=str(parent["execution_mode"]),
                portfolio_version=str(parent["portfolio_version"]),
            )
            if (
                registration.registration_hash
                != str(parent["registration_hash"])
                or registration.artifact_payload()
                != _object(parent["artifact_payload"])
                or len(component_values)
                != int(parent["component_count"])
                or registration.total_allocation
                != Decimal(str(parent["total_allocation"]))
            ):
                raise ValueError(
                    "portfolio registration columns do not match payload"
                )
            for component, row in zip(
                component_values,
                components,
                strict=True,
            ):
                if (
                    component.registration_hash
                    != str(row["component_hash"])
                    or component.artifact_payload()
                    != _object(row["component_payload"])
                ):
                    raise ValueError(
                        "portfolio component failed integrity verification"
                    )
            return registration
        except (KeyError, TypeError, ValueError):
            raise PersistenceUnavailableError(
                "stored portfolio registration is invalid"
            ) from None

    async def _verify_valuation_manifest(
        self,
        connection: AsyncConnection,
        *,
        registration: ValidatedSmaPortfolioRegistration,
    ) -> None:
        row = (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT as_of, production_complete, payload
                        FROM {self._schema}.dataset_manifests
                        WHERE manifest_hash = :manifest_hash
                        """
                    ),
                    {
                        "manifest_hash": (
                            registration.valuation_manifest_hash
                        )
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        payload = None if row is None else _object(row["payload"])
        if (
            row is None
            or not bool(row["production_complete"])
            or to_utc(row["as_of"])
            != registration.valuation_manifest_as_of
            or payload is None
            or tuple(sorted(_string_tuple(payload, "instruments")))
            != registration.instruments
        ):
            raise ValueError(
                "portfolio valuation manifest is not exact and complete"
            )

    async def _verify_component_candidate(
        self,
        connection: AsyncConnection,
        *,
        component: ValidatedSmaRegistration,
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
                    {"experiment_id": component.experiment_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if experiment is None:
            raise ValueError(
                "portfolio component experiment does not exist"
            )
        request = _object(experiment["request_payload"])
        summary = _object(experiment["summary_payload"])
        folds = (
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
                    {"experiment_id": component.experiment_id},
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
                for row in folds
            )
        )
        signal_manifest = (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT as_of, production_complete, payload
                        FROM {self._schema}.dataset_manifests
                        WHERE manifest_hash = :manifest_hash
                        """
                    ),
                    {"manifest_hash": component.signal_manifest_hash},
                )
            )
            .mappings()
            .one_or_none()
        )
        signal_payload = (
            None
            if signal_manifest is None
            else _object(signal_manifest["payload"])
        )
        if (
            str(experiment["state"]) != "completed"
            or str(experiment["result_hash"])
            != component.validation_result_hash
            or str(experiment["manifest_hash"])
            != component.validation_manifest_hash
            or summary.get("evidence_status")
            != "research_candidate"
            or _string_tuple(summary, "gate_failures")
            or request.get("instrument") != component.instrument
            or Decimal(str(request.get("allocation")))
            != component.allocation
            or Decimal(str(request.get("slippage_bps")))
            != component.slippage_bps
            or (
                selected.fast_sessions,
                selected.slow_sessions,
            )
            != (
                component.fast_sessions,
                component.slow_sessions,
            )
            or signal_manifest is None
            or not bool(signal_manifest["production_complete"])
            or to_utc(signal_manifest["as_of"])
            != component.signal_manifest_as_of
            or signal_payload is None
            or _string_tuple(signal_payload, "instruments")
            != (component.instrument,)
        ):
            raise ValueError(
                "portfolio component does not match a gate-passing candidate"
            )

    async def _require_single_strategy_inactive(
        self,
        connection: AsyncConnection,
        *,
        account_id: str,
        strategy_id: str,
    ) -> None:
        action = await connection.scalar(
            text(
                f"""
                SELECT action
                FROM {self._schema}.paper_strategy_activation_events
                WHERE account_id = :account_id
                  AND strategy_id = :strategy_id
                ORDER BY sequence DESC
                LIMIT 1
                """
            ),
            {
                "account_id": account_id,
                "strategy_id": strategy_id,
            },
        )
        if action == "approve":
            raise ValueError(
                "single strategy must be revoked before portfolio approval"
            )

    async def _append_event(
        self,
        connection: AsyncConnection,
        *,
        registration: ValidatedSmaPortfolioRegistration,
        action: PortfolioActivationAction,
        actor: str,
        reason: str,
        occurred_at: datetime,
        latest: RowMapping | None,
    ) -> None:
        sequence = 1 if latest is None else int(latest["sequence"]) + 1
        previous_hash = (
            ZERO_HASH if latest is None else str(latest["event_hash"])
        )
        registration_hash = (
            registration.registration_hash
            if action is PortfolioActivationAction.APPROVE
            else None
        )
        payload = {
            "account_id": registration.account_id,
            "action": action.value,
            "actor": actor,
            "occurred_at": _datetime_text(occurred_at),
            "previous_hash": previous_hash,
            "reason": reason,
            "registration_hash": registration_hash,
            "sequence": sequence,
            "strategy_id": registration.strategy_id,
            "version": "paper-portfolio-activation-v1",
        }
        await connection.execute(
            text(
                f"""
                INSERT INTO {self._schema}.paper_portfolio_activation_events
                    (event_hash, account_id, strategy_id, sequence,
                     action, registration_hash, actor, reason,
                     occurred_at, previous_hash, event_payload)
                VALUES
                    (:event_hash, :account_id, :strategy_id, :sequence,
                     :action, :registration_hash, :actor, :reason,
                     :occurred_at, :previous_hash,
                     CAST(:event_payload AS jsonb))
                """
            ),
            {
                **payload,
                "event_hash": _canonical_hash(payload),
                "occurred_at": occurred_at,
                "event_payload": _json(payload),
            },
        )

    async def _latest_event(
        self,
        connection: AsyncConnection,
        *,
        account_id: str,
        strategy_id: str,
    ) -> RowMapping | None:
        events = (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT *
                        FROM {self._schema}.paper_portfolio_activation_events
                        WHERE account_id = :account_id
                          AND strategy_id = :strategy_id
                        ORDER BY sequence
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
            .all()
        )
        self._verify_event_chain(
            events,
            account_id=account_id,
            strategy_id=strategy_id,
        )
        return None if not events else events[-1]

    async def _lock(
        self,
        connection: AsyncConnection,
        *,
        account_id: str,
        strategy_id: str,
    ) -> None:
        await connection.execute(
            text(
                "SELECT pg_advisory_xact_lock("
                "hashtextextended(:key, 0))"
            ),
            {
                "key": (
                    f"paper-deployment:{account_id}:{strategy_id}"
                )
            },
        )

    @staticmethod
    def _verify_event_chain(
        events: Sequence[RowMapping],
        *,
        account_id: str,
        strategy_id: str,
    ) -> None:
        previous_hash = ZERO_HASH
        for expected_sequence, row in enumerate(events, start=1):
            payload = _object(row["event_payload"])
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
                or _canonical_hash(payload) != str(row["event_hash"])
                or payload.get("account_id") != account_id
                or payload.get("strategy_id") != strategy_id
                or payload.get("action") != str(row["action"])
                or payload.get("actor") != str(row["actor"])
                or payload.get("reason") != str(row["reason"])
                or payload.get("registration_hash")
                != registration_hash
                or payload.get("previous_hash") != previous_hash
                or payload.get("occurred_at")
                != _datetime_text(to_utc(row["occurred_at"]))
                or int(str(payload.get("sequence")))
                != expected_sequence
                or (
                    str(row["action"]) == "approve"
                    and registration_hash is None
                )
                or (
                    str(row["action"]) == "revoke"
                    and registration_hash is not None
                )
            ):
                raise PersistenceUnavailableError(
                    "portfolio activation chain failed integrity verification"
                )
            previous_hash = str(row["event_hash"])


def _json(value: Mapping[str, object]) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    )


def _object(raw: object) -> dict[str, object]:
    value = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(value, dict):
        raise TypeError("stored payload must be an object")
    return value


def _string_tuple(
    payload: Mapping[str, object],
    key: str,
) -> tuple[str, ...]:
    raw = payload.get(key)
    if not isinstance(raw, (list, tuple)):
        raise TypeError(f"{key} must be an array")
    if any(not isinstance(item, str) for item in raw):
        raise TypeError(f"{key} must contain strings")
    return tuple(raw)
