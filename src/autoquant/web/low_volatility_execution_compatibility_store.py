from __future__ import annotations

import json
import re

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.backtest.low_volatility_execution_compatibility import (
    LowVolatilityExecutionCompatibilitySpec,
)
from autoquant.data.models import _require_lowercase_sha256
from autoquant.errors import PersistenceUnavailableError

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class PostgresLowVolatilityExecutionCompatibilityRepository:
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
    ) -> PostgresLowVolatilityExecutionCompatibilityRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(
                dsn,
                pool_pre_ping=True,
            )
        except Exception:
            raise PersistenceUnavailableError(
                "low-volatility compatibility connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def save(
        self,
        spec: LowVolatilityExecutionCompatibilitySpec,
    ) -> LowVolatilityExecutionCompatibilitySpec:
        if not isinstance(
            spec,
            LowVolatilityExecutionCompatibilitySpec,
        ):
            raise TypeError("spec must be low-volatility execution compatibility")
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.
                            low_volatility_execution_compatibility_specs
                            (spec_hash, source_spec_hash,
                             forward_spec_hash,
                             observed_forward_session_count,
                             partial_outcome_observed_before_freeze,
                             terminal_outcome_observed_before_freeze,
                             order_intent_invariance_required,
                             same_forward_window_required,
                             compatibility_can_only_disqualify,
                             historical_reclassification_allowed,
                             paper_activation_allowed,
                             live_trading_locked,
                             decision_order_policy_version,
                             research_execution_version,
                             frozen_by, frozen_at,
                             compatibility_version, payload)
                        VALUES
                            (:spec_hash, :source_spec_hash,
                             :forward_spec_hash,
                             :observed_forward_session_count,
                             :partial_outcome_observed_before_freeze,
                             false, true, true, true,
                             false, false, true,
                             :decision_order_policy_version,
                             :research_execution_version,
                             :frozen_by, :frozen_at,
                             :compatibility_version,
                             CAST(:payload AS jsonb))
                        ON CONFLICT (spec_hash) DO NOTHING
                        """
                    ),
                    _parameters(spec),
                )
        except Exception:
            raise PersistenceUnavailableError(
                "low-volatility compatibility persistence failed"
            ) from None
        stored = await self.for_forward_spec(spec.forward_spec_hash)
        if stored != spec:
            raise ValueError("a different compatibility spec is stored")
        return stored

    async def read(
        self,
        spec_hash: str,
    ) -> LowVolatilityExecutionCompatibilitySpec:
        _require_lowercase_sha256(
            spec_hash,
            name="low-volatility compatibility spec hash",
        )
        row = await self._row(
            "spec_hash = :identity",
            spec_hash,
        )
        if row is None:
            raise LookupError("low-volatility compatibility spec does not exist")
        return _spec(row)

    async def for_forward_spec(
        self,
        forward_spec_hash: str,
    ) -> LowVolatilityExecutionCompatibilitySpec:
        _require_lowercase_sha256(
            forward_spec_hash,
            name="low-volatility forward spec hash",
        )
        row = await self._row(
            "forward_spec_hash = :identity",
            forward_spec_hash,
        )
        if row is None:
            raise LookupError("low-volatility compatibility spec does not exist")
        return _spec(row)

    async def _row(
        self,
        predicate: str,
        identity: str,
    ) -> RowMapping | None:
        try:
            async with self._engine.connect() as connection:
                return (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.
                                    low_volatility_execution_compatibility_specs
                                WHERE {predicate}
                                """
                            ),
                            {"identity": identity},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "low-volatility compatibility lookup failed"
            ) from None


def _parameters(
    spec: LowVolatilityExecutionCompatibilitySpec,
) -> dict[str, object]:
    return {
        **spec.payload(),
        "compatibility_version": spec.version,
        "frozen_at": spec.frozen_at,
        "payload": json.dumps(
            spec.payload(),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ),
        "spec_hash": spec.spec_hash,
    }


def _spec(
    row: RowMapping,
) -> LowVolatilityExecutionCompatibilitySpec:
    try:
        raw = row["payload"]
        payload = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(payload, dict):
            raise TypeError("compatibility payload is not an object")
        spec = LowVolatilityExecutionCompatibilitySpec.from_payload(
            {str(key): value for key, value in payload.items()}
        )
        if (
            spec.spec_hash != str(row["spec_hash"])
            or spec.source_spec_hash != str(row["source_spec_hash"])
            or spec.forward_spec_hash != str(row["forward_spec_hash"])
            or spec.observed_forward_session_count != int(row["observed_forward_session_count"])
            or spec.partial_outcome_observed_before_freeze
            is not row["partial_outcome_observed_before_freeze"]
            or row["terminal_outcome_observed_before_freeze"] is not False
            or row["order_intent_invariance_required"] is not True
            or row["same_forward_window_required"] is not True
            or row["compatibility_can_only_disqualify"] is not True
            or row["historical_reclassification_allowed"] is not False
            or row["paper_activation_allowed"] is not False
            or row["live_trading_locked"] is not True
            or spec.decision_order_policy_version != str(row["decision_order_policy_version"])
            or spec.research_execution_version != str(row["research_execution_version"])
            or spec.frozen_by != str(row["frozen_by"])
            or spec.frozen_at != row["frozen_at"]
            or spec.version != str(row["compatibility_version"])
        ):
            raise ValueError("stored low-volatility compatibility mismatch")
        return spec
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "stored low-volatility compatibility failed integrity"
        ) from None
