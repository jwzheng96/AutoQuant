from __future__ import annotations

import json
import re

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.backtest.codec import (
    decode_backtest_result,
    encode_backtest_result,
)
from autoquant.backtest.low_volatility_execution_compatibility import (
    LowVolatilityExecutionCompatibilityRun,
)
from autoquant.data.models import _require_lowercase_sha256
from autoquant.errors import PersistenceUnavailableError

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class PostgresLowVolatilityExecutionCompatibilityRunRepository:
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
    ) -> PostgresLowVolatilityExecutionCompatibilityRunRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(
                dsn,
                pool_pre_ping=True,
            )
        except Exception:
            raise PersistenceUnavailableError(
                "execution compatibility run connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def save(
        self,
        run: LowVolatilityExecutionCompatibilityRun,
    ) -> LowVolatilityExecutionCompatibilityRun:
        if not isinstance(
            run,
            LowVolatilityExecutionCompatibilityRun,
        ):
            raise TypeError("run must be low-volatility execution compatibility")
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.
                            low_volatility_execution_compatibility_runs
                            (run_hash, compatibility_spec_hash,
                             original_evaluation_result_hash,
                             corrected_forward_result_hash,
                             corrected_assessment_hash,
                             forward_spec_hash, source_spec_hash,
                             evaluation_dataset_manifest_hash,
                             panel_hash, compatibility_status,
                             execution_timing_compatible,
                             order_intent_invariance_verified,
                             strategy_rejected_order_count,
                             strategy_unresolved_position_count,
                             session_count, block_count,
                             original_evidence_status,
                             paper_activation_allowed,
                             runtime_activation_allowed,
                             live_trading_locked,
                             decision_order_policy_version,
                             completed_by, completed_at,
                             run_version, strategy_payload, payload)
                        VALUES
                            (:run_hash, :compatibility_spec_hash,
                             :original_evaluation_result_hash,
                             :corrected_forward_result_hash,
                             :corrected_assessment_hash,
                             :forward_spec_hash, :source_spec_hash,
                             :evaluation_dataset_manifest_hash,
                             :panel_hash, :compatibility_status,
                             :execution_timing_compatible, true,
                             :strategy_rejected_order_count,
                             :strategy_unresolved_position_count,
                             126, 6, 'paper_candidate',
                             false, false, true,
                             :decision_order_policy_version,
                             :completed_by, :completed_at,
                             :run_version,
                             CAST(:strategy_payload AS jsonb),
                             CAST(:payload AS jsonb))
                        ON CONFLICT (run_hash) DO NOTHING
                        """
                    ),
                    _parameters(run),
                )
        except Exception:
            raise PersistenceUnavailableError(
                "execution compatibility run persistence failed"
            ) from None
        stored = await self.for_spec(run.compatibility_spec_hash)
        if stored != run:
            raise ValueError("a different execution compatibility run is stored")
        return stored

    async def read(
        self,
        run_hash: str,
    ) -> LowVolatilityExecutionCompatibilityRun:
        _require_lowercase_sha256(
            run_hash,
            name="execution compatibility run hash",
        )
        row = await self._row(
            "run_hash = :identity",
            run_hash,
        )
        if row is None:
            raise LookupError("execution compatibility run does not exist")
        return _run(row)

    async def for_spec(
        self,
        spec_hash: str,
    ) -> LowVolatilityExecutionCompatibilityRun:
        _require_lowercase_sha256(
            spec_hash,
            name="execution compatibility spec hash",
        )
        row = await self._row(
            "compatibility_spec_hash = :identity",
            spec_hash,
        )
        if row is None:
            raise LookupError("execution compatibility run does not exist")
        return _run(row)

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
                                    low_volatility_execution_compatibility_runs
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
            raise PersistenceUnavailableError("execution compatibility run lookup failed") from None


def _parameters(
    run: LowVolatilityExecutionCompatibilityRun,
) -> dict[str, object]:
    return {
        **run.payload(),
        "completed_at": run.completed_at,
        "payload": _json(run.payload()),
        "run_hash": run.run_hash,
        "run_version": run.version,
        "strategy_payload": _json(encode_backtest_result(run.strategy_result)),
    }


def _run(
    row: RowMapping,
) -> LowVolatilityExecutionCompatibilityRun:
    try:
        payload = _object(row["payload"])
        run = LowVolatilityExecutionCompatibilityRun.from_payload(
            payload,
            strategy_result=decode_backtest_result(_object(row["strategy_payload"])),
        )
        if (
            run.run_hash != str(row["run_hash"])
            or run.compatibility_spec_hash != str(row["compatibility_spec_hash"])
            or run.original_evaluation_result_hash != str(row["original_evaluation_result_hash"])
            or run.corrected_forward_result_hash != str(row["corrected_forward_result_hash"])
            or run.corrected_assessment_hash != str(row["corrected_assessment_hash"])
            or run.forward_spec_hash != str(row["forward_spec_hash"])
            or run.source_spec_hash != str(row["source_spec_hash"])
            or run.evaluation_dataset_manifest_hash != str(row["evaluation_dataset_manifest_hash"])
            or run.panel_hash != str(row["panel_hash"])
            or run.compatibility_status != str(row["compatibility_status"])
            or run.execution_timing_compatible is not row["execution_timing_compatible"]
            or row["order_intent_invariance_verified"] is not True
            or run.strategy_rejected_order_count != int(row["strategy_rejected_order_count"])
            or run.strategy_unresolved_position_count
            != int(row["strategy_unresolved_position_count"])
            or run.session_count != int(row["session_count"])
            or run.block_count != int(row["block_count"])
            or run.original_evidence_status != str(row["original_evidence_status"])
            or row["paper_activation_allowed"] is not False
            or row["runtime_activation_allowed"] is not False
            or row["live_trading_locked"] is not True
            or run.decision_order_policy_version != str(row["decision_order_policy_version"])
            or run.completed_by != str(row["completed_by"])
            or run.completed_at != row["completed_at"]
            or run.version != str(row["run_version"])
        ):
            raise ValueError("stored execution compatibility run mismatch")
        return run
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "stored execution compatibility run failed integrity"
        ) from None


def _object(value: object) -> dict[str, object]:
    raw = json.loads(value) if isinstance(value, str) else value
    if not isinstance(raw, dict):
        raise TypeError("execution compatibility payload is not an object")
    return {str(key): item for key, item in raw.items()}


def _json(payload: object) -> str:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
