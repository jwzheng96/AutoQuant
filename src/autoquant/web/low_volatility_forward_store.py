from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.backtest.low_volatility_forward import (
    LowVolatilityForwardEvidenceSpec,
)
from autoquant.data.models import _require_lowercase_sha256
from autoquant.errors import PersistenceUnavailableError

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


@dataclass(frozen=True, slots=True)
class LowVolatilityForwardEvidenceSpecRecord:
    spec: LowVolatilityForwardEvidenceSpec
    requested_by: str
    created_at: datetime
    live_trading_locked: bool = True

    def __post_init__(self) -> None:
        if (
            not self.requested_by.strip()
            or self.requested_by != self.requested_by.strip()
            or len(self.requested_by) > 128
            or self.created_at.tzinfo is None
            or self.created_at.utcoffset() is None
            or not self.live_trading_locked
        ):
            raise ValueError("forward evidence record is inconsistent")
        object.__setattr__(
            self,
            "created_at",
            self.created_at.astimezone(UTC),
        )


class PostgresLowVolatilityForwardEvidenceSpecRepository:
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
    ) -> PostgresLowVolatilityForwardEvidenceSpecRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(
                dsn,
                pool_pre_ping=True,
            )
        except Exception:
            raise PersistenceUnavailableError("forward evidence connection failed") from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def freeze(
        self,
        spec: LowVolatilityForwardEvidenceSpec,
        *,
        requested_by: str,
        created_at: datetime,
    ) -> LowVolatilityForwardEvidenceSpecRecord:
        requested = LowVolatilityForwardEvidenceSpecRecord(
            spec=spec,
            requested_by=requested_by,
            created_at=created_at,
        )
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:identity))"),
                    {
                        "identity": (
                            f"low-volatility-forward-evidence:{spec.predecessor_result_hash}"
                        )
                    },
                )
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO
                            {self._schema}.low_volatility_forward_evidence_specs
                            (spec_hash, predecessor_result_hash,
                             predecessor_assessment_hash,
                             source_spec_hash,
                             source_dataset_manifest_hash,
                             strategy_id, methodology_version,
                             specification_version,
                             forward_start_date,
                             minimum_forward_sessions,
                             minimum_paper_sessions,
                             formal_hypothesis_count,
                             outcome_observed_at_design,
                             strategy_parameters_unchanged,
                             retrospective_reclassification_allowed,
                             historical_result_eligible_for_promotion,
                             requested_by, created_at,
                             live_trading_locked, payload)
                        VALUES
                            (:spec_hash, :predecessor_result_hash,
                             :predecessor_assessment_hash,
                             :source_spec_hash,
                             :source_dataset_manifest_hash,
                             :strategy_id, :methodology_version,
                             :specification_version,
                             :forward_start_date,
                             :minimum_forward_sessions,
                             :minimum_paper_sessions,
                             :formal_hypothesis_count, true, true,
                             false, false, :requested_by,
                             :created_at, true,
                             CAST(:payload AS jsonb))
                        ON CONFLICT (predecessor_result_hash)
                        DO NOTHING
                        """
                    ),
                    {
                        "created_at": requested.created_at,
                        "formal_hypothesis_count": (spec.formal_hypothesis_count),
                        "forward_start_date": (spec.forward_start_date),
                        "methodology_version": (spec.stability_method_version),
                        "minimum_forward_sessions": (spec.minimum_forward_sessions),
                        "minimum_paper_sessions": (spec.minimum_paper_sessions),
                        "payload": _json(spec.payload()),
                        "predecessor_assessment_hash": (spec.predecessor_assessment_hash),
                        "predecessor_result_hash": (spec.predecessor_result_hash),
                        "requested_by": requested.requested_by,
                        "source_dataset_manifest_hash": (spec.source_dataset_manifest_hash),
                        "source_spec_hash": spec.source_spec_hash,
                        "spec_hash": spec.spec_hash,
                        "specification_version": spec.version,
                        "strategy_id": spec.strategy_id,
                    },
                )
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.low_volatility_forward_evidence_specs
                                WHERE predecessor_result_hash =
                                    :predecessor_result_hash
                                """
                            ),
                            {"predecessor_result_hash": (spec.predecessor_result_hash)},
                        )
                    )
                    .mappings()
                    .one()
                )
            stored = _record(row)
            if stored.spec != spec:
                raise ValueError("a different forward evidence spec is frozen")
            return stored
        except ValueError:
            raise
        except Exception:
            raise PersistenceUnavailableError("forward evidence freeze failed") from None

    async def read(
        self,
        spec_hash: str,
    ) -> LowVolatilityForwardEvidenceSpecRecord:
        _require_lowercase_sha256(
            spec_hash,
            name="forward evidence spec hash",
        )
        try:
            async with self._engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.low_volatility_forward_evidence_specs
                                WHERE spec_hash = :spec_hash
                                """
                            ),
                            {"spec_hash": spec_hash},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
            if row is None:
                raise LookupError("forward evidence spec does not exist")
            return _record(row)
        except (LookupError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError("forward evidence lookup failed") from None


def _record(
    row: RowMapping,
) -> LowVolatilityForwardEvidenceSpecRecord:
    try:
        raw = row["payload"]
        payload = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(payload, dict):
            raise TypeError("forward evidence payload is not an object")
        spec = LowVolatilityForwardEvidenceSpec.from_payload(
            {str(key): value for key, value in payload.items()}
        )
        if (
            spec.spec_hash != str(row["spec_hash"])
            or spec.predecessor_result_hash != str(row["predecessor_result_hash"])
            or spec.predecessor_assessment_hash != str(row["predecessor_assessment_hash"])
            or spec.source_spec_hash != str(row["source_spec_hash"])
            or spec.source_dataset_manifest_hash != str(row["source_dataset_manifest_hash"])
            or spec.strategy_id != str(row["strategy_id"])
            or spec.stability_method_version != str(row["methodology_version"])
            or spec.version != str(row["specification_version"])
            or spec.forward_start_date != row["forward_start_date"]
            or spec.minimum_forward_sessions != int(row["minimum_forward_sessions"])
            or spec.minimum_paper_sessions != int(row["minimum_paper_sessions"])
            or spec.formal_hypothesis_count != int(row["formal_hypothesis_count"])
            or row["outcome_observed_at_design"] is not True
            or row["strategy_parameters_unchanged"] is not True
            or row["retrospective_reclassification_allowed"] is not False
            or row["historical_result_eligible_for_promotion"] is not False
            or row["live_trading_locked"] is not True
        ):
            raise ValueError("stored forward evidence metadata mismatch")
        return LowVolatilityForwardEvidenceSpecRecord(
            spec=spec,
            requested_by=str(row["requested_by"]),
            created_at=row["created_at"],
        )
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError("stored forward evidence failed integrity") from None


def _json(payload: object) -> str:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
