from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.backtest.codec import (
    decode_backtest_result,
    encode_backtest_result,
)
from autoquant.backtest.low_volatility_forward import (
    LowVolatilityForwardSessionBinding,
)
from autoquant.backtest.low_volatility_forward_evaluation import (
    LowVolatilityForwardAssessment,
    LowVolatilityForwardBlockResult,
    LowVolatilityForwardEvaluationResult,
)
from autoquant.data.models import (
    _decimal_text,
    _require_lowercase_sha256,
)
from autoquant.errors import PersistenceUnavailableError

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


@dataclass(frozen=True, slots=True)
class LowVolatilityForwardEvaluationRecord:
    result: LowVolatilityForwardEvaluationResult
    assessment: LowVolatilityForwardAssessment
    requested_by: str
    completed_at: datetime
    paper_deployment_allowed: bool = False
    live_trading_locked: bool = True

    def __post_init__(self) -> None:
        if (
            self.assessment.result_hash != self.result.result_hash
            or self.assessment.forward_spec_hash
            != self.result.forward_spec_hash
            or self.assessment.session_count
            != len(self.result.session_bindings)
            or self.assessment.block_count != len(self.result.blocks)
            or not self.requested_by.strip()
            or self.requested_by != self.requested_by.strip()
            or len(self.requested_by) > 128
            or self.completed_at.tzinfo is None
            or self.completed_at.utcoffset() is None
            or self.paper_deployment_allowed
            or not self.live_trading_locked
        ):
            raise ValueError(
                "low-volatility forward evaluation record is inconsistent"
            )
        object.__setattr__(
            self,
            "completed_at",
            self.completed_at.astimezone(UTC),
        )


class PostgresLowVolatilityForwardEvaluationRepository:
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
    ) -> PostgresLowVolatilityForwardEvaluationRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(
                dsn,
                pool_pre_ping=True,
            )
        except Exception:
            raise PersistenceUnavailableError(
                "forward evaluation connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def save(
        self,
        result: LowVolatilityForwardEvaluationResult,
        assessment: LowVolatilityForwardAssessment,
        *,
        requested_by: str,
        completed_at: datetime,
    ) -> LowVolatilityForwardEvaluationRecord:
        requested = LowVolatilityForwardEvaluationRecord(
            result=result,
            assessment=assessment,
            requested_by=requested_by,
            completed_at=completed_at,
        )
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text(
                        "SELECT pg_advisory_xact_lock("
                        "hashtext(:identity))"
                    ),
                    {
                        "identity": (
                            "low-volatility-forward-evaluation:"
                            f"{result.forward_spec_hash}"
                        )
                    },
                )
                existing = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT result_hash
                                FROM {self._schema}.
                                    low_volatility_forward_evaluation_runs
                                WHERE forward_spec_hash =
                                    :forward_spec_hash
                                """
                            ),
                            {
                                "forward_spec_hash": (
                                    result.forward_spec_hash
                                )
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if existing is None:
                    await connection.execute(
                        text(
                            f"""
                            INSERT INTO {self._schema}.
                                low_volatility_forward_evaluation_runs
                                (result_hash, forward_spec_hash,
                                 evaluation_dataset_manifest_hash,
                                 source_spec_hash,
                                 predecessor_result_hash,
                                 predecessor_assessment_hash,
                                 panel_hash, market_panel_hash,
                                 assessment_hash, evidence_status,
                                 session_count, block_count,
                                 paper_trading_eligible,
                                 paper_deployment_allowed,
                                 live_trading_locked,
                                 strategy_rejected_order_count,
                                 benchmark_rejected_order_count,
                                 strategy_unresolved_position_count,
                                 benchmark_unresolved_position_count,
                                 evaluation_version,
                                 assessment_version,
                                 requested_by, as_of, completed_at,
                                 strategy_payload, benchmark_payload,
                                 summary_payload, assessment_payload)
                            VALUES
                                (:result_hash, :forward_spec_hash,
                                 :evaluation_dataset_manifest_hash,
                                 :source_spec_hash,
                                 :predecessor_result_hash,
                                 :predecessor_assessment_hash,
                                 :panel_hash, :market_panel_hash,
                                 :assessment_hash, :evidence_status,
                                 :session_count, :block_count,
                                 :paper_trading_eligible, false, true,
                                 :strategy_rejected_order_count,
                                 :benchmark_rejected_order_count,
                                 :strategy_unresolved_position_count,
                                 :benchmark_unresolved_position_count,
                                 :evaluation_version,
                                 :assessment_version,
                                 :requested_by, :as_of, :completed_at,
                                 CAST(:strategy_payload AS jsonb),
                                 CAST(:benchmark_payload AS jsonb),
                                 CAST(:summary_payload AS jsonb),
                                 CAST(:assessment_payload AS jsonb))
                            """
                        ),
                        _run_parameters(requested),
                    )
                    await connection.execute(
                        text(
                            f"""
                            INSERT INTO {self._schema}.
                                low_volatility_forward_evaluation_bindings
                                (result_hash, sequence, binding_hash,
                                 session_date, binding_payload)
                            VALUES
                                (:result_hash, :sequence, :binding_hash,
                                 :session_date,
                                 CAST(:binding_payload AS jsonb))
                            """
                        ),
                        [
                            _binding_parameters(
                                result.result_hash,
                                sequence,
                                binding,
                            )
                            for sequence, binding in enumerate(
                                result.session_bindings,
                                start=1,
                            )
                        ],
                    )
                    await connection.execute(
                        text(
                            f"""
                            INSERT INTO {self._schema}.
                                low_volatility_forward_evaluation_blocks
                                (result_hash, sequence, block_hash,
                                 start_date, end_date, block_payload)
                            VALUES
                                (:result_hash, :sequence, :block_hash,
                                 :start_date, :end_date,
                                 CAST(:block_payload AS jsonb))
                            """
                        ),
                        [
                            _block_parameters(
                                result.result_hash,
                                block,
                            )
                            for block in result.blocks
                        ],
                    )
                    result_hash = result.result_hash
                else:
                    result_hash = str(existing["result_hash"])
        except Exception as error:
            if isinstance(error, ValueError):
                raise
            raise PersistenceUnavailableError(
                "forward evaluation save failed"
            ) from None
        stored = await self.read(result_hash)
        if (
            stored.result != requested.result
            or stored.assessment != requested.assessment
        ):
            raise ValueError(
                "a different forward evaluation is already stored"
            )
        return stored

    async def read(
        self,
        result_hash: str,
    ) -> LowVolatilityForwardEvaluationRecord:
        _require_lowercase_sha256(
            result_hash,
            name="forward evaluation result hash",
        )
        try:
            async with self._engine.connect() as connection:
                run = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.
                                    low_volatility_forward_evaluation_runs
                                WHERE result_hash = :result_hash
                                """
                            ),
                            {"result_hash": result_hash},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if run is None:
                    raise LookupError(
                        "forward evaluation does not exist"
                    )
                bindings = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.
                                    low_volatility_forward_evaluation_bindings
                                WHERE result_hash = :result_hash
                                ORDER BY sequence
                                """
                            ),
                            {"result_hash": result_hash},
                        )
                    )
                    .mappings()
                    .all()
                )
                blocks = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.
                                    low_volatility_forward_evaluation_blocks
                                WHERE result_hash = :result_hash
                                ORDER BY sequence
                                """
                            ),
                            {"result_hash": result_hash},
                        )
                    )
                    .mappings()
                    .all()
                )
            return _record(run, tuple(bindings), tuple(blocks))
        except (LookupError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "forward evaluation lookup failed"
            ) from None

    async def read_for_spec(
        self,
        forward_spec_hash: str,
    ) -> LowVolatilityForwardEvaluationRecord | None:
        _require_lowercase_sha256(
            forward_spec_hash,
            name="forward evaluation spec hash",
        )
        try:
            async with self._engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT result_hash
                                FROM {self._schema}.
                                    low_volatility_forward_evaluation_runs
                                WHERE forward_spec_hash =
                                    :forward_spec_hash
                                """
                            ),
                            {
                                "forward_spec_hash": forward_spec_hash
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "forward evaluation lookup failed"
            ) from None
        return (
            None
            if row is None
            else await self.read(str(row["result_hash"]))
        )


def _run_parameters(
    record: LowVolatilityForwardEvaluationRecord,
) -> dict[str, object]:
    result = record.result
    assessment = record.assessment
    return {
        "assessment_hash": assessment.assessment_hash,
        "assessment_payload": _json(
            _assessment_payload(assessment)
        ),
        "assessment_version": assessment.version,
        "as_of": result.as_of,
        "benchmark_payload": _json(
            encode_backtest_result(result.benchmark_result)
        ),
        "benchmark_rejected_order_count": (
            result.benchmark_rejected_order_count
        ),
        "benchmark_unresolved_position_count": (
            result.benchmark_unresolved_position_count
        ),
        "block_count": len(result.blocks),
        "completed_at": record.completed_at,
        "evaluation_version": result.version,
        "evidence_status": assessment.evidence_status,
        "evaluation_dataset_manifest_hash": (
            result.evaluation_dataset_manifest_hash
        ),
        "forward_spec_hash": result.forward_spec_hash,
        "market_panel_hash": result.market_panel_hash,
        "panel_hash": result.panel_hash,
        "paper_trading_eligible": (
            assessment.paper_trading_eligible
        ),
        "predecessor_assessment_hash": (
            result.predecessor_assessment_hash
        ),
        "predecessor_result_hash": result.predecessor_result_hash,
        "requested_by": record.requested_by,
        "result_hash": result.result_hash,
        "session_count": len(result.session_bindings),
        "source_spec_hash": result.source_spec_hash,
        "strategy_payload": _json(
            encode_backtest_result(result.strategy_result)
        ),
        "strategy_rejected_order_count": (
            result.strategy_rejected_order_count
        ),
        "strategy_unresolved_position_count": (
            result.strategy_unresolved_position_count
        ),
        "summary_payload": _json(_summary_payload(result)),
    }


def _binding_parameters(
    result_hash: str,
    sequence: int,
    binding: LowVolatilityForwardSessionBinding,
) -> dict[str, object]:
    return {
        "binding_hash": binding.binding_hash,
        "binding_payload": _json(binding.payload()),
        "result_hash": result_hash,
        "sequence": sequence,
        "session_date": binding.session_date,
    }


def _block_parameters(
    result_hash: str,
    block: LowVolatilityForwardBlockResult,
) -> dict[str, object]:
    return {
        "block_hash": block.block_hash,
        "block_payload": _json(block.payload()),
        "end_date": block.end_date,
        "result_hash": result_hash,
        "sequence": block.sequence,
        "start_date": block.start_date,
    }


def _record(
    run: RowMapping,
    binding_rows: tuple[RowMapping, ...],
    block_rows: tuple[RowMapping, ...],
) -> LowVolatilityForwardEvaluationRecord:
    try:
        summary = _object(run["summary_payload"])
        assessment_payload = _object(run["assessment_payload"])
        bindings = tuple(_binding(row) for row in binding_rows)
        blocks = tuple(_block(row) for row in block_rows)
        result = LowVolatilityForwardEvaluationResult(
            forward_spec_hash=str(run["forward_spec_hash"]),
            evaluation_dataset_manifest_hash=str(
                run["evaluation_dataset_manifest_hash"]
            ),
            source_spec_hash=str(run["source_spec_hash"]),
            predecessor_result_hash=str(
                run["predecessor_result_hash"]
            ),
            predecessor_assessment_hash=str(
                run["predecessor_assessment_hash"]
            ),
            panel_hash=str(run["panel_hash"]),
            market_panel_hash=str(run["market_panel_hash"]),
            as_of=run["as_of"],
            session_bindings=bindings,
            strategy_result=decode_backtest_result(
                _object(run["strategy_payload"])
            ),
            benchmark_result=decode_backtest_result(
                _object(run["benchmark_payload"])
            ),
            blocks=blocks,
            source_mean_training_return=Decimal(
                str(summary["source_mean_training_return"])
            ),
            source_training_sessions=int(
                str(summary["source_training_sessions"])
            ),
            forward_compounded_return=Decimal(
                str(summary["forward_compounded_return"])
            ),
            benchmark_compounded_return=Decimal(
                str(summary["benchmark_compounded_return"])
            ),
            forward_excess_return=Decimal(
                str(summary["forward_excess_return"])
            ),
            profitable_block_rate=Decimal(
                str(summary["profitable_block_rate"])
            ),
            annualized_training_return=Decimal(
                str(summary["annualized_training_return"])
            ),
            annualized_forward_return=Decimal(
                str(summary["annualized_forward_return"])
            ),
            annualized_stability_gap=Decimal(
                str(summary["annualized_stability_gap"])
            ),
            strategy_rejected_order_count=int(
                str(summary["strategy_rejected_order_count"])
            ),
            benchmark_rejected_order_count=int(
                str(summary["benchmark_rejected_order_count"])
            ),
            strategy_unresolved_position_count=int(
                str(summary["strategy_unresolved_position_count"])
            ),
            benchmark_unresolved_position_count=int(
                str(summary["benchmark_unresolved_position_count"])
            ),
            version=str(run["evaluation_version"]),
        )
        assessment = LowVolatilityForwardAssessment(
            result_hash=result.result_hash,
            forward_spec_hash=result.forward_spec_hash,
            session_count=int(
                str(assessment_payload["session_count"])
            ),
            block_count=int(str(assessment_payload["block_count"])),
            evidence_status=str(
                assessment_payload["evidence_status"]
            ),
            gate_failures=_string_array(
                assessment_payload["gate_failures"]
            ),
            paper_trading_eligible=_boolean(
                assessment_payload["paper_trading_eligible"]
            ),
            live_trading_locked=_boolean(
                assessment_payload["live_trading_locked"]
            ),
            version=str(run["assessment_version"]),
        )
        if (
            result.result_hash != str(run["result_hash"])
            or assessment.assessment_hash
            != str(run["assessment_hash"])
            or assessment.evidence_status
            != str(run["evidence_status"])
            or assessment.paper_trading_eligible
            is not run["paper_trading_eligible"]
            or len(bindings) != int(run["session_count"])
            or len(blocks) != int(run["block_count"])
            or result.strategy_rejected_order_count
            != int(run["strategy_rejected_order_count"])
            or result.benchmark_rejected_order_count
            != int(run["benchmark_rejected_order_count"])
            or result.strategy_unresolved_position_count
            != int(run["strategy_unresolved_position_count"])
            or result.benchmark_unresolved_position_count
            != int(run["benchmark_unresolved_position_count"])
            or tuple(
                value.binding_hash for value in bindings
            )
            != _string_array(summary["session_binding_hashes"])
            or tuple(value.block_hash for value in blocks)
            != _string_array(summary["block_hashes"])
            or run["paper_deployment_allowed"] is not False
            or run["live_trading_locked"] is not True
        ):
            raise ValueError(
                "stored forward evaluation metadata mismatch"
            )
        return LowVolatilityForwardEvaluationRecord(
            result=result,
            assessment=assessment,
            requested_by=str(run["requested_by"]),
            completed_at=run["completed_at"],
        )
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "stored forward evaluation failed integrity"
        ) from None


def _binding(row: RowMapping) -> LowVolatilityForwardSessionBinding:
    binding = LowVolatilityForwardSessionBinding.from_payload(
        _object(row["binding_payload"])
    )
    if (
        binding.binding_hash != str(row["binding_hash"])
        or binding.session_date != row["session_date"]
    ):
        raise ValueError("stored forward binding hash mismatch")
    return binding


def _block(row: RowMapping) -> LowVolatilityForwardBlockResult:
    payload = _object(row["block_payload"])
    block = LowVolatilityForwardBlockResult(
        sequence=int(str(payload["sequence"])),
        start_date=date.fromisoformat(str(payload["start_date"])),
        end_date=date.fromisoformat(str(payload["end_date"])),
        session_count=int(str(payload["session_count"])),
        strategy_starting_equity=Decimal(
            str(payload["strategy_starting_equity"])
        ),
        strategy_ending_equity=Decimal(
            str(payload["strategy_ending_equity"])
        ),
        benchmark_starting_equity=Decimal(
            str(payload["benchmark_starting_equity"])
        ),
        benchmark_ending_equity=Decimal(
            str(payload["benchmark_ending_equity"])
        ),
        strategy_return=Decimal(str(payload["strategy_return"])),
        benchmark_return=Decimal(str(payload["benchmark_return"])),
        version=str(payload["version"]),
    )
    if (
        block.payload() != payload
        or block.block_hash != str(row["block_hash"])
        or block.sequence != int(row["sequence"])
        or block.start_date != row["start_date"]
        or block.end_date != row["end_date"]
    ):
        raise ValueError("stored forward block hash mismatch")
    return block


def _summary_payload(
    result: LowVolatilityForwardEvaluationResult,
) -> dict[str, object]:
    return {
        "annualized_forward_return": _decimal_text(
            result.annualized_forward_return
        ),
        "annualized_stability_gap": _decimal_text(
            result.annualized_stability_gap
        ),
        "annualized_training_return": _decimal_text(
            result.annualized_training_return
        ),
        "benchmark_compounded_return": _decimal_text(
            result.benchmark_compounded_return
        ),
        "benchmark_rejected_order_count": (
            result.benchmark_rejected_order_count
        ),
        "benchmark_unresolved_position_count": (
            result.benchmark_unresolved_position_count
        ),
        "block_hashes": [value.block_hash for value in result.blocks],
        "forward_compounded_return": _decimal_text(
            result.forward_compounded_return
        ),
        "forward_excess_return": _decimal_text(
            result.forward_excess_return
        ),
        "profitable_block_rate": _decimal_text(
            result.profitable_block_rate
        ),
        "session_binding_hashes": [
            value.binding_hash for value in result.session_bindings
        ],
        "source_mean_training_return": _decimal_text(
            result.source_mean_training_return
        ),
        "source_training_sessions": result.source_training_sessions,
        "strategy_rejected_order_count": (
            result.strategy_rejected_order_count
        ),
        "strategy_unresolved_position_count": (
            result.strategy_unresolved_position_count
        ),
    }


def _assessment_payload(
    assessment: LowVolatilityForwardAssessment,
) -> dict[str, object]:
    return {
        "block_count": assessment.block_count,
        "evidence_status": assessment.evidence_status,
        "gate_failures": list(assessment.gate_failures),
        "live_trading_locked": assessment.live_trading_locked,
        "paper_trading_eligible": (
            assessment.paper_trading_eligible
        ),
        "session_count": assessment.session_count,
    }


def _object(value: object) -> dict[str, object]:
    raw = json.loads(value) if isinstance(value, str) else value
    if not isinstance(raw, dict):
        raise TypeError(
            "forward evaluation payload is not an object"
        )
    return {str(key): item for key, item in raw.items()}


def _string_array(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) for item in value
    ):
        raise TypeError(
            "forward evaluation value is not a string array"
        )
    return tuple(value)


def _boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise TypeError("forward evaluation boolean is invalid")
    return value


def _json(payload: object) -> str:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
