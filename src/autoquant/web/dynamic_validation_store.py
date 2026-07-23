from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.backtest.codec import (
    decode_backtest_result,
    encode_backtest_result,
)
from autoquant.backtest.dynamic_validation import (
    DynamicCandidateEvaluation,
    DynamicValidationEvidence,
    DynamicValidationFold,
    DynamicValidationResult,
)
from autoquant.backtest.portfolio_validation import (
    CrossSectionalMomentumParameters,
)
from autoquant.data.models import _decimal_text, _require_lowercase_sha256
from autoquant.errors import PersistenceUnavailableError

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


@dataclass(frozen=True, slots=True)
class DynamicValidationRecord:
    result: DynamicValidationResult
    evidence: DynamicValidationEvidence
    requested_by: str
    completed_at: datetime
    live_trading_locked: bool = True

    def __post_init__(self) -> None:
        if (
            self.evidence.result_hash != self.result.result_hash
            or self.evidence.fold_count != len(self.result.folds)
            or self.evidence.rejected_order_count
            != self.result.rejected_order_count
        ):
            raise ValueError(
                "dynamic validation record evidence is inconsistent"
            )
        if (
            not self.requested_by.strip()
            or self.requested_by != self.requested_by.strip()
            or len(self.requested_by) > 128
        ):
            raise ValueError(
                "dynamic validation requester is invalid"
            )
        if (
            self.completed_at.tzinfo is None
            or self.completed_at.utcoffset() is None
            or not self.live_trading_locked
        ):
            raise ValueError(
                "dynamic validation completion metadata is invalid"
            )
        object.__setattr__(
            self,
            "completed_at",
            self.completed_at.astimezone(UTC),
        )


class PostgresDynamicValidationRepository:
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
    ) -> PostgresDynamicValidationRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "dynamic validation connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def save(
        self,
        result: DynamicValidationResult,
        evidence: DynamicValidationEvidence,
        *,
        requested_by: str,
        completed_at: datetime,
    ) -> DynamicValidationRecord:
        requested = DynamicValidationRecord(
            result=result,
            evidence=evidence,
            requested_by=requested_by,
            completed_at=completed_at,
        )
        existing_hash: str | None = None
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text(
                        "SELECT pg_advisory_xact_lock("
                        "hashtext(:identity))"
                    ),
                    {
                        "identity": (
                            f"dynamic-validation:{result.spec_hash}:"
                            f"{result.panel_hash}"
                        )
                    },
                )
                existing = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT result_hash
                                FROM {self._schema}.dynamic_validation_runs
                                WHERE spec_hash = :spec_hash
                                  AND panel_hash = :panel_hash
                                """
                            ),
                            {
                                "spec_hash": result.spec_hash,
                                "panel_hash": result.panel_hash,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if existing is not None:
                    existing_hash = str(existing["result_hash"])
                else:
                    await connection.execute(
                        text(
                            f"""
                            INSERT INTO
                                {self._schema}.dynamic_validation_runs
                                (result_hash, spec_hash, panel_hash,
                                 dataset_manifest_hash, assessment_hash,
                                 evidence_status, fold_count, oos_sessions,
                                 rejected_order_count, validator_version,
                                 objective_version, assessment_version,
                                 requested_by, as_of, completed_at,
                                 live_trading_locked, summary_payload,
                                 assessment_payload)
                            VALUES
                                (:result_hash, :spec_hash, :panel_hash,
                                 :dataset_manifest_hash, :assessment_hash,
                                 :evidence_status, :fold_count,
                                 :oos_sessions, :rejected_order_count,
                                 :validator_version, :objective_version,
                                 :assessment_version, :requested_by,
                                 :as_of, :completed_at, true,
                                 CAST(:summary_payload AS jsonb),
                                 CAST(:assessment_payload AS jsonb))
                            """
                        ),
                        _run_parameters(requested),
                    )
                    await connection.execute(
                        text(
                            f"""
                            INSERT INTO
                                {self._schema}.dynamic_validation_folds
                                (result_hash, sequence, train_start,
                                 train_end, test_start, test_end,
                                 fold_hash, selected_payload,
                                 selection_score,
                                 candidate_evaluations,
                                 training_payload, test_payload,
                                 benchmark_payload)
                            VALUES
                                (:result_hash, :sequence, :train_start,
                                 :train_end, :test_start, :test_end,
                                 :fold_hash,
                                 CAST(:selected_payload AS jsonb),
                                 :selection_score,
                                 CAST(:candidate_evaluations AS jsonb),
                                 CAST(:training_payload AS jsonb),
                                 CAST(:test_payload AS jsonb),
                                 CAST(:benchmark_payload AS jsonb))
                            """
                        ),
                        [
                            _fold_parameters(result.result_hash, fold)
                            for fold in result.folds
                        ],
                    )
        except Exception as error:
            if isinstance(error, ValueError):
                raise
            raise PersistenceUnavailableError(
                "dynamic validation save failed"
            ) from None
        stored = await self.read(existing_hash or result.result_hash)
        if (
            stored.result != requested.result
            or stored.evidence != requested.evidence
        ):
            raise ValueError(
                "a different dynamic validation result is already stored"
            )
        return stored

    async def read(
        self,
        result_hash: str,
    ) -> DynamicValidationRecord:
        _require_lowercase_sha256(
            result_hash,
            name="dynamic validation result hash",
        )
        try:
            async with self._engine.connect() as connection:
                run = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.dynamic_validation_runs
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
                        "dynamic validation result does not exist"
                    )
                folds = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.dynamic_validation_folds
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
            return _record(run, tuple(folds))
        except (LookupError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "dynamic validation lookup failed"
            ) from None


def _run_parameters(
    record: DynamicValidationRecord,
) -> dict[str, object]:
    result = record.result
    evidence = record.evidence
    return {
        "assessment_hash": evidence.assessment_hash,
        "assessment_payload": _json(_evidence_payload(evidence)),
        "assessment_version": evidence.version,
        "as_of": result.as_of,
        "completed_at": record.completed_at,
        "dataset_manifest_hash": result.dataset_manifest_hash,
        "evidence_status": evidence.evidence_status,
        "fold_count": evidence.fold_count,
        "objective_version": result.objective_version,
        "oos_sessions": evidence.oos_sessions,
        "panel_hash": result.panel_hash,
        "rejected_order_count": evidence.rejected_order_count,
        "requested_by": record.requested_by,
        "result_hash": result.result_hash,
        "spec_hash": result.spec_hash,
        "summary_payload": _json(_summary_payload(result)),
        "validator_version": result.version,
    }


def _fold_parameters(
    result_hash: str,
    fold: DynamicValidationFold,
) -> dict[str, object]:
    return {
        "benchmark_payload": _json(
            encode_backtest_result(fold.benchmark_result)
        ),
        "candidate_evaluations": _json(
            [
                {
                    "evaluation_hash": value.evaluation_hash,
                    "parameters": value.parameters.payload(),
                    "result": encode_backtest_result(value.result),
                    "score": _decimal_text(value.score),
                }
                for value in fold.candidate_evaluations
            ]
        ),
        "fold_hash": fold.fold_hash,
        "result_hash": result_hash,
        "selected_payload": _json(fold.selected.payload()),
        "selection_score": fold.selection_score,
        "sequence": fold.sequence,
        "test_end": fold.test_end,
        "test_payload": _json(
            encode_backtest_result(fold.test_result)
        ),
        "test_start": fold.test_start,
        "train_end": fold.train_end,
        "train_start": fold.train_start,
        "training_payload": _json(
            encode_backtest_result(fold.training_result)
        ),
    }


def _record(
    run: RowMapping,
    fold_rows: tuple[RowMapping, ...],
) -> DynamicValidationRecord:
    try:
        summary = _object(run["summary_payload"])
        assessment = _object(run["assessment_payload"])
        folds = tuple(_fold(row) for row in fold_rows)
        result = DynamicValidationResult(
            panel_hash=str(run["panel_hash"]),
            spec_hash=str(run["spec_hash"]),
            dataset_manifest_hash=str(
                run["dataset_manifest_hash"]
            ),
            as_of=run["as_of"],
            folds=folds,
            compounded_oos_return=Decimal(
                str(summary["compounded_oos_return"])
            ),
            benchmark_compounded_oos_return=Decimal(
                str(summary["benchmark_compounded_oos_return"])
            ),
            excess_oos_return=Decimal(
                str(summary["excess_oos_return"])
            ),
            profitable_fold_rate=Decimal(
                str(summary["profitable_fold_rate"])
            ),
            worst_oos_drawdown=Decimal(
                str(summary["worst_oos_drawdown"])
            ),
            mean_training_return=Decimal(
                str(summary["mean_training_return"])
            ),
            selection_optimism=Decimal(
                str(summary["selection_optimism"])
            ),
            rejected_order_count=int(
                str(summary["rejected_order_count"])
            ),
            version=str(run["validator_version"]),
            objective_version=str(run["objective_version"]),
        )
        evidence = DynamicValidationEvidence(
            result_hash=result.result_hash,
            policy_hash=str(assessment["policy_hash"]),
            fold_count=int(str(assessment["fold_count"])),
            oos_sessions=int(str(assessment["oos_sessions"])),
            rejected_order_count=int(
                str(assessment["rejected_order_count"])
            ),
            evidence_status=str(assessment["evidence_status"]),
            gate_failures=_string_array(
                assessment["gate_failures"],
                name="gate_failures",
            ),
            version=str(run["assessment_version"]),
        )
        if (
            result.result_hash != str(run["result_hash"])
            or evidence.assessment_hash
            != str(run["assessment_hash"])
            or evidence.evidence_status
            != str(run["evidence_status"])
            or evidence.fold_count != int(run["fold_count"])
            or evidence.oos_sessions != int(run["oos_sessions"])
            or evidence.rejected_order_count
            != int(run["rejected_order_count"])
            or tuple(
                value.fold_hash for value in result.folds
            )
            != _string_array(
                summary["fold_hashes"],
                name="fold_hashes",
            )
            or run["live_trading_locked"] is not True
        ):
            raise ValueError(
                "dynamic validation stored metadata mismatch"
            )
        return DynamicValidationRecord(
            result=result,
            evidence=evidence,
            requested_by=str(run["requested_by"]),
            completed_at=run["completed_at"],
            live_trading_locked=True,
        )
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "stored dynamic validation failed integrity verification"
        ) from None


def _fold(row: RowMapping) -> DynamicValidationFold:
    selected = _parameters(_object(row["selected_payload"]))
    raw_evaluations = _array(row["candidate_evaluations"])
    evaluations = tuple(
        DynamicCandidateEvaluation(
            parameters=_parameters(
                _object(value["parameters"])
            ),
            score=Decimal(str(value["score"])),
            result=decode_backtest_result(
                _object(value["result"])
            ),
        )
        for value in raw_evaluations
    )
    if tuple(
        value.evaluation_hash for value in evaluations
    ) != tuple(
        str(value["evaluation_hash"])
        for value in raw_evaluations
    ):
        raise ValueError(
            "stored dynamic candidate hash mismatch"
        )
    fold = DynamicValidationFold(
        sequence=int(row["sequence"]),
        train_start=row["train_start"],
        train_end=row["train_end"],
        test_start=row["test_start"],
        test_end=row["test_end"],
        selected=selected,
        selection_score=Decimal(str(row["selection_score"])),
        candidate_evaluations=evaluations,
        training_result=decode_backtest_result(
            _object(row["training_payload"])
        ),
        test_result=decode_backtest_result(
            _object(row["test_payload"])
        ),
        benchmark_result=decode_backtest_result(
            _object(row["benchmark_payload"])
        ),
    )
    if fold.fold_hash != str(row["fold_hash"]):
        raise ValueError("stored dynamic fold hash mismatch")
    return fold


def _summary_payload(
    result: DynamicValidationResult,
) -> dict[str, object]:
    return {
        "benchmark_compounded_oos_return": _decimal_text(
            result.benchmark_compounded_oos_return
        ),
        "compounded_oos_return": _decimal_text(
            result.compounded_oos_return
        ),
        "excess_oos_return": _decimal_text(result.excess_oos_return),
        "fold_hashes": [
            value.fold_hash for value in result.folds
        ],
        "mean_training_return": _decimal_text(
            result.mean_training_return
        ),
        "profitable_fold_rate": _decimal_text(
            result.profitable_fold_rate
        ),
        "rejected_order_count": result.rejected_order_count,
        "selection_optimism": _decimal_text(
            result.selection_optimism
        ),
        "worst_oos_drawdown": _decimal_text(
            result.worst_oos_drawdown
        ),
    }


def _evidence_payload(
    evidence: DynamicValidationEvidence,
) -> dict[str, object]:
    return {
        "evidence_status": evidence.evidence_status,
        "fold_count": evidence.fold_count,
        "gate_failures": list(evidence.gate_failures),
        "oos_sessions": evidence.oos_sessions,
        "policy_hash": evidence.policy_hash,
        "rejected_order_count": evidence.rejected_order_count,
    }


def _parameters(
    payload: dict[str, object],
) -> CrossSectionalMomentumParameters:
    return CrossSectionalMomentumParameters(
        lookback_sessions=int(str(payload["lookback_sessions"])),
        rebalance_sessions=int(
            str(payload["rebalance_sessions"])
        ),
        selection_count=int(str(payload["selection_count"])),
    )


def _object(value: object) -> dict[str, object]:
    raw = json.loads(value) if isinstance(value, str) else value
    if not isinstance(raw, dict):
        raise TypeError("dynamic validation payload is not an object")
    return {str(key): item for key, item in raw.items()}


def _array(value: object) -> list[dict[str, object]]:
    raw = json.loads(value) if isinstance(value, str) else value
    if not isinstance(raw, list) or any(
        not isinstance(item, dict) for item in raw
    ):
        raise TypeError("dynamic validation payload is not an array")
    return [
        {str(key): item for key, item in value.items()}
        for value in raw
    ]


def _string_array(
    value: object,
    *,
    name: str,
) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) for item in value
    ):
        raise TypeError(f"dynamic validation {name} is not a string array")
    return tuple(value)


def _json(payload: object) -> str:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
