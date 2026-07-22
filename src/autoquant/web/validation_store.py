from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.backtest.codec import decode_backtest_result, encode_backtest_result
from autoquant.backtest.models import BacktestResult, backtest_artifact_hash
from autoquant.backtest.validation import (
    SmaParameters,
    WalkForwardConfig,
    WalkForwardFold,
    WalkForwardResult,
)
from autoquant.errors import PersistenceUnavailableError
from autoquant.web.models import (
    BacktestMetrics,
    OperatorJobState,
    SmaCandidateRequest,
    ValidationExperiment,
    ValidationExperimentDetail,
    ValidationFoldView,
    ValidationSummary,
    WalkForwardJobRequest,
)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_EXPERIMENT_COLUMNS = """
experiment_id, state, validator_id, request_payload, requested_by, created_at,
started_at, completed_at, as_of, result_hash, summary_payload, error_code
"""
_QUALIFIED_EXPERIMENT_COLUMNS = """
experiments.experiment_id, experiments.state, experiments.validator_id,
experiments.request_payload, experiments.requested_by, experiments.created_at,
experiments.started_at, experiments.completed_at, experiments.as_of,
experiments.result_hash, experiments.summary_payload, experiments.error_code
"""


class PostgresValidationRepository:
    """Persistent walk-forward queue with complete selected-fold artifacts."""

    def __init__(self, *, engine: AsyncEngine, schema: str = "public") -> None:
        if _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError("schema must be a safe PostgreSQL identifier")
        self._engine = engine
        self._schema = schema

    @classmethod
    def connect(
        cls, *, dsn: str, schema: str = "public"
    ) -> PostgresValidationRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "PostgreSQL validation connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def create_experiment(
        self,
        request: WalkForwardJobRequest,
        *,
        requested_by: str,
        now: datetime,
    ) -> ValidationExperiment:
        experiment_id = uuid4()
        sql = text(
            f"""
            INSERT INTO {self._schema}.validation_experiments
                (experiment_id, idempotency_key, state, validator_id,
                 manifest_hash, request_payload, requested_by, created_at)
            VALUES
                (:experiment_id, :idempotency_key, 'queued',
                 'sma_cross_walk_forward_v1', :manifest_hash,
                 CAST(:request_payload AS jsonb), :requested_by, :created_at)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING {_EXPERIMENT_COLUMNS}
            """
        )
        parameters = {
            "experiment_id": experiment_id,
            "idempotency_key": request.idempotency_key,
            "manifest_hash": request.manifest_hash,
            "request_payload": _json(request.model_dump(mode="json")),
            "requested_by": requested_by,
            "created_at": _aware_utc(now),
        }
        try:
            async with self._engine.begin() as connection:
                row = (await connection.execute(sql, parameters)).mappings().one_or_none()
                if row is None:
                    row = (
                        (
                            await connection.execute(
                                text(
                                    f"SELECT {_EXPERIMENT_COLUMNS} "
                                    f"FROM {self._schema}.validation_experiments "
                                    "WHERE idempotency_key = :idempotency_key"
                                ),
                                {"idempotency_key": request.idempotency_key},
                            )
                        )
                        .mappings()
                        .one()
                    )
        except Exception:
            raise PersistenceUnavailableError(
                "Validation experiment creation failed"
            ) from None
        experiment = self._experiment_from_row(row)
        if experiment.request != request or experiment.requested_by != requested_by:
            raise ValueError("idempotency key already belongs to another request")
        return experiment

    async def list_experiments(
        self, *, limit: int = 50
    ) -> tuple[ValidationExperiment, ...]:
        if limit < 1 or limit > 200:
            raise ValueError("limit must be between 1 and 200")
        try:
            async with self._engine.connect() as connection:
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"SELECT {_EXPERIMENT_COLUMNS} "
                                f"FROM {self._schema}.validation_experiments "
                                "ORDER BY created_at DESC, experiment_id DESC LIMIT :limit"
                            ),
                            {"limit": limit},
                        )
                    )
                    .mappings()
                    .all()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "Validation experiment listing failed"
            ) from None
        return tuple(self._experiment_from_row(row) for row in rows)

    async def claim_next_experiment(
        self, *, now: datetime
    ) -> ValidationExperiment | None:
        sql = text(
            f"""
            WITH next_experiment AS (
                SELECT experiment_id FROM {self._schema}.validation_experiments
                WHERE state = 'queued'
                ORDER BY created_at, experiment_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE {self._schema}.validation_experiments AS experiments
            SET state = 'running', started_at = :started_at
            FROM next_experiment
            WHERE experiments.experiment_id = next_experiment.experiment_id
            RETURNING {_QUALIFIED_EXPERIMENT_COLUMNS}
            """
        )
        try:
            async with self._engine.begin() as connection:
                row = (
                    (await connection.execute(sql, {"started_at": _aware_utc(now)}))
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "Validation experiment claim failed"
            ) from None
        return None if row is None else self._experiment_from_row(row)

    async def complete_experiment(
        self,
        experiment_id: UUID,
        *,
        result: WalkForwardResult,
        now: datetime,
    ) -> ValidationExperiment:
        fold_parameters = [
            {
                "experiment_id": experiment_id,
                "sequence": fold.sequence,
                "train_start": fold.train_start,
                "train_end": fold.train_end,
                "test_start": fold.test_start,
                "test_end": fold.test_end,
                "selected_fast": fold.selected.fast_sessions,
                "selected_slow": fold.selected.slow_sessions,
                "selection_score": fold.selection_score,
                "fold_hash": fold.fold_hash,
                "training_payload": _json(
                    encode_backtest_result(fold.training_result)
                ),
                "test_payload": _json(encode_backtest_result(fold.test_result)),
                "benchmark_payload": (
                    None
                    if fold.benchmark_result is None
                    else _json(encode_backtest_result(fold.benchmark_result))
                ),
            }
            for fold in result.folds
        ]
        summary = _summary(result)
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.validation_folds
                            (experiment_id, sequence, train_start, train_end,
                             test_start, test_end, selected_fast, selected_slow,
                             selection_score, fold_hash, training_payload, test_payload,
                             benchmark_payload)
                        VALUES
                            (:experiment_id, :sequence, :train_start, :train_end,
                             :test_start, :test_end, :selected_fast, :selected_slow,
                             :selection_score, :fold_hash,
                             CAST(:training_payload AS jsonb),
                             CAST(:test_payload AS jsonb),
                             CAST(:benchmark_payload AS jsonb))
                        """
                    ),
                    fold_parameters,
                )
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                UPDATE {self._schema}.validation_experiments
                                SET state = 'completed', completed_at = :completed_at,
                                    as_of = :as_of, result_hash = :result_hash,
                                    summary_payload = CAST(:summary_payload AS jsonb),
                                    error_code = NULL
                                WHERE experiment_id = :experiment_id
                                  AND state = 'running'
                                  AND manifest_hash = :manifest_hash
                                RETURNING {_EXPERIMENT_COLUMNS}
                                """
                            ),
                            {
                                "experiment_id": experiment_id,
                                "completed_at": _aware_utc(now),
                                "as_of": result.as_of,
                                "result_hash": result.result_hash,
                                "summary_payload": _json(
                                    summary.model_dump(mode="json")
                                ),
                                "manifest_hash": result.manifest_hash,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise RuntimeError("validation experiment is not claimable")
        except Exception:
            raise PersistenceUnavailableError(
                "Validation result persistence failed"
            ) from None
        return self._experiment_from_row(row)

    async def fail_experiment(
        self,
        experiment_id: UUID,
        *,
        error_code: str,
        now: datetime,
        queued: bool = False,
    ) -> ValidationExperiment:
        if not error_code or len(error_code) > 80:
            raise ValueError("error_code must be 1-80 characters")
        expected = "queued" if queued else "running"
        try:
            async with self._engine.begin() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                UPDATE {self._schema}.validation_experiments
                                SET state = 'failed', completed_at = :completed_at,
                                    error_code = :error_code
                                WHERE experiment_id = :experiment_id
                                  AND state = :expected
                                RETURNING {_EXPERIMENT_COLUMNS}
                                """
                            ),
                            {
                                "experiment_id": experiment_id,
                                "completed_at": _aware_utc(now),
                                "error_code": error_code,
                                "expected": expected,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "Validation failure persistence failed"
            ) from None
        if row is None:
            raise PersistenceUnavailableError(
                "Validation experiment is not in the expected state"
            )
        return self._experiment_from_row(row)

    async def interrupt_running_experiments(self, *, now: datetime) -> int:
        try:
            async with self._engine.begin() as connection:
                result = await connection.execute(
                    text(
                        f"""
                        UPDATE {self._schema}.validation_experiments
                        SET state = 'interrupted', completed_at = :completed_at,
                            error_code = 'worker_restarted'
                        WHERE state = 'running'
                        """
                    ),
                    {"completed_at": _aware_utc(now)},
                )
        except Exception:
            raise PersistenceUnavailableError(
                "Validation experiment recovery failed"
            ) from None
        return int(result.rowcount or 0)

    async def detail(self, experiment_id: UUID) -> ValidationExperimentDetail:
        try:
            async with self._engine.connect() as connection:
                experiment_row = (
                    (
                        await connection.execute(
                            text(
                                f"SELECT {_EXPERIMENT_COLUMNS} "
                                f"FROM {self._schema}.validation_experiments "
                                "WHERE experiment_id = :experiment_id"
                            ),
                            {"experiment_id": experiment_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if experiment_row is None:
                    raise LookupError("validation experiment not found")
                fold_rows = (
                    (
                        await connection.execute(
                            text(
                                f"SELECT * FROM {self._schema}.validation_folds "
                                "WHERE experiment_id = :experiment_id ORDER BY sequence"
                            ),
                            {"experiment_id": experiment_id},
                        )
                    )
                    .mappings()
                    .all()
                )
        except LookupError:
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "Validation detail query failed"
            ) from None
        experiment = self._experiment_from_row(experiment_row)
        try:
            decoded_folds = tuple(_fold_from_row(row) for row in fold_rows)
            domain_folds = tuple(item[0] for item in decoded_folds)
            folds = tuple(item[1] for item in decoded_folds)
            _verify_experiment(experiment, domain_folds)
        except (KeyError, TypeError, ValueError):
            raise PersistenceUnavailableError(
                "Stored validation result failed integrity verification"
            ) from None
        return ValidationExperimentDetail(experiment=experiment, folds=folds)

    @staticmethod
    def _experiment_from_row(row: RowMapping) -> ValidationExperiment:
        try:
            raw_summary = row["summary_payload"]
            return ValidationExperiment(
                experiment_id=row["experiment_id"],
                state=OperatorJobState(str(row["state"])),
                validator_id=str(row["validator_id"]),
                request=WalkForwardJobRequest.model_validate(
                    _object(row["request_payload"])
                ),
                requested_by=str(row["requested_by"]),
                created_at=row["created_at"],
                started_at=row["started_at"],
                completed_at=row["completed_at"],
                as_of=row["as_of"],
                result_hash=(
                    None if row["result_hash"] is None else str(row["result_hash"])
                ),
                summary=(
                    None
                    if raw_summary is None
                    else ValidationSummary.model_validate(_object(raw_summary))
                ),
                error_code=(
                    None if row["error_code"] is None else str(row["error_code"])
                ),
            )
        except (KeyError, TypeError, ValueError):
            raise PersistenceUnavailableError(
                "Stored validation experiment is malformed"
            ) from None


def validation_config(request: WalkForwardJobRequest) -> WalkForwardConfig:
    return WalkForwardConfig(
        initial_cash=request.initial_cash,
        allocation=request.allocation,
        slippage_bps=request.slippage_bps,
        train_sessions=request.train_sessions,
        test_sessions=request.test_sessions,
        embargo_sessions=request.embargo_sessions,
        candidates=tuple(
            SmaParameters(candidate.fast_sessions, candidate.slow_sessions)
            for candidate in request.candidates
        ),
    )


def _summary(result: WalkForwardResult) -> ValidationSummary:
    oos_sessions = sum(len(fold.test_result.snapshots) for fold in result.folds)
    failures: list[str] = []
    if len(result.folds) < 6:
        failures.append("minimum_fold_count")
    if oos_sessions < 120:
        failures.append("minimum_oos_sessions")
    if result.compounded_oos_return <= 0:
        failures.append("nonpositive_oos_return")
    if result.excess_oos_return is None:
        failures.append("benchmark_missing")
    elif result.excess_oos_return <= 0:
        failures.append("nonpositive_excess_return")
    if result.profitable_fold_rate < Decimal("0.5"):
        failures.append("profitable_fold_rate")
    if result.worst_oos_drawdown > Decimal("0.15"):
        failures.append("oos_drawdown_limit")
    sample_failures = {"minimum_fold_count", "minimum_oos_sessions"}
    evidence_status = (
        "research_candidate"
        if not failures
        else "insufficient"
        if set(failures).issubset(sample_failures)
        else "rejected"
    )
    return ValidationSummary(
        fold_count=len(result.folds),
        compounded_oos_return=result.compounded_oos_return,
        mean_oos_return=result.mean_oos_return,
        worst_oos_drawdown=result.worst_oos_drawdown,
        profitable_fold_rate=result.profitable_fold_rate,
        mean_training_return=result.mean_training_return,
        selection_optimism=result.selection_optimism,
        validation_version=result.validation_version,
        objective_version=result.objective_version,
        benchmark_compounded_oos_return=result.benchmark_compounded_oos_return,
        excess_oos_return=result.excess_oos_return,
        oos_sessions=oos_sessions,
        evidence_status=evidence_status,
        gate_failures=tuple(failures),
    )


def _metrics(result: BacktestResult) -> BacktestMetrics:
    return BacktestMetrics(
        initial_cash=result.initial_cash,
        ending_equity=result.ending_equity,
        total_return=result.total_return,
        max_drawdown=result.max_drawdown,
        turnover=result.turnover,
        total_fees=result.total_fees,
        rule_versions=result.rule_versions,
        fee_version=result.fee_version,
        execution_version=result.execution_version,
        artifact_hash=backtest_artifact_hash(result),
    )


def _fold_from_row(row: RowMapping) -> tuple[WalkForwardFold, ValidationFoldView]:
    training = decode_backtest_result(row["training_payload"])
    test = decode_backtest_result(row["test_payload"])
    raw_benchmark = row["benchmark_payload"]
    benchmark = (
        None if raw_benchmark is None else decode_backtest_result(raw_benchmark)
    )
    selected = SmaParameters(
        int(row["selected_fast"]), int(row["selected_slow"])
    )
    fold = WalkForwardFold(
        sequence=int(row["sequence"]),
        train_start=row["train_start"],
        train_end=row["train_end"],
        test_start=row["test_start"],
        test_end=row["test_end"],
        selected=selected,
        selection_score=row["selection_score"],
        training_result=training,
        test_result=test,
        benchmark_result=benchmark,
    )
    if fold.fold_hash != str(row["fold_hash"]):
        raise ValueError("validation fold hash mismatch")
    return (
        fold,
        ValidationFoldView(
            sequence=fold.sequence,
            train_start=fold.train_start,
            train_end=fold.train_end,
            test_start=fold.test_start,
            test_end=fold.test_end,
            selected=SmaCandidateRequest(
                fast_sessions=selected.fast_sessions,
                slow_sessions=selected.slow_sessions,
            ),
            selection_score=fold.selection_score,
            training=_metrics(training),
            test=_metrics(test),
            benchmark=None if benchmark is None else _metrics(benchmark),
            training_result_hash=training.result_hash,
            test_result_hash=test.result_hash,
            fold_hash=fold.fold_hash,
        ),
    )


def _verify_experiment(
    experiment: ValidationExperiment,
    folds: tuple[WalkForwardFold, ...],
) -> None:
    if experiment.state is not OperatorJobState.COMPLETED:
        if folds:
            raise ValueError("incomplete experiment cannot contain folds")
        return
    if (
        experiment.summary is None
        or experiment.as_of is None
        or experiment.result_hash is None
    ):
        raise ValueError("completed experiment is missing result fields")
    summary = experiment.summary
    if summary.fold_count != len(folds):
        raise ValueError("validation fold count mismatch")
    reconstructed = WalkForwardResult(
        manifest_hash=experiment.request.manifest_hash,
        instrument=experiment.request.instrument,
        as_of=experiment.as_of,
        config=validation_config(experiment.request),
        folds=folds,
        compounded_oos_return=summary.compounded_oos_return,
        mean_oos_return=summary.mean_oos_return,
        worst_oos_drawdown=summary.worst_oos_drawdown,
        profitable_fold_rate=summary.profitable_fold_rate,
        mean_training_return=summary.mean_training_return,
        selection_optimism=summary.selection_optimism,
        benchmark_compounded_oos_return=summary.benchmark_compounded_oos_return,
        excess_oos_return=summary.excess_oos_return,
        validation_version=summary.validation_version,
        objective_version=summary.objective_version,
    )
    if reconstructed.result_hash != experiment.result_hash:
        raise ValueError("validation experiment hash mismatch")
    if "legacy_assessment_missing" not in summary.gate_failures:
        expected_summary = _summary(reconstructed)
        if (
            summary.oos_sessions != expected_summary.oos_sessions
            or summary.evidence_status != expected_summary.evidence_status
            or summary.gate_failures != expected_summary.gate_failures
        ):
            raise ValueError("validation evidence assessment mismatch")


def _object(value: object) -> dict[str, object]:
    decoded = json.loads(value) if isinstance(value, str) else value
    if not isinstance(decoded, dict):
        raise TypeError("stored JSON value must be an object")
    return dict(decoded)


def _json(value: object) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)
