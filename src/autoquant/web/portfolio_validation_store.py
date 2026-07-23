from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.backtest.codec import (
    decode_backtest_result,
    encode_backtest_result,
)
from autoquant.backtest.models import BacktestResult, backtest_artifact_hash
from autoquant.backtest.portfolio_diagnostics import (
    diagnose_portfolio_validation,
)
from autoquant.backtest.portfolio_validation import (
    CrossSectionalMomentumParameters,
    PortfolioValidationEvidencePolicy,
    PortfolioWalkForwardConfig,
    PortfolioWalkForwardFold,
    PortfolioWalkForwardResult,
    assess_portfolio_validation,
)
from autoquant.errors import PersistenceUnavailableError
from autoquant.web.models import (
    BacktestMetrics,
    MomentumCandidateRequest,
    OperatorJobState,
    PortfolioValidationDiagnosticsView,
    PortfolioValidationExperiment,
    PortfolioValidationExperimentDetail,
    PortfolioValidationFoldView,
    PortfolioValidationSummary,
    PortfolioWalkForwardJobRequest,
)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_VALIDATOR_ID = "cross_sectional_momentum_walk_forward_v1"
_EXPERIMENT_COLUMNS = """
experiment_id, state, validator_id, request_payload, requested_by, created_at,
started_at, completed_at, as_of, result_hash, summary_payload, error_code
"""
_QUALIFIED_COLUMNS = """
experiments.experiment_id, experiments.state, experiments.validator_id,
experiments.request_payload, experiments.requested_by, experiments.created_at,
experiments.started_at, experiments.completed_at, experiments.as_of,
experiments.result_hash, experiments.summary_payload, experiments.error_code
"""


class PostgresPortfolioValidationRepository:
    """Queue and verify complete cross-sectional validation artifacts."""

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
    ) -> PostgresPortfolioValidationRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "portfolio validation connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def create_experiment(
        self,
        request: PortfolioWalkForwardJobRequest,
        *,
        requested_by: str,
        now: datetime,
    ) -> PortfolioValidationExperiment:
        normalized_requested_by = requested_by.strip()
        if (
            not normalized_requested_by
            or len(normalized_requested_by) > 128
            or normalized_requested_by != requested_by
        ):
            raise ValueError("requested_by must contain 1-128 characters")
        parameters = {
            "experiment_id": uuid4(),
            "idempotency_key": request.idempotency_key,
            "manifest_hash": request.manifest_hash,
            "request_payload": _json(request.model_dump(mode="json")),
            "requested_by": normalized_requested_by,
            "created_at": _aware_utc(now),
        }
        try:
            async with self._engine.begin() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                INSERT INTO {self._schema}.portfolio_validation_experiments
                                    (experiment_id, idempotency_key, state,
                                     validator_id, manifest_hash, request_payload,
                                     requested_by, created_at)
                                VALUES
                                    (:experiment_id, :idempotency_key, 'queued',
                                     '{_VALIDATOR_ID}', :manifest_hash,
                                     CAST(:request_payload AS jsonb),
                                     :requested_by, :created_at)
                                ON CONFLICT (idempotency_key) DO NOTHING
                                RETURNING {_EXPERIMENT_COLUMNS}
                                """
                            ),
                            parameters,
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    row = (
                        (
                            await connection.execute(
                                text(
                                    f"""
                                    SELECT {_EXPERIMENT_COLUMNS}
                                    FROM {self._schema}.portfolio_validation_experiments
                                    WHERE idempotency_key = :idempotency_key
                                    """
                                ),
                                {
                                    "idempotency_key": (
                                        request.idempotency_key
                                    )
                                },
                            )
                        )
                        .mappings()
                        .one()
                    )
        except Exception:
            raise PersistenceUnavailableError(
                "portfolio validation creation failed"
            ) from None
        experiment = self._experiment_from_row(row)
        if (
            experiment.request != request
            or experiment.requested_by != normalized_requested_by
        ):
            raise ValueError(
                "idempotency key belongs to another portfolio request"
            )
        return experiment

    async def list_experiments(
        self,
        *,
        limit: int = 50,
    ) -> tuple[PortfolioValidationExperiment, ...]:
        if limit < 1 or limit > 200:
            raise ValueError("limit must be between 1 and 200")
        try:
            async with self._engine.connect() as connection:
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT {_EXPERIMENT_COLUMNS}
                                FROM {self._schema}.portfolio_validation_experiments
                                ORDER BY created_at DESC, experiment_id DESC
                                LIMIT :limit
                                """
                            ),
                            {"limit": limit},
                        )
                    )
                    .mappings()
                    .all()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "portfolio validation listing failed"
            ) from None
        return tuple(self._experiment_from_row(row) for row in rows)

    async def claim_next_experiment(
        self,
        *,
        now: datetime,
    ) -> PortfolioValidationExperiment | None:
        try:
            async with self._engine.begin() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                WITH next_experiment AS (
                                    SELECT experiment_id
                                    FROM {self._schema}.portfolio_validation_experiments
                                    WHERE state = 'queued'
                                    ORDER BY created_at, experiment_id
                                    FOR UPDATE SKIP LOCKED
                                    LIMIT 1
                                )
                                UPDATE {self._schema}.portfolio_validation_experiments
                                    AS experiments
                                SET state = 'running', started_at = :started_at
                                FROM next_experiment
                                WHERE experiments.experiment_id =
                                      next_experiment.experiment_id
                                RETURNING {_QUALIFIED_COLUMNS}
                                """
                            ),
                            {"started_at": _aware_utc(now)},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "portfolio validation claim failed"
            ) from None
        return None if row is None else self._experiment_from_row(row)

    async def complete_experiment(
        self,
        experiment_id: UUID,
        *,
        result: PortfolioWalkForwardResult,
        now: datetime,
    ) -> PortfolioValidationExperiment:
        summary = _summary(result)
        fold_parameters = [
            {
                "experiment_id": experiment_id,
                "sequence": fold.sequence,
                "train_start": fold.train_start,
                "train_end": fold.train_end,
                "test_start": fold.test_start,
                "test_end": fold.test_end,
                "selected_payload": _json(fold.selected.payload()),
                "selection_score": fold.selection_score,
                "fold_hash": fold.fold_hash,
                "training_payload": _json(
                    encode_backtest_result(fold.training_result)
                ),
                "test_payload": _json(
                    encode_backtest_result(fold.test_result)
                ),
                "benchmark_payload": _json(
                    encode_backtest_result(fold.benchmark_result)
                ),
            }
            for fold in result.folds
        ]
        try:
            async with self._engine.begin() as connection:
                experiment_row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT {_EXPERIMENT_COLUMNS}
                                FROM {self._schema}.portfolio_validation_experiments
                                WHERE experiment_id = :experiment_id
                                FOR UPDATE
                                """
                            ),
                            {"experiment_id": experiment_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if experiment_row is None:
                    raise RuntimeError(
                        "portfolio validation does not exist"
                    )
                experiment = self._experiment_from_row(
                    experiment_row
                )
                if (
                    experiment.state is not OperatorJobState.RUNNING
                    or result.manifest_hash
                    != experiment.request.manifest_hash
                    or result.config
                    != portfolio_validation_config(experiment.request)
                ):
                    raise RuntimeError(
                        "portfolio validation result does not match request"
                    )
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.portfolio_validation_folds
                            (experiment_id, sequence, train_start, train_end,
                             test_start, test_end, selected_payload,
                             selection_score, fold_hash, training_payload,
                             test_payload, benchmark_payload)
                        VALUES
                            (:experiment_id, :sequence, :train_start, :train_end,
                             :test_start, :test_end,
                             CAST(:selected_payload AS jsonb),
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
                                UPDATE {self._schema}.portfolio_validation_experiments
                                SET state = 'completed',
                                    completed_at = :completed_at,
                                    as_of = :as_of,
                                    result_hash = :result_hash,
                                    summary_payload =
                                        CAST(:summary_payload AS jsonb),
                                    error_code = NULL
                                WHERE experiment_id = :experiment_id
                                  AND state = 'running'
                                  AND manifest_hash = :manifest_hash
                                RETURNING {_EXPERIMENT_COLUMNS}
                                """
                            ),
                            {
                                "completed_at": _aware_utc(now),
                                "as_of": result.as_of,
                                "result_hash": result.result_hash,
                                "summary_payload": _json(
                                    summary.model_dump(mode="json")
                                ),
                                "experiment_id": experiment_id,
                                "manifest_hash": result.manifest_hash,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise RuntimeError(
                        "portfolio validation is not claimable"
                    )
        except Exception:
            raise PersistenceUnavailableError(
                "portfolio validation result persistence failed"
            ) from None
        return self._experiment_from_row(row)

    async def fail_experiment(
        self,
        experiment_id: UUID,
        *,
        error_code: str,
        now: datetime,
        queued: bool = False,
    ) -> PortfolioValidationExperiment:
        if not error_code or len(error_code) > 80:
            raise ValueError("error_code must contain 1-80 characters")
        expected = "queued" if queued else "running"
        try:
            async with self._engine.begin() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                UPDATE {self._schema}.portfolio_validation_experiments
                                SET state = 'failed',
                                    completed_at = :completed_at,
                                    error_code = :error_code
                                WHERE experiment_id = :experiment_id
                                  AND state = :expected
                                RETURNING {_EXPERIMENT_COLUMNS}
                                """
                            ),
                            {
                                "completed_at": _aware_utc(now),
                                "error_code": error_code,
                                "experiment_id": experiment_id,
                                "expected": expected,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "portfolio validation failure persistence failed"
            ) from None
        if row is None:
            raise PersistenceUnavailableError(
                "portfolio validation is not in the expected state"
            )
        return self._experiment_from_row(row)

    async def interrupt_running_experiments(
        self,
        *,
        now: datetime,
    ) -> int:
        try:
            async with self._engine.begin() as connection:
                result = await connection.execute(
                    text(
                        f"""
                        UPDATE {self._schema}.portfolio_validation_experiments
                        SET state = 'interrupted',
                            completed_at = :completed_at,
                            error_code = 'worker_restarted'
                        WHERE state = 'running'
                        """
                    ),
                    {"completed_at": _aware_utc(now)},
                )
        except Exception:
            raise PersistenceUnavailableError(
                "portfolio validation recovery failed"
            ) from None
        return int(result.rowcount or 0)

    async def detail(
        self,
        experiment_id: UUID,
    ) -> PortfolioValidationExperimentDetail:
        try:
            async with self._engine.connect() as connection:
                experiment_row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT {_EXPERIMENT_COLUMNS}
                                FROM {self._schema}.portfolio_validation_experiments
                                WHERE experiment_id = :experiment_id
                                """
                            ),
                            {"experiment_id": experiment_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if experiment_row is None:
                    raise LookupError(
                        "portfolio validation experiment not found"
                    )
                fold_rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.portfolio_validation_folds
                                WHERE experiment_id = :experiment_id
                                ORDER BY sequence
                                """
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
                "portfolio validation detail query failed"
            ) from None
        experiment = self._experiment_from_row(experiment_row)
        try:
            decoded = tuple(_fold_from_row(row) for row in fold_rows)
            domain_folds = tuple(value[0] for value in decoded)
            views = tuple(value[1] for value in decoded)
            result = _verify_experiment(experiment, domain_folds)
            diagnostics = (
                None
                if result is None
                else PortfolioValidationDiagnosticsView.model_validate(
                    diagnose_portfolio_validation(result).payload()
                )
            )
        except (KeyError, TypeError, ValueError):
            raise PersistenceUnavailableError(
                "stored portfolio validation failed integrity verification"
            ) from None
        return PortfolioValidationExperimentDetail(
            experiment=experiment,
            folds=views,
            diagnostics=diagnostics,
        )

    @staticmethod
    def _experiment_from_row(
        row: RowMapping,
    ) -> PortfolioValidationExperiment:
        try:
            raw_summary = row["summary_payload"]
            return PortfolioValidationExperiment(
                experiment_id=row["experiment_id"],
                state=OperatorJobState(str(row["state"])),
                validator_id=str(row["validator_id"]),
                request=PortfolioWalkForwardJobRequest.model_validate(
                    _object(row["request_payload"])
                ),
                requested_by=str(row["requested_by"]),
                created_at=row["created_at"],
                started_at=row["started_at"],
                completed_at=row["completed_at"],
                as_of=row["as_of"],
                result_hash=(
                    None
                    if row["result_hash"] is None
                    else str(row["result_hash"])
                ),
                summary=(
                    None
                    if raw_summary is None
                    else PortfolioValidationSummary.model_validate(
                        _object(raw_summary)
                    )
                ),
                error_code=(
                    None
                    if row["error_code"] is None
                    else str(row["error_code"])
                ),
            )
        except (KeyError, TypeError, ValueError):
            raise PersistenceUnavailableError(
                "stored portfolio validation experiment is malformed"
            ) from None


def portfolio_validation_config(
    request: PortfolioWalkForwardJobRequest,
) -> PortfolioWalkForwardConfig:
    return PortfolioWalkForwardConfig(
        initial_cash=request.initial_cash,
        gross_allocation=request.gross_allocation,
        maximum_order_notional=request.maximum_order_notional,
        slippage_bps=request.slippage_bps,
        train_sessions=request.train_sessions,
        test_sessions=request.test_sessions,
        embargo_sessions=request.embargo_sessions,
        candidates=tuple(
            CrossSectionalMomentumParameters(
                lookback_sessions=value.lookback_sessions,
                rebalance_sessions=value.rebalance_sessions,
                selection_count=value.selection_count,
            )
            for value in request.candidates
        ),
    )


def _summary(
    result: PortfolioWalkForwardResult,
) -> PortfolioValidationSummary:
    evidence = assess_portfolio_validation(result)
    return PortfolioValidationSummary(
        fold_count=len(result.folds),
        oos_sessions=evidence.oos_sessions,
        compounded_oos_return=result.compounded_oos_return,
        benchmark_compounded_oos_return=(
            result.benchmark_compounded_oos_return
        ),
        excess_oos_return=result.excess_oos_return,
        profitable_fold_rate=result.profitable_fold_rate,
        worst_oos_drawdown=result.worst_oos_drawdown,
        mean_training_return=result.mean_training_return,
        selection_optimism=result.selection_optimism,
        rejected_order_count=evidence.rejected_order_count,
        evidence_status=evidence.evidence_status,
        gate_failures=evidence.gate_failures,
        validation_version=result.validation_version,
        objective_version=result.objective_version,
        policy_hash=evidence.policy_hash,
        assessment_hash=evidence.assessment_hash,
    )


def _fold_from_row(
    row: RowMapping,
) -> tuple[PortfolioWalkForwardFold, PortfolioValidationFoldView]:
    selected_raw = _object(row["selected_payload"])
    selected = CrossSectionalMomentumParameters(
        lookback_sessions=int(str(selected_raw["lookback_sessions"])),
        rebalance_sessions=int(
            str(selected_raw["rebalance_sessions"])
        ),
        selection_count=int(str(selected_raw["selection_count"])),
    )
    if selected.payload() != selected_raw:
        raise ValueError("portfolio selected payload is not canonical")
    training = decode_backtest_result(row["training_payload"])
    test = decode_backtest_result(row["test_payload"])
    benchmark = decode_backtest_result(row["benchmark_payload"])
    fold = PortfolioWalkForwardFold(
        sequence=int(row["sequence"]),
        train_start=row["train_start"],
        train_end=row["train_end"],
        test_start=row["test_start"],
        test_end=row["test_end"],
        selected=selected,
        selection_score=Decimal(str(row["selection_score"])),
        training_result=training,
        test_result=test,
        benchmark_result=benchmark,
    )
    if fold.fold_hash != str(row["fold_hash"]):
        raise ValueError("portfolio validation fold hash mismatch")
    return (
        fold,
        PortfolioValidationFoldView(
            sequence=fold.sequence,
            train_start=fold.train_start,
            train_end=fold.train_end,
            test_start=fold.test_start,
            test_end=fold.test_end,
            selected=MomentumCandidateRequest(
                **selected.payload(),
            ),
            selection_score=fold.selection_score,
            training=_metrics(training),
            test=_metrics(test),
            benchmark=_metrics(benchmark),
            training_result_hash=training.result_hash,
            test_result_hash=test.result_hash,
            benchmark_result_hash=benchmark.result_hash,
            fold_hash=fold.fold_hash,
        ),
    )


def _verify_experiment(
    experiment: PortfolioValidationExperiment,
    folds: tuple[PortfolioWalkForwardFold, ...],
) -> PortfolioWalkForwardResult | None:
    if experiment.state is not OperatorJobState.COMPLETED:
        if folds:
            raise ValueError(
                "incomplete portfolio experiment cannot contain folds"
            )
        return None
    if (
        experiment.summary is None
        or experiment.as_of is None
        or experiment.result_hash is None
    ):
        raise ValueError(
            "completed portfolio experiment lacks result fields"
        )
    summary = experiment.summary
    result = PortfolioWalkForwardResult(
        manifest_hash=experiment.request.manifest_hash,
        instruments=tuple(
            sorted(
                {
                    report.instrument
                    for fold in folds
                    for report in fold.benchmark_result.reports
                }
            )
        ),
        as_of=experiment.as_of,
        config=portfolio_validation_config(experiment.request),
        folds=folds,
        compounded_oos_return=summary.compounded_oos_return,
        benchmark_compounded_oos_return=(
            summary.benchmark_compounded_oos_return
        ),
        excess_oos_return=summary.excess_oos_return,
        profitable_fold_rate=summary.profitable_fold_rate,
        worst_oos_drawdown=summary.worst_oos_drawdown,
        mean_training_return=summary.mean_training_return,
        selection_optimism=summary.selection_optimism,
        validation_version=summary.validation_version,
        objective_version=summary.objective_version,
    )
    if result.result_hash != experiment.result_hash:
        raise ValueError("portfolio validation result hash mismatch")
    expected = _summary(result)
    if expected != summary:
        raise ValueError("portfolio validation assessment mismatch")
    policy = PortfolioValidationEvidencePolicy()
    if summary.policy_hash != policy.policy_hash:
        raise ValueError("portfolio validation policy hash mismatch")
    return result


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


def _object(raw: object) -> dict[str, object]:
    value = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(value, dict):
        raise TypeError("stored portfolio payload must be an object")
    return dict(value)


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)
