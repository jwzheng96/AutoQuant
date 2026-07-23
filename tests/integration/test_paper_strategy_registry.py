from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.data.models import DatasetManifest
from autoquant.data.quality import QualityReport
from autoquant.execution.control import KillSwitchReason
from autoquant.execution.control_store import (
    PostgresExecutionControlRepository,
)
from autoquant.execution.paper_deployment import (
    PostgresPaperDeploymentRegistry,
)
from autoquant.execution.paper_scheduler_lease_store import (
    PostgresPaperSchedulerLeaseRepository,
)
from autoquant.execution.paper_unlock import (
    PaperRuntimeUnlockEvidence,
    PostgresPaperRuntimeUnlockRepository,
)
from autoquant.execution.portfolio_validation import (
    PortfolioOosComponentEvidence,
    PortfolioOosFold,
    assess_portfolio_oos,
)
from autoquant.execution.promotion_audit import (
    PaperPromotionAuditor,
    PostgresPaperPromotionFactRepository,
    PromotionGateCode,
)
from autoquant.execution.qmt_readonly import (
    build_qmt_readonly_baseline,
    normalize_qmt_asset,
)
from autoquant.execution.qmt_readonly_store import (
    PostgresQmtReadOnlyAcceptanceRepository,
    QmtReadOnlyAcceptanceEvidence,
)
from autoquant.execution.qmt_recovery_drill import (
    PostgresQmtRecoveryDrillRepository,
    QmtRecoveryDrillAction,
    QmtRecoveryDrillKind,
)
from autoquant.execution.qmt_session_store import (
    PostgresQmtSessionLeaseRepository,
)
from autoquant.execution.reconciliation import (
    AccountReconciler,
    ExecutionAccountSnapshot,
)
from autoquant.execution.session_risk import (
    SessionRiskObservation,
    derive_session_turnover,
)
from autoquant.execution.session_risk_store import (
    PostgresPaperSessionRiskRepository,
)
from autoquant.execution.store import PostgresPaperExecutionRepository
from autoquant.execution.strategy_portfolio_store import (
    PostgresPaperPortfolioRegistry,
)
from autoquant.execution.strategy_registry_store import (
    PostgresPaperStrategyRegistry,
)
from autoquant.execution.validated_sma import ValidatedSmaRegistration
from autoquant.execution.validated_sma_portfolio import (
    ValidatedSmaPortfolioRegistration,
)
from autoquant.risk.models import RiskPolicy
from autoquant.web.models import WalkForwardJobRequest
from autoquant.web.validation_store import PostgresValidationRepository

POSTGRES_DSN = os.environ.get("AQ_POSTGRES_DSN", "").strip()
AS_OF = datetime(2026, 7, 22, 8, tzinfo=UTC)
APPROVED_AT = datetime(2026, 7, 22, 9, tzinfo=UTC)
INSTRUMENT = "600000.XSHG"
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not POSTGRES_DSN,
        reason="AQ_POSTGRES_DSN is not configured; PostgreSQL infrastructure unavailable",
    ),
]


@pytest_asyncio.fixture
async def registry_fixture() -> AsyncIterator[
    tuple[PostgresPaperStrategyRegistry, ValidatedSmaRegistration, AsyncEngine, str]
]:
    schema = f"autoquant_test_{uuid4().hex}"
    control = PostgresControlRepository.connect(dsn=POSTGRES_DSN, schema=schema)
    validations = PostgresValidationRepository.connect(
        dsn=POSTGRES_DSN,
        schema=schema,
    )
    registry = PostgresPaperStrategyRegistry.connect(
        dsn=POSTGRES_DSN,
        schema=schema,
    )
    engine = create_async_engine(POSTGRES_DSN, pool_pre_ping=True)
    migration = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "migrations/postgres/001_phase1.sql",
            "migrations/postgres/005_walk_forward_validation.sql",
            "migrations/postgres/006_validation_benchmark.sql",
            "migrations/postgres/007_risk_decisions.sql",
            "migrations/postgres/008_paper_execution.sql",
            "migrations/postgres/009_execution_controls.sql",
            "migrations/postgres/011_paper_session_risk.sql",
            "migrations/postgres/012_qmt_session_leases.sql",
            "migrations/postgres/013_paper_scheduler_events.sql",
            "migrations/postgres/014_paper_scheduler_leases.sql",
            "migrations/postgres/015_paper_strategy_registry.sql",
            "migrations/postgres/016_paper_runtime_unlock.sql",
            "migrations/postgres/017_qmt_readonly_acceptance.sql",
            "migrations/postgres/018_qmt_recovery_drills.sql",
            "migrations/postgres/019_paper_portfolio_registry.sql",
            "migrations/postgres/020_portfolio_oos_assessment.sql",
        )
    )
    report = QualityReport(
        requested_instruments=(INSTRUMENT,),
        start=datetime(2025, 1, 1, tzinfo=UTC),
        end=datetime(2026, 7, 22, 7, tzinfo=UTC),
        as_of=AS_OF,
        issues=(),
        production_complete=True,
    )
    manifest = DatasetManifest(
        source="tushare",
        instruments=(INSTRUMENT,),
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2026, 7, 22, 7, tzinfo=UTC),
        as_of=AS_OF,
        record_hashes=("a" * 64,),
        quality_report_hash=report.report_hash,
        production_complete=True,
        row_count=1,
    )
    try:
        await control.initialize(migration)
        await control.save_quality_report(report)
        await control.save_manifest(manifest)
        experiment = await validations.create_experiment(
            WalkForwardJobRequest(
                manifest_hash=manifest.manifest_hash,
                instrument=INSTRUMENT,
                allocation=Decimal("0.20"),
                slippage_bps=Decimal("5"),
                train_sessions=60,
                test_sessions=20,
                candidates=({"fast_sessions": 5, "slow_sessions": 20},),
                idempotency_key="paper-registry-integration-0001",
            ),
            requested_by="researcher",
            now=AS_OF,
        )
        async with engine.begin() as connection:
            await connection.exec_driver_sql(
                f'SET LOCAL search_path TO "{schema}"'
            )
            await connection.execute(
                text(
                    """
                    UPDATE validation_experiments
                    SET state = 'completed', started_at = :as_of,
                        completed_at = :as_of, as_of = :as_of,
                        result_hash = :result_hash,
                        summary_payload = CAST(:summary AS jsonb)
                    WHERE experiment_id = :experiment_id
                    """
                ),
                {
                    "as_of": AS_OF,
                    "experiment_id": experiment.experiment_id,
                    "result_hash": "b" * 64,
                    "summary": json.dumps(
                        {
                            "evidence_status": "research_candidate",
                            "gate_failures": [],
                        }
                    ),
                },
            )
            for sequence in range(1, 7):
                await connection.execute(
                    text(
                        """
                        INSERT INTO validation_folds
                            (experiment_id, sequence, train_start, train_end,
                             test_start, test_end, selected_fast, selected_slow,
                             selection_score, fold_hash, training_payload,
                             test_payload, benchmark_payload)
                        VALUES
                            (:experiment_id, :sequence, '2025-01-01',
                             '2025-03-01', '2025-03-03', '2025-03-22',
                             5, 20, 1, :fold_hash, '{}'::jsonb, '{}'::jsonb,
                             '{}'::jsonb)
                        """
                    ),
                    {
                        "experiment_id": experiment.experiment_id,
                        "sequence": sequence,
                        "fold_hash": f"{sequence:064x}",
                    },
                )
        rules = AshareRuleBook().resolve(
            INSTRUMENT,
            date(2026, 7, 23),
            SecurityStatus(risk_warning=False, listing_session_number=1000),
        )
        policy = RiskPolicy(
            allowed_instruments=(INSTRUMENT,),
            max_position_weight=Decimal("0.20"),
            max_gross_exposure=Decimal("0.20"),
        )
        registration = ValidatedSmaRegistration(
            account_id="paper-main",
            strategy_id="validated-sma-paper",
            strategy_version="sma-paper-v1:integration:5-20",
            experiment_id=experiment.experiment_id,
            validation_result_hash="b" * 64,
            validation_manifest_hash=manifest.manifest_hash,
            signal_manifest_hash=manifest.manifest_hash,
            signal_manifest_as_of=manifest.as_of,
            instrument=INSTRUMENT,
            fast_sessions=5,
            slow_sessions=20,
            allocation=Decimal("0.20"),
            slippage_bps=Decimal("5"),
            risk_policy_hash=policy.policy_hash,
            rule_version=rules.rule_version,
            approved_by="operator",
            approved_at=APPROVED_AT,
        )
        yield registry, registration, engine, schema
    finally:
        try:
            await registry.close()
        finally:
            try:
                await validations.close()
            finally:
                try:
                    await engine.dispose()
                finally:
                    try:
                        await control.drop_test_schema()
                    finally:
                        await control.close()


@pytest.mark.asyncio
async def test_registry_replays_immutable_approval_and_revocation_chain(
    registry_fixture: tuple[
        PostgresPaperStrategyRegistry,
        ValidatedSmaRegistration,
        AsyncEngine,
        str,
    ],
) -> None:
    registry, registration, _, _ = registry_fixture

    first = await registry.approve(registration)
    repeated = await registry.approve(registration)
    active = await registry.active(
        account_id=registration.account_id,
        strategy_id=registration.strategy_id,
    )

    assert first == repeated == active

    with pytest.raises(ValueError, match="revoked before replacement"):
        await registry.approve(
            replace(registration, strategy_version="replacement-v2")
        )

    await registry.revoke(
        account_id=registration.account_id,
        strategy_id=registration.strategy_id,
        revoked_by="operator",
        reason="scheduled_research_refresh",
        revoked_at=APPROVED_AT,
    )

    assert (
        await registry.active(
            account_id=registration.account_id,
            strategy_id=registration.strategy_id,
        )
        is None
    )


@pytest.mark.asyncio
async def test_registry_tables_reject_mutation(
    registry_fixture: tuple[
        PostgresPaperStrategyRegistry,
        ValidatedSmaRegistration,
        AsyncEngine,
        str,
    ],
) -> None:
    registry, registration, engine, schema = registry_fixture
    await registry.approve(registration)

    with pytest.raises(SQLAlchemyError):
        async with engine.begin() as connection:
            await connection.exec_driver_sql(
                f'SET LOCAL search_path TO "{schema}"'
            )
            await connection.execute(
                text(
                    """
                    UPDATE paper_strategy_registrations
                    SET approved_by = 'tampered'
                    WHERE registration_hash = :registration_hash
                    """
                ),
                {"registration_hash": registration.registration_hash},
            )


@pytest.mark.asyncio
async def test_portfolio_registry_activates_only_independent_components(
    registry_fixture: tuple[
        PostgresPaperStrategyRegistry,
        ValidatedSmaRegistration,
        AsyncEngine,
        str,
    ],
) -> None:
    single_registry, base, engine, schema = registry_fixture
    control = PostgresControlRepository(engine=engine, schema=schema)
    validations = PostgresValidationRepository(
        engine=engine,
        schema=schema,
    )
    portfolio_registry = PostgresPaperPortfolioRegistry(
        engine=engine,
        schema=schema,
    )
    instruments = (
        "000001.XSHE",
        "600000.XSHG",
        "600519.XSHG",
    )
    policy = RiskPolicy(
        allowed_instruments=instruments,
        max_position_weight=Decimal("0.20"),
        max_gross_exposure=Decimal("0.60"),
    )
    components = [
        replace(
            base,
            strategy_version="sma-paper-v1:600000:5-20",
            risk_policy_hash=policy.policy_hash,
        )
    ]
    for index, instrument in enumerate(
        ("000001.XSHE", "600519.XSHG"),
        start=2,
    ):
        report = QualityReport(
            requested_instruments=(instrument,),
            start=datetime(2025, 1, 1, tzinfo=UTC),
            end=datetime(2026, 7, 22, 7, tzinfo=UTC),
            as_of=AS_OF,
            issues=(),
            production_complete=True,
        )
        manifest = DatasetManifest(
            source="tushare",
            instruments=(instrument,),
            start_time=datetime(2025, 1, 1, tzinfo=UTC),
            end_time=datetime(2026, 7, 22, 7, tzinfo=UTC),
            as_of=AS_OF,
            record_hashes=(f"{index:x}" * 64,),
            quality_report_hash=report.report_hash,
            production_complete=True,
            row_count=1,
        )
        await control.save_quality_report(report)
        await control.save_manifest(manifest)
        experiment = await validations.create_experiment(
            WalkForwardJobRequest(
                manifest_hash=manifest.manifest_hash,
                instrument=instrument,
                allocation=Decimal("0.20"),
                slippage_bps=Decimal("5"),
                train_sessions=60,
                test_sessions=20,
                candidates=(
                    {"fast_sessions": 5, "slow_sessions": 20},
                ),
                idempotency_key=(
                    f"paper-portfolio-integration-000{index}"
                ),
            ),
            requested_by="researcher",
            now=AS_OF,
        )
        async with engine.begin() as connection:
            await connection.exec_driver_sql(
                f'SET LOCAL search_path TO "{schema}"'
            )
            await connection.execute(
                text(
                    """
                    UPDATE validation_experiments
                    SET state = 'completed', started_at = :as_of,
                        completed_at = :as_of, as_of = :as_of,
                        result_hash = :result_hash,
                        summary_payload = CAST(:summary AS jsonb)
                    WHERE experiment_id = :experiment_id
                    """
                ),
                {
                    "as_of": AS_OF,
                    "experiment_id": experiment.experiment_id,
                    "result_hash": f"{index + 3:x}" * 64,
                    "summary": json.dumps(
                        {
                            "evidence_status": "research_candidate",
                            "gate_failures": [],
                        }
                    ),
                },
            )
            for sequence in range(1, 7):
                await connection.execute(
                    text(
                        """
                        INSERT INTO validation_folds
                            (experiment_id, sequence, train_start, train_end,
                             test_start, test_end, selected_fast, selected_slow,
                             selection_score, fold_hash, training_payload,
                             test_payload, benchmark_payload)
                        VALUES
                            (:experiment_id, :sequence, '2025-01-01',
                             '2025-03-01', '2025-03-03', '2025-03-22',
                             5, 20, 1, :fold_hash, '{}'::jsonb, '{}'::jsonb,
                             '{}'::jsonb)
                        """
                    ),
                    {
                        "experiment_id": experiment.experiment_id,
                        "sequence": sequence,
                        "fold_hash": (
                            f"{index * 10 + sequence:064x}"
                        ),
                    },
                )
        rules = AshareRuleBook().resolve(
            instrument,
            date(2026, 7, 23),
            SecurityStatus(
                risk_warning=False,
                listing_session_number=1000,
            ),
        )
        components.append(
            ValidatedSmaRegistration(
                account_id="paper-main",
                strategy_id="validated-sma-paper",
                strategy_version=f"sma-paper-v1:{instrument}:5-20",
                experiment_id=experiment.experiment_id,
                validation_result_hash=f"{index + 3:x}" * 64,
                validation_manifest_hash=manifest.manifest_hash,
                signal_manifest_hash=manifest.manifest_hash,
                signal_manifest_as_of=manifest.as_of,
                instrument=instrument,
                fast_sessions=5,
                slow_sessions=20,
                allocation=Decimal("0.20"),
                slippage_bps=Decimal("5"),
                risk_policy_hash=policy.policy_hash,
                rule_version=rules.rule_version,
                approved_by="operator",
                approved_at=APPROVED_AT,
            )
        )
    valuation_report = QualityReport(
        requested_instruments=instruments,
        start=datetime(2025, 1, 1, tzinfo=UTC),
        end=datetime(2026, 7, 22, 7, tzinfo=UTC),
        as_of=AS_OF,
        issues=(),
        production_complete=True,
    )
    valuation_manifest = DatasetManifest(
        source="tushare",
        instruments=instruments,
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2026, 7, 22, 7, tzinfo=UTC),
        as_of=AS_OF,
        record_hashes=("7" * 64, "8" * 64, "9" * 64),
        quality_report_hash=valuation_report.report_hash,
        production_complete=True,
        row_count=3,
    )
    await control.save_quality_report(valuation_report)
    await control.save_manifest(valuation_manifest)
    assessment = assess_portfolio_oos(
        tuple(
            PortfolioOosComponentEvidence(
                experiment_id=component.experiment_id,
                validation_result_hash=(
                    component.validation_result_hash
                ),
                instrument=component.instrument,
                allocation=component.allocation,
                folds=tuple(
                    PortfolioOosFold(
                        sequence=sequence,
                        test_start=date(2024, sequence, 1),
                        test_end=date(2024, sequence, 20),
                        total_return=Decimal(value),
                        max_drawdown=Decimal("0.01"),
                    )
                    for sequence, value in enumerate(
                        component_returns,
                        start=1,
                    )
                ),
            )
            for component, component_returns in zip(
                components,
                (
                    (
                        "0.010",
                        "0.020",
                        "-0.005",
                        "0.015",
                        "0.003",
                        "0.012",
                    ),
                    (
                        "0.008",
                        "-0.003",
                        "0.018",
                        "0.004",
                        "0.014",
                        "0.006",
                    ),
                    (
                        "-0.002",
                        "0.011",
                        "0.005",
                        "0.017",
                        "0.007",
                        "0.009",
                    ),
                ),
                strict=True,
            )
        )
    )
    assert assessment.passed
    portfolio = ValidatedSmaPortfolioRegistration(
        account_id="paper-main",
        strategy_id="validated-sma-paper",
        strategy_version="sma-portfolio-paper-v1:integration",
        components=tuple(components),
        oos_assessment=assessment,
        valuation_manifest_hash=valuation_manifest.manifest_hash,
        valuation_manifest_as_of=valuation_manifest.as_of,
        risk_policy_hash=policy.policy_hash,
        approved_by="operator",
        approved_at=APPROVED_AT,
    )

    stored = await portfolio_registry.approve(portfolio)
    deployment = await PostgresPaperDeploymentRegistry(
        singles=single_registry,
        portfolios=portfolio_registry,
    ).active(
        account_id="paper-main",
        strategy_id="validated-sma-paper",
    )

    assert stored == deployment == portfolio
    with pytest.raises(ValueError, match="portfolio must be revoked"):
        await single_registry.approve(components[0])
    with pytest.raises(SQLAlchemyError):
        async with engine.begin() as connection:
            await connection.exec_driver_sql(
                f'SET LOCAL search_path TO "{schema}"'
            )
            await connection.execute(
                text(
                    """
                    UPDATE paper_portfolio_registrations
                    SET oos_assessment_hash = :tampered
                    WHERE registration_hash = :registration_hash
                    """
                ),
                {
                    "registration_hash": portfolio.registration_hash,
                    "tampered": "f" * 64,
                },
            )


@pytest.mark.asyncio
async def test_paper_runtime_reset_atomically_rechecks_persisted_fences(
    registry_fixture: tuple[
        PostgresPaperStrategyRegistry,
        ValidatedSmaRegistration,
        AsyncEngine,
        str,
    ],
) -> None:
    registry, registration, engine, schema = registry_fixture
    await registry.approve(registration)
    controls = PostgresExecutionControlRepository(engine=engine, schema=schema)
    executions = PostgresPaperExecutionRepository(engine=engine, schema=schema)
    sessions = PostgresPaperSessionRiskRepository(engine=engine, schema=schema)
    leases = PostgresPaperSchedulerLeaseRepository(engine=engine, schema=schema)
    unlocks = PostgresPaperRuntimeUnlockRepository(engine=engine, schema=schema)
    await unlocks.check_connection()
    now = datetime(2026, 7, 23, 2, tzinfo=UTC)
    session_date = date(2026, 7, 23)
    active = await controls.ensure_fail_closed(
        account_id="paper-main",
        now=now,
    )
    snapshot = ExecutionAccountSnapshot(
        account_id="paper-main",
        as_of=now,
        cash=Decimal("1000000"),
        equity=Decimal("1000000"),
    )
    report = AccountReconciler().reconcile(
        internal=snapshot,
        broker=snapshot,
        now=now,
    )
    await executions.save_reconciliation(
        internal=snapshot,
        broker=snapshot,
        report=report,
    )
    turnover = derive_session_turnover(
        account_id="paper-main",
        session_date=session_date,
        histories=(),
    )
    session = await sessions.initialize(
        SessionRiskObservation(
            account_id="paper-main",
            session_date=session_date,
            as_of=now,
            equity=snapshot.equity,
            cumulative_turnover=turnover.cumulative_turnover,
            snapshot_hash=snapshot.snapshot_hash,
            turnover_evidence_hash=turnover.evidence_hash,
        )
    )
    token = SecretStr("integration-paper-runtime-token-0001")
    lease = await leases.acquire(
        account_id="paper-main",
        strategy_id="validated-sma-paper",
        holder_id="paper-node-01",
        token=token,
        now=now,
        ttl=timedelta(seconds=30),
    )
    evidence = PaperRuntimeUnlockEvidence(
        account_id="paper-main",
        strategy_id="validated-sma-paper",
        session_date=session_date,
        evaluated_at=now,
        registration_hash=registration.registration_hash,
        calendar_hash="c" * 64,
        session_state_hash=session.state_hash,
        quote_evidence_hash="d" * 64,
        reconciliation_report_hash=report.report_hash,
        lease_holder_id=lease.holder_id,
        lease_token_hash=lease.token_hash,
        lease_generation=lease.generation,
    )
    await unlocks.append(evidence)

    reset = await controls.reset_paper_runtime(
        evidence=evidence,
        lease_token=token,
        command_id="paper-runtime-integration-reset-0001",
        actor="operator",
        now=now + timedelta(seconds=1),
        expected_version=active.version,
    )

    assert not reset.active
    assert reset.last_event_hash
    assert await unlocks.get(evidence_hash=evidence.evidence_hash) == evidence

    reactivated = await controls.activate(
        account_id="paper-main",
        command_id="paper-runtime-reactivate-integration-0001",
        reason=KillSwitchReason.MANUAL,
        actor="operator",
        now=now + timedelta(milliseconds=1100),
    )
    await leases.release(
        account_id="paper-main",
        strategy_id="validated-sma-paper",
        holder_id="paper-node-01",
        token=token,
        now=now + timedelta(milliseconds=1200),
    )
    with pytest.raises(ValueError, match="lease changed"):
        await controls.reset_paper_runtime(
            evidence=evidence,
            lease_token=token,
            command_id="paper-runtime-lost-lease-reset-0001",
            actor="operator",
            now=now + timedelta(seconds=2),
            expected_version=reactivated.version,
        )


@pytest.mark.asyncio
async def test_qmt_readonly_acceptance_persists_only_redacted_fenced_evidence(
    registry_fixture: tuple[
        PostgresPaperStrategyRegistry,
        ValidatedSmaRegistration,
        AsyncEngine,
        str,
    ],
) -> None:
    _, _, engine, schema = registry_fixture
    controls = PostgresExecutionControlRepository(
        engine=engine,
        schema=schema,
    )
    leases = PostgresQmtSessionLeaseRepository(
        engine=engine,
        schema=schema,
    )
    acceptances = PostgresQmtReadOnlyAcceptanceRepository(
        engine=engine,
        schema=schema,
    )
    await acceptances.check_connection()
    now = datetime(2026, 7, 23, 3, tzinfo=UTC)
    await controls.ensure_fail_closed(
        account_id="paper-main",
        now=now,
    )
    token = SecretStr("integration-qmt-readonly-token-0001")
    lease = await leases.acquire(
        session_id=731101,
        holder_id="windows-qmt-01",
        token=token,
        now=now,
        ttl=timedelta(seconds=30),
    )
    broker_account_id = "sensitive-broker-account"
    baseline = build_qmt_readonly_baseline(
        baseline_id="qmt-readonly-integration-0001",
        generation=1,
        logical_account_id="paper-main",
        query_started_at=now,
        query_completed_at=now + timedelta(milliseconds=10),
        callback_cursor_before=0,
        callback_cursor_after=0,
        callback_stream_healthy=True,
        asset=normalize_qmt_asset(
            {
                "account_id": broker_account_id,
                "cash": 1_000_000,
                "frozen_cash": 0,
                "market_value": 0,
                "total_asset": 1_000_000,
            },
            expected_account_id=broker_account_id,
            observed_at=now + timedelta(milliseconds=10),
        ),
        positions=(),
        orders=(),
        trades=(),
    )
    evidence = QmtReadOnlyAcceptanceEvidence.from_baseline(
        baseline=baseline,
        package_manifest_hash="e" * 64,
        lease=lease,
    )

    stored = await acceptances.append(
        evidence,
        now=now + timedelta(milliseconds=20),
    )

    assert stored == evidence
    assert (
        await acceptances.latest(logical_account_id="paper-main")
    ) == evidence
    async with engine.connect() as connection:
        payload = await connection.scalar(
            text(
                f"""
                SELECT evidence_payload::text
                FROM {schema}.qmt_readonly_acceptance_evidence
                WHERE evidence_hash = :evidence_hash
                """
            ),
            {"evidence_hash": evidence.evidence_hash},
        )
    assert broker_account_id not in str(payload)
    assert token.get_secret_value() not in str(payload)
    with pytest.raises(SQLAlchemyError):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"""
                    UPDATE {schema}.qmt_readonly_acceptance_evidence
                    SET position_count = 99
                    WHERE evidence_hash = :evidence_hash
                    """
                ),
                {"evidence_hash": evidence.evidence_hash},
            )

    await leases.release(
        session_id=731101,
        holder_id="windows-qmt-01",
        token=token,
        now=now + timedelta(seconds=1),
    )
    with pytest.raises(ValueError, match="lease changed"):
        await acceptances.append(
            evidence,
            now=now + timedelta(seconds=2),
        )


@pytest.mark.asyncio
async def test_promotion_facts_are_read_from_one_fail_closed_snapshot(
    registry_fixture: tuple[
        PostgresPaperStrategyRegistry,
        ValidatedSmaRegistration,
        AsyncEngine,
        str,
    ],
) -> None:
    registry, registration, engine, schema = registry_fixture
    await registry.approve(registration)
    controls = PostgresExecutionControlRepository(
        engine=engine,
        schema=schema,
    )
    state = await controls.ensure_fail_closed(
        account_id=registration.account_id,
        now=APPROVED_AT,
    )
    repository = PostgresPaperPromotionFactRepository(
        engine=engine,
        schema=schema,
    )

    facts = await repository.read(
        account_id=registration.account_id,
        strategy_id=registration.strategy_id,
        now=APPROVED_AT + timedelta(hours=1),
        lookback_days=180,
    )
    report = PaperPromotionAuditor().evaluate(facts)

    assert facts.kill_switch_active is True
    assert facts.control_state_hash == state.state_hash
    assert (
        facts.active_registration_hash
        == registration.registration_hash
    )
    assert facts.sessions == ()
    assert facts.scheduler_sessions == ()
    assert facts.filled_order_count == 0
    assert report.live_trading_ready is False
    assert PromotionGateCode.PAPER_SESSION_COUNT in report.blockers
    assert PromotionGateCode.QMT_ACCEPTANCE_FRESH in report.blockers


@pytest.mark.asyncio
async def test_qmt_recovery_drill_requires_failure_and_new_acceptance(
    registry_fixture: tuple[
        PostgresPaperStrategyRegistry,
        ValidatedSmaRegistration,
        AsyncEngine,
        str,
    ],
) -> None:
    _, registration, engine, schema = registry_fixture
    controls = PostgresExecutionControlRepository(
        engine=engine,
        schema=schema,
    )
    drills = PostgresQmtRecoveryDrillRepository(
        engine=engine,
        schema=schema,
    )
    now = datetime(2026, 7, 23, 4, tzinfo=UTC)
    await controls.ensure_fail_closed(
        account_id=registration.account_id,
        now=now,
    )
    await _insert_qmt_acceptance(
        engine=engine,
        schema=schema,
        account_id=registration.account_id,
        evidence_hash="8" * 64,
        observed_at=now,
    )

    started = await drills.start(
        account_id=registration.account_id,
        kind=QmtRecoveryDrillKind.DISCONNECT,
        actor="operator",
        now=now + timedelta(seconds=1),
    )
    with pytest.raises(ValueError, match="fail-closed event"):
        await drills.complete(
            drill_id=started.drill_id,
            actor="operator",
            now=now + timedelta(seconds=2),
        )

    await controls.activate(
        account_id=registration.account_id,
        command_id="qmt-disconnect-drill-failure-0001",
        reason=KillSwitchReason.DEPENDENCY_UNAVAILABLE,
        actor="resident-paper-runtime",
        now=now + timedelta(seconds=3),
    )
    with pytest.raises(ValueError, match="post-failure acceptance"):
        await drills.complete(
            drill_id=started.drill_id,
            actor="operator",
            now=now + timedelta(seconds=4),
        )

    await _insert_qmt_acceptance(
        engine=engine,
        schema=schema,
        account_id=registration.account_id,
        evidence_hash="9" * 64,
        observed_at=now + timedelta(seconds=5),
    )
    completed = await drills.complete(
        drill_id=started.drill_id,
        actor="operator",
        now=now + timedelta(seconds=6),
    )
    replayed = await drills.complete(
        drill_id=started.drill_id,
        actor="operator",
        now=now + timedelta(seconds=7),
    )

    assert completed.action is QmtRecoveryDrillAction.COMPLETE
    assert completed.previous_hash == started.event_hash
    assert completed.recovery_qmt_evidence_hash == "9" * 64
    assert replayed == completed


async def _insert_qmt_acceptance(
    *,
    engine: AsyncEngine,
    schema: str,
    account_id: str,
    evidence_hash: str,
    observed_at: datetime,
) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                f"""
                INSERT INTO {schema}.qmt_readonly_acceptance_evidence
                    (evidence_hash, logical_account_id, observed_at,
                     baseline_evidence_hash, account_snapshot_hash,
                     package_manifest_hash, position_count, order_count,
                     trade_count, callback_cursor, lease_session_id,
                     lease_holder_id, lease_token_hash, lease_generation,
                     evidence_payload)
                VALUES
                    (:evidence_hash, :account_id, :observed_at,
                     :baseline_hash, :snapshot_hash, :package_hash,
                     0, 0, 0, 0, 731102, 'windows-qmt-drill',
                     :token_hash, 1, '{{}}'::jsonb)
                """
            ),
            {
                "evidence_hash": evidence_hash,
                "account_id": account_id,
                "observed_at": observed_at,
                "baseline_hash": "4" * 64,
                "snapshot_hash": "5" * 64,
                "package_hash": "6" * 64,
                "token_hash": "7" * 64,
            },
        )
