from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.execution.low_volatility_decision_signal import (
    LowVolatilityDecisionTimePaperSignal,
)
from autoquant.execution.low_volatility_decision_signal_store import (
    PostgresLowVolatilityDecisionTimeSignalRepository,
)

POSTGRES_DSN = os.environ.get("AQ_POSTGRES_DSN", "").strip()
SESSION_DATE = date(2026, 7, 27)
PREPARED_AT = datetime(2026, 7, 27, 1, 5, tzinfo=UTC)
EVIDENCE_AT = PREPARED_AT - timedelta(seconds=1)
INSTRUMENTS = tuple(f"{index:06d}.XSHE" for index in range(1, 61))
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not POSTGRES_DSN,
        reason="AQ_POSTGRES_DSN is not configured; PostgreSQL infrastructure unavailable",
    ),
]


def _signal() -> LowVolatilityDecisionTimePaperSignal:
    return LowVolatilityDecisionTimePaperSignal(
        deployment_contract_hash="a" * 64,
        candidate_approval_hash="b" * 64,
        compatibility_run_hash="c" * 64,
        observation_signal_hash="d" * 64,
        reconciliation_report_hash="e" * 64,
        internal_account_snapshot_hash="f" * 64,
        broker_account_snapshot_hash="1" * 64,
        kill_switch_event_hash="2" * 64,
        account_id="paper-main",
        strategy_id="low-volatility-paper",
        source_spec_hash="3" * 64,
        forward_spec_hash="4" * 64,
        compatibility_spec_hash="5" * 64,
        risk_policy_hash="6" * 64,
        snapshot_hash="7" * 64,
        dataset_manifest_hash="8" * 64,
        rule_set_hash="9" * 64,
        session_sequence=1,
        session_date=SESSION_DATE,
        signal_date=SESSION_DATE - timedelta(days=1),
        account_evidence_at=EVIDENCE_AT,
        kill_switch_changed_at=EVIDENCE_AT - timedelta(minutes=1),
        selected_instruments=(),
        held_instruments=(INSTRUMENTS[0],),
        valuation_instruments=INSTRUMENTS,
        prepared_by="integration-risk-operator",
        prepared_at=PREPARED_AT,
    )


@pytest_asyncio.fixture
async def decision_store() -> AsyncIterator[
    tuple[
        PostgresLowVolatilityDecisionTimeSignalRepository,
        AsyncEngine,
        str,
    ]
]:
    schema = f"autoquant_test_{uuid4().hex}"
    engine = create_async_engine(POSTGRES_DSN, pool_pre_ping=True)
    control = PostgresControlRepository(engine=engine, schema=schema)
    repository = PostgresLowVolatilityDecisionTimeSignalRepository(
        engine=engine,
        schema=schema,
    )
    phase1 = Path("migrations/postgres/001_phase1.sql").read_text(
        encoding="utf-8"
    )
    migration = Path(
        "migrations/postgres/051_low_volatility_decision_time_signals.sql"
    ).read_text(encoding="utf-8")
    try:
        await control.initialize(phase1)
        async with engine.begin() as connection:
            await connection.exec_driver_sql(
                f'SET LOCAL search_path TO "{schema}"'
            )
            for statement in _prerequisite_tables():
                await connection.execute(text(statement))
            for statement, parameters in _evidence_rows():
                await connection.execute(text(statement), parameters)
        await control.initialize(migration)
        yield repository, engine, schema
    finally:
        try:
            await control.drop_test_schema()
        finally:
            await engine.dispose()


def _prerequisite_tables() -> tuple[str, ...]:
    return (
        "CREATE TABLE low_volatility_research_specs "
        "(spec_hash text PRIMARY KEY)",
        "CREATE TABLE low_volatility_forward_evidence_specs "
        "(spec_hash text PRIMARY KEY)",
        "CREATE TABLE low_volatility_execution_compatibility_specs "
        "(spec_hash text PRIMARY KEY)",
        "CREATE TABLE research_universe_snapshots "
        "(snapshot_hash text PRIMARY KEY)",
        "CREATE TABLE research_dataset_manifests "
        "(manifest_hash text PRIMARY KEY)",
        """
        CREATE TABLE low_volatility_paper_deployment_contracts
        (
            contract_hash text PRIMARY KEY,
            source_spec_hash text NOT NULL,
            forward_spec_hash text NOT NULL,
            compatibility_spec_hash text NOT NULL,
            frozen_at timestamptz NOT NULL
        )
        """,
        """
        CREATE TABLE low_volatility_paper_candidate_approvals
        (
            approval_hash text PRIMARY KEY,
            account_id text NOT NULL,
            strategy_id text NOT NULL,
            forward_spec_hash text NOT NULL,
            evaluation_result_hash text NOT NULL,
            source_spec_hash text NOT NULL,
            risk_policy_hash text NOT NULL,
            approved_at timestamptz NOT NULL
        )
        """,
        """
        CREATE TABLE low_volatility_paper_candidate_revocations
        (
            revocation_hash text,
            approval_hash text NOT NULL
        )
        """,
        """
        CREATE TABLE low_volatility_execution_compatibility_runs
        (
            run_hash text PRIMARY KEY,
            compatibility_spec_hash text NOT NULL,
            original_evaluation_result_hash text NOT NULL,
            forward_spec_hash text NOT NULL,
            source_spec_hash text NOT NULL,
            compatibility_status text NOT NULL,
            execution_timing_compatible boolean NOT NULL,
            completed_at timestamptz NOT NULL
        )
        """,
        """
        CREATE TABLE low_volatility_paper_daily_signals
        (
            signal_hash text PRIMARY KEY,
            candidate_approval_hash text NOT NULL,
            account_id text NOT NULL,
            strategy_id text NOT NULL,
            source_spec_hash text NOT NULL,
            risk_policy_hash text NOT NULL,
            session_sequence integer NOT NULL,
            session_date date NOT NULL,
            signal_date date NOT NULL,
            snapshot_hash text NOT NULL,
            dataset_manifest_hash text NOT NULL,
            rule_set_hash text NOT NULL,
            prepared_at timestamptz NOT NULL,
            payload jsonb NOT NULL
        )
        """,
        """
        CREATE TABLE execution_account_snapshots
        (
            snapshot_hash text PRIMARY KEY,
            account_id text NOT NULL,
            as_of timestamptz NOT NULL,
            payload jsonb NOT NULL
        )
        """,
        """
        CREATE TABLE execution_reconciliation_reports
        (
            report_hash text PRIMARY KEY,
            account_id text NOT NULL,
            evaluated_at timestamptz NOT NULL,
            internal_snapshot_hash text NOT NULL,
            broker_snapshot_hash text NOT NULL,
            reconciled boolean NOT NULL
        )
        """,
        "CREATE TABLE execution_control_events "
        "(event_hash text PRIMARY KEY)",
        """
        CREATE TABLE execution_control_state
        (
            account_id text PRIMARY KEY,
            active boolean NOT NULL,
            last_event_hash text NOT NULL,
            changed_at timestamptz NOT NULL
        )
        """,
    )


def _evidence_rows() -> tuple[tuple[str, dict[str, object]], ...]:
    signal = _signal()
    account_payload = json.dumps(
        {
            "account_id": signal.account_id,
            "as_of": (
                signal.account_evidence_at - timedelta(seconds=1)
            ).isoformat(),
            "cash": "999000",
            "equity": "1000000",
            "open_client_order_ids": [],
            "positions": [
                {
                    "instrument": INSTRUMENTS[0],
                    "market_value": "1000",
                    "sellable_quantity": 100,
                    "total_quantity": 100,
                }
            ],
        }
    )
    observation_payload = json.dumps(
        {
            "selected_instruments": [],
            "valuations": [
                {"instrument": instrument} for instrument in INSTRUMENTS
            ],
        }
    )
    return (
        (
            "INSERT INTO low_volatility_research_specs VALUES (:value)",
            {"value": signal.source_spec_hash},
        ),
        (
            "INSERT INTO low_volatility_forward_evidence_specs "
            "VALUES (:value)",
            {"value": signal.forward_spec_hash},
        ),
        (
            "INSERT INTO low_volatility_execution_compatibility_specs "
            "VALUES (:value)",
            {"value": signal.compatibility_spec_hash},
        ),
        (
            "INSERT INTO research_universe_snapshots VALUES (:value)",
            {"value": signal.snapshot_hash},
        ),
        (
            "INSERT INTO research_dataset_manifests VALUES (:value)",
            {"value": signal.dataset_manifest_hash},
        ),
        (
            """
            INSERT INTO low_volatility_paper_deployment_contracts
            VALUES (:hash, :source, :forward, :compatibility, :frozen_at)
            """,
            {
                "compatibility": signal.compatibility_spec_hash,
                "forward": signal.forward_spec_hash,
                "frozen_at": PREPARED_AT - timedelta(days=4),
                "hash": signal.deployment_contract_hash,
                "source": signal.source_spec_hash,
            },
        ),
        (
            """
            INSERT INTO low_volatility_paper_candidate_approvals
            VALUES (:hash, :account, :strategy, :forward,
                    :evaluation, :source, :risk, :approved_at)
            """,
            {
                "account": signal.account_id,
                "approved_at": PREPARED_AT - timedelta(days=1),
                "evaluation": "0" * 64,
                "forward": signal.forward_spec_hash,
                "hash": signal.candidate_approval_hash,
                "risk": signal.risk_policy_hash,
                "source": signal.source_spec_hash,
                "strategy": signal.strategy_id,
            },
        ),
        (
            """
            INSERT INTO low_volatility_execution_compatibility_runs
            VALUES (:hash, :spec, :evaluation, :forward, :source,
                    'compatible', true, :completed_at)
            """,
            {
                "completed_at": PREPARED_AT - timedelta(days=2),
                "evaluation": "0" * 64,
                "forward": signal.forward_spec_hash,
                "hash": signal.compatibility_run_hash,
                "source": signal.source_spec_hash,
                "spec": signal.compatibility_spec_hash,
            },
        ),
        (
            """
            INSERT INTO low_volatility_paper_daily_signals
            VALUES (:hash, :candidate, :account, :strategy, :source,
                    :risk, :sequence, :session_date, :signal_date,
                    :snapshot, :manifest, :rules, :prepared_at,
                    CAST(:payload AS jsonb))
            """,
            {
                "account": signal.account_id,
                "candidate": signal.candidate_approval_hash,
                "hash": signal.observation_signal_hash,
                "manifest": signal.dataset_manifest_hash,
                "payload": observation_payload,
                "prepared_at": PREPARED_AT - timedelta(minutes=1),
                "risk": signal.risk_policy_hash,
                "rules": signal.rule_set_hash,
                "sequence": signal.session_sequence,
                "session_date": signal.session_date,
                "signal_date": signal.signal_date,
                "snapshot": signal.snapshot_hash,
                "source": signal.source_spec_hash,
                "strategy": signal.strategy_id,
            },
        ),
        (
            """
            INSERT INTO execution_account_snapshots
            VALUES (:hash, :account, :as_of, CAST(:payload AS jsonb))
            """,
            {
                "account": signal.account_id,
                "as_of": signal.account_evidence_at
                - timedelta(seconds=1),
                "hash": signal.internal_account_snapshot_hash,
                "payload": account_payload,
            },
        ),
        (
            """
            INSERT INTO execution_account_snapshots
            VALUES (:hash, :account, :as_of, CAST(:payload AS jsonb))
            """,
            {
                "account": signal.account_id,
                "as_of": signal.account_evidence_at
                - timedelta(seconds=1),
                "hash": signal.broker_account_snapshot_hash,
                "payload": account_payload,
            },
        ),
        (
            """
            INSERT INTO execution_reconciliation_reports
            VALUES (:hash, :account, :evaluated_at, :internal,
                    :broker, true)
            """,
            {
                "account": signal.account_id,
                "broker": signal.broker_account_snapshot_hash,
                "evaluated_at": signal.account_evidence_at,
                "hash": signal.reconciliation_report_hash,
                "internal": signal.internal_account_snapshot_hash,
            },
        ),
        (
            "INSERT INTO execution_control_events VALUES (:hash)",
            {"hash": signal.kill_switch_event_hash},
        ),
        (
            """
            INSERT INTO execution_control_state
            VALUES (:account, true, :event_hash, :changed_at)
            """,
            {
                "account": signal.account_id,
                "changed_at": signal.kill_switch_changed_at,
                "event_hash": signal.kill_switch_event_hash,
            },
        ),
    )


@pytest.mark.asyncio
async def test_decision_signal_is_database_verified_and_immutable(
    decision_store: tuple[
        PostgresLowVolatilityDecisionTimeSignalRepository,
        AsyncEngine,
        str,
    ],
) -> None:
    repository, engine, schema = decision_store
    signal = _signal()

    assert await repository.save(signal) == signal
    assert await repository.read(signal.signal_hash) == signal
    assert (
        await repository.for_session(
            candidate_approval_hash=signal.candidate_approval_hash,
            session_date=signal.session_date,
        )
        == signal
    )

    with pytest.raises(DBAPIError):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"""
                    UPDATE "{schema}".
                        low_volatility_decision_time_paper_signals
                    SET prepared_by = 'tampered'
                    WHERE signal_hash = :signal_hash
                    """
                ),
                {"signal_hash": signal.signal_hash},
            )
    with pytest.raises(DBAPIError):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"""
                    DELETE FROM "{schema}".
                        low_volatility_decision_time_paper_signals
                    WHERE signal_hash = :signal_hash
                    """
                ),
                {"signal_hash": signal.signal_hash},
            )

    assert await repository.read(signal.signal_hash) == signal
