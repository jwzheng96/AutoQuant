from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.low_volatility_paper_deployment_contract import (
    LowVolatilityPaperDeploymentContract,
)
from autoquant.execution.low_volatility_paper_deployment_contract_store import (
    PostgresLowVolatilityPaperDeploymentContractRepository,
)

POSTGRES_DSN = os.environ.get("AQ_POSTGRES_DSN", "").strip()
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not POSTGRES_DSN,
        reason="AQ_POSTGRES_DSN is not configured; PostgreSQL infrastructure unavailable",
    ),
]


@pytest_asyncio.fixture
async def contract_store() -> AsyncIterator[
    tuple[
        PostgresLowVolatilityPaperDeploymentContractRepository,
        AsyncEngine,
        str,
    ]
]:
    schema = f"autoquant_test_{uuid4().hex}"
    engine = create_async_engine(POSTGRES_DSN, pool_pre_ping=True)
    control = PostgresControlRepository(engine=engine, schema=schema)
    repository = PostgresLowVolatilityPaperDeploymentContractRepository(
        engine=engine,
        schema=schema,
    )
    phase1 = Path("migrations/postgres/001_phase1.sql").read_text(encoding="utf-8")
    deployment = Path(
        "migrations/postgres/050_low_volatility_paper_deployment_contracts.sql"
    ).read_text(encoding="utf-8")
    try:
        await control.initialize(phase1)
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
            for statement in (
                """
                    CREATE TABLE low_volatility_research_specs
                    (
                        spec_hash text PRIMARY KEY
                    )
                """,
                """
                    CREATE TABLE low_volatility_forward_evidence_specs
                    (
                        spec_hash text PRIMARY KEY,
                        source_spec_hash text NOT NULL REFERENCES
                            low_volatility_research_specs(spec_hash)
                    )
                """,
                """
                    CREATE TABLE
                        low_volatility_execution_compatibility_specs
                    (
                        spec_hash text PRIMARY KEY,
                        source_spec_hash text NOT NULL REFERENCES
                            low_volatility_research_specs(spec_hash),
                        forward_spec_hash text NOT NULL REFERENCES
                            low_volatility_forward_evidence_specs(spec_hash)
                    )
                """,
                """
                    CREATE TABLE low_volatility_forward_sessions
                    (
                        forward_spec_hash text NOT NULL
                    )
                """,
                """
                    CREATE TABLE low_volatility_forward_evaluation_runs
                    (
                        forward_spec_hash text NOT NULL
                    )
                """,
                """
                    CREATE TABLE
                        low_volatility_execution_compatibility_runs
                    (
                        compatibility_spec_hash text NOT NULL
                    )
                """,
                """
                    CREATE TABLE low_volatility_paper_candidate_approvals
                    (
                        forward_spec_hash text NOT NULL
                    )
                """,
            ):
                await connection.execute(text(statement))
            await connection.execute(
                text(
                    """
                    INSERT INTO low_volatility_research_specs(spec_hash)
                    VALUES (:source), (:late_source)
                    """
                ),
                {
                    "late_source": "d" * 64,
                    "source": "a" * 64,
                },
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO low_volatility_forward_evidence_specs
                        (spec_hash, source_spec_hash)
                    VALUES
                        (:forward, :source),
                        (:late_forward, :late_source)
                    """
                ),
                {
                    "forward": "b" * 64,
                    "late_forward": "e" * 64,
                    "late_source": "d" * 64,
                    "source": "a" * 64,
                },
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO
                        low_volatility_execution_compatibility_specs
                        (spec_hash, source_spec_hash, forward_spec_hash)
                    VALUES
                        (:compatibility, :source, :forward),
                        (:late_compatibility, :late_source,
                         :late_forward)
                    """
                ),
                {
                    "compatibility": "c" * 64,
                    "forward": "b" * 64,
                    "late_compatibility": "f" * 64,
                    "late_forward": "e" * 64,
                    "late_source": "d" * 64,
                    "source": "a" * 64,
                },
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO low_volatility_forward_evaluation_runs
                        (forward_spec_hash)
                    VALUES (:late_forward)
                    """
                ),
                {
                    "late_forward": "e" * 64,
                },
            )
        await control.initialize(deployment)
        yield repository, engine, schema
    finally:
        try:
            await control.drop_test_schema()
        finally:
            await engine.dispose()


def _contract(*, late: bool = False) -> LowVolatilityPaperDeploymentContract:
    return LowVolatilityPaperDeploymentContract(
        source_spec_hash=("d" if late else "a") * 64,
        forward_spec_hash=("e" if late else "b") * 64,
        compatibility_spec_hash=("f" if late else "c") * 64,
        observed_forward_session_count=0,
        frozen_by="integration-risk-operator",
        frozen_at=datetime(2026, 7, 24, 12, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_contract_is_persisted_early_and_database_immutable(
    contract_store: tuple[
        PostgresLowVolatilityPaperDeploymentContractRepository,
        AsyncEngine,
        str,
    ],
) -> None:
    repository, engine, schema = contract_store
    contract = _contract()

    assert await repository.save(contract) == contract
    assert await repository.read(contract.contract_hash) == contract
    assert await repository.for_forward_spec(contract.forward_spec_hash) == contract

    with pytest.raises(DBAPIError):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"""
                    UPDATE "{schema}".
                        low_volatility_paper_deployment_contracts
                    SET frozen_by = 'tampered'
                    WHERE contract_hash = :contract_hash
                    """
                ),
                {"contract_hash": contract.contract_hash},
            )
    with pytest.raises(DBAPIError):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"""
                    DELETE FROM "{schema}".
                        low_volatility_paper_deployment_contracts
                    WHERE contract_hash = :contract_hash
                    """
                ),
                {"contract_hash": contract.contract_hash},
            )

    assert await repository.read(contract.contract_hash) == contract


@pytest.mark.asyncio
async def test_contract_rejects_freeze_after_terminal_evidence(
    contract_store: tuple[
        PostgresLowVolatilityPaperDeploymentContractRepository,
        AsyncEngine,
        str,
    ],
) -> None:
    repository, _, _ = contract_store

    with pytest.raises(
        PersistenceUnavailableError,
        match="persistence failed",
    ):
        await repository.save(_contract(late=True))
