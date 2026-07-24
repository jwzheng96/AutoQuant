from __future__ import annotations

import json
import re

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.data.models import _require_lowercase_sha256
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.low_volatility_paper_deployment_contract import (
    LowVolatilityPaperDeploymentContract,
)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class PostgresLowVolatilityPaperDeploymentContractRepository:
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
    ) -> PostgresLowVolatilityPaperDeploymentContractRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(
                dsn,
                pool_pre_ping=True,
            )
        except Exception:
            raise PersistenceUnavailableError(
                "paper deployment contract connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def save(
        self,
        contract: LowVolatilityPaperDeploymentContract,
    ) -> LowVolatilityPaperDeploymentContract:
        if not isinstance(
            contract,
            LowVolatilityPaperDeploymentContract,
        ):
            raise TypeError("contract must be low-volatility paper deployment")
        try:
            async with self._engine.begin() as connection:
                await connection.exec_driver_sql(f'SET LOCAL search_path TO "{self._schema}"')
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.
                            low_volatility_paper_deployment_contracts
                            (contract_hash, source_spec_hash,
                             forward_spec_hash,
                             compatibility_spec_hash,
                             observed_forward_session_count,
                             partial_outcome_observed_before_freeze,
                             terminal_outcome_observed_before_freeze,
                             minimum_forward_sessions,
                             minimum_paper_sessions,
                             required_forward_evidence_status,
                             required_compatibility_status,
                             required_order_policy_version,
                             required_daily_signal_policy_version,
                             candidate_approval_after_compatibility_required,
                             exact_session_signal_required,
                             decision_time_signal_required,
                             preopen_signal_required,
                             point_in_time_universe_required,
                             held_position_valuation_coverage_required,
                             exact_risk_policy_required,
                             kill_switch_active_at_authorization_required,
                             exclusive_paper_deployment_required,
                             fresh_runtime_unlock_evidence_required,
                             runtime_authorization_separate,
                             historical_reclassification_allowed,
                             paper_activation_authority_granted,
                             runtime_activation_allowed,
                             live_trading_locked,
                             frozen_by, frozen_at,
                             contract_version, payload)
                        VALUES
                            (:contract_hash, :source_spec_hash,
                             :forward_spec_hash,
                             :compatibility_spec_hash,
                             :observed_forward_session_count,
                             :partial_outcome_observed_before_freeze,
                             false, 126, 60, 'paper_candidate',
                             'compatible',
                             :required_order_policy_version,
                             :required_daily_signal_policy_version,
                             true, true, true, true, true, true,
                             true, true, true, true, true,
                             false, false, false, true,
                             :frozen_by, :frozen_at,
                             :contract_version,
                             CAST(:payload AS jsonb))
                        ON CONFLICT (contract_hash) DO NOTHING
                        """
                    ),
                    _parameters(contract),
                )
        except Exception:
            raise PersistenceUnavailableError(
                "paper deployment contract persistence failed"
            ) from None
        stored = await self.for_forward_spec(contract.forward_spec_hash)
        if stored != contract:
            raise ValueError("a different paper deployment contract is stored")
        return stored

    async def read(
        self,
        contract_hash: str,
    ) -> LowVolatilityPaperDeploymentContract:
        _require_lowercase_sha256(
            contract_hash,
            name="paper deployment contract hash",
        )
        row = await self._row(
            "contract_hash = :identity",
            contract_hash,
        )
        if row is None:
            raise LookupError("paper deployment contract does not exist")
        return _contract(row)

    async def for_forward_spec(
        self,
        forward_spec_hash: str,
    ) -> LowVolatilityPaperDeploymentContract:
        _require_lowercase_sha256(
            forward_spec_hash,
            name="paper deployment forward spec hash",
        )
        row = await self._row(
            "forward_spec_hash = :identity",
            forward_spec_hash,
        )
        if row is None:
            raise LookupError("paper deployment contract does not exist")
        return _contract(row)

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
                                    low_volatility_paper_deployment_contracts
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
            raise PersistenceUnavailableError("paper deployment contract lookup failed") from None


def _parameters(
    contract: LowVolatilityPaperDeploymentContract,
) -> dict[str, object]:
    return {
        **contract.payload(),
        "contract_hash": contract.contract_hash,
        "contract_version": contract.version,
        "frozen_at": contract.frozen_at,
        "payload": json.dumps(
            contract.payload(),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ),
    }


def _contract(
    row: RowMapping,
) -> LowVolatilityPaperDeploymentContract:
    try:
        raw = row["payload"]
        payload = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(payload, dict):
            raise TypeError("paper deployment payload is not an object")
        contract = LowVolatilityPaperDeploymentContract.from_payload(
            {str(key): value for key, value in payload.items()}
        )
        boolean_columns = (
            "candidate_approval_after_compatibility_required",
            "exact_session_signal_required",
            "decision_time_signal_required",
            "preopen_signal_required",
            "point_in_time_universe_required",
            "held_position_valuation_coverage_required",
            "exact_risk_policy_required",
            "kill_switch_active_at_authorization_required",
            "exclusive_paper_deployment_required",
            "fresh_runtime_unlock_evidence_required",
            "runtime_authorization_separate",
        )
        if (
            contract.contract_hash != str(row["contract_hash"])
            or contract.source_spec_hash != str(row["source_spec_hash"])
            or contract.forward_spec_hash != str(row["forward_spec_hash"])
            or contract.compatibility_spec_hash != str(row["compatibility_spec_hash"])
            or contract.observed_forward_session_count != int(row["observed_forward_session_count"])
            or contract.partial_outcome_observed_before_freeze
            is not row["partial_outcome_observed_before_freeze"]
            or row["terminal_outcome_observed_before_freeze"] is not False
            or contract.minimum_forward_sessions != int(row["minimum_forward_sessions"])
            or contract.minimum_paper_sessions != int(row["minimum_paper_sessions"])
            or contract.required_forward_evidence_status
            != str(row["required_forward_evidence_status"])
            or contract.required_compatibility_status != str(row["required_compatibility_status"])
            or contract.required_order_policy_version != str(row["required_order_policy_version"])
            or contract.required_daily_signal_policy_version
            != str(row["required_daily_signal_policy_version"])
            or any(row[name] is not True for name in boolean_columns)
            or row["historical_reclassification_allowed"] is not False
            or row["paper_activation_authority_granted"] is not False
            or row["runtime_activation_allowed"] is not False
            or row["live_trading_locked"] is not True
            or contract.frozen_by != str(row["frozen_by"])
            or contract.frozen_at != row["frozen_at"]
            or contract.version != str(row["contract_version"])
        ):
            raise ValueError("stored paper deployment contract mismatch")
        return contract
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "stored paper deployment contract failed integrity"
        ) from None
