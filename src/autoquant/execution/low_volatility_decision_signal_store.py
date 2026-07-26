from __future__ import annotations

import json
import re
from datetime import date

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.data.models import _require_lowercase_sha256
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.low_volatility_decision_signal import (
    LowVolatilityDecisionTimePaperSignal,
)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class PostgresLowVolatilityDecisionTimeSignalRepository:
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
    ) -> PostgresLowVolatilityDecisionTimeSignalRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(
                dsn,
                pool_pre_ping=True,
            )
        except Exception:
            raise PersistenceUnavailableError(
                "decision-time paper signal connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def save(
        self,
        signal: LowVolatilityDecisionTimePaperSignal,
    ) -> LowVolatilityDecisionTimePaperSignal:
        if not isinstance(
            signal,
            LowVolatilityDecisionTimePaperSignal,
        ):
            raise TypeError(
                "signal must be low-volatility decision-time evidence"
            )
        try:
            async with self._engine.begin() as connection:
                await connection.exec_driver_sql(
                    f'SET LOCAL search_path TO "{self._schema}"'
                )
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.
                            low_volatility_decision_time_paper_signals
                            (signal_hash, deployment_contract_hash,
                             candidate_approval_hash,
                             compatibility_run_hash,
                             observation_signal_hash,
                             reconciliation_report_hash,
                             internal_account_snapshot_hash,
                             broker_account_snapshot_hash,
                             kill_switch_event_hash,
                             account_id, strategy_id,
                             source_spec_hash, forward_spec_hash,
                             compatibility_spec_hash,
                             risk_policy_hash, snapshot_hash,
                             dataset_manifest_hash, rule_set_hash,
                             session_sequence, session_date,
                             signal_date, account_evidence_at,
                             kill_switch_changed_at,
                             selected_count, held_position_count,
                             valuation_count, prepared_by, prepared_at,
                             point_in_time_universe_verified,
                             held_position_valuation_coverage_verified,
                             exact_risk_policy_verified,
                             exact_session_rules_verified,
                             decision_time_inputs_verified,
                             account_reconciled,
                             no_open_orders_verified,
                             kill_switch_active,
                             execution_timing_compatible,
                             paper_activation_authority_granted,
                             runtime_activation_allowed,
                             live_trading_locked,
                             decision_order_policy_version,
                             signal_version, payload)
                        VALUES
                            (:signal_hash, :deployment_contract_hash,
                             :candidate_approval_hash,
                             :compatibility_run_hash,
                             :observation_signal_hash,
                             :reconciliation_report_hash,
                             :internal_account_snapshot_hash,
                             :broker_account_snapshot_hash,
                             :kill_switch_event_hash,
                             :account_id, :strategy_id,
                             :source_spec_hash, :forward_spec_hash,
                             :compatibility_spec_hash,
                             :risk_policy_hash, :snapshot_hash,
                             :dataset_manifest_hash, :rule_set_hash,
                             :session_sequence, :session_date,
                             :signal_date, :account_evidence_at,
                             :kill_switch_changed_at,
                             :selected_count, :held_position_count,
                             :valuation_count, :prepared_by, :prepared_at,
                             true, true, true, true, true,
                             true, true, true, true,
                             false, false, true,
                             :decision_order_policy_version,
                             :signal_version,
                             CAST(:payload AS jsonb))
                        ON CONFLICT (signal_hash) DO NOTHING
                        """
                    ),
                    _parameters(signal),
                )
        except Exception:
            raise PersistenceUnavailableError(
                "decision-time paper signal persistence failed"
            ) from None
        stored = await self.read(signal.signal_hash)
        if stored != signal:
            raise ValueError("a different decision-time paper signal is stored")
        return stored

    async def read(
        self,
        signal_hash: str,
    ) -> LowVolatilityDecisionTimePaperSignal:
        _require_lowercase_sha256(
            signal_hash,
            name="decision-time paper signal hash",
        )
        row = await self._row(
            "signal_hash = :signal_hash",
            {
                "signal_hash": signal_hash,
            },
        )
        if row is None:
            raise LookupError("decision-time paper signal does not exist")
        return _signal(row)

    async def for_session(
        self,
        *,
        candidate_approval_hash: str,
        session_date: date,
    ) -> LowVolatilityDecisionTimePaperSignal | None:
        _require_lowercase_sha256(
            candidate_approval_hash,
            name="decision-time candidate approval hash",
        )
        row = await self._row(
            """
            candidate_approval_hash = :candidate_approval_hash
            AND session_date = :session_date
            """,
            {
                "candidate_approval_hash": candidate_approval_hash,
                "session_date": session_date,
            },
        )
        return None if row is None else _signal(row)

    async def _row(
        self,
        predicate: str,
        parameters: dict[str, object],
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
                                    low_volatility_decision_time_paper_signals
                                WHERE {predicate}
                                """
                            ),
                            parameters,
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "decision-time paper signal lookup failed"
            ) from None


def _parameters(
    signal: LowVolatilityDecisionTimePaperSignal,
) -> dict[str, object]:
    return {
        **signal.payload(),
        "account_evidence_at": signal.account_evidence_at,
        "held_position_count": len(signal.held_instruments),
        "kill_switch_changed_at": signal.kill_switch_changed_at,
        "payload": json.dumps(
            signal.payload(),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ),
        "prepared_at": signal.prepared_at,
        "selected_count": len(signal.selected_instruments),
        "session_date": signal.session_date,
        "signal_hash": signal.signal_hash,
        "signal_date": signal.signal_date,
        "signal_version": signal.version,
        "valuation_count": len(signal.valuation_instruments),
    }


def _signal(
    row: RowMapping,
) -> LowVolatilityDecisionTimePaperSignal:
    try:
        raw = row["payload"]
        payload = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(payload, dict):
            raise TypeError("decision-time signal payload is not an object")
        signal = LowVolatilityDecisionTimePaperSignal.from_payload(
            {str(key): value for key, value in payload.items()}
        )
        true_columns = (
            "point_in_time_universe_verified",
            "held_position_valuation_coverage_verified",
            "exact_risk_policy_verified",
            "exact_session_rules_verified",
            "decision_time_inputs_verified",
            "account_reconciled",
            "no_open_orders_verified",
            "kill_switch_active",
            "execution_timing_compatible",
        )
        if (
            signal.signal_hash != str(row["signal_hash"])
            or signal.deployment_contract_hash
            != str(row["deployment_contract_hash"])
            or signal.candidate_approval_hash
            != str(row["candidate_approval_hash"])
            or signal.compatibility_run_hash
            != str(row["compatibility_run_hash"])
            or signal.observation_signal_hash
            != str(row["observation_signal_hash"])
            or signal.reconciliation_report_hash
            != str(row["reconciliation_report_hash"])
            or signal.internal_account_snapshot_hash
            != str(row["internal_account_snapshot_hash"])
            or signal.broker_account_snapshot_hash
            != str(row["broker_account_snapshot_hash"])
            or signal.kill_switch_event_hash
            != str(row["kill_switch_event_hash"])
            or signal.account_id != str(row["account_id"])
            or signal.strategy_id != str(row["strategy_id"])
            or signal.source_spec_hash != str(row["source_spec_hash"])
            or signal.forward_spec_hash != str(row["forward_spec_hash"])
            or signal.compatibility_spec_hash
            != str(row["compatibility_spec_hash"])
            or signal.risk_policy_hash != str(row["risk_policy_hash"])
            or signal.snapshot_hash != str(row["snapshot_hash"])
            or signal.dataset_manifest_hash
            != str(row["dataset_manifest_hash"])
            or signal.rule_set_hash != str(row["rule_set_hash"])
            or signal.session_sequence != int(row["session_sequence"])
            or signal.session_date != row["session_date"]
            or signal.signal_date != row["signal_date"]
            or signal.account_evidence_at != row["account_evidence_at"]
            or signal.kill_switch_changed_at
            != row["kill_switch_changed_at"]
            or len(signal.selected_instruments)
            != int(row["selected_count"])
            or len(signal.held_instruments)
            != int(row["held_position_count"])
            or len(signal.valuation_instruments)
            != int(row["valuation_count"])
            or signal.prepared_by != str(row["prepared_by"])
            or signal.prepared_at != row["prepared_at"]
            or any(row[name] is not True for name in true_columns)
            or row["paper_activation_authority_granted"] is not False
            or row["runtime_activation_allowed"] is not False
            or row["live_trading_locked"] is not True
            or signal.decision_order_policy_version
            != str(row["decision_order_policy_version"])
            or signal.version != str(row["signal_version"])
        ):
            raise ValueError("stored decision-time paper signal mismatch")
        return signal
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "stored decision-time paper signal failed integrity"
        ) from None
