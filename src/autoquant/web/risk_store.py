from __future__ import annotations

import json
import re

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.data.models import _canonical_hash
from autoquant.errors import PersistenceUnavailableError
from autoquant.risk.models import RiskDecision, risk_decision_payload
from autoquant.web.models import RiskDecisionView

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class PostgresRiskDecisionRepository:
    """Append-only audit store for pre-trade risk decisions."""

    def __init__(self, *, engine: AsyncEngine, schema: str = "public") -> None:
        if _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError("schema must be a safe PostgreSQL identifier")
        self._engine = engine
        self._schema = schema

    @classmethod
    def connect(
        cls, *, dsn: str, schema: str = "public"
    ) -> PostgresRiskDecisionRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "PostgreSQL risk decision connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def append(self, decision: RiskDecision) -> RiskDecisionView:
        payload = risk_decision_payload(decision)
        sql = text(
            f"""
            INSERT INTO {self._schema}.risk_decisions
                (decision_hash, account_id, client_order_id, mode, state,
                 evaluated_at, policy_hash, account_state_hash, quote_hash, payload)
            VALUES
                (:decision_hash, :account_id, :client_order_id, :mode, :state,
                 :evaluated_at, :policy_hash, :account_state_hash, :quote_hash,
                 CAST(:payload AS jsonb))
            ON CONFLICT (account_id, client_order_id) DO NOTHING
            RETURNING decision_hash, account_id, client_order_id, mode, state,
                      evaluated_at, policy_hash, account_state_hash, quote_hash, payload
            """
        )
        parameters = {
            "decision_hash": decision.decision_hash,
            "account_id": decision.account_id,
            "client_order_id": decision.order.client_order_id,
            "mode": decision.mode.value,
            "state": decision.state.value,
            "evaluated_at": decision.evaluated_at,
            "policy_hash": decision.policy_hash,
            "account_state_hash": decision.account_state_hash,
            "quote_hash": decision.quote_hash,
            "payload": _json(payload),
        }
        try:
            async with self._engine.begin() as connection:
                row = (await connection.execute(sql, parameters)).mappings().one_or_none()
                if row is None:
                    row = (
                        (
                            await connection.execute(
                                text(
                                    f"""
                                    SELECT decision_hash, account_id, client_order_id,
                                           mode, state, evaluated_at, policy_hash,
                                           account_state_hash, quote_hash, payload
                                    FROM {self._schema}.risk_decisions
                                    WHERE account_id = :account_id
                                      AND client_order_id = :client_order_id
                                    """
                                ),
                                {
                                    "account_id": decision.account_id,
                                    "client_order_id": decision.order.client_order_id,
                                },
                            )
                        )
                        .mappings()
                        .one()
                    )
        except Exception:
            raise PersistenceUnavailableError(
                "Risk decision persistence failed"
            ) from None
        view = _view(row)
        if view.decision_hash != decision.decision_hash:
            raise ValueError("client_order_id already belongs to another risk decision")
        return view

    async def list_recent(self, *, limit: int = 50) -> tuple[RiskDecisionView, ...]:
        if limit < 1 or limit > 200:
            raise ValueError("limit must be between 1 and 200")
        try:
            async with self._engine.connect() as connection:
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT decision_hash, account_id, client_order_id,
                                       mode, state, evaluated_at, policy_hash,
                                       account_state_hash, quote_hash, payload
                                FROM {self._schema}.risk_decisions
                                ORDER BY evaluated_at DESC, decision_hash DESC
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
                "Risk decision listing failed"
            ) from None
        return tuple(_view(row) for row in rows)

    async def count(self) -> int:
        try:
            async with self._engine.connect() as connection:
                value = await connection.scalar(
                    text(f"SELECT count(*) FROM {self._schema}.risk_decisions")
                )
        except Exception:
            raise PersistenceUnavailableError("Risk decision count failed") from None
        return int(value or 0)


def _view(row: RowMapping) -> RiskDecisionView:
    try:
        payload_raw = row["payload"]
        payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
        if not isinstance(payload, dict):
            raise TypeError("risk payload must be an object")
        decision_hash = str(row["decision_hash"])
        if _canonical_hash(payload) != decision_hash:
            raise ValueError("risk decision hash mismatch")
        order = payload["order"]
        if not isinstance(order, dict):
            raise TypeError("risk order payload must be an object")
        violations = payload["violations"]
        if not isinstance(violations, list):
            raise TypeError("risk violations must be a list")
        return RiskDecisionView(
            decision_hash=decision_hash,
            account_id=str(row["account_id"]),
            client_order_id=str(row["client_order_id"]),
            instrument=str(order["instrument"]),
            side=str(order["side"]),
            quantity=int(str(order["quantity"])),
            mode=str(row["mode"]),
            state=str(row["state"]),
            violations=tuple(str(value) for value in violations),
            evaluated_at=row["evaluated_at"],
            policy_hash=str(row["policy_hash"]),
            account_state_hash=str(row["account_state_hash"]),
            quote_hash=str(row["quote_hash"]),
            order_notional=payload["order_notional"],
        )
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "Stored risk decision failed integrity verification"
        ) from None


def _json(value: object) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)
