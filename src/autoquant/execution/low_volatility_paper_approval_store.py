from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.data.models import (
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.low_volatility_paper_approval import (
    LowVolatilityPaperCandidateApproval,
    LowVolatilityPaperCandidateRevocation,
    LowVolatilityPaperRevocationReason,
)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


@dataclass(frozen=True, slots=True)
class LowVolatilityPaperCandidateRecord:
    approval: LowVolatilityPaperCandidateApproval
    revocation: LowVolatilityPaperCandidateRevocation | None = None

    def __post_init__(self) -> None:
        if self.revocation is not None and (
            self.revocation.approval_hash != self.approval.approval_hash
            or self.revocation.revoked_at < self.approval.approved_at
        ):
            raise ValueError("low-volatility paper candidate record is inconsistent")

    @property
    def active(self) -> bool:
        return self.revocation is None


class PostgresLowVolatilityPaperCandidateRepository:
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
    ) -> PostgresLowVolatilityPaperCandidateRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(
                dsn,
                pool_pre_ping=True,
            )
        except Exception:
            raise PersistenceUnavailableError(
                "low-volatility paper candidate connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def approve(
        self,
        approval: LowVolatilityPaperCandidateApproval,
    ) -> LowVolatilityPaperCandidateRecord:
        if not isinstance(
            approval,
            LowVolatilityPaperCandidateApproval,
        ):
            raise TypeError("approval must be a low-volatility paper candidate")
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:identity))"),
                    {
                        "identity": (
                            "low-volatility-paper-candidate:"
                            f"{approval.account_id}:"
                            f"{approval.strategy_id}"
                        )
                    },
                )
                active = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT a.approval_hash
                                FROM {self._schema}.
                                    low_volatility_paper_candidate_approvals a
                                LEFT JOIN {self._schema}.
                                    low_volatility_paper_candidate_revocations r
                                  ON r.approval_hash = a.approval_hash
                                WHERE a.account_id = :account_id
                                  AND a.strategy_id = :strategy_id
                                  AND r.revocation_hash IS NULL
                                ORDER BY a.approved_at DESC,
                                         a.approval_hash DESC
                                LIMIT 1
                                """
                            ),
                            {
                                "account_id": approval.account_id,
                                "strategy_id": approval.strategy_id,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if active is not None and str(active["approval_hash"]) != approval.approval_hash:
                    raise ValueError(
                        "active low-volatility candidate must be revoked before replacement"
                    )
                if active is None:
                    await connection.execute(
                        text(
                            f"""
                            INSERT INTO {self._schema}.
                                low_volatility_paper_candidate_approvals
                                (approval_hash, account_id, strategy_id,
                                 forward_spec_hash,
                                 evaluation_result_hash,
                                 evaluation_assessment_hash,
                                 evaluation_dataset_manifest_hash,
                                 source_spec_hash, risk_policy_hash,
                                 instrument_count, evidence_status,
                                 minimum_paper_sessions,
                                 execution_mode,
                                 daily_signal_evidence_required,
                                 runtime_activation_allowed,
                                 live_trading_locked,
                                 approved_by, approved_at,
                                 approval_version, payload)
                            VALUES
                                (:approval_hash, :account_id, :strategy_id,
                                 :forward_spec_hash,
                                 :evaluation_result_hash,
                                 :evaluation_assessment_hash,
                                 :evaluation_dataset_manifest_hash,
                                 :source_spec_hash, :risk_policy_hash,
                                 :instrument_count, 'paper_candidate',
                                 60, 'paper', true, false, true,
                                 :approved_by, :approved_at,
                                 :approval_version,
                                 CAST(:payload AS jsonb))
                            ON CONFLICT (approval_hash) DO NOTHING
                            """
                        ),
                        _approval_parameters(approval),
                    )
        except (TypeError, ValueError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "low-volatility paper candidate approval failed"
            ) from None
        stored = await self.read(approval.approval_hash)
        if stored.approval != approval or not stored.active:
            raise ValueError("a different low-volatility paper candidate is stored")
        return stored

    async def revoke(
        self,
        revocation: LowVolatilityPaperCandidateRevocation,
    ) -> LowVolatilityPaperCandidateRecord:
        if not isinstance(
            revocation,
            LowVolatilityPaperCandidateRevocation,
        ):
            raise TypeError("revocation must be a low-volatility paper revocation")
        try:
            async with self._engine.begin() as connection:
                approval = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT account_id, strategy_id, approved_at
                                FROM {self._schema}.
                                    low_volatility_paper_candidate_approvals
                                WHERE approval_hash = :approval_hash
                                """
                            ),
                            {"approval_hash": (revocation.approval_hash)},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if approval is None:
                    raise LookupError("low-volatility paper candidate does not exist")
                await connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:identity))"),
                    {
                        "identity": (
                            "low-volatility-paper-candidate:"
                            f"{approval['account_id']}:"
                            f"{approval['strategy_id']}"
                        )
                    },
                )
                if revocation.revoked_at < approval["approved_at"]:
                    raise ValueError("candidate revocation cannot precede approval")
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.
                            low_volatility_paper_candidate_revocations
                            (revocation_hash, approval_hash,
                             revoked_by, revoked_at, reason,
                             live_trading_locked,
                             revocation_version, payload)
                        VALUES
                            (:revocation_hash, :approval_hash,
                             :revoked_by, :revoked_at, :reason,
                             true, :revocation_version,
                             CAST(:payload AS jsonb))
                        ON CONFLICT (approval_hash) DO NOTHING
                        """
                    ),
                    _revocation_parameters(revocation),
                )
        except (LookupError, TypeError, ValueError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "low-volatility paper candidate revocation failed"
            ) from None
        stored = await self.read(revocation.approval_hash)
        if stored.revocation != revocation:
            raise ValueError("a different low-volatility revocation is stored")
        return stored

    async def read(
        self,
        approval_hash: str,
    ) -> LowVolatilityPaperCandidateRecord:
        _require_lowercase_sha256(
            approval_hash,
            name="low-volatility paper approval hash",
        )
        try:
            async with self._engine.connect() as connection:
                approval = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.
                                    low_volatility_paper_candidate_approvals
                                WHERE approval_hash = :approval_hash
                                """
                            ),
                            {"approval_hash": approval_hash},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if approval is None:
                    raise LookupError("low-volatility paper candidate does not exist")
                revocation = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.
                                    low_volatility_paper_candidate_revocations
                                WHERE approval_hash = :approval_hash
                                """
                            ),
                            {"approval_hash": approval_hash},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
            return LowVolatilityPaperCandidateRecord(
                approval=_approval(approval),
                revocation=(None if revocation is None else _revocation(revocation)),
            )
        except (
            LookupError,
            PersistenceUnavailableError,
        ):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "low-volatility paper candidate lookup failed"
            ) from None

    async def active(
        self,
        *,
        account_id: str,
        strategy_id: str,
    ) -> LowVolatilityPaperCandidateRecord | None:
        _require_nonblank(account_id, name="account_id")
        _require_nonblank(strategy_id, name="strategy_id")
        try:
            async with self._engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT a.approval_hash
                                FROM {self._schema}.
                                    low_volatility_paper_candidate_approvals a
                                LEFT JOIN {self._schema}.
                                    low_volatility_paper_candidate_revocations r
                                  ON r.approval_hash = a.approval_hash
                                WHERE a.account_id = :account_id
                                  AND a.strategy_id = :strategy_id
                                  AND r.revocation_hash IS NULL
                                ORDER BY a.approved_at DESC,
                                         a.approval_hash DESC
                                LIMIT 1
                                """
                            ),
                            {
                                "account_id": account_id,
                                "strategy_id": strategy_id,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "low-volatility paper candidate lookup failed"
            ) from None
        return None if row is None else await self.read(str(row["approval_hash"]))


def _approval_parameters(
    approval: LowVolatilityPaperCandidateApproval,
) -> dict[str, object]:
    return {
        **approval.payload(),
        "approval_hash": approval.approval_hash,
        "approval_version": approval.version,
        "approved_at": approval.approved_at,
        "instrument_count": len(approval.instruments),
        "payload": _json(approval.payload()),
    }


def _revocation_parameters(
    revocation: LowVolatilityPaperCandidateRevocation,
) -> dict[str, object]:
    return {
        **revocation.payload(),
        "reason": revocation.reason.value,
        "revocation_hash": revocation.revocation_hash,
        "revocation_version": revocation.version,
        "revoked_at": revocation.revoked_at,
        "payload": _json(revocation.payload()),
    }


def _approval(
    row: RowMapping,
) -> LowVolatilityPaperCandidateApproval:
    try:
        approval = LowVolatilityPaperCandidateApproval.from_payload(_object(row["payload"]))
        if (
            approval.approval_hash != str(row["approval_hash"])
            or approval.account_id != str(row["account_id"])
            or approval.strategy_id != str(row["strategy_id"])
            or approval.forward_spec_hash != str(row["forward_spec_hash"])
            or approval.evaluation_result_hash != str(row["evaluation_result_hash"])
            or approval.evaluation_assessment_hash != str(row["evaluation_assessment_hash"])
            or approval.evaluation_dataset_manifest_hash
            != str(row["evaluation_dataset_manifest_hash"])
            or approval.source_spec_hash != str(row["source_spec_hash"])
            or approval.risk_policy_hash != str(row["risk_policy_hash"])
            or len(approval.instruments) != int(row["instrument_count"])
            or approval.evidence_status != str(row["evidence_status"])
            or approval.minimum_paper_sessions != int(row["minimum_paper_sessions"])
            or approval.execution_mode != str(row["execution_mode"])
            or approval.approved_by != str(row["approved_by"])
            or approval.approved_at != row["approved_at"]
            or approval.version != str(row["approval_version"])
            or row["daily_signal_evidence_required"] is not True
            or row["runtime_activation_allowed"] is not False
            or row["live_trading_locked"] is not True
        ):
            raise ValueError("stored low-volatility paper approval mismatch")
        return approval
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "stored low-volatility paper approval failed integrity"
        ) from None


def _revocation(
    row: RowMapping,
) -> LowVolatilityPaperCandidateRevocation:
    try:
        payload = _object(row["payload"])
        revocation = LowVolatilityPaperCandidateRevocation(
            approval_hash=str(payload["approval_hash"]),
            revoked_by=str(payload["revoked_by"]),
            revoked_at=datetime.fromisoformat(str(payload["revoked_at"])),
            reason=LowVolatilityPaperRevocationReason(str(payload["reason"])),
            live_trading_locked=_boolean(payload["live_trading_locked"]),
            version=str(payload["version"]),
        )
        if (
            revocation.payload() != payload
            or revocation.revocation_hash != str(row["revocation_hash"])
            or revocation.approval_hash != str(row["approval_hash"])
            or revocation.revoked_at != row["revoked_at"]
            or revocation.version != str(row["revocation_version"])
            or row["live_trading_locked"] is not True
        ):
            raise ValueError("stored low-volatility paper revocation mismatch")
        return revocation
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "stored low-volatility paper revocation failed integrity"
        ) from None


def _object(value: object) -> dict[str, object]:
    raw = json.loads(value) if isinstance(value, str) else value
    if not isinstance(raw, dict):
        raise TypeError("low-volatility paper candidate payload is not an object")
    return {str(key): item for key, item in raw.items()}


def _boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise TypeError("low-volatility paper candidate boolean is invalid")
    return value


def _json(payload: object) -> str:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
