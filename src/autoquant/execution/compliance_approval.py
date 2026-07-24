from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _datetime_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.errors import PersistenceUnavailableError

COMPLIANCE_APPROVAL_VERSION = "paper-compliance-approval-v1"
COMPLIANCE_REVOCATION_VERSION = "paper-compliance-revocation-v1"
MAXIMUM_COMPLIANCE_APPROVAL_AGE = timedelta(days=31)
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_REFERENCE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{7,127}\Z")


class ComplianceRevocationReason(StrEnum):
    SCOPE_CHANGED = "scope_changed"
    RISK_CHANGED = "risk_changed"
    EXTERNAL_APPROVAL_WITHDRAWN = "external_approval_withdrawn"
    OPERATOR_SAFETY_ACTION = "operator_safety_action"


@dataclass(frozen=True, slots=True)
class ComplianceApproval:
    account_id: str
    strategy_id: str
    registration_hash: str
    policy_hash: str
    external_artifact_hash: str
    approval_reference: str
    approved_by: str
    approved_at: datetime
    valid_until: datetime
    version: str = COMPLIANCE_APPROVAL_VERSION
    approval_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.account_id, "compliance account_id"),
            (self.strategy_id, "compliance strategy_id"),
            (self.approved_by, "compliance approved_by"),
        ):
            _require_nonblank(value, name=name)
            if value != value.strip():
                raise ValueError(f"{name} must be trimmed")
            if len(value) > 128:
                raise ValueError(f"{name} cannot exceed 128 characters")
        for value, name in (
            (self.registration_hash, "compliance registration hash"),
            (self.policy_hash, "compliance policy hash"),
            (
                self.external_artifact_hash,
                "external compliance artifact hash",
            ),
        ):
            _require_lowercase_sha256(value, name=name)
        if _REFERENCE.fullmatch(self.approval_reference) is None:
            raise ValueError("approval_reference must contain 8-128 safe characters")
        approved_at = to_utc(
            self.approved_at,
            name="compliance approval time",
        )
        valid_until = to_utc(
            self.valid_until,
            name="compliance approval expiry",
        )
        if (
            valid_until <= approved_at
            or valid_until - approved_at > MAXIMUM_COMPLIANCE_APPROVAL_AGE
            or self.version != COMPLIANCE_APPROVAL_VERSION
        ):
            raise ValueError("compliance approval validity is unsupported")
        object.__setattr__(self, "approved_at", approved_at)
        object.__setattr__(self, "valid_until", valid_until)
        object.__setattr__(
            self,
            "approval_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "account_id": self.account_id,
            "approval_reference": self.approval_reference,
            "approved_at": _datetime_text(self.approved_at),
            "approved_by": self.approved_by,
            "external_artifact_hash": self.external_artifact_hash,
            "policy_hash": self.policy_hash,
            "registration_hash": self.registration_hash,
            "strategy_id": self.strategy_id,
            "valid_until": _datetime_text(self.valid_until),
            "version": self.version,
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> ComplianceApproval:
        value = cls(
            account_id=str(payload["account_id"]),
            strategy_id=str(payload["strategy_id"]),
            registration_hash=str(payload["registration_hash"]),
            policy_hash=str(payload["policy_hash"]),
            external_artifact_hash=str(payload["external_artifact_hash"]),
            approval_reference=str(payload["approval_reference"]),
            approved_by=str(payload["approved_by"]),
            approved_at=datetime.fromisoformat(str(payload["approved_at"])),
            valid_until=datetime.fromisoformat(str(payload["valid_until"])),
            version=str(payload["version"]),
        )
        if value.payload() != payload:
            raise ValueError("compliance approval payload is not canonical")
        return value


@dataclass(frozen=True, slots=True)
class ComplianceRevocation:
    approval_hash: str
    revoked_by: str
    revoked_at: datetime
    reason: ComplianceRevocationReason
    version: str = COMPLIANCE_REVOCATION_VERSION
    revocation_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.approval_hash,
            name="compliance approval hash",
        )
        _require_nonblank(
            self.revoked_by,
            name="compliance revoked_by",
        )
        if (
            self.revoked_by != self.revoked_by.strip()
            or len(self.revoked_by) > 128
            or not isinstance(
                self.reason,
                ComplianceRevocationReason,
            )
            or self.version != COMPLIANCE_REVOCATION_VERSION
        ):
            raise ValueError("compliance revocation is unsupported")
        object.__setattr__(
            self,
            "revoked_at",
            to_utc(
                self.revoked_at,
                name="compliance revocation time",
            ),
        )
        object.__setattr__(
            self,
            "revocation_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "approval_hash": self.approval_hash,
            "reason": self.reason.value,
            "revoked_at": _datetime_text(self.revoked_at),
            "revoked_by": self.revoked_by,
            "version": self.version,
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> ComplianceRevocation:
        value = cls(
            approval_hash=str(payload["approval_hash"]),
            revoked_by=str(payload["revoked_by"]),
            revoked_at=datetime.fromisoformat(str(payload["revoked_at"])),
            reason=ComplianceRevocationReason(str(payload["reason"])),
            version=str(payload["version"]),
        )
        if value.payload() != payload:
            raise ValueError("compliance revocation payload is not canonical")
        return value


class PostgresComplianceApprovalRepository:
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
    ) -> PostgresComplianceApprovalRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(
                dsn,
                pool_pre_ping=True,
            )
        except Exception:
            raise PersistenceUnavailableError("compliance approval connection failed") from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def approve(
        self,
        approval: ComplianceApproval,
    ) -> ComplianceApproval:
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:identity))"),
                    {
                        "identity": (
                            f"paper-compliance:{approval.account_id}:{approval.strategy_id}"
                        )
                    },
                )
                existing_hashes = (
                    await connection.scalars(
                        text(
                            f"""
                            SELECT approval_hash
                            FROM
                                {self._schema}.paper_compliance_approvals
                            WHERE account_id = :account_id
                              AND strategy_id = :strategy_id
                              AND (
                                  approval_reference =
                                      :approval_reference
                                  OR (
                                      registration_hash =
                                          :registration_hash
                                      AND policy_hash = :policy_hash
                                      AND external_artifact_hash =
                                          :external_artifact_hash
                                  )
                              )
                            """
                        ),
                        approval.payload(),
                    )
                ).all()
                if any(str(value) != approval.approval_hash for value in existing_hashes):
                    raise ValueError("compliance approval identity conflicts")
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO
                            {self._schema}.paper_compliance_approvals
                            (approval_hash, account_id, strategy_id,
                             registration_hash, policy_hash,
                             external_artifact_hash,
                             approval_reference, approved_by,
                             approved_at, valid_until,
                             approval_version,
                             live_trading_locked, payload)
                        VALUES
                            (:approval_hash, :account_id, :strategy_id,
                             :registration_hash, :policy_hash,
                             :external_artifact_hash,
                             :approval_reference, :approved_by,
                             :approved_at, :valid_until,
                             :approval_version, true,
                             CAST(:payload AS jsonb))
                        ON CONFLICT (approval_hash) DO NOTHING
                        """
                    ),
                    {
                        **approval.payload(),
                        "approval_hash": approval.approval_hash,
                        "approval_version": approval.version,
                        "approved_at": approval.approved_at,
                        "payload": _json(approval.payload()),
                        "valid_until": approval.valid_until,
                    },
                )
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.paper_compliance_approvals
                                WHERE approval_hash = :approval_hash
                                """
                            ),
                            {"approval_hash": (approval.approval_hash)},
                        )
                    )
                    .mappings()
                    .one()
                )
            stored = _approval_from_row(row)
            if stored != approval:
                raise ValueError("a different compliance approval is stored")
            return stored
        except ValueError:
            raise
        except Exception:
            raise PersistenceUnavailableError("compliance approval persistence failed") from None

    async def read(
        self,
        approval_hash: str,
    ) -> ComplianceApproval:
        _require_lowercase_sha256(
            approval_hash,
            name="compliance approval hash",
        )
        try:
            async with self._engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.paper_compliance_approvals
                                WHERE approval_hash = :approval_hash
                                """
                            ),
                            {"approval_hash": approval_hash},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
            if row is None:
                raise LookupError("compliance approval does not exist")
            return _approval_from_row(row)
        except (
            LookupError,
            PersistenceUnavailableError,
        ):
            raise
        except Exception:
            raise PersistenceUnavailableError("compliance approval lookup failed") from None

    async def revoke(
        self,
        revocation: ComplianceRevocation,
    ) -> ComplianceRevocation:
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:identity))"),
                    {"identity": (f"paper-compliance-revoke:{revocation.approval_hash}")},
                )
                approved_at = await connection.scalar(
                    text(
                        f"""
                        SELECT approved_at
                        FROM
                            {self._schema}.paper_compliance_approvals
                        WHERE approval_hash = :approval_hash
                        """
                    ),
                    {"approval_hash": (revocation.approval_hash)},
                )
                if approved_at is None:
                    raise LookupError("compliance approval does not exist")
                if revocation.revoked_at < approved_at:
                    raise ValueError("compliance revocation precedes approval")
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO
                            {self._schema}.paper_compliance_revocations
                            (revocation_hash, approval_hash,
                             revoked_by, revoked_at,
                             reason, revocation_version, payload)
                        VALUES
                            (:revocation_hash, :approval_hash,
                             :revoked_by, :revoked_at,
                             :reason, :revocation_version,
                             CAST(:payload AS jsonb))
                        ON CONFLICT (approval_hash) DO NOTHING
                        """
                    ),
                    {
                        **revocation.payload(),
                        "revocation_hash": (revocation.revocation_hash),
                        "revocation_version": (revocation.version),
                        "revoked_at": revocation.revoked_at,
                        "reason": revocation.reason.value,
                        "payload": _json(revocation.payload()),
                    },
                )
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.paper_compliance_revocations
                                WHERE approval_hash = :approval_hash
                                """
                            ),
                            {"approval_hash": (revocation.approval_hash)},
                        )
                    )
                    .mappings()
                    .one()
                )
            stored = _revocation_from_row(row)
            if stored != revocation:
                raise ValueError("a different compliance revocation is stored")
            return stored
        except (LookupError, ValueError):
            raise
        except Exception:
            raise PersistenceUnavailableError("compliance revocation persistence failed") from None


def _approval_from_row(
    row: RowMapping,
) -> ComplianceApproval:
    try:
        payload = _object(row["payload"])
        approval = ComplianceApproval.from_payload(payload)
        if (
            approval.approval_hash != str(row["approval_hash"])
            or approval.account_id != str(row["account_id"])
            or approval.strategy_id != str(row["strategy_id"])
            or approval.registration_hash != str(row["registration_hash"])
            or approval.policy_hash != str(row["policy_hash"])
            or approval.external_artifact_hash != str(row["external_artifact_hash"])
            or approval.approval_reference != str(row["approval_reference"])
            or approval.approved_by != str(row["approved_by"])
            or approval.approved_at != row["approved_at"]
            or approval.valid_until != row["valid_until"]
            or approval.version != str(row["approval_version"])
            or row["live_trading_locked"] is not True
        ):
            raise ValueError("stored compliance approval metadata mismatch")
        return approval
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError("stored compliance approval failed integrity") from None


def _revocation_from_row(
    row: RowMapping,
) -> ComplianceRevocation:
    try:
        payload = _object(row["payload"])
        revocation = ComplianceRevocation.from_payload(payload)
        if (
            revocation.revocation_hash != str(row["revocation_hash"])
            or revocation.approval_hash != str(row["approval_hash"])
            or revocation.revoked_by != str(row["revoked_by"])
            or revocation.revoked_at != row["revoked_at"]
            or revocation.reason.value != str(row["reason"])
            or revocation.version != str(row["revocation_version"])
        ):
            raise ValueError("stored compliance revocation metadata mismatch")
        return revocation
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError("stored compliance revocation failed integrity") from None


def _object(value: object) -> dict[str, object]:
    raw = json.loads(value) if isinstance(value, str) else value
    if not isinstance(raw, dict):
        raise TypeError("compliance payload is not an object")
    return {str(key): item for key, item in raw.items()}


def _json(payload: object) -> str:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
