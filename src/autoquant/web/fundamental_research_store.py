from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.backtest.fundamental_portfolio import (
    FundamentalPortfolioResearchSpec,
)
from autoquant.data.models import _require_lowercase_sha256
from autoquant.errors import PersistenceUnavailableError

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


@dataclass(frozen=True, slots=True)
class FundamentalResearchSpecRecord:
    spec: FundamentalPortfolioResearchSpec
    requested_by: str
    created_at: datetime
    live_trading_locked: bool = True

    def __post_init__(self) -> None:
        if (
            not self.requested_by.strip()
            or self.requested_by != self.requested_by.strip()
            or len(self.requested_by) > 128
        ):
            raise ValueError(
                "fundamental research requester is invalid"
            )
        if (
            self.created_at.tzinfo is None
            or self.created_at.utcoffset() is None
            or not self.live_trading_locked
        ):
            raise ValueError(
                "fundamental research metadata is invalid"
            )
        object.__setattr__(
            self,
            "created_at",
            self.created_at.astimezone(UTC),
        )


class PostgresFundamentalResearchSpecRepository:
    def __init__(
        self,
        *,
        engine: AsyncEngine,
        schema: str = "public",
    ) -> None:
        if _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError(
                "schema must be a safe PostgreSQL identifier"
            )
        self._engine = engine
        self._schema = schema

    @classmethod
    def connect(
        cls,
        *,
        dsn: str,
        schema: str = "public",
    ) -> PostgresFundamentalResearchSpecRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "fundamental research spec connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def freeze(
        self,
        spec: FundamentalPortfolioResearchSpec,
        *,
        requested_by: str,
        created_at: datetime,
    ) -> FundamentalResearchSpecRecord:
        record = FundamentalResearchSpecRecord(
            spec=spec,
            requested_by=requested_by,
            created_at=created_at,
        )
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO
                            {self._schema}.fundamental_research_specs
                            (spec_hash, predecessor_result_hash,
                             daily_dataset_manifest_hash, plan_hash,
                             universe_policy_hash, data_policy_hash,
                             strategy_id, specification_version,
                             start_date, end_date, requested_by,
                             created_at, live_trading_locked, payload)
                        VALUES
                            (:spec_hash, :predecessor_result_hash,
                             :daily_dataset_manifest_hash, :plan_hash,
                             :universe_policy_hash, :data_policy_hash,
                             :strategy_id, :specification_version,
                             :start_date, :end_date, :requested_by,
                             :created_at, true, CAST(:payload AS jsonb))
                        ON CONFLICT (predecessor_result_hash)
                        DO NOTHING
                        """
                    ),
                    {
                        "spec_hash": spec.spec_hash,
                        "predecessor_result_hash": (
                            spec.predecessor_result_hash
                        ),
                        "daily_dataset_manifest_hash": (
                            spec.daily_dataset_manifest_hash
                        ),
                        "plan_hash": spec.plan_hash,
                        "universe_policy_hash": (
                            spec.universe_policy_hash
                        ),
                        "data_policy_hash": (
                            spec.data_policy.policy_hash
                        ),
                        "strategy_id": spec.strategy_id,
                        "specification_version": spec.version,
                        "start_date": spec.start_date,
                        "end_date": spec.end_date,
                        "requested_by": record.requested_by,
                        "created_at": record.created_at,
                        "payload": _json(spec.payload()),
                    },
                )
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM
                                    {self._schema}.fundamental_research_specs
                                WHERE predecessor_result_hash =
                                    :predecessor_result_hash
                                """
                            ),
                            {
                                "predecessor_result_hash": (
                                    spec.predecessor_result_hash
                                )
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
            stored = _record(row)
            if stored.spec != spec:
                raise ValueError(
                    "a different fundamental strategy spec "
                    "is already frozen"
                )
            return stored
        except ValueError:
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "fundamental research spec freeze failed"
            ) from None

    async def read(
        self,
        spec_hash: str,
    ) -> FundamentalResearchSpecRecord:
        _require_lowercase_sha256(
            spec_hash,
            name="fundamental research spec hash",
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
                                    {self._schema}.fundamental_research_specs
                                WHERE spec_hash = :spec_hash
                                """
                            ),
                            {"spec_hash": spec_hash},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
            if row is None:
                raise LookupError(
                    "fundamental research spec does not exist"
                )
            return _record(row)
        except (LookupError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "fundamental research spec lookup failed"
            ) from None


def _record(row: RowMapping) -> FundamentalResearchSpecRecord:
    try:
        raw = row["payload"]
        payload = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(payload, dict):
            raise TypeError(
                "fundamental research payload is not an object"
            )
        spec = FundamentalPortfolioResearchSpec.from_payload(
            {str(key): value for key, value in payload.items()}
        )
        if (
            spec.spec_hash != str(row["spec_hash"])
            or spec.predecessor_result_hash
            != str(row["predecessor_result_hash"])
            or spec.daily_dataset_manifest_hash
            != str(row["daily_dataset_manifest_hash"])
            or spec.plan_hash != str(row["plan_hash"])
            or spec.universe_policy_hash
            != str(row["universe_policy_hash"])
            or spec.data_policy.policy_hash
            != str(row["data_policy_hash"])
            or spec.strategy_id != str(row["strategy_id"])
            or spec.version != str(row["specification_version"])
            or spec.start_date != row["start_date"]
            or spec.end_date != row["end_date"]
            or row["live_trading_locked"] is not True
        ):
            raise ValueError(
                "fundamental research metadata mismatch"
            )
        return FundamentalResearchSpecRecord(
            spec=spec,
            requested_by=str(row["requested_by"]),
            created_at=row["created_at"],
            live_trading_locked=True,
        )
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "stored fundamental research spec failed integrity verification"
        ) from None


def _json(payload: dict[str, object]) -> str:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
