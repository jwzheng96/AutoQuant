from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.data.universe import (
    PointInTimeUniverseSnapshot,
    point_in_time_universe_from_payload,
)
from autoquant.errors import PersistenceUnavailableError
from autoquant.web.models import (
    ResearchUniverseMemberView,
    ResearchUniverseSnapshotDetail,
    ResearchUniverseSnapshotView,
)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_COLUMNS = """
snapshot_hash, policy_hash, index_code, reference_date,
index_constituent_date, liquidity_date, knowledge_as_of,
member_count, created_at
"""


class PostgresResearchUniverseRepository:
    def __init__(self, *, engine: AsyncEngine, schema: str = "public") -> None:
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
    ) -> PostgresResearchUniverseRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "research universe connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def save(
        self,
        snapshot: PointInTimeUniverseSnapshot,
        *,
        created_at: datetime,
    ) -> ResearchUniverseSnapshotDetail:
        created = _aware(created_at)
        payload = json.dumps(
            snapshot.payload(),
            separators=(",", ":"),
            sort_keys=True,
        )
        parameters = {
            "snapshot_hash": snapshot.snapshot_hash,
            "policy_hash": snapshot.policy.policy_hash,
            "index_code": snapshot.policy.index_code,
            "reference_date": snapshot.reference_date,
            "index_constituent_date": (
                snapshot.index_constituent_date
            ),
            "liquidity_date": snapshot.liquidity_date,
            "knowledge_as_of": snapshot.knowledge_as_of,
            "index_response_hash": snapshot.index_response_hash,
            "liquidity_response_hash": (
                snapshot.liquidity_response_hash
            ),
            "member_count": len(snapshot.members),
            "payload": payload,
            "created_at": created,
        }
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.research_universe_snapshots
                            (snapshot_hash, policy_hash, index_code,
                             reference_date, index_constituent_date,
                             liquidity_date, knowledge_as_of,
                             index_response_hash, liquidity_response_hash,
                             member_count, payload, created_at)
                        VALUES
                            (:snapshot_hash, :policy_hash, :index_code,
                             :reference_date, :index_constituent_date,
                             :liquidity_date, :knowledge_as_of,
                             :index_response_hash, :liquidity_response_hash,
                             :member_count, CAST(:payload AS jsonb),
                             :created_at)
                        ON CONFLICT (snapshot_hash) DO NOTHING
                        """
                    ),
                    parameters,
                )
        except Exception:
            raise PersistenceUnavailableError(
                "research universe persistence failed"
            ) from None
        detail = await self.detail(snapshot.snapshot_hash)
        if (
            detail.snapshot.snapshot_hash != snapshot.snapshot_hash
            or len(detail.members) != len(snapshot.members)
        ):
            raise ValueError(
                "research universe snapshot conflicts with stored value"
            )
        return detail

    async def list(
        self,
        *,
        limit: int = 50,
    ) -> tuple[ResearchUniverseSnapshotView, ...]:
        if limit < 1 or limit > 200:
            raise ValueError("limit must be between 1 and 200")
        try:
            async with self._engine.connect() as connection:
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT {_COLUMNS}
                                FROM {self._schema}.research_universe_snapshots
                                ORDER BY reference_date DESC,
                                         knowledge_as_of DESC
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
                "research universe listing failed"
            ) from None
        return tuple(_view(row) for row in rows)

    async def find(
        self,
        *,
        policy_hash: str,
        reference_date: date,
    ) -> ResearchUniverseSnapshotDetail | None:
        if re.fullmatch(r"[0-9a-f]{64}", policy_hash) is None:
            raise ValueError("policy_hash must be SHA-256")
        try:
            async with self._engine.connect() as connection:
                value = (
                    await connection.execute(
                        text(
                            f"""
                            SELECT snapshot_hash
                            FROM {self._schema}.research_universe_snapshots
                            WHERE policy_hash = :policy_hash
                              AND reference_date = :reference_date
                            """
                        ),
                        {
                            "policy_hash": policy_hash,
                            "reference_date": reference_date,
                        },
                    )
                ).scalar_one_or_none()
        except Exception:
            raise PersistenceUnavailableError(
                "research universe identity lookup failed"
            ) from None
        return (
            None
            if value is None
            else await self.detail(str(value))
        )

    async def detail(
        self,
        snapshot_hash: str,
    ) -> ResearchUniverseSnapshotDetail:
        if re.fullmatch(r"[0-9a-f]{64}", snapshot_hash) is None:
            raise ValueError("snapshot_hash must be SHA-256")
        try:
            async with self._engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT {_COLUMNS}, index_response_hash,
                                       liquidity_response_hash, payload
                                FROM {self._schema}.research_universe_snapshots
                                WHERE snapshot_hash = :snapshot_hash
                                """
                            ),
                            {"snapshot_hash": snapshot_hash},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "research universe detail failed"
            ) from None
        if row is None:
            raise LookupError("research universe snapshot not found")
        try:
            raw = row["payload"]
            payload = json.loads(raw) if isinstance(raw, str) else raw
            snapshot = point_in_time_universe_from_payload(payload)
            if (
                snapshot.snapshot_hash != row["snapshot_hash"]
                or snapshot.index_response_hash
                != row["index_response_hash"]
                or snapshot.liquidity_response_hash
                != row["liquidity_response_hash"]
                or len(snapshot.members) != row["member_count"]
            ):
                raise ValueError("research universe metadata mismatch")
            return ResearchUniverseSnapshotDetail(
                snapshot=_view(row),
                index_response_hash=snapshot.index_response_hash,
                liquidity_response_hash=(
                    snapshot.liquidity_response_hash
                ),
                members=tuple(
                    ResearchUniverseMemberView.model_validate(
                        member.payload()
                    )
                    for member in snapshot.members
                ),
            )
        except (KeyError, TypeError, ValueError):
            raise PersistenceUnavailableError(
                "stored research universe failed integrity verification"
            ) from None


def _view(row: RowMapping) -> ResearchUniverseSnapshotView:
    try:
        return ResearchUniverseSnapshotView(
            snapshot_hash=str(row["snapshot_hash"]),
            policy_hash=str(row["policy_hash"]),
            index_code=str(row["index_code"]),
            reference_date=row["reference_date"],
            index_constituent_date=row["index_constituent_date"],
            liquidity_date=row["liquidity_date"],
            knowledge_as_of=row["knowledge_as_of"],
            member_count=int(row["member_count"]),
            created_at=row["created_at"],
        )
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "stored research universe metadata is malformed"
        ) from None


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("created_at must be timezone-aware")
    return value.astimezone(UTC)
