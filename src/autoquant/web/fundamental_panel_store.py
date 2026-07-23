from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.backtest.fundamental_panel import (
    FundamentalResearchPanel,
)
from autoquant.clock import to_utc
from autoquant.data.models import _require_lowercase_sha256
from autoquant.errors import PersistenceUnavailableError

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


@dataclass(frozen=True, slots=True)
class FundamentalPanelRecord:
    panel_hash: str
    spec_hash: str
    daily_panel_hash: str
    fundamental_dataset_manifest_hash: str
    requested_by: str
    created_at: datetime
    payload: dict[str, object]
    live_trading_locked: bool = True

    def __post_init__(self) -> None:
        for value, name in (
            (self.panel_hash, "fundamental panel hash"),
            (self.spec_hash, "fundamental panel spec hash"),
            (self.daily_panel_hash, "daily panel hash"),
            (
                self.fundamental_dataset_manifest_hash,
                "fundamental dataset manifest hash",
            ),
        ):
            _require_lowercase_sha256(value, name=name)
        if (
            not self.requested_by.strip()
            or self.requested_by != self.requested_by.strip()
            or len(self.requested_by) > 128
            or not self.live_trading_locked
        ):
            raise ValueError(
                "fundamental panel metadata is invalid"
            )
        object.__setattr__(
            self,
            "created_at",
            to_utc(
                self.created_at,
                name="fundamental panel creation time",
            ),
        )


class PostgresFundamentalPanelRepository:
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
    ) -> PostgresFundamentalPanelRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "fundamental panel connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def freeze(
        self,
        panel: FundamentalResearchPanel,
        *,
        requested_by: str,
        created_at: datetime,
    ) -> FundamentalPanelRecord:
        instant = to_utc(
            created_at,
            name="fundamental panel creation time",
        )
        payload = panel.summary_payload()
        record = FundamentalPanelRecord(
            panel_hash=panel.panel_hash,
            spec_hash=panel.spec_hash,
            daily_panel_hash=panel.daily_panel_hash,
            fundamental_dataset_manifest_hash=(
                panel.fundamental_dataset_manifest_hash
            ),
            requested_by=requested_by,
            created_at=instant,
            payload=payload,
        )
        counts = [
            len(value.observations) for value in panel.sessions
        ]
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text(
                        "SELECT pg_advisory_xact_lock("
                        "hashtext(:identity))"
                    ),
                    {
                        "identity": (
                            f"fundamental-panel:{panel.spec_hash}"
                        )
                    },
                )
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO
                            {self._schema}.fundamental_research_panels
                            (panel_hash, spec_hash, daily_panel_hash,
                             fundamental_dataset_manifest_hash, as_of,
                             session_count, eligible_session_count,
                             observation_count,
                             minimum_eligible_members,
                             maximum_eligible_members,
                             minimum_required_members, requested_by,
                             created_at, live_trading_locked, payload)
                        VALUES
                            (:panel_hash, :spec_hash, :daily_panel_hash,
                             :fundamental_dataset_manifest_hash, :as_of,
                             :session_count, :eligible_session_count,
                             :observation_count,
                             :minimum_eligible_members,
                             :maximum_eligible_members,
                             :minimum_required_members, :requested_by,
                             :created_at, true, CAST(:payload AS jsonb))
                        ON CONFLICT (spec_hash) DO NOTHING
                        """
                    ),
                    {
                        "panel_hash": panel.panel_hash,
                        "spec_hash": panel.spec_hash,
                        "daily_panel_hash": panel.daily_panel_hash,
                        "fundamental_dataset_manifest_hash": (
                            panel.fundamental_dataset_manifest_hash
                        ),
                        "as_of": panel.as_of,
                        "session_count": len(panel.sessions),
                        "eligible_session_count": sum(
                            value >= panel.minimum_required_members
                            for value in counts
                        ),
                        "observation_count": sum(counts),
                        "minimum_eligible_members": min(counts),
                        "maximum_eligible_members": max(counts),
                        "minimum_required_members": (
                            panel.minimum_required_members
                        ),
                        "requested_by": record.requested_by,
                        "created_at": record.created_at,
                        "payload": json.dumps(
                            payload,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    },
                )
            stored = await self.read_for_spec(panel.spec_hash)
            if stored != record:
                raise ValueError(
                    "a different fundamental panel is already frozen"
                )
            return stored
        except (ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "fundamental panel freeze failed"
            ) from None

    async def read_for_spec(
        self,
        spec_hash: str,
    ) -> FundamentalPanelRecord:
        _require_lowercase_sha256(
            spec_hash,
            name="fundamental panel spec hash",
        )
        try:
            async with self._engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT panel_hash, spec_hash,
                                       daily_panel_hash,
                                       fundamental_dataset_manifest_hash,
                                       requested_by, created_at,
                                       live_trading_locked, payload
                                FROM
                                    {self._schema}.fundamental_research_panels
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
                    "fundamental panel does not exist"
                )
            raw_payload = row["payload"]
            if not isinstance(raw_payload, dict):
                raise TypeError("fundamental panel payload is invalid")
            record = FundamentalPanelRecord(
                panel_hash=str(row["panel_hash"]),
                spec_hash=str(row["spec_hash"]),
                daily_panel_hash=str(row["daily_panel_hash"]),
                fundamental_dataset_manifest_hash=str(
                    row["fundamental_dataset_manifest_hash"]
                ),
                requested_by=str(row["requested_by"]),
                created_at=row["created_at"],
                live_trading_locked=bool(
                    row["live_trading_locked"]
                ),
                payload=dict(raw_payload),
            )
            if (
                record.payload.get("panel_hash")
                != record.panel_hash
                or record.payload.get("spec_hash")
                != record.spec_hash
                or record.payload.get("daily_panel_hash")
                != record.daily_panel_hash
                or record.payload.get(
                    "fundamental_dataset_manifest_hash"
                )
                != record.fundamental_dataset_manifest_hash
            ):
                raise ValueError(
                    "fundamental panel failed integrity verification"
                )
            return record
        except (LookupError, ValueError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "fundamental panel lookup failed"
            ) from None
