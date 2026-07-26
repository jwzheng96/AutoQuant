from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from autoquant.clock import to_utc
from autoquant.data.models import _require_lowercase_sha256
from autoquant.data.research_data_campaign import (
    ResearchDataCampaignSpec,
    ResearchDatasetManifest,
    ResearchDatasetShard,
)
from autoquant.errors import PersistenceUnavailableError

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_CAMPAIGN_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{15,127}\Z")
_ITEM_STATES = frozenset({"queued", "running", "completed", "failed"})


@dataclass(frozen=True, slots=True)
class ResearchDataCampaignItem:
    sequence: int
    instrument: str
    state: str
    attempts: int
    max_attempts: int
    manifest_hash: str | None
    started_at: datetime | None
    completed_at: datetime | None
    error_code: str | None


@dataclass(frozen=True, slots=True)
class ResearchDataCampaignStatus:
    spec: ResearchDataCampaignSpec
    created_at: datetime
    status: str
    items: tuple[ResearchDataCampaignItem, ...]
    manifest: ResearchDatasetManifest | None


class PostgresResearchDataCampaignRepository:
    """Persistent, restart-safe queue for survivorship-free daily data shards."""

    def __init__(self, *, engine: AsyncEngine, schema: str = "public") -> None:
        if _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError("schema must be a safe PostgreSQL identifier")
        self._engine = engine
        self._schema = schema
        self._worker_lock_connection: AsyncConnection | None = None
        self._worker_lock_identity: str | None = None

    @classmethod
    def connect(
        cls,
        *,
        dsn: str,
        schema: str = "public",
    ) -> PostgresResearchDataCampaignRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "research data campaign connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self.release_worker_lock()
        await self._engine.dispose()

    async def try_acquire_worker_lock(
        self,
        *,
        campaign_hash: str,
    ) -> bool:
        """Hold one PostgreSQL session lock for the lifetime of a worker batch."""

        _require_lowercase_sha256(campaign_hash, name="research data campaign hash")
        if self._worker_lock_connection is not None:
            raise ValueError("research data campaign worker lock is already held")
        identity = f"autoquant:research-data-campaign:{campaign_hash}"
        connection: AsyncConnection | None = None
        try:
            connection = await self._engine.connect()
            acquired = bool(
                await connection.scalar(
                    text(
                        """
                        SELECT pg_try_advisory_lock(
                            hashtextextended(:identity, 0)
                        )
                        """
                    ),
                    {"identity": identity},
                )
            )
            await connection.commit()
        except Exception:
            if connection is not None:
                await connection.close()
            raise PersistenceUnavailableError("research data campaign worker lock failed") from None
        if not acquired:
            await connection.close()
            return False
        self._worker_lock_connection = connection
        self._worker_lock_identity = identity
        return True

    async def release_worker_lock(self) -> None:
        connection = self._worker_lock_connection
        identity = self._worker_lock_identity
        self._worker_lock_connection = None
        self._worker_lock_identity = None
        if connection is None:
            return
        try:
            if identity is not None:
                await connection.scalar(
                    text(
                        """
                        SELECT pg_advisory_unlock(
                            hashtextextended(:identity, 0)
                        )
                        """
                    ),
                    {"identity": identity},
                )
                await connection.commit()
        except Exception:
            # A session close releases PostgreSQL advisory locks even when an
            # explicit unlock cannot be confirmed.
            pass
        finally:
            await connection.close()

    async def read_manifest(
        self,
        manifest_hash: str,
    ) -> ResearchDatasetManifest:
        _require_lowercase_sha256(
            manifest_hash,
            name="research dataset manifest hash",
        )
        try:
            async with self._engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT manifest_hash, campaign_hash, payload
                                FROM {self._schema}.research_dataset_manifests
                                WHERE manifest_hash = :manifest_hash
                                """
                            ),
                            {"manifest_hash": manifest_hash},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                shard_rows = (
                    ()
                    if row is None
                    else (
                        (
                            await connection.execute(
                                text(
                                    f"""
                                    SELECT sequence, instrument,
                                           shard_manifest_hash
                                    FROM {self._schema}.research_dataset_manifest_shards
                                    WHERE manifest_hash = :manifest_hash
                                    ORDER BY sequence
                                    """
                                ),
                                {"manifest_hash": manifest_hash},
                            )
                        )
                        .mappings()
                        .all()
                    )
                )
            if row is None:
                raise LookupError(
                    "research dataset manifest does not exist"
                )
            manifest = ResearchDatasetManifest.from_payload(
                _object(row["payload"])
            )
            stored_shards = tuple(
                ResearchDatasetShard(
                    sequence=int(value["sequence"]),
                    instrument=str(value["instrument"]),
                    manifest_hash=str(value["shard_manifest_hash"]),
                )
                for value in shard_rows
            )
            if (
                manifest.manifest_hash != str(row["manifest_hash"])
                or manifest.campaign_hash != str(row["campaign_hash"])
                or manifest.shards != stored_shards
            ):
                raise PersistenceUnavailableError(
                    "research dataset manifest failed integrity verification"
                )
            return manifest
        except (LookupError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "research dataset manifest lookup failed"
            ) from None

    async def status_for_key(
        self,
        *,
        campaign_key: str,
    ) -> ResearchDataCampaignStatus | None:
        if _CAMPAIGN_KEY.fullmatch(campaign_key) is None:
            raise ValueError("campaign_key must contain 16-128 safe characters")
        try:
            async with self._engine.connect() as connection:
                campaign_hash = await connection.scalar(
                    text(
                        f"""
                        SELECT campaign_hash
                        FROM {self._schema}.research_data_campaigns
                        WHERE campaign_key = :campaign_key
                        """
                    ),
                    {"campaign_key": campaign_key},
                )
        except Exception:
            raise PersistenceUnavailableError(
                "research data campaign key lookup failed"
            ) from None
        if campaign_hash is None:
            return None
        return await self.status(campaign_hash=str(campaign_hash))

    async def create(
        self,
        spec: ResearchDataCampaignSpec,
        *,
        created_at: datetime,
    ) -> ResearchDataCampaignStatus:
        instant = to_utc(created_at, name="research data campaign creation time")
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:campaign_key))"),
                    {"campaign_key": spec.campaign_key},
                )
                existing = await connection.scalar(
                    text(
                        f"""
                        SELECT campaign_hash
                        FROM {self._schema}.research_data_campaigns
                        WHERE campaign_key = :campaign_key
                        """
                    ),
                    {"campaign_key": spec.campaign_key},
                )
                if existing is not None:
                    if str(existing) != spec.campaign_hash:
                        raise ValueError(
                            "campaign key belongs to another research data specification"
                        )
                else:
                    await self._insert_campaign(
                        connection,
                        spec=spec,
                        created_at=instant,
                    )
            return await self.status(campaign_hash=spec.campaign_hash)
        except (ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "research data campaign creation failed"
            ) from None

    async def status(
        self,
        *,
        campaign_hash: str,
    ) -> ResearchDataCampaignStatus:
        _require_lowercase_sha256(campaign_hash, name="research data campaign hash")
        try:
            async with self._engine.connect() as connection:
                parent = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.research_data_campaigns
                                WHERE campaign_hash = :campaign_hash
                                """
                            ),
                            {"campaign_hash": campaign_hash},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if parent is None:
                    raise LookupError("research data campaign does not exist")
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.research_data_campaign_items
                                WHERE campaign_hash = :campaign_hash
                                ORDER BY sequence
                                """
                            ),
                            {"campaign_hash": campaign_hash},
                        )
                    )
                    .mappings()
                    .all()
                )
                manifest_row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT manifest_hash, payload
                                FROM {self._schema}.research_dataset_manifests
                                WHERE campaign_hash = :campaign_hash
                                """
                            ),
                            {"campaign_hash": campaign_hash},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
            spec = ResearchDataCampaignSpec.from_payload(
                _object(parent["specification_payload"])
            )
            items = tuple(
                _item(row, expected_sequence=index)
                for index, row in enumerate(rows, start=1)
            )
            if (
                spec.campaign_hash != str(parent["campaign_hash"])
                or spec.policy_hash != str(parent["policy_hash"])
                or spec.start_date != parent["start_date"]
                or spec.end_date != parent["end_date"]
                or len(spec.snapshot_hashes) != int(parent["snapshot_count"])
                or len(spec.instruments) != int(parent["instrument_count"])
                or tuple(value.instrument for value in items) != spec.instruments
            ):
                raise PersistenceUnavailableError(
                    "research data campaign failed integrity verification"
                )
            manifest = None
            if manifest_row is not None:
                manifest = ResearchDatasetManifest.from_payload(
                    _object(manifest_row["payload"])
                )
                if (
                    manifest.manifest_hash != str(manifest_row["manifest_hash"])
                    or manifest.campaign_hash != spec.campaign_hash
                    or manifest.instruments != spec.instruments
                ):
                    raise PersistenceUnavailableError(
                        "research dataset manifest failed integrity verification"
                    )
            return ResearchDataCampaignStatus(
                spec=spec,
                created_at=to_utc(parent["created_at"]),
                status=_campaign_status(items, manifest),
                items=items,
                manifest=manifest,
            )
        except (LookupError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "research data campaign read failed"
            ) from None

    async def claim_next(
        self,
        *,
        campaign_hash: str,
        now: datetime,
    ) -> ResearchDataCampaignItem | None:
        _require_lowercase_sha256(campaign_hash, name="research data campaign hash")
        started_at = to_utc(now, name="research data item start time")
        try:
            async with self._engine.begin() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                WITH next_item AS (
                                    SELECT campaign_hash, sequence
                                    FROM {self._schema}.research_data_campaign_items
                                    WHERE campaign_hash = :campaign_hash
                                      AND state = 'queued'
                                    ORDER BY sequence
                                    FOR UPDATE SKIP LOCKED
                                    LIMIT 1
                                )
                                UPDATE {self._schema}.research_data_campaign_items AS item
                                SET state = 'running',
                                    attempts = item.attempts + 1,
                                    started_at = :started_at,
                                    completed_at = NULL,
                                    error_code = NULL
                                FROM next_item
                                WHERE item.campaign_hash = next_item.campaign_hash
                                  AND item.sequence = next_item.sequence
                                RETURNING item.*
                                """
                            ),
                            {
                                "campaign_hash": campaign_hash,
                                "started_at": started_at,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "research data campaign claim failed"
            ) from None
        return None if row is None else _item(row)

    async def complete_item(
        self,
        *,
        campaign_hash: str,
        sequence: int,
        manifest_hash: str,
        now: datetime,
    ) -> ResearchDataCampaignItem:
        _require_lowercase_sha256(campaign_hash, name="research data campaign hash")
        _require_lowercase_sha256(manifest_hash, name="daily shard manifest hash")
        return await self._finish_item(
            campaign_hash=campaign_hash,
            sequence=sequence,
            manifest_hash=manifest_hash,
            error_code=None,
            retryable=False,
            now=now,
        )

    async def fail_item(
        self,
        *,
        campaign_hash: str,
        sequence: int,
        error_code: str,
        retryable: bool,
        now: datetime,
    ) -> ResearchDataCampaignItem:
        _require_lowercase_sha256(campaign_hash, name="research data campaign hash")
        if not error_code or len(error_code) > 80:
            raise ValueError("error_code must contain 1-80 characters")
        return await self._finish_item(
            campaign_hash=campaign_hash,
            sequence=sequence,
            manifest_hash=None,
            error_code=error_code,
            retryable=retryable,
            now=now,
        )

    async def recover_running(
        self,
        *,
        campaign_hash: str,
    ) -> int:
        _require_lowercase_sha256(campaign_hash, name="research data campaign hash")
        try:
            async with self._engine.begin() as connection:
                result = await connection.execute(
                    text(
                        f"""
                        UPDATE {self._schema}.research_data_campaign_items
                        SET state = CASE
                                WHEN attempts < max_attempts THEN 'queued'
                                ELSE 'failed'
                            END,
                            started_at = CASE
                                WHEN attempts < max_attempts THEN NULL
                                ELSE started_at
                            END,
                            completed_at = CASE
                                WHEN attempts < max_attempts THEN NULL
                                ELSE clock_timestamp()
                            END,
                            error_code = 'worker_restarted'
                        WHERE campaign_hash = :campaign_hash
                          AND state = 'running'
                        """
                    ),
                    {"campaign_hash": campaign_hash},
                )
            return int(result.rowcount or 0)
        except Exception:
            raise PersistenceUnavailableError(
                "research data campaign recovery failed"
            ) from None

    async def retry_failed_item(
        self,
        *,
        campaign_hash: str,
        sequence: int,
        additional_attempts: int = 3,
    ) -> ResearchDataCampaignItem:
        _require_lowercase_sha256(campaign_hash, name="research data campaign hash")
        if sequence < 1:
            raise ValueError("sequence must be positive")
        if additional_attempts < 1 or additional_attempts > 3:
            raise ValueError("additional_attempts must be between 1 and 3")
        try:
            async with self._engine.begin() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                UPDATE {self._schema}.research_data_campaign_items
                                SET state = 'queued',
                                    max_attempts = max_attempts + :additional_attempts,
                                    started_at = NULL,
                                    completed_at = NULL,
                                    error_code = 'operator_retry_authorized'
                                WHERE campaign_hash = :campaign_hash
                                  AND sequence = :sequence
                                  AND state = 'failed'
                                  AND max_attempts + :additional_attempts <= 10
                                RETURNING *
                                """
                            ),
                            {
                                "additional_attempts": additional_attempts,
                                "campaign_hash": campaign_hash,
                                "sequence": sequence,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "research data campaign retry authorization failed"
            ) from None
        if row is None:
            raise ValueError(
                "research data item is not retryable or attempt ceiling would be exceeded"
            )
        return _item(row)

    async def finalize(
        self,
        *,
        campaign_hash: str,
        created_at: datetime,
    ) -> ResearchDatasetManifest | None:
        status = await self.status(campaign_hash=campaign_hash)
        if status.manifest is not None:
            return status.manifest
        if any(value.state != "completed" for value in status.items):
            return None
        instant = to_utc(created_at, name="research dataset manifest creation time")
        try:
            async with self._engine.begin() as connection:
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT item.sequence, item.instrument,
                                       item.manifest_hash, manifest.source,
                                       manifest.production_complete,
                                       (manifest.start_time AT TIME ZONE
                                           'Asia/Shanghai')::date AS start_date,
                                       (manifest.end_time AT TIME ZONE
                                           'Asia/Shanghai')::date AS end_date,
                                       manifest.payload
                                FROM {self._schema}.research_data_campaign_items item
                                JOIN {self._schema}.dataset_manifests manifest
                                  ON manifest.manifest_hash = item.manifest_hash
                                WHERE item.campaign_hash = :campaign_hash
                                  AND item.state = 'completed'
                                ORDER BY item.sequence
                                FOR SHARE
                                """
                            ),
                            {"campaign_hash": campaign_hash},
                        )
                    )
                    .mappings()
                    .all()
                )
                shards = self._verified_shards(status.spec, rows)
                manifest = ResearchDatasetManifest(
                    campaign_hash=status.spec.campaign_hash,
                    policy_hash=status.spec.policy_hash,
                    snapshot_hashes=status.spec.snapshot_hashes,
                    start_date=status.spec.start_date,
                    end_date=status.spec.end_date,
                    shards=shards,
                )
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._schema}.research_dataset_manifests
                            (manifest_hash, campaign_hash, policy_hash,
                             start_date, end_date, snapshot_count,
                             instrument_count, shard_count, created_at, payload)
                        VALUES
                            (:manifest_hash, :campaign_hash, :policy_hash,
                             :start_date, :end_date, :snapshot_count,
                             :instrument_count, :shard_count, :created_at,
                             CAST(:payload AS jsonb))
                        ON CONFLICT (campaign_hash) DO NOTHING
                        """
                    ),
                    {
                        "manifest_hash": manifest.manifest_hash,
                        "campaign_hash": manifest.campaign_hash,
                        "policy_hash": manifest.policy_hash,
                        "start_date": manifest.start_date,
                        "end_date": manifest.end_date,
                        "snapshot_count": len(manifest.snapshot_hashes),
                        "instrument_count": len(manifest.instruments),
                        "shard_count": len(manifest.shards),
                        "created_at": instant,
                        "payload": _json(manifest.payload()),
                    },
                )
                stored_hash = await connection.scalar(
                    text(
                        f"""
                        SELECT manifest_hash
                        FROM {self._schema}.research_dataset_manifests
                        WHERE campaign_hash = :campaign_hash
                        """
                    ),
                    {"campaign_hash": campaign_hash},
                )
                if str(stored_hash) != manifest.manifest_hash:
                    raise ValueError(
                        "research campaign already has another dataset manifest"
                    )
                for shard in manifest.shards:
                    await connection.execute(
                        text(
                            f"""
                            INSERT INTO {self._schema}.research_dataset_manifest_shards
                                (manifest_hash, sequence, instrument,
                                 shard_manifest_hash)
                            VALUES
                                (:manifest_hash, :sequence, :instrument,
                                 :shard_manifest_hash)
                            ON CONFLICT (manifest_hash, sequence) DO NOTHING
                            """
                        ),
                        {
                            "manifest_hash": manifest.manifest_hash,
                            "sequence": shard.sequence,
                            "instrument": shard.instrument,
                            "shard_manifest_hash": shard.manifest_hash,
                        },
                    )
            verified = await self.status(campaign_hash=campaign_hash)
            if verified.manifest is None:
                raise PersistenceUnavailableError(
                    "research dataset manifest was not persisted"
                )
            return verified.manifest
        except (ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "research dataset finalization failed"
            ) from None

    async def _insert_campaign(
        self,
        connection: AsyncConnection,
        *,
        spec: ResearchDataCampaignSpec,
        created_at: datetime,
    ) -> None:
        await connection.execute(
            text(
                f"""
                INSERT INTO {self._schema}.research_data_campaigns
                    (campaign_hash, campaign_key, policy_hash,
                     start_date, end_date, snapshot_count,
                     instrument_count, requested_by, created_at,
                     specification_payload)
                VALUES
                    (:campaign_hash, :campaign_key, :policy_hash,
                     :start_date, :end_date, :snapshot_count,
                     :instrument_count, :requested_by, :created_at,
                     CAST(:specification_payload AS jsonb))
                """
            ),
            {
                "campaign_hash": spec.campaign_hash,
                "campaign_key": spec.campaign_key,
                "policy_hash": spec.policy_hash,
                "start_date": spec.start_date,
                "end_date": spec.end_date,
                "snapshot_count": len(spec.snapshot_hashes),
                "instrument_count": len(spec.instruments),
                "requested_by": spec.requested_by,
                "created_at": created_at,
                "specification_payload": _json(spec.payload()),
            },
        )
        for sequence, instrument in enumerate(spec.instruments, start=1):
            await connection.execute(
                text(
                    f"""
                    INSERT INTO {self._schema}.research_data_campaign_items
                        (campaign_hash, sequence, instrument, state,
                         attempts, max_attempts)
                    VALUES
                        (:campaign_hash, :sequence, :instrument, 'queued',
                         0, :max_attempts)
                    """
                ),
                {
                    "campaign_hash": spec.campaign_hash,
                    "sequence": sequence,
                    "instrument": instrument,
                    "max_attempts": spec.max_attempts,
                },
            )

    async def _finish_item(
        self,
        *,
        campaign_hash: str,
        sequence: int,
        manifest_hash: str | None,
        error_code: str | None,
        retryable: bool,
        now: datetime,
    ) -> ResearchDataCampaignItem:
        if sequence < 1:
            raise ValueError("sequence must be positive")
        completed_at = to_utc(now, name="research data item completion time")
        is_success = manifest_hash is not None
        try:
            async with self._engine.begin() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                UPDATE {self._schema}.research_data_campaign_items
                                SET state = CASE
                                        WHEN :is_success THEN 'completed'
                                        WHEN :retryable
                                             AND attempts < max_attempts
                                            THEN 'queued'
                                        ELSE 'failed'
                                    END,
                                    manifest_hash = :manifest_hash,
                                    started_at = CASE
                                        WHEN NOT :is_success
                                             AND :retryable
                                             AND attempts < max_attempts
                                            THEN NULL
                                        ELSE started_at
                                    END,
                                    completed_at = CASE
                                        WHEN NOT :is_success
                                             AND :retryable
                                             AND attempts < max_attempts
                                            THEN NULL
                                        ELSE CAST(:completed_at AS timestamptz)
                                    END,
                                    error_code = :error_code
                                WHERE campaign_hash = :campaign_hash
                                  AND sequence = :sequence
                                  AND state = 'running'
                                RETURNING *
                                """
                            ),
                            {
                                "campaign_hash": campaign_hash,
                                "sequence": sequence,
                                "is_success": is_success,
                                "retryable": retryable,
                                "manifest_hash": manifest_hash,
                                "completed_at": completed_at,
                                "error_code": error_code,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except Exception:
            raise PersistenceUnavailableError(
                "research data campaign item completion failed"
            ) from None
        if row is None:
            raise PersistenceUnavailableError(
                "research data campaign item is not running"
            )
        return _item(row)

    @staticmethod
    def _verified_shards(
        spec: ResearchDataCampaignSpec,
        rows: Sequence[RowMapping],
    ) -> tuple[ResearchDatasetShard, ...]:
        if len(rows) != len(spec.instruments):
            raise PersistenceUnavailableError(
                "research dataset campaign is missing completed shards"
            )
        shards: list[ResearchDatasetShard] = []
        for sequence, (instrument, row) in enumerate(
            zip(spec.instruments, rows, strict=True),
            start=1,
        ):
            payload = _object(row["payload"])
            payload_instruments = payload.get("instruments")
            if (
                int(row["sequence"]) != sequence
                or str(row["instrument"]) != instrument
                or row["source"] != "tushare"
                or row["production_complete"] is not True
                or row["start_date"] != spec.start_date
                or row["end_date"] != spec.end_date
                or payload_instruments != [instrument]
            ):
                raise PersistenceUnavailableError(
                    "daily shard does not match research campaign bounds"
                )
            shards.append(
                ResearchDatasetShard(
                    sequence=sequence,
                    instrument=instrument,
                    manifest_hash=str(row["manifest_hash"]),
                )
            )
        return tuple(shards)


def _item(
    row: RowMapping,
    *,
    expected_sequence: int | None = None,
) -> ResearchDataCampaignItem:
    try:
        sequence = int(row["sequence"])
        state = str(row["state"])
        if expected_sequence is not None and sequence != expected_sequence:
            raise ValueError("research data item sequence is not contiguous")
        if state not in _ITEM_STATES:
            raise ValueError("research data item state is invalid")
        return ResearchDataCampaignItem(
            sequence=sequence,
            instrument=str(row["instrument"]),
            state=state,
            attempts=int(row["attempts"]),
            max_attempts=int(row["max_attempts"]),
            manifest_hash=(
                None if row["manifest_hash"] is None else str(row["manifest_hash"])
            ),
            started_at=(
                None if row["started_at"] is None else to_utc(row["started_at"])
            ),
            completed_at=(
                None if row["completed_at"] is None else to_utc(row["completed_at"])
            ),
            error_code=(
                None if row["error_code"] is None else str(row["error_code"])
            ),
        )
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "stored research data item is malformed"
        ) from None


def _campaign_status(
    items: tuple[ResearchDataCampaignItem, ...],
    manifest: ResearchDatasetManifest | None,
) -> str:
    if manifest is not None:
        return "completed"
    states = {value.state for value in items}
    if "failed" in states:
        return "failed"
    if "running" in states:
        return "running"
    if states == {"completed"}:
        return "awaiting_finalization"
    if "completed" in states:
        return "partially_completed"
    return "queued"


def _object(value: object) -> dict[str, object]:
    raw = json.loads(value) if isinstance(value, str) else value
    if not isinstance(raw, dict):
        raise TypeError("stored JSON must be an object")
    return {str(key): item for key, item in raw.items()}


def _json(value: dict[str, object]) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)
