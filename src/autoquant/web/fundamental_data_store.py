from __future__ import annotations

import json
import re
from collections.abc import Sequence
from datetime import date, datetime

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from autoquant.backtest.fundamental_portfolio import (
    FundamentalPortfolioResearchSpec,
)
from autoquant.clock import to_shanghai, to_utc
from autoquant.data.fundamental_dataset import (
    FundamentalDatasetShard,
    FundamentalResearchDatasetManifest,
)
from autoquant.data.models import (
    DatasetManifest,
    _require_lowercase_sha256,
)
from autoquant.errors import PersistenceUnavailableError

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class PostgresFundamentalDatasetRepository:
    """Index restart-safe per-instrument manifests and freeze their union."""

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
    ) -> PostgresFundamentalDatasetRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "fundamental dataset connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def completed_shards(
        self,
        *,
        instruments: tuple[str, ...],
        start_date: date,
        end_date: date,
    ) -> tuple[FundamentalDatasetShard, ...]:
        self._validate_request(instruments, start_date, end_date)
        try:
            async with self._engine.connect() as connection:
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT manifest_hash, payload, created_at
                                FROM {self._schema}.dataset_manifests
                                WHERE source = 'tushare-fundamental'
                                  AND production_complete
                                ORDER BY created_at DESC, manifest_hash DESC
                                """
                            )
                        )
                    )
                    .mappings()
                    .all()
                )
            return self._select_shards(
                instruments=instruments,
                start_date=start_date,
                end_date=end_date,
                rows=rows,
            )
        except PersistenceUnavailableError:
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "fundamental shard index read failed"
            ) from None

    async def read_for_spec(
        self,
        spec_hash: str,
    ) -> FundamentalResearchDatasetManifest | None:
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
                                SELECT manifest_hash, payload
                                FROM
                                    {self._schema}.fundamental_dataset_manifests
                                WHERE spec_hash = :spec_hash
                                """
                            ),
                            {"spec_hash": spec_hash},
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
                                    FROM
                                        {self._schema}.fundamental_dataset_manifest_shards
                                    WHERE manifest_hash = :manifest_hash
                                    ORDER BY sequence
                                    """
                                ),
                                {
                                    "manifest_hash": str(
                                        row["manifest_hash"]
                                    )
                                },
                            )
                        )
                        .mappings()
                        .all()
                    )
                )
            if row is None:
                return None
            manifest = FundamentalResearchDatasetManifest.from_payload(
                _object(row["payload"])
            )
            normalized = tuple(
                FundamentalDatasetShard(
                    sequence=int(value["sequence"]),
                    instrument=str(value["instrument"]),
                    manifest_hash=str(
                        value["shard_manifest_hash"]
                    ),
                )
                for value in shard_rows
            )
            if (
                manifest.manifest_hash
                != str(row["manifest_hash"])
                or manifest.spec_hash != spec_hash
                or manifest.shards != normalized
            ):
                raise PersistenceUnavailableError(
                    "fundamental dataset manifest failed "
                    "integrity verification"
                )
            return manifest
        except PersistenceUnavailableError:
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "fundamental dataset manifest read failed"
            ) from None

    async def finalize(
        self,
        *,
        spec: FundamentalPortfolioResearchSpec,
        instruments: tuple[str, ...],
        created_at: datetime,
    ) -> FundamentalResearchDatasetManifest | None:
        existing = await self.read_for_spec(spec.spec_hash)
        if existing is not None:
            if existing.instruments != instruments:
                raise PersistenceUnavailableError(
                    "frozen fundamental dataset universe differs"
                )
            return existing
        instant = to_utc(
            created_at,
            name="fundamental dataset creation time",
        )
        shards = await self.completed_shards(
            instruments=instruments,
            start_date=spec.start_date,
            end_date=spec.end_date,
        )
        if len(shards) != len(instruments):
            return None
        manifest = FundamentalResearchDatasetManifest(
            spec_hash=spec.spec_hash,
            start_date=spec.start_date,
            end_date=spec.end_date,
            shards=shards,
        )
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text(
                        "SELECT pg_advisory_xact_lock("
                        "hashtext(:identity))"
                    ),
                    {
                        "identity": (
                            f"fundamental-dataset:{spec.spec_hash}"
                        )
                    },
                )
                await self._verify_shards(
                    connection,
                    spec=spec,
                    shards=shards,
                )
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO
                            {self._schema}.fundamental_dataset_manifests
                            (manifest_hash, spec_hash, start_date,
                             end_date, instrument_count, shard_count,
                             created_at, payload)
                        VALUES
                            (:manifest_hash, :spec_hash, :start_date,
                             :end_date, :instrument_count, :shard_count,
                             :created_at, CAST(:payload AS jsonb))
                        ON CONFLICT (spec_hash) DO NOTHING
                        """
                    ),
                    {
                        "manifest_hash": manifest.manifest_hash,
                        "spec_hash": manifest.spec_hash,
                        "start_date": manifest.start_date,
                        "end_date": manifest.end_date,
                        "instrument_count": len(
                            manifest.instruments
                        ),
                        "shard_count": len(manifest.shards),
                        "created_at": instant,
                        "payload": _json(manifest.payload()),
                    },
                )
                stored_hash = await connection.scalar(
                    text(
                        f"""
                        SELECT manifest_hash
                        FROM
                            {self._schema}.fundamental_dataset_manifests
                        WHERE spec_hash = :spec_hash
                        """
                    ),
                    {"spec_hash": spec.spec_hash},
                )
                if str(stored_hash) != manifest.manifest_hash:
                    raise ValueError(
                        "fundamental spec already has another dataset"
                    )
                for shard in manifest.shards:
                    await connection.execute(
                        text(
                            f"""
                            INSERT INTO
                                {self._schema}.fundamental_dataset_manifest_shards
                                (manifest_hash, sequence, instrument,
                                 shard_manifest_hash)
                            VALUES
                                (:manifest_hash, :sequence, :instrument,
                                 :shard_manifest_hash)
                            ON CONFLICT (manifest_hash, sequence)
                            DO NOTHING
                            """
                        ),
                        {
                            "manifest_hash": manifest.manifest_hash,
                            "sequence": shard.sequence,
                            "instrument": shard.instrument,
                            "shard_manifest_hash": (
                                shard.manifest_hash
                            ),
                        },
                    )
            stored = await self.read_for_spec(spec.spec_hash)
            if stored != manifest:
                raise PersistenceUnavailableError(
                    "fundamental dataset manifest was not persisted"
                )
            return stored
        except (ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "fundamental dataset finalization failed"
            ) from None

    async def _verify_shards(
        self,
        connection: AsyncConnection,
        *,
        spec: FundamentalPortfolioResearchSpec,
        shards: tuple[FundamentalDatasetShard, ...],
    ) -> None:
        rows = (
            (
                await connection.execute(
                    text(
                        f"""
                        SELECT manifest_hash, payload
                        FROM {self._schema}.dataset_manifests
                        WHERE manifest_hash = ANY(:manifest_hashes)
                        FOR SHARE
                        """
                    ),
                    {
                        "manifest_hashes": [
                            value.manifest_hash for value in shards
                        ]
                    },
                )
            )
            .mappings()
            .all()
        )
        by_hash = {
            str(row["manifest_hash"]): _dataset_manifest(row)
            for row in rows
        }
        for shard in shards:
            value = by_hash.get(shard.manifest_hash)
            if (
                value is None
                or value.source != "tushare-fundamental"
                or value.instruments != (shard.instrument,)
                or not value.production_complete
                or to_shanghai(value.start_time).date()
                != spec.start_date
                or to_shanghai(value.end_time).date()
                != spec.end_date
            ):
                raise PersistenceUnavailableError(
                    "fundamental shard failed final verification"
                )

    @staticmethod
    def _select_shards(
        *,
        instruments: tuple[str, ...],
        start_date: date,
        end_date: date,
        rows: Sequence[RowMapping],
    ) -> tuple[FundamentalDatasetShard, ...]:
        expected = set(instruments)
        selected: dict[str, DatasetManifest] = {}
        for row in rows:
            manifest = _dataset_manifest(row)
            if (
                manifest.source != "tushare-fundamental"
                or not manifest.production_complete
                or len(manifest.instruments) != 1
                or manifest.instruments[0] not in expected
                or to_shanghai(manifest.start_time).date()
                != start_date
                or to_shanghai(manifest.end_time).date() != end_date
            ):
                continue
            selected.setdefault(manifest.instruments[0], manifest)
        return tuple(
            FundamentalDatasetShard(
                sequence=sequence,
                instrument=instrument,
                manifest_hash=selected[instrument].manifest_hash,
            )
            for sequence, instrument in enumerate(
                instruments,
                start=1,
            )
            if instrument in selected
        )

    @staticmethod
    def _validate_request(
        instruments: tuple[str, ...],
        start_date: date,
        end_date: date,
    ) -> None:
        if (
            not instruments
            or tuple(instruments) != tuple(sorted(instruments))
            or len(set(instruments)) != len(instruments)
        ):
            raise ValueError(
                "fundamental instruments must be sorted and unique"
            )
        if start_date > end_date:
            raise ValueError(
                "fundamental dataset interval is invalid"
            )


def _dataset_manifest(row: RowMapping) -> DatasetManifest:
    payload = _object(row["payload"])
    try:
        raw_instruments = payload["instruments"]
        raw_hashes = payload["record_hashes"]
        if (
            not isinstance(raw_instruments, list)
            or not isinstance(raw_hashes, list)
        ):
            raise TypeError("manifest arrays are invalid")
        manifest = DatasetManifest(
            source=str(payload["source"]),
            instruments=tuple(
                str(value) for value in raw_instruments
            ),
            start_time=datetime.fromisoformat(
                str(payload["start_time"])
            ),
            end_time=datetime.fromisoformat(
                str(payload["end_time"])
            ),
            as_of=datetime.fromisoformat(str(payload["as_of"])),
            record_hashes=tuple(
                str(value) for value in raw_hashes
            ),
            quality_report_hash=str(
                payload["quality_report_hash"]
            ),
            production_complete=bool(
                payload["production_complete"]
            ),
            row_count=int(str(payload["row_count"])),
        )
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "stored fundamental shard manifest is malformed"
        ) from None
    if manifest.manifest_hash != str(row["manifest_hash"]):
        raise PersistenceUnavailableError(
            "fundamental shard manifest hash mismatch"
        )
    return manifest


def _object(value: object) -> dict[str, object]:
    parsed = json.loads(value) if isinstance(value, str) else value
    if not isinstance(parsed, dict):
        raise TypeError("payload is not an object")
    return {str(key): item for key, item in parsed.items()}


def _json(payload: dict[str, object]) -> str:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
