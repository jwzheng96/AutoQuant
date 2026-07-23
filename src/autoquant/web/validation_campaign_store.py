from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from autoquant.backtest.validation import SmaParameters
from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.errors import PersistenceUnavailableError
from autoquant.web.models import (
    OperatorJobState,
    SmaCandidateRequest,
    WalkForwardJobRequest,
)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_CAMPAIGN_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{15,127}\Z")
CAMPAIGN_VERSION = "validation-campaign-v1"


@dataclass(frozen=True, slots=True)
class ValidationCampaignSpec:
    campaign_key: str
    manifest_hash: str
    instruments: tuple[str, ...]
    initial_cash: Decimal
    allocation: Decimal
    slippage_bps: Decimal
    train_sessions: int
    test_sessions: int
    embargo_sessions: int
    candidates: tuple[SmaParameters, ...]
    requested_by: str
    version: str = CAMPAIGN_VERSION
    campaign_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if _CAMPAIGN_KEY.fullmatch(self.campaign_key) is None:
            raise ValueError(
                "campaign_key must contain 16-128 safe characters"
            )
        _require_lowercase_sha256(
            self.manifest_hash,
            name="campaign manifest hash",
        )
        _require_nonblank(self.requested_by, name="campaign requested_by")
        if len(self.requested_by) > 128:
            raise ValueError("campaign requested_by cannot exceed 128 characters")
        instruments = tuple(sorted(self.instruments))
        if (
            len(instruments) < 3
            or len(instruments) > 20
            or len(set(instruments)) != len(instruments)
        ):
            raise ValueError(
                "validation campaign requires 3-20 unique instruments"
            )
        candidates = tuple(sorted(self.candidates))
        if (
            not candidates
            or len(candidates) > 25
            or len(set(candidates)) != len(candidates)
        ):
            raise ValueError(
                "validation campaign candidates must be unique"
            )
        object.__setattr__(
            self,
            "initial_cash",
            Decimal(_decimal_text(self.initial_cash)),
        )
        object.__setattr__(
            self,
            "allocation",
            Decimal(_decimal_text(self.allocation)),
        )
        object.__setattr__(
            self,
            "slippage_bps",
            Decimal(_decimal_text(self.slippage_bps)),
        )
        for instrument in instruments:
            self._request(
                instrument=instrument,
                idempotency_key=(
                    "validation-campaign-placeholder-"
                    f"{instrument.replace('.', '-')}"
                ),
                candidates=candidates,
            )
        if self.allocation * len(instruments) > Decimal("1"):
            raise ValueError(
                "validation campaign gross allocation exceeds one"
            )
        _require_nonblank(self.version, name="campaign version")
        if self.version != CAMPAIGN_VERSION:
            raise ValueError("validation campaign version is unsupported")
        object.__setattr__(self, "instruments", instruments)
        object.__setattr__(self, "candidates", candidates)
        object.__setattr__(
            self,
            "campaign_hash",
            _canonical_hash(self.payload()),
        )

    def request_for(self, instrument: str) -> WalkForwardJobRequest:
        if instrument not in self.instruments:
            raise ValueError(
                "campaign request instrument is not in the universe"
            )
        return self._request(
            instrument=instrument,
            idempotency_key=(
                f"validation-campaign-{self.campaign_hash[:24]}-"
                f"{instrument.replace('.', '-')}"
            ),
            candidates=self.candidates,
        )

    def payload(self) -> dict[str, object]:
        return {
            "allocation": _decimal_text(self.allocation),
            "campaign_key": self.campaign_key,
            "candidates": [
                {
                    "fast_sessions": value.fast_sessions,
                    "slow_sessions": value.slow_sessions,
                }
                for value in self.candidates
            ],
            "embargo_sessions": self.embargo_sessions,
            "initial_cash": _decimal_text(self.initial_cash),
            "instruments": list(self.instruments),
            "manifest_hash": self.manifest_hash,
            "requested_by": self.requested_by,
            "slippage_bps": _decimal_text(self.slippage_bps),
            "test_sessions": self.test_sessions,
            "train_sessions": self.train_sessions,
            "version": self.version,
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> ValidationCampaignSpec:
        raw_instruments = payload.get("instruments")
        raw_candidates = payload.get("candidates")
        if not isinstance(raw_instruments, list) or not isinstance(
            raw_candidates,
            list,
        ):
            raise TypeError("campaign payload arrays are invalid")
        candidates: list[SmaParameters] = []
        for raw in raw_candidates:
            if not isinstance(raw, dict):
                raise TypeError("campaign candidate payload is invalid")
            candidates.append(
                SmaParameters(
                    int(str(raw["fast_sessions"])),
                    int(str(raw["slow_sessions"])),
                )
            )
        value = cls(
            campaign_key=str(payload["campaign_key"]),
            manifest_hash=str(payload["manifest_hash"]),
            instruments=tuple(str(item) for item in raw_instruments),
            initial_cash=Decimal(str(payload["initial_cash"])),
            allocation=Decimal(str(payload["allocation"])),
            slippage_bps=Decimal(str(payload["slippage_bps"])),
            train_sessions=int(str(payload["train_sessions"])),
            test_sessions=int(str(payload["test_sessions"])),
            embargo_sessions=int(str(payload["embargo_sessions"])),
            candidates=tuple(candidates),
            requested_by=str(payload["requested_by"]),
            version=str(payload["version"]),
        )
        if value.payload() != payload:
            raise ValueError("campaign payload is not canonical")
        return value

    def _request(
        self,
        *,
        instrument: str,
        idempotency_key: str,
        candidates: tuple[SmaParameters, ...],
    ) -> WalkForwardJobRequest:
        return WalkForwardJobRequest(
            manifest_hash=self.manifest_hash,
            instrument=instrument,
            initial_cash=self.initial_cash,
            allocation=self.allocation,
            slippage_bps=self.slippage_bps,
            train_sessions=self.train_sessions,
            test_sessions=self.test_sessions,
            embargo_sessions=self.embargo_sessions,
            candidates=tuple(
                SmaCandidateRequest(
                    fast_sessions=value.fast_sessions,
                    slow_sessions=value.slow_sessions,
                )
                for value in candidates
            ),
            idempotency_key=idempotency_key,
        )


@dataclass(frozen=True, slots=True)
class ValidationCampaignComponentStatus:
    sequence: int
    instrument: str
    experiment_id: UUID
    state: OperatorJobState
    evidence_status: str | None
    gate_failures: tuple[str, ...]
    result_hash: str | None


@dataclass(frozen=True, slots=True)
class ValidationCampaignStatus:
    spec: ValidationCampaignSpec
    created_at: datetime
    status: str
    components: tuple[ValidationCampaignComponentStatus, ...]


class PostgresValidationCampaignRepository:
    """Atomically queue and verify one immutable multi-instrument campaign."""

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
    ) -> PostgresValidationCampaignRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "validation campaign connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def create(
        self,
        spec: ValidationCampaignSpec,
        *,
        created_at: datetime,
    ) -> ValidationCampaignStatus:
        instant = to_utc(created_at, name="campaign creation time")
        try:
            async with self._engine.begin() as connection:
                await self._lock(
                    connection,
                    campaign_key=spec.campaign_key,
                )
                existing = await connection.scalar(
                    text(
                        f"""
                        SELECT campaign_hash
                        FROM {self._schema}.validation_campaigns
                        WHERE campaign_key = :campaign_key
                        """
                    ),
                    {"campaign_key": spec.campaign_key},
                )
                if existing is not None:
                    if str(existing) != spec.campaign_hash:
                        raise ValueError(
                            "campaign key belongs to another specification"
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
                "validation campaign creation failed"
            ) from None

    async def status(
        self,
        *,
        campaign_hash: str,
    ) -> ValidationCampaignStatus:
        _require_lowercase_sha256(
            campaign_hash,
            name="campaign hash",
        )
        try:
            async with self._engine.connect() as connection:
                parent = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.validation_campaigns
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
                    raise LookupError("validation campaign does not exist")
                rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT c.*, e.state, e.result_hash,
                                       e.summary_payload,
                                       e.request_payload AS experiment_request
                                FROM {self._schema}.validation_campaign_components c
                                JOIN {self._schema}.validation_experiments e
                                  ON e.experiment_id = c.experiment_id
                                WHERE c.campaign_hash = :campaign_hash
                                ORDER BY c.sequence
                                """
                            ),
                            {"campaign_hash": campaign_hash},
                        )
                    )
                    .mappings()
                    .all()
                )
            spec = ValidationCampaignSpec.from_payload(
                _object(parent["specification_payload"])
            )
            if (
                spec.campaign_hash != str(parent["campaign_hash"])
                or spec.manifest_hash != str(parent["manifest_hash"])
                or spec.campaign_key != str(parent["campaign_key"])
                or spec.requested_by != str(parent["requested_by"])
                or len(rows) != int(parent["component_count"])
            ):
                raise PersistenceUnavailableError(
                    "validation campaign parent failed integrity verification"
                )
            components = tuple(
                self._component_status(
                    spec=spec,
                    row=row,
                    expected_sequence=sequence,
                )
                for sequence, row in enumerate(rows, start=1)
            )
            return ValidationCampaignStatus(
                spec=spec,
                created_at=to_utc(parent["created_at"]),
                status=_campaign_state(components),
                components=components,
            )
        except (LookupError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "validation campaign read failed"
            ) from None

    async def list_campaigns(
        self,
        *,
        limit: int = 50,
    ) -> tuple[ValidationCampaignStatus, ...]:
        if limit < 1 or limit > 200:
            raise ValueError("campaign limit must be between 1 and 200")
        try:
            async with self._engine.connect() as connection:
                hashes = tuple(
                    str(value)
                    for value in (
                        await connection.scalars(
                            text(
                                f"""
                                SELECT campaign_hash
                                FROM {self._schema}.validation_campaigns
                                ORDER BY created_at DESC, campaign_hash
                                LIMIT :limit
                                """
                            ),
                            {"limit": limit},
                        )
                    ).all()
                )
            return tuple(
                [
                    await self.status(campaign_hash=campaign_hash)
                    for campaign_hash in hashes
                ]
            )
        except (ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "validation campaign list failed"
            ) from None

    async def _insert_campaign(
        self,
        connection: AsyncConnection,
        *,
        spec: ValidationCampaignSpec,
        created_at: datetime,
    ) -> None:
        await connection.execute(
            text(
                f"""
                INSERT INTO {self._schema}.validation_campaigns
                    (campaign_hash, campaign_key, manifest_hash,
                     component_count, requested_by, created_at,
                     specification_payload)
                VALUES
                    (:campaign_hash, :campaign_key, :manifest_hash,
                     :component_count, :requested_by, :created_at,
                     CAST(:specification_payload AS jsonb))
                """
            ),
            {
                "campaign_hash": spec.campaign_hash,
                "campaign_key": spec.campaign_key,
                "manifest_hash": spec.manifest_hash,
                "component_count": len(spec.instruments),
                "requested_by": spec.requested_by,
                "created_at": created_at,
                "specification_payload": _json(spec.payload()),
            },
        )
        for sequence, instrument in enumerate(
            spec.instruments,
            start=1,
        ):
            request = spec.request_for(instrument)
            request_payload = request.model_dump(mode="json")
            request_hash = _canonical_hash(request_payload)
            experiment_id = uuid4()
            await connection.execute(
                text(
                    f"""
                    INSERT INTO {self._schema}.validation_experiments
                        (experiment_id, idempotency_key, state,
                         validator_id, manifest_hash, request_payload,
                         requested_by, created_at)
                    VALUES
                        (:experiment_id, :idempotency_key, 'queued',
                         'sma_cross_walk_forward_v1', :manifest_hash,
                         CAST(:request_payload AS jsonb),
                         :requested_by, :created_at)
                    """
                ),
                {
                    "experiment_id": experiment_id,
                    "idempotency_key": request.idempotency_key,
                    "manifest_hash": request.manifest_hash,
                    "request_payload": _json(request_payload),
                    "requested_by": spec.requested_by,
                    "created_at": created_at,
                },
            )
            await connection.execute(
                text(
                    f"""
                    INSERT INTO {self._schema}.validation_campaign_components
                        (campaign_hash, sequence, instrument,
                         experiment_id, request_hash, request_payload)
                    VALUES
                        (:campaign_hash, :sequence, :instrument,
                         :experiment_id, :request_hash,
                         CAST(:request_payload AS jsonb))
                    """
                ),
                {
                    "campaign_hash": spec.campaign_hash,
                    "sequence": sequence,
                    "instrument": instrument,
                    "experiment_id": experiment_id,
                    "request_hash": request_hash,
                    "request_payload": _json(request_payload),
                },
            )

    @staticmethod
    def _component_status(
        *,
        spec: ValidationCampaignSpec,
        row: RowMapping,
        expected_sequence: int,
    ) -> ValidationCampaignComponentStatus:
        instrument = str(row["instrument"])
        request = spec.request_for(instrument)
        expected_payload = request.model_dump(mode="json")
        stored_payload = _object(row["request_payload"])
        experiment_payload = _object(row["experiment_request"])
        if int(row["sequence"]) != expected_sequence:
            raise PersistenceUnavailableError(
                "validation campaign component sequence failed integrity verification"
            )
        if instrument != spec.instruments[expected_sequence - 1]:
            raise PersistenceUnavailableError(
                "validation campaign component instrument failed integrity verification"
            )
        if stored_payload != expected_payload:
            raise PersistenceUnavailableError(
                "validation campaign component payload failed integrity verification "
                f"(expected {_canonical_hash(expected_payload)}, "
                f"found {_canonical_hash(stored_payload)})"
            )
        if experiment_payload != expected_payload:
            raise PersistenceUnavailableError(
                "validation campaign experiment payload failed integrity verification"
            )
        if str(row["request_hash"]) != _canonical_hash(expected_payload):
            raise PersistenceUnavailableError(
                "validation campaign component hash failed integrity verification"
            )
        raw_summary = row["summary_payload"]
        summary = None if raw_summary is None else _object(raw_summary)
        failures = (
            ()
            if summary is None
            else _string_tuple(summary, "gate_failures")
        )
        result_hash = row["result_hash"]
        return ValidationCampaignComponentStatus(
            sequence=expected_sequence,
            instrument=instrument,
            experiment_id=UUID(str(row["experiment_id"])),
            state=OperatorJobState(str(row["state"])),
            evidence_status=(
                None
                if summary is None
                else str(summary.get("evidence_status"))
            ),
            gate_failures=failures,
            result_hash=(
                None if result_hash is None else str(result_hash)
            ),
        )

    async def _lock(
        self,
        connection: AsyncConnection,
        *,
        campaign_key: str,
    ) -> None:
        await connection.execute(
            text(
                "SELECT pg_advisory_xact_lock("
                "hashtextextended(:key, 0))"
            ),
            {"key": f"validation-campaign:{campaign_key}"},
        )


def _campaign_state(
    components: tuple[ValidationCampaignComponentStatus, ...],
) -> str:
    states = {value.state for value in components}
    if states == {"completed"}:
        if all(
            value.evidence_status == "research_candidate"
            and not value.gate_failures
            for value in components
        ):
            return "candidate_pool_ready"
        return "completed_with_rejections"
    if states & {"failed", "interrupted"}:
        return "failed"
    if states & {"running"}:
        return "running"
    return "queued"


def _json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    )


def _object(raw: object) -> dict[str, object]:
    value = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(value, dict):
        raise TypeError("stored campaign payload must be an object")
    return dict(value)


def _string_tuple(
    payload: dict[str, object],
    key: str,
) -> tuple[str, ...]:
    raw = payload.get(key)
    if not isinstance(raw, list) or any(
        not isinstance(value, str) for value in raw
    ):
        raise TypeError(f"{key} must be an array of strings")
    return tuple(raw)
