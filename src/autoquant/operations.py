from __future__ import annotations

from datetime import UTC, date, datetime

from pydantic import SecretStr

from autoquant.adapters.clickhouse_daily import ClickHouseDailyRepository
from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.adapters.tushare import TushareDailySource, TushareHttpClient
from autoquant.clock import to_shanghai
from autoquant.config import AppSettings
from autoquant.data.calendar_refresh import TradingCalendarRefreshService
from autoquant.data.daily_ingestion import DailyIngestionRequest, DailyIngestionService
from autoquant.data.daily_quality import DailyQualityGate
from autoquant.data.session_reference import SessionReferenceRefreshService
from autoquant.errors import MissingCapabilityError
from autoquant.execution.control_store import PostgresExecutionControlRepository
from autoquant.execution.pre_open_marks import DailyClosePreOpenMarkReader


def configured_dsn(value: SecretStr | None, *, capability: str) -> str:
    if value is None or not value.get_secret_value().strip():
        raise MissingCapabilityError(f"{capability} is not configured")
    return value.get_secret_value()


def tushare_source(settings: AppSettings) -> TushareDailySource:
    return TushareDailySource(
        client=TushareHttpClient(
            credentials=settings.require_tushare(),
            api_url=settings.tushare_api_url,
        ),
        now=lambda: datetime.now(UTC),
    )


async def run_daily_ingestion(
    settings: AppSettings,
    instruments: tuple[str, ...],
    start: date,
    end: date,
) -> dict[str, object]:
    source: TushareDailySource | None = None
    clickhouse: ClickHouseDailyRepository | None = None
    postgres: PostgresControlRepository | None = None
    try:
        source = tushare_source(settings)
        clickhouse = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(settings.clickhouse_dsn, capability="ClickHouse"),
            source="tushare",
        )
        postgres = PostgresControlRepository.connect(
            dsn=configured_dsn(settings.postgres_dsn, capability="PostgreSQL")
        )
        service = DailyIngestionService(
            source=source,
            quality_gate=DailyQualityGate(),
            market_repository=clickhouse,
            control_repository=postgres,
            now=lambda: datetime.now(UTC),
        )
        result = await service.run(
            DailyIngestionRequest(
                instruments=instruments,
                start=start,
                end=end,
                as_of=None,
                production_complete_requested=True,
            )
        )
    finally:
        if source is not None:
            await source.close()
        if postgres is not None:
            await postgres.close()
        if clickhouse is not None:
            await clickhouse.client.close()
    return {
        "fetched_bars": result.fetched_bars,
        "fetched_factors": result.fetched_factors,
        "manifest_hash": result.manifest_hash,
        "persisted_bars": result.persisted_bars,
        "persisted_factors": result.persisted_factors,
        "quality_hash": result.quality_hash,
        "status": result.status,
    }


async def inspect_paper_pre_open(
    settings: AppSettings,
    instruments: tuple[str, ...],
    as_of: datetime,
    manifest_hash: str,
) -> dict[str, object]:
    """Prove the database-backed pre-open valuation boundary without enabling trading."""

    if settings.environment.value != "paper":
        raise MissingCapabilityError("paper environment is not configured")
    clickhouse: ClickHouseDailyRepository | None = None
    evidence: PostgresControlRepository | None = None
    controls: PostgresExecutionControlRepository | None = None
    try:
        clickhouse = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(settings.clickhouse_dsn, capability="ClickHouse"),
            source="tushare",
        )
        controls = PostgresExecutionControlRepository.connect(
            dsn=configured_dsn(settings.postgres_dsn, capability="PostgreSQL")
        )
        evidence = PostgresControlRepository.connect(
            dsn=configured_dsn(settings.postgres_dsn, capability="PostgreSQL")
        )
        await clickhouse.check_connection()
        control = await controls.replay(account_id=settings.paper_account_id)
        if not control.active:
            raise MissingCapabilityError(
                "paper pre-open inspection requires the kill switch to remain active"
            )
        marks = await DailyClosePreOpenMarkReader(
            repository=clickhouse,
            evidence_repository=evidence,
            manifest_hash=manifest_hash,
            source="tushare",
        )(
            to_shanghai(as_of, name="paper pre-open inspection time").date(),
            instruments,
            as_of,
        )
        return {
            "instrument_count": len(marks.marks),
            "kill_switch_active": control.active,
            "marks_hash": marks.marks_hash,
            "session_date": marks.session_date.isoformat(),
            "source_evidence_hash": marks.source_evidence_hash,
            "status": "ok",
            "valuation_session_date": marks.valuation_session_date.isoformat(),
        }
    finally:
        if controls is not None:
            await controls.close()
        if evidence is not None:
            await evidence.close()
        if clickhouse is not None:
            await clickhouse.client.close()


async def run_trading_calendar_refresh(
    settings: AppSettings,
    start: date,
    end: date,
) -> dict[str, object]:
    """Refresh exact Tushare calendar evidence without requesting incomplete daily bars."""

    source: TushareDailySource | None = None
    clickhouse: ClickHouseDailyRepository | None = None
    postgres: PostgresControlRepository | None = None
    try:
        source = tushare_source(settings)
        clickhouse = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(settings.clickhouse_dsn, capability="ClickHouse"),
            source="tushare",
        )
        postgres = PostgresControlRepository.connect(
            dsn=configured_dsn(settings.postgres_dsn, capability="PostgreSQL")
        )
        result = await TradingCalendarRefreshService(
            source=source,
            market_repository=clickhouse,
            control_repository=postgres,
            now=lambda: datetime.now(UTC),
        ).run(start=start, end=end)
    finally:
        if source is not None:
            await source.close()
        if postgres is not None:
            await postgres.close()
        if clickhouse is not None:
            await clickhouse.client.close()
    return {
        "audit_event_hash": result.audit_event_hash,
        "session_count": result.session_count,
        "source_evidence_hash": result.source_evidence_hash,
        "status": result.status,
    }


async def run_session_reference_refresh(
    settings: AppSettings,
    instruments: tuple[str, ...],
    session_date: date,
) -> dict[str, object]:
    """Refresh exact session controls without requesting the unfinished daily bar."""

    source: TushareDailySource | None = None
    clickhouse: ClickHouseDailyRepository | None = None
    postgres: PostgresControlRepository | None = None
    try:
        source = tushare_source(settings)
        clickhouse = await ClickHouseDailyRepository.connect(
            dsn=configured_dsn(settings.clickhouse_dsn, capability="ClickHouse"),
            source="tushare",
        )
        postgres = PostgresControlRepository.connect(
            dsn=configured_dsn(settings.postgres_dsn, capability="PostgreSQL")
        )
        result = await SessionReferenceRefreshService(
            source=source,
            market_repository=clickhouse,
            control_repository=postgres,
            now=lambda: datetime.now(UTC),
        ).run(instruments=instruments, session_date=session_date)
    finally:
        if source is not None:
            await source.close()
        if postgres is not None:
            await postgres.close()
        if clickhouse is not None:
            await clickhouse.client.close()
    return {
        "audit_event_hash": result.audit_event_hash,
        "instrument_count": result.instrument_count,
        "reference_hash": result.reference_hash,
        "session_date": result.session_date.isoformat(),
        "status": result.status,
    }
