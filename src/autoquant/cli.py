from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Annotated, NoReturn
from uuid import UUID

import typer
from pydantic import SecretStr, ValidationError

from autoquant.adapters.clickhouse import ClickHouseMinuteBarRepository
from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.adapters.rqdata import RqdataHttpSource
from autoquant.adapters.tushare import TushareDailySource
from autoquant.clock import to_utc
from autoquant.config import AppSettings
from autoquant.data.availability import HistoricalMinutePolicy
from autoquant.data.ingestion import IngestionRequest, IngestionService
from autoquant.data.quality import MinuteBarQualityGate
from autoquant.errors import AutoQuantError, MissingCapabilityError
from autoquant.execution.control_store import PostgresExecutionControlRepository
from autoquant.execution.qmt_preflight import inspect_qmt_readiness
from autoquant.execution.qmt_session_store import PostgresQmtSessionLeaseRepository
from autoquant.operations import (
    approve_paper_sma_strategy,
    inspect_paper_pre_open,
    inspect_paper_runtime_readiness,
    revoke_paper_strategy,
    run_daily_ingestion,
    run_qmt_readonly_acceptance,
    run_session_reference_refresh,
    run_trading_calendar_refresh,
    tushare_source,
    unlock_paper_runtime,
)

app = typer.Typer(add_completion=False, no_args_is_help=True)


def _emit(payload: Mapping[str, object]) -> None:
    typer.echo(json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True))


def _fail(message: str, *, code: int = 2) -> NoReturn:
    _emit({"error": message, "status": "failed"})
    raise typer.Exit(code=code)


def _settings() -> AppSettings:
    try:
        return AppSettings()
    except ValidationError:
        _fail("configuration is invalid")


def _configured_secret(value: SecretStr | None) -> bool:
    return value is not None and bool(value.get_secret_value().strip())


def _parse_instant(value: str, *, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
        return to_utc(parsed, name=name)
    except ValueError as error:
        message = (
            "timezone-aware timestamp required"
            if "timezone-aware" in str(error)
            else "invalid timestamp"
        )
        _fail(f"{name}: {message}")


def _parse_date(value: str, *, name: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        _fail(f"{name}: invalid date")
    if parsed.isoformat() != value:
        _fail(f"{name}: invalid date")
    return parsed


def _require_dsn(value: SecretStr | None, *, capability: str) -> str:
    if not _configured_secret(value):
        raise MissingCapabilityError(f"{capability} is not configured")
    if value is None:
        raise MissingCapabilityError(f"{capability} is not configured")
    return value.get_secret_value()


@app.command("config-check")
def config_check() -> None:
    """Report capability presence without emitting values."""
    settings = _settings()
    try:
        settings.require_rqdata()
        rqdata = "configured"
    except MissingCapabilityError:
        rqdata = "missing"
    try:
        settings.require_tushare()
        tushare = "configured"
    except MissingCapabilityError:
        tushare = "missing"
    try:
        settings.require_web()
        web = "configured"
    except MissingCapabilityError:
        web = "missing"
    payload = {
        "clickhouse": "configured" if _configured_secret(settings.clickhouse_dsn) else "missing",
        "environment": settings.environment.value,
        "live_trading_enabled": settings.live_trading_enabled,
        "postgres": "configured" if _configured_secret(settings.postgres_dsn) else "missing",
        "rqdata": rqdata,
        "tushare": tushare,
        "web": web,
    }
    _emit(payload)
    if "missing" in payload.values():
        raise typer.Exit(code=2)


async def _rqdata_read(
    settings: AppSettings, instrument: str, start: datetime, end: datetime
) -> int:
    source = RqdataHttpSource(
        credentials=settings.require_rqdata(),
        auth_url=settings.rqdata_auth_url,
        api_url=settings.rqdata_api_url,
        availability=HistoricalMinutePolicy(
            version="rqdata-minute-v1", delay=timedelta(seconds=5)
        ),
    )
    try:
        await source.authenticate()
        batch = await source.fetch_minute_bars((instrument,), start, end)
        return len(batch.records)
    finally:
        await source.close()


@app.command("rqdata-check")
def rqdata_check(
    instrument: Annotated[str, typer.Option("--instrument")],
    start: Annotated[str, typer.Option("--start")],
    end: Annotated[str, typer.Option("--end")],
) -> None:
    """Perform one explicit read-only RQData minute request."""
    settings = _settings()
    try:
        count = asyncio.run(
            _rqdata_read(
                settings,
                instrument,
                _parse_instant(start, name="start"),
                _parse_instant(end, name="end"),
            )
        )
    except AutoQuantError:
        _fail("RQData read-only check failed")
    _emit({"records": count, "status": "ok"})


def _tushare_source(settings: AppSettings) -> TushareDailySource:
    return tushare_source(settings)


async def _tushare_capabilities(
    settings: AppSettings, instrument: str, session_date: date
) -> dict[str, str]:
    source = _tushare_source(settings)
    try:
        return await source.probe_capabilities(
            instrument=instrument, session_date=session_date
        )
    finally:
        await source.close()


@app.command("tushare-check")
def tushare_check(
    instrument: Annotated[str, typer.Option("--instrument")] = "000001.XSHE",
    date_value: Annotated[str, typer.Option("--date")] = "2020-01-02",
) -> None:
    """Probe the required Tushare daily endpoints without writing data."""
    settings = _settings()
    try:
        statuses = asyncio.run(
            _tushare_capabilities(
                settings,
                instrument,
                _parse_date(date_value, name="date"),
            )
        )
    except (AutoQuantError, ValueError):
        _fail("Tushare capability check failed")
    sorted_statuses = dict(sorted(statuses.items()))
    complete = bool(sorted_statuses) and all(
        status == "available" for status in sorted_statuses.values()
    )
    _emit(
        {
            "capabilities": sorted_statuses,
            "status": "ok" if complete else "incomplete",
        }
    )
    if not complete:
        raise typer.Exit(code=2)


async def _database_check(settings: AppSettings) -> None:
    postgres: PostgresControlRepository | None = None
    clickhouse: ClickHouseMinuteBarRepository | None = None
    try:
        postgres = PostgresControlRepository.connect(
            dsn=_require_dsn(settings.postgres_dsn, capability="PostgreSQL")
        )
        clickhouse = await ClickHouseMinuteBarRepository.connect(
            dsn=_require_dsn(settings.clickhouse_dsn, capability="ClickHouse"),
            source="rqdata",
        )
        await postgres.check_connection()
        await clickhouse.check_connection()
    finally:
        if postgres is not None:
            await postgres.close()
        if clickhouse is not None:
            await clickhouse.client.close()


@app.command("db-check")
def db_check() -> None:
    """Verify both configured databases and phase-1 schemas."""
    try:
        asyncio.run(_database_check(_settings()))
    except AutoQuantError:
        _fail("database check failed")
    _emit({"clickhouse": "ok", "postgres": "ok", "status": "ok"})


async def _qmt_preflight_db_state(settings: AppSettings) -> tuple[bool, tuple[int, ...]]:
    dsn = _require_dsn(settings.postgres_dsn, capability="PostgreSQL")
    controls = PostgresExecutionControlRepository.connect(dsn=dsn)
    sessions = PostgresQmtSessionLeaseRepository.connect(dsn=dsn)
    try:
        control = await controls.replay(account_id=settings.paper_account_id)
        active_session_ids = await sessions.active_session_ids(now=datetime.now(UTC))
        return control.active, active_session_ids
    finally:
        await controls.close()
        await sessions.close()


@app.command("qmt-check")
def qmt_check() -> None:
    """Inspect QMT readiness without importing XtQuant or connecting to MiniQMT."""

    settings = _settings()
    try:
        kill_switch_active, active_session_ids = asyncio.run(
            _qmt_preflight_db_state(settings)
        )
    except (AutoQuantError, LookupError, ValueError):
        kill_switch_active = None
        active_session_ids = None
    report = inspect_qmt_readiness(
        settings,
        kill_switch_active=kill_switch_active,
        active_session_ids=active_session_ids,
    )
    _emit(
        {
            "checks": {
                check.code.value: "pass" if check.passed else "blocked"
                for check in report.checks
            },
            "live_trading_ready": report.live_trading_ready,
            "order_drill_ready": report.order_drill_ready,
            "read_only_ready": report.read_only_ready,
            "status": "ok" if report.order_drill_ready else "blocked",
        }
    )
    if not report.order_drill_ready:
        raise typer.Exit(code=2)


@app.command("qmt-readonly-accept")
def qmt_readonly_accept(
    actor: Annotated[str, typer.Option("--actor")],
    confirm_read_only: Annotated[
        bool,
        typer.Option("--confirm-read-only"),
    ] = False,
) -> None:
    """Connect to XtTrader for redacted read-only acceptance evidence."""

    if not confirm_read_only:
        _fail("QMT read-only acceptance requires explicit confirmation")
    try:
        payload = asyncio.run(
            run_qmt_readonly_acceptance(
                _settings(),
                actor=actor,
            )
        )
    except MissingCapabilityError as error:
        _fail(str(error))
    except (AutoQuantError, LookupError, ValueError):
        _fail("QMT read-only acceptance failed closed")
    _emit(payload)


@app.command("paper-preopen-check")
def paper_preopen_check(
    instrument: Annotated[list[str], typer.Option("--instrument")],
    manifest_hash: Annotated[str, typer.Option("--manifest-hash")],
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    """Inspect trusted pre-open marks while the paper kill switch remains active."""

    settings = _settings()
    instant = (
        datetime.now(UTC)
        if as_of is None
        else _parse_instant(as_of, name="as-of")
    )
    try:
        payload = asyncio.run(
            inspect_paper_pre_open(
                settings,
                tuple(instrument),
                instant,
                manifest_hash,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("paper pre-open check failed")
    _emit(payload)


@app.command("paper-runtime-check")
def paper_runtime_check() -> None:
    """Replay cold-start evidence without opening QMT or resetting controls."""

    try:
        payload = asyncio.run(
            inspect_paper_runtime_readiness(_settings())
        )
    except MissingCapabilityError as error:
        _fail(str(error))
    except (AutoQuantError, LookupError, ValueError):
        _fail("paper runtime readiness check failed")
    _emit(payload)


async def _run_resident_paper(settings: AppSettings) -> None:
    from autoquant.execution.paper_runtime_assembly import (
        assemble_paper_runtime,
    )
    from autoquant.execution.qmt_quote_runtime import (
        ImportedXtDataClient,
        QmtWholeQuoteRuntime,
    )

    client = ImportedXtDataClient.load()
    assembled = await assemble_paper_runtime(
        settings,
        quote_runtime_factory=lambda bridge, instruments, calendar, clock: (
            QmtWholeQuoteRuntime(
                client=client,
                bridge=bridge,
                instruments=instruments,
                calendar=calendar,
                market_clock=clock,
            )
        ),
    )
    async with assembled:
        await assembled.runtime.run(stop=asyncio.Event())


@app.command("run-paper")
def run_paper() -> None:
    """Run the leased QMT-quote paper simulator; real broker mutations stay locked."""

    try:
        asyncio.run(_run_resident_paper(_settings()))
    except (AutoQuantError, LookupError, ValueError):
        _fail("resident paper runtime failed closed")


@app.command("unlock-paper")
def unlock_paper(
    actor: Annotated[str, typer.Option("--actor")],
    confirm_paper_unlock: Annotated[
        bool,
        typer.Option("--confirm-paper-unlock"),
    ] = False,
) -> None:
    """Unlock only the running paper simulator from fresh fenced evidence."""

    if not confirm_paper_unlock:
        _fail("paper runtime unlock confirmation is required")
    try:
        payload = asyncio.run(
            unlock_paper_runtime(
                _settings(),
                actor=actor,
            )
        )
    except MissingCapabilityError as error:
        _fail(str(error))
    except (AutoQuantError, LookupError, ValueError):
        _fail("paper runtime unlock failed closed")
    _emit(payload)


@app.command("refresh-trading-calendar")
def refresh_trading_calendar(
    start: Annotated[str, typer.Option("--start")],
    end: Annotated[str, typer.Option("--end")],
) -> None:
    """Persist an exact read-only Tushare calendar interval with audit evidence."""

    settings = _settings()
    start_date = _parse_date(start, name="start")
    end_date = _parse_date(end, name="end")
    try:
        payload = asyncio.run(
            run_trading_calendar_refresh(settings, start_date, end_date)
        )
    except (AutoQuantError, ValueError):
        _fail("trading calendar refresh failed")
    _emit(payload)
    if payload["status"] != "completed":
        raise typer.Exit(code=2)


@app.command("refresh-session-reference")
def refresh_session_reference(
    instrument: Annotated[list[str], typer.Option("--instrument")],
    date_value: Annotated[str, typer.Option("--date")],
) -> None:
    """Persist exact Tushare session controls without fetching its unfinished daily bar."""

    settings = _settings()
    try:
        payload = asyncio.run(
            run_session_reference_refresh(
                settings,
                tuple(instrument),
                _parse_date(date_value, name="date"),
            )
        )
    except (AutoQuantError, ValueError):
        _fail("session reference refresh failed")
    _emit(payload)
    if payload["status"] != "completed":
        raise typer.Exit(code=2)


@app.command("approve-paper-sma")
def approve_paper_sma(
    experiment_id: Annotated[str, typer.Option("--experiment-id")],
    signal_manifest_hash: Annotated[str, typer.Option("--signal-manifest-hash")],
    reference_date: Annotated[str, typer.Option("--reference-date")],
    approved_by: Annotated[str, typer.Option("--approved-by")],
    confirm_paper_only: Annotated[
        bool,
        typer.Option("--confirm-paper-only"),
    ] = False,
) -> None:
    """Approve one gate-passing SMA artifact for paper only; never unlock live."""

    if not confirm_paper_only:
        _fail("paper-only approval confirmation is required")
    try:
        parsed_experiment_id = UUID(experiment_id)
        payload = asyncio.run(
            approve_paper_sma_strategy(
                _settings(),
                experiment_id=parsed_experiment_id,
                signal_manifest_hash=signal_manifest_hash,
                reference_session_date=_parse_date(
                    reference_date,
                    name="reference-date",
                ),
                approved_by=approved_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("paper SMA approval failed")
    _emit(payload)


@app.command("revoke-paper-strategy")
def revoke_paper_strategy_command(
    revoked_by: Annotated[str, typer.Option("--revoked-by")],
    reason: Annotated[str, typer.Option("--reason")],
    confirm_revoke: Annotated[
        bool,
        typer.Option("--confirm-revoke"),
    ] = False,
) -> None:
    """Revoke the configured paper strategy; live trading remains locked."""

    if not confirm_revoke:
        _fail("paper strategy revocation confirmation is required")
    try:
        payload = asyncio.run(
            revoke_paper_strategy(
                _settings(),
                revoked_by=revoked_by,
                reason=reason,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("paper strategy revocation failed")
    _emit(payload)


async def _ingest(
    settings: AppSettings,
    instruments: tuple[str, ...],
    start: datetime,
    end: datetime,
) -> dict[str, object]:
    source: RqdataHttpSource | None = None
    clickhouse: ClickHouseMinuteBarRepository | None = None
    postgres: PostgresControlRepository | None = None
    try:
        source = RqdataHttpSource(
            credentials=settings.require_rqdata(),
            auth_url=settings.rqdata_auth_url,
            api_url=settings.rqdata_api_url,
            availability=HistoricalMinutePolicy(
                version="rqdata-minute-v1", delay=timedelta(seconds=5)
            ),
        )
        clickhouse = await ClickHouseMinuteBarRepository.connect(
            dsn=_require_dsn(settings.clickhouse_dsn, capability="ClickHouse"),
            source="rqdata",
        )
        postgres = PostgresControlRepository.connect(
            dsn=_require_dsn(settings.postgres_dsn, capability="PostgreSQL")
        )
        service = IngestionService(
            source=source,
            quality_gate=MinuteBarQualityGate(),
            minute_repository=clickhouse,
            control_repository=postgres,
            now=lambda: datetime.now(UTC),
        )
        result = await service.run(
            IngestionRequest(
                instruments=instruments,
                start=start,
                end=end,
                as_of=datetime.now(UTC),
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
        "fetched_count": result.fetched_count,
        "manifest_hash": result.manifest_hash,
        "persisted_count": result.persisted_count,
        "quality_hash": result.quality_hash,
        "status": result.status,
    }


async def _ingest_daily(
    settings: AppSettings,
    instruments: tuple[str, ...],
    start: date,
    end: date,
) -> dict[str, object]:
    return await run_daily_ingestion(settings, instruments, start, end)


@app.command("ingest-minute")
def ingest_minute(
    instrument: Annotated[list[str], typer.Option("--instrument")],
    start: Annotated[str, typer.Option("--start")],
    end: Annotated[str, typer.Option("--end")],
) -> None:
    """Ingest an explicit RQData minute interval without enabling trading."""
    settings = _settings()
    if settings.live_trading_enabled:
        _fail("phase-1 ingestion does not enable trading")
    start_time = _parse_instant(start, name="start")
    end_time = _parse_instant(end, name="end")
    try:
        payload = asyncio.run(_ingest(settings, tuple(instrument), start_time, end_time))
    except (AutoQuantError, ValueError):
        _fail("minute ingestion failed")
    _emit(payload)
    if payload["status"] != "completed" or payload["manifest_hash"] is None:
        raise typer.Exit(code=2)


@app.command("ingest-daily")
def ingest_daily(
    instrument: Annotated[list[str], typer.Option("--instrument")],
    start: Annotated[str, typer.Option("--start")],
    end: Annotated[str, typer.Option("--end")],
) -> None:
    """Ingest an explicit Tushare daily interval without enabling trading."""
    settings = _settings()
    if settings.live_trading_enabled:
        _fail("daily ingestion does not enable trading")
    start_date = _parse_date(start, name="start")
    end_date = _parse_date(end, name="end")
    try:
        payload = asyncio.run(
            _ingest_daily(settings, tuple(instrument), start_date, end_date)
        )
    except (AutoQuantError, ValueError):
        _fail("daily ingestion failed")
    _emit(payload)
    if payload["status"] != "completed" or payload["manifest_hash"] is None:
        raise typer.Exit(code=2)


@app.command("serve-web")
def serve_web() -> None:
    """Serve the authenticated local operator console."""
    settings = _settings()
    if settings.live_trading_enabled:
        _fail("operator console does not enable live trading")
    try:
        settings.require_web()
        import uvicorn

        from autoquant.web.app import create_app

        web_app = create_app(settings)
    except (AutoQuantError, ValueError):
        _fail("operator console configuration failed")
    uvicorn.run(
        web_app,
        host=settings.web_host,
        port=settings.web_port,
        server_header=False,
    )
