from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Annotated, NoReturn
from uuid import UUID

import typer
from pydantic import SecretStr, ValidationError

from autoquant.adapters.clickhouse import ClickHouseMinuteBarRepository
from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.adapters.rqdata import RqdataHttpSource
from autoquant.adapters.tushare import TushareDailySource
from autoquant.backtest.validation import SmaParameters
from autoquant.clock import to_utc
from autoquant.config import AppSettings
from autoquant.data.availability import HistoricalMinutePolicy
from autoquant.data.ingestion import IngestionRequest, IngestionService
from autoquant.data.quality import MinuteBarQualityGate
from autoquant.errors import AutoQuantError, MissingCapabilityError
from autoquant.execution.compliance_approval import (
    ComplianceRevocationReason,
)
from autoquant.execution.control_store import PostgresExecutionControlRepository
from autoquant.execution.low_volatility_paper_approval import (
    LowVolatilityPaperRevocationReason,
)
from autoquant.execution.qmt_preflight import inspect_qmt_readiness
from autoquant.execution.qmt_recovery_drill import QmtRecoveryDrillKind
from autoquant.execution.qmt_session_store import PostgresQmtSessionLeaseRepository
from autoquant.operations import (
    approve_low_volatility_paper_candidate,
    approve_paper_sma_portfolio_strategy,
    approve_paper_sma_strategy,
    backfill_research_universe_snapshots,
    compile_dynamic_market_panel,
    compile_fundamental_research_panel,
    compile_research_input,
    complete_qmt_recovery_drill,
    create_compliance_approval,
    create_low_volatility_forward_evaluation_campaign,
    create_low_volatility_forward_session_campaign,
    create_portfolio_validation,
    create_research_data_campaign,
    create_research_universe_snapshot,
    create_validation_campaign,
    finalize_low_volatility_forward_session,
    freeze_dynamic_regime_research_spec,
    freeze_dynamic_research_spec,
    freeze_fundamental_research_spec,
    freeze_low_volatility_forward_evidence_spec,
    freeze_low_volatility_research_spec,
    inspect_fundamental_data_backfill,
    inspect_paper_pre_open,
    inspect_paper_promotion,
    inspect_paper_runtime_readiness,
    inspect_portfolio_validation,
    inspect_research_data_campaign,
    inspect_research_input_shard,
    inspect_validation_campaign,
    retry_research_data_campaign_item,
    revoke_compliance_approval,
    revoke_low_volatility_paper_candidate,
    revoke_paper_strategy,
    run_daily_ingestion,
    run_dynamic_validation,
    run_fundamental_data_backfill,
    run_fundamental_ingestion,
    run_fundamental_validation,
    run_low_volatility_forward_cycle,
    run_low_volatility_forward_evaluation,
    run_low_volatility_forward_window,
    run_low_volatility_validation,
    run_qmt_observer,
    run_qmt_readonly_acceptance,
    run_research_data_campaign,
    run_session_reference_refresh,
    run_trading_calendar_refresh,
    start_qmt_recovery_drill,
    tushare_source,
    unlock_paper_runtime,
)
from autoquant.web.models import (
    MomentumCandidateRequest,
    PortfolioWalkForwardJobRequest,
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


def _parse_decimal(value: str, *, name: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        _fail(f"{name}: invalid decimal")
    if not parsed.is_finite():
        _fail(f"{name}: invalid decimal")
    return parsed


def _parse_sma_candidate(value: str) -> SmaParameters:
    try:
        fast_text, slow_text = value.split(":", maxsplit=1)
        return SmaParameters(
            fast_sessions=int(fast_text),
            slow_sessions=int(slow_text),
        )
    except (TypeError, ValueError):
        _fail("candidate must use FAST:SLOW with valid SMA windows")


def _parse_momentum_candidate(
    value: str,
) -> MomentumCandidateRequest:
    try:
        lookback, rebalance, selection = value.split(":")
        return MomentumCandidateRequest(
            lookback_sessions=int(lookback),
            rebalance_sessions=int(rebalance),
            selection_count=int(selection),
        )
    except (TypeError, ValueError):
        _fail("candidate must use LOOKBACK:REBALANCE:COUNT with valid momentum windows")


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
        availability=HistoricalMinutePolicy(version="rqdata-minute-v1", delay=timedelta(seconds=5)),
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
        return await source.probe_capabilities(instrument=instrument, session_date=session_date)
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
        kill_switch_active, active_session_ids = asyncio.run(_qmt_preflight_db_state(settings))
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
                check.code.value: "pass" if check.passed else "blocked" for check in report.checks
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


@app.command("run-qmt-observer")
def qmt_observer(
    confirm_read_only: Annotated[
        bool,
        typer.Option("--confirm-read-only"),
    ] = False,
) -> None:
    """Run durable QMT callbacks and read-only full-query reconciliation."""

    if not confirm_read_only:
        _fail("QMT observer requires explicit read-only confirmation")
    try:
        asyncio.run(run_qmt_observer(_settings()))
    except MissingCapabilityError as error:
        _fail(str(error))
    except (AutoQuantError, LookupError, ValueError):
        _fail("QMT read-only observer failed closed")


@app.command("qmt-drill-start")
def qmt_drill_start(
    kind: Annotated[QmtRecoveryDrillKind, typer.Option("--kind")],
    actor: Annotated[str, typer.Option("--actor")],
    confirm_controlled_drill: Annotated[
        bool,
        typer.Option("--confirm-controlled-drill"),
    ] = False,
) -> None:
    """Start a bounded QMT failure-recovery evidence challenge."""

    if not confirm_controlled_drill:
        _fail("controlled QMT recovery drill confirmation is required")
    try:
        payload = asyncio.run(
            start_qmt_recovery_drill(
                _settings(),
                kind=kind,
                actor=actor,
            )
        )
    except MissingCapabilityError as error:
        _fail(str(error))
    except (AutoQuantError, LookupError, ValueError):
        _fail("QMT recovery drill start failed closed")
    _emit(payload)


@app.command("qmt-drill-complete")
def qmt_drill_complete(
    drill_id: Annotated[str, typer.Option("--drill-id")],
    actor: Annotated[str, typer.Option("--actor")],
    confirm_intervention_complete: Annotated[
        bool,
        typer.Option("--confirm-intervention-complete"),
    ] = False,
) -> None:
    """Complete a drill from observed fail-close and post-failure acceptance."""

    if not confirm_intervention_complete:
        _fail("QMT recovery intervention confirmation is required")
    try:
        payload = asyncio.run(
            complete_qmt_recovery_drill(
                _settings(),
                drill_id=UUID(drill_id),
                actor=actor,
            )
        )
    except MissingCapabilityError as error:
        _fail(str(error))
    except (AutoQuantError, LookupError, ValueError):
        _fail("QMT recovery drill completion failed closed")
    _emit(payload)


@app.command("paper-preopen-check")
def paper_preopen_check(
    instrument: Annotated[list[str], typer.Option("--instrument")],
    manifest_hash: Annotated[str, typer.Option("--manifest-hash")],
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    """Inspect trusted pre-open marks while the paper kill switch remains active."""

    settings = _settings()
    instant = datetime.now(UTC) if as_of is None else _parse_instant(as_of, name="as-of")
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
        payload = asyncio.run(inspect_paper_runtime_readiness(_settings()))
    except MissingCapabilityError as error:
        _fail(str(error))
    except (AutoQuantError, LookupError, ValueError):
        _fail("paper runtime readiness check failed")
    _emit(payload)


@app.command("promotion-check")
def promotion_check() -> None:
    """Audit paper-to-live evidence without changing controls or enabling orders."""

    try:
        payload = asyncio.run(inspect_paper_promotion(_settings()))
    except MissingCapabilityError as error:
        _fail(str(error))
    except (AutoQuantError, LookupError, ValueError):
        _fail("paper promotion audit failed closed")
    _emit(payload)
    if payload["status"] != "ok":
        raise typer.Exit(code=2)


@app.command("compliance-approve")
def compliance_approve(
    external_artifact_hash: Annotated[
        str,
        typer.Option("--external-artifact-hash"),
    ],
    approval_reference: Annotated[
        str,
        typer.Option("--approval-reference"),
    ],
    approved_by: Annotated[
        str,
        typer.Option("--approved-by"),
    ],
    valid_until: Annotated[
        str,
        typer.Option("--valid-until"),
    ],
    confirm_independent_compliance: Annotated[
        bool,
        typer.Option("--confirm-independent-compliance"),
    ] = False,
) -> None:
    """Record external compliance scope; never unlock trading."""

    if not confirm_independent_compliance:
        _fail("independent compliance confirmation is required")
    try:
        payload = asyncio.run(
            create_compliance_approval(
                _settings(),
                external_artifact_hash=external_artifact_hash,
                approval_reference=approval_reference,
                approved_by=approved_by,
                valid_until=_parse_instant(
                    valid_until,
                    name="valid-until",
                ),
            )
        )
    except MissingCapabilityError as error:
        _fail(str(error))
    except (AutoQuantError, LookupError, ValueError):
        _fail("compliance approval failed closed")
    _emit(payload)


@app.command("compliance-revoke")
def compliance_revoke(
    approval_hash: Annotated[
        str,
        typer.Option("--approval-hash"),
    ],
    revoked_by: Annotated[
        str,
        typer.Option("--revoked-by"),
    ],
    reason: Annotated[
        ComplianceRevocationReason,
        typer.Option("--reason"),
    ],
    confirm_revocation: Annotated[
        bool,
        typer.Option("--confirm-revocation"),
    ] = False,
) -> None:
    """Append a compliance revocation while trading stays locked."""

    if not confirm_revocation:
        _fail("compliance revocation confirmation is required")
    try:
        payload = asyncio.run(
            revoke_compliance_approval(
                _settings(),
                approval_hash=approval_hash,
                revoked_by=revoked_by,
                reason=reason,
            )
        )
    except MissingCapabilityError as error:
        _fail(str(error))
    except (AutoQuantError, LookupError, ValueError):
        _fail("compliance revocation failed closed")
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
        quote_runtime_factory=lambda bridge, instruments, calendar, clock: QmtWholeQuoteRuntime(
            client=client,
            bridge=bridge,
            instruments=instruments,
            calendar=calendar,
            market_clock=clock,
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
        payload = asyncio.run(run_trading_calendar_refresh(settings, start_date, end_date))
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


@app.command("validation-campaign-create")
def validation_campaign_create(
    campaign_key: Annotated[str, typer.Option("--campaign-key")],
    manifest_hash: Annotated[str, typer.Option("--manifest-hash")],
    instrument: Annotated[list[str], typer.Option("--instrument")],
    candidate: Annotated[list[str], typer.Option("--candidate")],
    requested_by: Annotated[str, typer.Option("--requested-by")],
    allocation: Annotated[str, typer.Option("--allocation")] = "0.20",
    slippage_bps: Annotated[
        str,
        typer.Option("--slippage-bps"),
    ] = "5",
    train_sessions: Annotated[
        int,
        typer.Option("--train-sessions"),
    ] = 120,
    test_sessions: Annotated[
        int,
        typer.Option("--test-sessions"),
    ] = 20,
    embargo_sessions: Annotated[
        int,
        typer.Option("--embargo-sessions"),
    ] = 1,
) -> None:
    """Atomically queue aligned OOS validations; never approve trading."""

    try:
        payload = asyncio.run(
            create_validation_campaign(
                _settings(),
                campaign_key=campaign_key,
                manifest_hash=manifest_hash,
                instruments=tuple(instrument),
                allocation=_parse_decimal(
                    allocation,
                    name="allocation",
                ),
                slippage_bps=_parse_decimal(
                    slippage_bps,
                    name="slippage-bps",
                ),
                train_sessions=train_sessions,
                test_sessions=test_sessions,
                embargo_sessions=embargo_sessions,
                candidates=tuple(_parse_sma_candidate(value) for value in candidate),
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("validation campaign creation failed")
    _emit(payload)


@app.command("validation-campaign-status")
def validation_campaign_status(
    campaign_hash: Annotated[str, typer.Option("--campaign-hash")],
) -> None:
    """Read one redacted validation campaign status without mutation."""

    try:
        payload = asyncio.run(
            inspect_validation_campaign(
                _settings(),
                campaign_hash=campaign_hash,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("validation campaign status failed")
    _emit(payload)
    if payload["status"] in {
        "failed",
        "completed_with_rejections",
    }:
        raise typer.Exit(code=2)


@app.command("portfolio-validation-create")
def portfolio_validation_create(
    manifest_hash: Annotated[str, typer.Option("--manifest-hash")],
    idempotency_key: Annotated[
        str,
        typer.Option("--idempotency-key"),
    ],
    requested_by: Annotated[str, typer.Option("--requested-by")],
    candidate: Annotated[list[str], typer.Option("--candidate")],
    initial_cash: Annotated[
        str,
        typer.Option("--initial-cash"),
    ] = "1000000",
    gross_allocation: Annotated[
        str,
        typer.Option("--gross-allocation"),
    ] = "0.29",
    maximum_order_notional: Annotated[
        str,
        typer.Option("--maximum-order-notional"),
    ] = "100000",
    slippage_bps: Annotated[
        str,
        typer.Option("--slippage-bps"),
    ] = "5",
    train_sessions: Annotated[
        int,
        typer.Option("--train-sessions"),
    ] = 252,
    test_sessions: Annotated[
        int,
        typer.Option("--test-sessions"),
    ] = 21,
    embargo_sessions: Annotated[
        int,
        typer.Option("--embargo-sessions"),
    ] = 1,
) -> None:
    """Queue one audited portfolio experiment; never approve trading."""

    try:
        request = PortfolioWalkForwardJobRequest(
            manifest_hash=manifest_hash,
            initial_cash=_parse_decimal(
                initial_cash,
                name="initial-cash",
            ),
            gross_allocation=_parse_decimal(
                gross_allocation,
                name="gross-allocation",
            ),
            maximum_order_notional=_parse_decimal(
                maximum_order_notional,
                name="maximum-order-notional",
            ),
            slippage_bps=_parse_decimal(
                slippage_bps,
                name="slippage-bps",
            ),
            train_sessions=train_sessions,
            test_sessions=test_sessions,
            embargo_sessions=embargo_sessions,
            candidates=tuple(_parse_momentum_candidate(value) for value in candidate),
            idempotency_key=idempotency_key,
        )
        payload = asyncio.run(
            create_portfolio_validation(
                _settings(),
                request=request,
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("portfolio validation creation failed")
    _emit(payload)


@app.command("portfolio-validation-status")
def portfolio_validation_status(
    experiment_id: Annotated[UUID, typer.Option("--experiment-id")],
) -> None:
    """Verify one stored portfolio artifact and report its gates."""

    try:
        payload = asyncio.run(
            inspect_portfolio_validation(
                _settings(),
                experiment_id=experiment_id,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("portfolio validation status failed")
    _emit(payload)
    assessment = payload.get("assessment")
    evidence_status = assessment.get("evidence_status") if isinstance(assessment, dict) else None
    if payload["state"] in {"failed", "interrupted"} or (
        payload["state"] == "completed" and evidence_status != "research_candidate"
    ):
        raise typer.Exit(code=2)


@app.command("universe-snapshot-create")
def universe_snapshot_create(
    reference_date: Annotated[str, typer.Option("--reference-date")],
    requested_by: Annotated[str, typer.Option("--requested-by")],
    index_code: Annotated[
        str,
        typer.Option("--index-code"),
    ] = "399300.SZ",
    minimum_turnover_rate_f: Annotated[
        str,
        typer.Option("--minimum-turnover-rate-f"),
    ] = "0",
    minimum_circulating_market_value: Annotated[
        str,
        typer.Option("--minimum-circulating-market-value"),
    ] = "0",
) -> None:
    """Persist one source-backed historical research universe."""

    try:
        payload = asyncio.run(
            create_research_universe_snapshot(
                _settings(),
                index_code=index_code,
                reference_date=_parse_date(
                    reference_date,
                    name="reference-date",
                ),
                minimum_turnover_rate_f=_parse_decimal(
                    minimum_turnover_rate_f,
                    name="minimum-turnover-rate-f",
                ),
                minimum_circulating_market_value=_parse_decimal(
                    minimum_circulating_market_value,
                    name="minimum-circulating-market-value",
                ),
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("research universe creation failed")
    _emit(payload)


@app.command("universe-snapshot-backfill")
def universe_snapshot_backfill(
    start_month: Annotated[str, typer.Option("--start-month")],
    end_month: Annotated[str, typer.Option("--end-month")],
    requested_by: Annotated[str, typer.Option("--requested-by")],
    index_code: Annotated[
        str,
        typer.Option("--index-code"),
    ] = "399300.SZ",
) -> None:
    """Backfill up to 12 idempotent month-end universes."""

    try:
        payload = asyncio.run(
            backfill_research_universe_snapshots(
                _settings(),
                start_month=_parse_date(
                    start_month,
                    name="start-month",
                ),
                end_month=_parse_date(
                    end_month,
                    name="end-month",
                ),
                index_code=index_code,
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("research universe backfill failed")
    _emit(payload)


@app.command("research-data-campaign-create")
def research_data_campaign_create(
    campaign_key: Annotated[str, typer.Option("--campaign-key")],
    start: Annotated[str, typer.Option("--start")],
    end: Annotated[str, typer.Option("--end")],
    requested_by: Annotated[str, typer.Option("--requested-by")],
    index_code: Annotated[
        str,
        typer.Option("--index-code"),
    ] = "399300.SZ",
    max_attempts: Annotated[
        int,
        typer.Option("--max-attempts", min=1, max=10),
    ] = 3,
) -> None:
    """Freeze an audited, survivorship-free daily data collection plan."""

    try:
        payload = asyncio.run(
            create_research_data_campaign(
                _settings(),
                campaign_key=campaign_key,
                index_code=index_code,
                start_date=_parse_date(start, name="start"),
                end_date=_parse_date(end, name="end"),
                requested_by=requested_by,
                max_attempts=max_attempts,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("research data campaign creation failed")
    _emit(payload)


@app.command("research-data-campaign-status")
def research_data_campaign_status(
    campaign_hash: Annotated[str, typer.Option("--campaign-hash")],
) -> None:
    """Inspect one research data campaign without changing its queue."""

    try:
        payload = asyncio.run(
            inspect_research_data_campaign(
                _settings(),
                campaign_hash=campaign_hash,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("research data campaign status failed")
    _emit(payload)
    if payload["status"] == "failed":
        raise typer.Exit(code=2)


@app.command("research-input-plan-compile")
def research_input_plan_compile(
    manifest_hash: Annotated[str, typer.Option("--manifest-hash")],
    requested_by: Annotated[str, typer.Option("--requested-by")],
) -> None:
    """Compile and audit strict point-in-time aggregate research inputs."""

    try:
        payload = asyncio.run(
            compile_research_input(
                _settings(),
                manifest_hash=manifest_hash,
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("research input plan compilation failed")
    _emit(payload)


@app.command("dynamic-research-spec-freeze")
def dynamic_research_spec_freeze(
    manifest_hash: Annotated[str, typer.Option("--manifest-hash")],
    requested_by: Annotated[str, typer.Option("--requested-by")],
    confirm_pre_registration: Annotated[
        bool,
        typer.Option("--confirm-pre-registration"),
    ] = False,
) -> None:
    """Freeze one strategy and its evidence gates before validation."""

    if not confirm_pre_registration:
        _fail("dynamic research pre-registration confirmation is required")
    try:
        payload = asyncio.run(
            freeze_dynamic_research_spec(
                _settings(),
                manifest_hash=manifest_hash,
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("dynamic research specification freeze failed")
    _emit(payload)


@app.command("dynamic-market-panel-compile")
def dynamic_market_panel_compile(
    spec_hash: Annotated[str, typer.Option("--spec-hash")],
    requested_by: Annotated[str, typer.Option("--requested-by")],
) -> None:
    """Compile the frozen point-in-time market panel; execution stays locked."""

    try:
        payload = asyncio.run(
            compile_dynamic_market_panel(
                _settings(),
                spec_hash=spec_hash,
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("dynamic market panel compilation failed")
    _emit(payload)


@app.command("dynamic-regime-spec-freeze")
def dynamic_regime_spec_freeze(
    predecessor_result_hash: Annotated[
        str,
        typer.Option("--predecessor-result-hash"),
    ],
    requested_by: Annotated[str, typer.Option("--requested-by")],
) -> None:
    """Freeze the v2 regime hypothesis from a rejected v1 result."""

    try:
        payload = asyncio.run(
            freeze_dynamic_regime_research_spec(
                _settings(),
                predecessor_result_hash=predecessor_result_hash,
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("dynamic regime specification freeze failed")
    _emit(payload)


@app.command("dynamic-validation-run")
def dynamic_validation_run(
    spec_hash: Annotated[str, typer.Option("--spec-hash")],
    requested_by: Annotated[str, typer.Option("--requested-by")],
) -> None:
    """Run frozen nested OOS validation; live execution remains locked."""

    try:
        payload = asyncio.run(
            run_dynamic_validation(
                _settings(),
                spec_hash=spec_hash,
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("dynamic validation failed")
    _emit(payload)


@app.command("fundamental-spec-freeze")
def fundamental_spec_freeze(
    predecessor_result_hash: Annotated[
        str,
        typer.Option("--predecessor-result-hash"),
    ],
    requested_by: Annotated[str, typer.Option("--requested-by")],
) -> None:
    """Freeze v3 quality/value factors from a rejected v2 result."""

    try:
        payload = asyncio.run(
            freeze_fundamental_research_spec(
                _settings(),
                predecessor_result_hash=predecessor_result_hash,
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("fundamental specification freeze failed")
    _emit(payload)


@app.command("fundamental-data-status")
def fundamental_data_status(
    spec_hash: Annotated[str, typer.Option("--spec-hash")],
) -> None:
    """Inspect resumable v3 fundamental collection progress."""

    try:
        payload = asyncio.run(
            inspect_fundamental_data_backfill(
                _settings(),
                spec_hash=spec_hash,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("fundamental data status failed")
    _emit(payload)


@app.command("low-volatility-spec-freeze")
def low_volatility_spec_freeze(
    predecessor_result_hash: Annotated[
        str,
        typer.Option("--predecessor-result-hash"),
    ],
    requested_by: Annotated[
        str,
        typer.Option("--requested-by"),
    ],
) -> None:
    """Freeze v4 low-volatility research from rejected v3 evidence."""

    try:
        payload = asyncio.run(
            freeze_low_volatility_research_spec(
                _settings(),
                predecessor_result_hash=(predecessor_result_hash),
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("low-volatility specification freeze failed")
    _emit(payload)


@app.command("low-volatility-forward-spec-freeze")
def low_volatility_forward_spec_freeze(
    predecessor_result_hash: Annotated[
        str,
        typer.Option("--predecessor-result-hash"),
    ],
    requested_by: Annotated[
        str,
        typer.Option("--requested-by"),
    ],
) -> None:
    """Freeze future-only v5 evidence; v4 remains rejected."""

    try:
        payload = asyncio.run(
            freeze_low_volatility_forward_evidence_spec(
                _settings(),
                predecessor_result_hash=(predecessor_result_hash),
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("low-volatility forward evidence freeze failed")
    _emit(payload)


@app.command("low-volatility-forward-session-create")
def low_volatility_forward_session_create(
    forward_spec_hash: Annotated[
        str,
        typer.Option("--forward-spec-hash"),
    ],
    session: Annotated[str, typer.Option("--session")],
    requested_by: Annotated[
        str,
        typer.Option("--requested-by"),
    ],
) -> None:
    """Create a resumable queue for one completed forward session."""

    try:
        payload = asyncio.run(
            create_low_volatility_forward_session_campaign(
                _settings(),
                forward_spec_hash=forward_spec_hash,
                session_date=_parse_date(
                    session,
                    name="session",
                ),
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("low-volatility forward session create failed")
    _emit(payload)


@app.command("low-volatility-forward-session-finalize")
def low_volatility_forward_session_finalize(
    forward_spec_hash: Annotated[
        str,
        typer.Option("--forward-spec-hash"),
    ],
    dataset_manifest_hash: Annotated[
        str,
        typer.Option("--dataset-manifest-hash"),
    ],
    requested_by: Annotated[
        str,
        typer.Option("--requested-by"),
    ],
) -> None:
    """Verify and freeze one completed forward-session dataset."""

    try:
        payload = asyncio.run(
            finalize_low_volatility_forward_session(
                _settings(),
                forward_spec_hash=forward_spec_hash,
                dataset_manifest_hash=(dataset_manifest_hash),
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("low-volatility forward session finalization failed")
    _emit(payload)


@app.command("low-volatility-forward-cycle-run")
def low_volatility_forward_cycle_run(
    forward_spec_hash: Annotated[
        str,
        typer.Option("--forward-spec-hash"),
    ],
    requested_by: Annotated[
        str,
        typer.Option("--requested-by"),
    ],
    max_items: Annotated[
        int,
        typer.Option("--max-items", min=1, max=25),
    ] = 10,
    pause_seconds: Annotated[
        str,
        typer.Option("--pause-seconds"),
    ] = "1.25",
) -> None:
    """Advance one bounded future-only evidence cycle."""

    try:
        payload = asyncio.run(
            run_low_volatility_forward_cycle(
                _settings(),
                forward_spec_hash=forward_spec_hash,
                requested_by=requested_by,
                max_items=max_items,
                pause_seconds=_parse_decimal(
                    pause_seconds,
                    name="pause-seconds",
                ),
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("low-volatility forward cycle failed")
    _emit(payload)
    if payload["status"] == "failed":
        raise typer.Exit(code=2)


@app.command("low-volatility-forward-window-run")
def low_volatility_forward_window_run(
    forward_spec_hash: Annotated[
        str,
        typer.Option("--forward-spec-hash"),
    ],
    requested_by: Annotated[
        str,
        typer.Option("--requested-by"),
    ],
    max_cycles: Annotated[
        int,
        typer.Option("--max-cycles", min=1, max=100),
    ] = 20,
    max_items: Annotated[
        int,
        typer.Option("--max-items", min=1, max=25),
    ] = 25,
    pause_seconds: Annotated[
        str,
        typer.Option("--pause-seconds"),
    ] = "1.25",
    interval_seconds: Annotated[
        str,
        typer.Option("--interval-seconds"),
    ] = "5",
) -> None:
    """Run one bounded unattended forward-collection window."""

    try:
        payload = asyncio.run(
            run_low_volatility_forward_window(
                _settings(),
                forward_spec_hash=forward_spec_hash,
                requested_by=requested_by,
                max_cycles=max_cycles,
                max_items=max_items,
                pause_seconds=_parse_decimal(
                    pause_seconds,
                    name="pause-seconds",
                ),
                interval_seconds=_parse_decimal(
                    interval_seconds,
                    name="interval-seconds",
                ),
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("low-volatility forward window failed closed")
    _emit(payload)
    if payload["status"] in {"failed", "window_exhausted"}:
        raise typer.Exit(code=2)


@app.command("low-volatility-forward-evaluate")
def low_volatility_forward_evaluate(
    forward_spec_hash: Annotated[
        str,
        typer.Option("--forward-spec-hash"),
    ],
    evaluation_dataset_manifest_hash: Annotated[
        str,
        typer.Option("--evaluation-dataset-manifest-hash"),
    ],
    requested_by: Annotated[
        str,
        typer.Option("--requested-by"),
    ],
) -> None:
    """Evaluate the frozen 126-session prefix without deploying it."""

    try:
        payload = asyncio.run(
            run_low_volatility_forward_evaluation(
                _settings(),
                forward_spec_hash=forward_spec_hash,
                evaluation_dataset_manifest_hash=(evaluation_dataset_manifest_hash),
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("low-volatility forward evaluation failed closed")
    _emit(payload)


@app.command("approve-paper-low-volatility-candidate")
def approve_paper_low_volatility_candidate(
    evaluation_result_hash: Annotated[
        str,
        typer.Option("--evaluation-result-hash"),
    ],
    approved_by: Annotated[
        str,
        typer.Option("--approved-by"),
    ],
    confirm_paper_only: Annotated[
        bool,
        typer.Option("--confirm-paper-only"),
    ] = False,
) -> None:
    """Record a paper candidate; this does not activate a runtime."""

    if not confirm_paper_only:
        _fail("paper-only candidate approval confirmation is required")
    try:
        payload = asyncio.run(
            approve_low_volatility_paper_candidate(
                _settings(),
                evaluation_result_hash=evaluation_result_hash,
                approved_by=approved_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("low-volatility paper candidate approval failed closed")
    _emit(payload)


@app.command("revoke-paper-low-volatility-candidate")
def revoke_paper_low_volatility_candidate(
    revoked_by: Annotated[
        str,
        typer.Option("--revoked-by"),
    ],
    reason: Annotated[
        LowVolatilityPaperRevocationReason,
        typer.Option("--reason"),
    ],
    confirm_revoke: Annotated[
        bool,
        typer.Option("--confirm-revoke"),
    ] = False,
) -> None:
    """Revoke the active paper candidate without touching broker state."""

    if not confirm_revoke:
        _fail("low-volatility paper candidate revocation confirmation is required")
    try:
        payload = asyncio.run(
            revoke_low_volatility_paper_candidate(
                _settings(),
                revoked_by=revoked_by,
                reason=reason,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("low-volatility paper candidate revocation failed closed")
    _emit(payload)


@app.command("low-volatility-forward-evaluation-data-create")
def low_volatility_forward_evaluation_data_create(
    forward_spec_hash: Annotated[
        str,
        typer.Option("--forward-spec-hash"),
    ],
    requested_by: Annotated[
        str,
        typer.Option("--requested-by"),
    ],
) -> None:
    """Create deterministic full coverage after the 126-day gate."""

    try:
        payload = asyncio.run(
            create_low_volatility_forward_evaluation_campaign(
                _settings(),
                forward_spec_hash=forward_spec_hash,
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("low-volatility forward evaluation data creation failed closed")
    _emit(payload)


@app.command("fundamental-data-run")
def fundamental_data_run(
    spec_hash: Annotated[str, typer.Option("--spec-hash")],
    max_items: Annotated[
        int,
        typer.Option("--max-items", min=1, max=25),
    ] = 10,
    pause_seconds: Annotated[
        str,
        typer.Option("--pause-seconds"),
    ] = "0",
) -> None:
    """Run a bounded resumable v3 fundamental-data batch."""

    try:
        payload = asyncio.run(
            run_fundamental_data_backfill(
                _settings(),
                spec_hash=spec_hash,
                max_items=max_items,
                pause_seconds=_parse_decimal(
                    pause_seconds,
                    name="pause-seconds",
                ),
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("fundamental data batch failed")
    _emit(payload)
    if payload["failed"] != 0:
        raise typer.Exit(code=2)


@app.command("fundamental-panel-compile")
def fundamental_panel_compile(
    spec_hash: Annotated[str, typer.Option("--spec-hash")],
    requested_by: Annotated[str, typer.Option("--requested-by")],
) -> None:
    """Compile and freeze the v3 point-in-time feature panel."""

    try:
        payload = asyncio.run(
            compile_fundamental_research_panel(
                _settings(),
                spec_hash=spec_hash,
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("fundamental panel compilation failed")
    _emit(payload)


@app.command("fundamental-validation-run")
def fundamental_validation_run(
    spec_hash: Annotated[str, typer.Option("--spec-hash")],
    requested_by: Annotated[str, typer.Option("--requested-by")],
) -> None:
    """Run fixed v3 OOS validation; live trading remains locked."""

    try:
        payload = asyncio.run(
            run_fundamental_validation(
                _settings(),
                spec_hash=spec_hash,
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("fundamental validation failed")
    _emit(payload)


@app.command("low-volatility-validation-run")
def low_volatility_validation_run(
    spec_hash: Annotated[str, typer.Option("--spec-hash")],
    requested_by: Annotated[str, typer.Option("--requested-by")],
) -> None:
    """Run fixed v4 OOS validation; live trading remains locked."""

    try:
        payload = asyncio.run(
            run_low_volatility_validation(
                _settings(),
                spec_hash=spec_hash,
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("low-volatility validation failed")
    _emit(payload)


@app.command("research-input-shard-check")
def research_input_shard_check(
    manifest_hash: Annotated[str, typer.Option("--manifest-hash")],
    instrument: Annotated[str, typer.Option("--instrument")],
    requested_by: Annotated[str, typer.Option("--requested-by")],
) -> None:
    """Verify one aggregate daily shard without enabling execution."""

    try:
        payload = asyncio.run(
            inspect_research_input_shard(
                _settings(),
                manifest_hash=manifest_hash,
                instrument=instrument,
                requested_by=requested_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("research input shard verification failed")
    _emit(payload)


@app.command("research-data-campaign-run")
def research_data_campaign_run(
    campaign_hash: Annotated[str, typer.Option("--campaign-hash")],
    max_items: Annotated[
        int,
        typer.Option("--max-items", min=1, max=25),
    ] = 1,
    pause_seconds: Annotated[
        str,
        typer.Option("--pause-seconds"),
    ] = "1",
) -> None:
    """Run a bounded restart-safe data batch; live trading stays locked."""

    try:
        payload = asyncio.run(
            run_research_data_campaign(
                _settings(),
                campaign_hash=campaign_hash,
                max_items=max_items,
                pause_seconds=_parse_decimal(
                    pause_seconds,
                    name="pause-seconds",
                ),
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("research data campaign execution failed")
    _emit(payload)
    if payload["status"] == "failed":
        raise typer.Exit(code=2)


@app.command("research-data-campaign-retry")
def research_data_campaign_retry(
    campaign_hash: Annotated[str, typer.Option("--campaign-hash")],
    sequence: Annotated[
        int,
        typer.Option("--sequence", min=1),
    ],
    authorized_by: Annotated[str, typer.Option("--authorized-by")],
    confirm_data_retry: Annotated[
        bool,
        typer.Option("--confirm-data-retry"),
    ] = False,
) -> None:
    """Audit and requeue one failed data shard after its cause is corrected."""

    if not confirm_data_retry:
        _fail("research data retry confirmation is required")
    try:
        payload = asyncio.run(
            retry_research_data_campaign_item(
                _settings(),
                campaign_hash=campaign_hash,
                sequence=sequence,
                authorized_by=authorized_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("research data campaign retry failed")
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


@app.command("approve-paper-portfolio")
def approve_paper_portfolio(
    experiment_id: Annotated[
        list[str],
        typer.Option("--experiment-id"),
    ],
    signal_manifest_hash: Annotated[
        list[str],
        typer.Option("--signal-manifest-hash"),
    ],
    valuation_manifest_hash: Annotated[
        str,
        typer.Option("--valuation-manifest-hash"),
    ],
    reference_date: Annotated[str, typer.Option("--reference-date")],
    approved_by: Annotated[str, typer.Option("--approved-by")],
    confirm_paper_only: Annotated[
        bool,
        typer.Option("--confirm-paper-only"),
    ] = False,
) -> None:
    """Approve matched OOS SMA components as one paper-only portfolio."""

    if not confirm_paper_only:
        _fail("paper-only approval confirmation is required")
    if (
        len(experiment_id) < 3
        or len(experiment_id) > 20
        or len(experiment_id) != len(signal_manifest_hash)
    ):
        _fail("provide 3-20 matched experiment and signal manifest options")
    try:
        payload = asyncio.run(
            approve_paper_sma_portfolio_strategy(
                _settings(),
                experiment_ids=tuple(UUID(value) for value in experiment_id),
                signal_manifest_hashes=tuple(signal_manifest_hash),
                valuation_manifest_hash=valuation_manifest_hash,
                reference_session_date=_parse_date(
                    reference_date,
                    name="reference-date",
                ),
                approved_by=approved_by,
            )
        )
    except (AutoQuantError, LookupError, ValueError):
        _fail("paper portfolio approval failed")
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


async def _ingest_fundamental(
    settings: AppSettings,
    instruments: tuple[str, ...],
    start: date,
    end: date,
) -> dict[str, object]:
    return await run_fundamental_ingestion(
        settings,
        instruments,
        start,
        end,
    )


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
        payload = asyncio.run(_ingest_daily(settings, tuple(instrument), start_date, end_date))
    except (AutoQuantError, ValueError):
        _fail("daily ingestion failed")
    _emit(payload)
    if payload["status"] != "completed" or payload["manifest_hash"] is None:
        raise typer.Exit(code=2)


@app.command("ingest-fundamental")
def ingest_fundamental(
    instrument: Annotated[list[str], typer.Option("--instrument")],
    start: Annotated[str, typer.Option("--start")],
    end: Annotated[str, typer.Option("--end")],
) -> None:
    """Ingest Tushare valuation and announcement-dated quality inputs."""

    settings = _settings()
    if settings.live_trading_enabled:
        _fail("fundamental ingestion does not enable trading")
    start_date = _parse_date(start, name="start")
    end_date = _parse_date(end, name="end")
    try:
        payload = asyncio.run(
            _ingest_fundamental(
                settings,
                tuple(instrument),
                start_date,
                end_date,
            )
        )
    except (AutoQuantError, ValueError):
        _fail("fundamental ingestion failed")
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
