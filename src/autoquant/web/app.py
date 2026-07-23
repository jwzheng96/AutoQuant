from __future__ import annotations

import hmac
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from autoquant.adapters.clickhouse_daily import ClickHouseDailyRepository
from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.backtest.runner import ManifestBacktestRunner
from autoquant.backtest.validation import WalkForwardValidator
from autoquant.config import AppSettings, WebCredentials
from autoquant.data.daily_ingestion import ValidatedDailyDatasetReader
from autoquant.errors import AutoQuantError
from autoquant.execution.control_store import PostgresExecutionControlRepository
from autoquant.execution.paper_deployment import (
    PostgresPaperDeploymentRegistry,
)
from autoquant.execution.paper_scheduler_store import PostgresPaperSchedulerRepository
from autoquant.execution.promotion_audit import (
    PostgresPaperPromotionFactRepository,
)
from autoquant.execution.qmt_readonly_store import (
    PostgresQmtReadOnlyAcceptanceRepository,
)
from autoquant.execution.qmt_session_store import (
    PostgresQmtSessionLeaseRepository,
)
from autoquant.execution.simulated_broker import PersistentSimulatedBroker
from autoquant.execution.store import PostgresPaperExecutionRepository
from autoquant.operations import configured_dsn
from autoquant.web.backtest_store import PostgresBacktestRepository
from autoquant.web.models import (
    BacktestRunRequest,
    DailyIngestionJobRequest,
    KillSwitchActivationRequest,
    WalkForwardJobRequest,
)
from autoquant.web.risk_store import PostgresRiskDecisionRepository
from autoquant.web.service import ConsoleService, ConsoleServicePort
from autoquant.web.store import PostgresOperatorRepository
from autoquant.web.validation_store import PostgresValidationRepository

_WEB_ROOT = Path(__file__).parent
_INSTRUMENT_PATTERN = r"^[0-9]{6}\.(?:XSHG|XSHE)$"


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; img-src 'self' data:; style-src 'self'; "
            "script-src 'self'; connect-src 'self'; frame-ancestors 'none'; "
            "base-uri 'none'; form-action 'self'"
        )
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        return response


def create_app(
    settings: AppSettings,
    *,
    service: ConsoleServicePort | None = None,
) -> FastAPI:
    credentials = settings.require_web()
    csrf_token = secrets.token_urlsafe(32)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        active_service = service
        if active_service is None:
            active_service = await _production_service(settings)
        app.state.console_service = active_service
        try:
            await active_service.start()
            yield
        finally:
            await active_service.stop()

    app = FastAPI(
        title="AutoQuant Operator Console",
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.add_middleware(SecurityHeadersMiddleware)
    app.mount("/static", StaticFiles(directory=_WEB_ROOT / "static"), name="static")
    templates = Jinja2Templates(directory=_WEB_ROOT / "templates")
    basic = HTTPBasic(auto_error=False)

    def authenticated_user(
        supplied: HTTPBasicCredentials | None = Depends(basic),  # noqa: B008
    ) -> str:
        if supplied is None or not _credentials_match(credentials, supplied):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="authentication required",
                headers={"WWW-Authenticate": 'Basic realm="AutoQuant"'},
            )
        return credentials.username

    def csrf_protected(
        supplied: Annotated[str | None, Header(alias="X-AutoQuant-CSRF")] = None,
    ) -> None:
        if supplied is None or not hmac.compare_digest(csrf_token, supplied):
            raise HTTPException(status_code=403, detail="CSRF validation failed")

    def active_service(request: Request) -> ConsoleServicePort:
        current: ConsoleServicePort = request.app.state.console_service
        return current

    @app.exception_handler(AutoQuantError)
    async def autoquant_error_handler(_: Request, __: AutoQuantError) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={"error": "dependency unavailable", "status": "failed"},
        )

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/", response_class=HTMLResponse)
    @app.get("/data", response_class=HTMLResponse)
    @app.get("/ingestion", response_class=HTMLResponse)
    @app.get("/research", response_class=HTMLResponse)
    @app.get("/trading", response_class=HTMLResponse)
    async def page(
        request: Request,
        _: str = Depends(authenticated_user),
    ) -> Response:
        return templates.TemplateResponse(
            request=request,
            name="console.html",
            context={
                "active_path": request.url.path,
                "csrf_token": csrf_token,
            },
        )

    @app.get("/api/v1/overview")
    async def overview(
        request: Request,
        _: str = Depends(authenticated_user),
    ) -> dict[str, object]:
        result = await active_service(request).overview()
        return result.model_dump(mode="json")

    @app.get("/api/v1/jobs")
    async def jobs(
        request: Request,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        _: str = Depends(authenticated_user),
    ) -> dict[str, object]:
        items = await active_service(request).list_jobs(limit=limit)
        return {"items": [item.model_dump(mode="json") for item in items]}

    @app.post("/api/v1/jobs/daily-ingestion", status_code=202)
    async def create_daily_ingestion(
        request: Request,
        payload: DailyIngestionJobRequest,
        user: str = Depends(authenticated_user),
        _: None = Depends(csrf_protected),
    ) -> dict[str, object]:
        job = await active_service(request).create_daily_job(payload, requested_by=user)
        return job.model_dump(mode="json")

    @app.get("/api/v1/bars")
    async def bars(
        request: Request,
        instrument: Annotated[str, Query(pattern=_INSTRUMENT_PATTERN)],
        start: date,
        end: date,
        as_of: datetime,
        _: str = Depends(authenticated_user),
    ) -> dict[str, object]:
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise HTTPException(status_code=422, detail="as_of must be timezone-aware")
        if start > end or (end - start).days > 365:
            raise HTTPException(
                status_code=422,
                detail="bar query interval must be between 1 and 366 days",
            )
        items = await active_service(request).bars(
            instrument=instrument,
            start=start,
            end=end,
            as_of=as_of,
        )
        return {"items": list(items)}

    @app.get("/api/v1/research/manifests")
    async def research_manifests(
        request: Request,
        limit: Annotated[int, Query(ge=1, le=200)] = 100,
        _: str = Depends(authenticated_user),
    ) -> dict[str, object]:
        items = await active_service(request).list_research_manifests(limit=limit)
        return {"items": [item.model_dump(mode="json") for item in items]}

    @app.get("/api/v1/backtests")
    async def backtests(
        request: Request,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        _: str = Depends(authenticated_user),
    ) -> dict[str, object]:
        items = await active_service(request).list_backtests(limit=limit)
        return {"items": [item.model_dump(mode="json") for item in items]}

    @app.post("/api/v1/backtests", status_code=202)
    async def create_backtest(
        request: Request,
        payload: BacktestRunRequest,
        user: str = Depends(authenticated_user),
        _: None = Depends(csrf_protected),
    ) -> dict[str, object]:
        try:
            run = await active_service(request).create_backtest(
                payload, requested_by=user
            )
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from None
        return run.model_dump(mode="json")

    @app.get("/api/v1/backtests/{run_id}")
    async def backtest_detail(
        request: Request,
        run_id: UUID,
        _: str = Depends(authenticated_user),
    ) -> dict[str, object]:
        try:
            detail = await active_service(request).backtest_detail(run_id)
        except LookupError:
            raise HTTPException(status_code=404, detail="backtest run not found") from None
        return detail.model_dump(mode="json")

    @app.get("/api/v1/validations")
    async def validations(
        request: Request,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        _: str = Depends(authenticated_user),
    ) -> dict[str, object]:
        items = await active_service(request).list_validations(limit=limit)
        return {"items": [item.model_dump(mode="json") for item in items]}

    @app.post("/api/v1/validations", status_code=202)
    async def create_validation(
        request: Request,
        payload: WalkForwardJobRequest,
        user: str = Depends(authenticated_user),
        _: None = Depends(csrf_protected),
    ) -> dict[str, object]:
        try:
            experiment = await active_service(request).create_validation(
                payload, requested_by=user
            )
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from None
        return experiment.model_dump(mode="json")

    @app.get("/api/v1/validations/{experiment_id}")
    async def validation_detail(
        request: Request,
        experiment_id: UUID,
        _: str = Depends(authenticated_user),
    ) -> dict[str, object]:
        try:
            detail = await active_service(request).validation_detail(experiment_id)
        except LookupError:
            raise HTTPException(
                status_code=404, detail="validation experiment not found"
            ) from None
        return detail.model_dump(mode="json")

    @app.get("/api/v1/trading")
    async def trading(
        request: Request,
        _: str = Depends(authenticated_user),
    ) -> dict[str, object]:
        risk = await active_service(request).risk_status()
        execution = await active_service(request).execution_status()
        strategy = await active_service(request).paper_strategy_status()
        qmt = await active_service(request).qmt_readonly_status()
        promotion = await active_service(request).promotion_status()
        return {
            "status": "unavailable",
            "orders": [],
            "positions": [],
            "risk": risk.model_dump(mode="json"),
            "execution": execution.model_dump(mode="json"),
            "strategy": strategy.model_dump(mode="json"),
            "qmt": qmt.model_dump(mode="json"),
            "promotion": promotion.model_dump(mode="json"),
            "reason": (
                "Paper risk, reconciliation, simulation, and approval evidence are "
                "audited. Live mode remains hard-locked until the remaining runtime, "
                "Windows QMT, and continuous-evidence gates pass"
            ),
        }

    @app.get("/api/v1/risk")
    async def risk_status(
        request: Request,
        _: str = Depends(authenticated_user),
    ) -> dict[str, object]:
        result = await active_service(request).risk_status()
        return result.model_dump(mode="json")

    @app.get("/api/v1/execution")
    async def execution_status(
        request: Request,
        _: str = Depends(authenticated_user),
    ) -> dict[str, object]:
        result = await active_service(request).execution_status()
        return result.model_dump(mode="json")

    @app.get("/api/v1/execution/strategy")
    async def paper_strategy_status(
        request: Request,
        _: str = Depends(authenticated_user),
    ) -> dict[str, object]:
        result = await active_service(request).paper_strategy_status()
        return result.model_dump(mode="json")

    @app.get("/api/v1/qmt")
    async def qmt_readonly_status(
        request: Request,
        _: str = Depends(authenticated_user),
    ) -> dict[str, object]:
        result = await active_service(request).qmt_readonly_status()
        return result.model_dump(mode="json")

    @app.get("/api/v1/promotion")
    async def promotion_status(
        request: Request,
        _: str = Depends(authenticated_user),
    ) -> dict[str, object]:
        result = await active_service(request).promotion_status()
        return result.model_dump(mode="json")

    @app.post("/api/v1/execution/kill-switch/activate")
    async def activate_kill_switch(
        request: Request,
        payload: KillSwitchActivationRequest,
        user: str = Depends(authenticated_user),
        _: None = Depends(csrf_protected),
    ) -> dict[str, object]:
        try:
            result = await active_service(request).activate_kill_switch(
                command_id=payload.command_id,
                reason=payload.reason,
                requested_by=user,
            )
        except ValueError:
            raise HTTPException(
                status_code=409,
                detail="kill switch command conflicts with existing state",
            ) from None
        return result.model_dump(mode="json")

    return app


async def _production_service(settings: AppSettings) -> ConsoleService:
    postgres_dsn = configured_dsn(settings.postgres_dsn, capability="PostgreSQL")
    clickhouse_dsn = configured_dsn(settings.clickhouse_dsn, capability="ClickHouse")
    operators = PostgresOperatorRepository.connect(dsn=postgres_dsn)
    backtests = PostgresBacktestRepository.connect(dsn=postgres_dsn)
    validations = PostgresValidationRepository.connect(dsn=postgres_dsn)
    risks = PostgresRiskDecisionRepository.connect(dsn=postgres_dsn)
    executions = PostgresPaperExecutionRepository.connect(dsn=postgres_dsn)
    execution_controls = PostgresExecutionControlRepository.connect(dsn=postgres_dsn)
    simulated_broker = PersistentSimulatedBroker.connect(dsn=postgres_dsn)
    scheduler = PostgresPaperSchedulerRepository.connect(dsn=postgres_dsn)
    strategy_registry = PostgresPaperDeploymentRegistry.connect(
        dsn=postgres_dsn
    )
    qmt_acceptances = PostgresQmtReadOnlyAcceptanceRepository.connect(
        dsn=postgres_dsn
    )
    qmt_sessions = PostgresQmtSessionLeaseRepository.connect(dsn=postgres_dsn)
    promotions = PostgresPaperPromotionFactRepository.connect(
        dsn=postgres_dsn
    )
    control = PostgresControlRepository.connect(dsn=postgres_dsn)
    try:
        market = await ClickHouseDailyRepository.connect(dsn=clickhouse_dsn, source="tushare")
    except Exception:
        await operators.close()
        await backtests.close()
        await validations.close()
        await risks.close()
        await executions.close()
        await execution_controls.close()
        await simulated_broker.close()
        await scheduler.close()
        await strategy_registry.close()
        await qmt_acceptances.close()
        await qmt_sessions.close()
        await promotions.close()
        await control.close()
        raise
    reader = ValidatedDailyDatasetReader(
        control_repository=control,
        market_repository=market,
    )
    backtest_runner = ManifestBacktestRunner(
        control_repository=control,
        dataset_reader=reader,
    )
    validation_runner = WalkForwardValidator(
        control_repository=control,
        dataset_reader=reader,
    )
    return ConsoleService(
        settings=settings,
        operator_repository=operators,
        control_repository=control,
        market_repository=market,
        backtest_repository=backtests,
        backtest_runner=backtest_runner,
        validation_repository=validations,
        validation_runner=validation_runner,
        risk_repository=risks,
        execution_repository=executions,
        execution_control_repository=execution_controls,
        simulated_broker=simulated_broker,
        scheduler_repository=scheduler,
        strategy_registry=strategy_registry,
        qmt_acceptance_repository=qmt_acceptances,
        qmt_session_repository=qmt_sessions,
        promotion_repository=promotions,
    )


def _credentials_match(expected: WebCredentials, supplied: HTTPBasicCredentials) -> bool:
    return hmac.compare_digest(
        expected.username.encode("utf-8"), supplied.username.encode("utf-8")
    ) and hmac.compare_digest(
        expected.password.get_secret_value().encode("utf-8"),
        supplied.password.encode("utf-8"),
    )
