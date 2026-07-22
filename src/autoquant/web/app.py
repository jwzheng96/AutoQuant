from __future__ import annotations

import hmac
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from autoquant.adapters.clickhouse_daily import ClickHouseDailyRepository
from autoquant.adapters.postgres import PostgresControlRepository
from autoquant.config import AppSettings, WebCredentials
from autoquant.errors import AutoQuantError
from autoquant.operations import configured_dsn
from autoquant.web.models import DailyIngestionJobRequest
from autoquant.web.service import ConsoleService, ConsoleServicePort
from autoquant.web.store import PostgresOperatorRepository

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
        await active_service.start()
        try:
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

    @app.get("/api/v1/trading")
    async def trading(
        _: str = Depends(authenticated_user),
    ) -> dict[str, object]:
        return {
            "status": "unavailable",
            "orders": [],
            "positions": [],
            "reason": "Execution ledger, risk engine, and QMT gateway are not implemented",
        }

    return app


async def _production_service(settings: AppSettings) -> ConsoleService:
    postgres_dsn = configured_dsn(settings.postgres_dsn, capability="PostgreSQL")
    clickhouse_dsn = configured_dsn(settings.clickhouse_dsn, capability="ClickHouse")
    operators = PostgresOperatorRepository.connect(dsn=postgres_dsn)
    control = PostgresControlRepository.connect(dsn=postgres_dsn)
    try:
        market = await ClickHouseDailyRepository.connect(dsn=clickhouse_dsn, source="tushare")
    except Exception:
        await operators.close()
        await control.close()
        raise
    return ConsoleService(
        settings=settings,
        operator_repository=operators,
        control_repository=control,
        market_repository=market,
    )


def _credentials_match(expected: WebCredentials, supplied: HTTPBasicCredentials) -> bool:
    return hmac.compare_digest(
        expected.username.encode("utf-8"), supplied.username.encode("utf-8")
    ) and hmac.compare_digest(
        expected.password.get_secret_value().encode("utf-8"),
        supplied.password.encode("utf-8"),
    )
