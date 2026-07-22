from __future__ import annotations

import re
from datetime import UTC, date, datetime
from uuid import uuid4

from starlette.testclient import TestClient

from autoquant.config import AppSettings
from autoquant.web.app import create_app
from autoquant.web.models import (
    BacktestRun,
    BacktestRunDetail,
    BacktestRunRequest,
    ControlSummary,
    DailyIngestionJobRequest,
    OperatorJob,
    OperatorJobState,
    OperatorOverview,
    ResearchManifest,
)


class FakeConsoleService:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.created: list[DailyIngestionJobRequest] = []
        self.created_backtests: list[BacktestRunRequest] = []
        self.backtest_run_id = uuid4()

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def overview(self) -> OperatorOverview:
        return OperatorOverview(
            status="ok",
            postgres="ok",
            clickhouse="ok",
            tushare="configured",
            control=ControlSummary(manifests=2, quality_reports=3, audit_events=4, checkpoints=5),
            generated_at=datetime(2025, 1, 1, tzinfo=UTC),
        )

    async def list_jobs(self, *, limit: int = 50) -> tuple[OperatorJob, ...]:
        assert 1 <= limit <= 200
        return ()

    async def create_daily_job(
        self, request: DailyIngestionJobRequest, *, requested_by: str
    ) -> OperatorJob:
        self.created.append(request)
        return OperatorJob(
            job_id=uuid4(),
            state=OperatorJobState.QUEUED,
            request=request,
            requested_by=requested_by,
            created_at=datetime(2025, 1, 1, tzinfo=UTC),
        )

    async def bars(
        self,
        *,
        instrument: str,
        start: date,
        end: date,
        as_of: datetime,
    ) -> tuple[dict[str, object], ...]:
        assert instrument == "000001.XSHE"
        assert start <= end
        assert as_of.tzinfo is not None
        return ({"instrument": instrument, "session_date": start.isoformat()},)

    async def list_backtests(self, *, limit: int = 50) -> tuple[BacktestRun, ...]:
        assert 1 <= limit <= 200
        return ()

    async def list_research_manifests(
        self, *, limit: int = 100
    ) -> tuple[ResearchManifest, ...]:
        assert 1 <= limit <= 200
        return (
            ResearchManifest(
                manifest_hash="a" * 64,
                instruments=("000001.XSHE",),
                start_time=datetime(2025, 1, 1, tzinfo=UTC),
                end_time=datetime(2025, 1, 2, tzinfo=UTC),
                as_of=datetime(2025, 1, 3, tzinfo=UTC),
                row_count=8,
            ),
        )

    async def create_backtest(
        self, request: BacktestRunRequest, *, requested_by: str
    ) -> BacktestRun:
        self.created_backtests.append(request)
        return BacktestRun(
            run_id=self.backtest_run_id,
            state=OperatorJobState.QUEUED,
            strategy_id="manifest_buy_hold_v1",
            request=request,
            requested_by=requested_by,
            created_at=datetime(2025, 1, 1, tzinfo=UTC),
        )

    async def backtest_detail(self, run_id: object) -> BacktestRunDetail:
        assert run_id == self.backtest_run_id
        run = await self.create_backtest(
            BacktestRunRequest(
                manifest_hash="a" * 64,
                instrument="000001.XSHE",
                idempotency_key="web-backtest-detail-0001",
            ),
            requested_by="operator",
        )
        return BacktestRunDetail(run=run, executions=(), snapshots=(), events=())


def _settings() -> AppSettings:
    return AppSettings(
        _env_file=None,
        web_username="operator",
        web_password="local-console-password",
    )


def _auth() -> tuple[str, str]:
    return ("operator", "local-console-password")


def test_health_is_public_but_contains_no_dependency_or_secret_details() -> None:
    service = FakeConsoleService()
    app = create_app(_settings(), service=service)

    with TestClient(app) as client:
        response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert response.headers["x-frame-options"] == "DENY"
    assert "local-console-password" not in response.text
    assert service.started is True
    assert service.stopped is True


def test_console_and_api_require_authentication() -> None:
    app = create_app(_settings(), service=FakeConsoleService())

    with TestClient(app) as client:
        page = client.get("/")
        api = client.get("/api/v1/overview")
        authenticated = client.get("/", auth=_auth())

    assert page.status_code == 401
    assert api.status_code == 401
    assert authenticated.status_code == 200
    assert "AutoQuant" in authenticated.text
    assert "frame-ancestors 'none'" in authenticated.headers["content-security-policy"]


def test_overview_returns_only_operational_summary() -> None:
    app = create_app(_settings(), service=FakeConsoleService())

    with TestClient(app) as client:
        response = client.get("/api/v1/overview", auth=_auth())

    assert response.status_code == 200
    assert response.json()["control"]["quality_reports"] == 3
    assert "password" not in response.text.casefold()
    assert "dsn" not in response.text.casefold()
    assert "token" not in response.text.casefold()


def test_daily_job_requires_csrf_and_accepts_scoped_request() -> None:
    service = FakeConsoleService()
    app = create_app(_settings(), service=service)
    payload = {
        "instruments": ["000001.XSHE"],
        "start": "2025-01-01",
        "end": "2025-01-02",
        "idempotency_key": "web-test-request-0001",
    }

    with TestClient(app) as client:
        denied = client.post("/api/v1/jobs/daily-ingestion", json=payload, auth=_auth())
        page = client.get("/ingestion", auth=_auth())
        match = re.search(r'name="autoquant-csrf" content="([^"]+)"', page.text)
        assert match is not None
        accepted = client.post(
            "/api/v1/jobs/daily-ingestion",
            json=payload,
            auth=_auth(),
            headers={"X-AutoQuant-CSRF": match.group(1)},
        )

    assert denied.status_code == 403
    assert accepted.status_code == 202
    assert accepted.json()["state"] == "queued"
    assert service.created[0].instruments == ("000001.XSHE",)


def test_bar_query_requires_point_in_time_as_of() -> None:
    app = create_app(_settings(), service=FakeConsoleService())

    with TestClient(app) as client:
        missing = client.get(
            "/api/v1/bars?instrument=000001.XSHE&start=2025-01-01&end=2025-01-02",
            auth=_auth(),
        )
        naive = client.get(
            "/api/v1/bars?instrument=000001.XSHE&start=2025-01-01&end=2025-01-02"
            "&as_of=2025-01-03T00:00:00",
            auth=_auth(),
        )
        valid = client.get(
            "/api/v1/bars?instrument=000001.XSHE&start=2025-01-01&end=2025-01-02"
            "&as_of=2025-01-03T00:00:00Z",
            auth=_auth(),
        )

    assert missing.status_code == 422
    assert naive.status_code == 422
    assert valid.status_code == 200
    assert valid.json()["items"][0]["instrument"] == "000001.XSHE"


def test_trading_endpoint_is_explicitly_unavailable() -> None:
    app = create_app(_settings(), service=FakeConsoleService())

    with TestClient(app) as client:
        response = client.get("/api/v1/trading", auth=_auth())

    assert response.status_code == 200
    assert response.json()["status"] == "unavailable"
    assert response.json()["orders"] == []
    assert response.json()["positions"] == []


def test_backtest_creation_is_csrf_protected_and_strategy_is_server_selected() -> None:
    service = FakeConsoleService()
    app = create_app(_settings(), service=service)
    payload = {
        "manifest_hash": "a" * 64,
        "instrument": "000001.XSHE",
        "initial_cash": "1000000",
        "allocation": "0.95",
        "slippage_bps": "5",
        "liquidate_at_end": True,
        "idempotency_key": "web-backtest-request-0001",
    }

    with TestClient(app) as client:
        denied = client.post("/api/v1/backtests", json=payload, auth=_auth())
        page = client.get("/research", auth=_auth())
        match = re.search(r'name="autoquant-csrf" content="([^"]+)"', page.text)
        assert match is not None
        accepted = client.post(
            "/api/v1/backtests",
            json=payload,
            auth=_auth(),
            headers={"X-AutoQuant-CSRF": match.group(1)},
        )
        manifests = client.get("/api/v1/research/manifests", auth=_auth())

    assert denied.status_code == 403
    assert accepted.status_code == 202
    assert accepted.json()["strategy_id"] == "manifest_buy_hold_v1"
    assert service.created_backtests[0].manifest_hash == "a" * 64
    assert manifests.json()["items"][0]["row_count"] == 8
