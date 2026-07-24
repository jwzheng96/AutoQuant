from __future__ import annotations

import re
from datetime import UTC, date, datetime
from decimal import Decimal
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
    FundamentalValidationDetailView,
    FundamentalValidationFoldView,
    FundamentalValidationPhaseView,
    FundamentalValidationSummaryView,
    LowVolatilityForwardProgressView,
    LowVolatilityForwardSessionView,
    LowVolatilityValidationDetailView,
    LowVolatilityValidationFoldView,
    LowVolatilityValidationListItemView,
    LowVolatilityValidationPhaseView,
    OperatorJob,
    OperatorJobState,
    OperatorOverview,
    PaperExecutionStatus,
    PaperPromotionStatus,
    PaperStrategyStatus,
    PortfolioValidationExperiment,
    PortfolioValidationExperimentDetail,
    PortfolioWalkForwardJobRequest,
    PromotionGateView,
    QmtOperationsStatus,
    QmtReadOnlyStatus,
    ResearchManifest,
    RiskControlStatus,
    ValidationCampaignComponentView,
    ValidationCampaignView,
    ValidationExperiment,
    ValidationExperimentDetail,
    WalkForwardJobRequest,
)


class FakeConsoleService:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.created: list[DailyIngestionJobRequest] = []
        self.created_backtests: list[BacktestRunRequest] = []
        self.backtest_run_id = uuid4()
        self.created_validations: list[WalkForwardJobRequest] = []
        self.validation_experiment_id = uuid4()
        self.created_portfolio_validations: list[PortfolioWalkForwardJobRequest] = []
        self.portfolio_validation_experiment_id = uuid4()
        self.fundamental_result_hash = "f" * 64
        self.low_volatility_result_hash = "1" * 64
        self.kill_switch_activations: list[tuple[str, str, str]] = []

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

    async def list_research_manifests(self, *, limit: int = 100) -> tuple[ResearchManifest, ...]:
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

    async def list_validations(self, *, limit: int = 50) -> tuple[ValidationExperiment, ...]:
        assert 1 <= limit <= 200
        return ()

    async def create_validation(
        self, request: WalkForwardJobRequest, *, requested_by: str
    ) -> ValidationExperiment:
        self.created_validations.append(request)
        return ValidationExperiment(
            experiment_id=self.validation_experiment_id,
            state=OperatorJobState.QUEUED,
            validator_id="sma_cross_walk_forward_v1",
            request=request,
            requested_by=requested_by,
            created_at=datetime(2025, 1, 1, tzinfo=UTC),
        )

    async def validation_detail(self, experiment_id: object) -> ValidationExperimentDetail:
        assert experiment_id == self.validation_experiment_id
        experiment = await self.create_validation(
            WalkForwardJobRequest(
                manifest_hash="a" * 64,
                instrument="000001.XSHE",
                idempotency_key="web-validation-detail-0001",
            ),
            requested_by="operator",
        )
        return ValidationExperimentDetail(experiment=experiment, folds=())

    async def list_portfolio_validations(
        self,
        *,
        limit: int = 50,
    ) -> tuple[PortfolioValidationExperiment, ...]:
        assert 1 <= limit <= 200
        return ()

    async def list_fundamental_validations(
        self,
        *,
        limit: int = 20,
    ) -> tuple[FundamentalValidationSummaryView, ...]:
        assert 1 <= limit <= 200
        return (self._fundamental_summary(),)

    async def fundamental_validation_detail(
        self,
        result_hash: str,
    ) -> FundamentalValidationDetailView:
        assert result_hash == self.fundamental_result_hash
        phase = FundamentalValidationPhaseView(
            total_return=Decimal("0.01"),
            max_drawdown=Decimal("0.02"),
            ending_equity=Decimal("1010000"),
            rejected_order_count=0,
            unresolved_position_count=0,
            artifact_hash="e" * 64,
        )
        return FundamentalValidationDetailView(
            summary=self._fundamental_summary(),
            folds=(
                FundamentalValidationFoldView(
                    sequence=1,
                    train_start=date(2020, 1, 1),
                    train_end=date(2021, 12, 31),
                    test_start=date(2022, 1, 10),
                    test_end=date(2022, 4, 8),
                    training=phase,
                    test=phase,
                    benchmark=phase,
                    fold_hash="d" * 64,
                ),
            ),
        )

    def _fundamental_summary(
        self,
    ) -> FundamentalValidationSummaryView:
        return FundamentalValidationSummaryView(
            result_hash=self.fundamental_result_hash,
            assessment_hash="a" * 64,
            spec_hash="b" * 64,
            strategy_id="dynamic-universe-quality-value-v3",
            evidence_status="rejected",
            gate_failures=("nonpositive_excess_return",),
            fold_count=1,
            oos_sessions=63,
            compounded_oos_return=Decimal("0.01"),
            benchmark_compounded_oos_return=Decimal("0.02"),
            excess_oos_return=Decimal("-0.01"),
            profitable_fold_rate=Decimal("1"),
            worst_oos_drawdown=Decimal("0.02"),
            train_test_gap=Decimal("0"),
            rejected_order_count=0,
            unresolved_position_count=0,
            strategy_unresolved_position_count=0,
            benchmark_unresolved_position_count=0,
            requested_by="operator",
            completed_at=datetime(2026, 7, 23, tzinfo=UTC),
        )

    async def list_low_volatility_validations(
        self,
        *,
        limit: int = 20,
    ) -> tuple[LowVolatilityValidationListItemView, ...]:
        assert 1 <= limit <= 200
        return (self._low_volatility_summary(),)

    async def low_volatility_validation_detail(
        self,
        result_hash: str,
    ) -> LowVolatilityValidationDetailView:
        assert result_hash == self.low_volatility_result_hash
        phase = LowVolatilityValidationPhaseView(
            total_return=Decimal("0.01"),
            max_drawdown=Decimal("0.02"),
            ending_equity=Decimal("1010000"),
            rejected_order_count=0,
            unresolved_position_count=0,
            artifact_hash="2" * 64,
        )
        return LowVolatilityValidationDetailView(
            summary=self._low_volatility_summary(),
            folds=(
                LowVolatilityValidationFoldView(
                    sequence=1,
                    train_start=date(2020, 1, 1),
                    train_end=date(2021, 12, 31),
                    test_start=date(2022, 1, 10),
                    test_end=date(2022, 4, 8),
                    training=phase,
                    test=phase,
                    benchmark=phase,
                    fold_hash="3" * 64,
                ),
            ),
        )

    async def low_volatility_forward_progress(
        self,
    ) -> LowVolatilityForwardProgressView:
        session = LowVolatilityForwardSessionView(
            binding_hash="6" * 64,
            dataset_manifest_hash="7" * 64,
            session_date=date(2026, 7, 23),
            snapshot_hash="8" * 64,
            snapshot_reference_date=date(2026, 7, 22),
            instrument_count=300,
            completed_at=datetime(
                2026,
                7,
                24,
                tzinfo=UTC,
            ),
        )
        return LowVolatilityForwardProgressView(
            spec_hash="9" * 64,
            forward_start_date=date(2026, 7, 23),
            safe_cutoff_date=date(2026, 7, 23),
            minimum_forward_sessions=126,
            minimum_paper_sessions=60,
            observed_open_sessions=1,
            completed_sessions=1,
            completed_required_sessions=1,
            remaining_required_sessions=125,
            missing_session_dates=(),
            calendar_conflict_dates=(),
            status="collecting_forward_sessions",
            sessions=(session,),
        )

    def _low_volatility_summary(
        self,
    ) -> LowVolatilityValidationListItemView:
        return LowVolatilityValidationListItemView(
            result_hash=self.low_volatility_result_hash,
            assessment_hash="4" * 64,
            spec_hash="5" * 64,
            strategy_id="dynamic-universe-low-volatility-v4",
            evidence_status="rejected",
            gate_failures=("train_test_gap",),
            fold_count=1,
            oos_sessions=63,
            compounded_oos_return=Decimal("0.02"),
            benchmark_compounded_oos_return=Decimal("0.01"),
            excess_oos_return=Decimal("0.01"),
            profitable_fold_rate=Decimal("1"),
            worst_oos_drawdown=Decimal("0.02"),
            train_test_gap=Decimal("0.15"),
            strategy_rejected_order_count=0,
            benchmark_rejected_order_count=0,
            strategy_unresolved_position_count=0,
            benchmark_unresolved_position_count=1,
            requested_by="operator",
            completed_at=datetime(2026, 7, 24, tzinfo=UTC),
        )

    async def create_portfolio_validation(
        self,
        request: PortfolioWalkForwardJobRequest,
        *,
        requested_by: str,
    ) -> PortfolioValidationExperiment:
        self.created_portfolio_validations.append(request)
        return PortfolioValidationExperiment(
            experiment_id=self.portfolio_validation_experiment_id,
            state=OperatorJobState.QUEUED,
            validator_id=("cross_sectional_momentum_walk_forward_v1"),
            request=request,
            requested_by=requested_by,
            created_at=datetime(2025, 1, 1, tzinfo=UTC),
        )

    async def portfolio_validation_detail(
        self,
        experiment_id: object,
    ) -> PortfolioValidationExperimentDetail:
        assert experiment_id == self.portfolio_validation_experiment_id
        experiment = await self.create_portfolio_validation(
            PortfolioWalkForwardJobRequest(
                manifest_hash="a" * 64,
                idempotency_key="web-portfolio-detail-0001",
            ),
            requested_by="operator",
        )
        return PortfolioValidationExperimentDetail(
            experiment=experiment,
            folds=(),
        )

    async def list_validation_campaigns(
        self,
        *,
        limit: int = 50,
    ) -> tuple[ValidationCampaignView, ...]:
        assert 1 <= limit <= 200
        return (
            ValidationCampaignView(
                campaign_hash="c" * 64,
                campaign_key="campaign-web-test-0001",
                manifest_hash="a" * 64,
                instruments=(
                    "000001.XSHE",
                    "600000.XSHG",
                    "600519.XSHG",
                ),
                created_at=datetime(2025, 1, 1, tzinfo=UTC),
                status="queued",
                components=tuple(
                    ValidationCampaignComponentView(
                        sequence=sequence,
                        instrument=instrument,
                        experiment_id=uuid4(),
                        state="queued",
                    )
                    for sequence, instrument in enumerate(
                        (
                            "000001.XSHE",
                            "600000.XSHG",
                            "600519.XSHG",
                        ),
                        start=1,
                    )
                ),
            ),
        )

    async def risk_status(self) -> RiskControlStatus:
        return RiskControlStatus(
            status="locked",
            live_trading_locked=True,
            paper_gateway_available=False,
            decision_count=7,
            recent_decisions=(),
            remaining_gates=("paper_account_state", "qmt_gateway"),
        )

    async def execution_status(self) -> PaperExecutionStatus:
        return PaperExecutionStatus(
            status="locked",
            persistence_available=True,
            recovery_verified=True,
            gateway_available=False,
            order_count=2,
            event_count=3,
            reconciliation_count=1,
            open_order_count=1,
            latest_reconciliation_at=datetime(2025, 1, 1, tzinfo=UTC),
            latest_reconciled=True,
            kill_switch_active=True,
            kill_switch_reason="initializing",
            kill_switch_version=1,
            simulated_broker_available=True,
            simulated_broker_recovery_verified=True,
            simulated_broker_order_count=0,
            simulated_broker_fact_count=0,
            remaining_gates=("paper_broker_adapter", "kill_switch_drill"),
        )

    async def paper_strategy_status(self) -> PaperStrategyStatus:
        return PaperStrategyStatus(
            status="inactive",
            active=False,
            account_id="paper-main",
            strategy_id="validated-sma-paper",
            remaining_gates=("sample_out_candidate", "explicit_paper_approval"),
        )

    async def qmt_readonly_status(self) -> QmtReadOnlyStatus:
        return QmtReadOnlyStatus(
            status="blocked",
            current_host_read_only_ready=False,
            checks={"windows_runtime": "blocked"},
            evidence_fresh=False,
            remaining_gates=("windows_qmt_readonly_acceptance",),
        )

    async def qmt_operations_status(self) -> QmtOperationsStatus:
        return QmtOperationsStatus(
            status="idle",
            integrity_verified=True,
            lease_active=False,
            callback_cursor=0,
            processing_event_count=0,
            processing_hash="0" * 64,
            broker_state_known=False,
            reconciliation_current=False,
            orders=(),
            trades=(),
        )

    async def promotion_status(self) -> PaperPromotionStatus:
        return PaperPromotionStatus(
            status="blocked",
            evaluated_at=datetime(2025, 1, 1, tzinfo=UTC),
            policy_hash="a" * 64,
            fact_hash="b" * 64,
            report_hash="c" * 64,
            blockers=("paper_session_count", "compliance_approval"),
            gates={
                "paper_session_count": PromotionGateView(
                    status="blocked",
                    actual="0",
                    required=">=60",
                )
            },
        )

    async def activate_kill_switch(
        self, *, command_id: str, reason: str, requested_by: str
    ) -> PaperExecutionStatus:
        self.kill_switch_activations.append((command_id, reason, requested_by))
        return await self.execution_status()


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


def test_fundamental_validation_endpoints_are_authenticated_and_read_only() -> None:
    service = FakeConsoleService()
    app = create_app(_settings(), service=service)

    with TestClient(app) as client:
        denied = client.get("/api/v1/fundamental-validations")
        listed = client.get(
            "/api/v1/fundamental-validations",
            auth=_auth(),
        )
        detail = client.get(
            (f"/api/v1/fundamental-validations/{service.fundamental_result_hash}"),
            auth=_auth(),
        )

    assert denied.status_code == 401
    assert listed.status_code == 200
    assert listed.json()["items"][0]["evidence_status"] == "rejected"
    assert listed.json()["items"][0]["live_trading_locked"] is True
    assert detail.status_code == 200
    assert detail.json()["integrity_verified"] is True
    assert detail.json()["folds"][0]["benchmark"]["artifact_hash"] == "e" * 64
    assert "approve" not in detail.text.casefold()


def test_low_volatility_validation_endpoints_are_authenticated_and_read_only() -> None:
    service = FakeConsoleService()
    app = create_app(_settings(), service=service)

    with TestClient(app) as client:
        denied = client.get("/api/v1/low-volatility-validations")
        listed = client.get(
            "/api/v1/low-volatility-validations",
            auth=_auth(),
        )
        detail = client.get(
            (f"/api/v1/low-volatility-validations/{service.low_volatility_result_hash}"),
            auth=_auth(),
        )

    assert denied.status_code == 401
    assert listed.status_code == 200
    assert listed.json()["items"][0]["gate_failures"] == ["train_test_gap"]
    assert listed.json()["items"][0]["live_trading_locked"] is True
    assert detail.status_code == 200
    assert detail.json()["integrity_verified"] is True
    assert detail.json()["summary"]["benchmark_unresolved_position_count"] == 1
    assert "approve" not in detail.text.casefold()


def test_low_volatility_forward_progress_is_authenticated_and_locked() -> None:
    app = create_app(_settings(), service=FakeConsoleService())

    with TestClient(app) as client:
        denied = client.get("/api/v1/low-volatility-forward-progress")
        progress = client.get(
            "/api/v1/low-volatility-forward-progress",
            auth=_auth(),
        )
        research = client.get("/research", auth=_auth())

    assert denied.status_code == 401
    assert progress.status_code == 200
    assert progress.json()["completed_required_sessions"] == 1
    assert progress.json()["remaining_required_sessions"] == 125
    assert progress.json()["paper_trading_unlocked"] is False
    assert progress.json()["compatibility_status"] == "not_configured"
    assert progress.json()["execution_timing_compatible"] is False
    assert progress.json()["deployment_blockers"] == ["deployment_gate_unavailable"]
    assert progress.json()["deployment_contract_hash"] is None
    assert progress.json()["deployment_contract_status"] == "not_configured"
    assert progress.json()["ready_for_runtime"] is False
    assert progress.json()["runtime_activation_allowed"] is False
    assert progress.json()["live_trading_locked"] is True
    assert "approve" not in progress.text.casefold()
    assert "low-volatility-forward-sessions-table" in research.text
    assert "low-volatility-forward-compatibility" in research.text
    assert "low-volatility-forward-deployment" in research.text
    assert "low-volatility-forward-signal" in research.text


def test_trading_endpoint_is_explicitly_unavailable() -> None:
    app = create_app(_settings(), service=FakeConsoleService())

    with TestClient(app) as client:
        response = client.get("/api/v1/trading", auth=_auth())

    assert response.status_code == 200
    assert response.json()["status"] == "unavailable"
    assert response.json()["orders"] == []
    assert response.json()["positions"] == []
    assert response.json()["risk"]["live_trading_locked"] is True
    assert response.json()["execution"]["recovery_verified"] is True
    assert response.json()["strategy"]["active"] is False
    assert response.json()["qmt"]["live_trading_locked"] is True
    assert response.json()["qmt"]["status"] == "blocked"
    assert response.json()["qmt_operations"]["live_trading_locked"] is True
    assert response.json()["qmt_operations"]["broker_mutation_allowed"] is False
    assert response.json()["qmt_operations"]["status"] == "idle"
    assert response.json()["promotion"]["live_trading_ready"] is False
    assert "paper_session_count" in response.json()["promotion"]["blockers"]


def test_promotion_endpoint_is_authenticated_and_never_unlocks_live() -> None:
    app = create_app(_settings(), service=FakeConsoleService())

    with TestClient(app) as client:
        denied = client.get("/api/v1/promotion")
        accepted = client.get("/api/v1/promotion", auth=_auth())

    assert denied.status_code == 401
    assert accepted.status_code == 200
    assert accepted.json()["live_trading_ready"] is False
    assert accepted.json()["report_hash"] == "c" * 64


def test_risk_endpoint_is_authenticated_and_read_only() -> None:
    app = create_app(_settings(), service=FakeConsoleService())

    with TestClient(app) as client:
        denied = client.get("/api/v1/risk")
        accepted = client.get("/api/v1/risk", auth=_auth())

    assert denied.status_code == 401
    assert accepted.status_code == 200
    assert accepted.json()["decision_count"] == 7
    assert accepted.json()["live_trading_locked"] is True


def test_execution_endpoint_reports_verified_recovery_without_order_actions() -> None:
    app = create_app(_settings(), service=FakeConsoleService())

    with TestClient(app) as client:
        response = client.get("/api/v1/execution", auth=_auth())

    assert response.status_code == 200
    assert response.json()["recovery_verified"] is True
    assert response.json()["gateway_available"] is False
    assert "submit" not in response.text.casefold()


def test_strategy_endpoint_reports_paper_only_approval_state() -> None:
    app = create_app(_settings(), service=FakeConsoleService())

    with TestClient(app) as client:
        response = client.get("/api/v1/execution/strategy", auth=_auth())

    assert response.status_code == 200
    assert response.json()["active"] is False
    assert response.json()["live_trading_locked"] is True


def test_qmt_endpoint_is_authenticated_read_only_and_redacted() -> None:
    app = create_app(_settings(), service=FakeConsoleService())

    with TestClient(app) as client:
        denied = client.get("/api/v1/qmt")
        accepted = client.get("/api/v1/qmt", auth=_auth())

    assert denied.status_code == 401
    assert accepted.status_code == 200
    assert accepted.json()["status"] == "blocked"
    assert accepted.json()["live_trading_locked"] is True
    assert "account_id" not in accepted.text
    assert "session_id" not in accepted.text
    assert "userdata" not in accepted.text


def test_qmt_operations_endpoint_is_authenticated_read_only_and_redacted() -> None:
    app = create_app(_settings(), service=FakeConsoleService())

    with TestClient(app) as client:
        denied = client.get("/api/v1/qmt/operations")
        accepted = client.get("/api/v1/qmt/operations", auth=_auth())
        mutation = client.post("/api/v1/qmt/operations", auth=_auth(), json={})
        page = client.get("/trading", auth=_auth())

    assert denied.status_code == 401
    assert accepted.status_code == 200
    assert mutation.status_code == 405
    assert accepted.json()["status"] == "idle"
    assert accepted.json()["integrity_verified"] is True
    assert accepted.json()["live_trading_locked"] is True
    assert accepted.json()["broker_mutation_allowed"] is False
    assert accepted.json()["orders"] == []
    assert accepted.json()["trades"] == []
    redacted_payload = accepted.text.casefold()
    for secret_name in ("password", "token", "dsn", "userdata", "broker_account"):
        assert secret_name not in redacted_payload
    assert "qmt-orders-table" in page.text
    assert "qmt-trades-table" in page.text
    assert "下单功能已安全禁用" in page.text


def test_kill_switch_activation_is_authenticated_and_csrf_protected() -> None:
    service = FakeConsoleService()
    app = create_app(_settings(), service=service)
    payload = {"command_id": "web-kill-switch-test-0001", "reason": "manual"}

    with TestClient(app) as client:
        denied = client.post(
            "/api/v1/execution/kill-switch/activate",
            json=payload,
            auth=_auth(),
        )
        page = client.get("/trading", auth=_auth())
        match = re.search(r'name="autoquant-csrf" content="([^"]+)"', page.text)
        assert match is not None
        accepted = client.post(
            "/api/v1/execution/kill-switch/activate",
            json=payload,
            auth=_auth(),
            headers={"X-AutoQuant-CSRF": match.group(1)},
        )

    assert denied.status_code == 403
    assert accepted.status_code == 200
    assert accepted.json()["kill_switch_active"] is True
    assert service.kill_switch_activations == [("web-kill-switch-test-0001", "manual", "operator")]


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


def test_walk_forward_validation_is_csrf_protected_and_parameter_grid_is_bounded() -> None:
    service = FakeConsoleService()
    app = create_app(_settings(), service=service)
    payload = {
        "manifest_hash": "a" * 64,
        "instrument": "000001.XSHE",
        "train_sessions": 60,
        "test_sessions": 20,
        "embargo_sessions": 1,
        "candidates": [
            {"fast_sessions": 5, "slow_sessions": 20},
            {"fast_sessions": 10, "slow_sessions": 30},
        ],
        "idempotency_key": "web-validation-request-0001",
    }

    with TestClient(app) as client:
        page = client.get("/research", auth=_auth())
        match = re.search(r'name="autoquant-csrf" content="([^"]+)"', page.text)
        assert match is not None
        accepted = client.post(
            "/api/v1/validations",
            json=payload,
            auth=_auth(),
            headers={"X-AutoQuant-CSRF": match.group(1)},
        )
        rejected = client.post(
            "/api/v1/validations",
            json={**payload, "candidates": [{"fast_sessions": 20, "slow_sessions": 20}]},
            auth=_auth(),
            headers={"X-AutoQuant-CSRF": match.group(1)},
        )

    assert accepted.status_code == 202
    assert accepted.json()["validator_id"] == "sma_cross_walk_forward_v1"
    assert rejected.status_code == 422
    assert service.created_validations[0].train_sessions == 60


def test_validation_campaigns_are_read_only_and_live_locked() -> None:
    app = create_app(_settings(), service=FakeConsoleService())

    with TestClient(app) as client:
        response = client.get(
            "/api/v1/validation-campaigns?limit=10",
            auth=_auth(),
        )

    assert response.status_code == 200
    campaign = response.json()["items"][0]
    assert campaign["status"] == "queued"
    assert campaign["live_trading_locked"] is True
    assert len(campaign["components"]) == 3


def test_portfolio_validation_is_csrf_protected_and_risk_bounded() -> None:
    service = FakeConsoleService()
    app = create_app(_settings(), service=service)
    payload = {
        "manifest_hash": "a" * 64,
        "idempotency_key": "web-portfolio-validation-0001",
    }

    with TestClient(app) as client:
        denied = client.post(
            "/api/v1/portfolio-validations",
            json=payload,
            auth=_auth(),
        )
        page = client.get("/research", auth=_auth())
        match = re.search(
            r'name="autoquant-csrf" content="([^"]+)"',
            page.text,
        )
        assert match is not None
        accepted = client.post(
            "/api/v1/portfolio-validations",
            json=payload,
            auth=_auth(),
            headers={"X-AutoQuant-CSRF": match.group(1)},
        )
        rejected = client.post(
            "/api/v1/portfolio-validations",
            json={**payload, "gross_allocation": "0.30"},
            auth=_auth(),
            headers={"X-AutoQuant-CSRF": match.group(1)},
        )

    assert denied.status_code == 403
    assert accepted.status_code == 202
    assert accepted.json()["live_trading_locked"] is True
    assert accepted.json()["validator_id"] == "cross_sectional_momentum_walk_forward_v1"
    assert rejected.status_code == 422
    assert service.created_portfolio_validations[0].gross_allocation == Decimal("0.29")
