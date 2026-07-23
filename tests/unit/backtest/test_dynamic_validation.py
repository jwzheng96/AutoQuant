from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from autoquant.backtest.dynamic_panel import (
    DynamicMarketPanel,
    DynamicMarketSession,
    InstrumentMarketHistory,
)
from autoquant.backtest.dynamic_portfolio import (
    DynamicPortfolioResearchSpec,
)
from autoquant.backtest.dynamic_validation import (
    DynamicWalkForwardValidator,
    assess_dynamic_validation,
)
from autoquant.backtest.models import MarketState
from autoquant.backtest.portfolio_validation import (
    CrossSectionalMomentumParameters,
)
from autoquant.backtest.rules import AshareRuleBook, SecurityStatus
from autoquant.data.daily_models import DailyBarRevision
from autoquant.errors import PersistenceUnavailableError
from autoquant.web.dynamic_validation_store import (
    DynamicValidationRecord,
    _fold_parameters,
    _record,
    _run_parameters,
)

INSTRUMENTS = ("000001.XSHE", "600000.XSHG", "600519.XSHG")
PARAMETERS = CrossSectionalMomentumParameters(20, 5, 2)
AS_OF = datetime(2027, 1, 1, tzinfo=UTC)


def _market(instrument: str, index: int) -> MarketState:
    session_date = date(2025, 1, 1) + timedelta(days=index)
    rank = Decimal(INSTRUMENTS.index(instrument) + 1)
    previous = (
        Decimal("10")
        + rank * Decimal(max(index - 1, 0)) / Decimal("200")
        + Decimal(max(index - 1, 0) % 7) / Decimal("100")
    )
    close = (
        Decimal("10")
        + rank * Decimal(index) / Decimal("200")
        + Decimal(index % 7) / Decimal("100")
    )
    event = datetime.combine(
        session_date,
        datetime.min.time(),
        tzinfo=UTC,
    ) + timedelta(hours=7)
    return MarketState(
        bar=DailyBarRevision.from_values(
            source="tushare",
            instrument=instrument,
            session_date=session_date,
            event_time=event,
            available_at=event + timedelta(hours=1),
            ingested_at=event + timedelta(hours=2),
            source_revision="dynamic-validation-test",
            availability_policy="test-v1",
            evidence_hash="d" * 64,
            open_price=str(previous),
            high_price=str(max(previous, close) + Decimal("0.5")),
            low_price=str(min(previous, close) - Decimal("0.5")),
            close_price=str(close),
            pre_close=str(previous),
            volume=100_000_000,
            turnover="1000000000",
        ),
        rules=AshareRuleBook().resolve(
            instrument,
            session_date,
            SecurityStatus(
                risk_warning=False,
                listing_session_number=1000,
            ),
        ),
        suspended=False,
    )


def _spec(count: int) -> DynamicPortfolioResearchSpec:
    return DynamicPortfolioResearchSpec(
        dataset_manifest_hash="a" * 64,
        plan_hash="b" * 64,
        policy_hash="c" * 64,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1) + timedelta(days=count - 1),
        gross_allocation=Decimal("0.10"),
        maximum_position_weight=Decimal("0.05"),
        train_sessions=252,
        test_sessions=20,
        embargo_sessions=1,
        minimum_member_history_sessions=20,
        candidates=(PARAMETERS,),
    )


def _panel(count: int, spec: DynamicPortfolioResearchSpec) -> DynamicMarketPanel:
    histories = tuple(
        InstrumentMarketHistory(
            instrument=instrument,
            markets=tuple(
                _market(instrument, index) for index in range(count)
            ),
            list_date=date(2020, 1, 1),
            delist_date=None,
        )
        for instrument in INSTRUMENTS
    )
    by_instrument = {
        value.instrument: value.markets for value in histories
    }
    sessions = tuple(
        DynamicMarketSession(
            session_date=date(2025, 1, 1) + timedelta(days=index),
            snapshot_hash=f"{index + 1:064x}",
            active_members=INSTRUMENTS,
            markets=tuple(
                by_instrument[instrument][index]
                for instrument in INSTRUMENTS
            ),
        )
        for index in range(count)
    )
    return DynamicMarketPanel(
        dataset_manifest_hash=spec.dataset_manifest_hash,
        plan_hash=spec.plan_hash,
        spec_hash=spec.spec_hash,
        as_of=AS_OF,
        sessions=sessions,
        histories=histories,
    )


def test_dynamic_walk_forward_is_deterministic_and_disjoint() -> None:
    count = 315
    spec = _spec(count)
    panel = _panel(count, spec)
    validator = DynamicWalkForwardValidator()

    first = validator.run(panel=panel, spec=spec)
    second = validator.run(panel=panel, spec=spec)
    evidence = assess_dynamic_validation(
        first,
        policy=spec.evidence_policy,
    )
    repeated = assess_dynamic_validation(
        second,
        policy=spec.evidence_policy,
    )

    assert first.result_hash == second.result_hash
    assert first.panel_hash == panel.panel_hash
    assert len(first.folds) == 3
    assert all(
        left.test_end < right.test_start
        for left, right in zip(
            first.folds,
            first.folds[1:],
            strict=False,
        )
    )
    assert first.rejected_order_count == 0
    assert evidence.assessment_hash == repeated.assessment_hash
    assert evidence.fold_count == 3
    assert evidence.oos_sessions == 60
    assert "minimum_fold_count" in evidence.gate_failures
    assert "minimum_oos_sessions" in evidence.gate_failures


def test_dynamic_validation_rejects_aggregate_metric_tampering() -> None:
    count = 273
    spec = _spec(count)
    result = DynamicWalkForwardValidator().run(
        panel=_panel(count, spec),
        spec=spec,
    )

    with pytest.raises(ValueError, match="aggregate metrics"):
        replace(
            result,
            rejected_order_count=result.rejected_order_count + 1,
        )


def test_dynamic_validation_store_payload_round_trips_and_detects_tampering() -> None:
    count = 273
    spec = _spec(count)
    result = DynamicWalkForwardValidator().run(
        panel=_panel(count, spec),
        spec=spec,
    )
    evidence = assess_dynamic_validation(
        result,
        policy=spec.evidence_policy,
    )
    expected = DynamicValidationRecord(
        result=result,
        evidence=evidence,
        requested_by="operator",
        completed_at=AS_OF,
    )
    run = _run_parameters(expected)
    run["live_trading_locked"] = True
    folds = tuple(
        _fold_parameters(result.result_hash, fold)
        for fold in result.folds
    )

    assert _record(
        cast(Any, run),
        cast(Any, folds),
    ) == expected

    evaluations = json.loads(
        str(folds[0]["candidate_evaluations"])
    )
    evaluations[0]["evaluation_hash"] = "0" * 64
    folds[0]["candidate_evaluations"] = json.dumps(evaluations)
    with pytest.raises(
        PersistenceUnavailableError,
        match="integrity",
    ):
        _record(cast(Any, run), cast(Any, folds))


def test_dynamic_validation_migration_is_additive_and_immutable() -> None:
    sql = Path(
        "migrations/postgres/026_dynamic_validation_evidence.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS dynamic_validation_runs" in sql
    assert "CREATE TABLE IF NOT EXISTS dynamic_validation_folds" in sql
    assert sql.count("autoquant_reject_immutable_change()") == 2
    assert "live_trading_locked" in sql
    assert "VALUES ('postgres', 26)" in sql
