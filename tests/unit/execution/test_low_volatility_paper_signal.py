from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from autoquant.backtest.low_volatility_portfolio import (
    LowVolatilityResearchSpec,
)
from autoquant.backtest.models import MarketState
from autoquant.backtest.rules import (
    AshareRuleBook,
    SecurityStatus,
)
from autoquant.data.daily_ingestion import ValidatedDailyDataset
from autoquant.data.daily_models import (
    DailyBarRevision,
    DailyCoverageEvidence,
    TradingSession,
)
from autoquant.data.models import DatasetManifest
from autoquant.data.research_data_campaign import (
    ResearchDatasetManifest,
    ResearchDatasetShard,
)
from autoquant.data.research_input import ValidatedResearchShard
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.low_volatility_paper_approval import (
    LowVolatilityPaperCandidateApproval,
)
from autoquant.execution.low_volatility_paper_signal import (
    LOW_VOLATILITY_PAPER_DAILY_SIGNAL_VERSION,
    LOW_VOLATILITY_PAPER_SIGNAL_GENESIS_HASH,
    LowVolatilityPaperDailySignal,
    LowVolatilityPaperValuation,
)
from autoquant.execution.low_volatility_paper_signal_compiler import (
    LowVolatilityPaperSignalCompiler,
)
from autoquant.execution.low_volatility_paper_signal_store import (
    _signal,
)
from autoquant.execution.paper_policy import default_paper_policy
from autoquant.execution.session_rules import SessionRuleSet

SESSION_DATE = date(2026, 7, 27)
PREPARED_AT = datetime(2026, 7, 27, 1, tzinfo=UTC)
INSTRUMENTS = tuple(f"{index:06d}.XSHE" for index in range(1, 61))


def _spec() -> LowVolatilityResearchSpec:
    return LowVolatilityResearchSpec(
        predecessor_result_hash="a" * 64,
        dataset_manifest_hash="b" * 64,
        plan_hash="c" * 64,
        policy_hash="d" * 64,
        start_date=date(2020, 1, 1),
        end_date=date(2026, 7, 22),
    )


def _candidate() -> LowVolatilityPaperCandidateApproval:
    policy = default_paper_policy(INSTRUMENTS)
    return LowVolatilityPaperCandidateApproval(
        account_id="paper-main",
        strategy_id="low-volatility-paper",
        forward_spec_hash="e" * 64,
        evaluation_result_hash="f" * 64,
        evaluation_assessment_hash="1" * 64,
        evaluation_dataset_manifest_hash="2" * 64,
        source_spec_hash=_spec().spec_hash,
        risk_policy_hash=policy.policy_hash,
        instruments=INSTRUMENTS,
        approved_by="risk-operator",
        approved_at=datetime(2026, 7, 25, tzinfo=UTC),
    )


def _valuation(
    instrument: str,
) -> LowVolatilityPaperValuation:
    return LowVolatilityPaperValuation(
        instrument=instrument,
        price_date=SESSION_DATE - timedelta(days=1),
        adjusted_close=Decimal("10"),
        prior_volume=1_000_000,
        adjusted_bar_hash="3" * 64,
    )


def _signal_value() -> LowVolatilityPaperDailySignal:
    return LowVolatilityPaperDailySignal(
        candidate_approval_hash=_candidate().approval_hash,
        account_id="paper-main",
        strategy_id="low-volatility-paper",
        source_spec_hash=_spec().spec_hash,
        risk_policy_hash=_candidate().risk_policy_hash,
        session_sequence=1,
        session_date=SESSION_DATE,
        signal_date=SESSION_DATE - timedelta(days=1),
        window_start_date=SESSION_DATE - timedelta(days=253),
        previous_signal_hash=(LOW_VOLATILITY_PAPER_SIGNAL_GENESIS_HASH),
        snapshot_hash="4" * 64,
        dataset_manifest_hash="5" * 64,
        rule_set_hash="6" * 64,
        universe_members=INSTRUMENTS,
        evidence_instruments=INSTRUMENTS,
        observations=(),
        valuations=tuple(_valuation(value) for value in INSTRUMENTS),
        selected_instruments=(),
        rebalance_due=True,
        prepared_by="paper-operator",
        prepared_at=PREPARED_AT,
    )


def test_daily_signal_round_trips_and_cannot_activate_runtime() -> None:
    signal = _signal_value()

    restored = LowVolatilityPaperDailySignal.from_payload(signal.payload())

    assert restored == signal
    assert signal.version == (LOW_VOLATILITY_PAPER_DAILY_SIGNAL_VERSION)
    assert signal.execution_timing_compatible is False
    assert signal.runtime_activation_allowed is False
    assert signal.live_trading_locked is True


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("execution_timing_compatible", True),
        ("runtime_activation_allowed", True),
        ("live_trading_locked", False),
        ("rebalance_due", False),
        ("previous_signal_hash", "7" * 64),
    ),
)
def test_daily_signal_rejects_governance_or_chain_weakening(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match="daily signal"):
        replace(_signal_value(), **{field: value})


def test_daily_signal_rejects_after_open_preparation() -> None:
    with pytest.raises(ValueError, match="daily signal"):
        replace(
            _signal_value(),
            prepared_at=datetime(
                2026,
                7,
                27,
                2,
                tzinfo=UTC,
            ),
        )


def test_daily_signal_store_row_verifies_hard_locks() -> None:
    signal = _signal_value()
    row: dict[str, object] = {
        **signal.payload(),
        "evidence_instrument_count": len(signal.evidence_instruments),
        "observation_count": len(signal.observations),
        "payload": signal.payload(),
        "prepared_at": signal.prepared_at,
        "selected_count": len(signal.selected_instruments),
        "session_date": signal.session_date,
        "signal_hash": signal.signal_hash,
        "signal_date": signal.signal_date,
        "signal_version": signal.version,
        "universe_member_count": len(signal.universe_members),
        "window_start_date": signal.window_start_date,
    }

    assert _signal(cast(Any, row)) == signal

    row["execution_timing_compatible"] = True
    with pytest.raises(PersistenceUnavailableError, match="integrity"):
        _signal(cast(Any, row))


class _ShardReader:
    def __init__(
        self,
        shards: tuple[ValidatedResearchShard, ...],
    ) -> None:
        self._shards = shards

    async def iter_all(self) -> Any:
        for value in self._shards:
            yield value


class _MarketCompiler:
    def __init__(
        self,
        dates: tuple[date, ...],
    ) -> None:
        self._dates = dates

    def compile(
        self,
        instrument: str,
        dataset: ValidatedDailyDataset,
    ) -> tuple[MarketState, ...]:
        del dataset
        offset = Decimal(int(instrument[:6])) / Decimal("1000")
        rules = AshareRuleBook().resolve(
            instrument,
            self._dates[0],
            SecurityStatus(
                risk_warning=False,
                listing_session_number=1000,
            ),
        )
        return tuple(
            MarketState(
                bar=DailyBarRevision.from_values(
                    source="tushare",
                    instrument=instrument,
                    session_date=session,
                    event_time=datetime(
                        session.year,
                        session.month,
                        session.day,
                        7,
                        tzinfo=UTC,
                    ),
                    available_at=datetime(
                        session.year,
                        session.month,
                        session.day,
                        8,
                        tzinfo=UTC,
                    ),
                    ingested_at=datetime(
                        session.year,
                        session.month,
                        session.day,
                        8,
                        1,
                        tzinfo=UTC,
                    ),
                    source_revision="paper-signal-test",
                    availability_policy="test-v1",
                    evidence_hash="8" * 64,
                    open_price=Decimal("10") + offset + Decimal(index) / Decimal("100"),
                    high_price=Decimal("11") + offset + Decimal(index) / Decimal("100"),
                    low_price=Decimal("9") + offset + Decimal(index) / Decimal("100"),
                    close_price=Decimal("10") + offset + Decimal(index) / Decimal("100"),
                    pre_close=Decimal("10") + offset + Decimal(max(index - 1, 0)) / Decimal("100"),
                    volume=1_000_000,
                    turnover=Decimal("10000000"),
                ),
                rules=replace(
                    rules,
                    effective_from=session,
                ),
                suspended=False,
            )
            for index, session in enumerate(self._dates)
        )


def _compiler_inputs() -> tuple[
    ResearchDatasetManifest,
    tuple[ValidatedResearchShard, ...],
    SessionRuleSet,
    tuple[date, ...],
]:
    first = date(2025, 11, 15)
    dates = tuple(first + timedelta(days=index) for index in range(253))
    sessions = tuple(
        TradingSession(
            source="tushare",
            session_date=value,
            is_open=True,
            available_at=PREPARED_AT - timedelta(days=300),
            response_hash="9" * 64,
        )
        for value in dates
    )
    shards: list[ValidatedResearchShard] = []
    manifest_shards: list[ResearchDatasetShard] = []
    for sequence, instrument in enumerate(
        INSTRUMENTS,
        start=1,
    ):
        manifest = DatasetManifest(
            source="tushare",
            instruments=(instrument,),
            start_time=datetime(
                dates[0].year,
                dates[0].month,
                dates[0].day,
                tzinfo=UTC,
            ),
            end_time=datetime(
                dates[-1].year,
                dates[-1].month,
                dates[-1].day,
                tzinfo=UTC,
            ),
            as_of=PREPARED_AT,
            record_hashes=(),
            quality_report_hash="quality",
            production_complete=True,
            row_count=0,
        )
        manifest_shards.append(
            ResearchDatasetShard(
                sequence=sequence,
                instrument=instrument,
                manifest_hash=manifest.manifest_hash,
            )
        )
        shards.append(
            ValidatedResearchShard(
                instrument=instrument,
                manifest=manifest,
                dataset=ValidatedDailyDataset(
                    bars=(),
                    factors=(),
                    coverage=DailyCoverageEvidence(
                        sessions=sessions,
                        lifecycles=(),
                        suspensions=(),
                    ),
                ),
            )
        )
    aggregate = ResearchDatasetManifest(
        campaign_hash="a" * 64,
        policy_hash=_spec().policy_hash,
        snapshot_hashes=("4" * 64,),
        start_date=dates[0],
        end_date=dates[-1],
        shards=tuple(manifest_shards),
    )
    rules = tuple(
        AshareRuleBook().resolve(
            instrument,
            SESSION_DATE,
            SecurityStatus(
                risk_warning=False,
                listing_session_number=1000,
            ),
        )
        for instrument in INSTRUMENTS
    )
    rule_set = SessionRuleSet(
        session_date=SESSION_DATE,
        as_of=PREPARED_AT,
        rules=rules,
        suspended_instruments=(),
        source_evidence_hashes=("b" * 64,),
    )
    return aggregate, tuple(shards), rule_set, dates


@pytest.mark.asyncio
async def test_compiler_uses_exactly_253_prior_sessions_only() -> None:
    manifest, shards, rule_set, dates = _compiler_inputs()

    signal = await LowVolatilityPaperSignalCompiler(
        reader=_ShardReader(shards),
        market_compiler=_MarketCompiler(dates),
    ).compile(
        candidate=_candidate(),
        spec=_spec(),
        manifest=manifest,
        snapshot_hash="4" * 64,
        universe_members=INSTRUMENTS,
        rule_set=rule_set,
        session_date=SESSION_DATE,
        previous=None,
        prepared_by="paper-operator",
        prepared_at=PREPARED_AT,
    )

    assert signal.window_start_date == dates[0]
    assert signal.signal_date == dates[-1]
    assert signal.session_date == SESSION_DATE
    assert len(signal.observations) == 60
    assert len(signal.selected_instruments) == 20
    assert len(signal.valuations) == 60
    assert all(
        value.execution_date == SESSION_DATE and value.signal_date < value.execution_date
        for value in signal.observations
    )
    assert signal.execution_timing_compatible is False
    assert signal.runtime_activation_allowed is False


def test_daily_signal_migration_is_immutable_and_runtime_locked() -> None:
    sql = Path("migrations/postgres/047_low_volatility_paper_daily_signals.sql").read_text(
        encoding="utf-8"
    )

    assert ("CREATE TABLE IF NOT EXISTS low_volatility_paper_daily_signals") in sql
    assert "NOT execution_timing_compatible" in sql
    assert "NOT runtime_activation_allowed" in sql
    assert "mod(session_sequence - 1, 21) = 0" in sql
    assert "autoquant_validate_low_volatility_paper_daily_signal" in sql
    assert "prepared_at <" in sql
    assert sql.count("autoquant_reject_immutable_change()") == 1
    assert "VALUES ('postgres', 47)" in sql
