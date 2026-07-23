from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from autoquant.execution.promotion_audit import (
    PaperPromotionAuditor,
    PaperPromotionFacts,
    PaperPromotionPolicy,
    PaperSessionPromotionEvidence,
    PromotionGate,
    PromotionGateCode,
    SchedulerPromotionEvidence,
)

CAPTURED_AT = datetime(2026, 7, 23, 8, tzinfo=UTC)
FIRST_SESSION = date(2026, 5, 1)


def _sessions(
    *,
    count: int = 60,
    daily_return: Decimal = Decimal("0.002"),
) -> tuple[PaperSessionPromotionEvidence, ...]:
    sessions = []
    equity = Decimal("1000000")
    for offset in range(count):
        session_date = FIRST_SESSION + timedelta(days=offset)
        end_equity = equity * (Decimal("1") + daily_return)
        sessions.append(
            PaperSessionPromotionEvidence(
                session_date=session_date,
                observed_at=datetime.combine(
                    session_date,
                    datetime.min.time(),
                    tzinfo=UTC,
                )
                + timedelta(hours=7),
                day_start_equity=equity,
                end_equity=end_equity,
                state_hash=f"{offset + 1:064x}",
            )
        )
        equity = end_equity
    return tuple(sessions)


def _scheduler(
    sessions: tuple[PaperSessionPromotionEvidence, ...],
    *,
    failure_offset: int | None = None,
) -> tuple[SchedulerPromotionEvidence, ...]:
    return tuple(
        SchedulerPromotionEvidence(
            session_date=session.session_date,
            healthy_minute_count=216,
            failure_count=1 if offset == failure_offset else 0,
            latest_evaluated_at=session.observed_at,
            latest_event_hash=f"{offset + 1001:064x}",
        )
        for offset, session in enumerate(sessions)
    )


def _facts(
    *,
    sessions: tuple[PaperSessionPromotionEvidence, ...] = (),
    scheduler: tuple[SchedulerPromotionEvidence, ...] = (),
) -> PaperPromotionFacts:
    dates = tuple(session.session_date for session in sessions)
    return PaperPromotionFacts(
        account_id="paper-main",
        strategy_id="validated-sma-paper",
        captured_at=CAPTURED_AT,
        kill_switch_active=True,
        control_state_hash="a" * 64,
        active_registration_hash="b" * 64,
        qmt_evidence_hash="c" * 64,
        qmt_observed_at=CAPTURED_AT - timedelta(hours=1),
        sessions=sessions,
        scheduler_sessions=scheduler,
        reconciled_session_dates=dates,
        failed_reconciliation_count=0,
        filled_order_count=30,
        unknown_order_count=0,
        risk_decision_count=30,
        kill_switch_drill_dates=(
            FIRST_SESSION,
            FIRST_SESSION + timedelta(days=1),
            FIRST_SESSION + timedelta(days=2),
        ),
    )


def _gate(
    facts: PaperPromotionFacts,
    code: PromotionGateCode,
) -> PromotionGate:
    report = PaperPromotionAuditor().evaluate(facts)
    return next(gate for gate in report.gates if gate.code is code)


def test_empty_evidence_fails_closed_with_a_deterministic_report() -> None:
    facts = replace(
        _facts(),
        kill_switch_active=False,
        active_registration_hash=None,
        qmt_evidence_hash=None,
        qmt_observed_at=None,
        filled_order_count=0,
        kill_switch_drill_dates=(),
    )

    first = PaperPromotionAuditor().evaluate(facts)
    second = PaperPromotionAuditor().evaluate(facts)

    assert first.report_hash == second.report_hash
    assert first.live_trading_ready is False
    assert first.evidence_gates_passed is False
    assert PromotionGateCode.KILL_SWITCH_ACTIVE in first.blockers
    assert PromotionGateCode.PAPER_SESSION_COUNT in first.blockers
    assert PromotionGateCode.QMT_ACCEPTANCE_FRESH in first.blockers


def test_complete_paper_evidence_still_requires_external_safety_artifacts() -> None:
    sessions = _sessions()
    report = PaperPromotionAuditor().evaluate(
        _facts(sessions=sessions, scheduler=_scheduler(sessions))
    )

    expected_blockers = {
        PromotionGateCode.WINDOWS_RECOVERY_DRILLS,
        PromotionGateCode.COMPLIANCE_APPROVAL,
    }
    assert set(report.blockers) == expected_blockers
    assert report.live_trading_ready is False
    assert report.evidence_gates_passed is False


def test_qmt_time_and_scheduler_failures_are_fail_closed() -> None:
    sessions = _sessions()
    scheduler = _scheduler(sessions, failure_offset=10)
    facts = replace(
        _facts(sessions=sessions, scheduler=scheduler),
        qmt_observed_at=CAPTURED_AT + timedelta(seconds=1),
    )

    assert not _gate(
        facts,
        PromotionGateCode.QMT_ACCEPTANCE_FRESH,
    ).passed
    assert not _gate(
        facts,
        PromotionGateCode.SCHEDULER_FAILURE_FREE,
    ).passed


def test_performance_gates_use_compounded_session_returns() -> None:
    sessions = _sessions(daily_return=Decimal("-0.002"))
    facts = _facts(sessions=sessions, scheduler=_scheduler(sessions))

    assert not _gate(facts, PromotionGateCode.PAPER_TOTAL_RETURN).passed
    assert not _gate(
        facts,
        PromotionGateCode.PROFITABLE_SESSION_RATE,
    ).passed
    assert not _gate(facts, PromotionGateCode.PAPER_MAX_DRAWDOWN).passed


def test_fact_hash_is_independent_of_evidence_input_order() -> None:
    sessions = _sessions()
    scheduler = _scheduler(sessions)

    ordered = _facts(sessions=sessions, scheduler=scheduler)
    reversed_facts = replace(
        ordered,
        sessions=tuple(reversed(sessions)),
        scheduler_sessions=tuple(reversed(scheduler)),
        reconciled_session_dates=tuple(
            reversed(ordered.reconciled_session_dates)
        ),
        kill_switch_drill_dates=tuple(
            reversed(ordered.kill_switch_drill_dates)
        ),
    )

    assert ordered.fact_hash == reversed_facts.fact_hash
    assert (
        PaperPromotionAuditor().evaluate(ordered).report_hash
        == PaperPromotionAuditor().evaluate(reversed_facts).report_hash
    )


@pytest.mark.parametrize(
    "factory",
    (
        lambda: PaperPromotionPolicy(minimum_paper_sessions=0),
        lambda: PaperPromotionPolicy(
            minimum_healthy_minutes_per_session=241
        ),
        lambda: PaperPromotionPolicy(
            minimum_profitable_session_rate=Decimal("0")
        ),
        lambda: PaperPromotionPolicy(maximum_drawdown=Decimal("1")),
        lambda: PaperPromotionPolicy(
            maximum_qmt_acceptance_age=timedelta(0)
        ),
    ),
)
def test_policy_rejects_unsafe_thresholds(
    factory: Callable[[], PaperPromotionPolicy],
) -> None:
    with pytest.raises(ValueError):
        factory()
