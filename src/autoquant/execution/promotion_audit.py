from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import StrEnum
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _datetime_text,
    _decimal_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.errors import PersistenceUnavailableError

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CLOSE_EVIDENCE_TIME = time(14, 55)


class PromotionGateCode(StrEnum):
    LIVE_RELEASE_LOCK = "live_release_lock"
    KILL_SWITCH_ACTIVE = "kill_switch_active"
    STRATEGY_APPROVED = "strategy_approved"
    QMT_ACCEPTANCE_FRESH = "qmt_acceptance_fresh"
    PAPER_SESSION_COUNT = "paper_session_count"
    SCHEDULER_COVERAGE = "scheduler_coverage"
    SCHEDULER_FAILURE_FREE = "scheduler_failure_free"
    RECONCILIATION_COVERAGE = "reconciliation_coverage"
    FILLED_ORDER_COUNT = "filled_order_count"
    UNKNOWN_ORDER_FREE = "unknown_order_free"
    PAPER_TOTAL_RETURN = "paper_total_return"
    PAPER_MAX_DRAWDOWN = "paper_max_drawdown"
    PROFITABLE_SESSION_RATE = "profitable_session_rate"
    KILL_SWITCH_DRILLS = "kill_switch_drills"
    WINDOWS_RECOVERY_DRILLS = "windows_recovery_drills"
    COMPLIANCE_APPROVAL = "compliance_approval"


@dataclass(frozen=True, slots=True)
class PaperPromotionPolicy:
    minimum_paper_sessions: int = 60
    minimum_healthy_minutes_per_session: int = 216
    minimum_filled_orders: int = 30
    minimum_profitable_session_rate: Decimal = Decimal("0.50")
    maximum_drawdown: Decimal = Decimal("0.10")
    minimum_kill_switch_drills: int = 3
    maximum_qmt_acceptance_age: timedelta = timedelta(hours=24)
    evidence_lookback_days: int = 180
    policy_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for count_name, count_value in (
            ("minimum_paper_sessions", self.minimum_paper_sessions),
            (
                "minimum_healthy_minutes_per_session",
                self.minimum_healthy_minutes_per_session,
            ),
            ("minimum_filled_orders", self.minimum_filled_orders),
            (
                "minimum_kill_switch_drills",
                self.minimum_kill_switch_drills,
            ),
            ("evidence_lookback_days", self.evidence_lookback_days),
        ):
            if (
                not isinstance(count_value, int)
                or isinstance(count_value, bool)
                or count_value < 1
            ):
                raise ValueError(f"{count_name} must be positive")
        if self.minimum_healthy_minutes_per_session > 240:
            raise ValueError(
                "minimum healthy minutes cannot exceed the A-share session"
            )
        if (
            not Decimal("0") < self.minimum_profitable_session_rate <= 1
        ):
            raise ValueError(
                "minimum profitable session rate must be in (0, 1]"
            )
        if not Decimal("0") < self.maximum_drawdown < 1:
            raise ValueError("maximum drawdown must be in (0, 1)")
        if self.maximum_qmt_acceptance_age <= timedelta(0):
            raise ValueError(
                "maximum QMT acceptance age must be positive"
            )
        object.__setattr__(
            self,
            "policy_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "evidence_lookback_days": self.evidence_lookback_days,
            "maximum_drawdown": _decimal_text(self.maximum_drawdown),
            "maximum_qmt_acceptance_age_seconds": int(
                self.maximum_qmt_acceptance_age.total_seconds()
            ),
            "minimum_filled_orders": self.minimum_filled_orders,
            "minimum_healthy_minutes_per_session": (
                self.minimum_healthy_minutes_per_session
            ),
            "minimum_kill_switch_drills": (
                self.minimum_kill_switch_drills
            ),
            "minimum_paper_sessions": self.minimum_paper_sessions,
            "minimum_profitable_session_rate": _decimal_text(
                self.minimum_profitable_session_rate
            ),
            "version": "paper-live-promotion-policy-v1",
        }


@dataclass(frozen=True, slots=True)
class PaperSessionPromotionEvidence:
    session_date: date
    observed_at: datetime
    day_start_equity: Decimal
    end_equity: Decimal
    state_hash: str

    def __post_init__(self) -> None:
        observed_at = to_utc(
            self.observed_at,
            name="paper session promotion observed_at",
        )
        if observed_at.astimezone(_SHANGHAI).date() != self.session_date:
            raise ValueError(
                "paper promotion session observation belongs to another date"
            )
        for name, value in (
            ("day_start_equity", self.day_start_equity),
            ("end_equity", self.end_equity),
        ):
            if (
                not isinstance(value, Decimal)
                or not value.is_finite()
                or value <= 0
            ):
                raise ValueError(f"{name} must be positive and finite")
        _require_lowercase_sha256(self.state_hash, name="state_hash")
        object.__setattr__(self, "observed_at", observed_at)

    @property
    def closed(self) -> bool:
        return (
            self.observed_at.astimezone(_SHANGHAI).time()
            >= _CLOSE_EVIDENCE_TIME
        )

    @property
    def session_return(self) -> Decimal:
        return self.end_equity / self.day_start_equity - Decimal("1")

    def payload(self) -> dict[str, object]:
        return {
            "day_start_equity": _decimal_text(self.day_start_equity),
            "end_equity": _decimal_text(self.end_equity),
            "observed_at": _datetime_text(self.observed_at),
            "session_date": self.session_date.isoformat(),
            "state_hash": self.state_hash,
        }


@dataclass(frozen=True, slots=True)
class SchedulerPromotionEvidence:
    session_date: date
    healthy_minute_count: int
    failure_count: int
    latest_evaluated_at: datetime
    latest_event_hash: str

    def __post_init__(self) -> None:
        for count_name, count_value in (
            ("healthy_minute_count", self.healthy_minute_count),
            ("failure_count", self.failure_count),
        ):
            if (
                not isinstance(count_value, int)
                or isinstance(count_value, bool)
                or count_value < 0
            ):
                raise ValueError(f"{count_name} must be nonnegative")
        latest = to_utc(
            self.latest_evaluated_at,
            name="scheduler promotion time",
        )
        if latest.astimezone(_SHANGHAI).date() != self.session_date:
            raise ValueError(
                "scheduler promotion evidence belongs to another date"
            )
        _require_lowercase_sha256(
            self.latest_event_hash,
            name="latest_event_hash",
        )
        object.__setattr__(self, "latest_evaluated_at", latest)

    def payload(self) -> dict[str, object]:
        return {
            "failure_count": self.failure_count,
            "healthy_minute_count": self.healthy_minute_count,
            "latest_evaluated_at": _datetime_text(
                self.latest_evaluated_at
            ),
            "latest_event_hash": self.latest_event_hash,
            "session_date": self.session_date.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class PaperPromotionFacts:
    account_id: str
    strategy_id: str
    captured_at: datetime
    kill_switch_active: bool
    control_state_hash: str
    active_registration_hash: str | None
    qmt_evidence_hash: str | None
    qmt_observed_at: datetime | None
    sessions: tuple[PaperSessionPromotionEvidence, ...]
    scheduler_sessions: tuple[SchedulerPromotionEvidence, ...]
    reconciled_session_dates: tuple[date, ...]
    failed_reconciliation_count: int
    filled_order_count: int
    unknown_order_count: int
    risk_decision_count: int
    kill_switch_drill_dates: tuple[date, ...]
    fact_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(self.account_id, name="account_id")
        _require_nonblank(self.strategy_id, name="strategy_id")
        object.__setattr__(
            self,
            "captured_at",
            to_utc(self.captured_at, name="promotion facts captured_at"),
        )
        if type(self.kill_switch_active) is not bool:
            raise TypeError("kill_switch_active must be bool")
        _require_lowercase_sha256(
            self.control_state_hash,
            name="control_state_hash",
        )
        for name, value in (
            (
                "active_registration_hash",
                self.active_registration_hash,
            ),
            ("qmt_evidence_hash", self.qmt_evidence_hash),
        ):
            if value is not None:
                _require_lowercase_sha256(value, name=name)
        qmt_time = (
            None
            if self.qmt_observed_at is None
            else to_utc(
                self.qmt_observed_at,
                name="QMT promotion evidence time",
            )
        )
        if (self.qmt_evidence_hash is None) != (qmt_time is None):
            raise ValueError("QMT promotion evidence is incomplete")
        object.__setattr__(self, "qmt_observed_at", qmt_time)
        sessions = tuple(sorted(self.sessions, key=lambda item: item.session_date))
        scheduler = tuple(
            sorted(
                self.scheduler_sessions,
                key=lambda item: item.session_date,
            )
        )
        reconciled = tuple(sorted(set(self.reconciled_session_dates)))
        drills = tuple(sorted(set(self.kill_switch_drill_dates)))
        if len({item.session_date for item in sessions}) != len(sessions):
            raise ValueError("paper promotion sessions must be unique")
        if len({item.session_date for item in scheduler}) != len(scheduler):
            raise ValueError("scheduler promotion sessions must be unique")
        object.__setattr__(self, "sessions", sessions)
        object.__setattr__(self, "scheduler_sessions", scheduler)
        object.__setattr__(self, "reconciled_session_dates", reconciled)
        object.__setattr__(self, "kill_switch_drill_dates", drills)
        for count_name, count_value in (
            (
                "failed_reconciliation_count",
                self.failed_reconciliation_count,
            ),
            ("filled_order_count", self.filled_order_count),
            ("unknown_order_count", self.unknown_order_count),
            ("risk_decision_count", self.risk_decision_count),
        ):
            if (
                not isinstance(count_value, int)
                or isinstance(count_value, bool)
                or count_value < 0
            ):
                raise ValueError(f"{count_name} must be nonnegative")
        object.__setattr__(
            self,
            "fact_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "account_id": self.account_id,
            "active_registration_hash": self.active_registration_hash,
            "captured_at": _datetime_text(self.captured_at),
            "control_state_hash": self.control_state_hash,
            "failed_reconciliation_count": (
                self.failed_reconciliation_count
            ),
            "filled_order_count": self.filled_order_count,
            "kill_switch_active": self.kill_switch_active,
            "kill_switch_drill_dates": [
                value.isoformat() for value in self.kill_switch_drill_dates
            ],
            "qmt_evidence_hash": self.qmt_evidence_hash,
            "qmt_observed_at": (
                None
                if self.qmt_observed_at is None
                else _datetime_text(self.qmt_observed_at)
            ),
            "reconciled_session_dates": [
                value.isoformat()
                for value in self.reconciled_session_dates
            ],
            "risk_decision_count": self.risk_decision_count,
            "scheduler_sessions": [
                item.payload() for item in self.scheduler_sessions
            ],
            "sessions": [item.payload() for item in self.sessions],
            "strategy_id": self.strategy_id,
            "unknown_order_count": self.unknown_order_count,
            "version": "paper-promotion-facts-v1",
        }


@dataclass(frozen=True, slots=True)
class PromotionGate:
    code: PromotionGateCode
    passed: bool
    actual: str
    required: str

    def __post_init__(self) -> None:
        if not isinstance(self.code, PromotionGateCode):
            raise TypeError("promotion gate code is invalid")
        if type(self.passed) is not bool:
            raise TypeError("promotion gate passed must be bool")
        _require_nonblank(self.actual, name="promotion gate actual")
        _require_nonblank(self.required, name="promotion gate required")

    def payload(self) -> dict[str, object]:
        return {
            "actual": self.actual,
            "code": self.code.value,
            "passed": self.passed,
            "required": self.required,
        }


@dataclass(frozen=True, slots=True)
class PaperPromotionAudit:
    evaluated_at: datetime
    policy_hash: str
    fact_hash: str
    gates: tuple[PromotionGate, ...]
    report_hash: str = field(init=False)
    live_trading_ready: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "evaluated_at",
            to_utc(self.evaluated_at, name="promotion audit time"),
        )
        _require_lowercase_sha256(self.policy_hash, name="policy_hash")
        _require_lowercase_sha256(self.fact_hash, name="fact_hash")
        gates = tuple(sorted(self.gates, key=lambda item: item.code.value))
        if len({item.code for item in gates}) != len(gates):
            raise ValueError("promotion audit gates must be unique")
        if set(item.code for item in gates) != set(PromotionGateCode):
            raise ValueError("promotion audit gate set is incomplete")
        object.__setattr__(self, "gates", gates)
        object.__setattr__(
            self,
            "report_hash",
            _canonical_hash(self.payload()),
        )

    @property
    def evidence_gates_passed(self) -> bool:
        return all(gate.passed for gate in self.gates)

    @property
    def blockers(self) -> tuple[PromotionGateCode, ...]:
        return tuple(gate.code for gate in self.gates if not gate.passed)

    def payload(self) -> dict[str, object]:
        return {
            "evaluated_at": _datetime_text(self.evaluated_at),
            "fact_hash": self.fact_hash,
            "gates": [gate.payload() for gate in self.gates],
            "live_trading_ready": False,
            "policy_hash": self.policy_hash,
            "version": "paper-live-promotion-audit-v1",
        }


class PaperPromotionAuditor:
    def __init__(
        self,
        *,
        policy: PaperPromotionPolicy | None = None,
    ) -> None:
        self._policy = policy or PaperPromotionPolicy()

    @property
    def policy(self) -> PaperPromotionPolicy:
        return self._policy

    def evaluate(self, facts: PaperPromotionFacts) -> PaperPromotionAudit:
        policy = self._policy
        closed = tuple(item for item in facts.sessions if item.closed)
        window = closed[-policy.minimum_paper_sessions :]
        window_dates = {item.session_date for item in window}
        scheduler = {
            item.session_date: item for item in facts.scheduler_sessions
        }
        scheduler_covered = sum(
            1
            for session_date in window_dates
            if session_date in scheduler
            and scheduler[session_date].healthy_minute_count
            >= policy.minimum_healthy_minutes_per_session
        )
        scheduler_failures = sum(
            scheduler[session_date].failure_count
            for session_date in window_dates
            if session_date in scheduler
        )
        reconciled = len(
            window_dates.intersection(facts.reconciled_session_dates)
        )
        total_return, max_drawdown, profitable_rate = _performance(window)
        qmt_age = (
            None
            if facts.qmt_observed_at is None
            else facts.captured_at - facts.qmt_observed_at
        )
        qmt_fresh = (
            qmt_age is not None
            and timedelta(0) <= qmt_age <= policy.maximum_qmt_acceptance_age
        )
        gates = (
            PromotionGate(
                PromotionGateCode.LIVE_RELEASE_LOCK,
                True,
                "locked",
                "locked",
            ),
            PromotionGate(
                PromotionGateCode.KILL_SWITCH_ACTIVE,
                facts.kill_switch_active,
                "active" if facts.kill_switch_active else "inactive",
                "active",
            ),
            PromotionGate(
                PromotionGateCode.STRATEGY_APPROVED,
                facts.active_registration_hash is not None,
                (
                    "approved"
                    if facts.active_registration_hash is not None
                    else "inactive"
                ),
                "approved",
            ),
            PromotionGate(
                PromotionGateCode.QMT_ACCEPTANCE_FRESH,
                qmt_fresh,
                (
                    "missing"
                    if qmt_age is None
                    else f"{int(qmt_age.total_seconds())}s"
                ),
                (
                    f"<={int(policy.maximum_qmt_acceptance_age.total_seconds())}s"
                ),
            ),
            PromotionGate(
                PromotionGateCode.PAPER_SESSION_COUNT,
                len(closed) >= policy.minimum_paper_sessions,
                str(len(closed)),
                f">={policy.minimum_paper_sessions}",
            ),
            PromotionGate(
                PromotionGateCode.SCHEDULER_COVERAGE,
                len(window) == policy.minimum_paper_sessions
                and scheduler_covered == len(window),
                f"{scheduler_covered}/{len(window)}",
                (
                    f"{policy.minimum_paper_sessions} sessions with "
                    f">={policy.minimum_healthy_minutes_per_session} minutes"
                ),
            ),
            PromotionGate(
                PromotionGateCode.SCHEDULER_FAILURE_FREE,
                len(window) == policy.minimum_paper_sessions
                and scheduler_failures == 0,
                str(scheduler_failures),
                "0",
            ),
            PromotionGate(
                PromotionGateCode.RECONCILIATION_COVERAGE,
                len(window) == policy.minimum_paper_sessions
                and reconciled == len(window)
                and facts.failed_reconciliation_count == 0,
                (
                    f"{reconciled}/{len(window)} passing, "
                    f"{facts.failed_reconciliation_count} failed"
                ),
                (
                    f"{policy.minimum_paper_sessions}/"
                    f"{policy.minimum_paper_sessions} passing, 0 failed"
                ),
            ),
            PromotionGate(
                PromotionGateCode.FILLED_ORDER_COUNT,
                facts.filled_order_count >= policy.minimum_filled_orders,
                str(facts.filled_order_count),
                f">={policy.minimum_filled_orders}",
            ),
            PromotionGate(
                PromotionGateCode.UNKNOWN_ORDER_FREE,
                facts.unknown_order_count == 0,
                str(facts.unknown_order_count),
                "0",
            ),
            PromotionGate(
                PromotionGateCode.PAPER_TOTAL_RETURN,
                total_return is not None and total_return > 0,
                (
                    "unavailable"
                    if total_return is None
                    else _decimal_text(total_return)
                ),
                ">0",
            ),
            PromotionGate(
                PromotionGateCode.PAPER_MAX_DRAWDOWN,
                max_drawdown is not None
                and max_drawdown <= policy.maximum_drawdown,
                (
                    "unavailable"
                    if max_drawdown is None
                    else _decimal_text(max_drawdown)
                ),
                f"<={_decimal_text(policy.maximum_drawdown)}",
            ),
            PromotionGate(
                PromotionGateCode.PROFITABLE_SESSION_RATE,
                profitable_rate is not None
                and profitable_rate
                >= policy.minimum_profitable_session_rate,
                (
                    "unavailable"
                    if profitable_rate is None
                    else _decimal_text(profitable_rate)
                ),
                (
                    f">={_decimal_text(policy.minimum_profitable_session_rate)}"
                ),
            ),
            PromotionGate(
                PromotionGateCode.KILL_SWITCH_DRILLS,
                len(facts.kill_switch_drill_dates)
                >= policy.minimum_kill_switch_drills,
                str(len(facts.kill_switch_drill_dates)),
                f">={policy.minimum_kill_switch_drills}",
            ),
            PromotionGate(
                PromotionGateCode.WINDOWS_RECOVERY_DRILLS,
                False,
                "not_persisted",
                "disconnect_and_miniqmt_restart_verified",
            ),
            PromotionGate(
                PromotionGateCode.COMPLIANCE_APPROVAL,
                False,
                "not_persisted",
                "explicit_approved_artifact",
            ),
        )
        return PaperPromotionAudit(
            evaluated_at=facts.captured_at,
            policy_hash=policy.policy_hash,
            fact_hash=facts.fact_hash,
            gates=gates,
        )


class PostgresPaperPromotionFactRepository:
    """Read promotion facts from one repeatable, read-only PostgreSQL snapshot."""

    def __init__(
        self,
        *,
        engine: AsyncEngine,
        schema: str = "public",
    ) -> None:
        if _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError(
                "schema must be a safe PostgreSQL identifier"
            )
        self._engine = engine
        self._schema = schema

    @classmethod
    def connect(
        cls,
        *,
        dsn: str,
        schema: str = "public",
    ) -> PostgresPaperPromotionFactRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
        except Exception:
            raise PersistenceUnavailableError(
                "PostgreSQL promotion audit connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def read(
        self,
        *,
        account_id: str,
        strategy_id: str,
        now: datetime,
        lookback_days: int,
    ) -> PaperPromotionFacts:
        _require_nonblank(account_id, name="account_id")
        _require_nonblank(strategy_id, name="strategy_id")
        instant = to_utc(now, name="promotion fact time")
        if (
            not isinstance(lookback_days, int)
            or isinstance(lookback_days, bool)
            or lookback_days < 1
        ):
            raise ValueError("lookback_days must be positive")
        cutoff = instant - timedelta(days=lookback_days)
        try:
            async with self._engine.connect() as connection:
                await connection.exec_driver_sql(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                control = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT active, state_hash
                                FROM {self._schema}.execution_control_state
                                WHERE account_id = :account_id
                                """
                            ),
                            {"account_id": account_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                registration_hash = await connection.scalar(
                    text(
                        f"""
                        SELECT registration_hash
                        FROM {self._schema}.paper_strategy_activation_events
                        WHERE account_id = :account_id
                          AND strategy_id = :strategy_id
                          AND action = 'approve'
                          AND sequence = (
                              SELECT max(sequence)
                              FROM {self._schema}.paper_strategy_activation_events
                              WHERE account_id = :account_id
                                AND strategy_id = :strategy_id
                          )
                        """
                    ),
                    {
                        "account_id": account_id,
                        "strategy_id": strategy_id,
                    },
                )
                qmt = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT evidence_hash, observed_at
                                FROM {self._schema}.qmt_readonly_acceptance_evidence
                                WHERE logical_account_id = :account_id
                                ORDER BY observed_at DESC, evidence_hash DESC
                                LIMIT 1
                                """
                            ),
                            {"account_id": account_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                session_rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT s.session_date, s.as_of,
                                       s.day_start_equity,
                                       s.state_hash,
                                       p.payload ->> 'equity' AS end_equity
                                FROM {self._schema}.paper_session_risk_state s
                                JOIN {self._schema}.execution_account_snapshots p
                                  ON p.snapshot_hash = s.latest_snapshot_hash
                                WHERE s.account_id = :account_id
                                  AND s.session_date >= :cutoff_date
                                ORDER BY s.session_date
                                """
                            ),
                            {
                                "account_id": account_id,
                                "cutoff_date": cutoff.astimezone(
                                    _SHANGHAI
                                ).date(),
                            },
                        )
                    )
                    .mappings()
                    .all()
                )
                scheduler_rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT session_date,
                                       count(DISTINCT date_trunc(
                                           'minute', evaluated_at
                                       )) FILTER (
                                           WHERE phase IN (
                                               'morning_continuous',
                                               'afternoon_continuous'
                                           )
                                             AND quote_evidence_hash IS NOT NULL
                                             AND error_code IS NULL
                                             AND status IN (
                                                 'no_intents', 'completed'
                                             )
                                       ) AS healthy_minutes,
                                       count(*) FILTER (
                                           WHERE status = 'failed'
                                              OR error_code IS NOT NULL
                                       ) AS failure_count,
                                       max(evaluated_at) AS latest_evaluated_at,
                                       (array_agg(
                                           event_hash
                                           ORDER BY evaluated_at DESC,
                                                    sequence DESC
                                       ))[1] AS latest_event_hash
                                FROM {self._schema}.paper_scheduler_events
                                WHERE account_id = :account_id
                                  AND strategy_id = :strategy_id
                                  AND evaluated_at >= :cutoff
                                GROUP BY session_date
                                ORDER BY session_date
                                """
                            ),
                            {
                                "account_id": account_id,
                                "strategy_id": strategy_id,
                                "cutoff": cutoff,
                            },
                        )
                    )
                    .mappings()
                    .all()
                )
                reconciliation_rows = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT (
                                           evaluated_at AT TIME ZONE
                                           'Asia/Shanghai'
                                       )::date AS session_date,
                                       bool_or(reconciled) AS has_passing,
                                       count(*) FILTER (
                                           WHERE NOT reconciled
                                       ) AS failure_count
                                FROM {self._schema}.execution_reconciliation_reports
                                WHERE account_id = :account_id
                                  AND evaluated_at >= :cutoff
                                  AND (
                                      evaluated_at AT TIME ZONE
                                      'Asia/Shanghai'
                                  )::time >= TIME '14:55:00'
                                GROUP BY session_date
                                ORDER BY session_date
                                """
                            ),
                            {
                                "account_id": account_id,
                                "cutoff": cutoff,
                            },
                        )
                    )
                    .mappings()
                    .all()
                )
                order_counts = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT count(*) FILTER (
                                           WHERE state = 'filled'
                                       ) AS filled_count,
                                       count(*) FILTER (
                                           WHERE state = 'unknown'
                                       ) AS unknown_count
                                FROM {self._schema}.paper_orders
                                WHERE account_id = :account_id
                                  AND approved_at >= :cutoff
                                """
                            ),
                            {
                                "account_id": account_id,
                                "cutoff": cutoff,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
                risk_count = await connection.scalar(
                    text(
                        f"""
                        SELECT count(*)
                        FROM {self._schema}.risk_decisions
                        WHERE account_id = :account_id
                          AND mode = 'paper'
                          AND evaluated_at >= :cutoff
                        """
                    ),
                    {
                        "account_id": account_id,
                        "cutoff": cutoff,
                    },
                )
                drill_dates = (
                    await connection.scalars(
                        text(
                            f"""
                            SELECT DISTINCT (
                                occurred_at AT TIME ZONE
                                'Asia/Shanghai'
                            )::date
                            FROM {self._schema}.execution_control_events
                            WHERE account_id = :account_id
                              AND action = 'activate'
                              AND reason = 'drill'
                              AND occurred_at >= :cutoff
                            ORDER BY 1
                            """
                        ),
                        {
                            "account_id": account_id,
                            "cutoff": cutoff,
                        },
                    )
                ).all()
                await connection.rollback()
        except (LookupError, ValueError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "Promotion audit fact snapshot failed"
            ) from None
        return PaperPromotionFacts(
            account_id=account_id,
            strategy_id=strategy_id,
            captured_at=instant,
            kill_switch_active=(
                False if control is None else bool(control["active"])
            ),
            control_state_hash=(
                _canonical_hash(
                    {
                        "account_id": account_id,
                        "state": "missing",
                        "version": "promotion-control-missing-v1",
                    }
                )
                if control is None
                else str(control["state_hash"])
            ),
            active_registration_hash=(
                None
                if registration_hash is None
                else str(registration_hash)
            ),
            qmt_evidence_hash=(
                None if qmt is None else str(qmt["evidence_hash"])
            ),
            qmt_observed_at=(
                None if qmt is None else qmt["observed_at"]
            ),
            sessions=tuple(_session_from_row(row) for row in session_rows),
            scheduler_sessions=tuple(
                _scheduler_from_row(row) for row in scheduler_rows
            ),
            reconciled_session_dates=tuple(
                row["session_date"]
                for row in reconciliation_rows
                if bool(row["has_passing"])
            ),
            failed_reconciliation_count=sum(
                int(row["failure_count"])
                for row in reconciliation_rows
            ),
            filled_order_count=int(order_counts["filled_count"]),
            unknown_order_count=int(order_counts["unknown_count"]),
            risk_decision_count=int(risk_count or 0),
            kill_switch_drill_dates=tuple(drill_dates),
        )


def _session_from_row(row: RowMapping) -> PaperSessionPromotionEvidence:
    try:
        return PaperSessionPromotionEvidence(
            session_date=row["session_date"],
            observed_at=row["as_of"],
            day_start_equity=Decimal(str(row["day_start_equity"])),
            end_equity=Decimal(str(row["end_equity"])),
            state_hash=str(row["state_hash"]),
        )
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "Paper promotion session evidence is invalid"
        ) from None


def _scheduler_from_row(row: RowMapping) -> SchedulerPromotionEvidence:
    try:
        return SchedulerPromotionEvidence(
            session_date=row["session_date"],
            healthy_minute_count=int(row["healthy_minutes"]),
            failure_count=int(row["failure_count"]),
            latest_evaluated_at=row["latest_evaluated_at"],
            latest_event_hash=str(row["latest_event_hash"]),
        )
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "Paper promotion scheduler evidence is invalid"
        ) from None


def _performance(
    sessions: tuple[PaperSessionPromotionEvidence, ...],
) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    if not sessions:
        return None, None, None
    wealth = Decimal("1")
    peak = wealth
    max_drawdown = Decimal("0")
    profitable = 0
    for session in sessions:
        wealth *= Decimal("1") + session.session_return
        peak = max(peak, wealth)
        drawdown = Decimal("1") - wealth / peak
        max_drawdown = max(max_drawdown, drawdown)
        if session.session_return > 0:
            profitable += 1
    total_return = wealth - Decimal("1")
    profitable_rate = Decimal(profitable) / Decimal(len(sessions))
    return total_return, max_drawdown, profitable_rate
