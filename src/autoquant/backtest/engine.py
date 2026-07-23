from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from itertools import pairwise

from autoquant.backtest.ledger import ExecutionModel, PortfolioLedger
from autoquant.backtest.models import (
    AccountSnapshot,
    BacktestResult,
    BacktestSession,
    ExecutionReport,
    MarketState,
    OrderIntent,
)
from autoquant.backtest.rules import FeeSchedule
from autoquant.clock import to_utc
from autoquant.data.models import _require_lowercase_sha256, _require_nonblank


class BacktestEngine:
    def __init__(
        self,
        *,
        fees: FeeSchedule | None = None,
        execution: ExecutionModel | None = None,
    ) -> None:
        self._fees = fees or FeeSchedule()
        self._execution = execution or ExecutionModel()

    def run(
        self,
        *,
        strategy_id: str,
        manifest_hash: str,
        as_of: datetime,
        initial_cash: Decimal,
        sessions: tuple[BacktestSession, ...],
    ) -> BacktestResult:
        sessions = tuple(sessions)
        if not sessions:
            raise ValueError("sessions cannot be empty")
        return self._run_with_order_factory(
            strategy_id=strategy_id,
            manifest_hash=manifest_hash,
            as_of=as_of,
            initial_cash=initial_cash,
            market_sessions=tuple(
                session.markets for session in sessions
            ),
            order_factory=lambda index, _markets, _previous: (
                sessions[index].orders
            ),
        )

    def run_dynamic(
        self,
        *,
        strategy_id: str,
        manifest_hash: str,
        as_of: datetime,
        initial_cash: Decimal,
        market_sessions: tuple[tuple[MarketState, ...], ...],
        order_factory: Callable[
            [
                int,
                tuple[MarketState, ...],
                AccountSnapshot | None,
            ],
            tuple[OrderIntent, ...],
        ],
    ) -> BacktestResult:
        """Run an order policy against actual prior-session fills."""

        return self._run_with_order_factory(
            strategy_id=strategy_id,
            manifest_hash=manifest_hash,
            as_of=as_of,
            initial_cash=initial_cash,
            market_sessions=market_sessions,
            order_factory=order_factory,
        )

    def _run_with_order_factory(
        self,
        *,
        strategy_id: str,
        manifest_hash: str,
        as_of: datetime,
        initial_cash: Decimal,
        market_sessions: tuple[tuple[MarketState, ...], ...],
        order_factory: Callable[
            [
                int,
                tuple[MarketState, ...],
                AccountSnapshot | None,
            ],
            tuple[OrderIntent, ...],
        ],
    ) -> BacktestResult:
        _require_nonblank(strategy_id, name="strategy_id")
        _require_lowercase_sha256(manifest_hash, name="manifest_hash")
        cutoff = to_utc(as_of, name="as_of")
        market_sessions = tuple(
            tuple(markets) for markets in market_sessions
        )
        if not market_sessions:
            raise ValueError("sessions cannot be empty")
        if any(not markets for markets in market_sessions):
            raise ValueError("every session requires market data")
        empty_sessions = tuple(
            BacktestSession(
                session_date=markets[0].bar.session_date,
                markets=markets,
                orders=(),
            )
            for markets in market_sessions
        )
        if any(
            current.session_date >= following.session_date
            for current, following in pairwise(empty_sessions)
        ):
            raise ValueError("sessions must be strictly increasing and unique")
        for session in empty_sessions:
            for market in session.markets:
                if market.bar.available_at > cutoff:
                    raise ValueError(
                        "market data is not visible at the requested as_of"
                    )

        ledger = PortfolioLedger(
            initial_cash=initial_cash,
            fees=self._fees,
            execution=self._execution,
        )
        reports: list[ExecutionReport] = []
        snapshots: list[AccountSnapshot] = []
        rule_versions: set[str] = set()
        for index, empty_session in enumerate(empty_sessions):
            orders = tuple(
                order_factory(
                    index,
                    empty_session.markets,
                    None if not snapshots else snapshots[-1],
                )
            )
            session = BacktestSession(
                session_date=empty_session.session_date,
                markets=empty_session.markets,
                orders=orders,
            )
            ledger.start_session(session.session_date)
            market_by_instrument = {
                market.bar.instrument: market for market in session.markets
            }
            for market in session.markets:
                rule_versions.add(market.rules.rule_version)
                rule_versions.add(market.rules.price_limit.rule_version)
            for order in session.orders:
                execution_market = market_by_instrument.get(order.instrument)
                if execution_market is None:
                    # Use the first market only to produce a stable, audited unknown-
                    # instrument rejection without fabricating prices for that symbol.
                    reports.append(ledger.execute(order, session.markets[0]))
                else:
                    reports.append(ledger.execute(order, execution_market))
            snapshots.append(ledger.snapshot(session.markets))

        equities = tuple(snapshot.equity for snapshot in snapshots)
        peak = ledger.initial_cash
        max_drawdown = Decimal("0")
        for equity in equities:
            peak = max(peak, equity)
            drawdown = Decimal("0") if peak == 0 else (peak - equity) / peak
            max_drawdown = max(max_drawdown, drawdown)
        ending_equity = equities[-1]
        return BacktestResult(
            strategy_id=strategy_id,
            manifest_hash=manifest_hash,
            as_of=cutoff,
            initial_cash=ledger.initial_cash,
            ending_equity=ending_equity,
            total_return=(ending_equity / ledger.initial_cash) - Decimal("1"),
            max_drawdown=max_drawdown,
            turnover=ledger.gross_turnover / ledger.initial_cash,
            total_fees=ledger.total_fees,
            reports=tuple(reports),
            snapshots=tuple(snapshots),
            events=ledger.events,
            rule_versions=tuple(sorted(rule_versions)),
            fee_version=self._fees.version,
            execution_version=self._execution.version,
            ledger_hash=ledger.ledger_hash,
        )
