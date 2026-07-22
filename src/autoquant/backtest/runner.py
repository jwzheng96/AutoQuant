from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Protocol

from autoquant.backtest.engine import BacktestEngine
from autoquant.backtest.ledger import ExecutionModel
from autoquant.backtest.models import (
    BacktestResult,
    BacktestSession,
    MarketState,
    OrderIntent,
    OrderSide,
)
from autoquant.backtest.rules import AshareRuleBook
from autoquant.data.daily_ingestion import ValidatedDailyDataset
from autoquant.data.models import DatasetManifest
from autoquant.web.models import BacktestRunRequest

STRATEGY_ID = "manifest_buy_hold_v1"


class ManifestControlPort(Protocol):
    async def read_manifest(self, manifest_hash: str) -> DatasetManifest: ...


class DailyDatasetReaderPort(Protocol):
    async def query(
        self, manifest_hash: str, as_of: datetime
    ) -> ValidatedDailyDataset: ...


class ManifestMarketCompiler:
    """Validate a daily research dataset and compile point-in-time market states."""

    def __init__(self, *, rulebook: AshareRuleBook | None = None) -> None:
        self._rulebook = rulebook or AshareRuleBook()

    def compile(
        self,
        instrument: str,
        dataset: ValidatedDailyDataset,
    ) -> tuple[MarketState, ...]:
        bars = tuple(
            sorted(
                (bar for bar in dataset.bars if bar.instrument == instrument),
                key=lambda bar: bar.session_date,
            )
        )
        if not bars:
            raise ValueError("manifest has no bars for the selected instrument")
        factors = tuple(
            factor.factor
            for factor in dataset.factors
            if factor.instrument == instrument
        )
        if len(factors) != len(bars) or len(set(factors)) != 1:
            raise ValueError("corporate-action accounting is required for this manifest")

        lifecycle = tuple(
            item
            for item in dataset.coverage.lifecycles
            if item.instrument == instrument
        )
        if len(lifecycle) != 1:
            raise ValueError("manifest must contain one instrument lifecycle")
        first_date = bars[0].session_date
        last_date = bars[-1].session_date
        if lifecycle[0].list_date > first_date or (
            lifecycle[0].delist_date is not None
            and lifecycle[0].delist_date < last_date
        ):
            raise ValueError("instrument lifecycle does not cover the backtest interval")

        open_sessions = {
            item.session_date for item in dataset.coverage.sessions if item.is_open
        }
        suspension_by_date = {
            item.session_date: item
            for item in dataset.coverage.suspensions
            if item.instrument == instrument
        }
        limit_by_date = {
            item.session_date: item
            for item in dataset.coverage.price_limits
            if item.instrument == instrument
        }
        bar_dates = {bar.session_date for bar in bars}
        if not bar_dates.issubset(open_sessions):
            raise ValueError("trading calendar does not cover every market bar")
        if set(suspension_by_date) != bar_dates:
            raise ValueError("suspension evidence does not cover every market bar")
        if set(limit_by_date) != bar_dates:
            raise ValueError("exact price limits do not cover every market bar")

        return tuple(
            MarketState(
                bar=bar,
                rules=self._rulebook.resolve_with_price_limit(
                    instrument, bar.session_date, limit_by_date[bar.session_date]
                ),
                suspended=suspension_by_date[bar.session_date].suspended,
                daily_price_limit=limit_by_date[bar.session_date],
            )
            for bar in bars
        )


class ManifestBacktestRunner:
    """Compile one immutable daily manifest into a deterministic baseline run."""

    def __init__(
        self,
        *,
        control_repository: ManifestControlPort,
        dataset_reader: DailyDatasetReaderPort,
        rulebook: AshareRuleBook | None = None,
        compiler: ManifestMarketCompiler | None = None,
    ) -> None:
        self._control = control_repository
        self._reader = dataset_reader
        if rulebook is not None and compiler is not None:
            raise ValueError("rulebook and compiler cannot both be supplied")
        self._compiler = compiler or ManifestMarketCompiler(rulebook=rulebook)

    async def run(self, request: BacktestRunRequest) -> BacktestResult:
        manifest = await self._control.read_manifest(request.manifest_hash)
        if request.instrument not in manifest.instruments:
            raise ValueError("instrument is not present in the selected manifest")
        dataset = await self._reader.query(manifest.manifest_hash, manifest.as_of)
        sessions = self._compile_sessions(request, dataset)
        engine = BacktestEngine(
            execution=ExecutionModel(slippage_bps=request.slippage_bps)
        )
        return engine.run(
            strategy_id=STRATEGY_ID,
            manifest_hash=manifest.manifest_hash,
            as_of=manifest.as_of,
            initial_cash=request.initial_cash,
            sessions=sessions,
        )

    def _compile_sessions(
        self,
        request: BacktestRunRequest,
        dataset: ValidatedDailyDataset,
    ) -> tuple[BacktestSession, ...]:
        markets = self._compiler.compile(request.instrument, dataset)
        first_date = markets[0].bar.session_date
        last_date = markets[-1].bar.session_date
        first_rules = markets[0].rules
        quantity = _baseline_quantity(
            initial_cash=request.initial_cash,
            allocation=request.allocation,
            reference_price=markets[0].bar.pre_close,
            slippage_bps=request.slippage_bps,
            buy_minimum=first_rules.buy_minimum,
            buy_step=first_rules.buy_step,
            maximum=first_rules.max_order_quantity,
        )

        compiled: list[BacktestSession] = []
        for market in markets:
            session_date = market.bar.session_date
            orders: list[OrderIntent] = []
            if session_date == first_date:
                orders.append(
                    _order(
                        order_id="baseline-entry",
                        instrument=request.instrument,
                        side=OrderSide.BUY,
                        quantity=quantity,
                        session_date=session_date,
                    )
                )
            if (
                request.liquidate_at_end
                and last_date > first_date
                and session_date == last_date
            ):
                orders.append(
                    _order(
                        order_id="baseline-exit",
                        instrument=request.instrument,
                        side=OrderSide.SELL,
                        quantity=quantity,
                        session_date=session_date,
                    )
                )
            compiled.append(
                BacktestSession(
                    session_date=session_date,
                    markets=(market,),
                    orders=tuple(orders),
                )
            )
        return tuple(compiled)


def _baseline_quantity(
    *,
    initial_cash: Decimal,
    allocation: Decimal,
    reference_price: Decimal,
    slippage_bps: Decimal,
    buy_minimum: int,
    buy_step: int,
    maximum: int,
) -> int:
    estimated_price = reference_price * (
        Decimal("1") + slippage_bps / Decimal("10000")
    )
    raw = min(int((initial_cash * allocation) // estimated_price), maximum)
    if raw < buy_minimum:
        raise ValueError("initial cash is too small for one valid board lot")
    return buy_minimum + ((raw - buy_minimum) // buy_step) * buy_step


def _order(
    *,
    order_id: str,
    instrument: str,
    side: OrderSide,
    quantity: int,
    session_date: date,
) -> OrderIntent:
    submitted = datetime.combine(
        session_date - timedelta(days=1),
        time.min,
        tzinfo=UTC,
    )
    return OrderIntent(
        client_order_id=order_id,
        instrument=instrument,
        side=side,
        quantity=quantity,
        session_date=session_date,
        submitted_at=submitted,
    )
