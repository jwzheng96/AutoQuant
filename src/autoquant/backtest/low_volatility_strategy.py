from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import ROUND_FLOOR, Decimal, localcontext
from itertools import pairwise

from autoquant.backtest.dynamic_panel import (
    DynamicMarketPanel,
    DynamicMarketSession,
)
from autoquant.backtest.dynamic_strategy import (
    _can_trade,
    _liquidity_quantity,
)
from autoquant.backtest.low_volatility_portfolio import (
    LowVolatilityResearchSpec,
)
from autoquant.backtest.models import (
    AccountSnapshot,
    MarketState,
    OrderIntent,
    OrderSide,
)
from autoquant.backtest.runner import _baseline_quantity, _order
from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
)

LOW_VOLATILITY_EXECUTABLE_PANEL_VERSION = "low-volatility-executable-panel-v1"
LOW_VOLATILITY_ORDER_POLICY_VERSION = "low-volatility-equal-weight-orders-v1"


@dataclass(frozen=True, slots=True)
class LowVolatilityObservation:
    instrument: str
    signal_date: date
    execution_date: date
    volatility: Decimal
    window_hash: str
    observation_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            self.signal_date >= self.execution_date
            or not isinstance(self.volatility, Decimal)
            or not self.volatility.is_finite()
            or self.volatility < 0
        ):
            raise ValueError("low-volatility observation is inconsistent")
        _require_lowercase_sha256(
            self.window_hash,
            name="low-volatility window hash",
        )
        object.__setattr__(
            self,
            "observation_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "execution_date": self.execution_date.isoformat(),
            "instrument": self.instrument,
            "signal_date": self.signal_date.isoformat(),
            "volatility": _decimal_text(self.volatility),
            "window_hash": self.window_hash,
        }


@dataclass(frozen=True, slots=True)
class LowVolatilityExecutableSession:
    session_date: date
    snapshot_hash: str
    active_members: tuple[str, ...]
    observations: tuple[LowVolatilityObservation, ...]
    markets: tuple[MarketState, ...]
    session_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.snapshot_hash,
            name="low-volatility snapshot hash",
        )
        members = tuple(self.active_members)
        observations = tuple(
            sorted(
                self.observations,
                key=lambda value: value.instrument,
            )
        )
        markets = tuple(
            sorted(
                self.markets,
                key=lambda value: value.bar.instrument,
            )
        )
        if (
            not members
            or members != tuple(sorted(members))
            or len(set(members)) != len(members)
            or len({value.instrument for value in observations}) != len(observations)
            or any(
                value.execution_date != self.session_date or value.instrument not in members
                for value in observations
            )
            or not markets
            or any(value.bar.session_date != self.session_date for value in markets)
            or len({value.bar.instrument for value in markets}) != len(markets)
        ):
            raise ValueError("low-volatility executable session is inconsistent")
        object.__setattr__(self, "active_members", members)
        object.__setattr__(self, "observations", observations)
        object.__setattr__(self, "markets", markets)
        object.__setattr__(
            self,
            "session_hash",
            _canonical_hash(
                {
                    "market_rows": [
                        {
                            "bar_hash": value.bar.content_hash,
                            "limit_hash": (
                                None
                                if value.daily_price_limit is None
                                else value.daily_price_limit.content_hash
                            ),
                            "price_limit_rule": (value.rules.price_limit.rule_version),
                            "rule_version": (value.rules.rule_version),
                            "suspended": value.suspended,
                        }
                        for value in markets
                    ],
                    "observation_hashes": [value.observation_hash for value in observations],
                    "session_date": self.session_date.isoformat(),
                    "snapshot_hash": self.snapshot_hash,
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class LowVolatilityExecutablePanel:
    spec_hash: str
    market_panel_hash: str
    as_of: datetime
    sessions: tuple[LowVolatilityExecutableSession, ...]
    version: str = LOW_VOLATILITY_EXECUTABLE_PANEL_VERSION
    panel_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.spec_hash,
            name="low-volatility executable spec hash",
        )
        _require_lowercase_sha256(
            self.market_panel_hash,
            name="low-volatility market panel hash",
        )
        sessions = tuple(self.sessions)
        if (
            not sessions
            or any(
                current.session_date >= following.session_date
                for current, following in pairwise(sessions)
            )
            or self.version != LOW_VOLATILITY_EXECUTABLE_PANEL_VERSION
        ):
            raise ValueError("low-volatility executable panel is inconsistent")
        object.__setattr__(self, "sessions", sessions)
        object.__setattr__(
            self,
            "as_of",
            to_utc(
                self.as_of,
                name="low-volatility executable panel as_of",
            ),
        )
        object.__setattr__(
            self,
            "panel_hash",
            _canonical_hash(
                {
                    "as_of": self.as_of.isoformat(),
                    "market_panel_hash": self.market_panel_hash,
                    "session_hashes": [value.session_hash for value in sessions],
                    "spec_hash": self.spec_hash,
                    "version": self.version,
                }
            ),
        )


def compile_low_volatility_executable_panel(
    *,
    spec: LowVolatilityResearchSpec,
    markets: DynamicMarketPanel,
) -> LowVolatilityExecutablePanel:
    if (
        markets.spec_hash != spec.spec_hash
        or markets.dataset_manifest_hash != spec.dataset_manifest_hash
        or markets.plan_hash != spec.plan_hash
    ):
        raise ValueError("low-volatility market panel does not match the spec")
    market_maps = tuple(
        {value.bar.instrument: value for value in session.markets} for session in markets.sessions
    )
    sessions: list[LowVolatilityExecutableSession] = []
    for execution_index, session in enumerate(markets.sessions):
        observations = _session_observations(
            execution_index=execution_index,
            sessions=markets.sessions,
            market_maps=market_maps,
            spec=spec,
        )
        sessions.append(
            LowVolatilityExecutableSession(
                session_date=session.session_date,
                snapshot_hash=session.snapshot_hash,
                active_members=session.active_members,
                observations=observations,
                markets=session.markets,
            )
        )
    return LowVolatilityExecutablePanel(
        spec_hash=spec.spec_hash,
        market_panel_hash=markets.panel_hash,
        as_of=markets.as_of,
        sessions=tuple(sessions),
    )


def _session_observations(
    *,
    execution_index: int,
    sessions: tuple[DynamicMarketSession, ...],
    market_maps: tuple[dict[str, MarketState], ...],
    spec: LowVolatilityResearchSpec,
) -> tuple[LowVolatilityObservation, ...]:
    signal_index = execution_index - spec.signal_lag_sessions
    first_index = signal_index - spec.volatility_lookback_sessions
    if first_index < 0:
        return ()
    execution = sessions[execution_index]
    signal = sessions[signal_index]
    values: list[LowVolatilityObservation] = []
    for instrument in execution.active_members:
        window = tuple(
            market_maps[index].get(instrument) for index in range(first_index, signal_index + 1)
        )
        if len(window) != spec.minimum_history_sessions or any(value is None for value in window):
            continue
        complete = tuple(value for value in window if value is not None)
        closes = tuple(value.bar.close_price for value in complete)
        volatility = _realized_volatility(closes)
        values.append(
            LowVolatilityObservation(
                instrument=instrument,
                signal_date=signal.session_date,
                execution_date=execution.session_date,
                volatility=volatility,
                window_hash=_canonical_hash(
                    {
                        "bar_hashes": [value.bar.content_hash for value in complete],
                        "instrument": instrument,
                        "version": (LOW_VOLATILITY_EXECUTABLE_PANEL_VERSION),
                    }
                ),
            )
        )
    return tuple(values)


def _realized_volatility(
    closes: tuple[Decimal, ...],
) -> Decimal:
    if len(closes) < 2 or any(value <= 0 for value in closes):
        raise ValueError("low-volatility close window is invalid")
    with localcontext() as context:
        context.prec = 34
        returns = tuple(current / previous - Decimal("1") for previous, current in pairwise(closes))
        mean = sum(returns, Decimal("0")) / Decimal(len(returns))
        variance = sum((value - mean) ** 2 for value in returns) / Decimal(len(returns) - 1)
        return +variance.sqrt()


class LowVolatilityOrderPolicy:
    """Trade the lowest fixed trailing-volatility ranks."""

    def __init__(
        self,
        *,
        sessions: tuple[LowVolatilityExecutableSession, ...],
        start_index: int,
        trade_session_count: int,
        spec: LowVolatilityResearchSpec,
    ) -> None:
        sessions = tuple(sessions)
        if (
            not sessions
            or start_index < spec.minimum_history_sessions
            or trade_session_count < 2
            or start_index + trade_session_count > len(sessions)
        ):
            raise ValueError("low-volatility order policy interval is invalid")
        self._sessions = sessions
        self._start_index = start_index
        self._trade_session_count = trade_session_count
        self._spec = spec
        self._last_rebalance: int | None = None
        self._order_sequence = 0

    def __call__(
        self,
        trade_index: int,
        markets: tuple[MarketState, ...],
        previous: AccountSnapshot | None,
    ) -> tuple[OrderIntent, ...]:
        if not 0 <= trade_index < self._trade_session_count:
            raise ValueError("low-volatility trade index is invalid")
        session = self._sessions[self._start_index + trade_index]
        if not markets or markets != session.markets:
            raise ValueError("low-volatility markets do not match the panel")
        holdings = (
            {}
            if previous is None
            else {value.instrument: value.total_quantity for value in previous.positions}
        )
        current = {value.bar.instrument: value for value in markets}
        if trade_index == self._trade_session_count - 1:
            return self._orders_to_targets(
                holdings=holdings,
                targets={},
                markets=current,
                session_date=session.session_date,
                suffix="forced-exit",
            )
        rebalance = (
            self._last_rebalance is None
            or trade_index - self._last_rebalance >= self._spec.rebalance_sessions
        )
        if not rebalance:
            return ()
        self._last_rebalance = trade_index
        selected = (
            tuple(
                value.instrument
                for value in sorted(
                    session.observations,
                    key=lambda value: (
                        value.volatility,
                        value.instrument,
                    ),
                )[: self._spec.selection_count]
            )
            if len(session.observations) >= self._spec.minimum_eligible_members
            else ()
        )
        targets = {
            instrument: quantity
            for instrument in selected
            if (market := current.get(instrument)) is not None
            and _can_trade(OrderSide.BUY, market)
            and (
                quantity := _target_quantity(
                    market=market,
                    spec=self._spec,
                )
            )
            >= market.rules.buy_minimum
        }
        return self._orders_to_targets(
            holdings=holdings,
            targets=targets,
            markets=current,
            session_date=session.session_date,
            suffix="rebalance",
        )

    def _orders_to_targets(
        self,
        *,
        holdings: dict[str, int],
        targets: dict[str, int],
        markets: dict[str, MarketState],
        session_date: date,
        suffix: str,
    ) -> tuple[OrderIntent, ...]:
        orders: list[OrderIntent] = []
        for side in (OrderSide.SELL, OrderSide.BUY):
            for instrument in sorted(set(holdings) | set(targets)):
                held = holdings.get(instrument, 0)
                target = targets.get(instrument, 0)
                delta = target - held
                if (side is OrderSide.SELL and delta >= 0) or (
                    side is OrderSide.BUY and delta <= 0
                ):
                    continue
                market = markets.get(instrument)
                if market is None or not _can_trade(side, market):
                    continue
                step = market.rules.sell_step if side is OrderSide.SELL else market.rules.buy_step
                desired = abs(delta)
                if side is OrderSide.BUY:
                    desired = desired // step * step
                elif desired != held:
                    desired = desired // step * step
                quantity = min(
                    desired,
                    _liquidity_quantity(
                        market,
                        participation=(self._spec.maximum_volume_participation),
                        step=step,
                    ),
                    _notional_quantity_limit(
                        market=market,
                        maximum_notional=(self._spec.maximum_order_notional),
                        step=step,
                    ),
                )
                if quantity <= 0 or (side is OrderSide.BUY and quantity < market.rules.buy_minimum):
                    continue
                self._order_sequence += 1
                orders.append(
                    _order(
                        order_id=(f"low-volatility-{self._order_sequence:06d}-{suffix}"),
                        instrument=instrument,
                        side=side,
                        quantity=quantity,
                        session_date=session_date,
                    )
                )
        return tuple(orders)


def _target_quantity(
    *,
    market: MarketState,
    spec: LowVolatilityResearchSpec,
) -> int:
    allocation = min(
        spec.maximum_position_weight,
        spec.gross_allocation / Decimal(spec.selection_count),
    )
    estimated_price = market.bar.pre_close * (Decimal("1") + spec.slippage_bps / Decimal("10000"))
    affordable = int((spec.initial_cash * allocation) // estimated_price)
    if affordable < market.rules.buy_minimum:
        return 0
    quantity = _baseline_quantity(
        initial_cash=spec.initial_cash,
        allocation=allocation,
        reference_price=market.bar.pre_close,
        slippage_bps=spec.slippage_bps,
        buy_minimum=market.rules.buy_minimum,
        buy_step=market.rules.buy_step,
        maximum=market.rules.max_order_quantity,
    )
    return min(
        quantity,
        _notional_quantity_limit(
            market=market,
            maximum_notional=spec.maximum_order_notional,
            step=market.rules.buy_step,
        ),
    )


def _notional_quantity_limit(
    *,
    market: MarketState,
    maximum_notional: Decimal,
    step: int,
) -> int:
    raw = (maximum_notional / market.bar.pre_close).to_integral_value(rounding=ROUND_FLOOR)
    return int(raw) // step * step
