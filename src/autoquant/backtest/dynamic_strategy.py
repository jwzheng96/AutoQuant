from __future__ import annotations

from bisect import bisect_right
from datetime import date
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal

from autoquant.backtest.dynamic_panel import DynamicMarketSession
from autoquant.backtest.dynamic_portfolio import (
    DynamicPortfolioResearchSpec,
)
from autoquant.backtest.models import (
    AccountSnapshot,
    MarketState,
    OrderIntent,
    OrderSide,
)
from autoquant.backtest.portfolio_validation import (
    CrossSectionalMomentumParameters,
)
from autoquant.backtest.runner import _baseline_quantity, _order


class DynamicMomentumOrderPolicy:
    """Point-in-time momentum orders constrained to observable liquidity."""

    def __init__(
        self,
        *,
        sessions: tuple[DynamicMarketSession, ...],
        start_index: int,
        trade_session_count: int,
        parameters: CrossSectionalMomentumParameters,
        spec: DynamicPortfolioResearchSpec,
    ) -> None:
        sessions = tuple(sessions)
        if (
            not sessions
            or start_index < 0
            or trade_session_count < 1
            or start_index + trade_session_count > len(sessions)
            or parameters not in spec.candidates
        ):
            raise ValueError("dynamic momentum policy interval is invalid")
        self._sessions = sessions
        self._start_index = start_index
        self._trade_session_count = trade_session_count
        self._parameters = parameters
        self._spec = spec
        self._last_rebalance: int | None = None
        self._order_sequence = 0
        observations: dict[str, list[int]] = {}
        for index, session in enumerate(sessions):
            for market in session.markets:
                observations.setdefault(
                    market.bar.instrument,
                    [],
                ).append(index)
        self._observations = {
            instrument: tuple(indices)
            for instrument, indices in observations.items()
        }

    def __call__(
        self,
        trade_index: int,
        markets: tuple[MarketState, ...],
        previous: AccountSnapshot | None,
    ) -> tuple[OrderIntent, ...]:
        if not 0 <= trade_index < self._trade_session_count:
            raise ValueError("dynamic momentum trade index is invalid")
        absolute_index = self._start_index + trade_index
        session = self._sessions[absolute_index]
        if (
            not markets
            or markets != session.markets
            or any(
                value.bar.session_date != session.session_date
                for value in markets
            )
        ):
            raise ValueError(
                "dynamic momentum markets do not match the frozen panel"
            )
        holdings = (
            {}
            if previous is None
            else {
                value.instrument: value.total_quantity
                for value in previous.positions
            }
        )
        current = {
            value.bar.instrument: value for value in session.markets
        }
        if trade_index == self._trade_session_count - 1:
            return self._exit_orders(
                holdings=holdings,
                markets=current,
                session_date=session.session_date,
                suffix="forced-exit",
            )
        signal_index = absolute_index - self._spec.signal_lag_sessions
        lookback_index = (
            signal_index - self._parameters.lookback_sessions
        )
        can_signal = lookback_index >= 0
        rebalance = can_signal and (
            self._last_rebalance is None
            or trade_index - self._last_rebalance
            >= self._parameters.rebalance_sessions
        )
        if not rebalance:
            return ()
        self._last_rebalance = trade_index
        selected = self._selected(
            absolute_index=absolute_index,
            signal_index=signal_index,
            lookback_index=lookback_index,
        )
        orders = list(
            self._exit_orders(
                holdings={
                    instrument: quantity
                    for instrument, quantity in holdings.items()
                    if instrument not in selected
                },
                markets=current,
                session_date=session.session_date,
                suffix="exit",
            )
        )
        for instrument in sorted(selected - set(holdings)):
            market = current.get(instrument)
            if market is None or not _can_trade(
                OrderSide.BUY,
                market,
            ):
                continue
            quantity = _baseline_quantity(
                initial_cash=self._spec.initial_cash,
                allocation=(
                    self._spec.gross_allocation
                    / self._parameters.selection_count
                ),
                reference_price=market.bar.pre_close,
                slippage_bps=self._spec.slippage_bps,
                buy_minimum=market.rules.buy_minimum,
                buy_step=market.rules.buy_step,
                maximum=market.rules.max_order_quantity,
            )
            quantity = min(
                quantity,
                _liquidity_quantity(
                    market,
                    participation=self._spec.maximum_volume_participation,
                    step=market.rules.buy_step,
                ),
            )
            if quantity < market.rules.buy_minimum:
                continue
            orders.append(
                self._order(
                    instrument=instrument,
                    side=OrderSide.BUY,
                    quantity=quantity,
                    session_date=session.session_date,
                    suffix="entry",
                )
            )
        return tuple(orders)

    def _selected(
        self,
        *,
        absolute_index: int,
        signal_index: int,
        lookback_index: int,
    ) -> set[str]:
        current_members = set(
            self._sessions[absolute_index].active_members
        )
        signal = {
            value.bar.instrument: value
            for value in self._sessions[signal_index].markets
        }
        lookback = {
            value.bar.instrument: value
            for value in self._sessions[lookback_index].markets
        }
        scored = sorted(
            (
                (
                    signal[instrument].bar.close_price
                    / lookback[instrument].bar.close_price
                    - Decimal("1"),
                    instrument,
                )
                for instrument in current_members
                if instrument in signal
                and instrument in lookback
                and self._observed_session_count(
                    instrument,
                    signal_index,
                )
                >= self._spec.minimum_member_history_sessions
            ),
            key=lambda value: (-value[0], value[1]),
        )
        return {
            instrument
            for score, instrument in scored[
                : self._parameters.selection_count
            ]
            if score > 0
        }

    def _observed_session_count(
        self,
        instrument: str,
        through_index: int,
    ) -> int:
        return bisect_right(
            self._observations.get(instrument, ()),
            through_index,
        )

    def _exit_orders(
        self,
        *,
        holdings: dict[str, int],
        markets: dict[str, MarketState],
        session_date: date,
        suffix: str,
    ) -> tuple[OrderIntent, ...]:
        orders: list[OrderIntent] = []
        for instrument, held_quantity in sorted(holdings.items()):
            market = markets.get(instrument)
            if market is None or not _can_trade(
                OrderSide.SELL,
                market,
            ):
                continue
            quantity = min(
                held_quantity,
                _liquidity_quantity(
                    market,
                    participation=self._spec.maximum_volume_participation,
                    step=market.rules.sell_step,
                ),
            )
            if quantity <= 0:
                continue
            orders.append(
                self._order(
                    instrument=instrument,
                    side=OrderSide.SELL,
                    quantity=quantity,
                    session_date=session_date,
                    suffix=suffix,
                )
            )
        return tuple(orders)

    def _order(
        self,
        *,
        instrument: str,
        side: OrderSide,
        quantity: int,
        session_date: date,
        suffix: str,
    ) -> OrderIntent:
        self._order_sequence += 1
        return _order(
            order_id=(
                f"dynamic-{self._order_sequence:06d}-{suffix}"
            ),
            instrument=instrument,
            side=side,
            quantity=quantity,
            session_date=session_date,
        )


def _liquidity_quantity(
    market: MarketState,
    *,
    participation: Decimal,
    step: int,
) -> int:
    maximum = min(
        market.rules.max_order_quantity,
        int(
            (Decimal(market.bar.volume) * participation).to_integral_value(
                rounding=ROUND_FLOOR
            )
        ),
    )
    return maximum // step * step


def _can_trade(side: OrderSide, market: MarketState) -> bool:
    if market.suspended or market.bar.volume <= 0:
        return False
    exact = market.daily_price_limit
    if exact is not None:
        return not (
            (
                side is OrderSide.BUY
                and market.bar.open_price >= exact.up_limit
                and market.bar.low_price >= exact.up_limit
            )
            or (
                side is OrderSide.SELL
                and market.bar.open_price <= exact.down_limit
                and market.bar.high_price <= exact.down_limit
            )
        )
    rate = market.rules.price_limit.rate
    if rate is None:
        return True
    tick = market.rules.price_tick
    direction = Decimal("1") if side is OrderSide.BUY else Decimal("-1")
    limit = (
        market.bar.pre_close * (Decimal("1") + direction * rate)
    ).quantize(tick, rounding=ROUND_HALF_UP)
    return not (
        (
            side is OrderSide.BUY
            and market.bar.open_price >= limit
            and market.bar.low_price >= limit
        )
        or (
            side is OrderSide.SELL
            and market.bar.open_price <= limit
            and market.bar.high_price <= limit
        )
    )
