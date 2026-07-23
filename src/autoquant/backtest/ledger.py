from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal

from autoquant.backtest.models import (
    AccountSnapshot,
    ExecutionReport,
    ExecutionState,
    FeeBreakdown,
    LedgerEvent,
    MarketState,
    OrderIntent,
    OrderSide,
    PositionSnapshot,
    RejectionCode,
)
from autoquant.backtest.rules import FeeSchedule
from autoquant.clock import SHANGHAI
from autoquant.data.models import _decimal_text

_GENESIS_HASH = "0" * 64


@dataclass(frozen=True, slots=True)
class ExecutionModel:
    slippage_bps: Decimal = Decimal("5")
    max_volume_participation: Decimal = Decimal("0.10")
    version: str = "daily-open-conservative-v1"

    def __post_init__(self) -> None:
        if (
            not isinstance(self.slippage_bps, Decimal)
            or not self.slippage_bps.is_finite()
            or self.slippage_bps < 0
            or self.slippage_bps > 1_000
        ):
            raise ValueError("slippage_bps must be between 0 and 1000")
        if (
            not isinstance(self.max_volume_participation, Decimal)
            or not self.max_volume_participation.is_finite()
            or self.max_volume_participation <= 0
            or self.max_volume_participation > 1
        ):
            raise ValueError("max_volume_participation must be between zero and one")
        if not self.version.strip():
            raise ValueError("execution model version cannot be empty")


@dataclass(slots=True)
class _Position:
    total_quantity: int
    sellable_quantity: int
    average_cost: Decimal


class PortfolioLedger:
    """Single-threaded deterministic cash-account ledger for research runs."""

    def __init__(
        self,
        *,
        initial_cash: Decimal,
        fees: FeeSchedule,
        execution: ExecutionModel,
    ) -> None:
        if (
            not isinstance(initial_cash, Decimal)
            or not initial_cash.is_finite()
            or initial_cash <= 0
        ):
            raise ValueError("initial_cash must be a positive finite Decimal")
        self._initial_cash = initial_cash.quantize(Decimal("0.01"))
        self._cash = self._initial_cash
        self._fees = fees
        self._execution = execution
        self._positions: dict[str, _Position] = {}
        self._events: list[LedgerEvent] = []
        self._orders: dict[str, OrderIntent] = {}
        self._reports: dict[str, ExecutionReport] = {}
        self._session_date: date | None = None
        self._total_fees = Decimal("0")
        self._gross_turnover = Decimal("0")

    @property
    def initial_cash(self) -> Decimal:
        return self._initial_cash

    @property
    def cash(self) -> Decimal:
        return self._cash

    @property
    def total_fees(self) -> Decimal:
        return self._total_fees

    @property
    def gross_turnover(self) -> Decimal:
        return self._gross_turnover

    @property
    def events(self) -> tuple[LedgerEvent, ...]:
        return tuple(self._events)

    @property
    def ledger_hash(self) -> str:
        return self._events[-1].event_hash if self._events else _GENESIS_HASH

    def start_session(self, session_date: date) -> None:
        if self._session_date is not None and session_date <= self._session_date:
            raise ValueError("sessions must be strictly increasing")
        for position in self._positions.values():
            position.sellable_quantity = position.total_quantity
        self._session_date = session_date

    def execute(self, order: OrderIntent, market: MarketState) -> ExecutionReport:
        previous = self._orders.get(order.client_order_id)
        if previous is not None:
            if previous == order:
                return self._reports[order.client_order_id]
            return self._reject(order, RejectionCode.DUPLICATE_ORDER)
        if self._session_date != order.session_date:
            return self._reject(order, RejectionCode.OUTSIDE_SESSION)
        self._orders[order.client_order_id] = order
        if market.bar.instrument != order.instrument:
            return self._remember_rejection(order, RejectionCode.UNKNOWN_INSTRUMENT)
        if market.bar.session_date != order.session_date:
            return self._remember_rejection(order, RejectionCode.OUTSIDE_SESSION)
        session_open = datetime.combine(
            order.session_date, time(9, 30), tzinfo=SHANGHAI
        ).astimezone(UTC)
        if order.submitted_at >= session_open:
            return self._remember_rejection(order, RejectionCode.OUTSIDE_SESSION)
        if market.suspended:
            return self._remember_rejection(order, RejectionCode.SUSPENDED)
        if market.bar.volume <= 0:
            return self._remember_rejection(order, RejectionCode.PRICE_UNAVAILABLE)
        rules = market.rules
        if order.quantity > rules.max_order_quantity:
            return self._remember_rejection(order, RejectionCode.LIQUIDITY_LIMIT)
        maximum_liquid_quantity = int(
            Decimal(market.bar.volume) * self._execution.max_volume_participation
        )
        if order.quantity > maximum_liquid_quantity:
            return self._remember_rejection(order, RejectionCode.LIQUIDITY_LIMIT)
        if self._locked_at_limit(order.side, market):
            code = (
                RejectionCode.LIMIT_UP_LOCKED
                if order.side is OrderSide.BUY
                else RejectionCode.LIMIT_DOWN_LOCKED
            )
            return self._remember_rejection(order, code)
        if order.side is OrderSide.BUY:
            quantity_error = self._validate_buy_quantity(order, market)
            if quantity_error is not None:
                return self._remember_rejection(order, quantity_error)
        else:
            quantity_error = self._validate_sell_quantity(order, market)
            if quantity_error is not None:
                return self._remember_rejection(order, quantity_error)

        fill_price = self._fill_price(order.side, market)
        gross = fill_price * order.quantity
        fees = self._fees.calculate(
            side=order.side,
            gross_amount=gross,
            session_date=order.session_date,
        )
        if order.side is OrderSide.BUY and self._cash < gross + fees.total:
            return self._remember_rejection(order, RejectionCode.CASH_INSUFFICIENT)
        self._apply_fill(
            order,
            fill_price=fill_price,
            gross=gross,
            fees=fees,
            t_plus_one=rules.t_plus_one,
        )
        event = self._append_event(
            event_type="order_filled",
            order=order,
            payload={
                "commission": _decimal_text(fees.commission),
                "fill_price": _decimal_text(fill_price),
                "gross_amount": _decimal_text(gross),
                "quantity": str(order.quantity),
                "side": order.side.value,
                "stamp_duty": _decimal_text(fees.stamp_duty),
                "transfer_fee": _decimal_text(fees.transfer_fee),
            },
        )
        report = ExecutionReport(
            client_order_id=order.client_order_id,
            instrument=order.instrument,
            side=order.side,
            requested_quantity=order.quantity,
            state=ExecutionState.FILLED,
            session_date=order.session_date,
            filled_quantity=order.quantity,
            fill_price=fill_price,
            gross_amount=gross,
            fees=fees,
            ledger_hash=event.event_hash,
        )
        self._reports[order.client_order_id] = report
        return report

    def snapshot(
        self,
        markets: tuple[MarketState, ...],
        *,
        valuation_prices: Mapping[str, Decimal] | None = None,
    ) -> AccountSnapshot:
        if self._session_date is None:
            raise ValueError("a session must be started before snapshot")
        prices = dict(valuation_prices or {})
        if any(
            not isinstance(instrument, str)
            or not instrument.strip()
            or not isinstance(price, Decimal)
            or not price.is_finite()
            or price <= 0
            for instrument, price in prices.items()
        ):
            raise ValueError(
                "valuation prices must map instruments to positive Decimals"
            )
        prices.update({
            market.bar.instrument: market.bar.close_price for market in markets
        })
        positions: list[PositionSnapshot] = []
        for instrument, position in sorted(self._positions.items()):
            if position.total_quantity == 0:
                continue
            try:
                market_price = prices[instrument]
            except KeyError:
                raise ValueError(
                    f"market price is missing for held instrument {instrument}"
                ) from None
            market_value = market_price * position.total_quantity
            positions.append(
                PositionSnapshot(
                    instrument=instrument,
                    total_quantity=position.total_quantity,
                    sellable_quantity=position.sellable_quantity,
                    average_cost=position.average_cost,
                    market_price=market_price,
                    market_value=market_value,
                    unrealized_pnl=(market_price - position.average_cost)
                    * position.total_quantity,
                )
            )
        total_market_value = sum(
            (position.market_value for position in positions), Decimal("0")
        )
        return AccountSnapshot(
            session_date=self._session_date,
            cash=self._cash,
            market_value=total_market_value,
            equity=self._cash + total_market_value,
            positions=tuple(positions),
            ledger_hash=self.ledger_hash,
        )

    def _validate_buy_quantity(
        self, order: OrderIntent, market: MarketState
    ) -> RejectionCode | None:
        rules = market.rules
        if (
            order.quantity < rules.buy_minimum
            or (order.quantity - rules.buy_minimum) % rules.buy_step != 0
        ):
            return RejectionCode.INVALID_BUY_QUANTITY
        return None

    def _validate_sell_quantity(
        self, order: OrderIntent, market: MarketState
    ) -> RejectionCode | None:
        position = self._positions.get(order.instrument)
        if position is None or position.total_quantity == 0:
            return RejectionCode.NO_POSITION
        if order.quantity > position.sellable_quantity:
            return RejectionCode.NOT_SELLABLE
        if (
            order.quantity != position.total_quantity
            and order.quantity % market.rules.sell_step != 0
        ):
            return RejectionCode.INVALID_SELL_QUANTITY
        return None

    @staticmethod
    def _locked_at_limit(side: OrderSide, market: MarketState) -> bool:
        exact = market.daily_price_limit
        if exact is not None:
            if side is OrderSide.BUY:
                return (
                    market.bar.open_price >= exact.up_limit
                    and market.bar.low_price >= exact.up_limit
                )
            return (
                market.bar.open_price <= exact.down_limit
                and market.bar.high_price <= exact.down_limit
            )
        rate = market.rules.price_limit.rate
        if rate is None:
            return False
        tick = market.rules.price_tick
        if side is OrderSide.BUY:
            limit = (market.bar.pre_close * (Decimal("1") + rate)).quantize(
                tick, rounding=ROUND_HALF_UP
            )
            return market.bar.open_price >= limit and market.bar.low_price >= limit
        limit = (market.bar.pre_close * (Decimal("1") - rate)).quantize(
            tick, rounding=ROUND_HALF_UP
        )
        return market.bar.open_price <= limit and market.bar.high_price <= limit

    def _fill_price(self, side: OrderSide, market: MarketState) -> Decimal:
        direction = Decimal("1") if side is OrderSide.BUY else Decimal("-1")
        raw = market.bar.open_price * (
            Decimal("1") + direction * self._execution.slippage_bps / Decimal("10000")
        )
        bounded = min(max(raw, market.bar.low_price), market.bar.high_price)
        rounding = ROUND_CEILING if side is OrderSide.BUY else ROUND_FLOOR
        return bounded.quantize(market.rules.price_tick, rounding=rounding)

    def _apply_fill(
        self,
        order: OrderIntent,
        *,
        fill_price: Decimal,
        gross: Decimal,
        fees: FeeBreakdown,
        t_plus_one: bool,
    ) -> None:
        position = self._positions.setdefault(
            order.instrument,
            _Position(total_quantity=0, sellable_quantity=0, average_cost=Decimal("0")),
        )
        if order.side is OrderSide.BUY:
            old_cost = position.average_cost * position.total_quantity
            new_quantity = position.total_quantity + order.quantity
            position.average_cost = (old_cost + gross + fees.total) / new_quantity
            position.total_quantity = new_quantity
            if not t_plus_one:
                position.sellable_quantity += order.quantity
            self._cash -= gross + fees.total
        else:
            position.total_quantity -= order.quantity
            position.sellable_quantity -= order.quantity
            self._cash += gross - fees.total
            if position.total_quantity == 0:
                position.average_cost = Decimal("0")
        self._cash = self._cash.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if self._cash < 0:
            raise RuntimeError("ledger invariant violated: cash became negative")
        self._total_fees += fees.total
        self._gross_turnover += gross

    def _remember_rejection(
        self, order: OrderIntent, code: RejectionCode
    ) -> ExecutionReport:
        report = self._reject(order, code)
        self._reports[order.client_order_id] = report
        return report

    def _reject(self, order: OrderIntent, code: RejectionCode) -> ExecutionReport:
        event = self._append_event(
            event_type="order_rejected",
            order=order,
            payload={
                "quantity": str(order.quantity),
                "reason": code.value,
                "side": order.side.value,
            },
        )
        return ExecutionReport(
            client_order_id=order.client_order_id,
            instrument=order.instrument,
            side=order.side,
            requested_quantity=order.quantity,
            state=ExecutionState.REJECTED,
            session_date=order.session_date,
            rejection_code=code,
            ledger_hash=event.event_hash,
        )

    def _append_event(
        self,
        *,
        event_type: str,
        order: OrderIntent,
        payload: dict[str, str],
    ) -> LedgerEvent:
        normalized_payload = {"instrument": order.instrument, **payload}
        event = LedgerEvent(
            sequence=len(self._events) + 1,
            event_type=event_type,
            session_date=order.session_date,
            client_order_id=order.client_order_id,
            payload=tuple(sorted(normalized_payload.items())),
            previous_hash=self.ledger_hash,
        )
        self._events.append(event)
        return event
