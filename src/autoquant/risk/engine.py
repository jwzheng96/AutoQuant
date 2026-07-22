from __future__ import annotations

from decimal import Decimal

from autoquant.backtest.models import OrderSide
from autoquant.clock import to_utc
from autoquant.risk.models import (
    ExecutionMode,
    RiskCode,
    RiskDecision,
    RiskDecisionState,
    RiskEvaluationInput,
)


class PreTradeRiskEngine:
    """Deterministic pre-trade gate; live mode is hard-locked in this phase."""

    def evaluate(self, request: RiskEvaluationInput) -> RiskDecision:
        if not isinstance(request, RiskEvaluationInput):
            raise TypeError("request must be RiskEvaluationInput")
        now = to_utc(request.now, name="risk evaluation time")
        policy = request.policy
        account = request.account
        order = request.order
        quote = request.quote
        rules = request.rules
        if quote.instrument != order.instrument or rules.instrument != order.instrument:
            raise ValueError("quote, rules, and order instrument must match")

        estimated_price = (
            order.limit_price
            if order.limit_price is not None
            else quote.ask_price
            if order.side is OrderSide.BUY
            else quote.bid_price
        )
        notional = estimated_price * order.quantity
        current_position = next(
            (
                position
                for position in account.positions
                if position.instrument == order.instrument
            ),
            None,
        )
        current_position_value = (
            Decimal("0") if current_position is None else current_position.market_value
        )
        if order.side is OrderSide.BUY:
            projected_cash = account.cash - notional * (
                Decimal("1") + policy.fee_buffer_rate
            )
            projected_gross = account.gross_exposure + notional
            projected_position_value = current_position_value + notional
        else:
            projected_cash = account.cash + notional * (
                Decimal("1") - policy.fee_buffer_rate
            )
            projected_position_value = max(
                Decimal("0"), current_position_value - notional
            )
            projected_gross = max(
                Decimal("0"), account.gross_exposure - min(notional, current_position_value)
            )
        projected_position_weight = projected_position_value / account.equity
        projected_daily_turnover = (
            account.daily_turnover + notional
        ) / account.day_start_equity

        violations: list[RiskCode] = []
        self._add(violations, request.mode is ExecutionMode.LIVE, RiskCode.LIVE_MODE_LOCKED)
        self._add(violations, account.kill_switch, RiskCode.KILL_SWITCH_ACTIVE)
        self._add(
            violations,
            not account.reconciled,
            RiskCode.RECONCILIATION_UNHEALTHY,
        )
        self._add(
            violations,
            now < account.as_of or now - account.as_of > policy.max_account_state_age,
            RiskCode.STALE_ACCOUNT_STATE,
        )
        self._add(
            violations,
            now < quote.as_of or now - quote.as_of > policy.max_quote_age,
            RiskCode.STALE_MARKET_DATA,
        )
        self._add(violations, not quote.market_open, RiskCode.MARKET_CLOSED)
        self._add(
            violations,
            order.instrument not in policy.allowed_instruments,
            RiskCode.INSTRUMENT_NOT_ALLOWED,
        )
        self._add(
            violations,
            order.client_order_id in account.seen_client_order_ids,
            RiskCode.DUPLICATE_ORDER,
        )
        self._add(violations, order.submitted_at > now, RiskCode.FUTURE_ORDER)
        self._add(
            violations,
            account.open_order_count >= policy.max_open_orders,
            RiskCode.OPEN_ORDER_LIMIT,
        )
        invalid_quantity = (
            order.quantity > rules.max_order_quantity
            or (
                order.side is OrderSide.BUY
                and (
                    order.quantity < rules.buy_minimum
                    or (order.quantity - rules.buy_minimum) % rules.buy_step != 0
                )
            )
            or (
                order.side is OrderSide.SELL
                and current_position is not None
                and order.quantity != current_position.total_quantity
                and order.quantity % rules.sell_step != 0
            )
        )
        self._add(violations, invalid_quantity, RiskCode.INVALID_QUANTITY)
        self._add(
            violations,
            order.side is OrderSide.SELL and current_position is None,
            RiskCode.NO_POSITION,
        )
        self._add(
            violations,
            order.side is OrderSide.SELL
            and current_position is not None
            and order.quantity > current_position.sellable_quantity,
            RiskCode.NOT_SELLABLE,
        )
        self._add(
            violations,
            notional > policy.max_order_notional,
            RiskCode.ORDER_NOTIONAL_LIMIT,
        )
        self._add(
            violations,
            order.side is OrderSide.BUY and projected_cash < 0,
            RiskCode.INSUFFICIENT_CASH,
        )
        self._add(
            violations,
            projected_position_weight > policy.max_position_weight,
            RiskCode.POSITION_WEIGHT_LIMIT,
        )
        self._add(
            violations,
            projected_gross / account.equity > policy.max_gross_exposure,
            RiskCode.GROSS_EXPOSURE_LIMIT,
        )
        self._add(
            violations,
            projected_daily_turnover > policy.max_daily_turnover,
            RiskCode.DAILY_TURNOVER_LIMIT,
        )
        daily_loss = max(
            Decimal("0"),
            (account.day_start_equity - account.equity) / account.day_start_equity,
        )
        drawdown = max(
            Decimal("0"),
            (account.peak_equity - account.equity) / account.peak_equity,
        )
        self._add(
            violations,
            daily_loss >= policy.max_daily_loss,
            RiskCode.DAILY_LOSS_LIMIT,
        )
        self._add(
            violations,
            drawdown >= policy.max_drawdown,
            RiskCode.DRAWDOWN_LIMIT,
        )
        deviation_bps = (
            (estimated_price - quote.last_price).copy_abs()
            / quote.last_price
            * Decimal("10000")
        )
        self._add(
            violations,
            deviation_bps > policy.max_price_deviation_bps,
            RiskCode.PRICE_DEVIATION_LIMIT,
        )

        return RiskDecision(
            account_id=account.account_id,
            mode=request.mode,
            order=order,
            evaluated_at=now,
            state=(
                RiskDecisionState.ACCEPTED
                if not violations
                else RiskDecisionState.REJECTED
            ),
            violations=tuple(violations),
            policy_hash=policy.policy_hash,
            account_state_hash=account.state_hash,
            quote_hash=quote.quote_hash,
            rules_version=rules.rule_version,
            estimated_price=estimated_price,
            order_notional=notional,
            projected_cash=projected_cash,
            projected_gross_exposure=projected_gross,
            projected_position_weight=projected_position_weight,
            projected_daily_turnover=projected_daily_turnover,
        )

    @staticmethod
    def _add(values: list[RiskCode], condition: bool, code: RiskCode) -> None:
        if condition:
            values.append(code)
