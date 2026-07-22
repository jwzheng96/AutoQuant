from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from autoquant.backtest.models import OrderSide
from autoquant.backtest.rules import FeeSchedule
from autoquant.clock import to_utc
from autoquant.data.models import _canonical_hash, _decimal_text, _require_nonblank
from autoquant.execution.models import PaperOrderHistory, PaperOrderState
from autoquant.execution.reconciliation import (
    AccountPosition,
    ExecutionAccountSnapshot,
)

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_TERMINAL = {
    PaperOrderState.FILLED,
    PaperOrderState.CANCELLED,
    PaperOrderState.REJECTED,
}


@dataclass(slots=True)
class _Lot:
    session_date: date
    quantity: int


class PaperAccountProjector:
    """Rebuild a cash account independently from one immutable order history source."""

    def __init__(self, *, fees: FeeSchedule | None = None) -> None:
        self._fees = fees or FeeSchedule()
        self.version = f"paper-account-projection-v1:{self._fees.version}"

    def project(
        self,
        *,
        account_id: str,
        initial_cash: Decimal,
        histories: tuple[PaperOrderHistory, ...],
        marks: dict[str, Decimal],
        as_of: datetime,
    ) -> ExecutionAccountSnapshot:
        _require_nonblank(account_id, name="account_id")
        cutoff = to_utc(as_of, name="account projection as_of")
        if (
            not isinstance(initial_cash, Decimal)
            or not initial_cash.is_finite()
            or initial_cash <= 0
        ):
            raise ValueError("initial_cash must be a positive finite Decimal")
        histories = tuple(
            sorted(histories, key=lambda item: item.order.client_order_id)
        )
        client_ids = tuple(history.order.client_order_id for history in histories)
        if len(set(client_ids)) != len(client_ids):
            raise ValueError("account histories contain duplicate client_order_id")
        if any(history.order.account_id != account_id for history in histories):
            raise ValueError("account history belongs to another account")
        if any(history.order.approved_at > cutoff for history in histories):
            raise ValueError("account projection cannot include future approved orders")
        for instrument, mark in marks.items():
            _require_nonblank(instrument, name="mark instrument")
            if not isinstance(mark, Decimal) or not mark.is_finite() or mark <= 0:
                raise ValueError("marks must contain positive finite Decimal prices")

        facts = sorted(
            (
                update.occurred_at,
                history.order.client_order_id,
                update.broker_sequence,
                history,
                update,
            )
            for history in histories
            for update in history.updates
        )
        if facts and facts[-1][0] > cutoff:
            raise ValueError("account projection cannot include future broker facts")

        cash = initial_cash
        lots: dict[str, list[_Lot]] = {}
        previous_quantity: dict[str, int] = {}
        previous_gross: dict[str, Decimal] = {}
        previous_commission: dict[str, Decimal] = {}
        for _, client_order_id, _, history, update in facts:
            cumulative = update.cumulative_filled_quantity
            prior_quantity = previous_quantity.get(client_order_id, 0)
            if cumulative == prior_quantity:
                continue
            if cumulative < prior_quantity or update.average_fill_price is None:
                raise ValueError("account fill facts are inconsistent")
            cumulative_gross = update.average_fill_price * cumulative
            prior_gross = previous_gross.get(client_order_id, Decimal("0"))
            fill_quantity = cumulative - prior_quantity
            fill_gross = cumulative_gross - prior_gross
            if fill_quantity <= 0 or fill_gross <= 0:
                raise ValueError("incremental fill must be positive")
            session_date = update.occurred_at.astimezone(_SHANGHAI).date()
            cumulative_fee = self._fees.calculate(
                side=history.order.side,
                gross_amount=cumulative_gross,
                session_date=session_date,
            )
            incremental_fee = self._fees.calculate(
                side=history.order.side,
                gross_amount=fill_gross,
                session_date=session_date,
            )
            fill_commission = cumulative_fee.commission - previous_commission.get(
                client_order_id, Decimal("0")
            )
            if fill_commission < 0:
                raise ValueError("cumulative order commission cannot decrease")
            fill_fees = (
                fill_commission
                + incremental_fee.stamp_duty
                + incremental_fee.transfer_fee
            )
            instrument_lots = lots.setdefault(history.order.instrument, [])
            if history.order.side is OrderSide.BUY:
                cash -= fill_gross + fill_fees
                if cash < 0:
                    raise ValueError("projected account cash became negative")
                instrument_lots.append(_Lot(session_date, fill_quantity))
            else:
                self._consume_sellable(
                    instrument_lots,
                    quantity=fill_quantity,
                    session_date=session_date,
                )
                cash += fill_gross - fill_fees
            previous_quantity[client_order_id] = cumulative
            previous_gross[client_order_id] = cumulative_gross
            previous_commission[client_order_id] = cumulative_fee.commission

        position_items: list[AccountPosition] = []
        cutoff_session = cutoff.astimezone(_SHANGHAI).date()
        for instrument in sorted(lots):
            instrument_lots = lots[instrument]
            total_quantity = sum(item.quantity for item in instrument_lots)
            if total_quantity == 0:
                continue
            position_mark = marks.get(instrument)
            if position_mark is None:
                raise ValueError("every open position requires a current mark")
            sellable_quantity = sum(
                item.quantity
                for item in instrument_lots
                if item.session_date < cutoff_session
            )
            position_items.append(
                AccountPosition(
                    instrument=instrument,
                    total_quantity=total_quantity,
                    sellable_quantity=sellable_quantity,
                    market_value=position_mark * total_quantity,
                )
            )
        market_value = sum(
            (position.market_value for position in position_items), Decimal("0")
        )
        open_orders = tuple(
            sorted(
                history.order.client_order_id
                for history in histories
                if history.state not in _TERMINAL
            )
        )
        evidence_hash = _canonical_hash(
            {
                "account_id": account_id,
                "as_of": cutoff.isoformat(timespec="microseconds"),
                "histories": [
                    {
                        "order_hash": history.order.order_hash,
                        "state": history.state.value,
                        "update_hashes": [
                            update.update_hash for update in history.updates
                        ],
                    }
                    for history in histories
                ],
                "initial_cash": _decimal_text(initial_cash),
                "marks": {
                    instrument: _decimal_text(mark)
                    for instrument, mark in sorted(marks.items())
                },
                "projection_version": self.version,
            }
        )
        return ExecutionAccountSnapshot(
            account_id=account_id,
            as_of=cutoff,
            cash=cash,
            equity=cash + market_value,
            positions=tuple(position_items),
            open_client_order_ids=open_orders,
            projection_version=self.version,
            evidence_hash=evidence_hash,
        )

    @staticmethod
    def _consume_sellable(
        lots: list[_Lot], *, quantity: int, session_date: date
    ) -> None:
        remaining = quantity
        for lot in lots:
            if lot.session_date >= session_date or lot.quantity == 0:
                continue
            consumed = min(lot.quantity, remaining)
            lot.quantity -= consumed
            remaining -= consumed
            if remaining == 0:
                break
        if remaining:
            raise ValueError("sell fill exceeds T+1 sellable quantity")
