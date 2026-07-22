from __future__ import annotations

from autoquant.execution.models import (
    TERMINAL_STATES,
    BrokerOrderUpdate,
    PaperOrderEvent,
    PaperOrderProjection,
    PaperOrderState,
    PaperOrderTransition,
)

_ALLOWED: dict[PaperOrderState, frozenset[PaperOrderState]] = {
    PaperOrderState.APPROVED: frozenset(
        {
            PaperOrderState.SUBMITTED,
            PaperOrderState.REJECTED,
            PaperOrderState.UNKNOWN,
        }
    ),
    PaperOrderState.SUBMITTED: frozenset(
        {
            PaperOrderState.PARTIALLY_FILLED,
            PaperOrderState.FILLED,
            PaperOrderState.CANCELLED,
            PaperOrderState.REJECTED,
            PaperOrderState.UNKNOWN,
        }
    ),
    PaperOrderState.PARTIALLY_FILLED: frozenset(
        {
            PaperOrderState.PARTIALLY_FILLED,
            PaperOrderState.FILLED,
            PaperOrderState.CANCELLED,
            PaperOrderState.UNKNOWN,
        }
    ),
    PaperOrderState.UNKNOWN: frozenset(
        {
            PaperOrderState.SUBMITTED,
            PaperOrderState.PARTIALLY_FILLED,
            PaperOrderState.FILLED,
            PaperOrderState.CANCELLED,
            PaperOrderState.REJECTED,
            PaperOrderState.UNKNOWN,
        }
    ),
    PaperOrderState.FILLED: frozenset(),
    PaperOrderState.CANCELLED: frozenset(),
    PaperOrderState.REJECTED: frozenset(),
}


class PaperOrderStateMachine:
    """Apply ordered broker facts without guessing through gaps or conflicts."""

    def apply(
        self,
        projection: PaperOrderProjection,
        update: BrokerOrderUpdate,
    ) -> PaperOrderTransition:
        order = projection.order
        if update.account_id != order.account_id:
            raise ValueError("broker update account does not match order")
        if update.client_order_id != order.client_order_id:
            raise ValueError("broker update client_order_id does not match order")
        if (
            projection.broker_order_id is not None
            and update.broker_order_id != projection.broker_order_id
        ):
            raise ValueError("broker_order_id cannot change")
        if update.broker_sequence < projection.last_broker_sequence:
            raise ValueError("out-of-order broker update")
        if update.broker_sequence == projection.last_broker_sequence:
            if update.update_hash != projection.last_update_hash:
                raise ValueError("broker sequence already belongs to another update")
            return PaperOrderTransition(projection=projection, event=None, applied=False)
        if projection.state in TERMINAL_STATES:
            raise ValueError("terminal paper order state cannot change")
        if update.state not in _ALLOWED[projection.state]:
            raise ValueError("invalid paper order state transition")
        if update.occurred_at < projection.updated_at:
            raise ValueError("broker update time cannot move backwards")
        if update.cumulative_filled_quantity < projection.cumulative_filled_quantity:
            raise ValueError("cumulative filled quantity cannot decrease")
        if update.cumulative_filled_quantity > order.quantity:
            raise ValueError("cumulative filled quantity cannot exceed order quantity")
        if update.state is PaperOrderState.SUBMITTED:
            if update.cumulative_filled_quantity != 0:
                raise ValueError("submitted order cannot contain fills")
        elif update.state is PaperOrderState.PARTIALLY_FILLED:
            if not 0 < update.cumulative_filled_quantity < order.quantity:
                raise ValueError("partial fill must be between zero and order quantity")
        elif update.state is PaperOrderState.FILLED:
            if update.cumulative_filled_quantity != order.quantity:
                raise ValueError("filled order must equal requested quantity")
        elif update.state is PaperOrderState.REJECTED:
            if update.cumulative_filled_quantity != 0:
                raise ValueError("rejected order cannot contain fills")
        elif update.state is PaperOrderState.CANCELLED:
            if update.cumulative_filled_quantity == order.quantity:
                raise ValueError("fully filled order cannot be cancelled")

        provisional = PaperOrderProjection(
            order=order,
            state=update.state,
            broker_order_id=update.broker_order_id,
            cumulative_filled_quantity=update.cumulative_filled_quantity,
            average_fill_price=update.average_fill_price,
            last_broker_sequence=update.broker_sequence,
            last_update_hash=update.update_hash,
            last_event_hash=projection.last_event_hash,
            updated_at=update.occurred_at,
            version=projection.version + 1,
        )
        event = PaperOrderEvent(
            sequence=provisional.version,
            client_order_id=order.client_order_id,
            previous_hash=projection.last_event_hash,
            update_hash=update.update_hash,
            resulting_state=update.state,
            projection_hash=provisional.projection_hash,
        )
        result = PaperOrderProjection(
            order=order,
            state=provisional.state,
            broker_order_id=provisional.broker_order_id,
            cumulative_filled_quantity=provisional.cumulative_filled_quantity,
            average_fill_price=provisional.average_fill_price,
            last_broker_sequence=provisional.last_broker_sequence,
            last_update_hash=provisional.last_update_hash,
            last_event_hash=event.event_hash,
            updated_at=provisional.updated_at,
            version=provisional.version,
        )
        return PaperOrderTransition(projection=result, event=event, applied=True)
