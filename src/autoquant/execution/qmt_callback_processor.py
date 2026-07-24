from __future__ import annotations

from pydantic import SecretStr

from autoquant.errors import BrokerStateUnknownError
from autoquant.execution.qmt_callback_inbox import QmtCallbackInboxEvent
from autoquant.execution.qmt_callback_store import PostgresQmtCallbackInbox
from autoquant.execution.qmt_canary_contract import QmtOrderCorrelation
from autoquant.execution.qmt_canary_store import PostgresQmtCanaryOrderLedger
from autoquant.execution.qmt_gateway import QmtCallbackKind


class QmtPersistedAsyncResponseBinder:
    """Bind one durable async response to its staged identity without broker calls."""

    def __init__(
        self,
        *,
        inbox: PostgresQmtCallbackInbox,
        ledger: PostgresQmtCanaryOrderLedger,
    ) -> None:
        if not isinstance(inbox, PostgresQmtCallbackInbox):
            raise TypeError("inbox must be PostgresQmtCallbackInbox")
        if not isinstance(ledger, PostgresQmtCanaryOrderLedger):
            raise TypeError("ledger must be PostgresQmtCanaryOrderLedger")
        self._inbox = inbox
        self._ledger = ledger

    async def bind(
        self,
        event: QmtCallbackInboxEvent,
        *,
        lease_token: SecretStr,
    ) -> QmtOrderCorrelation:
        if not isinstance(event, QmtCallbackInboxEvent):
            raise TypeError("event must be QmtCallbackInboxEvent")
        if event.callback.kind is not QmtCallbackKind.ASYNC_ORDER_RESPONSE:
            raise BrokerStateUnknownError("QMT order binding requires an async response callback")
        durable_events = await self._inbox.replay_current(
            account_id=event.callback.account_id,
            gateway_holder_id=event.gateway_holder_id,
            qmt_session_id=event.qmt_session_id,
            qmt_lease_generation=event.qmt_lease_generation,
            lease_token=lease_token,
        )
        matches = tuple(item for item in durable_events if item.event_hash == event.event_hash)
        if len(matches) != 1 or matches[0] != event:
            raise BrokerStateUnknownError("QMT async response must be durable before order binding")
        payload = event.callback.redacted_payload
        async_request_id = payload["seq"]
        broker_order_id = payload["order_id"]
        broker_order_remark = payload["order_remark"]
        if (
            not isinstance(async_request_id, int)
            or isinstance(async_request_id, bool)
            or not isinstance(broker_order_id, int)
            or isinstance(broker_order_id, bool)
            or not isinstance(broker_order_remark, str)
        ):
            raise BrokerStateUnknownError("durable QMT async response has an invalid identity")
        return await self._ledger.bind(
            account_id=event.callback.account_id,
            gateway_holder_id=event.gateway_holder_id,
            qmt_session_id=event.qmt_session_id,
            qmt_lease_generation=event.qmt_lease_generation,
            lease_token=lease_token,
            async_request_id=async_request_id,
            broker_order_id=str(broker_order_id),
            broker_order_remark=broker_order_remark,
            bound_at=event.callback.received_at,
        )
