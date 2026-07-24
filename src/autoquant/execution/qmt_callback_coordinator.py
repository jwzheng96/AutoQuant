from __future__ import annotations

import re
from asyncio import Lock
from dataclasses import dataclass

from pydantic import SecretStr

from autoquant.errors import BrokerStateUnknownError
from autoquant.execution.qmt_callback_inbox import (
    QmtCallbackInboxEvent,
    sanitize_qmt_callback,
)
from autoquant.execution.qmt_callback_processor import (
    QmtPersistedAsyncResponseBinder,
)
from autoquant.execution.qmt_callback_store import PostgresQmtCallbackInbox
from autoquant.execution.qmt_canary_contract import QmtOrderCorrelation
from autoquant.execution.qmt_gateway import (
    QmtCallbackBuffer,
    QmtCallbackKind,
)

_HOLDER_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")


@dataclass(frozen=True, slots=True)
class QmtCallbackCoordinationResult:
    persisted_events: tuple[QmtCallbackInboxEvent, ...]
    async_bindings: tuple[QmtOrderCorrelation, ...]

    @property
    def broker_mutation_allowed(self) -> bool:
        return False


class QmtCallbackPersistenceCoordinator:
    """Persist reserved callback batches before any internal interpretation."""

    def __init__(
        self,
        *,
        buffer: QmtCallbackBuffer,
        inbox: PostgresQmtCallbackInbox,
        expected_broker_account_id: str,
        logical_account_id: str,
        gateway_holder_id: str,
        qmt_session_id: int,
        qmt_lease_generation: int,
        async_response_binder: QmtPersistedAsyncResponseBinder | None = None,
    ) -> None:
        if not isinstance(buffer, QmtCallbackBuffer):
            raise TypeError("buffer must be QmtCallbackBuffer")
        if not isinstance(inbox, PostgresQmtCallbackInbox):
            raise TypeError("inbox must be PostgresQmtCallbackInbox")
        if (
            async_response_binder is not None
            and not isinstance(
                async_response_binder,
                QmtPersistedAsyncResponseBinder,
            )
        ):
            raise TypeError(
                "async_response_binder must be QmtPersistedAsyncResponseBinder"
            )
        _validate_identity(
            expected_broker_account_id=expected_broker_account_id,
            logical_account_id=logical_account_id,
            gateway_holder_id=gateway_holder_id,
            qmt_session_id=qmt_session_id,
            qmt_lease_generation=qmt_lease_generation,
        )
        self._buffer = buffer
        self._inbox = inbox
        self._expected_broker_account_id = expected_broker_account_id
        self._logical_account_id = logical_account_id
        self._gateway_holder_id = gateway_holder_id
        self._qmt_session_id = qmt_session_id
        self._qmt_lease_generation = qmt_lease_generation
        self._async_response_binder = async_response_binder
        self._coordination_lock = Lock()

    async def persist_and_process(
        self,
        *,
        lease_token: SecretStr,
        limit: int = 1000,
    ) -> QmtCallbackCoordinationResult:
        async with self._coordination_lock:
            return await self._persist_and_process_locked(
                lease_token=lease_token,
                limit=limit,
            )

    async def _persist_and_process_locked(
        self,
        *,
        lease_token: SecretStr,
        limit: int,
    ) -> QmtCallbackCoordinationResult:
        reservation = self._buffer.reserve_durable(limit=limit)
        if reservation is None:
            return QmtCallbackCoordinationResult((), ())
        persisted: list[QmtCallbackInboxEvent] = []
        try:
            for envelope in reservation.events:
                callback = sanitize_qmt_callback(
                    envelope,
                    expected_broker_account_id=(
                        self._expected_broker_account_id
                    ),
                    logical_account_id=self._logical_account_id,
                )
                persisted.append(
                    await self._inbox.append(
                        callback,
                        gateway_holder_id=self._gateway_holder_id,
                        qmt_session_id=self._qmt_session_id,
                        qmt_lease_generation=self._qmt_lease_generation,
                        lease_token=lease_token,
                    )
                )
            self._buffer.acknowledge_durable(
                reservation_id=reservation.reservation_id,
            )
        except BaseException:
            try:
                self._buffer.release_durable(
                    reservation_id=reservation.reservation_id,
                )
            except RuntimeError:
                pass
            raise
        events = tuple(persisted)
        return QmtCallbackCoordinationResult(
            persisted_events=events,
            async_bindings=await self._bind_async_responses(
                events,
                lease_token=lease_token,
            ),
        )

    async def replay_persisted(
        self,
        *,
        lease_token: SecretStr,
    ) -> QmtCallbackCoordinationResult:
        async with self._coordination_lock:
            return await self._replay_persisted_locked(
                lease_token=lease_token,
            )

    async def restore_before_capture(
        self,
        *,
        lease_token: SecretStr,
    ) -> QmtCallbackCoordinationResult:
        """Replay one lease generation and restore a fresh buffer cursor."""

        async with self._coordination_lock:
            result = await self._replay_persisted_locked(
                lease_token=lease_token,
            )
            local_sequence = (
                0
                if not result.persisted_events
                else result.persisted_events[-1].callback.local_sequence
            )
            self._buffer.restore_cursor(local_sequence=local_sequence)
            return result

    async def _replay_persisted_locked(
        self,
        *,
        lease_token: SecretStr,
    ) -> QmtCallbackCoordinationResult:
        events = await self._inbox.replay_current(
            account_id=self._logical_account_id,
            gateway_holder_id=self._gateway_holder_id,
            qmt_session_id=self._qmt_session_id,
            qmt_lease_generation=self._qmt_lease_generation,
            lease_token=lease_token,
        )
        return QmtCallbackCoordinationResult(
            persisted_events=events,
            async_bindings=await self._bind_async_responses(
                events,
                lease_token=lease_token,
            ),
        )

    async def _bind_async_responses(
        self,
        events: tuple[QmtCallbackInboxEvent, ...],
        *,
        lease_token: SecretStr,
    ) -> tuple[QmtOrderCorrelation, ...]:
        responses = tuple(
            event
            for event in events
            if event.callback.kind is QmtCallbackKind.ASYNC_ORDER_RESPONSE
        )
        if responses and self._async_response_binder is None:
            raise BrokerStateUnknownError(
                "durable QMT async responses require a configured binder"
            )
        if self._async_response_binder is None:
            return ()
        bindings: list[QmtOrderCorrelation] = []
        for response in responses:
            bindings.append(
                await self._async_response_binder.bind(
                    response,
                    lease_token=lease_token,
                )
            )
        return tuple(bindings)


def _validate_identity(
    *,
    expected_broker_account_id: str,
    logical_account_id: str,
    gateway_holder_id: str,
    qmt_session_id: int,
    qmt_lease_generation: int,
) -> None:
    for value, name, maximum in (
        (expected_broker_account_id, "expected_broker_account_id", 256),
        (logical_account_id, "logical_account_id", 128),
    ):
        if (
            not isinstance(value, str)
            or not value.strip()
            or value != value.strip()
            or len(value) > maximum
        ):
            raise ValueError(
                f"{name} must be trimmed, nonblank and at most {maximum} characters"
            )
    if (
        not isinstance(gateway_holder_id, str)
        or _HOLDER_ID.fullmatch(gateway_holder_id) is None
    ):
        raise ValueError(
            "gateway_holder_id must be a safe 1-64 character identifier"
        )
    if (
        not isinstance(qmt_session_id, int)
        or isinstance(qmt_session_id, bool)
        or not 1 <= qmt_session_id <= 2_147_483_647
    ):
        raise ValueError("qmt_session_id must be a positive 32-bit integer")
    if (
        not isinstance(qmt_lease_generation, int)
        or isinstance(qmt_lease_generation, bool)
        or qmt_lease_generation < 1
    ):
        raise ValueError("qmt_lease_generation must be positive")
