from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from autoquant.execution.models import ZERO_HASH
from autoquant.execution.qmt_callback_inbox import (
    QmtCallbackInboxEvent,
    QmtCallbackPersistenceReceipt,
    replay_qmt_callback_inbox,
    sanitize_qmt_callback,
)
from autoquant.execution.qmt_gateway import QmtCallbackBuffer, QmtCallbackKind

NOW = datetime(2026, 7, 24, 1, tzinfo=UTC)
BROKER_ACCOUNT = "sensitive-broker-account"
LOGICAL_ACCOUNT = "paper-main"


def _order_payload() -> dict[str, object]:
    return {
        "account_id": BROKER_ACCOUNT,
        "order_id": 88001,
        "order_remark": "AQ1234567890abcdef123456",
        "order_status": 50,
        "order_volume": 100,
        "price": 10.5,
        "side": "buy",
        "status_msg": "sensitive broker rejection details",
        "stock_code": "600000.SH",
        "traded_price": 0.0,
        "traded_volume": 0,
    }


def _sanitize(
    kind: QmtCallbackKind,
    payload: dict[str, object],
    *,
    sequence_offset: int = 0,
):
    buffer = QmtCallbackBuffer()
    for index in range(sequence_offset):
        buffer.capture(
            QmtCallbackKind.DISCONNECTED,
            {"reason": f"prior_{index}"},
            received_at=NOW,
        )
    envelope = buffer.capture(
        kind,
        payload,  # type: ignore[arg-type]
        received_at=NOW + timedelta(milliseconds=sequence_offset),
    )
    return sanitize_qmt_callback(
        envelope,
        expected_broker_account_id=BROKER_ACCOUNT,
        logical_account_id=LOGICAL_ACCOUNT,
    )


def test_order_callback_is_allowlisted_and_removes_broker_identity_and_message() -> None:
    callback = _sanitize(QmtCallbackKind.ORDER, _order_payload())
    event = QmtCallbackInboxEvent(
        callback=callback,
        gateway_holder_id="windows-qmt-canary-01",
        qmt_session_id=20260724,
        qmt_lease_generation=3,
        previous_hash=ZERO_HASH,
    )
    serialized = json.dumps(event.payload(), sort_keys=True)

    assert callback.redacted_payload["order_id"] == 88001
    assert callback.redacted_payload["order_remark"] == "AQ1234567890abcdef123456"
    assert "account_id" not in callback.redacted_payload
    assert "status_msg" not in callback.redacted_payload
    assert BROKER_ACCOUNT not in serialized
    assert "sensitive broker rejection" not in serialized
    assert event.broker_mutation_allowed is False


def test_persistence_receipt_proves_bounded_database_acceptance_without_secrets() -> None:
    callback = _sanitize(QmtCallbackKind.ORDER, _order_payload())
    event = QmtCallbackInboxEvent(
        callback=callback,
        gateway_holder_id="windows-qmt-canary-01",
        qmt_session_id=20260724,
        qmt_lease_generation=3,
        previous_hash=ZERO_HASH,
    )
    receipt = QmtCallbackPersistenceReceipt(
        event=event,
        persisted_at=NOW + timedelta(seconds=4),
    )

    serialized = json.dumps(receipt.payload(), sort_keys=True)
    assert receipt.broker_mutation_allowed is False
    assert receipt.payload()["event_hash"] == event.event_hash
    assert BROKER_ACCOUNT not in serialized
    with pytest.raises(ValueError, match="within five seconds"):
        QmtCallbackPersistenceReceipt(
            event=event,
            persisted_at=NOW + timedelta(seconds=6),
        )


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({**_order_payload(), "account_id": "another-account"}, "another broker"),
        ({**_order_payload(), "unexpected": "secret"}, "documented contract"),
        ({**_order_payload(), "order_remark": "x" * 25}, "documented limit"),
    ],
)
def test_callback_sanitizer_rejects_wrong_account_unknown_fields_or_long_remark(
    payload: dict[str, object],
    expected: str,
) -> None:
    with pytest.raises(ValueError, match=expected):
        _sanitize(QmtCallbackKind.ORDER, payload)


def test_invalid_vendor_object_reason_can_be_persisted_without_raw_fields() -> None:
    callback = _sanitize(
        QmtCallbackKind.ORDER_ERROR,
        {"reason": "invalid_order_callback"},
    )

    assert dict(callback.redacted_payload) == {"reason": "invalid_order_callback"}


@pytest.mark.parametrize(
    "updates",
    [
        {"side": "hold"},
        {"stock_code": "600000.XSHG"},
        {"order_id": 0},
        {"order_volume": -1},
        {"price": float("inf")},
    ],
)
def test_sanitized_callback_cannot_bypass_kind_contract(
    updates: dict[str, object],
) -> None:
    callback = _sanitize(QmtCallbackKind.ORDER, _order_payload())

    with pytest.raises((TypeError, ValueError)):
        replace(
            callback,
            redacted_payload={**dict(callback.redacted_payload), **updates},
        )


def test_callback_chain_replay_rejects_gap_or_wrong_previous_hash() -> None:
    first_callback = _sanitize(
        QmtCallbackKind.DISCONNECTED,
        {"reason": "xttrader_disconnected"},
    )
    second_callback = _sanitize(
        QmtCallbackKind.ACCOUNT_STATUS,
        {"account_id": BROKER_ACCOUNT, "status": 0},
        sequence_offset=1,
    )
    first = QmtCallbackInboxEvent(
        callback=first_callback,
        gateway_holder_id="windows-qmt-canary-01",
        qmt_session_id=20260724,
        qmt_lease_generation=3,
        previous_hash=ZERO_HASH,
    )
    second = QmtCallbackInboxEvent(
        callback=second_callback,
        gateway_holder_id=first.gateway_holder_id,
        qmt_session_id=first.qmt_session_id,
        qmt_lease_generation=first.qmt_lease_generation,
        previous_hash=first.event_hash,
    )

    assert replay_qmt_callback_inbox((first, second)) == (first, second)
    with pytest.raises(ValueError, match="chain"):
        replay_qmt_callback_inbox((first, replace(second, previous_hash=ZERO_HASH)))
    with pytest.raises(ValueError, match="chain"):
        replay_qmt_callback_inbox(
            (
                first,
                QmtCallbackInboxEvent(
                    callback=replace(
                        second.callback,
                        account_id="another-logical-account",
                    ),
                    gateway_holder_id=first.gateway_holder_id,
                    qmt_session_id=first.qmt_session_id,
                    qmt_lease_generation=first.qmt_lease_generation,
                    previous_hash=first.event_hash,
                ),
            )
        )


def test_callback_inbox_migration_is_append_only_redacted_and_locked() -> None:
    sql = Path("migrations/postgres/040_qmt_callback_inbox.sql").read_text(encoding="utf-8")

    assert "qmt_callback_inbox_events" in sql
    assert "NOT (redacted_payload ? 'account_id')" in sql
    assert "NOT (redacted_payload ? 'status_msg')" in sql
    assert "event_payload->'redacted_payload' = redacted_payload" in sql
    assert "NOT broker_mutation_allowed" in sql
    assert "autoquant_reject_immutable_change()" in sql
    assert "VALUES ('postgres', 40)" in sql


def test_callback_receipt_migration_proves_timely_immutable_persistence() -> None:
    sql = Path(
        "migrations/postgres/041_qmt_callback_persistence_receipts.sql"
    ).read_text(encoding="utf-8")

    assert "qmt_callback_persistence_receipts" in sql
    assert "persisted_at <= received_at + interval '5 seconds'" in sql
    assert "qmt_callback_persistence_receipts_immutable" in sql
    assert "NOT broker_mutation_allowed" in sql
    assert "VALUES ('postgres', 41)" in sql
