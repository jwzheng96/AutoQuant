from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.execution.models import ZERO_HASH


class KillSwitchAction(StrEnum):
    INITIALIZE = "initialize"
    ACTIVATE = "activate"
    RESET = "reset"


class KillSwitchReason(StrEnum):
    INITIALIZING = "initializing"
    MANUAL = "manual"
    RECONCILIATION_FAILED = "reconciliation_failed"
    DEPENDENCY_UNAVAILABLE = "dependency_unavailable"
    ORDER_STATE_UNKNOWN = "order_state_unknown"
    RECOVERY_FAILED = "recovery_failed"
    DRILL = "drill"
    RESET_APPROVED = "reset_approved"


@dataclass(frozen=True, slots=True)
class KillSwitchControl:
    account_id: str
    active: bool
    version: int
    reason: KillSwitchReason
    changed_at: datetime
    changed_by: str
    last_event_hash: str
    state_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(self.account_id, name="account_id")
        _require_nonblank(self.changed_by, name="changed_by")
        if type(self.active) is not bool:
            raise TypeError("active must be a bool")
        if not isinstance(self.version, int) or isinstance(self.version, bool):
            raise TypeError("version must be an integer")
        if self.version < 1:
            raise ValueError("version must be positive")
        if not isinstance(self.reason, KillSwitchReason):
            raise TypeError("reason must be KillSwitchReason")
        if not self.active and self.reason is not KillSwitchReason.RESET_APPROVED:
            raise ValueError("inactive kill switch requires reset-approved reason")
        changed_at = to_utc(self.changed_at, name="kill switch changed_at")
        object.__setattr__(self, "changed_at", changed_at)
        _require_lowercase_sha256(self.last_event_hash, name="last_event_hash")
        object.__setattr__(
            self,
            "state_hash",
            _canonical_hash(control_payload(self)),
        )


@dataclass(frozen=True, slots=True)
class KillSwitchCommand:
    command_id: str
    account_id: str
    action: KillSwitchAction
    reason: KillSwitchReason
    actor: str
    occurred_at: datetime
    evidence_hash: str | None = None
    command_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for name, value in (
            ("command_id", self.command_id),
            ("account_id", self.account_id),
            ("actor", self.actor),
        ):
            _require_nonblank(value, name=name)
        if not 16 <= len(self.command_id) <= 128:
            raise ValueError("command_id must contain 16-128 characters")
        if not isinstance(self.action, KillSwitchAction):
            raise TypeError("action must be KillSwitchAction")
        if not isinstance(self.reason, KillSwitchReason):
            raise TypeError("reason must be KillSwitchReason")
        if self.action is KillSwitchAction.INITIALIZE:
            if self.reason is not KillSwitchReason.INITIALIZING:
                raise ValueError("initialization requires initializing reason")
        elif self.action is KillSwitchAction.RESET:
            if self.reason is not KillSwitchReason.RESET_APPROVED:
                raise ValueError("reset requires reset-approved reason")
            if self.evidence_hash is None:
                raise ValueError("reset requires reconciliation evidence")
        elif self.reason in {
            KillSwitchReason.INITIALIZING,
            KillSwitchReason.RESET_APPROVED,
        }:
            raise ValueError("activation requires an activation reason")
        if self.evidence_hash is not None:
            _require_lowercase_sha256(self.evidence_hash, name="evidence_hash")
        occurred_at = to_utc(self.occurred_at, name="kill switch occurred_at")
        object.__setattr__(self, "occurred_at", occurred_at)
        object.__setattr__(
            self,
            "command_hash",
            _canonical_hash(command_payload(self)),
        )


@dataclass(frozen=True, slots=True)
class KillSwitchEvent:
    sequence: int
    command: KillSwitchCommand
    previous_hash: str
    transition_state_hash: str
    event_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if self.sequence < 1:
            raise ValueError("event sequence must be positive")
        _require_lowercase_sha256(self.previous_hash, name="previous_hash")
        _require_lowercase_sha256(
            self.transition_state_hash, name="transition_state_hash"
        )
        object.__setattr__(
            self,
            "event_hash",
            _canonical_hash(
                {
                    "command_hash": self.command.command_hash,
                    "previous_hash": self.previous_hash,
                    "sequence": self.sequence,
                    "transition_state_hash": self.transition_state_hash,
                }
            ),
        )


def apply_kill_switch_command(
    current: KillSwitchControl | None,
    command: KillSwitchCommand,
) -> tuple[KillSwitchControl, KillSwitchEvent]:
    if current is None:
        if command.action is not KillSwitchAction.INITIALIZE:
            raise ValueError("kill switch must be initialized fail-closed")
        version = 1
        previous_hash = ZERO_HASH
        active = True
    else:
        if command.account_id != current.account_id:
            raise ValueError("kill switch command account does not match state")
        if command.occurred_at < current.changed_at:
            raise ValueError("kill switch time cannot move backwards")
        if command.action is KillSwitchAction.INITIALIZE:
            raise ValueError("kill switch is already initialized")
        version = current.version + 1
        previous_hash = current.last_event_hash
        active = command.action is KillSwitchAction.ACTIVATE
    provisional = KillSwitchControl(
        account_id=command.account_id,
        active=active,
        version=version,
        reason=command.reason,
        changed_at=command.occurred_at,
        changed_by=command.actor,
        last_event_hash=previous_hash,
    )
    event = KillSwitchEvent(
        sequence=version,
        command=command,
        previous_hash=previous_hash,
        transition_state_hash=provisional.state_hash,
    )
    state = KillSwitchControl(
        account_id=provisional.account_id,
        active=provisional.active,
        version=provisional.version,
        reason=provisional.reason,
        changed_at=provisional.changed_at,
        changed_by=provisional.changed_by,
        last_event_hash=event.event_hash,
    )
    return state, event


def control_payload(control: KillSwitchControl) -> dict[str, object]:
    return {
        "account_id": control.account_id,
        "active": control.active,
        "changed_at": control.changed_at.isoformat(timespec="microseconds"),
        "changed_by": control.changed_by,
        "last_event_hash": control.last_event_hash,
        "reason": control.reason.value,
        "version": control.version,
    }


def command_payload(command: KillSwitchCommand) -> dict[str, object]:
    return {
        "account_id": command.account_id,
        "action": command.action.value,
        "actor": command.actor,
        "command_id": command.command_id,
        "evidence_hash": command.evidence_hash,
        "occurred_at": command.occurred_at.isoformat(timespec="microseconds"),
        "reason": command.reason.value,
    }
