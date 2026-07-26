from __future__ import annotations

import importlib.util
import platform
import struct
from collections.abc import Callable, Collection
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from autoquant.clock import to_utc
from autoquant.config import AppSettings
from autoquant.data.models import _canonical_hash, _datetime_text
from autoquant.errors import BrokerStateUnknownError

_MAX_CLOCK_ERROR = timedelta(seconds=2)
_MAX_CLOCK_ROUND_TRIP = timedelta(seconds=2)


class QmtReadinessCode(StrEnum):
    LIVE_RELEASE_LOCK = "live_release_lock"
    KILL_SWITCH = "kill_switch"
    WINDOWS_RUNTIME = "windows_runtime"
    PYTHON_64_BIT = "python_64_bit"
    USERDATA_PATH = "userdata_path"
    ACCOUNT_ID = "account_id"
    SESSION_ID = "session_id"
    SESSION_ID_UNIQUE = "session_id_unique"
    TRUSTED_CLOCK = "trusted_clock"
    XTQUANT_MODULE = "xtquant_module"
    ORDER_PERMISSION = "order_permission"


@dataclass(frozen=True, slots=True)
class QmtReadinessCheck:
    code: QmtReadinessCode
    passed: bool
    detail: str


_READ_ONLY_CODES = frozenset(
    {
        QmtReadinessCode.LIVE_RELEASE_LOCK,
        QmtReadinessCode.KILL_SWITCH,
        QmtReadinessCode.WINDOWS_RUNTIME,
        QmtReadinessCode.PYTHON_64_BIT,
        QmtReadinessCode.USERDATA_PATH,
        QmtReadinessCode.ACCOUNT_ID,
        QmtReadinessCode.SESSION_ID,
        QmtReadinessCode.SESSION_ID_UNIQUE,
        QmtReadinessCode.TRUSTED_CLOCK,
        QmtReadinessCode.XTQUANT_MODULE,
    }
)


@dataclass(frozen=True, slots=True)
class QmtClockAttestation:
    request_started_at: datetime
    database_observed_at: datetime
    request_completed_at: datetime
    attestation_hash: str = field(init=False)

    def __post_init__(self) -> None:
        started = to_utc(
            self.request_started_at,
            name="QMT clock request start",
        )
        observed = to_utc(
            self.database_observed_at,
            name="QMT database clock observation",
        )
        completed = to_utc(
            self.request_completed_at,
            name="QMT clock request completion",
        )
        if completed < started:
            raise ValueError("QMT clock request completion precedes its start")
        object.__setattr__(self, "request_started_at", started)
        object.__setattr__(self, "database_observed_at", observed)
        object.__setattr__(self, "request_completed_at", completed)
        object.__setattr__(
            self,
            "attestation_hash",
            _canonical_hash(self.payload()),
        )

    @property
    def round_trip(self) -> timedelta:
        return self.request_completed_at - self.request_started_at

    @property
    def estimated_offset(self) -> timedelta:
        midpoint = self.request_started_at + self.round_trip / 2
        return self.database_observed_at - midpoint

    @property
    def worst_case_error(self) -> timedelta:
        return abs(self.estimated_offset) + self.round_trip / 2

    @property
    def trusted(self) -> bool:
        return (
            self.round_trip <= _MAX_CLOCK_ROUND_TRIP
            and self.worst_case_error <= _MAX_CLOCK_ERROR
        )

    def payload(self) -> dict[str, object]:
        return {
            "database_observed_at": _datetime_text(
                self.database_observed_at
            ),
            "estimated_offset_microseconds": _microseconds(
                self.estimated_offset
            ),
            "max_clock_error_microseconds": _microseconds(
                _MAX_CLOCK_ERROR
            ),
            "max_round_trip_microseconds": _microseconds(
                _MAX_CLOCK_ROUND_TRIP
            ),
            "request_completed_at": _datetime_text(
                self.request_completed_at
            ),
            "request_started_at": _datetime_text(
                self.request_started_at
            ),
            "round_trip_microseconds": _microseconds(self.round_trip),
            "trusted": self.trusted,
            "version": "qmt-clock-attestation-v1",
            "worst_case_error_microseconds": _microseconds(
                self.worst_case_error
            ),
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> QmtClockAttestation:
        value = cls(
            request_started_at=datetime.fromisoformat(
                str(payload["request_started_at"])
            ),
            database_observed_at=datetime.fromisoformat(
                str(payload["database_observed_at"])
            ),
            request_completed_at=datetime.fromisoformat(
                str(payload["request_completed_at"])
            ),
        )
        if payload != value.payload():
            raise ValueError("QMT clock attestation payload is invalid")
        return value


class QmtDatabaseClock(Protocol):
    async def database_time(self) -> datetime: ...


class QmtDatabaseClockVerifier:
    """Create a fresh bounded clock proof against PostgreSQL."""

    def __init__(
        self,
        *,
        repository: QmtDatabaseClock,
        now: Callable[[], datetime],
    ) -> None:
        self._repository = repository
        self._now = now

    async def attest(self) -> QmtClockAttestation:
        started_at = self._now()
        database_observed_at = await self._repository.database_time()
        completed_at = self._now()
        attestation = QmtClockAttestation(
            request_started_at=started_at,
            database_observed_at=database_observed_at,
            request_completed_at=completed_at,
        )
        if not attestation.trusted:
            raise BrokerStateUnknownError(
                "QMT host clock is outside the trusted PostgreSQL bound"
            )
        return attestation


def _microseconds(value: timedelta) -> int:
    return (
        value.days * 86_400_000_000
        + value.seconds * 1_000_000
        + value.microseconds
    )


@dataclass(frozen=True, slots=True)
class QmtReadinessReport:
    checks: tuple[QmtReadinessCheck, ...]

    @property
    def read_only_ready(self) -> bool:
        return all(check.passed for check in self.checks if check.code in _READ_ONLY_CODES)

    @property
    def order_drill_ready(self) -> bool:
        return self.read_only_ready and all(check.passed for check in self.checks)

    @property
    def live_trading_ready(self) -> bool:
        """No preflight report can unlock money-bearing orders in this release."""

        return False

    @property
    def blockers(self) -> tuple[QmtReadinessCode, ...]:
        return tuple(check.code for check in self.checks if not check.passed)


def inspect_qmt_readiness(
    settings: AppSettings,
    *,
    kill_switch_active: bool | None,
    active_session_ids: Collection[int] | None = None,
    clock_attestation: QmtClockAttestation | None = None,
    system_name: str | None = None,
    pointer_bits: int | None = None,
    xtquant_module_available: bool | None = None,
) -> QmtReadinessReport:
    """Inspect a QMT host without importing XtQuant or connecting to MiniQMT."""

    actual_system = platform.system() if system_name is None else system_name
    actual_bits = struct.calcsize("P") * 8 if pointer_bits is None else pointer_bits
    if xtquant_module_available is None:
        try:
            module_available = importlib.util.find_spec("xtquant") is not None
        except (ImportError, ModuleNotFoundError, ValueError):
            module_available = False
    else:
        module_available = xtquant_module_available

    userdata_path = settings.qmt_userdata_path
    path_valid = (
        userdata_path is not None
        and userdata_path.is_absolute()
        and userdata_path.name.casefold() == "userdata_mini"
        and userdata_path.is_dir()
    )
    permission_available = bool(
        path_valid and userdata_path is not None and (userdata_path / "up_queue_xtquant").exists()
    )
    account_id = settings.qmt_account_id
    account_configured = bool(account_id is not None and account_id.get_secret_value().strip())
    session_id = settings.qmt_session_id
    session_configured = session_id is not None
    session_unique = (
        session_id is not None
        and active_session_ids is not None
        and session_id not in active_session_ids
    )
    clock_trusted = (
        clock_attestation is not None
        and clock_attestation.trusted
    )

    checks = (
        QmtReadinessCheck(
            QmtReadinessCode.LIVE_RELEASE_LOCK,
            not settings.live_trading_enabled,
            "live broker mutations remain release-locked",
        ),
        QmtReadinessCheck(
            QmtReadinessCode.KILL_SWITCH,
            kill_switch_active is True,
            (
                "durable kill switch is active"
                if kill_switch_active is True
                else "durable kill switch is inactive or unavailable"
            ),
        ),
        QmtReadinessCheck(
            QmtReadinessCode.WINDOWS_RUNTIME,
            actual_system.casefold() == "windows",
            f"detected operating system: {actual_system}",
        ),
        QmtReadinessCheck(
            QmtReadinessCode.PYTHON_64_BIT,
            actual_bits == 64,
            f"detected Python pointer width: {actual_bits}-bit",
        ),
        QmtReadinessCheck(
            QmtReadinessCode.USERDATA_PATH,
            path_valid,
            (
                "userdata_mini is an absolute existing directory"
                if path_valid
                else "configure an absolute existing userdata_mini directory"
            ),
        ),
        QmtReadinessCheck(
            QmtReadinessCode.ACCOUNT_ID,
            account_configured,
            "QMT account identifier is configured"
            if account_configured
            else "QMT account identifier is missing",
        ),
        QmtReadinessCheck(
            QmtReadinessCode.SESSION_ID,
            session_configured,
            "QMT session identifier is configured"
            if session_configured
            else "QMT session identifier is missing",
        ),
        QmtReadinessCheck(
            QmtReadinessCode.SESSION_ID_UNIQUE,
            session_unique,
            "QMT session identifier is not registered by another adapter"
            if session_unique
                else "QMT session identifier is missing, unverified, or already active",
        ),
        QmtReadinessCheck(
            QmtReadinessCode.TRUSTED_CLOCK,
            clock_trusted,
            (
                "host clock is within the trusted PostgreSQL error bound"
                if clock_trusted
                else "host clock is unavailable, too far from PostgreSQL, or too slow to attest"
            ),
        ),
        QmtReadinessCheck(
            QmtReadinessCode.XTQUANT_MODULE,
            module_available,
            "xtquant module is discoverable"
            if module_available
            else "xtquant module is not discoverable",
        ),
        QmtReadinessCheck(
            QmtReadinessCode.ORDER_PERMISSION,
            permission_available,
            "MiniQMT trading permission sentinel is present"
            if permission_available
            else "up_queue_xtquant trading permission sentinel is missing",
        ),
    )
    return QmtReadinessReport(checks=checks)


def qmt_permission_sentinel(userdata_path: Path) -> Path:
    """Expose the documented permission probe path for diagnostics and tests."""

    return userdata_path / "up_queue_xtquant"
