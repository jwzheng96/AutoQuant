from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from autoquant.config import AppSettings, RuntimeEnvironment
from autoquant.operations import run_qmt_readonly_acceptance


def _settings() -> AppSettings:
    return AppSettings(
        _env_file=None,
        environment=RuntimeEnvironment.PAPER,
        postgres_dsn=SecretStr("postgresql+asyncpg://configured"),
        qmt_userdata_path=Path("/trusted/userdata_mini"),
        qmt_account_id=SecretStr("broker-account"),
        qmt_session_id=20260723,
        qmt_holder_id="windows-qmt-readonly-01",
        qmt_lease_token=SecretStr("qmt-readonly-operation-token-value"),
    )


@pytest.mark.asyncio
async def test_qmt_acceptance_hashes_vendor_before_guarded_connection() -> None:
    events: list[str] = []
    controls = AsyncMock()
    controls.replay.return_value = SimpleNamespace(active=True)
    leases = AsyncMock()
    leases.active_session_ids.return_value = ()
    acceptances = AsyncMock()
    readiness = SimpleNamespace(read_only_ready=True, checks=())
    bindings = MagicMock(package_manifest_hash="a" * 64)
    baseline = MagicMock()
    acceptance = SimpleNamespace(
        baseline=baseline,
        package_manifest_hash=bindings.package_manifest_hash,
    )
    windows_session = MagicMock()
    windows_session.query.return_value = acceptance
    lease = MagicMock()
    evidence = MagicMock(
        account_snapshot_hash="b" * 64,
        evidence_hash="c" * 64,
        order_count=0,
        position_count=0,
        trade_count=0,
    )
    acceptances.append.return_value = evidence
    guard = AsyncMock()
    guard.verify.return_value = lease

    def load_bindings() -> object:
        events.append("bindings_loaded")
        return bindings

    async def start_guard() -> object:
        events.append("lease_started")
        return lease

    guard.start.side_effect = start_guard
    with (
        patch(
            "autoquant.operations.PostgresExecutionControlRepository.connect",
            return_value=controls,
        ),
        patch(
            "autoquant.operations.PostgresQmtSessionLeaseRepository.connect",
            return_value=leases,
        ),
        patch(
            "autoquant.operations.PostgresQmtReadOnlyAcceptanceRepository.connect",
            return_value=acceptances,
        ),
        patch(
            "autoquant.operations.inspect_qmt_readiness",
            return_value=readiness,
        ),
        patch(
            "autoquant.operations.QmtVendorBindings.load",
            side_effect=load_bindings,
        ),
        patch(
            "autoquant.operations.QmtSessionLeaseGuard",
            return_value=guard,
        ) as guard_type,
        patch(
            "autoquant.operations.QmtReadOnlyWindowsSession",
            return_value=windows_session,
        ),
        patch(
            "autoquant.operations.QmtReadOnlyAcceptanceEvidence.from_baseline",
            return_value=evidence,
        ) as from_baseline,
    ):
        result = await run_qmt_readonly_acceptance(
            _settings(),
            actor="operator",
        )

    assert events == ["bindings_loaded", "lease_started"]
    assert result["status"] == "qmt_readonly_accepted"
    assert result["live_trading_locked"] is True
    guard_type.assert_called_once()
    guard.start.assert_awaited_once()
    guard.verify.assert_awaited_once()
    guard.close.assert_awaited_once()
    windows_session.open.assert_called_once()
    windows_session.query.assert_called_once()
    windows_session.close.assert_called_once()
    from_baseline.assert_called_once_with(
        baseline=baseline,
        package_manifest_hash=bindings.package_manifest_hash,
        lease=lease,
    )
    acceptances.append.assert_awaited_once()
    leases.acquire.assert_not_awaited()
    leases.release.assert_not_awaited()
