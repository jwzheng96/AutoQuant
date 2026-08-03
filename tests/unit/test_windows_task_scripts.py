from __future__ import annotations

import re
from pathlib import Path

SCRIPTS = Path("scripts/windows")
RUNTIME = SCRIPTS / "install-paper-runtime-task.ps1"
WATCHDOG = SCRIPTS / "install-paper-watchdog-task.ps1"
READINESS_EXPORT = SCRIPTS / "export-readiness-evidence.ps1"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_windows_task_installers_never_embed_or_echo_credentials() -> None:
    for path in (RUNTIME, WATCHDOG):
        content = _text(path)
        lowered = content.casefold()

        assert "supportsShouldProcess = $true".casefold() in lowered
        assert "secrets_in_task_arguments = $false" in content
        assert "Start-ScheduledTask".casefold() not in lowered
        assert "Unregister-ScheduledTask".casefold() not in lowered
        assert "submit_order" not in lowered
        assert "order_stock" not in lowered
        assert "AQ_ENVIRONMENT=paper" in content
        assert "Select-String" in content
        assert "uv.lock" in content
        assert r".venv\Scripts\python.exe" in content
        assert "--frozen --no-sync" in content
        assert re.findall(r"\bAQ_[A-Z0-9_]+\s*=", content) == [
            "AQ_ENVIRONMENT="
        ]


def test_paper_runtime_task_is_interactive_delayed_and_paper_only() -> None:
    content = _text(RUNTIME)

    assert "-Argument 'run --frozen --no-sync autoquant run-paper'" in content
    assert "-LogonType Interactive" in content
    assert "-RunLevel Limited" in content
    assert "$trigger.Delay" in content
    assert "-RestartCount 999" in content
    assert "paper-watchdog-enforce" not in content
    assert "started = $false" in content
    assert "live_trading_locked = $true" in content


def test_watchdog_task_can_run_independently_and_only_fail_closed() -> None:
    content = _text(WATCHDOG)

    assert (
        "-Argument 'run --frozen --no-sync autoquant paper-watchdog-enforce'"
        in content
    )
    assert "-RepetitionInterval" in content
    assert "-MultipleInstances IgnoreNew" in content
    assert "-LogonType ServiceAccount" in content
    assert "-UserId 'SYSTEM'" in content
    assert "--no-sync autoquant run-paper" not in content
    assert "started = $false" in content
    assert "live_trading_locked = $true" in content


def test_windows_readiness_export_is_frozen_redacted_and_fails_closed() -> None:
    content = _text(READINESS_EXPORT)
    lowered = content.casefold()

    assert "supportsShouldProcess = $true".casefold() in lowered
    assert "operations-readiness-export" in content
    assert "--frozen" in content
    assert "--no-sync" in content
    assert "ConvertFrom-Json" in content
    assert "$nativeExitCode -notin @(0, 2)" in content
    assert "$summary.artifact_written -ne $true" in content
    assert "storage_mutation_allowed = $false" in content
    assert "broker_mutation_allowed = $false" in content
    assert "vendor_request_started = $false" in content
    assert "collection_started = $false" in content
    assert "live_trading_locked = $true" in content
    assert "AQ_ENVIRONMENT=paper" in content
    assert "Select-String" in content
    assert "uv.lock" in content
    assert r".venv\Scripts\python.exe" in content
    assert "qmt-readonly-accept" not in lowered
    assert "order_stock" not in lowered
    assert "retry-plan/authorize" not in lowered
    assert re.findall(r"\bAQ_[A-Z0-9_]+\s*=", content) == [
        "AQ_ENVIRONMENT="
    ]
