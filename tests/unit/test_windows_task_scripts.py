from __future__ import annotations

import re
from pathlib import Path

SCRIPTS = Path("scripts/windows")
RUNTIME = SCRIPTS / "install-paper-runtime-task.ps1"
WATCHDOG = SCRIPTS / "install-paper-watchdog-task.ps1"
READINESS_EXPORT = SCRIPTS / "export-readiness-evidence.ps1"
READINESS_ACCEPTANCE = SCRIPTS / "test-readiness-evidence.ps1"
WINDOWS_WORKFLOW = Path(".github/workflows/windows-powershell-safety.yml")


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
    assert "$exportExitCode -notin @(0, 2)" in content
    assert "$exportSummary.artifact_written -ne $true" in content
    assert "operations-readiness-sign" in content
    assert "$signSummary.signature_written -ne $true" in content
    assert "operations-readiness-verify-signed" in content
    assert "$verifySummary.signature_valid -ne $true" in content
    assert "$verifySummary.key_id -ne $signSummary.key_id" in content
    assert "$verifySummary.report_hash -ne $exportSummary.report_hash" in content
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


def test_windows_readiness_acceptance_uses_only_ast_parse_and_whatif() -> None:
    content = _text(READINESS_ACCEPTANCE)
    lowered = content.casefold()

    assert "Language.Parser]::ParseFile" in content
    assert "export-readiness-evidence.ps1" in content
    assert "-WhatIf" in content
    assert "must-not-execute" in content
    assert "$summary.what_if -ne $true" in content
    assert "$summary.artifact_written -ne $false" in content
    assert "$summary.signature_written -ne $false" in content
    assert "artifact_created = $false" in content
    assert "signature_created = $false" in content
    assert "uv_executed = $false" in content
    assert "operations-readiness-export" not in lowered
    assert "order_stock" not in lowered


def test_windows_safety_workflow_is_read_only_pinned_and_secret_free() -> None:
    content = _text(WINDOWS_WORKFLOW)
    lowered = content.casefold()

    assert "runs-on: windows-latest" in content
    assert "timeout-minutes: 10" in content
    assert "permissions:\n  contents: read" in content
    assert (
        "actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd"
        in content
    )
    assert "persist-credentials: false" in content
    assert r".\scripts\windows\test-readiness-evidence.ps1" in content
    assert "secrets." not in lowered
    assert "uv sync" not in lowered
    assert "pip install" not in lowered
