from __future__ import annotations

import importlib.util
import tomllib
from pathlib import Path

import pytest

from autoquant.config import AppSettings, RuntimeEnvironment

ROOT = Path(__file__).parents[2]


def test_autoquant_is_the_only_runtime_package_and_console_script() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert project["project"]["name"] == "autoquant"
    assert project["project"]["scripts"] == {"autoquant": "autoquant.cli:app"}
    assert project["tool"]["mypy"]["packages"] == ["autoquant"]
    assert importlib.util.find_spec("autoquant") is not None
    assert importlib.util.find_spec("open_quant") is None


def test_autoquant_uses_only_the_aq_environment_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AQ_ENVIRONMENT", "paper")
    monkeypatch.setenv("OQ_ENVIRONMENT", "live")

    settings = AppSettings(_env_file=None)

    assert settings.environment is RuntimeEnvironment.PAPER
    assert settings.model_config["env_prefix"] == "AQ_"


def test_active_configuration_and_sql_use_autoquant_identity() -> None:
    env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
    postgres = (ROOT / "migrations/postgres/001_phase1.sql").read_text(
        encoding="utf-8"
    )

    assert "AQ_ENVIRONMENT=" in env_example
    assert "OQ_" not in env_example
    assert "autoquant_validate_checkpoint" in postgres
    assert "autoquant.audit_events" in postgres
    assert "open_quant" not in postgres

    active_python = "\n".join(
        path.read_text(encoding="utf-8")
        for tree in (ROOT / "src", ROOT / "tests")
        for path in tree.rglob("*.py")
        if path.name != "test_project_identity.py"
    )
    assert "OpenQuant" not in active_python
    assert "oq_test_" not in active_python
