from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType


def _module() -> ModuleType:
    path = Path("scripts/configure-local-env.py")
    spec = spec_from_file_location("configure_local_env", path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_render_app_env_url_encodes_database_credentials_without_printing() -> None:
    module = _module()
    content = module.render_app_env(
        {"AQ_TUSHARE_TOKEN": "local-token"},
        {
            "POSTGRES_USER": "autoquant",
            "POSTGRES_PASSWORD": "pg@secret",
            "POSTGRES_DB": "autoquant",
            "CLICKHOUSE_USER": "autoquant",
            "CLICKHOUSE_PASSWORD": "ch/secret",
            "CLICKHOUSE_DB": "autoquant",
        },
    )

    assert "postgresql+asyncpg://autoquant:pg%40secret@127.0.0.1:5434/autoquant" in content
    assert "clickhouse://autoquant:ch%2Fsecret@127.0.0.1:8123/autoquant" in content
    assert "AQ_TUSHARE_TOKEN=local-token" in content
    web_password = next(
        line.split("=", 1)[1]
        for line in content.splitlines()
        if line.startswith("AQ_WEB_PASSWORD=")
    )
    assert len(web_password) >= 16


def test_render_app_env_accepts_an_alternate_loopback_port() -> None:
    module = _module()
    content = module.render_app_env(
        {"AQ_TUSHARE_TOKEN": "local-token"},
        {
            "POSTGRES_USER": "autoquant",
            "POSTGRES_PASSWORD": "postgres-secret",
            "POSTGRES_DB": "autoquant",
            "CLICKHOUSE_USER": "autoquant",
            "CLICKHOUSE_PASSWORD": "clickhouse-secret",
            "CLICKHOUSE_DB": "autoquant",
        },
        web_port=8010,
    )

    assert "AQ_WEB_PORT=8010\n" in content
