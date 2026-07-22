#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import secrets
import tempfile
from pathlib import Path
from urllib.parse import quote


def parse_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"{path.name}:{number}: invalid environment entry")
        key, value = line.split("=", 1)
        if not key or not key.replace("_", "A").isalnum():
            raise ValueError(f"{path.name}:{number}: invalid environment key")
        values[key] = value.strip().strip("'\"")
    return values


def required(values: dict[str, str], key: str, *, source: str) -> str:
    value = values.get(key, "").strip()
    if not value:
        raise ValueError(f"{source} is missing {key}")
    return value


def render_app_env(
    source: dict[str, str], infra: dict[str, str], *, web_port: int = 8000
) -> str:
    if not 1 <= web_port <= 65535:
        raise ValueError("web_port must be between 1 and 65535")
    postgres_user = required(infra, "POSTGRES_USER", source="infra/.env")
    postgres_password = required(infra, "POSTGRES_PASSWORD", source="infra/.env")
    postgres_db = required(infra, "POSTGRES_DB", source="infra/.env")
    clickhouse_user = required(infra, "CLICKHOUSE_USER", source="infra/.env")
    clickhouse_password = required(infra, "CLICKHOUSE_PASSWORD", source="infra/.env")
    clickhouse_db = required(infra, "CLICKHOUSE_DB", source="infra/.env")
    tushare_token = required(source, "AQ_TUSHARE_TOKEN", source="source .env")
    web_password = source.get("AQ_WEB_PASSWORD", "").strip() or secrets.token_urlsafe(24)

    values = {
        "AQ_ENVIRONMENT": "backtest",
        "AQ_LIVE_TRADING_ENABLED": "false",
        "AQ_RQDATA_USERNAME": source.get("AQ_RQDATA_USERNAME", ""),
        "AQ_RQDATA_PASSWORD": source.get("AQ_RQDATA_PASSWORD", ""),
        "AQ_RQDATA_AUTH_URL": source.get("AQ_RQDATA_AUTH_URL", "https://rqdata.ricequant.com/auth"),
        "AQ_RQDATA_API_URL": source.get("AQ_RQDATA_API_URL", "https://rqdata.ricequant.com/api"),
        "AQ_TUSHARE_TOKEN": tushare_token,
        "AQ_TUSHARE_API_URL": source.get("AQ_TUSHARE_API_URL", "https://api.tushare.pro"),
        "AQ_POSTGRES_DSN": (
            "postgresql+asyncpg://"
            f"{quote(postgres_user, safe='')}:{quote(postgres_password, safe='')}"
            f"@127.0.0.1:5434/{quote(postgres_db, safe='')}"
        ),
        "AQ_CLICKHOUSE_DSN": (
            "clickhouse://"
            f"{quote(clickhouse_user, safe='')}:{quote(clickhouse_password, safe='')}"
            f"@127.0.0.1:8123/{quote(clickhouse_db, safe='')}"
        ),
        "AQ_WEB_HOST": "127.0.0.1",
        "AQ_WEB_PORT": str(web_port),
        "AQ_WEB_USERNAME": source.get("AQ_WEB_USERNAME", "operator") or "operator",
        "AQ_WEB_PASSWORD": web_password,
        "AQ_PAPER_ACCOUNT_ID": source.get("AQ_PAPER_ACCOUNT_ID", "paper-main")
        or "paper-main",
        "AQ_PAPER_INITIAL_CASH": source.get("AQ_PAPER_INITIAL_CASH", "1000000")
        or "1000000",
    }
    return "".join(f"{key}={value}\n" for key, value in values.items())


def atomic_secret_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=".autoquant-env-", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create an untracked local AutoQuant .env")
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--infra", default=Path("infra/.env"), type=Path)
    parser.add_argument("--output", default=Path(".env"), type=Path)
    parser.add_argument("--web-port", default=8000, type=int)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    if args.output.exists() and not args.force:
        raise SystemExit(f"Refusing to overwrite {args.output}; pass --force explicitly")
    content = render_app_env(
        parse_env(args.source), parse_env(args.infra), web_port=args.web_port
    )
    atomic_secret_write(args.output, content)
    print(f"Configured {args.output} with mode 600; secret values were not printed")


if __name__ == "__main__":
    main()
