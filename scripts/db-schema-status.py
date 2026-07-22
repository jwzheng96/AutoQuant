#!/usr/bin/env python3
from __future__ import annotations

import asyncio

import clickhouse_connect  # type: ignore[import-untyped]
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from autoquant.config import AppSettings
from autoquant.operations import configured_dsn


async def main() -> None:
    settings = AppSettings()
    postgres_dsn = configured_dsn(settings.postgres_dsn, capability="PostgreSQL")
    clickhouse_dsn = configured_dsn(settings.clickhouse_dsn, capability="ClickHouse")

    engine = create_async_engine(postgres_dsn)
    try:
        async with engine.connect() as connection:
            result = await connection.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' ORDER BY table_name"
                )
            )
            postgres_tables = tuple(str(value) for value in result.scalars())
            print(f"postgres_tables={','.join(postgres_tables)}")
            if "schema_versions" in postgres_tables:
                version = (
                    await connection.execute(
                        text(
                            "SELECT version FROM schema_versions "
                            "WHERE component = 'postgres'"
                        )
                    )
                ).scalar_one_or_none()
                print(f"postgres_version={version}")
            if "quality_reports" in postgres_tables:
                latest_quality = (
                    await connection.execute(
                        text(
                            "SELECT passed, production_complete, payload "
                            "FROM quality_reports ORDER BY created_at DESC LIMIT 1"
                        )
                    )
                ).mappings().one_or_none()
                if latest_quality is not None:
                    payload = latest_quality["payload"]
                    issues = payload.get("issues", []) if isinstance(payload, dict) else []
                    codes = sorted(
                        str(issue.get("code"))
                        for issue in issues
                        if isinstance(issue, dict) and issue.get("code")
                    )
                    print(f"latest_quality_passed={latest_quality['passed']}")
                    print(
                        "latest_quality_production_complete="
                        f"{latest_quality['production_complete']}"
                    )
                    print(f"latest_quality_issue_codes={','.join(codes)}")
    finally:
        await engine.dispose()

    client = await clickhouse_connect.get_async_client(
        dsn=clickhouse_dsn, tz_mode="aware"
    )
    try:
        result = await client.query("SHOW TABLES")
        clickhouse_tables = tuple(sorted(str(row[0]) for row in result.result_rows))
        print(f"clickhouse_tables={','.join(clickhouse_tables)}")
        if "schema_versions" in clickhouse_tables:
            version = await client.command(
                "SELECT max(version) FROM schema_versions "
                "WHERE component = 'clickhouse'"
            )
            print(f"clickhouse_version={version}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
