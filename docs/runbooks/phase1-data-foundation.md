# Phase 1 Data Foundation Runbook

Run project tools only on `rlocal` in the project checkout.

## Reproducible setup and available checks

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv sync --frozen --all-groups'
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run open-quant config-check'
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest -m "not live" -q'
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run ruff check . && /Users/zjw/.local/bin/uv run mypy src'
```

Apply `migrations/postgres/001_phase1.sql` and
`migrations/clickhouse/001_phase1.sql` only to explicitly authorized phase-1 databases,
then run `open-quant db-check`.

## Opt-in external evidence

The RQData smoke test never runs by default. With credentials loaded only in the trusted
remote environment:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && OQ_RUN_RQDATA_LIVE=1 /Users/zjw/.local/bin/uv run pytest tests/live/test_rqdata_readonly.py -q -rs'
```

Real RQData, PostgreSQL, and ClickHouse checks are required before phase 1 can be declared
complete. A real end-to-end ingestion must produce a passing quality report, immutable
manifest, ClickHouse rows, PostgreSQL checkpoint, and audit event. Credential-dependent or
database-dependent skips are incomplete evidence, not successes.
