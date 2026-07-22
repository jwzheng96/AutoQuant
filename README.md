# AutoQuant

AutoQuant is an A-share research data foundation.

Phase 1 provides fail-closed RQData
minute ingestion, point-in-time records, deterministic quality gates, append-only
ClickHouse revisions, PostgreSQL manifests/checkpoints/audit events, and a JSON operator
CLI. It does not place orders or enable live trading.

All dependency, test, lint, type-check, migration, and Git mutation commands for this
checkout must run on `rlocal`; see [the phase-1 runbook](docs/runbooks/phase1-data-foundation.md).

```bash
git clone https://github.com/jwzheng96/AutoQuant.git
cd AutoQuant
/Users/zjw/.local/bin/uv sync --frozen --all-groups
/Users/zjw/.local/bin/uv run autoquant config-check
```

Copy `.env.example` to an untracked `.env` and supply credentials/DSNs only on the trusted
runtime host. Never commit them.
