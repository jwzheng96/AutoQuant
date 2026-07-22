# AutoQuant

AutoQuant is an A-share research data foundation.

Phase 1 provides fail-closed Tushare daily ingestion and retained RQData minute support,
point-in-time records, deterministic quality gates, append-only ClickHouse revisions,
PostgreSQL manifests/checkpoints/audit events, and a JSON operator CLI. It does not place
orders, run strategies, promise profitability, or enable live trading.

All dependency, test, lint, type-check, migration, and Git mutation commands for this
checkout must run on `rlocal`; see [the phase-1 runbook](docs/runbooks/phase1-data-foundation.md).

```bash
git clone https://github.com/jwzheng96/AutoQuant.git
cd AutoQuant
/Users/zjw/.local/bin/uv sync --frozen --all-groups
/Users/zjw/.local/bin/uv run autoquant config-check
/Users/zjw/.local/bin/uv run autoquant tushare-check
```

Copy `.env.example` to an untracked `.env` and supply credentials/DSNs only on the trusted
runtime host. The Tushare Token previously shared in chat must be rotated before use; set
only the replacement as `AQ_TUSHARE_TOKEN`. Never commit or paste it into logs or chat.

The current Tushare path uses `daily`, `adj_factor`, `trade_cal`, `stock_basic`, and
`suspend_d`. It does not call `stk_mins` and does not assume that a 2000-point account has
the independently licensed historical-minute permission. See the
[phase-1 runbook](docs/runbooks/phase1-data-foundation.md) for migrations and ingestion.
