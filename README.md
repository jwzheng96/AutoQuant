# AutoQuant

AutoQuant is an A-share research data foundation.

Phase 1 provides fail-closed Tushare daily ingestion and retained RQData minute support,
point-in-time records, deterministic quality gates, append-only ClickHouse revisions,
PostgreSQL manifests/checkpoints/audit events, a JSON operator CLI, and an authenticated
local Web operator console. It does not place orders, run strategies, promise profitability,
or enable live trading.

The repository also contains a deterministic A-share research ledger with versioned board
rules, T+1 sellability, lot-size validation, conservative daily-open fills, liquidity caps,
configurable commission/slippage, sell-side stamp duty, bilateral transfer fees, and a
hash-chained execution journal. It is a tested domain core, not yet a Web backtest endpoint:
historical security-status and suspension revisions must enter the point-in-time manifest
before the UI is allowed to execute it.

All dependency, test, lint, type-check, migration, and Git mutation commands for this
checkout must run on `rlocal`; see [the phase-1 runbook](docs/runbooks/phase1-data-foundation.md).

```bash
git clone https://github.com/jwzheng96/AutoQuant.git
cd AutoQuant
/Users/zjw/.local/bin/uv sync --frozen --all-groups
/Users/zjw/.local/bin/uv run autoquant config-check
/Users/zjw/.local/bin/uv run autoquant tushare-check
```

For local infrastructure and the operator console:

```bash
cp infra/.env.example infra/.env
# Fill two different strong database passwords locally, then chmod 600 infra/.env
scripts/local-db.sh up
scripts/local-db.sh migrate
/Users/zjw/.local/bin/uv run autoquant db-check
/Users/zjw/.local/bin/uv run autoquant serve-web
```

The console binds only to `127.0.0.1` and requires `AQ_WEB_USERNAME` plus a password of at
least 16 characters in the untracked root `.env`. It can inspect trusted daily data and
submit bounded, audited ingestion jobs. Its trading page remains explicitly locked until a
ledger, risk engine, QMT gateway, and simulation evidence exist.

Copy `.env.example` to an untracked `.env` and supply credentials/DSNs only on the trusted
runtime host. The Tushare Token previously shared in chat must be rotated before use; set
only the replacement as `AQ_TUSHARE_TOKEN`. Never commit or paste it into logs or chat.

The current Tushare path uses `daily`, `adj_factor`, `trade_cal`, `stock_basic`,
`suspend_d`, and the 2000-point `stk_limit` endpoint for exact historical daily price
boundaries. It does not call `stk_mins` and does not assume that a 2000-point account has
the independently licensed historical-minute permission. See the
[phase-1 runbook](docs/runbooks/phase1-data-foundation.md) for migrations and ingestion.
