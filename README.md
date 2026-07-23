# AutoQuant

AutoQuant is an A-share research data foundation.

Phase 1 provides fail-closed Tushare daily ingestion and retained RQData minute support,
point-in-time records, deterministic quality gates, append-only ClickHouse revisions,
PostgreSQL manifests/checkpoints/audit events, a JSON operator CLI, and an authenticated
local Web operator console, and an audited fixed-strategy backtest queue. It does not place
orders, promise profitability, or enable live trading.

The repository also contains a deterministic A-share research ledger with versioned board
rules, T+1 sellability, lot-size validation, conservative daily-open fills, liquidity caps,
configurable commission/slippage, sell-side stamp duty, bilateral transfer fees, and a
hash-chained execution journal. Schema-v3 manifests include point-in-time calendars,
lifecycles, suspension revisions, and exact daily price limits. PostgreSQL schema v4 persists
bounded baseline runs, executions, daily snapshots, and event chains atomically; `/research`
shows those results and verifies stored hashes when they are read.

PostgreSQL schemas v5-v6 add a persistent rolling walk-forward queue for the fixed
`sma_cross_v1` research baseline. Parameters are selected only in training windows, tests are
separated by an embargo, every fold is compared with the same-period buy-and-hold benchmark,
and complete selected-fold artifacts are hash-verified on read. This is a research filter,
not a live-trading release.

PostgreSQL schema v7 adds an immutable pre-trade risk decision ledger. The shared,
deterministic risk engine rejects stale or unreconciled account state, stale/closed quotes,
duplicate orders, A-share lot and T+1 violations, excessive notional/concentration/exposure,
daily turnover/loss/drawdown breaches, and price-deviation errors. Live mode is hard-locked.
The console exposes read-only risk status and audit counts; it still has no order action or
paper/QMT gateway.

The execution domain now also contains a deterministic paper-order lifecycle and account
reconciler. Duplicate broker facts are idempotent, conflicting/out-of-order facts fail closed,
terminal states are irreversible, and unknown state requires a newer broker fact to recover.
These are unconnected safety primitives, not a running paper broker or permission to trade.
PostgreSQL schema v8 persists their materialized orders, immutable transition events, account
snapshots, and reconciliation reports. Application startup and the read-only execution status
replay every bounded order history and fail closed if its projection or hash chain differs.

PostgreSQL schema v9 adds a fail-closed per-account kill switch and immutable control-command
chain. It initializes active, automatically activates on recovery/reconciliation failures, and
can only be reset by code that supplies a current passing persisted reconciliation, the expected
control version, and verified execution recovery. The Web console exposes activation only.
`AQ_LIVE_TRADING_ENABLED=true` is rejected even in a live environment in this release.

PostgreSQL schema v10 contains an explicitly local, zero-network simulated broker. It owns an
independent append-only broker-fact chain, applies deterministic bid/ask marketability, rejects
closed-market submissions, and verifies all broker projections by replay at startup. Internal
and broker histories can now be rebuilt into independent cash, position, T+1 sellability and
open-order snapshots whose evidence hashes are persisted with reconciliation reports. The
internal coordinator now serializes each account cycle across processes, fences dispatch against
the durable kill switch inside the broker transaction, and closes callback gaps idempotently.
It is not connected to a trusted session-risk-state source, continuous quote adapter, API or
scheduling loop, so paper submission remains unavailable from the console.

PostgreSQL schema v11 removes caller-supplied daily risk metrics. A paper session must be
initialized from a persisted opening account snapshot before its first fill; peak equity and
cumulative turnover then advance only from hash-verified snapshots and immutable fill deltas.
The coordinator refuses to create a decision or order when that daily state is missing. A
pre-market initializer and continuous scheduler are still required before the paper gateway can
be exposed.

The pre-market initializer core now accepts only an explicit `pre_open` phase, reconciles both
account sources, proves zero current-session turnover, and freezes one idempotent opening state.
Database crash drills cover restart before broker dispatch and restart after broker facts but
before internal callbacks. Operational market-phase/quote adapters and scheduling remain absent,
so this core is still not callable from the console.

Failure injection now also covers broker submission loss and an explicit broker `unknown` state.
Dependency loss leaves the approved local intent recoverable but activates the durable kill
switch. An `unknown` order activates `order_state_unknown` even when both account projections
otherwise reconcile, so numerical equality cannot falsely authorize another order.

The QMT preparation boundary now provides explicit Shanghai/Shenzhen symbol translation,
conservative XtQuant order-state normalization, `None`-query fail-closed handling, a
thread-safe callback buffer, and a host preflight command. The preflight checks Windows,
64-bit Python, the exact `userdata_mini` directory, a unique session ID, account configuration,
the `xtquant` module, the `up_queue_xtquant` permission sentinel, and the durable kill switch
without importing XtQuant or connecting to MiniQMT. Submit and cancel methods still raise a
release-lock error unconditionally; this is preparation for read-only reconciliation, not a
live gateway.

PostgreSQL schema v12 adds a cross-process QMT session lease with advisory-lock serialization,
short bounded heartbeats, hashed bearer tokens, fencing generations, and immutable acquire/release
events. The readiness command now checks the durable active-session registry, while the future
Windows adapter must still acquire and continuously renew its lease before connecting. Expiry or
token mismatch fails closed and never changes the live-order release lock.

PostgreSQL schema v13 adds an exchange-phase-aware paper scheduler and its append-only evidence
chain. The scheduler uses point-in-time trading calendars, permits strategy intents only during
morning/afternoon continuous auctions, replays daily session risk before use, and requires a
complete fresh sequenced quote snapshot. Stream gaps, time regression, stale/missing prices,
overlapping cycles, invalid strategy output, or evidence-sink failure activate the durable kill
switch. The operator console verifies and displays scheduler recovery, but no external real-time
adapter or production scheduler process is wired yet.

All dependency, test, lint, type-check, migration, and Git mutation commands for this
checkout must run on `rlocal`; see [the phase-1 runbook](docs/runbooks/phase1-data-foundation.md).

```bash
git clone https://github.com/jwzheng96/AutoQuant.git
cd AutoQuant
/Users/zjw/.local/bin/uv sync --frozen --all-groups
/Users/zjw/.local/bin/uv run autoquant config-check
/Users/zjw/.local/bin/uv run autoquant tushare-check
/Users/zjw/.local/bin/uv run autoquant qmt-check
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
submit bounded ingestion and fixed baseline backtest jobs. Its trading page remains explicitly
locked until sample-out strategy evidence, a portfolio risk engine, QMT gateway, reconciliation,
and simulation evidence exist. The portfolio risk core is now present, but the other release
gates remain open and the current real-data validation result is not profitable evidence.

Copy `.env.example` to an untracked `.env` and supply credentials/DSNs only on the trusted
runtime host. The Tushare Token previously shared in chat must be rotated before use; set
only the replacement as `AQ_TUSHARE_TOKEN`. Never commit or paste it into logs or chat.

The current Tushare path uses `daily`, `adj_factor`, `trade_cal`, `stock_basic`,
`suspend_d`, and the 2000-point `stk_limit` endpoint for exact historical daily price
boundaries. It does not call `stk_mins` and does not assume that a 2000-point account has
the independently licensed historical-minute permission. See the
[phase-1 runbook](docs/runbooks/phase1-data-foundation.md) for migrations and ingestion.

QMT must run on a separately controlled Windows machine with MiniQMT. See the
[QMT preparation runbook](docs/runbooks/qmt-read-only-preparation.md); no XtQuant package,
broker account, or MiniQMT process is expected on the current macOS development host.
