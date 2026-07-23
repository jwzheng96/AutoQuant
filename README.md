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
switch. The operator console verifies and displays scheduler recovery.

The QMT whole-quote adapter core now copies XtData callbacks into a bounded thread-safe queue,
requires an exact `get_full_tick` baseline, translates only configured Shanghai/Shenzhen symbols,
converts millisecond event times and numeric prices without binary-float arithmetic, and accepts
market-open quotes only when both the trusted exchange phase and QMT `stockStatus=13` indicate
continuous trading. Missing fields, invalid/zero/crossed prices, unexpected symbols, queue
overflow, callback gaps, or a split batch disconnect the whole stream until a new baseline.
It does not import XtQuant or connect to MiniQMT on this host.

The resident paper runtime now assembles the approved SMA artifact, exact calendar and session
rules, production manifest reader, account projection, pre-trade risk, simulated broker,
append-only scheduler sink and renewable process lease as one owned lifecycle. Its cold-start
gate requires an active kill switch, an active paper-only registration, convergent execution and
simulator replays, an intact scheduler chain and current source-backed calendar evidence before
opening quotes. The Windows-only XtData runtime uses `get_full_tick` before
`subscribe_whole_quote`, drains callbacks outside vendor threads and invalidates the complete
stream on any callback, calendar or subscription failure. It imports no XtTrader API and cannot
submit or cancel a real order.

Paper control reset now has a separate schema-v16 evidence path. A manual `unlock-paper` request
must take a fresh complete XtData snapshot during continuous trading, replay identical internal
and simulated-broker histories, persist a passing account reconciliation, advance the current
session-risk state and prove ownership of the running scheduler lease. PostgreSQL then atomically
rechecks the active strategy registration, exact session state, lease holder/token/generation and
three-second evidence age before resetting only the paper kill switch. The immutable unlock
artifact is never accepted by a live gateway.

The QMT trading-side read-only core now normalizes the documented `XtAsset`, `XtPosition`,
`XtOrder` and `XtTrade` fields through a Windows shim contract. A trusted baseline requires all
four queries to complete without an intervening callback; `None` never means an empty account.
Assets must balance to positions, order cumulative fills must converge with unique daily trades,
and unknown/inconsistent order states fail closed. A bounded callback cursor permits only normal
account-status heartbeats to retain the baseline; disconnects, gaps, order/trade changes or error
callbacks require a complete re-query. The resulting broker snapshot uses a logical account alias
and can feed the existing persisted reconciliation supervisor without storing the broker account
identifier in its snapshot payload. This remains unverified against a real Windows MiniQMT.

PostgreSQL schema v14 adds a separate single-owner paper-scheduler process lease. Advisory-lock
acquisition, short heartbeats, hashed bearer tokens, generation fencing, expiry takeover and
immutable acquire/release events prevent two scheduler processes from driving one account. The
resident runner activates the durable kill switch and stops if heartbeat ownership or clean
release cannot be proven.

Every continuous-auction strategy call now returns a versioned `PaperStrategyEvaluation`.
The evaluation binds the exact quote snapshot, strategy signal/state evidence and canonical
intent fingerprints. Its hash is mandatory for both order-producing and no-intent scheduler
cycles and is stored inside the immutable scheduler payload. A source that returns raw intents,
uses another strategy ID, changes the evaluation timestamp or fails to bind the quote snapshot
is rejected and activates the dependency kill switch. This makes “why nothing traded” auditable,
not only filled orders.

Before invoking that strategy, the scheduler now obtains `PaperStrategyAccountEvidence` under
the account coordination lock. It independently replays internal and simulated-broker histories,
projects both accounts at the exact quote marks, persists a reconciliation report, derives
session turnover from fill facts and advances the hash-chained daily risk state. The strategy
receives cash, positions, sellable quantities, open orders, exposure and daily risk metrics only
after reconciliation and a stable inactive control fence. Its evaluation hash must bind this
account-evidence hash as well as the quote hash. A mismatch activates
`reconciliation_failed`; a control change or missing session state fails closed.

The production intent adapter accepts an audited target portfolio rather than arbitrary broker
commands. A target signal fixes strategy/version/time, exact universe, per-instrument target
quantity, A-share rule version, risk-policy hash and upstream evidence hash. The adapter compares
it with the reconciled account, rounds buys to the configured minimum/step, caps every order,
never sells more than the currently sellable quantity, and generates deterministic client order
IDs from the signal and account evidence. An unreachable T+1 sell target remains an auditable
no-intent evaluation instead of creating an invalid order. Target providers from unregistered
strategies, times or universes are rejected before pre-trade risk.

The production pre-open reader now selects the latest single open session whose Tushare daily
close is point-in-time visible for every configured instrument. It rejects mixed-session marks,
future visibility, post-cutoff ingestion, stale valuation dates, unknown availability policies
and a current session that is not independently proven open. Its evidence hash commits the
calendar revisions, source revisions, ingestion times, valuation session and exact universe.
Every selected close and prior calendar revision must also belong to the exact
production-complete PostgreSQL manifest supplied to `paper-preopen-check`; the report and
underlying source-response hashes are replayed before marks are accepted. The command exercises
this path without returning prices and only while the durable kill switch is active. Resident
process assembly and an authorized Windows QMT quote process remain deployment gates; neither
paper submission nor live trading is enabled.

`refresh-trading-calendar` is a separate evidence path for an upcoming session. It calls only
Tushare `trade_cal`, requires exact day-by-day coverage, persists the redacted source response
hash and ClickHouse session revisions, then appends a PostgreSQL audit event. This avoids
requesting or blessing an unfinished current-day daily bar. Run it before the target pre-open
window; a calendar fetched after the fact cannot be backdated into an earlier check.

All dependency, test, lint, type-check, migration, and Git mutation commands for this
checkout must run on `rlocal`; see [the phase-1 runbook](docs/runbooks/phase1-data-foundation.md).

```bash
git clone https://github.com/jwzheng96/AutoQuant.git
cd AutoQuant
/Users/zjw/.local/bin/uv sync --frozen --all-groups
/Users/zjw/.local/bin/uv run autoquant config-check
/Users/zjw/.local/bin/uv run autoquant tushare-check
/Users/zjw/.local/bin/uv run autoquant qmt-check
# Refresh an upcoming session before its pre-open window:
/Users/zjw/.local/bin/uv run autoquant refresh-trading-calendar \
  --start 2026-07-24 --end 2026-07-24
# Refresh exact rules without requesting the unfinished session daily bar:
/Users/zjw/.local/bin/uv run autoquant refresh-session-reference \
  --instrument 600000.XSHG --date 2026-07-24
# Run during the target Shanghai pre-open window with the exact strategy universe:
/Users/zjw/.local/bin/uv run autoquant paper-preopen-check \
  --instrument 000001.XSHE --instrument 600000.XSHG \
  --manifest-hash <production-complete-tushare-manifest-hash>
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
submit bounded ingestion and fixed baseline backtest jobs. The execution core now includes
paper risk, reconciliation, persistent simulation, fenced scheduling, read-only QMT
convergence, and an immutable paper-strategy approval registry. Live trading remains hard
locked. No strategy may enter the paper scheduler until a completed walk-forward experiment
passes every sample-out gate and an operator explicitly approves the resulting paper-only
artifact. The current infrastructure canary is not profitable strategy evidence.

On an authorized Windows node, `autoquant qmt-readonly-accept` acquires a bounded QMT
session lease and persists only redacted schema v17 acceptance evidence after coherent
asset, position, order, and trade queries. It does not persist the broker account identifier
or expose any broker mutation method; live order submission and cancellation remain hard
locked. The authenticated trading console displays only the latest evidence time and
redacted record counts, current-host pass/blocked checks, and the remaining recovery gates.
Schema v18 adds bounded `qmt-drill-start` / `qmt-drill-complete` challenges for disconnect
and MiniQMT-restart drills. Completion requires a post-start fail-closed control event and
a distinct QMT acceptance captured after that failure; operator confirmation alone is not
sufficient.

`autoquant promotion-check` evaluates paper-to-live evidence from one PostgreSQL
repeatable-read, read-only snapshot. It emits policy, fact, and report hashes plus redacted
per-gate actual/required values; a blocked result exits nonzero. The authenticated trading
console shows the same report. This audit never enables live trading, and Windows recovery
drills plus an explicit compliance artifact remain hard blockers. See the
[paper promotion audit runbook](docs/runbooks/paper-promotion-audit.md).

Copy `.env.example` to an untracked `.env` and supply credentials/DSNs only on the trusted
runtime host. The Tushare Token previously shared in chat must be rotated before use; set
only the replacement as `AQ_TUSHARE_TOKEN`. Never commit or paste it into logs or chat.

The current Tushare path uses `daily`, `adj_factor`, `trade_cal`, `stock_basic`,
`suspend_d`, and the 2000-point `stk_limit` endpoint for exact historical daily price
boundaries. It does not call `stk_mins` and does not assume that a 2000-point account has
the independently licensed historical-minute permission. See the
[phase-1 runbook](docs/runbooks/phase1-data-foundation.md) for migrations and ingestion.

QMT must run on a separately controlled Windows machine with MiniQMT. See the
[QMT preparation runbook](docs/runbooks/qmt-read-only-preparation.md) and
[resident paper runtime runbook](docs/runbooks/resident-paper-runtime.md); no XtQuant package,
broker account, or MiniQMT process is expected on the current macOS development host.
