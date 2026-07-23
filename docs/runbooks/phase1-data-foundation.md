# Phase 1 Data Foundation Runbook

Run project tools only on `rlocal` in the project checkout.

## Reproducible setup and available checks

```bash
git clone https://github.com/jwzheng96/AutoQuant.git
cd AutoQuant
/Users/zjw/.local/bin/uv sync --frozen --all-groups
/Users/zjw/.local/bin/uv run autoquant config-check
/Users/zjw/.local/bin/uv run autoquant tushare-check
/Users/zjw/.local/bin/uv run pytest -m "not live" -q
/Users/zjw/.local/bin/uv run ruff check . && /Users/zjw/.local/bin/uv run mypy src
```

`config-check` reports all configured capabilities and exits nonzero if an intentionally
unused provider such as RQData is absent. Use `tushare-check` as the authoritative read-only
matrix for the Tushare daily endpoints.

Apply `migrations/postgres/001_phase1.sql`,
`migrations/postgres/002_operator_console.sql`, then
`migrations/postgres/003_revision_checkpoints.sql`, then
`migrations/postgres/004_backtest_runs.sql`, then
`migrations/postgres/005_walk_forward_validation.sql`, then
`migrations/postgres/006_validation_benchmark.sql`, then
`migrations/postgres/007_risk_decisions.sql`, then
`migrations/postgres/008_paper_execution.sql`, then
`migrations/postgres/009_execution_controls.sql`, then
`migrations/postgres/010_simulated_broker.sql`, then
`migrations/postgres/011_paper_session_risk.sql`, then
`migrations/postgres/012_qmt_session_leases.sql`, then
`migrations/postgres/013_paper_scheduler_events.sql`, then
`migrations/postgres/014_paper_scheduler_leases.sql`, then
`migrations/postgres/015_paper_strategy_registry.sql`, then
`migrations/postgres/016_paper_runtime_unlock.sql`, then
`migrations/postgres/017_qmt_readonly_acceptance.sql`, then
`migrations/postgres/018_qmt_recovery_drills.sql`, then
`migrations/clickhouse/001_phase1.sql` and
`migrations/clickhouse/002_tushare_daily.sql` and
`migrations/clickhouse/003_daily_coverage.sql` in order, only to explicitly authorized
phase-1 databases. Then run `autoquant db-check`.

For the repository's loopback-only Docker setup, keep database bootstrap secrets in the
ignored `infra/.env`, then run:

```bash
scripts/local-db.sh up
scripts/local-db.sh migrate
scripts/local-db.sh status
```

This Compose profile is for a single trusted development host. It binds database ports only
to loopback, but it is not a production HA deployment. Before any real-money phase, move
credentials to a secret manager, enable encrypted backups and restore drills, use TLS between
hosts, monitor disk/replication health, and define retention and disaster-recovery objectives.

Migration 015 adds the paper-only strategy registry. A strategy may be approved only from a
completed, integrity-checked walk-forward experiment whose evidence status is
`research_candidate` with no gate failures. Approval and revocation form an immutable hash
chain; replacing an active strategy requires an explicit revocation first. This registry
does not unlock live trading.

Configure only a newly rotated Token on the trusted host:

```bash
export AQ_TUSHARE_TOKEN='<enter locally; do not paste into chat>'
/Users/zjw/.local/bin/uv run autoquant tushare-check
```

When all six endpoints report `available` and both databases have the required schemas,
ingest a small explicit interval:

```bash
/Users/zjw/.local/bin/uv run autoquant ingest-daily \
  --instrument 000001.XSHE \
  --start 2020-01-02 \
  --end 2020-01-03
```

The command must produce `status=completed`, a quality hash, and a manifest hash. Any
permission denial, unexplained trading-day gap, missing database, or incomplete metadata
exits nonzero and must be investigated rather than bypassed.

## Opt-in external evidence

Vendor smoke tests never run by default. With credentials loaded only in the trusted remote
environment:

```bash
AQ_RUN_RQDATA_LIVE=1 /Users/zjw/.local/bin/uv run pytest tests/live/test_rqdata_readonly.py -q -rs
AQ_RUN_TUSHARE_LIVE=1 /Users/zjw/.local/bin/uv run pytest tests/live/test_tushare_readonly.py -q -rs
```

For the Tushare daily path, real Tushare, PostgreSQL, and ClickHouse checks are required
before it can be declared operational. A real end-to-end ingestion must produce a passing
quality report, immutable manifest, ClickHouse rows, separate daily/factor PostgreSQL
checkpoints, and an audit event. Credential-dependent or database-dependent skips are
incomplete evidence, not successes.

## Local operator console

Set the following only in the ignored root `.env`:

```dotenv
AQ_WEB_HOST=127.0.0.1
AQ_WEB_PORT=8000
AQ_WEB_USERNAME=operator
AQ_WEB_PASSWORD=<at least 16 characters; enter locally>
AQ_PAPER_ACCOUNT_ID=paper-main
AQ_PAPER_STRATEGY_ID=validated-sma-paper
AQ_PAPER_INITIAL_CASH=1000000
```

Start the console on `rlocal`:

```bash
/Users/zjw/.local/bin/uv run autoquant serve-web
```

Open `http://127.0.0.1:8000` on that Mac. If the browser is on another trusted machine,
use an SSH local port forward instead of changing the bind address:

```bash
ssh -L 8000:127.0.0.1:8000 rlocal
```

The console exposes health, data coverage, point-in-time daily queries, bounded audited
ingestion jobs, and `/research`. The research page can only run the built-in
`manifest_buy_hold_v1` baseline against a production-complete manifest; it does not execute
uploaded code. Select a manifest, verify its instrument and assumptions, create the run, then
inspect its daily equity, fills/rejections, fees, and stable failure code. The trading view
intentionally has no order action.

The trading page and `GET /api/v1/risk` also report the fail-closed pre-trade risk status and
the count of hash-verified, immutable risk decisions. This is observability only. A risk
decision marked `accepted` authorizes neither broker submission nor live trading; the paper
account, quote adapter, reconciliation loop, failure drills, and QMT gateway must be completed
as separate gates.

`GET /api/v1/execution` reports schema-v8 paper-order, event and reconciliation counts plus
the bounded restart-replay result. Console startup verifies persisted order projections from
their immutable event histories. Any mismatch aborts startup. This endpoint remains read-only;
it does not imply that a simulated broker, quote feed or scheduling loop is configured.

Schema v9 initializes the configured paper account kill switch in the active state. The trading
page may activate it with an authenticated, CSRF-protected command; there is intentionally no
Web reset endpoint. Reset code requires a matching optimistic version, a recent passing
reconciliation already persisted in schema v8, and a successful execution-history replay.
Setting `AQ_LIVE_TRADING_ENABLED=true` is rejected by configuration validation in this release.

Schema v10 adds a local deterministic simulated broker with no network client or real-money
capability. On startup the console replays its immutable broker facts independently from the
internal paper-order event chain. `/api/v1/execution` reports both recovery results. The adapter
is not an order endpoint. The account projector can independently rebuild both histories using
the configured initial cash, current marks, cumulative order fees and A-share T+1 lots, then
persist a hash-committed reconciliation. The internal coordinator serializes the full account
cycle with a PostgreSQL advisory lock, persists every risk decision, and uses the durable kill
switch state hash as a broker-transaction dispatch fence. A trusted session-risk-state source,
continuous quote adapter and scheduler must still be connected before paper submission can be
exposed.

Schema v11 persists a hash-chained paper-session risk state. It must be initialized from a
persisted, reconciled opening snapshot before any fill for that Shanghai session. The coordinator
derives cumulative turnover from immutable cumulative-fill deltas and advances peak equity from
persisted internal snapshots; callers cannot supply those three risk metrics. Until a pre-market
initializer and scheduler exist, a missing daily state intentionally activates the kill switch
and aborts submission.

Schema v12 persists bounded QMT session leases so two gateway processes cannot claim the same
XtQuant `session_id`. Acquisition is serialized in PostgreSQL; only a hash of the process-local
bearer token is stored. Renewals require an unexpired matching lease, while acquire/release
transitions are immutable audit events. This table is preparation for a Windows read-only
gateway and does not enable broker mutations.

Schema v13 persists hash-chained paper scheduler cycles. Market phase is derived from a
point-in-time calendar; only continuous-auction phases can request strategy intents. A complete
fresh quote snapshot, replayed daily session state, bounded strategy output and mandatory cycle
evidence sink are required. The console replays this chain at startup. External real-time quotes
and the resident scheduler runtime remain separate release gates.

The intent source boundary returns a `PaperStrategyEvaluation`, not a bare order tuple. It must
commit the registered strategy ID and implementation version, scheduler timestamp, exact quote
evidence hash, reconciled account-evidence hash, signal/state evidence hash and canonical intent
fingerprints. Before evaluation, the strategy-account reader independently rebuilds internal and
simulated-broker accounts from their fact streams, persists the reconciliation, derives turnover
from fill deltas and advances the session-risk chain under the account coordination lock. The
scheduler saves the resulting evaluation hash for both `no_intents` and `completed` cycles.
Missing or mismatched strategy/account evidence is an invalid scheduler input and fails closed.

Production strategy adapters must emit an exact `TargetPortfolioSignal`; they cannot submit raw
broker instructions. The target signal commits strategy/version/time, universe, target
quantities, rule versions, risk-policy hashes and upstream model evidence. The intent adapter
derives only the bounded difference from the reconciled account, enforces board-lot and
sellability constraints, and creates deterministic idempotency keys. T+1-unavailable quantities
remain pending target evidence with no order. The coordinator still independently reruns
reconciliation and the complete pre-trade risk engine before the simulated broker can accept an
intent.

Before assembling a resident paper runtime, set `AQ_ENVIRONMENT=paper`, leave
`AQ_LIVE_TRADING_ENABLED=false`, keep the account kill switch active, and run the read-only
calendar refresh ahead of the session:

```bash
uv run autoquant refresh-trading-calendar \
  --start 2026-07-24 \
  --end 2026-07-24
uv run autoquant refresh-session-reference \
  --instrument 600000.XSHG \
  --date 2026-07-24
```

The refresh interval is bounded to 32 calendar days and must contain exactly one vendor row per
calendar day. It calls no daily-price endpoint. Its source response, ClickHouse revisions and
PostgreSQL audit event must all persist before it reports `completed`. Run it before the target
pre-open; its real request timestamp is retained and cannot prove that a late refresh was known
earlier.

The session-reference command separately requires an open calendar row plus exact lifecycle,
suspension and `stk_limit` rows for every requested instrument. It never calls `daily` or
`adj_factor`. Missing or source-unbacked controls fail closed; the resulting revisions and
audit hash are the only accepted input to the current-session rule compiler.

After a walk-forward experiment is `completed`, reports `research_candidate`, has no gate
failures, and its deployment signal manifest is production-complete, approval is an explicit
paper-only operation:

```bash
uv run autoquant approve-paper-sma \
  --experiment-id <completed-experiment-uuid> \
  --signal-manifest-hash <current-production-complete-manifest-hash> \
  --reference-date 2026-07-23 \
  --approved-by operator \
  --confirm-paper-only
```

The configured account kill switch must already be active. The command re-verifies the
experiment, selects the modal training-fold parameters without using OOS returns for tuning,
reads exact session rules, checks the deployed allocation against the shared paper risk
policy, and appends an immutable approval event. It cannot approve a live strategy and cannot
reset the kill switch. Revoke before replacing an active artifact:

```bash
uv run autoquant revoke-paper-strategy \
  --revoked-by operator \
  --reason scheduled_research_refresh \
  --confirm-revoke
```

Then run the pre-open evidence gate during the target Shanghai pre-open window:

```bash
uv run autoquant paper-preopen-check \
  --instrument 000001.XSHE \
  --instrument 600000.XSHG \
  --manifest-hash <production-complete-tushare-manifest-hash>
```

Use the exact, complete strategy universe. The command reports only the instrument count,
valuation/session dates and evidence hashes; it never prints prices or credentials. A failure
means no scheduler should be started. The gate accepts only one prior open valuation session
covering the full universe and rejects future-visible or post-cutoff-ingested revisions. Because
the research availability policy may make the immediately preceding close unavailable before
09:30, the command can select an older complete visible session within the bounded four-day
calendar lag; it records that date explicitly rather than mixing dates or crossing the cutoff.
The supplied manifest must have passed a production-complete quality report for the exact
instrument set. Selected bars and the prior-session calendar must occur in its immutable record
hash set, and their persisted Tushare response evidence must replay successfully. Do not use a
quality-rejected ingestion result or a manifest from another universe.

The internal `PaperSessionInitializer` accepts only `pre_open`, requires a reconciled broker and
internal snapshot plus zero current-session fill turnover, and freezes the opening state
idempotently. It is not a manual bypass: no CLI or Web route exposes it, and production still
needs an exchange-calendar-aware scheduler. PostgreSQL integration drills verify recovery both
before initial broker dispatch and after broker facts are committed but before callbacks reach
the internal order chain.

Broker-disconnect injection verifies that a failed submit activates
`dependency_unavailable` while preserving the approved intent for audit. An injected broker
`unknown` fact is consumed and reconciled, then independently activates `order_state_unknown`;
operators must never reset that state merely because cash and positions happen to match.

## Research execution boundary

`autoquant.backtest` provides the deterministic cash-account ledger used by the research
phase. Its default fee model versions the 2022 transfer-fee and 2023 stamp-duty changes, but
the commission remains a reference assumption rather than a statement of the user's actual
broker tariff. Before comparing performance, configure the real commission schedule and keep
the rule, fee, execution-model, data-manifest and `as_of` versions with every result.

The current baseline fails closed when an adjustment factor changes because corporate-action
position and cash accounting is not implemented yet. A one-day positive baseline return proves
only that the pipeline works; it is not strategy evidence. Use multi-year walk-forward and
out-of-sample tests only after corporate actions and a real strategy interface are implemented.

The `/research` walk-forward form runs the built-in SMA-cross baseline only. Its candidates
use `fast/slow` session notation, are selected separately in every training window, and are
never reselected from the corresponding test result. Inspect absolute return, buy-and-hold
return, excess return, fold dispersion, drawdown, and the persisted evidence status together.
`research_candidate` means only that preliminary sample-size and performance filters passed;
it does not unlock paper trading or real orders.

Trading calendars, instrument lifecycles, suspension state and exact `stk_limit` boundaries are
persisted as point-in-time revisions and included in new validated manifests. Only manifests
created after schema v3 contain those constraints; older manifests remain immutable and must not
be silently upgraded. Daily OHLC still cannot reproduce limit-order queues; minute or tick replay
and simulation evidence remain required before any execution gateway is considered.
