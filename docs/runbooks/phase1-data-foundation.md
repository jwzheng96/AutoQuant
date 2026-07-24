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
`migrations/postgres/019_paper_portfolio_registry.sql`, then
`migrations/postgres/020_portfolio_oos_assessment.sql`, then
`migrations/postgres/021_validation_campaigns.sql`, then
`migrations/postgres/022_portfolio_validation.sql`, then
`migrations/postgres/023_research_universes.sql`, then
`migrations/postgres/024_research_data_campaigns.sql`, then
`migrations/postgres/025_dynamic_research_specs.sql`, then
`migrations/postgres/026_dynamic_validation_evidence.sql`, then
`migrations/postgres/027_dynamic_regime_research.sql`, then
`migrations/postgres/028_fundamental_research.sql`, then
`migrations/postgres/029_fundamental_dataset.sql`, then
`migrations/postgres/030_fundamental_panels.sql`, then
`migrations/postgres/031_fundamental_validation.sql`, then
`migrations/postgres/032_low_volatility_research.sql`, then
`migrations/postgres/033_low_volatility_validation.sql`, then
`migrations/postgres/034_low_volatility_forward_evidence.sql`, then
`migrations/postgres/035_low_volatility_forward_sessions.sql`, then
`migrations/postgres/036_paper_compliance_approvals.sql`, then
`migrations/postgres/037_qmt_canary_order_ledger.sql`, then
`migrations/clickhouse/001_phase1.sql` and
`migrations/clickhouse/002_tushare_daily.sql` and
`migrations/clickhouse/003_daily_coverage.sql` and
`migrations/clickhouse/004_fundamental_revisions.sql` in order, only to explicitly authorized
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

The research page also lists immutable fixed-factor validations from
`GET /api/v1/fundamental-validations`. Selecting one reads
`GET /api/v1/fundamental-validations/{result_hash}`, decodes every stored fold, and verifies the
result, assessment, fold, and backtest artifact hashes before displaying detail. Strategy and
benchmark unresolved positions are shown separately for diagnosis. Both endpoints are
authenticated and read-only; there is no approval, rerun, paper-promotion, or trading mutation
behind this view.

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

For portfolio research, first ingest one production-complete manifest that exactly
covers 3-20 instruments at a common cutoff. Queue all component validations
atomically with identical capital, allocation, costs, fold geometry and candidate
grid:

```bash
uv run autoquant validation-campaign-create \
  --campaign-key portfolio-research-20260723-0001 \
  --manifest-hash <exact-multi-instrument-manifest> \
  --instrument 000001.XSHE \
  --instrument 600000.XSHG \
  --instrument 600519.XSHG \
  --candidate 5:20 \
  --candidate 10:30 \
  --candidate 20:60 \
  --allocation 0.20 \
  --slippage-bps 5 \
  --train-sessions 120 \
  --test-sessions 20 \
  --embargo-sessions 1 \
  --requested-by operator
```

Creation fails before queuing anything unless adjusted bar/factor keys are exact,
the intersection of real (never synthesized) market dates covers at least 98% of
the longest component history, and that common calendar covers at least six OOS
folds. Every component must also afford at least one board lot within both its
allocation and the paper policy's order-notional cap. Every component is trained
and scored on this same calendar. The campaign
and component mappings are immutable, while component state is derived from the
existing validation experiment records. Run `serve-web` to process the queue; the
research console shows read-only campaign progress. Inspect the same redacted
aggregate status with:

```bash
uv run autoquant validation-campaign-status \
  --campaign-hash <campaign-hash>
```

Run the portfolio as one cross-sectional strategy, rather than treating
single-security validations as independent evidence:

```bash
uv run autoquant portfolio-validation-create \
  --manifest-hash <exact-multi-instrument-manifest> \
  --idempotency-key portfolio-research-20260723-v1 \
  --candidate 20:5:3 \
  --candidate 60:10:3 \
  --candidate 120:20:3 \
  --gross-allocation 0.29 \
  --maximum-order-notional 100000 \
  --train-sessions 252 \
  --test-sessions 21 \
  --embargo-sessions 1 \
  --requested-by operator
```

`serve-web` claims this queue with restart recovery and up to three transient
dependency attempts. It selects momentum parameters inside each training
window, evaluates the following embargoed test window, and compares it with an
equal-weight buy-and-hold benchmark on the same common calendar. Full training,
test and benchmark ledgers are stored per fold under immutable triggers.
Inspect and re-verify the artifact with:

```bash
uv run autoquant portfolio-validation-status \
  --experiment-id <experiment-uuid>
```

The status command exits nonzero when the experiment fails or its evidence is
not a `research_candidate`. A candidate still cannot approve paper or live
trading; real trading remains hard locked.

Completed portfolio details also derive a versioned diagnostic artifact from
the immutable folds. It reports per-fold benchmark hit rate, median and
positive/nonpositive excess magnitude, first-half versus second-half excess,
turnover, fee drag and parameter-selection frequency. The diagnostic has its
own hash and always reports `oos_tuning_permitted=false`: it may explain a
failure or motivate a separately pre-registered strategy version, but must
never be used to alter the completed experiment or select parameters for the
same OOS sample.

Build a source-backed historical universe before expanding portfolio research:

```bash
uv run autoquant universe-snapshot-create \
  --index-code 399300.SZ \
  --reference-date 2026-07-22 \
  --minimum-turnover-rate-f 0 \
  --minimum-circulating-market-value 0 \
  --requested-by operator
```

The command searches only backward for the latest monthly index constituent
cross-section, requires an exact daily liquidity cross-section no later than
the reference date, and atomically persists both raw source responses, the
canonical 250-350 member snapshot and its audit event. PostgreSQL schema v23
binds both evidence hashes by foreign key and makes the snapshot append-only.
The artifact is research-only and cannot unlock live trading.

Backfill at most 12 month-end snapshots per invocation:

```bash
uv run autoquant universe-snapshot-backfill \
  --index-code 399300.SZ \
  --start-month 2025-08-01 \
  --end-month 2026-07-01 \
  --requested-by operator
```

Bounds must be first days of months. The current month is capped at the
previous Shanghai calendar date, so future planned trading sessions can never
become research cutoffs. Schema v23 uniquely identifies a snapshot by policy
hash and reference date; reruns return existing artifacts instead of creating
new samples with later request timestamps. Completed months remain committed
if a later month fails, and a rerun safely resumes them.

After every month in the intended research interval exists, freeze the
survivorship-free daily collection plan:

```bash
uv run autoquant research-data-campaign-create \
  --campaign-key csi300-survivorship-free-202001-202607-v1 \
  --index-code 399300.SZ \
  --start 2020-01-01 \
  --end 2026-07-22 \
  --requested-by operator
```

Schema v24 requires exactly one snapshot for every calendar month and one
universe policy across the interval. It takes the union of all historical
members, freezes one hash-addressed shard per instrument, and keeps live
trading locked. Run only a bounded, sequential batch at a time:

```bash
uv run autoquant research-data-campaign-run \
  --campaign-hash <campaign-hash> \
  --max-items 10 \
  --pause-seconds 1

uv run autoquant research-data-campaign-status \
  --campaign-hash <campaign-hash>
```

Each shard covers the same full interval, passes the existing daily quality
gate, and points to a production-complete immutable dataset manifest. Calendar
and lifecycle responses are cached only inside one worker invocation; market,
factor, suspension and price-limit calls remain per instrument. The queue uses
row locks, persists attempts, and recovers interrupted `running` items on the
next invocation. Vendor response-shape and quality failures stop in a terminal
state instead of being skipped. After the cause is understood and corrected,
explicitly audit a bounded retry:

Before claiming each shard, the worker measures inactive MergeTree parts for
the six AutoQuant daily tables. It returns `maintenance_wait=true` without
claiming another item when either `AQ_RESEARCH_DATA_MAX_INACTIVE_BYTES`
(default 6 GiB) or `AQ_RESEARCH_DATA_MAX_INACTIVE_PARTS` (default 24,000) is
reached. Let ClickHouse remove old parts before rerunning; do not bypass the
guard by deleting storage directories or weakening merge durability.

```bash
uv run autoquant research-data-campaign-retry \
  --campaign-hash <campaign-hash> \
  --sequence <failed-sequence> \
  --authorized-by operator \
  --confirm-data-retry
```

When every shard is complete, the worker creates one immutable aggregate
research manifest binding the policy hash, all monthly snapshot hashes and all
daily shard manifest hashes. Until that aggregate exists, expanded portfolio
validation must not start.

Compile the completed aggregate into a deterministic point-in-time input plan:

```bash
uv run autoquant research-input-plan-compile \
  --manifest-hash <aggregate-research-manifest-hash> \
  --requested-by operator
```

The compiler cross-checks the aggregate payload against its normalized shard
rows, reloads every immutable universe snapshot, requires exactly one snapshot
per calendar month, and requires the union of historical members to equal the
shard instruments. Membership activates only when
`session_date > snapshot.reference_date`; the snapshot date itself is excluded
to prevent same-day constituent and liquidity lookahead. The deterministic
plan hash and rule version are appended to the immutable audit log. This
command never enables paper or live execution.

Before building portfolio features, verify representative shards end to end:

```bash
uv run autoquant research-input-shard-check \
  --manifest-hash <aggregate-research-manifest-hash> \
  --instrument 000001.XSHE \
  --requested-by operator
```

This reads only the requested shard. It verifies the daily manifest identity,
source, production-complete flag, instrument and interval, then reuses the
quality-report and exact record-hash reader against ClickHouse. The result is
appended to the immutable audit chain. Downstream research code should use the
same lazy reader rather than loading the entire historical union into memory.

Before running any dynamic-universe validation, freeze the strategy, execution
assumptions, fold construction and evidence gates:

```bash
uv run autoquant dynamic-research-spec-freeze \
  --manifest-hash <aggregate-research-manifest-hash> \
  --requested-by operator \
  --confirm-pre-registration
```

The v1 specification fixes 50% gross allocation across ten selected names,
5% maximum position weight, 10 bps slippage, 5% maximum daily volume
participation, one-session signal lag, 252-session new-member history, and
504/63/5-session train/test/embargo windows. Candidate lookback/rebalance
pairs are fixed at 20/5, 60/10, 120/20 and 252/21. Evidence requires at least
eight folds and 504 out-of-sample sessions, at least 55% profitable folds,
positive compounded and benchmark-relative OOS returns, no more than 18%
drawdown, no more than 15% selection optimism, and zero rejected orders.

The database permits only one specification for the same aggregate manifest
and strategy identity. The row and its full canonical payload are immutable;
running the command again can only return the original specification. Do not
change these values after observing validation results. A failed specification
means the strategy is rejected, not retuned against the same OOS sample.

Compile the full market panel only from the frozen specification:

```bash
uv run autoquant dynamic-market-panel-compile \
  --spec-hash <frozen-dynamic-spec-hash> \
  --requested-by operator
```

The compiler streams and verifies all shards, requires the exact same open
session calendar in every shard, and binds each session to the most recent
strictly-prior universe snapshot. It retains only observed market states;
missing bars on suspended or otherwise non-tradable sessions are not
fabricated. The panel summary and deterministic hash are appended to the
immutable audit chain, and execution remains locked.

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

For the diversified runtime, approve 3-20 unique instruments together. Each
`--experiment-id` is positionally paired with the following repeated
`--signal-manifest-hash`; every pair must independently pass the same OOS checks.
The valuation manifest is a separate production-complete manifest whose universe
must exactly equal all component instruments:

```bash
uv run autoquant approve-paper-portfolio \
  --experiment-id <experiment-a> \
  --signal-manifest-hash <signal-manifest-a> \
  --experiment-id <experiment-b> \
  --signal-manifest-hash <signal-manifest-b> \
  --experiment-id <experiment-c> \
  --signal-manifest-hash <signal-manifest-c> \
  --valuation-manifest-hash <exact-three-instrument-manifest> \
  --reference-date 2026-07-23 \
  --approved-by operator \
  --confirm-paper-only
```

Component allocations must each stay within the position cap and their sum must
stay within the gross-exposure cap. Single and portfolio deployments are mutually
exclusive. Portfolio approval also requires at least six exactly aligned OOS
folds generated with the configured paper initial capital and one shared
point-in-time cutoff, positive compounded portfolio return, at least 50%
profitable folds, a
conservative drawdown bound no greater than 12%, maximum pairwise component
correlation no greater than 0.85, and maximum absolute component return
contribution no greater than 65%. The immutable assessment hash is part of the
portfolio registration and is shown in the operator console. The same revocation
command handles either kind:

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

The frozen dynamic-universe campaign uses `dynamic-market-panel-compile` as a read-only
integrity rehearsal and `dynamic-validation-run` as the authoritative nested walk-forward job.
The latter rebuilds the same panel from all immutable shards, evaluates every frozen candidate
in every training fold, applies the embargo, runs disjoint test folds, compares against the
point-in-time quarterly equal-weight benchmark, and atomically stores every candidate and fold
artifact. It always reports `live_trading_locked: true`; even `research_candidate` is only
permission to begin the minimum 60-session paper observation and never enables real orders.

## Point-in-time fundamental research

ClickHouse schema v4 adds append-only `daily_valuation_revisions` and
`financial_indicator_revisions`. Apply `migrations/clickhouse/004_fundamental_revisions.sql`
after the prior ClickHouse migrations. PostgreSQL schema v28 adds the immutable v3 research
specification; apply `migrations/postgres/028_fundamental_research.sql` after v27.

Verify permissions with `tushare-check`. It now probes the daily market endpoints plus
`daily_basic`, `fina_indicator`, `income`, `balancesheet`, and `cashflow` without writing data.
Never paste the token into a command or commit it; load `AQ_TUSHARE_TOKEN` from the local
`.env`.

An explicit bounded ingestion is:

```bash
uv run autoquant ingest-fundamental \
  --instrument 600519.XSHG \
  --start 2024-01-01 \
  --end 2024-12-31
```

The adapter normalizes Tushare market values from CNY 10,000 to CNY. A financial row is keyed
by its report period and vendor update flag but is not visible on the report-period date. It
becomes signal-eligible only at the next exchange open after `ann_date`. Because Tushare filters
`fina_indicator` ranges by report period, the adapter prefetches a fixed 550-day report-period
lookback and then clips by announcement date. Both `available_at` and `ingested_at` are enforced
when a frozen manifest is read.

An all-null `fina_indicator` row, or one without `ann_date`, is retained inside its redacted
source evidence but is not converted into a numerical record; no zero or publication date is
fabricated. Such an instrument is ineligible on dates where the five frozen factor inputs are
incomplete. Dataset acceptance therefore proves the response and timestamp chain, while panel
compilation separately enforces the v3 minimum eligible-universe size.

Before full-universe ingestion, freeze the factor hypothesis from the rejected v2 result:

```bash
uv run autoquant fundamental-spec-freeze \
  --predecessor-result-hash <immutable-rejected-v2-result-hash> \
  --requested-by operator
```

The v3 spec fixes five equal-weight percentile factors (earnings yield, book-to-price, diluted
ROE, ROA, and operating-cash-flow-to-revenue), a 21-session rebalance, 20 holdings, one-session
signal lag, and the existing conservative risk/evidence gates. There is no candidate parameter
search. Do not change those values after inspecting outcomes; a different hypothesis requires a
new version and a new independent test. Fundamental ingestion and even a passing backtest do not
unlock real trading.

Full-universe collection is bounded and restart-safe. Each successful instrument is first
frozen as its own production-complete generic manifest. Schema v29 indexes the exact shard union
only after every instrument in the predecessor daily dataset is present:

```bash
uv run autoquant fundamental-data-status \
  --spec-hash <frozen-v3-spec-hash>

uv run autoquant fundamental-data-run \
  --spec-hash <frozen-v3-spec-hash> \
  --max-items 10 \
  --pause-seconds 0
```

`--max-items` is limited to 25. Repeating the command skips completed instruments; a worker or
terminal failure therefore cannot silently bless a partial union. `dataset_manifest_hash`
remains `null` until all shards pass and the normalized shard references are atomically frozen.

After the aggregate manifest is non-null, compile and freeze the v3 feature panel:

```bash
uv run autoquant fundamental-panel-compile \
  --spec-hash <frozen-v3-spec-hash> \
  --requested-by operator
```

The compiler verifies every daily shard's immutable manifest binding and every fundamental
shard's complete unique record-hash set. It reads the trading calendar once at the oldest daily
manifest cutoff; price, suspension, and price-limit rows are deliberately deferred to the
execution backtest where they are actually consumed. Each execution session uses the previous
exchange session's valuation and only financial reports visible by that execution session's
09:30 open. The panel records eligible and insufficient session counts; sessions below the
frozen 60-member threshold cannot emit a portfolio signal. The command never changes the
live-trading lock.

After schema v31 is applied, run the pre-registered validation exactly once:

```bash
uv run autoquant fundamental-validation-run \
  --spec-hash <frozen-v3-spec-hash> \
  --requested-by operator
```

The job rebuilds the frozen feature panel and requires the same panel hash before it reads
returns. Executable daily rows are loaded in bounded four-instrument batches. Each batch reads
the immutable rows named by each manifest, discovers the corresponding calendar evidence
without assuming equal row counts between endpoints, and verifies the original per-manifest
record order before compiling adjusted market states. The fixed strategy uses no candidate
search. It runs 504-session training windows, a five-session embargo, and independent
63-session test folds against the point-in-time quarterly equal-weight benchmark.

Schema v31 stores every training, test, and benchmark ledger as immutable JSON evidence plus
the fold hashes and aggregate assessment. A candidate must pass the frozen return, excess
return, profitable-fold, drawdown, train/test-gap, execution-rejection, sample-size, and
zero-unresolved-position gates. A rerun for an already stored spec returns the existing
immutable result. A passing result is only eligible for a separately controlled paper-trading
stage; it does not unlock paper or live execution.

The frozen v3 run completed with result hash
`fae7de9eab5868c3e1c9c0fc5ddac1b26a109ea75d35d0d1840d1b12a9b48f59` and was rejected.
Across 16 folds and 1,008 out-of-sample sessions, the strategy returned
`0.053454889625941883535723882`, the point-in-time equal-weight benchmark returned
`0.087692116199324626807795868`, and excess return was
`-0.034237226573382743272071986`. The profitable-fold rate was `0.5625`, worst test
drawdown was `0.07933934813208103801632638192`, and no order was rejected. Four unresolved
end positions belonged to the benchmark, not the strategy. The independent negative-excess
gate still rejects v3 even if benchmark liquidation diagnostics are separated, so v3 must not
enter paper trading or be tuned using these out-of-sample results.

## Pre-registered low-volatility v4 research

The rejected v3 result may seed exactly one independent v4 specification after PostgreSQL
schema v32 is applied:

```bash
uv run autoquant low-volatility-spec-freeze \
  --predecessor-result-hash \
  fae7de9eab5868c3e1c9c0fc5ddac1b26a109ea75d35d0d1840d1b12a9b48f59 \
  --requested-by operator
```

The economic hypothesis comes from Blitz, Hanauer, and van Vliet,
[“The Volatility Effect in China”](https://doi.org/10.1057/s41260-021-00218-0).
The paper reports a distinct, robust, investable low-risk effect in local China A shares,
including similar results across shorter and longer estimation periods. AutoQuant chooses one
implementation before observing v4 returns: trailing volatility over 252 daily returns,
253 required close observations, a 21-session rebalance, the 20 lowest-volatility eligible
members, 50% gross allocation, 5% maximum position weight, and a one-session signal lag.

The specification reuses the immutable point-in-time daily dataset and universe plan, applies
the existing 10 bp slippage, order-notional and 5% volume-participation limits, and fixes
504-session training, a five-session embargo, and 63-session tests. There is no candidate grid,
parameter search, fundamental factor, momentum overlay, or permission to inspect returns while
the implementation is being built. Schema v32 freezes only this design; it does not make v4 a
candidate, start paper trading, or unlock live execution.

After schema v33 is applied and the fixed implementation is committed, run the official v4
validation once:

```bash
uv run autoquant low-volatility-validation-run \
  --spec-hash \
  1a53d81f60c6443bebcb4583b1ef559484bdf866063acf0f6ee0ee091eb2a4c7 \
  --requested-by operator
```

The compiler loads the exact immutable daily manifests and permits a signal only when an
instrument has all 253 consecutive market observations ending on the previous session. It
ranks the resulting 252 daily returns without filling gaps. The validator uses the frozen
504/5/63 walk-forward schedule and quarterly point-in-time equal-weight benchmark. Schema v33
stores full training, test, and benchmark ledgers plus hash-verified folds. Strategy order
rejections and unresolved positions are evidence gates; benchmark liquidation anomalies remain
visible diagnostics but cannot falsely reject the strategy. A passing result remains live
locked and can only seed a separately controlled paper-trading stage.

The official run completed with result hash
`f3a9acca898d30ef0672a16f43a73fb20644d8f19a6603c026a58a8a868f2a92` and assessment hash
`85e6af23f80768a56915efa131b94fb842d991cb74006a068f496b7be8771ef5`. Across 12 folds and
756 out-of-sample sessions, v4 returned `0.21904943562578038236052043`, the benchmark returned
`0.098579794526777702963044235`, and excess return was
`0.120469641099002679397476195`. Its profitable-fold rate was `0.75`, worst test drawdown was
`0.05356652584954738693057764518`, and the strategy had no rejected orders or unresolved
positions. Three unresolved positions belonged only to the benchmark.

The frozen assessment is still `rejected` because `train_test_gap` was `0.1513014875`.
That gate compares total returns from unequal 504-session training and 63-session test
intervals, so a future methodology version must pre-register a horizon-normalized comparison.
This observation cannot retroactively change, overwrite, or promote v4; any revised method
requires separately frozen evidence and a new forward-data requirement. Live trading remains
locked.

For each completed forward trading session, create a queue only after the following Shanghai
calendar date has begun. The command selects the latest matching universe snapshot whose
reference date is strictly earlier than the session:

```bash
uv run autoquant low-volatility-forward-session-create \
  --forward-spec-hash \
  ae3d74b1a35d700efea01310bd80e8fd1264eabe6c569f83d9628670a85e34f0 \
  --session 2026-07-23 \
  --requested-by operator
```

Run the returned campaign in bounded, restart-safe batches:

```bash
uv run autoquant research-data-campaign-run \
  --campaign-hash <returned-campaign-hash> \
  --max-items 10 \
  --pause-seconds 0.25
```

Repeat until `research-data-campaign-status` reports `completed`. Each instrument receives its
own production-complete manifest and retry budget, so a vendor or network failure does not
discard other completed shards. Never create a queue for the current Shanghai trading date or
use a universe snapshot dated on or after the target session.

After completion, freeze the aggregate manifest into the forward evidence ledger:

```bash
uv run autoquant low-volatility-forward-session-finalize \
  --forward-spec-hash \
  ae3d74b1a35d700efea01310bd80e8fd1264eabe6c569f83d9628670a85e34f0 \
  --dataset-manifest-hash <completed-dataset-manifest-hash> \
  --requested-by operator
```

Schema v35 independently re-reads all shard manifests, proves exact single-session coverage,
uses the earliest shard cutoff to verify that the date was already known to be an open trading
session, and binds the aggregate manifest to the strictly earlier universe snapshot. The
binding is immutable and idempotent by forward specification and session date. Any later
snapshot, incomplete shard, wrong policy, changed instrument set, or different second manifest
for the same session is rejected. This evidence collection does not unlock paper or live
execution.

The first official binding covers 2026-07-23 and 300 instruments. Its immutable binding hash is
`2ef9a973f10b9c5c54a8589b600bd12336a3f9d1d136224d4e7671056a66557d`, backed by aggregate
dataset manifest
`dc0156eb6e042a0c7fdb3d2adf78972013a152b9f3264c1aa85476b77c470878` and the 2026-07-22
snapshot `28e2fe1da8855c67bb0503f25d39f6fbb093bcd1ebedf0c2b2f10a632640ed9b`. It is session 1
of the required 126; live trading remains locked.

The authenticated operator console exposes the same ledger read-only at
`GET /api/v1/low-volatility-forward-progress` and on the research page. It reports the latest
safe Shanghai cutoff, completed bindings, missing already-open sessions, calendar revisions,
the remaining forward-session count, and the still-locked 60-session paper gate. At the first
binding the verified state is `collecting_forward_sessions`, `1/126`, with no missing session
or calendar conflict.

For unattended, bounded progress, invoke one collection window after the conservative cutoff has
advanced (for example at 06:30 Shanghai time each day):

```bash
uv run autoquant low-volatility-forward-window-run \
  --forward-spec-hash \
  ae3d74b1a35d700efea01310bd80e8fd1264eabe6c569f83d9628670a85e34f0 \
  --requested-by forward-collector \
  --max-cycles 20 \
  --max-items 25 \
  --pause-seconds 1.25 \
  --interval-seconds 5
```

The window makes at most 20 cycle attempts and each attempt processes at most 25 persistent queue
items. It continues only while the cycle reports `batch_progress`, and stops immediately after
one session freezes, no completed session is eligible, the 126-session gate is complete, or a
terminal failure appears. Exhausting all attempts returns `window_exhausted` and exit code 2 so
an external scheduler can alert instead of treating partial progress as success. A calendar
conflict or terminal shard failure still requires investigation and the separate explicitly
authorized retry command. Schedule this command on one host; do not wrap it in a permanent tight
loop. The original `low-volatility-forward-cycle-run` remains available for one-attempt
diagnostics.

## Future-only low-volatility evidence correction

After schema v34 is applied, freeze the methodology correction once:

```bash
uv run autoquant low-volatility-forward-spec-freeze \
  --predecessor-result-hash \
  f3a9acca898d30ef0672a16f43a73fb20644d8f19a6603c026a58a8a868f2a92 \
  --requested-by operator
```

The specification keeps every v4 portfolio, signal, rebalance, cost and risk parameter
unchanged. It replaces only the invalid comparison of unequal holding-period totals with
annualized geometric returns:

The official immutable forward-spec hash is
`ae3d74b1a35d700efea01310bd80e8fd1264eabe6c569f83d9628670a85e34f0`; its state is
`frozen_awaiting_forward_data`.

```text
annualized_return = exp(252 / sessions * ln(1 + total_return)) - 1
stability_gap = annualized_training_return - annualized_forward_return
```

The record discloses that the outcome was observed before this correction and that v4 was the
fourth formal hypothesis. It sets `retrospective_reclassification_allowed=false` and
`historical_result_eligible_for_promotion=false`, so neither the corrected formula nor a
retrospective calculation can turn the rejected v4 record into a paper candidate.

Only sessions strictly after the frozen historical dataset ending 2026-07-22 may enter the new
forward ledger. At least 126 forward sessions in six non-overlapping 21-session blocks are
required, with positive compounded and excess return, at least a 55% profitable-block rate,
no more than 18% drawdown, zero rejected orders, zero unresolved strategy positions, and an
annualized stability gap no greater than the unchanged 0.15 threshold. Passing those gates
still requires at least 60 separately controlled paper sessions. The rationale follows the
warning that repeated historical trials increase backtest-overfitting risk:
<https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253>.

After the progress endpoint reports exactly
`session_gate_complete_awaiting_evaluation`, create the deterministic schema-v45 full-window
market-coverage campaign:

```bash
uv run autoquant low-volatility-forward-evaluation-data-create \
  --forward-spec-hash \
  ae3d74b1a35d700efea01310bd80e8fd1264eabe6c569f83d9628670a85e34f0 \
  --requested-by research-operator
```

Run the returned campaign with the existing bounded `research-data-run` command until it returns
its aggregate `dataset_manifest_hash`. The campaign interval is exactly the first 126 frozen
sessions and its instruments are the sorted union of those 126 immutable point-in-time
universes. This extra coverage is required so a held name remains observable and sellable after
it leaves a later universe; it does not alter which names were active on any signal date.

Then run the one-shot evaluation:

```bash
uv run autoquant low-volatility-forward-evaluate \
  --forward-spec-hash \
  ae3d74b1a35d700efea01310bd80e8fd1264eabe6c569f83d9628670a85e34f0 \
  --evaluation-dataset-manifest-hash \
  <completed-evaluation-dataset-manifest-hash> \
  --requested-by research-operator
```

Do not schedule either command before the gate completes. Both deliberately exit nonzero without
creating a partial artifact when fewer than 126 frozen sessions exist. The evaluator always
selects the first 126 ordered bindings from the pre-registered start date; later sessions cannot
replace an unfavorable day. It independently re-reads exact manifest rows, verifies the
evaluation dataset interval, snapshot set and deterministic instrument union, combines the
frozen historical lookback with the forward prefix, preserves the v4 parameters, runs one
continuous strategy and benchmark interval, derives six non-overlapping 21-session returns from
continuous equity, and stores both complete backtest artifacts.

The immutable result is either `paper_candidate` or `rejected`. A candidate changes the console
state to `forward_evaluation_passed_awaiting_paper_approval`, but schema v44 still enforces
`paper_deployment_allowed=false` and `live_trading_locked=true`. A rejected result cannot be
edited, deleted, re-windowed, or promoted. No command in this stage activates paper or live
trading.

If and only if the immutable assessment is `paper_candidate`, switch the process configuration
to `AQ_ENVIRONMENT=paper` while keeping `AQ_LIVE_TRADING_ENABLED=false` and the account kill
switch active. Record the independent paper-only decision with:

```bash
uv run autoquant approve-paper-low-volatility-candidate \
  --evaluation-result-hash <passing-forward-evaluation-result-hash> \
  --approved-by risk-operator \
  --confirm-paper-only
```

Schema v46 verifies the exact result, assessment, source specification, evaluation dataset and
completion time again inside PostgreSQL before accepting the append-only row. A successful
response is `approved_awaiting_daily_signal_runtime`; it is not an active paper deployment.
The row hard-codes a 60-session minimum, requires immutable daily signal evidence, sets
`runtime_activation_allowed=false`, and leaves live trading locked. The existing paper runtime
registry and unlock service cannot see this candidate.

Revoke an active candidate before replacing it or whenever its evidence or risk design is no
longer acceptable:

```bash
uv run autoquant revoke-paper-low-volatility-candidate \
  --revoked-by risk-operator \
  --reason operator_safety_action \
  --confirm-revoke
```

Allowed reasons are `evidence_invalidated`, `risk_changed`, `runtime_design_changed`, and
`operator_safety_action`. Revocation appends a second immutable artifact and performs no broker
operation. Do not attempt candidate approval before a passing 126-session evaluation exists;
the command exits nonzero without creating an approval.

### Daily paper observation evidence

After candidate approval, create data for a future open session before 09:30 Asia/Shanghai. The
snapshot must use the frozen universe policy, have a reference date strictly before the target
session, and contain only instruments inside the candidate's approved risk universe:

```bash
uv run autoquant low-volatility-paper-signal-data-create \
  --session-date 2026-07-27 \
  --snapshot-hash <point-in-time-universe-snapshot-hash> \
  --requested-by paper-operator \
  --confirm-paper-only
```

The command derives the last 253 open sessions from hash-backed Tushare calendar evidence. Its
campaign includes the current point-in-time members plus any selection carried from the prior
paper session, so removed holdings keep valuation coverage. Run the returned campaign with
`research-data-run` until it produces an aggregate dataset manifest.

Then freeze the observation:

```bash
uv run autoquant low-volatility-paper-signal-prepare \
  --session-date 2026-07-27 \
  --dataset-manifest-hash <completed-signal-dataset-manifest-hash> \
  --prepared-by paper-operator \
  --confirm-observation-only
```

Schema v47 reconstructs every exact shard, requires one shared 253-session calendar, calculates
the unchanged 252-return trailing volatility only from dates before the target session, binds
prior-close valuations and exact current-session reference rules, and appends a hash chain with
a fixed 21-session rebalance cadence. Fewer than 60 eligible members produces an empty
selection; otherwise a rebalance row contains exactly 20 names.

This is intentionally an observation-only stage. The historical execution simulator uses the
completed execution day's volume and high/low path to model participation, limit locks and fill
prices. Those values are legitimate ex-post simulation inputs but are not available to a
pre-open paper decision. Every v47 row therefore hard-codes
`execution_timing_compatible=false`, `runtime_activation_allowed=false`, and
`live_trading_locked=true`. Do not treat a prepared signal as a scheduler deployment. A
separately versioned, decision-time execution model must be validated before that lock can be
changed in a later schema.

### Decision-time execution compatibility

Schema v48 freezes the execution-compatibility methodology before the terminal forward outcome
is known:

```bash
uv run autoquant low-volatility-execution-compatibility-freeze \
  --forward-spec-hash \
  ae3d74b1a35d700efea01310bd80e8fd1264eabe6c569f83d9628670a85e34f0 \
  --requested-by risk-auditor \
  --confirm-decision-time-audit
```

The official immutable compatibility-spec hash is
`7da729b9dfe7b9035b9552236a6c1c80e4f5d814c3166a35bf57f234c997102a`.
It was frozen with 1 of 126 forward sessions already observed, so
`partial_outcome_observed_before_freeze=true`; no terminal evaluation existed, so
`terminal_outcome_observed_before_freeze=false`. PostgreSQL rejects a first freeze at 126
sessions or after a terminal evaluation exists.

The policy `low-volatility-prior-close-order-intents-v1` may use only prior adjusted close,
prior-session volume, exact pre-open instrument rules and pre-open suspension status. It may not
read execution-day open, high, low, close or final volume while constructing an order. Unit
tests perturb all five forbidden values and require identical order intents. The completed day
may still be read later by `daily-open-conservative-v1` to simulate fills, non-fills and
rejections; this keeps decision data and ex-post execution outcomes in separate stages.

This compatibility audit is disqualifying-only. It must reuse the exact first 126 frozen
sessions and unchanged signal/risk parameters. A failure blocks paper promotion; a pass cannot
rescue a rejected original evaluation, change historical classification, activate the runtime,
or unlock live trading.

Schema v49 implements that terminal audit:

```bash
uv run autoquant low-volatility-execution-compatibility-evaluate \
  --compatibility-spec-hash \
  7da729b9dfe7b9035b9552236a6c1c80e4f5d814c3166a35bf57f234c997102a \
  --requested-by risk-auditor
```

Run it only after `low-volatility-forward-evaluate` has stored a passing 126-session
`paper_candidate`. Before then it exits nonzero with a redacted fail-closed error and writes no
row. It refuses to run if the original evaluation was rejected, so the corrected intent policy
cannot rescue failed research.

At terminal time the command reproduces the exact source and forward manifests, point-in-time
universe bindings, panel hash, market-panel hash, and contiguous 126-session prefix. It reuses
the original benchmark and all frozen thresholds, then persists the corrected strategy's full
reports, snapshots, ledger events, assessment hashes, and gate failures in an append-only row.
Database constraints require the original row to remain a locked paper candidate and make the
compatibility result immutable.

A `compatible` result only resolves the decision-time research question. The v49 result still
hard-codes `paper_activation_allowed=false`, `runtime_activation_allowed=false`, and
`live_trading_locked=true`. Paper deployment needs a later, separately reviewed schema that
binds this result to daily signal evidence, session risk controls, and an explicitly approved
paper target; live trading remains out of scope.

Inspect the current deployment blockers without changing state:

```bash
uv run autoquant low-volatility-paper-deployment-status \
  --session-date 2026-07-27
```

This command requires `AQ_ENVIRONMENT=paper`, emits only hashes and blocker codes, and returns
exit code 2 while blocked. The resident runtime performs the same candidate lookup before
opening its quote connection. It rejects a missing compatibility run, an incompatible run, a
missing exact-session signal, any candidate/signal/spec hash mismatch, the observation-only
signal policy, and every v46/v49 artifact that still lacks runtime authority.

The current expected output contains `candidate_missing`, because no terminal 126-session
evaluation or candidate approval exists. Even after those artifacts exist, the current code
will continue to report `candidate_runtime_locked`,
`compatibility_runtime_authority_missing`, and `daily_signal_runtime_locked`. Removing those
blockers requires a future reviewed deployment contract and is not part of this stage.

The authenticated research console reads the same immutable stores through
`GET /api/v1/low-volatility-forward-progress`. Its decision-time compatibility, paper
deployment and daily-signal cards are read-only. Confirm that the response continues to show
`ready_for_runtime=false`, `runtime_activation_allowed=false`, and
`live_trading_locked=true`; a green compatibility result by itself must still leave the
deployment card blocked.

### Paper deployment contract pre-registration

Schema v50 creates the immutable contract registry without creating an official contract row.
It must be reviewed and frozen before the 126-session terminal evaluation, compatibility run,
or candidate approval exists:

```bash
uv run autoquant \
  low-volatility-paper-deployment-contract-freeze \
  --forward-spec-hash \
  ae3d74b1a35d700efea01310bd80e8fd1264eabe6c569f83d9628670a85e34f0 \
  --requested-by risk-auditor \
  --confirm-no-activation
```

The confirmation means exactly that the row grants no activation authority. PostgreSQL checks
the current frozen-session count, the matching v48 compatibility specification and absence of
terminal evaluation, compatibility-run and candidate rows. A first freeze at 126 sessions or
after any terminal evidence is rejected.

The frozen terms require this order:

1. the original 126-session evaluation is a `paper_candidate`;
2. the v49 decision-time compatibility result is `compatible`;
3. the explicit candidate approval occurs after compatibility completion;
4. the exact Shanghai session has a future
   `low-volatility-decision-time-paper-signal-v2` artifact using the pre-registered order
   policy;
5. point-in-time universe, held-position valuation coverage and risk-policy hashes match;
6. paper deployment is exclusive and the kill switch remains active at authorization;
7. runtime unlocking separately rechecks a live lease, fresh QMT quotes and converged
   reconciliation evidence.

The contract keeps `paper_activation_authority_granted=false`,
`runtime_activation_allowed=false`, and `live_trading_locked=true`. A later authorization
artifact and deployable v2 signal schema are still required; this contract cannot start the
scheduler.
