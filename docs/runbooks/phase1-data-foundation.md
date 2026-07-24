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
