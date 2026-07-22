# AutoQuant Audited Backtest Console Design

## Purpose

Provide a bounded research workflow that proves the data, execution, accounting,
and operator-control pipeline before any simulated or live trading capability is
unlocked. A positive return is not a release criterion and the baseline strategy
is not an alpha claim.

## Safety boundary

- Only the built-in `manifest_buy_hold_v1` strategy may run. No uploaded code,
  expressions, imports, or dynamic evaluation are accepted.
- Every run is bound to one production-complete `DatasetManifest` and uses that
  manifest's point-in-time cutoff.
- One run is limited to one instrument and at most 366 calendar days (enforced by
  the ingestion manifest contract).
- The service fails closed when manifest hashes, quality evidence, exact daily
  price limits, suspension status, or supported corporate-action assumptions are
  incomplete.
- Real order submission remains unavailable.

## Baseline strategy

The baseline allocates a requested fraction of initial cash on the first session
and optionally liquidates on the last session. Quantity is computed from the
previous close and the configured conservative slippage, never from the future
opening price or same-day volume. Exchange lot rules, T+1, fees, liquidity,
suspension, and exact point-in-time price limits remain authoritative and may
reject an order.

The baseline deliberately rejects a manifest whose adjustment factor changes;
corporate-action accounting must be implemented before such a period can be
represented faithfully.

## Persistence

PostgreSQL stores:

- `backtest_runs`: idempotent request, state machine, manifest binding, result
  hashes, metrics, stable failure code, and operator timestamps.
- `backtest_executions`: every fill or rejection with fee components and ledger
  hash.
- `backtest_snapshots`: daily cash, market value, equity, positions, and ledger
  hash.
- `backtest_events`: the ordered tamper-evident execution event chain.

Completion inserts all result rows and transitions the run to `completed` in one
transaction. A restart changes abandoned `running` rows to `interrupted`; it does
not silently replay them.

## API and console

- `GET /api/v1/research/manifests`
- `GET /api/v1/backtests`
- `POST /api/v1/backtests` (Basic Auth and CSRF protected)
- `GET /api/v1/backtests/{run_id}`

The `/research` page provides a bounded form, recent run state, metrics, an equity
curve, and execution/rejection detail. Responses never contain credentials,
vendor tokens, DSNs, tracebacks, or raw exception messages.

## Release gate

This feature establishes research observability only. Live trading remains locked
until point-in-time strategy validation, walk-forward and out-of-sample evidence,
portfolio risk controls, paper-trading reconciliation, broker idempotency, kill
switches, and incident drills independently pass their acceptance criteria.
