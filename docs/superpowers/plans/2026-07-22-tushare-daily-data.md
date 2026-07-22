# AutoQuant Tushare Daily Data Implementation Plan

> **Execution constraint:** Implement sequentially in the existing isolated worktree on
> `rlocal`. Use strict test-driven development for every production change and run every
> project tool and Git mutation on `rlocal`.

**Goal:** Add a fail-closed, point-in-time Tushare Pro daily-data ingestion path for a
2000-point account without assuming access to the independently licensed minute API.

**Architecture:** Add daily-specific domain types, quality rules, source/repository ports,
and ingestion orchestration alongside the existing RQData minute path. A direct HTTPX
adapter calls Tushare's JSON POST API, normalizes vendor symbols and units, and retains
credential-free source evidence. Daily bars and adjustment factors are append-only
ClickHouse revisions; existing PostgreSQL control records provide evidence, reports,
manifests, checkpoints, and audit events.

**Tech Stack:** Python 3.11, dataclasses, Decimal, HTTPX, Pydantic Settings, ClickHouse,
PostgreSQL/SQLAlchemy, Typer, pytest, respx, Hypothesis, Ruff, mypy, uv.

## Global Constraints

- Work only on branch `feature/tushare-daily` in
  `/Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation`.
- Run all tests, linters, type checks, dependency operations, migrations, and Git commands
  through `ssh rlocal`; invoke uv as `/Users/zjw/.local/bin/uv`.
- Never use, repeat, persist, or test the Token previously posted in chat. Live verification
  requires a rotated Token configured by the user only on `rlocal`.
- Treat 2000 points as configuration context, not proof that any particular endpoint is
  available. `tushare-check` must report the server's actual result.
- Do not call `stk_mins`, do not delete RQData support, and do not map daily bars into the
  existing minute model.
- Keep vendor prices unadjusted. Store adjustment factors separately and require `as_of`
  for any point-in-time query.
- Normalize Tushare daily `vol` from lots to shares (`* 100`) and `amount` from thousands
  of yuan to yuan (`* 1000`) using Decimal-safe parsing.
- Source records and metadata are append-only. A missing capability, conflicting evidence,
  unexplained gap, failed persistence step, or unavailable audit store must fail closed.
- Do not rewrite `001_phase1.sql`; add versioned `002` migrations.
- Default tests must not access Tushare or consume API quota.
- Commit after each green task. Do not push or open a PR until the complete branch has been
  verified and reviewed.

---

### Task 1: Tushare Secret Configuration and Stable Errors

**Files:**

- Modify: `tests/unit/test_config.py`
- Modify: `tests/unit/test_cli.py`
- Modify: `src/autoquant/config.py`
- Modify: `src/autoquant/errors.py`
- Modify: `.env.example`

**Interfaces:**

- Add `TushareCredentials(token: SecretStr)`.
- Add `AppSettings.tushare_token` and HTTPS-only `tushare_api_url` with default
  `https://api.tushare.pro`.
- Add `AppSettings.require_tushare()`.
- Add stable vendor permission/rate-limit errors below `AutoQuantError` without embedding
  the server's raw credential-bearing context.
- Add `tushare` presence to `config-check` while preserving all existing keys.

**TDD steps:**

1. Add failing tests proving missing/blank tokens fail closed, the token is a `SecretStr`,
   repr and CLI output exclude its value, non-HTTPS API URLs are rejected, and
   `config-check` reports only `configured`/`missing`.
2. Run:

   ```bash
   /Users/zjw/.local/bin/uv run pytest tests/unit/test_config.py tests/unit/test_cli.py -q
   ```

   Confirm the new tests fail for the intended missing interfaces.
3. Implement the minimum settings and error types; add empty placeholders to `.env.example`
   without a real token.
4. Re-run the focused tests and then:

   ```bash
   /Users/zjw/.local/bin/uv run ruff check src/autoquant/config.py src/autoquant/errors.py tests/unit/test_config.py tests/unit/test_cli.py
   /Users/zjw/.local/bin/uv run mypy src/autoquant/config.py src/autoquant/errors.py
   ```

5. Commit: `feat: add fail-closed Tushare configuration`.

---

### Task 2: Immutable Daily Domain Contracts and Availability

**Files:**

- Create: `src/autoquant/data/daily_models.py`
- Create: `src/autoquant/data/daily_availability.py`
- Create: `src/autoquant/data/daily_ports.py`
- Create: `tests/unit/data/test_daily_models.py`
- Create: `tests/unit/data/test_daily_availability.py`

**Interfaces:**

- `DailyBarRevision`: source, instrument, session date/event time, unadjusted OHLC,
  `pre_close`, volume in shares, turnover in yuan, availability/ingestion timestamps,
  source revision, availability policy, evidence hash, deterministic content hash.
- `AdjustmentFactorRevision`: source, instrument, session date/event time, positive factor,
  availability/ingestion timestamps, source revision, policy, evidence hash, content hash.
- Coverage values for open/closed sessions, instrument listing interval, and suspension
  status, each backed by a source-evidence hash.
- `DailyDatasetBatch`: bars, factors, coverage, source evidence; rejects mismatched sources,
  methods, hashes, duplicates, and missing evidence.
- `NextTradingSessionOpenPolicy(version="tushare-daily-v1")`: assigns visibility to the
  next open session at 09:30 Asia/Shanghai and fails when no later open session is supplied.
- Daily source and repository protocols for fetching a complete batch, appending bars and
  factors, and querying each stream as of a cutoff.

**TDD steps:**

1. Write failing examples and property tests for finite Decimal values, price ordering,
   nonnegative normalized volume/turnover, positive factors, UTC normalization, stable
   hashes, evidence linkage, duplicate conflicts, next-session availability, and
   `available_at <= as_of` visibility.
2. Run the two new test modules and confirm RED.
3. Implement only the required immutable types, helpers, policy, and protocols. Reuse
   `SourceEvidence` and hash conventions from `data/models.py`; do not duplicate secrets or
   persistence logic.
4. Run focused tests, Ruff, and mypy for the new modules.
5. Commit: `feat: define point-in-time daily data contracts`.

---

### Task 3: Tushare HTTP Transport Contract

**Files:**

- Create: `src/autoquant/adapters/tushare.py`
- Create: `tests/contract/test_tushare_http.py`

**Interfaces:**

- `TushareHttpClient.post(api_name, params, fields)` sends exactly one HTTPS JSON request
  containing `api_name`, secret token, `params`, and `fields`.
- Parse `{code, msg, data: {fields, items}}` by field name, not positional assumptions in
  production mapping.
- Preserve the credential-free response body as `SourceEvidence` with method equal to the
  Tushare API name.
- Classify authentication/permission rejection, rate limiting, HTTP/transient transport
  failures, malformed JSON, mismatched rows, and unexpected business codes.
- Retry only connection/timeouts, HTTP 429, HTTP 5xx, and a documented rate-limit business
  response, with a finite attempt count and injectable no-op sleep for tests.
- `repr(client)` and all exception messages must exclude the Token and full request body.

**TDD steps:**

1. Add respx contract tests for exact JSON shape, reordered fields, empty valid results,
   malformed fields/items, permission code 2002, transient retry boundaries, non-retryable
   errors, HTTPS enforcement, evidence hashing, close semantics, and token redaction.
2. Run the new contract module and confirm RED.
3. Implement the smallest transport client. Do not add the high-level daily mapping yet.
4. Run focused tests, Ruff, and mypy.
5. Commit: `feat: add secure Tushare HTTP transport`.

---

### Task 4: Daily Endpoint Mapping and Capability Probe

**Files:**

- Modify: `src/autoquant/adapters/tushare.py`
- Modify: `tests/contract/test_tushare_http.py`
- Create: `tests/unit/adapters/test_tushare_mapping.py`

**Interfaces:**

- `TushareDailySource.fetch_daily_dataset(instruments, start_date, end_date)` calls only:
  `daily`, `adj_factor`, `trade_cal`, `stock_basic`, and `suspend_d`.
- Map `.SZ`/`.SH` to `.XSHE`/`.XSHG` bidirectionally and reject unknown suffixes.
- Parse `YYYYMMDD` and Tushare's descending output order, return deterministic ascending
  records, and reject rows outside the requested symbols/dates.
- Convert `vol` lots to integer shares; reject fractional resulting shares instead of
  rounding. Convert `amount` thousands of yuan to exact Decimal yuan.
- Fetch enough `trade_cal` data after the requested end date to find the next open session
  required by the availability policy.
- Link each normalized record and coverage value to the response hash of its endpoint.
- Split bounded date ranges so endpoint row caps cannot silently truncate results.
- `probe_capabilities()` performs minimal read-only calls and returns, per endpoint,
  `available`, `permission_denied`, or `error`; a probe result is diagnostic and never
  becomes production coverage evidence.

**TDD steps:**

1. Add failing mapping/contract tests for every endpoint, symbol conversion, units, reverse
   ordering, lifecycle boundaries, suspended and resumed rows, pagination/window boundaries,
   last-session lookahead, permission matrices, and credential-free evidence.
2. Run both Tushare test modules and confirm RED.
3. Implement endpoint methods and `TushareDailySource` in small slices, rerunning the single
   failing test after each slice.
4. Run both modules, Ruff, and mypy.
5. Commit: `feat: map Tushare daily and coverage endpoints`.

---

### Task 5: Deterministic Daily Quality Gate

**Files:**

- Create: `src/autoquant/data/daily_quality.py`
- Create: `tests/unit/data/test_daily_quality.py`

**Interfaces:**

- `DailyQualityGate.evaluate(...) -> QualityReport` reuses the existing immutable report
  type and stable sorting/hash rules.
- Construct expected dates only from open sessions within each instrument's listing
  interval.
- Explain a missing bar only with closed-market, outside-lifecycle, or explicit suspension
  evidence visible by `as_of`.
- Reject unexpected instruments/dates, missing or duplicate factors, conflicting coverage,
  bar/factor evidence later than `as_of`, unexplained gaps, non-contiguous source windows,
  and missing required endpoint evidence.
- Treat a real zero-volume bar as a record, never as a missing-day substitute.
- Set `production_complete=True` only when every required capability and expected observation
  is evidenced and there are no errors.

**TDD steps:**

1. Add failing table-driven tests for normal open days, weekends/holidays, pre-listing,
   post-delisting, suspension, zero volume, unexplained gaps, stale/future evidence,
   duplicates/conflicts, partial endpoint permissions, multi-instrument ordering, and
   deterministic report hashes.
2. Run the new module and confirm RED.
3. Implement the gate with pure functions; do not perform HTTP or persistence inside it.
4. Run focused tests, Ruff, and mypy.
5. Commit: `feat: enforce daily dataset completeness`.

---

### Task 6: ClickHouse Daily Revisions and Point-in-Time Queries

**Files:**

- Create: `migrations/clickhouse/002_tushare_daily.sql`
- Create: `src/autoquant/adapters/clickhouse_daily.py`
- Create: `tests/unit/adapters/test_clickhouse_daily_mapping.py`
- Create: `tests/integration/test_clickhouse_daily_repository.py`
- Modify: `tests/unit/test_cli.py` for schema-version expectations only when appropriate

**Interfaces:**

- Add append-only `daily_bar_revisions` and `adjustment_factor_revisions` tables with
  deterministic UUID record IDs and schema version 2.
- Partition by session month and order by instrument/session/source/availability/ingestion.
- Enforce exact Decimal precision before writes.
- Query latest visible revision with ClickHouse `argMax` bounded by `available_at <= as_of`.
- Validate returned column names, types, and recomputed content hashes.
- `check_connection()` requires the new tables and ClickHouse schema version 2 for daily
  operations without breaking the existing minute repository's phase-1 check.

**TDD steps:**

1. Add failing unit mapping tests for inserts, deterministic IDs, wrong source/type,
   duplicates, Decimal overflow, exact query parameters, revision ordering, malformed
   result rows, and hash mismatch.
2. Add opt-in integration tests for append, correction revisions, duplicate reruns, and
   point-in-time selection.
3. Implement the migration/repository until unit tests pass; then run integration tests on
   the authorized remote ClickHouse if configured. Skips remain explicitly unverified.
4. Run Ruff and mypy.
5. Commit: `feat: persist daily revisions in ClickHouse`.

---

### Task 7: Fail-Closed Daily Ingestion and Validated Reader

**Files:**

- Create: `src/autoquant/data/daily_ingestion.py`
- Create: `tests/unit/data/test_daily_ingestion.py`
- Modify: `tests/unit/adapters/test_postgres_mapping.py`
- Modify: `tests/integration/test_postgres_repository.py`
- Create only if required by a proven schema gap:
  `migrations/postgres/002_tushare_daily.sql`

**Interfaces:**

- `DailyIngestionRequest` accepts unique instruments, inclusive date range, aware `as_of`,
  and explicit production-complete request.
- `DailyIngestionService` saves all source evidence, appends both ClickHouse streams,
  evaluates quality, and then atomically saves report/manifest/checkpoints/audit metadata.
- A completed manifest contains deterministic hashes for both daily bars and factors.
- Advance separate `daily` and `adj-factor` checkpoints only after a passing complete report;
  never advance across an empty or failed instrument.
- Persist rejected quality reports and rejection audits without producing a manifest.
- Return stable statuses for quality rejection and persistence failure.
- `ValidatedDailyDatasetReader` verifies manifest/report integrity and returns exactly the
  bar/factor hashes committed by the manifest at a cutoff not later than its `as_of`.

**TDD steps:**

1. Add failing fake-based tests for happy path order, evidence failure, partial ClickHouse
   failure, quality rejection, incomplete capability, transaction rollback, empty symbol,
   separate checkpoints, corrected rerun, and validated-reader hash mismatch.
2. Prove whether the existing PostgreSQL schema/payload can represent the daily manifest.
   Add a `002` migration only if a failing integration test demonstrates a real gap.
3. Implement the daily orchestration and any minimal generic Postgres serialization changes.
4. Run focused daily ingestion and Postgres tests, then Ruff and mypy.
5. Commit: `feat: orchestrate audited daily ingestion`.

---

### Task 8: Operator CLI, Live Smoke Test, and Runbook

**Files:**

- Modify: `src/autoquant/cli.py`
- Modify: `tests/unit/test_cli.py`
- Create: `tests/live/test_tushare_readonly.py`
- Modify: `docs/runbooks/phase1-data-foundation.md`
- Modify: `README.md`

**Interfaces:**

- `autoquant tushare-check` prints a sorted, secret-free capability matrix for the five
  daily endpoints and exits nonzero when the required production set is incomplete.
- `autoquant ingest-daily --instrument ... --start YYYY-MM-DD --end YYYY-MM-DD` wires the
  source, daily ClickHouse repository, PostgreSQL repository, quality gate, and ingestion
  service. It refuses trading enablement and exits nonzero unless a complete manifest is
  produced.
- Date parsing rejects datetimes, reversed ranges, and malformed dates.
- The live test runs only with `AQ_RUN_TUSHARE_LIVE=1`, reads one known symbol over a tiny
  historical range, never prints the Token, and does not write databases.
- Documentation tells the user to rotate the exposed Token, set it locally, apply migrations
  only to explicitly authorized databases, run capability checks, and interpret skipped
  external checks as incomplete evidence.

**TDD steps:**

1. Add failing CLI tests for help/options, parsing, wiring via mocks, capability states,
   safe errors, exit codes, no token leakage, and unchanged RQData commands.
2. Implement CLI wiring and the opt-in live test.
3. Update README/runbook with exact remote commands but no credentials.
4. Run CLI tests, all non-live tests, Ruff, and mypy.
5. Commit: `feat: expose Tushare daily ingestion CLI`.

---

### Task 9: Full Verification, Review, and Real Capability Check

**Files:**

- Modify only files required by verified failures or review findings.

**Verification steps:**

1. Confirm the diff contains no credentials or generated artifacts:

   ```bash
   git status --short
   git diff --check origin/main...HEAD
   git grep -n 'AQ_TUSHARE_TOKEN='
   ```

   Inspect every match: `.env.example` may contain only an empty placeholder, and no tracked
   file may contain a real Token. Also run the repository's configured secret scanner when
   one is available; never place a known secret literal into a scan command or tracked file.
2. Run the complete local-quality suite on `rlocal`:

   ```bash
   /Users/zjw/.local/bin/uv sync --frozen
   /Users/zjw/.local/bin/uv run pytest -m "not live" -q
   /Users/zjw/.local/bin/uv run ruff check .
   /Users/zjw/.local/bin/uv run mypy src
   ```

3. Run authorized PostgreSQL/ClickHouse integration tests and record skips separately.
4. Review the complete branch against the approved design for scope, security, point-in-time
   correctness, unit conversions, and fail-closed behavior. Fix findings with a failing test
   first and rerun the entire suite.
5. Ask the user to configure the rotated Token on `rlocal`; then run:

   ```bash
   /Users/zjw/.local/bin/uv run autoquant tushare-check
   AQ_RUN_TUSHARE_LIVE=1 /Users/zjw/.local/bin/uv run pytest tests/live/test_tushare_readonly.py -q -rs
   ```

6. Do not claim end-to-end readiness until the live capability check and authorized database
   ingestion both succeed. If only mocked/default checks pass, report exactly that boundary.
7. After verification and user authorization, push `feature/tushare-daily` and open a PR
   against `main`; never force-push.
