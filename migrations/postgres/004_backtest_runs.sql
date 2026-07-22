CREATE TABLE IF NOT EXISTS backtest_runs
(
    run_id uuid PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    state text NOT NULL CHECK (
        state IN ('queued', 'running', 'completed', 'failed', 'interrupted')
    ),
    strategy_id text NOT NULL CHECK (strategy_id = 'manifest_buy_hold_v1'),
    manifest_hash text NOT NULL REFERENCES dataset_manifests(manifest_hash),
    request_payload jsonb NOT NULL,
    requested_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz,
    completed_at timestamptz,
    as_of timestamptz,
    result_hash text CHECK (result_hash IS NULL OR result_hash ~ '^[0-9a-f]{64}$'),
    ledger_hash text CHECK (ledger_hash IS NULL OR ledger_hash ~ '^[0-9a-f]{64}$'),
    metrics_payload jsonb,
    error_code text,
    CHECK (
        (state = 'queued' AND started_at IS NULL AND completed_at IS NULL)
        OR (state = 'running' AND started_at IS NOT NULL AND completed_at IS NULL)
        OR (state IN ('completed', 'failed', 'interrupted') AND completed_at IS NOT NULL)
    ),
    CHECK (
        (state = 'completed' AND as_of IS NOT NULL AND result_hash IS NOT NULL
         AND ledger_hash IS NOT NULL AND metrics_payload IS NOT NULL
         AND error_code IS NULL)
        OR state <> 'completed'
    )
);

CREATE INDEX IF NOT EXISTS backtest_runs_created_at_idx
ON backtest_runs (created_at DESC);

CREATE INDEX IF NOT EXISTS backtest_runs_state_created_at_idx
ON backtest_runs (state, created_at);

CREATE TABLE IF NOT EXISTS backtest_executions
(
    run_id uuid NOT NULL REFERENCES backtest_runs(run_id),
    client_order_id text NOT NULL,
    instrument text NOT NULL,
    side text NOT NULL CHECK (side IN ('buy', 'sell')),
    requested_quantity bigint NOT NULL CHECK (requested_quantity > 0),
    state text NOT NULL CHECK (state IN ('filled', 'rejected')),
    session_date date NOT NULL,
    filled_quantity bigint NOT NULL CHECK (filled_quantity >= 0),
    fill_price numeric,
    gross_amount numeric NOT NULL CHECK (gross_amount >= 0),
    commission numeric NOT NULL CHECK (commission >= 0),
    stamp_duty numeric NOT NULL CHECK (stamp_duty >= 0),
    transfer_fee numeric NOT NULL CHECK (transfer_fee >= 0),
    rejection_code text,
    ledger_hash text NOT NULL CHECK (ledger_hash ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (run_id, client_order_id),
    CHECK (
        (state = 'filled' AND filled_quantity = requested_quantity
         AND fill_price IS NOT NULL AND rejection_code IS NULL)
        OR (state = 'rejected' AND filled_quantity = 0
            AND fill_price IS NULL AND rejection_code IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS backtest_snapshots
(
    run_id uuid NOT NULL REFERENCES backtest_runs(run_id),
    session_date date NOT NULL,
    cash numeric NOT NULL,
    market_value numeric NOT NULL,
    equity numeric NOT NULL,
    positions_payload jsonb NOT NULL,
    ledger_hash text NOT NULL CHECK (ledger_hash ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (run_id, session_date)
);

CREATE TABLE IF NOT EXISTS backtest_events
(
    run_id uuid NOT NULL REFERENCES backtest_runs(run_id),
    sequence bigint NOT NULL CHECK (sequence > 0),
    event_type text NOT NULL,
    session_date date NOT NULL,
    client_order_id text NOT NULL,
    payload jsonb NOT NULL,
    previous_hash text NOT NULL CHECK (previous_hash ~ '^[0-9a-f]{64}$'),
    event_hash text NOT NULL CHECK (event_hash ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (run_id, sequence),
    UNIQUE (run_id, event_hash)
);

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 4)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
