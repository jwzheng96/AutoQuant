CREATE TABLE IF NOT EXISTS validation_experiments
(
    experiment_id uuid PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    state text NOT NULL CHECK (
        state IN ('queued', 'running', 'completed', 'failed', 'interrupted')
    ),
    validator_id text NOT NULL CHECK (
        validator_id = 'sma_cross_walk_forward_v1'
    ),
    manifest_hash text NOT NULL REFERENCES dataset_manifests(manifest_hash),
    request_payload jsonb NOT NULL,
    requested_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz,
    completed_at timestamptz,
    as_of timestamptz,
    result_hash text CHECK (result_hash IS NULL OR result_hash ~ '^[0-9a-f]{64}$'),
    summary_payload jsonb,
    error_code text,
    CHECK (
        (state = 'queued' AND started_at IS NULL AND completed_at IS NULL)
        OR (state = 'running' AND started_at IS NOT NULL AND completed_at IS NULL)
        OR (state IN ('completed', 'failed', 'interrupted') AND completed_at IS NOT NULL)
    ),
    CHECK (
        (state = 'completed' AND as_of IS NOT NULL AND result_hash IS NOT NULL
         AND summary_payload IS NOT NULL AND error_code IS NULL)
        OR state <> 'completed'
    )
);

CREATE INDEX IF NOT EXISTS validation_experiments_created_at_idx
ON validation_experiments (created_at DESC);

CREATE INDEX IF NOT EXISTS validation_experiments_state_created_at_idx
ON validation_experiments (state, created_at);

CREATE TABLE IF NOT EXISTS validation_folds
(
    experiment_id uuid NOT NULL REFERENCES validation_experiments(experiment_id),
    sequence integer NOT NULL CHECK (sequence > 0),
    train_start date NOT NULL,
    train_end date NOT NULL,
    test_start date NOT NULL,
    test_end date NOT NULL,
    selected_fast integer NOT NULL CHECK (selected_fast >= 2),
    selected_slow integer NOT NULL CHECK (selected_slow > selected_fast),
    selection_score numeric NOT NULL,
    fold_hash text NOT NULL CHECK (fold_hash ~ '^[0-9a-f]{64}$'),
    training_payload jsonb NOT NULL,
    test_payload jsonb NOT NULL,
    PRIMARY KEY (experiment_id, sequence),
    UNIQUE (experiment_id, fold_hash),
    CHECK (train_start <= train_end AND train_end < test_start AND test_start <= test_end)
);

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 5)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
