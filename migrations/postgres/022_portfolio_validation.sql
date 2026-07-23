CREATE TABLE IF NOT EXISTS portfolio_validation_experiments
(
    experiment_id uuid PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE CHECK (
        idempotency_key ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]{15,127}$'
    ),
    state text NOT NULL CHECK (
        state IN ('queued', 'running', 'completed', 'failed', 'interrupted')
    ),
    validator_id text NOT NULL CHECK (
        validator_id = 'cross_sectional_momentum_walk_forward_v1'
    ),
    manifest_hash text NOT NULL REFERENCES dataset_manifests(manifest_hash),
    request_payload jsonb NOT NULL,
    requested_by text NOT NULL CHECK (
        char_length(requested_by) BETWEEN 1 AND 128
    ),
    created_at timestamptz NOT NULL,
    started_at timestamptz,
    completed_at timestamptz,
    as_of timestamptz,
    result_hash text CHECK (
        result_hash IS NULL OR result_hash ~ '^[0-9a-f]{64}$'
    ),
    summary_payload jsonb,
    error_code text CHECK (
        error_code IS NULL OR char_length(error_code) BETWEEN 1 AND 80
    ),
    CHECK (
        (state = 'queued' AND started_at IS NULL AND completed_at IS NULL)
        OR (state = 'running' AND started_at IS NOT NULL AND completed_at IS NULL)
        OR (
            state IN ('completed', 'failed', 'interrupted')
            AND completed_at IS NOT NULL
        )
    ),
    CHECK (
        (
            state = 'completed'
            AND as_of IS NOT NULL
            AND result_hash IS NOT NULL
            AND summary_payload IS NOT NULL
            AND error_code IS NULL
        )
        OR state <> 'completed'
    )
);

CREATE INDEX IF NOT EXISTS portfolio_validation_experiments_created_idx
ON portfolio_validation_experiments (created_at DESC, experiment_id DESC);

CREATE INDEX IF NOT EXISTS portfolio_validation_experiments_state_idx
ON portfolio_validation_experiments (state, created_at, experiment_id);

CREATE OR REPLACE FUNCTION autoquant_validate_portfolio_validation_change()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'portfolio validation experiments cannot be deleted';
    END IF;
    IF OLD.state IN ('completed', 'failed', 'interrupted') THEN
        RAISE EXCEPTION 'terminal portfolio validation experiments are immutable';
    END IF;
    IF (
        OLD.experiment_id,
        OLD.idempotency_key,
        OLD.validator_id,
        OLD.manifest_hash,
        OLD.request_payload,
        OLD.requested_by,
        OLD.created_at
    ) IS DISTINCT FROM (
        NEW.experiment_id,
        NEW.idempotency_key,
        NEW.validator_id,
        NEW.manifest_hash,
        NEW.request_payload,
        NEW.requested_by,
        NEW.created_at
    ) THEN
        RAISE EXCEPTION 'portfolio validation identity is immutable';
    END IF;
    IF NOT (
        (OLD.state = 'queued' AND NEW.state IN ('running', 'failed'))
        OR (
            OLD.state = 'running'
            AND NEW.state IN ('completed', 'failed', 'interrupted')
        )
    ) THEN
        RAISE EXCEPTION 'invalid portfolio validation state transition';
    END IF;
    RETURN NEW;
END;
$$;

DO $create_portfolio_validation_experiment_guard$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'portfolio_validation_experiment_guard'
          AND tgrelid = 'portfolio_validation_experiments'::regclass
    ) THEN
        CREATE TRIGGER portfolio_validation_experiment_guard
        BEFORE UPDATE OR DELETE ON portfolio_validation_experiments
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_validate_portfolio_validation_change();
    END IF;
END;
$create_portfolio_validation_experiment_guard$;

CREATE TABLE IF NOT EXISTS portfolio_validation_folds
(
    experiment_id uuid NOT NULL
        REFERENCES portfolio_validation_experiments(experiment_id),
    sequence integer NOT NULL CHECK (sequence > 0),
    train_start date NOT NULL,
    train_end date NOT NULL,
    test_start date NOT NULL,
    test_end date NOT NULL,
    selected_payload jsonb NOT NULL,
    selection_score numeric NOT NULL,
    fold_hash text NOT NULL CHECK (fold_hash ~ '^[0-9a-f]{64}$'),
    training_payload jsonb NOT NULL,
    test_payload jsonb NOT NULL,
    benchmark_payload jsonb NOT NULL,
    PRIMARY KEY (experiment_id, sequence),
    UNIQUE (experiment_id, fold_hash),
    CHECK (
        train_start <= train_end
        AND train_end < test_start
        AND test_start <= test_end
    )
);

DO $create_portfolio_validation_fold_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'portfolio_validation_folds_immutable'
          AND tgrelid = 'portfolio_validation_folds'::regclass
    ) THEN
        CREATE TRIGGER portfolio_validation_folds_immutable
        BEFORE UPDATE OR DELETE ON portfolio_validation_folds
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_portfolio_validation_fold_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 22)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
