CREATE TABLE IF NOT EXISTS dynamic_validation_runs
(
    result_hash text PRIMARY KEY CHECK (
        result_hash ~ '^[0-9a-f]{64}$'
    ),
    spec_hash text NOT NULL
        REFERENCES dynamic_research_specs(spec_hash),
    panel_hash text NOT NULL CHECK (
        panel_hash ~ '^[0-9a-f]{64}$'
    ),
    dataset_manifest_hash text NOT NULL
        REFERENCES research_dataset_manifests(manifest_hash),
    assessment_hash text NOT NULL CHECK (
        assessment_hash ~ '^[0-9a-f]{64}$'
    ),
    evidence_status text NOT NULL CHECK (
        evidence_status IN (
            'research_candidate', 'insufficient', 'rejected'
        )
    ),
    fold_count integer NOT NULL CHECK (fold_count > 0),
    oos_sessions integer NOT NULL CHECK (oos_sessions > 0),
    rejected_order_count integer NOT NULL CHECK (
        rejected_order_count >= 0
    ),
    validator_version text NOT NULL CHECK (
        validator_version = 'dynamic-nested-walk-forward-v1'
    ),
    objective_version text NOT NULL CHECK (
        objective_version =
            'return-minus-drawdown-turnover-v1'
    ),
    assessment_version text NOT NULL CHECK (
        assessment_version = 'dynamic-validation-assessment-v1'
    ),
    requested_by text NOT NULL CHECK (
        char_length(requested_by) BETWEEN 1 AND 128
    ),
    as_of timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    live_trading_locked boolean NOT NULL DEFAULT true CHECK (
        live_trading_locked
    ),
    summary_payload jsonb NOT NULL,
    assessment_payload jsonb NOT NULL,
    UNIQUE (spec_hash, panel_hash)
);

DO $create_dynamic_validation_runs_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'dynamic_validation_runs_immutable'
          AND tgrelid = 'dynamic_validation_runs'::regclass
    ) THEN
        CREATE TRIGGER dynamic_validation_runs_immutable
        BEFORE UPDATE OR DELETE ON dynamic_validation_runs
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_dynamic_validation_runs_immutable$;

CREATE TABLE IF NOT EXISTS dynamic_validation_folds
(
    result_hash text NOT NULL
        REFERENCES dynamic_validation_runs(result_hash),
    sequence integer NOT NULL CHECK (sequence > 0),
    train_start date NOT NULL,
    train_end date NOT NULL,
    test_start date NOT NULL,
    test_end date NOT NULL,
    fold_hash text NOT NULL CHECK (
        fold_hash ~ '^[0-9a-f]{64}$'
    ),
    selected_payload jsonb NOT NULL,
    selection_score numeric NOT NULL,
    candidate_evaluations jsonb NOT NULL,
    training_payload jsonb NOT NULL,
    test_payload jsonb NOT NULL,
    benchmark_payload jsonb NOT NULL,
    PRIMARY KEY (result_hash, sequence),
    UNIQUE (result_hash, fold_hash),
    CHECK (
        train_start <= train_end
        AND train_end < test_start
        AND test_start <= test_end
    )
);

DO $create_dynamic_validation_folds_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'dynamic_validation_folds_immutable'
          AND tgrelid = 'dynamic_validation_folds'::regclass
    ) THEN
        CREATE TRIGGER dynamic_validation_folds_immutable
        BEFORE UPDATE OR DELETE ON dynamic_validation_folds
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_dynamic_validation_folds_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 26)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
