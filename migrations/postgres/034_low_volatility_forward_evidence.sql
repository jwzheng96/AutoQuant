CREATE TABLE IF NOT EXISTS low_volatility_forward_evidence_specs
(
    spec_hash text PRIMARY KEY CHECK (
        spec_hash ~ '^[0-9a-f]{64}$'
    ),
    predecessor_result_hash text NOT NULL UNIQUE
        REFERENCES low_volatility_validation_runs(result_hash),
    predecessor_assessment_hash text NOT NULL CHECK (
        predecessor_assessment_hash ~ '^[0-9a-f]{64}$'
    ),
    source_spec_hash text NOT NULL
        REFERENCES low_volatility_research_specs(spec_hash),
    source_dataset_manifest_hash text NOT NULL
        REFERENCES research_dataset_manifests(manifest_hash),
    strategy_id text NOT NULL CHECK (
        strategy_id =
            'dynamic-universe-low-volatility-v4'
    ),
    methodology_version text NOT NULL CHECK (
        methodology_version =
            'annualized-geometric-return-gap-v1'
    ),
    specification_version text NOT NULL CHECK (
        specification_version =
            'low-volatility-forward-evidence-spec-v1'
    ),
    forward_start_date date NOT NULL,
    minimum_forward_sessions integer NOT NULL CHECK (
        minimum_forward_sessions = 126
    ),
    minimum_paper_sessions integer NOT NULL CHECK (
        minimum_paper_sessions = 60
    ),
    formal_hypothesis_count integer NOT NULL CHECK (
        formal_hypothesis_count = 4
    ),
    outcome_observed_at_design boolean NOT NULL CHECK (
        outcome_observed_at_design
    ),
    strategy_parameters_unchanged boolean NOT NULL CHECK (
        strategy_parameters_unchanged
    ),
    retrospective_reclassification_allowed boolean NOT NULL CHECK (
        NOT retrospective_reclassification_allowed
    ),
    historical_result_eligible_for_promotion boolean NOT NULL CHECK (
        NOT historical_result_eligible_for_promotion
    ),
    requested_by text NOT NULL CHECK (
        length(btrim(requested_by)) BETWEEN 1 AND 128
        AND requested_by = btrim(requested_by)
    ),
    created_at timestamptz NOT NULL,
    live_trading_locked boolean NOT NULL DEFAULT true CHECK (
        live_trading_locked
    ),
    payload jsonb NOT NULL
);

DO $create_low_volatility_forward_specs_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_forward_specs_immutable'
          AND tgrelid =
            'low_volatility_forward_evidence_specs'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_forward_specs_immutable
        BEFORE UPDATE OR DELETE
            ON low_volatility_forward_evidence_specs
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_low_volatility_forward_specs_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 34)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(
        schema_versions.version,
        EXCLUDED.version
    ),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version
            THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
