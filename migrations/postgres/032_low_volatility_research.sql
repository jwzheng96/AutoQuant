CREATE TABLE IF NOT EXISTS low_volatility_research_specs
(
    spec_hash text PRIMARY KEY CHECK (
        spec_hash ~ '^[0-9a-f]{64}$'
    ),
    predecessor_result_hash text NOT NULL UNIQUE
        REFERENCES fundamental_validation_runs(result_hash),
    dataset_manifest_hash text NOT NULL
        REFERENCES research_dataset_manifests(manifest_hash),
    plan_hash text NOT NULL CHECK (
        plan_hash ~ '^[0-9a-f]{64}$'
    ),
    policy_hash text NOT NULL CHECK (
        policy_hash ~ '^[0-9a-f]{64}$'
    ),
    evidence_policy_hash text NOT NULL CHECK (
        evidence_policy_hash ~ '^[0-9a-f]{64}$'
    ),
    strategy_id text NOT NULL CHECK (
        strategy_id = 'dynamic-universe-low-volatility-v4'
    ),
    specification_version text NOT NULL CHECK (
        specification_version =
            'low-volatility-research-spec-v4'
    ),
    start_date date NOT NULL,
    end_date date NOT NULL,
    requested_by text NOT NULL CHECK (
        length(btrim(requested_by)) BETWEEN 1 AND 128
        AND requested_by = btrim(requested_by)
    ),
    created_at timestamptz NOT NULL,
    live_trading_locked boolean NOT NULL DEFAULT true CHECK (
        live_trading_locked
    ),
    payload jsonb NOT NULL,
    CHECK (start_date <= end_date)
);

DO $create_low_volatility_research_specs_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_research_specs_immutable'
          AND tgrelid =
            'low_volatility_research_specs'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_research_specs_immutable
        BEFORE UPDATE OR DELETE
            ON low_volatility_research_specs
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_low_volatility_research_specs_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 32)
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
