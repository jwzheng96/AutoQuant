CREATE TABLE IF NOT EXISTS dynamic_research_specs
(
    spec_hash text PRIMARY KEY CHECK (
        spec_hash ~ '^[0-9a-f]{64}$'
    ),
    dataset_manifest_hash text NOT NULL
        REFERENCES research_dataset_manifests(manifest_hash),
    plan_hash text NOT NULL CHECK (
        plan_hash ~ '^[0-9a-f]{64}$'
    ),
    policy_hash text NOT NULL CHECK (
        policy_hash ~ '^[0-9a-f]{64}$'
    ),
    strategy_id text NOT NULL CHECK (
        strategy_id =
            'dynamic-universe-cross-sectional-momentum-v1'
    ),
    specification_version text NOT NULL CHECK (
        specification_version = 'dynamic-portfolio-research-spec-v1'
    ),
    start_date date NOT NULL,
    end_date date NOT NULL,
    requested_by text NOT NULL CHECK (
        char_length(requested_by) BETWEEN 1 AND 128
    ),
    created_at timestamptz NOT NULL,
    live_trading_locked boolean NOT NULL DEFAULT true CHECK (
        live_trading_locked
    ),
    payload jsonb NOT NULL,
    UNIQUE (dataset_manifest_hash, strategy_id),
    CHECK (start_date <= end_date)
);

DO $create_dynamic_research_specs_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'dynamic_research_specs_immutable'
          AND tgrelid = 'dynamic_research_specs'::regclass
    ) THEN
        CREATE TRIGGER dynamic_research_specs_immutable
        BEFORE UPDATE OR DELETE ON dynamic_research_specs
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_dynamic_research_specs_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 25)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
