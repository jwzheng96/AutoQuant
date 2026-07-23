CREATE TABLE IF NOT EXISTS fundamental_research_panels
(
    panel_hash text PRIMARY KEY CHECK (
        panel_hash ~ '^[0-9a-f]{64}$'
    ),
    spec_hash text NOT NULL UNIQUE
        REFERENCES fundamental_research_specs(spec_hash),
    daily_panel_hash text NOT NULL CHECK (
        daily_panel_hash ~ '^[0-9a-f]{64}$'
    ),
    fundamental_dataset_manifest_hash text NOT NULL UNIQUE
        REFERENCES fundamental_dataset_manifests(manifest_hash),
    as_of timestamptz NOT NULL,
    session_count integer NOT NULL CHECK (session_count > 0),
    eligible_session_count integer NOT NULL CHECK (
        eligible_session_count BETWEEN 0 AND session_count
    ),
    observation_count integer NOT NULL CHECK (
        observation_count >= 0
    ),
    minimum_eligible_members integer NOT NULL CHECK (
        minimum_eligible_members >= 0
    ),
    maximum_eligible_members integer NOT NULL CHECK (
        maximum_eligible_members >= minimum_eligible_members
    ),
    minimum_required_members integer NOT NULL CHECK (
        minimum_required_members > 0
    ),
    requested_by text NOT NULL CHECK (
        length(btrim(requested_by)) BETWEEN 1 AND 128
        AND requested_by = btrim(requested_by)
    ),
    created_at timestamptz NOT NULL CHECK (created_at >= as_of),
    live_trading_locked boolean NOT NULL DEFAULT true CHECK (
        live_trading_locked
    ),
    payload jsonb NOT NULL
);

DO $create_fundamental_research_panels_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'fundamental_research_panels_immutable'
          AND tgrelid = 'fundamental_research_panels'::regclass
    ) THEN
        CREATE TRIGGER fundamental_research_panels_immutable
        BEFORE UPDATE OR DELETE ON fundamental_research_panels
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_fundamental_research_panels_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 30)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version
            THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
