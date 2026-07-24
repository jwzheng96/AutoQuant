CREATE TABLE IF NOT EXISTS low_volatility_forward_sessions
(
    binding_hash text PRIMARY KEY CHECK (
        binding_hash ~ '^[0-9a-f]{64}$'
    ),
    forward_spec_hash text NOT NULL
        REFERENCES low_volatility_forward_evidence_specs(spec_hash),
    session_date date NOT NULL,
    snapshot_hash text NOT NULL
        REFERENCES research_universe_snapshots(snapshot_hash),
    snapshot_reference_date date NOT NULL,
    dataset_manifest_hash text NOT NULL
        REFERENCES research_dataset_manifests(manifest_hash),
    policy_hash text NOT NULL CHECK (
        policy_hash ~ '^[0-9a-f]{64}$'
    ),
    calendar_content_hash text NOT NULL CHECK (
        calendar_content_hash ~ '^[0-9a-f]{64}$'
    ),
    instrument_count integer NOT NULL CHECK (
        instrument_count BETWEEN 1 AND 1000
    ),
    binding_version text NOT NULL CHECK (
        binding_version =
            'low-volatility-forward-session-binding-v1'
    ),
    requested_by text NOT NULL CHECK (
        length(btrim(requested_by)) BETWEEN 1 AND 128
        AND requested_by = btrim(requested_by)
    ),
    completed_at timestamptz NOT NULL,
    live_trading_locked boolean NOT NULL DEFAULT true CHECK (
        live_trading_locked
    ),
    payload jsonb NOT NULL,
    UNIQUE (forward_spec_hash, session_date),
    UNIQUE (forward_spec_hash, dataset_manifest_hash),
    CHECK (snapshot_reference_date < session_date)
);

DO $create_low_volatility_forward_sessions_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_forward_sessions_immutable'
          AND tgrelid =
            'low_volatility_forward_sessions'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_forward_sessions_immutable
        BEFORE UPDATE OR DELETE
            ON low_volatility_forward_sessions
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_low_volatility_forward_sessions_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 35)
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
