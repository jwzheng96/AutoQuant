CREATE TABLE IF NOT EXISTS qmt_readonly_acceptance_evidence
(
    evidence_hash text PRIMARY KEY CHECK (evidence_hash ~ '^[0-9a-f]{64}$'),
    logical_account_id text NOT NULL
        CHECK (char_length(logical_account_id) BETWEEN 1 AND 128),
    observed_at timestamptz NOT NULL,
    baseline_evidence_hash text NOT NULL
        CHECK (baseline_evidence_hash ~ '^[0-9a-f]{64}$'),
    account_snapshot_hash text NOT NULL
        CHECK (account_snapshot_hash ~ '^[0-9a-f]{64}$'),
    package_manifest_hash text NOT NULL
        CHECK (package_manifest_hash ~ '^[0-9a-f]{64}$'),
    position_count integer NOT NULL CHECK (position_count >= 0),
    order_count integer NOT NULL CHECK (order_count >= 0),
    trade_count integer NOT NULL CHECK (trade_count >= 0),
    callback_cursor bigint NOT NULL CHECK (callback_cursor >= 0),
    lease_session_id integer NOT NULL CHECK (lease_session_id > 0),
    lease_holder_id text NOT NULL
        CHECK (lease_holder_id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'),
    lease_token_hash text NOT NULL
        CHECK (lease_token_hash ~ '^[0-9a-f]{64}$'),
    lease_generation bigint NOT NULL CHECK (lease_generation > 0),
    evidence_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS qmt_readonly_acceptance_account_time_idx
ON qmt_readonly_acceptance_evidence
    (logical_account_id, observed_at DESC);

DO $create_qmt_readonly_acceptance_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'qmt_readonly_acceptance_immutable'
          AND tgrelid = 'qmt_readonly_acceptance_evidence'::regclass
    ) THEN
        CREATE TRIGGER qmt_readonly_acceptance_immutable
        BEFORE UPDATE OR DELETE ON qmt_readonly_acceptance_evidence
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_qmt_readonly_acceptance_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 17)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
