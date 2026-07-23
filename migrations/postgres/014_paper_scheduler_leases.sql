CREATE TABLE IF NOT EXISTS paper_scheduler_leases
(
    account_id text PRIMARY KEY CHECK (char_length(account_id) BETWEEN 1 AND 128),
    strategy_id text NOT NULL CHECK (char_length(strategy_id) BETWEEN 1 AND 128),
    holder_id text NOT NULL CHECK (holder_id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'),
    token_hash text NOT NULL CHECK (token_hash ~ '^[0-9a-f]{64}$'),
    acquired_at timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    released_at timestamptz,
    generation bigint NOT NULL CHECK (generation > 0),
    version bigint NOT NULL CHECK (version > 0),
    event_sequence bigint NOT NULL CHECK (event_sequence > 0),
    last_event_hash text NOT NULL CHECK (last_event_hash ~ '^[0-9a-f]{64}$'),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (heartbeat_at >= acquired_at),
    CHECK (expires_at > heartbeat_at OR released_at IS NOT NULL),
    CHECK (released_at IS NULL OR released_at >= acquired_at)
);

CREATE INDEX IF NOT EXISTS paper_scheduler_leases_active_idx
ON paper_scheduler_leases (expires_at)
WHERE released_at IS NULL;

CREATE TABLE IF NOT EXISTS paper_scheduler_lease_events
(
    event_hash text PRIMARY KEY CHECK (event_hash ~ '^[0-9a-f]{64}$'),
    account_id text NOT NULL REFERENCES paper_scheduler_leases(account_id),
    sequence bigint NOT NULL CHECK (sequence > 0),
    generation bigint NOT NULL CHECK (generation > 0),
    action text NOT NULL CHECK (action IN ('acquire', 'release')),
    strategy_id text NOT NULL CHECK (char_length(strategy_id) BETWEEN 1 AND 128),
    holder_id text NOT NULL CHECK (holder_id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'),
    token_hash text NOT NULL CHECK (token_hash ~ '^[0-9a-f]{64}$'),
    occurred_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    previous_hash text NOT NULL CHECK (previous_hash ~ '^[0-9a-f]{64}$'),
    event_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (account_id, sequence),
    UNIQUE (account_id, generation, action)
);

DO $create_paper_scheduler_lease_events_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'paper_scheduler_lease_events_immutable'
          AND tgrelid = 'paper_scheduler_lease_events'::regclass
    ) THEN
        CREATE TRIGGER paper_scheduler_lease_events_immutable
        BEFORE UPDATE OR DELETE ON paper_scheduler_lease_events
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_paper_scheduler_lease_events_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 14)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
