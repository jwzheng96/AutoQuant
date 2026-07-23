CREATE TABLE IF NOT EXISTS qmt_session_leases
(
    session_id integer PRIMARY KEY CHECK (session_id > 0),
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

CREATE INDEX IF NOT EXISTS qmt_session_leases_active_idx
ON qmt_session_leases (expires_at)
WHERE released_at IS NULL;

CREATE TABLE IF NOT EXISTS qmt_session_lease_events
(
    event_hash text PRIMARY KEY CHECK (event_hash ~ '^[0-9a-f]{64}$'),
    session_id integer NOT NULL REFERENCES qmt_session_leases(session_id),
    sequence bigint NOT NULL CHECK (sequence > 0),
    generation bigint NOT NULL CHECK (generation > 0),
    action text NOT NULL CHECK (action IN ('acquire', 'release')),
    holder_id text NOT NULL CHECK (holder_id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'),
    token_hash text NOT NULL CHECK (token_hash ~ '^[0-9a-f]{64}$'),
    occurred_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    previous_hash text NOT NULL CHECK (previous_hash ~ '^[0-9a-f]{64}$'),
    event_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (session_id, sequence),
    UNIQUE (session_id, generation, action)
);

DO $create_qmt_session_lease_events_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'qmt_session_lease_events_immutable'
          AND tgrelid = 'qmt_session_lease_events'::regclass
    ) THEN
        CREATE TRIGGER qmt_session_lease_events_immutable
        BEFORE UPDATE OR DELETE ON qmt_session_lease_events
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_qmt_session_lease_events_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 12)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
