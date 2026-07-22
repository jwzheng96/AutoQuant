CREATE TABLE IF NOT EXISTS execution_control_state
(
    account_id text PRIMARY KEY,
    active boolean NOT NULL,
    version bigint NOT NULL CHECK (version > 0),
    reason text NOT NULL CHECK (
        reason IN ('initializing', 'manual', 'reconciliation_failed',
                   'dependency_unavailable', 'order_state_unknown',
                   'recovery_failed', 'drill', 'reset_approved')
    ),
    changed_at timestamptz NOT NULL,
    changed_by text NOT NULL,
    last_event_hash text NOT NULL CHECK (last_event_hash ~ '^[0-9a-f]{64}$'),
    state_hash text NOT NULL CHECK (state_hash ~ '^[0-9a-f]{64}$'),
    state_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS execution_control_events
(
    event_hash text PRIMARY KEY CHECK (event_hash ~ '^[0-9a-f]{64}$'),
    command_id text NOT NULL UNIQUE,
    command_hash text NOT NULL CHECK (command_hash ~ '^[0-9a-f]{64}$'),
    account_id text NOT NULL REFERENCES execution_control_state(account_id),
    sequence bigint NOT NULL CHECK (sequence > 0),
    action text NOT NULL CHECK (action IN ('initialize', 'activate', 'reset')),
    reason text NOT NULL,
    actor text NOT NULL,
    occurred_at timestamptz NOT NULL,
    evidence_hash text CHECK (evidence_hash IS NULL OR evidence_hash ~ '^[0-9a-f]{64}$'),
    previous_hash text NOT NULL CHECK (previous_hash ~ '^[0-9a-f]{64}$'),
    transition_state_hash text NOT NULL
        CHECK (transition_state_hash ~ '^[0-9a-f]{64}$'),
    command_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (account_id, sequence)
);

CREATE INDEX IF NOT EXISTS execution_control_events_account_time_idx
ON execution_control_events (account_id, occurred_at DESC);

DO $create_execution_control_events_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'execution_control_events_immutable'
          AND tgrelid = 'execution_control_events'::regclass
    ) THEN
        CREATE TRIGGER execution_control_events_immutable
        BEFORE UPDATE OR DELETE ON execution_control_events
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_execution_control_events_immutable$;

CREATE OR REPLACE FUNCTION autoquant_guard_execution_control_state()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.account_id IS DISTINCT FROM OLD.account_id
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'execution control identity is immutable';
    END IF;
    IF NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'execution control version must increment by one';
    END IF;
    IF NEW.changed_at < OLD.changed_at THEN
        RAISE EXCEPTION 'execution control time cannot move backwards';
    END IF;
    RETURN NEW;
END;
$$;

DO $create_execution_control_state_guard$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'execution_control_state_guard'
          AND tgrelid = 'execution_control_state'::regclass
    ) THEN
        CREATE TRIGGER execution_control_state_guard
        BEFORE UPDATE ON execution_control_state
        FOR EACH ROW EXECUTE FUNCTION autoquant_guard_execution_control_state();
    END IF;
END;
$create_execution_control_state_guard$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 9)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
