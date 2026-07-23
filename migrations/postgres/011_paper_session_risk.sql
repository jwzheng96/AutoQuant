CREATE TABLE IF NOT EXISTS paper_session_risk_state
(
    account_id text NOT NULL,
    session_date date NOT NULL,
    day_start_equity numeric NOT NULL CHECK (day_start_equity > 0),
    peak_equity numeric NOT NULL CHECK (peak_equity > 0),
    cumulative_turnover numeric NOT NULL CHECK (cumulative_turnover >= 0),
    as_of timestamptz NOT NULL,
    latest_observation_hash text NOT NULL
        CHECK (latest_observation_hash ~ '^[0-9a-f]{64}$'),
    latest_snapshot_hash text NOT NULL
        REFERENCES execution_account_snapshots(snapshot_hash),
    turnover_evidence_hash text NOT NULL
        CHECK (turnover_evidence_hash ~ '^[0-9a-f]{64}$'),
    version bigint NOT NULL CHECK (version > 0),
    last_event_hash text NOT NULL CHECK (last_event_hash ~ '^[0-9a-f]{64}$'),
    state_hash text NOT NULL CHECK (state_hash ~ '^[0-9a-f]{64}$'),
    state_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (account_id, session_date)
);

CREATE TABLE IF NOT EXISTS paper_session_risk_events
(
    event_hash text PRIMARY KEY CHECK (event_hash ~ '^[0-9a-f]{64}$'),
    account_id text NOT NULL,
    session_date date NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    previous_hash text NOT NULL CHECK (previous_hash ~ '^[0-9a-f]{64}$'),
    observation_hash text NOT NULL CHECK (observation_hash ~ '^[0-9a-f]{64}$'),
    transition_state_hash text NOT NULL
        CHECK (transition_state_hash ~ '^[0-9a-f]{64}$'),
    observation_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (account_id, session_date)
        REFERENCES paper_session_risk_state(account_id, session_date),
    UNIQUE (account_id, session_date, sequence),
    UNIQUE (account_id, session_date, observation_hash)
);

DO $create_paper_session_risk_events_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'paper_session_risk_events_immutable'
          AND tgrelid = 'paper_session_risk_events'::regclass
    ) THEN
        CREATE TRIGGER paper_session_risk_events_immutable
        BEFORE UPDATE OR DELETE ON paper_session_risk_events
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_paper_session_risk_events_immutable$;

CREATE OR REPLACE FUNCTION autoquant_guard_paper_session_risk_state()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.account_id IS DISTINCT FROM OLD.account_id
       OR NEW.session_date IS DISTINCT FROM OLD.session_date
       OR NEW.day_start_equity IS DISTINCT FROM OLD.day_start_equity
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'paper session risk identity is immutable';
    END IF;
    IF NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'paper session risk version must increment by one';
    END IF;
    IF NEW.as_of < OLD.as_of OR NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'paper session risk time cannot move backwards';
    END IF;
    IF NEW.peak_equity < OLD.peak_equity
       OR NEW.cumulative_turnover < OLD.cumulative_turnover THEN
        RAISE EXCEPTION 'paper session risk metrics cannot decrease';
    END IF;
    RETURN NEW;
END;
$$;

DO $create_paper_session_risk_state_guard$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'paper_session_risk_state_guard'
          AND tgrelid = 'paper_session_risk_state'::regclass
    ) THEN
        CREATE TRIGGER paper_session_risk_state_guard
        BEFORE UPDATE ON paper_session_risk_state
        FOR EACH ROW EXECUTE FUNCTION autoquant_guard_paper_session_risk_state();
    END IF;
END;
$create_paper_session_risk_state_guard$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 11)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
