CREATE TABLE IF NOT EXISTS paper_scheduler_state
(
    account_id text PRIMARY KEY,
    last_sequence bigint NOT NULL CHECK (last_sequence > 0),
    last_event_hash text NOT NULL CHECK (last_event_hash ~ '^[0-9a-f]{64}$'),
    last_cycle_hash text NOT NULL CHECK (last_cycle_hash ~ '^[0-9a-f]{64}$'),
    last_evaluated_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS paper_scheduler_events
(
    event_hash text PRIMARY KEY CHECK (event_hash ~ '^[0-9a-f]{64}$'),
    account_id text NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    previous_hash text NOT NULL CHECK (previous_hash ~ '^[0-9a-f]{64}$'),
    cycle_hash text NOT NULL CHECK (cycle_hash ~ '^[0-9a-f]{64}$'),
    strategy_id text NOT NULL,
    session_date date NOT NULL,
    evaluated_at timestamptz NOT NULL,
    phase text NOT NULL CHECK (
        phase IN (
            'non_trading_day', 'closed', 'pre_open', 'opening_auction',
            'auction_pause', 'morning_continuous', 'midday_break',
            'afternoon_continuous', 'closing_auction'
        )
    ),
    status text NOT NULL CHECK (
        status IN (
            'idle', 'locked', 'session_initialized', 'session_ready',
            'no_intents', 'completed', 'failed'
        )
    ),
    control_state_hash text NOT NULL CHECK (control_state_hash ~ '^[0-9a-f]{64}$'),
    calendar_hash text CHECK (calendar_hash IS NULL OR calendar_hash ~ '^[0-9a-f]{64}$'),
    mark_evidence_hash text CHECK (
        mark_evidence_hash IS NULL OR mark_evidence_hash ~ '^[0-9a-f]{64}$'
    ),
    quote_evidence_hash text CHECK (
        quote_evidence_hash IS NULL OR quote_evidence_hash ~ '^[0-9a-f]{64}$'
    ),
    error_code text,
    cycle_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (account_id, sequence),
    UNIQUE (account_id, cycle_hash)
);

CREATE INDEX IF NOT EXISTS paper_scheduler_events_account_time_idx
ON paper_scheduler_events (account_id, evaluated_at DESC);

DO $create_paper_scheduler_events_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'paper_scheduler_events_immutable'
          AND tgrelid = 'paper_scheduler_events'::regclass
    ) THEN
        CREATE TRIGGER paper_scheduler_events_immutable
        BEFORE UPDATE OR DELETE ON paper_scheduler_events
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_paper_scheduler_events_immutable$;

CREATE OR REPLACE FUNCTION autoquant_guard_paper_scheduler_state()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.account_id IS DISTINCT FROM OLD.account_id THEN
        RAISE EXCEPTION 'paper scheduler account identity is immutable';
    END IF;
    IF NEW.last_sequence <> OLD.last_sequence + 1 THEN
        RAISE EXCEPTION 'paper scheduler sequence must increment by one';
    END IF;
    IF NEW.last_evaluated_at < OLD.last_evaluated_at THEN
        RAISE EXCEPTION 'paper scheduler time cannot move backwards';
    END IF;
    RETURN NEW;
END;
$$;

DO $create_paper_scheduler_state_guard$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'paper_scheduler_state_guard'
          AND tgrelid = 'paper_scheduler_state'::regclass
    ) THEN
        CREATE TRIGGER paper_scheduler_state_guard
        BEFORE UPDATE ON paper_scheduler_state
        FOR EACH ROW EXECUTE FUNCTION autoquant_guard_paper_scheduler_state();
    END IF;
END;
$create_paper_scheduler_state_guard$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 13)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
