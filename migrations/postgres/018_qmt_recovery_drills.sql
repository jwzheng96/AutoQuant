CREATE TABLE IF NOT EXISTS qmt_recovery_drill_events
(
    event_hash text PRIMARY KEY CHECK (event_hash ~ '^[0-9a-f]{64}$'),
    drill_id uuid NOT NULL,
    sequence bigint NOT NULL CHECK (sequence IN (1, 2)),
    account_id text NOT NULL CHECK (char_length(account_id) BETWEEN 1 AND 128),
    kind text NOT NULL CHECK (
        kind IN ('disconnect_recovery', 'miniqmt_restart_recovery')
    ),
    action text NOT NULL CHECK (action IN ('start', 'complete')),
    actor text NOT NULL CHECK (char_length(actor) BETWEEN 1 AND 128),
    occurred_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL CHECK (expires_at > occurred_at),
    baseline_qmt_evidence_hash text NOT NULL
        REFERENCES qmt_readonly_acceptance_evidence(evidence_hash),
    recovery_qmt_evidence_hash text
        REFERENCES qmt_readonly_acceptance_evidence(evidence_hash),
    failure_control_event_hash text
        REFERENCES execution_control_events(event_hash),
    previous_hash text NOT NULL CHECK (previous_hash ~ '^[0-9a-f]{64}$'),
    event_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (drill_id, sequence),
    CHECK (
        (
            action = 'start'
            AND sequence = 1
            AND recovery_qmt_evidence_hash IS NULL
            AND failure_control_event_hash IS NULL
        )
        OR
        (
            action = 'complete'
            AND sequence = 2
            AND recovery_qmt_evidence_hash IS NOT NULL
            AND recovery_qmt_evidence_hash <> baseline_qmt_evidence_hash
            AND failure_control_event_hash IS NOT NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS qmt_recovery_drill_events_account_time_idx
ON qmt_recovery_drill_events (account_id, occurred_at DESC);

DO $create_qmt_recovery_drill_events_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'qmt_recovery_drill_events_immutable'
          AND tgrelid = 'qmt_recovery_drill_events'::regclass
    ) THEN
        CREATE TRIGGER qmt_recovery_drill_events_immutable
        BEFORE UPDATE OR DELETE ON qmt_recovery_drill_events
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_qmt_recovery_drill_events_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 18)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
