CREATE TABLE IF NOT EXISTS paper_runtime_unlock_evidence
(
    evidence_hash text PRIMARY KEY CHECK (evidence_hash ~ '^[0-9a-f]{64}$'),
    account_id text NOT NULL CHECK (char_length(account_id) BETWEEN 1 AND 128),
    strategy_id text NOT NULL CHECK (char_length(strategy_id) BETWEEN 1 AND 128),
    session_date date NOT NULL,
    evaluated_at timestamptz NOT NULL,
    registration_hash text NOT NULL
        REFERENCES paper_strategy_registrations(registration_hash),
    calendar_hash text NOT NULL CHECK (calendar_hash ~ '^[0-9a-f]{64}$'),
    session_state_hash text NOT NULL CHECK (session_state_hash ~ '^[0-9a-f]{64}$'),
    quote_evidence_hash text NOT NULL CHECK (quote_evidence_hash ~ '^[0-9a-f]{64}$'),
    reconciliation_report_hash text NOT NULL
        REFERENCES execution_reconciliation_reports(report_hash),
    lease_holder_id text NOT NULL
        CHECK (lease_holder_id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'),
    lease_token_hash text NOT NULL CHECK (lease_token_hash ~ '^[0-9a-f]{64}$'),
    lease_generation bigint NOT NULL CHECK (lease_generation > 0),
    evidence_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (account_id, session_date)
        REFERENCES paper_session_risk_state(account_id, session_date)
);

CREATE INDEX IF NOT EXISTS paper_runtime_unlock_account_time_idx
ON paper_runtime_unlock_evidence (account_id, evaluated_at DESC);

DO $create_paper_runtime_unlock_evidence_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'paper_runtime_unlock_evidence_immutable'
          AND tgrelid = 'paper_runtime_unlock_evidence'::regclass
    ) THEN
        CREATE TRIGGER paper_runtime_unlock_evidence_immutable
        BEFORE UPDATE OR DELETE ON paper_runtime_unlock_evidence
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_paper_runtime_unlock_evidence_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 16)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
