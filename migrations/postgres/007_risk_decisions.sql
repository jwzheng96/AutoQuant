CREATE TABLE IF NOT EXISTS risk_decisions
(
    decision_hash text PRIMARY KEY CHECK (decision_hash ~ '^[0-9a-f]{64}$'),
    account_id text NOT NULL,
    client_order_id text NOT NULL,
    mode text NOT NULL CHECK (mode IN ('paper', 'live')),
    state text NOT NULL CHECK (state IN ('accepted', 'rejected')),
    evaluated_at timestamptz NOT NULL,
    policy_hash text NOT NULL CHECK (policy_hash ~ '^[0-9a-f]{64}$'),
    account_state_hash text NOT NULL CHECK (account_state_hash ~ '^[0-9a-f]{64}$'),
    quote_hash text NOT NULL CHECK (quote_hash ~ '^[0-9a-f]{64}$'),
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (account_id, client_order_id)
);

CREATE INDEX IF NOT EXISTS risk_decisions_evaluated_at_idx
ON risk_decisions (evaluated_at DESC);

CREATE INDEX IF NOT EXISTS risk_decisions_state_evaluated_at_idx
ON risk_decisions (state, evaluated_at DESC);

DO $create_risk_decisions_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'risk_decisions_immutable'
          AND tgrelid = 'risk_decisions'::regclass
    ) THEN
        CREATE TRIGGER risk_decisions_immutable
        BEFORE UPDATE OR DELETE ON risk_decisions
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_risk_decisions_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 7)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
