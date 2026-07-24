CREATE TABLE IF NOT EXISTS qmt_callback_reconciliation_reports
(
    report_hash text PRIMARY KEY CHECK (
        report_hash ~ '^[0-9a-f]{64}$'
    ),
    logical_account_id text NOT NULL CHECK (
        length(btrim(logical_account_id)) BETWEEN 1 AND 128
        AND logical_account_id = btrim(logical_account_id)
    ),
    gateway_holder_id text NOT NULL CHECK (
        gateway_holder_id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'
    ),
    qmt_session_id integer NOT NULL CHECK (qmt_session_id > 0),
    qmt_lease_generation bigint NOT NULL CHECK (
        qmt_lease_generation > 0
    ),
    acceptance_evidence_hash text NOT NULL REFERENCES
        qmt_readonly_acceptance_evidence(evidence_hash),
    baseline_evidence_hash text NOT NULL CHECK (
        baseline_evidence_hash ~ '^[0-9a-f]{64}$'
    ),
    callback_processing_hash text NOT NULL CHECK (
        callback_processing_hash ~ '^[0-9a-f]{64}$'
    ),
    callback_cursor bigint NOT NULL CHECK (callback_cursor >= 0),
    state text NOT NULL CHECK (state IN ('passed', 'rejected')),
    projection_hashes jsonb NOT NULL CHECK (
        jsonb_typeof(projection_hashes) = 'array'
    ),
    trade_fact_hashes jsonb NOT NULL CHECK (
        jsonb_typeof(trade_fact_hashes) = 'array'
    ),
    matched_broker_order_ids jsonb NOT NULL CHECK (
        jsonb_typeof(matched_broker_order_ids) = 'array'
    ),
    matched_trade_ids jsonb NOT NULL CHECK (
        jsonb_typeof(matched_trade_ids) = 'array'
    ),
    issues jsonb NOT NULL CHECK (
        jsonb_typeof(issues) = 'array'
    ),
    observed_at timestamptz NOT NULL,
    broker_mutation_allowed boolean NOT NULL DEFAULT false CHECK (
        NOT broker_mutation_allowed
    ),
    report_version text NOT NULL CHECK (
        report_version = 'qmt-callback-reconciliation-v1'
    ),
    report_payload jsonb NOT NULL CHECK (
        jsonb_typeof(report_payload) = 'object'
        AND report_payload->>'broker_mutation_allowed' = 'false'
        AND report_payload->>'acceptance_evidence_hash' =
            acceptance_evidence_hash
        AND report_payload->>'baseline_evidence_hash' =
            baseline_evidence_hash
        AND report_payload->>'callback_processing_hash' =
            callback_processing_hash
        AND report_payload->>'state' = state
        AND report_payload->'projection_hashes' =
            projection_hashes
        AND report_payload->'trade_fact_hashes' =
            trade_fact_hashes
        AND report_payload->'matched_broker_order_ids' =
            matched_broker_order_ids
        AND report_payload->'matched_trade_ids' =
            matched_trade_ids
        AND report_payload->'issues' = issues
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (
        acceptance_evidence_hash, callback_processing_hash
    ),
    CHECK (
        (state = 'passed' AND issues = '[]'::jsonb)
        OR (state = 'rejected' AND issues <> '[]'::jsonb)
    )
);

CREATE INDEX IF NOT EXISTS qmt_callback_reconciliation_scope_idx
ON qmt_callback_reconciliation_reports(
    logical_account_id, qmt_session_id,
    qmt_lease_generation, observed_at DESC
);

DO $create_qmt_callback_reconciliation_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'qmt_callback_reconciliation_reports_immutable'
          AND tgrelid =
            'qmt_callback_reconciliation_reports'::regclass
    ) THEN
        CREATE TRIGGER qmt_callback_reconciliation_reports_immutable
        BEFORE UPDATE OR DELETE
        ON qmt_callback_reconciliation_reports
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_qmt_callback_reconciliation_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 43)
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
