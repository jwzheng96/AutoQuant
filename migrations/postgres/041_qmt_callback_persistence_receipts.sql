CREATE UNIQUE INDEX IF NOT EXISTS
    qmt_callback_inbox_event_scope_unique_idx
ON qmt_callback_inbox_events(
    event_hash, account_id, gateway_holder_id,
    qmt_session_id, qmt_lease_generation, local_sequence
);

CREATE TABLE IF NOT EXISTS qmt_callback_persistence_receipts
(
    receipt_hash text PRIMARY KEY CHECK (
        receipt_hash ~ '^[0-9a-f]{64}$'
    ),
    event_hash text NOT NULL UNIQUE,
    account_id text NOT NULL CHECK (
        length(btrim(account_id)) BETWEEN 1 AND 128
        AND account_id = btrim(account_id)
    ),
    gateway_holder_id text NOT NULL CHECK (
        gateway_holder_id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'
    ),
    qmt_session_id integer NOT NULL CHECK (qmt_session_id > 0),
    qmt_lease_generation bigint NOT NULL CHECK (
        qmt_lease_generation > 0
    ),
    broker_session_date date NOT NULL,
    local_sequence bigint NOT NULL CHECK (local_sequence > 0),
    received_at timestamptz NOT NULL,
    persisted_at timestamptz NOT NULL,
    broker_mutation_allowed boolean NOT NULL DEFAULT false CHECK (
        NOT broker_mutation_allowed
    ),
    receipt_version text NOT NULL CHECK (
        receipt_version = 'qmt-callback-persistence-receipt-v1'
    ),
    receipt_payload jsonb NOT NULL CHECK (
        jsonb_typeof(receipt_payload) = 'object'
        AND receipt_payload ? 'broker_mutation_allowed'
        AND receipt_payload->>'broker_mutation_allowed' = 'false'
        AND receipt_payload->>'event_hash' = event_hash
        AND receipt_payload->>'account_id' = account_id
    ),
    UNIQUE (
        account_id, gateway_holder_id, qmt_session_id,
        qmt_lease_generation, local_sequence
    ),
    FOREIGN KEY (
        event_hash, account_id, gateway_holder_id,
        qmt_session_id, qmt_lease_generation, local_sequence
    ) REFERENCES qmt_callback_inbox_events(
        event_hash, account_id, gateway_holder_id,
        qmt_session_id, qmt_lease_generation, local_sequence
    ),
    CHECK (
        persisted_at >= received_at
        AND persisted_at <= received_at + interval '5 seconds'
        AND (persisted_at AT TIME ZONE 'Asia/Shanghai')::date =
            broker_session_date
    )
);

CREATE INDEX IF NOT EXISTS qmt_callback_receipt_scope_idx
ON qmt_callback_persistence_receipts(
    account_id, qmt_session_id, qmt_lease_generation,
    local_sequence
);

DO $create_qmt_callback_receipt_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'qmt_callback_persistence_receipts_immutable'
          AND tgrelid = 'qmt_callback_persistence_receipts'::regclass
    ) THEN
        CREATE TRIGGER qmt_callback_persistence_receipts_immutable
        BEFORE UPDATE OR DELETE ON qmt_callback_persistence_receipts
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_qmt_callback_receipt_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 41)
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
