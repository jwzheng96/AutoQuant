CREATE TABLE IF NOT EXISTS qmt_callback_inbox_events
(
    event_hash text PRIMARY KEY CHECK (
        event_hash ~ '^[0-9a-f]{64}$'
    ),
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
    qmt_lease_action text NOT NULL DEFAULT 'acquire' CHECK (
        qmt_lease_action = 'acquire'
    ),
    broker_session_date date NOT NULL,
    local_sequence bigint NOT NULL CHECK (local_sequence > 0),
    kind text NOT NULL CHECK (
        kind IN (
            'disconnected',
            'account_status',
            'order',
            'trade',
            'order_error',
            'cancel_error',
            'async_order_response'
        )
    ),
    received_at timestamptz NOT NULL,
    previous_hash text NOT NULL CHECK (
        previous_hash ~ '^[0-9a-f]{64}$'
    ),
    callback_payload_hash text NOT NULL CHECK (
        callback_payload_hash ~ '^[0-9a-f]{64}$'
    ),
    redacted_payload jsonb NOT NULL CHECK (
        jsonb_typeof(redacted_payload) = 'object'
        AND NOT (redacted_payload ? 'account_id')
        AND NOT (redacted_payload ? 'status_msg')
    ),
    broker_mutation_allowed boolean NOT NULL DEFAULT false CHECK (
        NOT broker_mutation_allowed
    ),
    event_version text NOT NULL CHECK (
        event_version = 'qmt-callback-inbox-event-v1'
    ),
    event_payload jsonb NOT NULL CHECK (
        jsonb_typeof(event_payload) = 'object'
        AND event_payload ? 'broker_mutation_allowed'
        AND event_payload ? 'account_id'
        AND event_payload ? 'redacted_payload'
        AND event_payload->>'broker_mutation_allowed' = 'false'
        AND event_payload->>'account_id' = account_id
        AND event_payload->'redacted_payload' = redacted_payload
        AND NOT ((event_payload->'redacted_payload') ? 'account_id')
        AND NOT ((event_payload->'redacted_payload') ? 'status_msg')
    ),
    UNIQUE (
        account_id, gateway_holder_id, qmt_session_id,
        qmt_lease_generation, local_sequence
    ),
    FOREIGN KEY (
        qmt_session_id, qmt_lease_generation,
        qmt_lease_action, gateway_holder_id
    ) REFERENCES qmt_session_lease_events(
        session_id, generation, action, holder_id
    ),
    CHECK (
        (received_at AT TIME ZONE 'Asia/Shanghai')::date =
            broker_session_date
    )
);

CREATE INDEX IF NOT EXISTS qmt_callback_inbox_scope_idx
ON qmt_callback_inbox_events(
    account_id, qmt_session_id, qmt_lease_generation,
    local_sequence
);

DO $create_qmt_callback_inbox_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'qmt_callback_inbox_events_immutable'
          AND tgrelid = 'qmt_callback_inbox_events'::regclass
    ) THEN
        CREATE TRIGGER qmt_callback_inbox_events_immutable
        BEFORE UPDATE OR DELETE ON qmt_callback_inbox_events
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_qmt_callback_inbox_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 40)
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
