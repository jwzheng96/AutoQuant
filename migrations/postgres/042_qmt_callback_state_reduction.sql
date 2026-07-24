CREATE UNIQUE INDEX IF NOT EXISTS
    qmt_order_correlation_bindings_candidate_scope_idx
ON qmt_order_correlation_bindings(
    candidate_hash, gateway_holder_id,
    qmt_session_id, qmt_lease_generation
);

CREATE TABLE IF NOT EXISTS qmt_broker_order_projections
(
    projection_hash text NOT NULL CHECK (
        projection_hash ~ '^[0-9a-f]{64}$'
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
    candidate_hash text NOT NULL UNIQUE REFERENCES
        qmt_canary_order_candidates(candidate_hash),
    client_order_id text NOT NULL UNIQUE CHECK (
        length(btrim(client_order_id)) BETWEEN 1 AND 128
        AND client_order_id = btrim(client_order_id)
    ),
    broker_order_id text NOT NULL CHECK (
        broker_order_id ~ '^[1-9][0-9]*$'
    ),
    instrument text NOT NULL CHECK (
        instrument ~ '^[0-9]{6}\.(XSHG|XSHE)$'
    ),
    side text NOT NULL CHECK (side IN ('buy', 'sell')),
    quantity integer NOT NULL CHECK (quantity > 0),
    limit_price numeric NOT NULL CHECK (limit_price > 0),
    order_remark text NOT NULL CHECK (
        octet_length(order_remark) <= 24
    ),
    reported_traded_volume integer CHECK (
        reported_traded_volume BETWEEN 0 AND quantity
    ),
    reported_average_price numeric CHECK (
        reported_average_price > 0
    ),
    raw_order_status integer,
    order_state text NOT NULL CHECK (
        order_state IN (
            'submitted', 'partially_filled', 'filled',
            'cancelled', 'rejected', 'unknown'
        )
    ),
    trade_volume integer NOT NULL CHECK (
        trade_volume BETWEEN 0 AND quantity
    ),
    trade_amount numeric NOT NULL CHECK (trade_amount >= 0),
    convergence text NOT NULL CHECK (
        convergence IN ('pending', 'converged', 'unknown')
    ),
    last_callback_sequence bigint NOT NULL CHECK (
        last_callback_sequence > 0
    ),
    last_callback_event_hash text NOT NULL CHECK (
        last_callback_event_hash ~ '^[0-9a-f]{64}$'
    ) REFERENCES qmt_callback_inbox_events(event_hash),
    updated_at timestamptz NOT NULL,
    broker_mutation_allowed boolean NOT NULL DEFAULT false CHECK (
        NOT broker_mutation_allowed
    ),
    projection_version text NOT NULL CHECK (
        projection_version = 'qmt-broker-order-projection-v1'
    ),
    projection_payload jsonb NOT NULL CHECK (
        jsonb_typeof(projection_payload) = 'object'
        AND projection_payload->>'projection_hash' IS NULL
        AND projection_payload->>'broker_mutation_allowed' = 'false'
        AND projection_payload->>'candidate_hash' = candidate_hash
        AND projection_payload->>'broker_order_id' = broker_order_id
    ),
    PRIMARY KEY (
        account_id, gateway_holder_id, qmt_session_id,
        qmt_lease_generation, broker_order_id
    ),
    UNIQUE (
        account_id, gateway_holder_id, qmt_session_id,
        qmt_lease_generation, client_order_id
    ),
    UNIQUE (
        candidate_hash, gateway_holder_id,
        qmt_session_id, qmt_lease_generation
    ),
    FOREIGN KEY (
        candidate_hash, gateway_holder_id,
        qmt_session_id, qmt_lease_generation
    ) REFERENCES qmt_order_correlation_bindings(
        candidate_hash, gateway_holder_id,
        qmt_session_id, qmt_lease_generation
    ),
    CHECK (
        (reported_traded_volume IS NULL) =
            (raw_order_status IS NULL)
        AND (
            (reported_traded_volume IS NULL
                AND reported_average_price IS NULL)
            OR (reported_traded_volume = 0
                AND reported_average_price IS NULL)
            OR (reported_traded_volume > 0
                AND reported_average_price IS NOT NULL)
        )
        AND (
            (trade_volume = 0 AND trade_amount = 0)
            OR (trade_volume > 0 AND trade_amount > 0)
        )
    )
);

CREATE TABLE IF NOT EXISTS qmt_broker_trade_facts
(
    fact_hash text PRIMARY KEY CHECK (
        fact_hash ~ '^[0-9a-f]{64}$'
    ),
    account_id text NOT NULL,
    gateway_holder_id text NOT NULL,
    qmt_session_id integer NOT NULL,
    qmt_lease_generation bigint NOT NULL,
    candidate_hash text NOT NULL,
    client_order_id text NOT NULL,
    broker_order_id text NOT NULL,
    trade_id text NOT NULL CHECK (
        length(btrim(trade_id)) BETWEEN 1 AND 256
        AND trade_id = btrim(trade_id)
    ),
    instrument text NOT NULL CHECK (
        instrument ~ '^[0-9]{6}\.(XSHG|XSHE)$'
    ),
    side text NOT NULL CHECK (side IN ('buy', 'sell')),
    volume integer NOT NULL CHECK (volume > 0),
    price numeric NOT NULL CHECK (price > 0),
    amount numeric NOT NULL CHECK (amount > 0),
    order_remark text NOT NULL CHECK (
        octet_length(order_remark) <= 24
    ),
    callback_event_hash text NOT NULL UNIQUE REFERENCES
        qmt_callback_inbox_events(event_hash),
    observed_at timestamptz NOT NULL,
    broker_mutation_allowed boolean NOT NULL DEFAULT false CHECK (
        NOT broker_mutation_allowed
    ),
    fact_version text NOT NULL CHECK (
        fact_version = 'qmt-broker-trade-fact-v1'
    ),
    fact_payload jsonb NOT NULL CHECK (
        jsonb_typeof(fact_payload) = 'object'
        AND fact_payload->>'broker_mutation_allowed' = 'false'
        AND fact_payload->>'candidate_hash' = candidate_hash
        AND fact_payload->>'trade_id' = trade_id
    ),
    UNIQUE (
        account_id, gateway_holder_id, qmt_session_id,
        qmt_lease_generation, trade_id
    ),
    FOREIGN KEY (
        account_id, gateway_holder_id, qmt_session_id,
        qmt_lease_generation, broker_order_id
    ) REFERENCES qmt_broker_order_projections(
        account_id, gateway_holder_id, qmt_session_id,
        qmt_lease_generation, broker_order_id
    )
);

CREATE TABLE IF NOT EXISTS qmt_callback_processing_events
(
    processing_hash text PRIMARY KEY CHECK (
        processing_hash ~ '^[0-9a-f]{64}$'
    ),
    callback_event_hash text NOT NULL UNIQUE REFERENCES
        qmt_callback_inbox_events(event_hash),
    callback_receipt_hash text NOT NULL UNIQUE REFERENCES
        qmt_callback_persistence_receipts(receipt_hash),
    account_id text NOT NULL,
    gateway_holder_id text NOT NULL,
    qmt_session_id integer NOT NULL,
    qmt_lease_generation bigint NOT NULL,
    local_sequence bigint NOT NULL CHECK (local_sequence > 0),
    kind text NOT NULL,
    disposition text NOT NULL CHECK (
        disposition IN (
            'observed', 'async_bound', 'order_applied',
            'trade_applied', 'duplicate_trade',
            'pending_reconciliation', 'broker_state_unknown'
        )
    ),
    reason text NOT NULL CHECK (
        reason ~ '^[A-Za-z0-9_]{1,64}$'
    ),
    previous_hash text NOT NULL CHECK (
        previous_hash ~ '^[0-9a-f]{64}$'
    ),
    candidate_hash text,
    client_order_id text,
    broker_order_id text,
    projection_hash text CHECK (
        projection_hash IS NULL
        OR projection_hash ~ '^[0-9a-f]{64}$'
    ),
    broker_mutation_allowed boolean NOT NULL DEFAULT false CHECK (
        NOT broker_mutation_allowed
    ),
    processing_version text NOT NULL CHECK (
        processing_version = 'qmt-callback-processing-event-v1'
    ),
    processing_payload jsonb NOT NULL CHECK (
        jsonb_typeof(processing_payload) = 'object'
        AND processing_payload->>'broker_mutation_allowed' = 'false'
        AND processing_payload->>'callback_event_hash' =
            callback_event_hash
        AND processing_payload->>'callback_receipt_hash' =
            callback_receipt_hash
    ),
    UNIQUE (
        account_id, gateway_holder_id, qmt_session_id,
        qmt_lease_generation, local_sequence
    ),
    FOREIGN KEY (
        callback_event_hash, account_id, gateway_holder_id,
        qmt_session_id, qmt_lease_generation, local_sequence
    ) REFERENCES qmt_callback_inbox_events(
        event_hash, account_id, gateway_holder_id,
        qmt_session_id, qmt_lease_generation, local_sequence
    ),
    CHECK (
        (candidate_hash IS NULL)
        = (client_order_id IS NULL)
        AND (candidate_hash IS NULL)
        = (broker_order_id IS NULL)
        AND (
            projection_hash IS NULL
            OR candidate_hash IS NOT NULL
        )
    )
);

CREATE TABLE IF NOT EXISTS qmt_callback_processing_cursors
(
    account_id text NOT NULL,
    gateway_holder_id text NOT NULL,
    qmt_session_id integer NOT NULL,
    qmt_lease_generation bigint NOT NULL,
    last_local_sequence bigint NOT NULL CHECK (
        last_local_sequence >= 0
    ),
    last_callback_event_hash text NOT NULL CHECK (
        last_callback_event_hash ~ '^[0-9a-f]{64}$'
    ),
    last_processing_hash text NOT NULL CHECK (
        last_processing_hash ~ '^[0-9a-f]{64}$'
    ),
    fatal_reason text CHECK (
        fatal_reason IS NULL
        OR fatal_reason ~ '^[A-Za-z0-9_]{1,64}$'
    ),
    broker_state_known boolean NOT NULL,
    updated_at timestamptz NOT NULL,
    broker_mutation_allowed boolean NOT NULL DEFAULT false CHECK (
        NOT broker_mutation_allowed
    ),
    cursor_version text NOT NULL CHECK (
        cursor_version = 'qmt-callback-processing-cursor-v1'
    ),
    PRIMARY KEY (
        account_id, gateway_holder_id, qmt_session_id,
        qmt_lease_generation
    ),
    CHECK (
        (last_local_sequence = 0) =
            (last_callback_event_hash = repeat('0', 64))
        AND (last_local_sequence = 0) =
            (last_processing_hash = repeat('0', 64))
        AND (
            fatal_reason IS NULL
            OR NOT broker_state_known
        )
    )
);

DO $create_qmt_callback_reduction_constraints$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname =
            'qmt_callback_processing_events_candidate_hash_fkey'
          AND conrelid =
            'qmt_callback_processing_events'::regclass
    ) THEN
        ALTER TABLE qmt_callback_processing_events
            ADD CONSTRAINT
                qmt_callback_processing_events_candidate_hash_fkey
            FOREIGN KEY (candidate_hash)
            REFERENCES qmt_canary_order_candidates(candidate_hash);
    END IF;
END;
$create_qmt_callback_reduction_constraints$;

CREATE INDEX IF NOT EXISTS qmt_callback_processing_scope_idx
ON qmt_callback_processing_events(
    account_id, qmt_session_id, qmt_lease_generation,
    local_sequence
);

DO $create_qmt_callback_reduction_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'qmt_broker_trade_facts_immutable'
          AND tgrelid = 'qmt_broker_trade_facts'::regclass
    ) THEN
        CREATE TRIGGER qmt_broker_trade_facts_immutable
        BEFORE UPDATE OR DELETE ON qmt_broker_trade_facts
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'qmt_callback_processing_events_immutable'
          AND tgrelid = 'qmt_callback_processing_events'::regclass
    ) THEN
        CREATE TRIGGER qmt_callback_processing_events_immutable
        BEFORE UPDATE OR DELETE ON qmt_callback_processing_events
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_qmt_callback_reduction_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 42)
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
