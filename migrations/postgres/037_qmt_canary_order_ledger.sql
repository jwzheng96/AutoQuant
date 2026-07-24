CREATE UNIQUE INDEX IF NOT EXISTS
    qmt_session_lease_events_generation_holder_idx
ON qmt_session_lease_events
    (session_id, generation, action, holder_id);

CREATE TABLE IF NOT EXISTS qmt_canary_order_candidates
(
    candidate_hash text PRIMARY KEY CHECK (
        candidate_hash ~ '^[0-9a-f]{64}$'
    ),
    account_id text NOT NULL CHECK (
        length(btrim(account_id)) BETWEEN 1 AND 128
        AND account_id = btrim(account_id)
    ),
    strategy_id text NOT NULL CHECK (
        length(btrim(strategy_id)) BETWEEN 1 AND 128
        AND strategy_id = btrim(strategy_id)
    ),
    gateway_holder_id text NOT NULL CHECK (
        length(btrim(gateway_holder_id)) BETWEEN 1 AND 128
        AND gateway_holder_id = btrim(gateway_holder_id)
    ),
    qmt_session_id integer NOT NULL CHECK (qmt_session_id > 0),
    qmt_lease_generation bigint NOT NULL CHECK (
        qmt_lease_generation > 0
    ),
    qmt_lease_action text NOT NULL DEFAULT 'acquire' CHECK (
        qmt_lease_action = 'acquire'
    ),
    client_order_id text NOT NULL UNIQUE CHECK (
        length(btrim(client_order_id)) BETWEEN 1 AND 128
        AND client_order_id = btrim(client_order_id)
    ),
    risk_decision_hash text NOT NULL UNIQUE CHECK (
        risk_decision_hash ~ '^[0-9a-f]{64}$'
    ),
    created_at timestamptz NOT NULL,
    valid_until timestamptz NOT NULL,
    broker_mutation_allowed boolean NOT NULL DEFAULT false CHECK (
        NOT broker_mutation_allowed
    ),
    candidate_version text NOT NULL CHECK (
        candidate_version = 'qmt-canary-order-candidate-v1'
    ),
    payload jsonb NOT NULL,
    CHECK (
        valid_until > created_at
        AND valid_until <= created_at + interval '30 seconds'
    ),
    UNIQUE (
        candidate_hash, gateway_holder_id,
        qmt_session_id, qmt_lease_generation
    ),
    FOREIGN KEY (
        qmt_session_id, qmt_lease_generation,
        qmt_lease_action, gateway_holder_id
    ) REFERENCES qmt_session_lease_events(
        session_id, generation, action, holder_id
    )
);

CREATE TABLE IF NOT EXISTS qmt_order_correlation_reservations
(
    correlation_hash text PRIMARY KEY CHECK (
        correlation_hash ~ '^[0-9a-f]{64}$'
    ),
    candidate_hash text NOT NULL UNIQUE
        REFERENCES qmt_canary_order_candidates(candidate_hash),
    gateway_holder_id text NOT NULL CHECK (
        length(btrim(gateway_holder_id)) BETWEEN 1 AND 128
        AND gateway_holder_id = btrim(gateway_holder_id)
    ),
    qmt_session_id integer NOT NULL CHECK (qmt_session_id > 0),
    qmt_lease_generation bigint NOT NULL CHECK (
        qmt_lease_generation > 0
    ),
    client_order_id text NOT NULL UNIQUE CHECK (
        length(btrim(client_order_id)) BETWEEN 1 AND 128
        AND client_order_id = btrim(client_order_id)
    ),
    async_request_id integer NOT NULL CHECK (
        async_request_id > 0
    ),
    reserved_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    UNIQUE (
        gateway_holder_id, qmt_session_id,
        qmt_lease_generation, async_request_id
    ),
    UNIQUE (
        candidate_hash, gateway_holder_id,
        qmt_session_id, qmt_lease_generation,
        async_request_id
    ),
    FOREIGN KEY (
        candidate_hash, gateway_holder_id,
        qmt_session_id, qmt_lease_generation
    ) REFERENCES qmt_canary_order_candidates(
        candidate_hash, gateway_holder_id,
        qmt_session_id, qmt_lease_generation
    )
);

CREATE TABLE IF NOT EXISTS qmt_order_correlation_bindings
(
    correlation_hash text PRIMARY KEY CHECK (
        correlation_hash ~ '^[0-9a-f]{64}$'
    ),
    candidate_hash text NOT NULL UNIQUE,
    gateway_holder_id text NOT NULL,
    qmt_session_id integer NOT NULL,
    qmt_lease_generation bigint NOT NULL,
    async_request_id integer NOT NULL,
    broker_order_id text NOT NULL CHECK (
        broker_order_id ~ '^[1-9][0-9]*$'
    ),
    bound_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    UNIQUE (
        gateway_holder_id, qmt_session_id,
        qmt_lease_generation, async_request_id
    ),
    UNIQUE (
        gateway_holder_id, qmt_session_id,
        qmt_lease_generation, broker_order_id
    ),
    FOREIGN KEY (
        candidate_hash, gateway_holder_id,
        qmt_session_id, qmt_lease_generation,
        async_request_id
    )
        REFERENCES qmt_order_correlation_reservations(
            candidate_hash, gateway_holder_id,
            qmt_session_id, qmt_lease_generation,
            async_request_id
        )
);

CREATE INDEX IF NOT EXISTS qmt_canary_candidates_scope_idx
ON qmt_canary_order_candidates
    (account_id, gateway_holder_id, qmt_session_id,
     qmt_lease_generation, created_at DESC);

DO $create_qmt_canary_ledger_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'qmt_canary_order_candidates_immutable'
          AND tgrelid = 'qmt_canary_order_candidates'::regclass
    ) THEN
        CREATE TRIGGER qmt_canary_order_candidates_immutable
        BEFORE UPDATE OR DELETE ON qmt_canary_order_candidates
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'qmt_order_correlation_reservations_immutable'
          AND tgrelid = 'qmt_order_correlation_reservations'::regclass
    ) THEN
        CREATE TRIGGER qmt_order_correlation_reservations_immutable
        BEFORE UPDATE OR DELETE ON qmt_order_correlation_reservations
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'qmt_order_correlation_bindings_immutable'
          AND tgrelid = 'qmt_order_correlation_bindings'::regclass
    ) THEN
        CREATE TRIGGER qmt_order_correlation_bindings_immutable
        BEFORE UPDATE OR DELETE ON qmt_order_correlation_bindings
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_qmt_canary_ledger_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 37)
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
