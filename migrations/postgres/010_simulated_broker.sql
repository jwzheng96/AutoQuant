CREATE TABLE IF NOT EXISTS simulated_broker_orders
(
    order_hash text PRIMARY KEY REFERENCES paper_orders(order_hash),
    account_id text NOT NULL,
    client_order_id text NOT NULL,
    broker_order_id text NOT NULL UNIQUE,
    state text NOT NULL CHECK (
        state IN ('submitted', 'partially_filled', 'filled',
                  'cancelled', 'rejected', 'unknown')
    ),
    cumulative_filled_quantity bigint NOT NULL CHECK (cumulative_filled_quantity >= 0),
    average_fill_price numeric,
    last_broker_sequence bigint NOT NULL CHECK (last_broker_sequence > 0),
    last_fact_hash text NOT NULL CHECK (last_fact_hash ~ '^[0-9a-f]{64}$'),
    state_hash text NOT NULL CHECK (state_hash ~ '^[0-9a-f]{64}$'),
    order_payload jsonb NOT NULL,
    state_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL,
    UNIQUE (account_id, client_order_id)
);

CREATE INDEX IF NOT EXISTS simulated_broker_orders_state_time_idx
ON simulated_broker_orders (state, updated_at DESC);

CREATE TABLE IF NOT EXISTS simulated_broker_facts
(
    fact_hash text PRIMARY KEY CHECK (fact_hash ~ '^[0-9a-f]{64}$'),
    order_hash text NOT NULL REFERENCES simulated_broker_orders(order_hash),
    broker_sequence bigint NOT NULL CHECK (broker_sequence > 0),
    previous_hash text NOT NULL CHECK (previous_hash ~ '^[0-9a-f]{64}$'),
    update_hash text NOT NULL CHECK (update_hash ~ '^[0-9a-f]{64}$'),
    update_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (order_hash, broker_sequence)
);

CREATE INDEX IF NOT EXISTS simulated_broker_facts_order_sequence_idx
ON simulated_broker_facts (order_hash, broker_sequence);

DO $create_simulated_broker_facts_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'simulated_broker_facts_immutable'
          AND tgrelid = 'simulated_broker_facts'::regclass
    ) THEN
        CREATE TRIGGER simulated_broker_facts_immutable
        BEFORE UPDATE OR DELETE ON simulated_broker_facts
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_simulated_broker_facts_immutable$;

CREATE OR REPLACE FUNCTION autoquant_guard_simulated_broker_order()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.order_hash IS DISTINCT FROM OLD.order_hash
       OR NEW.account_id IS DISTINCT FROM OLD.account_id
       OR NEW.client_order_id IS DISTINCT FROM OLD.client_order_id
       OR NEW.broker_order_id IS DISTINCT FROM OLD.broker_order_id
       OR NEW.order_payload IS DISTINCT FROM OLD.order_payload
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'simulated broker order identity is immutable';
    END IF;
    IF NEW.last_broker_sequence <> OLD.last_broker_sequence + 1 THEN
        RAISE EXCEPTION 'simulated broker sequence must increment by one';
    END IF;
    IF NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'simulated broker time cannot move backwards';
    END IF;
    IF OLD.state IN ('filled', 'cancelled', 'rejected') THEN
        RAISE EXCEPTION 'terminal simulated broker order is immutable';
    END IF;
    RETURN NEW;
END;
$$;

DO $create_simulated_broker_order_guard$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'simulated_broker_orders_guard'
          AND tgrelid = 'simulated_broker_orders'::regclass
    ) THEN
        CREATE TRIGGER simulated_broker_orders_guard
        BEFORE UPDATE ON simulated_broker_orders
        FOR EACH ROW EXECUTE FUNCTION autoquant_guard_simulated_broker_order();
    END IF;
END;
$create_simulated_broker_order_guard$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 10)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
