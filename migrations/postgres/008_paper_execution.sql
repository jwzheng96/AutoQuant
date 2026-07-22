CREATE TABLE IF NOT EXISTS paper_orders
(
    order_hash text PRIMARY KEY CHECK (order_hash ~ '^[0-9a-f]{64}$'),
    account_id text NOT NULL,
    client_order_id text NOT NULL,
    risk_decision_hash text NOT NULL REFERENCES risk_decisions(decision_hash),
    instrument text NOT NULL,
    side text NOT NULL CHECK (side IN ('buy', 'sell')),
    quantity bigint NOT NULL CHECK (quantity > 0),
    approved_at timestamptz NOT NULL,
    state text NOT NULL CHECK (
        state IN ('approved', 'submitted', 'partially_filled', 'filled',
                  'cancelled', 'rejected', 'unknown')
    ),
    broker_order_id text,
    version bigint NOT NULL CHECK (version >= 0),
    projection_hash text NOT NULL CHECK (projection_hash ~ '^[0-9a-f]{64}$'),
    order_payload jsonb NOT NULL,
    projection_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL,
    UNIQUE (account_id, client_order_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS paper_orders_broker_order_id_idx
ON paper_orders (account_id, broker_order_id)
WHERE broker_order_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS paper_orders_state_updated_at_idx
ON paper_orders (state, updated_at DESC);

CREATE TABLE IF NOT EXISTS paper_order_events
(
    event_hash text PRIMARY KEY CHECK (event_hash ~ '^[0-9a-f]{64}$'),
    order_hash text NOT NULL REFERENCES paper_orders(order_hash),
    sequence bigint NOT NULL CHECK (sequence > 0),
    broker_sequence bigint NOT NULL CHECK (broker_sequence > 0),
    previous_hash text NOT NULL CHECK (previous_hash ~ '^[0-9a-f]{64}$'),
    update_hash text NOT NULL CHECK (update_hash ~ '^[0-9a-f]{64}$'),
    resulting_state text NOT NULL CHECK (
        resulting_state IN ('submitted', 'partially_filled', 'filled',
                            'cancelled', 'rejected', 'unknown')
    ),
    transition_projection_hash text NOT NULL
        CHECK (transition_projection_hash ~ '^[0-9a-f]{64}$'),
    update_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (order_hash, sequence),
    UNIQUE (order_hash, broker_sequence)
);

CREATE TABLE IF NOT EXISTS execution_account_snapshots
(
    snapshot_hash text PRIMARY KEY CHECK (snapshot_hash ~ '^[0-9a-f]{64}$'),
    account_id text NOT NULL,
    as_of timestamptz NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS execution_account_snapshots_account_time_idx
ON execution_account_snapshots (account_id, as_of DESC);

CREATE TABLE IF NOT EXISTS execution_reconciliation_reports
(
    report_hash text PRIMARY KEY CHECK (report_hash ~ '^[0-9a-f]{64}$'),
    account_id text NOT NULL,
    evaluated_at timestamptz NOT NULL,
    internal_snapshot_hash text NOT NULL
        REFERENCES execution_account_snapshots(snapshot_hash),
    broker_snapshot_hash text NOT NULL
        REFERENCES execution_account_snapshots(snapshot_hash),
    reconciled boolean NOT NULL,
    issues text[] NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS execution_reconciliation_reports_account_time_idx
ON execution_reconciliation_reports (account_id, evaluated_at DESC);

DO $create_paper_execution_immutable$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'paper_order_events',
        'execution_account_snapshots',
        'execution_reconciliation_reports'
    ]
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgname = table_name || '_immutable'
              AND tgrelid = table_name::regclass
        ) THEN
            EXECUTE format(
                'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I '
                'FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change()',
                table_name || '_immutable',
                table_name
            );
        END IF;
    END LOOP;
END;
$create_paper_execution_immutable$;

CREATE OR REPLACE FUNCTION autoquant_guard_paper_order_projection()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.order_hash IS DISTINCT FROM OLD.order_hash
       OR NEW.account_id IS DISTINCT FROM OLD.account_id
       OR NEW.client_order_id IS DISTINCT FROM OLD.client_order_id
       OR NEW.risk_decision_hash IS DISTINCT FROM OLD.risk_decision_hash
       OR NEW.instrument IS DISTINCT FROM OLD.instrument
       OR NEW.side IS DISTINCT FROM OLD.side
       OR NEW.quantity IS DISTINCT FROM OLD.quantity
       OR NEW.approved_at IS DISTINCT FROM OLD.approved_at
       OR NEW.order_payload IS DISTINCT FROM OLD.order_payload
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'paper order identity is immutable';
    END IF;
    IF NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'paper order projection version must increment by one';
    END IF;
    IF NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'paper order projection time cannot move backwards';
    END IF;
    IF OLD.state IN ('filled', 'cancelled', 'rejected') THEN
        RAISE EXCEPTION 'terminal paper order projection is immutable';
    END IF;
    RETURN NEW;
END;
$$;

DO $create_paper_order_projection_guard$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'paper_orders_projection_guard'
          AND tgrelid = 'paper_orders'::regclass
    ) THEN
        CREATE TRIGGER paper_orders_projection_guard
        BEFORE UPDATE ON paper_orders
        FOR EACH ROW EXECUTE FUNCTION autoquant_guard_paper_order_projection();
    END IF;
END;
$create_paper_order_projection_guard$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 8)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
