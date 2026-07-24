DO $require_empty_qmt_canary_stage_v1$
BEGIN
    IF COALESCE(
        (
            SELECT version
            FROM schema_versions
            WHERE component = 'postgres'
        ),
        0
    ) < 39
    AND EXISTS (
        SELECT 1
        FROM qmt_canary_order_candidates
        LIMIT 1
    ) THEN
        RAISE EXCEPTION
            'schema v39 requires the non-executable qmt canary candidate ledger to be empty';
    END IF;
END;
$require_empty_qmt_canary_stage_v1$;

ALTER TABLE qmt_canary_order_candidates
    DROP CONSTRAINT IF EXISTS qmt_canary_candidate_stage_version;

ALTER TABLE qmt_canary_order_candidates
    ADD CONSTRAINT qmt_canary_candidate_stage_version CHECK (
        stage_version = 'qmt-canary-order-stage-v2'
    );

CREATE UNIQUE INDEX IF NOT EXISTS
    qmt_canary_candidate_stage_candidate_unique_idx
ON qmt_canary_order_candidates(stage_hash, candidate_hash);

CREATE TABLE IF NOT EXISTS qmt_order_remark_recovery_bindings
(
    recovery_hash text PRIMARY KEY CHECK (
        recovery_hash ~ '^[0-9a-f]{64}$'
    ),
    stage_hash text NOT NULL UNIQUE,
    candidate_hash text NOT NULL UNIQUE,
    account_id text NOT NULL CHECK (
        length(btrim(account_id)) BETWEEN 1 AND 128
        AND account_id = btrim(account_id)
    ),
    broker_session_date date NOT NULL,
    broker_order_id text NOT NULL CHECK (
        broker_order_id ~ '^[1-9][0-9]*$'
    ),
    client_order_id text NOT NULL UNIQUE CHECK (
        length(btrim(client_order_id)) BETWEEN 1 AND 128
        AND client_order_id = btrim(client_order_id)
    ),
    baseline_hash text NOT NULL CHECK (
        baseline_hash ~ '^[0-9a-f]{64}$'
    ),
    observed_at timestamptz NOT NULL,
    recovery_holder_id text NOT NULL CHECK (
        recovery_holder_id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'
    ),
    qmt_session_id integer NOT NULL CHECK (qmt_session_id > 0),
    recovery_lease_generation bigint NOT NULL CHECK (
        recovery_lease_generation > 0
    ),
    recovery_lease_action text NOT NULL DEFAULT 'acquire' CHECK (
        recovery_lease_action = 'acquire'
    ),
    broker_mutation_allowed boolean NOT NULL DEFAULT false CHECK (
        NOT broker_mutation_allowed
    ),
    recovery_version text NOT NULL CHECK (
        recovery_version = 'qmt-canary-remark-recovery-v1'
    ),
    payload jsonb NOT NULL CHECK (
        payload ? 'broker_mutation_allowed'
        AND payload->>'broker_mutation_allowed' = 'false'
    ),
    UNIQUE (
        account_id, broker_session_date, broker_order_id
    ),
    FOREIGN KEY (stage_hash, candidate_hash)
        REFERENCES qmt_canary_order_candidates(
            stage_hash, candidate_hash
        ),
    FOREIGN KEY (
        qmt_session_id, recovery_lease_generation,
        recovery_lease_action, recovery_holder_id
    ) REFERENCES qmt_session_lease_events(
        session_id, generation, action, holder_id
    )
);

DO $create_qmt_remark_recovery_checks$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'qmt_remark_recovery_same_session_date'
          AND conrelid = 'qmt_order_remark_recovery_bindings'::regclass
    ) THEN
        ALTER TABLE qmt_order_remark_recovery_bindings
            ADD CONSTRAINT qmt_remark_recovery_same_session_date CHECK (
                (observed_at AT TIME ZONE 'Asia/Shanghai')::date =
                    broker_session_date
            );
    END IF;
END;
$create_qmt_remark_recovery_checks$;

DO $create_qmt_remark_recovery_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'qmt_order_remark_recovery_bindings_immutable'
          AND tgrelid = 'qmt_order_remark_recovery_bindings'::regclass
    ) THEN
        CREATE TRIGGER qmt_order_remark_recovery_bindings_immutable
        BEFORE UPDATE OR DELETE ON qmt_order_remark_recovery_bindings
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_qmt_remark_recovery_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 39)
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
