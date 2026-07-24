DO $require_empty_qmt_canary_candidate_ledger$
BEGIN
    IF COALESCE(
        (
            SELECT version
            FROM schema_versions
            WHERE component = 'postgres'
        ),
        0
    ) < 38
    AND EXISTS (
        SELECT 1
        FROM qmt_canary_order_candidates
        LIMIT 1
    ) THEN
        RAISE EXCEPTION
            'schema v38 requires the non-executable qmt canary candidate ledger to be empty';
    END IF;
END;
$require_empty_qmt_canary_candidate_ledger$;

ALTER TABLE qmt_canary_order_candidates
    ADD COLUMN IF NOT EXISTS stage_hash text,
    ADD COLUMN IF NOT EXISTS broker_order_remark text,
    ADD COLUMN IF NOT EXISTS staged_at timestamptz,
    ADD COLUMN IF NOT EXISTS stage_version text,
    ADD COLUMN IF NOT EXISTS stage_payload jsonb;

ALTER TABLE qmt_canary_order_candidates
    ALTER COLUMN stage_hash SET NOT NULL,
    ALTER COLUMN broker_order_remark SET NOT NULL,
    ALTER COLUMN staged_at SET NOT NULL,
    ALTER COLUMN stage_version SET NOT NULL,
    ALTER COLUMN stage_payload SET NOT NULL;

DO $create_qmt_canary_candidate_stage_checks$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'qmt_canary_candidate_stage_hash_format'
          AND conrelid = 'qmt_canary_order_candidates'::regclass
    ) THEN
        ALTER TABLE qmt_canary_order_candidates
            ADD CONSTRAINT qmt_canary_candidate_stage_hash_format CHECK (
                stage_hash ~ '^[0-9a-f]{64}$'
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'qmt_canary_candidate_remark_format'
          AND conrelid = 'qmt_canary_order_candidates'::regclass
    ) THEN
        ALTER TABLE qmt_canary_order_candidates
            ADD CONSTRAINT qmt_canary_candidate_remark_format CHECK (
                broker_order_remark ~ '^AQ[0-9a-f]{22}$'
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'qmt_canary_candidate_stage_time'
          AND conrelid = 'qmt_canary_order_candidates'::regclass
    ) THEN
        ALTER TABLE qmt_canary_order_candidates
            ADD CONSTRAINT qmt_canary_candidate_stage_time CHECK (
                staged_at >= created_at
                AND staged_at < valid_until
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'qmt_canary_candidate_stage_version'
          AND conrelid = 'qmt_canary_order_candidates'::regclass
    ) THEN
        ALTER TABLE qmt_canary_order_candidates
            ADD CONSTRAINT qmt_canary_candidate_stage_version CHECK (
                stage_version = 'qmt-canary-order-stage-v1'
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'qmt_canary_candidate_stage_locked'
          AND conrelid = 'qmt_canary_order_candidates'::regclass
    ) THEN
        ALTER TABLE qmt_canary_order_candidates
            ADD CONSTRAINT qmt_canary_candidate_stage_locked CHECK (
                stage_payload->>'broker_mutation_allowed' = 'false'
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'qmt_canary_candidate_stage_lock_key'
          AND conrelid = 'qmt_canary_order_candidates'::regclass
    ) THEN
        ALTER TABLE qmt_canary_order_candidates
            ADD CONSTRAINT qmt_canary_candidate_stage_lock_key CHECK (
                stage_payload ? 'broker_mutation_allowed'
            );
    END IF;
END;
$create_qmt_canary_candidate_stage_checks$;

CREATE UNIQUE INDEX IF NOT EXISTS
    qmt_canary_candidate_stage_hash_unique_idx
ON qmt_canary_order_candidates(stage_hash);

CREATE UNIQUE INDEX IF NOT EXISTS
    qmt_canary_candidate_remark_unique_idx
ON qmt_canary_order_candidates(broker_order_remark);

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 38)
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
