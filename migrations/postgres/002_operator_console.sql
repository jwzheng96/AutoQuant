CREATE TABLE IF NOT EXISTS operator_jobs
(
    job_id uuid PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    job_type text NOT NULL CHECK (job_type IN ('daily_ingestion')),
    state text NOT NULL CHECK (
        state IN ('queued', 'running', 'completed', 'failed', 'interrupted')
    ),
    request_payload jsonb NOT NULL,
    requested_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz,
    completed_at timestamptz,
    result_payload jsonb,
    error_code text,
    CHECK (
        (state = 'queued' AND started_at IS NULL AND completed_at IS NULL)
        OR (state = 'running' AND started_at IS NOT NULL AND completed_at IS NULL)
        OR (state IN ('completed', 'failed', 'interrupted') AND completed_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS operator_jobs_created_at_idx
ON operator_jobs (created_at DESC);

CREATE INDEX IF NOT EXISTS operator_jobs_state_created_at_idx
ON operator_jobs (state, created_at);

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 2)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
