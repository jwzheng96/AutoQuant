ALTER TABLE validation_folds
ADD COLUMN IF NOT EXISTS benchmark_payload jsonb;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 6)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
