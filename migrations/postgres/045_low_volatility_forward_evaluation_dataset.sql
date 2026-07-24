ALTER TABLE low_volatility_forward_evaluation_runs
ADD COLUMN IF NOT EXISTS evaluation_dataset_manifest_hash text
    REFERENCES research_dataset_manifests(manifest_hash);

DO $require_forward_evaluation_dataset_binding$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM low_volatility_forward_evaluation_runs
        WHERE evaluation_dataset_manifest_hash IS NULL
    ) THEN
        RAISE EXCEPTION
            'forward evaluations without a dataset binding cannot be upgraded';
    END IF;
END;
$require_forward_evaluation_dataset_binding$;

ALTER TABLE low_volatility_forward_evaluation_runs
ALTER COLUMN evaluation_dataset_manifest_hash SET NOT NULL;

ALTER TABLE low_volatility_forward_evaluation_runs
DROP CONSTRAINT IF EXISTS
    low_volatility_forward_evaluation_dataset_hash_check;

ALTER TABLE low_volatility_forward_evaluation_runs
ADD CONSTRAINT
    low_volatility_forward_evaluation_dataset_hash_check
CHECK (
    evaluation_dataset_manifest_hash ~ '^[0-9a-f]{64}$'
);

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 45)
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
