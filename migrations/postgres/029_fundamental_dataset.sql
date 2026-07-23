CREATE TABLE IF NOT EXISTS fundamental_dataset_manifests
(
    manifest_hash text PRIMARY KEY CHECK (
        manifest_hash ~ '^[0-9a-f]{64}$'
    ),
    spec_hash text NOT NULL UNIQUE
        REFERENCES fundamental_research_specs(spec_hash),
    start_date date NOT NULL,
    end_date date NOT NULL,
    instrument_count integer NOT NULL CHECK (
        instrument_count BETWEEN 1 AND 1000
    ),
    shard_count integer NOT NULL CHECK (
        shard_count = instrument_count
    ),
    created_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    CHECK (start_date <= end_date)
);

CREATE TABLE IF NOT EXISTS fundamental_dataset_manifest_shards
(
    manifest_hash text NOT NULL
        REFERENCES fundamental_dataset_manifests(manifest_hash),
    sequence integer NOT NULL CHECK (sequence > 0),
    instrument text NOT NULL CHECK (
        instrument ~ '^[0-9]{6}\.(XSHG|XSHE)$'
    ),
    shard_manifest_hash text NOT NULL
        REFERENCES dataset_manifests(manifest_hash),
    PRIMARY KEY (manifest_hash, sequence),
    UNIQUE (manifest_hash, instrument),
    UNIQUE (manifest_hash, shard_manifest_hash)
);

DO $create_fundamental_dataset_manifests_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'fundamental_dataset_manifests_immutable'
          AND tgrelid =
              'fundamental_dataset_manifests'::regclass
    ) THEN
        CREATE TRIGGER fundamental_dataset_manifests_immutable
        BEFORE UPDATE OR DELETE ON fundamental_dataset_manifests
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_fundamental_dataset_manifests_immutable$;

DO $create_fundamental_dataset_manifest_shards_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'fundamental_dataset_manifest_shards_immutable'
          AND tgrelid =
              'fundamental_dataset_manifest_shards'::regclass
    ) THEN
        CREATE TRIGGER
            fundamental_dataset_manifest_shards_immutable
        BEFORE UPDATE OR DELETE
        ON fundamental_dataset_manifest_shards
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_fundamental_dataset_manifest_shards_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 29)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
