CREATE TABLE IF NOT EXISTS research_data_campaigns
(
    campaign_hash text PRIMARY KEY CHECK (
        campaign_hash ~ '^[0-9a-f]{64}$'
    ),
    campaign_key text NOT NULL UNIQUE CHECK (
        campaign_key ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]{15,127}$'
    ),
    policy_hash text NOT NULL CHECK (
        policy_hash ~ '^[0-9a-f]{64}$'
    ),
    start_date date NOT NULL,
    end_date date NOT NULL,
    snapshot_count integer NOT NULL CHECK (
        snapshot_count BETWEEN 1 AND 200
    ),
    instrument_count integer NOT NULL CHECK (
        instrument_count BETWEEN 1 AND 1000
    ),
    requested_by text NOT NULL CHECK (
        length(requested_by) BETWEEN 1 AND 128
    ),
    created_at timestamptz NOT NULL,
    specification_payload jsonb NOT NULL,
    CHECK (start_date <= end_date)
);

CREATE TABLE IF NOT EXISTS research_data_campaign_items
(
    campaign_hash text NOT NULL
        REFERENCES research_data_campaigns(campaign_hash),
    sequence integer NOT NULL CHECK (sequence > 0),
    instrument text NOT NULL CHECK (
        instrument ~ '^[0-9]{6}\.(XSHG|XSHE)$'
    ),
    state text NOT NULL CHECK (
        state IN ('queued', 'running', 'completed', 'failed')
    ),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts integer NOT NULL CHECK (
        max_attempts BETWEEN 1 AND 10
    ),
    manifest_hash text NULL
        REFERENCES dataset_manifests(manifest_hash),
    started_at timestamptz NULL,
    completed_at timestamptz NULL,
    error_code text NULL CHECK (
        error_code IS NULL OR length(error_code) BETWEEN 1 AND 80
    ),
    PRIMARY KEY (campaign_hash, sequence),
    UNIQUE (campaign_hash, instrument),
    CHECK (
        (state = 'queued' AND manifest_hash IS NULL AND completed_at IS NULL)
        OR
        (state = 'running' AND manifest_hash IS NULL
         AND started_at IS NOT NULL AND completed_at IS NULL)
        OR
        (state = 'completed' AND manifest_hash IS NOT NULL
         AND started_at IS NOT NULL AND completed_at IS NOT NULL
         AND error_code IS NULL)
        OR
        (state = 'failed' AND manifest_hash IS NULL
         AND started_at IS NOT NULL AND completed_at IS NOT NULL
         AND error_code IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS research_data_campaign_items_queue_idx
ON research_data_campaign_items
    (state, campaign_hash, sequence);

CREATE TABLE IF NOT EXISTS research_dataset_manifests
(
    manifest_hash text PRIMARY KEY CHECK (
        manifest_hash ~ '^[0-9a-f]{64}$'
    ),
    campaign_hash text NOT NULL UNIQUE
        REFERENCES research_data_campaigns(campaign_hash),
    policy_hash text NOT NULL CHECK (
        policy_hash ~ '^[0-9a-f]{64}$'
    ),
    start_date date NOT NULL,
    end_date date NOT NULL,
    snapshot_count integer NOT NULL CHECK (
        snapshot_count BETWEEN 1 AND 200
    ),
    instrument_count integer NOT NULL CHECK (
        instrument_count BETWEEN 1 AND 1000
    ),
    shard_count integer NOT NULL CHECK (
        shard_count BETWEEN 1 AND 1000
    ),
    created_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    CHECK (
        start_date <= end_date
        AND instrument_count = shard_count
    )
);

CREATE TABLE IF NOT EXISTS research_dataset_manifest_shards
(
    manifest_hash text NOT NULL
        REFERENCES research_dataset_manifests(manifest_hash),
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

DO $create_research_data_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'research_data_campaigns_immutable'
          AND tgrelid = 'research_data_campaigns'::regclass
    ) THEN
        CREATE TRIGGER research_data_campaigns_immutable
        BEFORE UPDATE OR DELETE ON research_data_campaigns
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'research_dataset_manifests_immutable'
          AND tgrelid = 'research_dataset_manifests'::regclass
    ) THEN
        CREATE TRIGGER research_dataset_manifests_immutable
        BEFORE UPDATE OR DELETE ON research_dataset_manifests
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'research_dataset_manifest_shards_immutable'
          AND tgrelid = 'research_dataset_manifest_shards'::regclass
    ) THEN
        CREATE TRIGGER research_dataset_manifest_shards_immutable
        BEFORE UPDATE OR DELETE ON research_dataset_manifest_shards
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_research_data_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 24)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
