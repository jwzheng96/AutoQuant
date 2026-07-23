CREATE TABLE IF NOT EXISTS validation_campaigns
(
    campaign_hash text PRIMARY KEY CHECK (campaign_hash ~ '^[0-9a-f]{64}$'),
    campaign_key text NOT NULL UNIQUE CHECK (
        campaign_key ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]{15,127}$'
    ),
    manifest_hash text NOT NULL REFERENCES dataset_manifests(manifest_hash),
    component_count integer NOT NULL CHECK (component_count BETWEEN 3 AND 20),
    requested_by text NOT NULL CHECK (char_length(requested_by) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL,
    specification_payload jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS validation_campaign_components
(
    campaign_hash text NOT NULL
        REFERENCES validation_campaigns(campaign_hash),
    sequence integer NOT NULL CHECK (sequence > 0),
    instrument text NOT NULL CHECK (instrument ~ '^[0-9]{6}\.(XSHG|XSHE)$'),
    experiment_id uuid NOT NULL UNIQUE
        REFERENCES validation_experiments(experiment_id),
    request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    request_payload jsonb NOT NULL,
    PRIMARY KEY (campaign_hash, sequence),
    UNIQUE (campaign_hash, instrument)
);

DO $create_validation_campaign_immutable$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'validation_campaigns',
        'validation_campaign_components'
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
$create_validation_campaign_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 21)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
