CREATE TABLE IF NOT EXISTS schema_versions
(
    component text PRIMARY KEY,
    version integer NOT NULL CHECK (version > 0),
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 1)
ON CONFLICT (component) DO NOTHING;

CREATE TABLE IF NOT EXISTS ingestion_checkpoints
(
    source text NOT NULL,
    stream text NOT NULL,
    instrument text NOT NULL,
    event_time timestamptz NOT NULL,
    content_hash text NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source, stream, instrument)
);

CREATE TABLE IF NOT EXISTS source_evidence
(
    evidence_hash text PRIMARY KEY CHECK (evidence_hash ~ '^[0-9a-f]{64}$'),
    source text NOT NULL,
    method text NOT NULL,
    requested_at timestamptz NOT NULL,
    response_body bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS quality_reports
(
    report_hash text PRIMARY KEY CHECK (report_hash ~ '^[0-9a-f]{64}$'),
    passed boolean NOT NULL,
    production_complete boolean NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS dataset_manifests
(
    manifest_hash text PRIMARY KEY CHECK (manifest_hash ~ '^[0-9a-f]{64}$'),
    source text NOT NULL,
    start_time timestamptz NOT NULL,
    end_time timestamptz NOT NULL,
    as_of timestamptz NOT NULL,
    quality_report_hash text NOT NULL REFERENCES quality_reports(report_hash),
    row_count bigint NOT NULL CHECK (row_count >= 0),
    production_complete boolean NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS audit_events
(
    sequence bigserial PRIMARY KEY,
    event_type text NOT NULL,
    occurred_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    previous_hash text NOT NULL CHECK (previous_hash ~ '^[0-9a-f]{64}$'),
    event_hash text NOT NULL UNIQUE CHECK (event_hash ~ '^[0-9a-f]{64}$')
);

COMMENT ON TABLE audit_events IS 'AutoQuant audit log identity: autoquant.audit_events';

CREATE OR REPLACE FUNCTION autoquant_validate_checkpoint()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.event_time < OLD.event_time THEN
        RAISE EXCEPTION 'checkpoint cannot move backward';
    END IF;
    IF NEW.event_time = OLD.event_time AND NEW.content_hash <> OLD.content_hash THEN
        RAISE EXCEPTION 'checkpoint same timestamp has different content hash';
    END IF;
    RETURN NEW;
END;
$$;

DO $create_checkpoint_trigger$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'ingestion_checkpoints_monotonic'
          AND tgrelid = 'ingestion_checkpoints'::regclass
    ) THEN
        CREATE TRIGGER ingestion_checkpoints_monotonic
        BEFORE UPDATE ON ingestion_checkpoints
        FOR EACH ROW EXECUTE FUNCTION autoquant_validate_checkpoint();
    END IF;
END;
$create_checkpoint_trigger$;

CREATE OR REPLACE FUNCTION autoquant_validate_manifest()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    quality_passed boolean;
    quality_complete boolean;
BEGIN
    IF NEW.production_complete THEN
        SELECT passed, production_complete
        INTO quality_passed, quality_complete
        FROM quality_reports
        WHERE report_hash = NEW.quality_report_hash;
        IF quality_passed IS DISTINCT FROM TRUE OR quality_complete IS DISTINCT FROM TRUE THEN
            RAISE EXCEPTION 'production manifest requires passing complete quality report';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DO $create_manifest_trigger$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'dataset_manifests_validate_quality'
          AND tgrelid = 'dataset_manifests'::regclass
    ) THEN
        CREATE TRIGGER dataset_manifests_validate_quality
        BEFORE INSERT OR UPDATE OF quality_report_hash, production_complete, payload
        ON dataset_manifests
        FOR EACH ROW EXECUTE FUNCTION autoquant_validate_manifest();
    END IF;
END;
$create_manifest_trigger$;

CREATE OR REPLACE FUNCTION autoquant_reject_immutable_change()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'append-only record cannot be changed';
END;
$$;

DO $create_immutable_triggers$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'source_evidence_immutable'
          AND tgrelid = 'source_evidence'::regclass
    ) THEN
        CREATE TRIGGER source_evidence_immutable
        BEFORE UPDATE OR DELETE ON source_evidence
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'quality_reports_immutable'
          AND tgrelid = 'quality_reports'::regclass
    ) THEN
        CREATE TRIGGER quality_reports_immutable
        BEFORE UPDATE OR DELETE ON quality_reports
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'dataset_manifests_immutable'
          AND tgrelid = 'dataset_manifests'::regclass
    ) THEN
        CREATE TRIGGER dataset_manifests_immutable
        BEFORE UPDATE OR DELETE ON dataset_manifests
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'audit_events_immutable'
          AND tgrelid = 'audit_events'::regclass
    ) THEN
        CREATE TRIGGER audit_events_immutable
        BEFORE UPDATE OR DELETE ON audit_events
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_immutable_triggers$;
