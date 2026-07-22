CREATE OR REPLACE FUNCTION autoquant_validate_checkpoint()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.event_time < OLD.event_time THEN
        RAISE EXCEPTION 'checkpoint cannot move backward';
    END IF;
    RETURN NEW;
END;
$$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 3)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
