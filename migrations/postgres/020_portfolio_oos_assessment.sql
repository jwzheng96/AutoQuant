ALTER TABLE paper_portfolio_registrations
ADD COLUMN IF NOT EXISTS oos_assessment_hash text;

ALTER TABLE paper_portfolio_registrations
ADD COLUMN IF NOT EXISTS oos_assessment_payload jsonb;

ALTER TABLE paper_portfolio_registrations
DROP CONSTRAINT IF EXISTS paper_portfolio_registrations_portfolio_version_check;

ALTER TABLE paper_portfolio_registrations
DROP CONSTRAINT IF EXISTS paper_portfolio_oos_assessment_required;

ALTER TABLE paper_portfolio_registrations
ADD CONSTRAINT paper_portfolio_oos_assessment_required
CHECK (
    portfolio_version = 'validated-sma-portfolio-v2'
    AND oos_assessment_hash IS NOT NULL
    AND oos_assessment_hash ~ '^[0-9a-f]{64}$'
    AND oos_assessment_payload IS NOT NULL
) NOT VALID;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 20)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
