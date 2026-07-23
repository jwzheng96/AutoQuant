ALTER TABLE dynamic_research_specs
DROP CONSTRAINT IF EXISTS dynamic_research_specs_strategy_id_check;

ALTER TABLE dynamic_research_specs
DROP CONSTRAINT IF EXISTS
    dynamic_research_specs_specification_version_check;

ALTER TABLE dynamic_research_specs
ADD CONSTRAINT dynamic_research_specs_strategy_version_check
CHECK (
    (
        strategy_id =
            'dynamic-universe-cross-sectional-momentum-v1'
        AND specification_version =
            'dynamic-portfolio-research-spec-v1'
    )
    OR
    (
        strategy_id =
            'dynamic-universe-regime-filtered-momentum-v2'
        AND specification_version =
            'dynamic-regime-portfolio-research-spec-v2'
    )
);

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 27)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
