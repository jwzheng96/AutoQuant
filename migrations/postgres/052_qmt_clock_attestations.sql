ALTER TABLE qmt_readonly_acceptance_evidence
    ADD COLUMN IF NOT EXISTS clock_attestation_hash text
        CHECK (
            clock_attestation_hash IS NULL
            OR clock_attestation_hash ~ '^[0-9a-f]{64}$'
        ),
    ADD COLUMN IF NOT EXISTS clock_attestation_payload jsonb;

DO $add_qmt_acceptance_clock_binding$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'qmt_acceptance_clock_binding'
          AND conrelid =
              'qmt_readonly_acceptance_evidence'::regclass
    ) THEN
        ALTER TABLE qmt_readonly_acceptance_evidence
            ADD CONSTRAINT qmt_acceptance_clock_binding
            CHECK (
                (
                    clock_attestation_hash IS NULL
                    AND clock_attestation_payload IS NULL
                    AND COALESCE(
                        evidence_payload->>'version',
                        ''
                    ) = 'qmt-readonly-acceptance-v1'
                    AND NOT evidence_payload ? 'clock_attestation'
                    AND NOT evidence_payload ? 'clock_attestation_hash'
                )
                OR
                (
                    clock_attestation_hash IS NOT NULL
                    AND clock_attestation_payload IS NOT NULL
                    AND COALESCE(
                        evidence_payload->>'version',
                        ''
                    ) = 'qmt-readonly-acceptance-v2'
                    AND evidence_payload->>'clock_attestation_hash'
                        IS NOT DISTINCT FROM clock_attestation_hash
                    AND evidence_payload->'clock_attestation'
                        IS NOT DISTINCT FROM clock_attestation_payload
                    AND COALESCE(
                        clock_attestation_payload->>'version',
                        ''
                    ) = 'qmt-clock-attestation-v1'
                    AND COALESCE(
                        clock_attestation_payload->>'trusted',
                        ''
                    ) = 'true'
                )
            );
    END IF;
END;
$add_qmt_acceptance_clock_binding$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 52)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version
        THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
