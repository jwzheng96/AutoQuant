CREATE TABLE IF NOT EXISTS paper_compliance_approvals
(
    approval_hash text PRIMARY KEY CHECK (
        approval_hash ~ '^[0-9a-f]{64}$'
    ),
    account_id text NOT NULL CHECK (
        length(btrim(account_id)) BETWEEN 1 AND 128
        AND account_id = btrim(account_id)
    ),
    strategy_id text NOT NULL CHECK (
        length(btrim(strategy_id)) BETWEEN 1 AND 128
        AND strategy_id = btrim(strategy_id)
    ),
    registration_hash text NOT NULL CHECK (
        registration_hash ~ '^[0-9a-f]{64}$'
    ),
    policy_hash text NOT NULL CHECK (
        policy_hash ~ '^[0-9a-f]{64}$'
    ),
    external_artifact_hash text NOT NULL CHECK (
        external_artifact_hash ~ '^[0-9a-f]{64}$'
    ),
    approval_reference text NOT NULL CHECK (
        approval_reference ~
            '^[A-Za-z0-9][A-Za-z0-9._:/-]{7,127}$'
    ),
    approved_by text NOT NULL CHECK (
        length(btrim(approved_by)) BETWEEN 1 AND 128
        AND approved_by = btrim(approved_by)
    ),
    approved_at timestamptz NOT NULL,
    valid_until timestamptz NOT NULL,
    approval_version text NOT NULL CHECK (
        approval_version = 'paper-compliance-approval-v1'
    ),
    live_trading_locked boolean NOT NULL DEFAULT true CHECK (
        live_trading_locked
    ),
    payload jsonb NOT NULL,
    CHECK (
        valid_until > approved_at
        AND valid_until <= approved_at + interval '31 days'
    ),
    UNIQUE (
        account_id, strategy_id, registration_hash,
        policy_hash, external_artifact_hash
    ),
    UNIQUE (account_id, strategy_id, approval_reference)
);

CREATE TABLE IF NOT EXISTS paper_compliance_revocations
(
    revocation_hash text PRIMARY KEY CHECK (
        revocation_hash ~ '^[0-9a-f]{64}$'
    ),
    approval_hash text NOT NULL UNIQUE
        REFERENCES paper_compliance_approvals(approval_hash),
    revoked_by text NOT NULL CHECK (
        length(btrim(revoked_by)) BETWEEN 1 AND 128
        AND revoked_by = btrim(revoked_by)
    ),
    revoked_at timestamptz NOT NULL,
    reason text NOT NULL CHECK (
        reason IN (
            'scope_changed',
            'risk_changed',
            'external_approval_withdrawn',
            'operator_safety_action'
        )
    ),
    revocation_version text NOT NULL CHECK (
        revocation_version = 'paper-compliance-revocation-v1'
    ),
    payload jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS paper_compliance_approval_scope_idx
ON paper_compliance_approvals
    (account_id, strategy_id, registration_hash,
     policy_hash, approved_at DESC);

DO $create_paper_compliance_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'paper_compliance_approvals_immutable'
          AND tgrelid = 'paper_compliance_approvals'::regclass
    ) THEN
        CREATE TRIGGER paper_compliance_approvals_immutable
        BEFORE UPDATE OR DELETE ON paper_compliance_approvals
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'paper_compliance_revocations_immutable'
          AND tgrelid = 'paper_compliance_revocations'::regclass
    ) THEN
        CREATE TRIGGER paper_compliance_revocations_immutable
        BEFORE UPDATE OR DELETE ON paper_compliance_revocations
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_paper_compliance_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 36)
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
