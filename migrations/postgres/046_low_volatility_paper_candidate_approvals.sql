CREATE TABLE IF NOT EXISTS low_volatility_paper_candidate_approvals
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
    forward_spec_hash text NOT NULL REFERENCES
        low_volatility_forward_evidence_specs(spec_hash),
    evaluation_result_hash text NOT NULL UNIQUE REFERENCES
        low_volatility_forward_evaluation_runs(result_hash),
    evaluation_assessment_hash text NOT NULL CHECK (
        evaluation_assessment_hash ~ '^[0-9a-f]{64}$'
    ),
    evaluation_dataset_manifest_hash text NOT NULL REFERENCES
        research_dataset_manifests(manifest_hash),
    source_spec_hash text NOT NULL REFERENCES
        low_volatility_research_specs(spec_hash),
    risk_policy_hash text NOT NULL CHECK (
        risk_policy_hash ~ '^[0-9a-f]{64}$'
    ),
    instrument_count integer NOT NULL CHECK (
        instrument_count BETWEEN 1 AND 1000
    ),
    evidence_status text NOT NULL CHECK (
        evidence_status = 'paper_candidate'
    ),
    minimum_paper_sessions integer NOT NULL CHECK (
        minimum_paper_sessions = 60
    ),
    execution_mode text NOT NULL CHECK (
        execution_mode = 'paper'
    ),
    daily_signal_evidence_required boolean NOT NULL CHECK (
        daily_signal_evidence_required
    ),
    runtime_activation_allowed boolean NOT NULL DEFAULT false CHECK (
        NOT runtime_activation_allowed
    ),
    live_trading_locked boolean NOT NULL DEFAULT true CHECK (
        live_trading_locked
    ),
    approved_by text NOT NULL CHECK (
        length(btrim(approved_by)) BETWEEN 1 AND 128
        AND approved_by = btrim(approved_by)
    ),
    approved_at timestamptz NOT NULL,
    approval_version text NOT NULL CHECK (
        approval_version =
            'low-volatility-paper-candidate-approval-v1'
    ),
    payload jsonb NOT NULL CHECK (
        jsonb_typeof(payload) = 'object'
        AND payload->>'runtime_activation_allowed' = 'false'
        AND payload->>'live_trading_locked' = 'true'
        AND payload->>'daily_signal_evidence_required' = 'true'
    ),
    UNIQUE (
        account_id, strategy_id, approval_hash
    )
);

CREATE TABLE IF NOT EXISTS low_volatility_paper_candidate_revocations
(
    revocation_hash text PRIMARY KEY CHECK (
        revocation_hash ~ '^[0-9a-f]{64}$'
    ),
    approval_hash text NOT NULL UNIQUE REFERENCES
        low_volatility_paper_candidate_approvals(approval_hash),
    revoked_by text NOT NULL CHECK (
        length(btrim(revoked_by)) BETWEEN 1 AND 128
        AND revoked_by = btrim(revoked_by)
    ),
    revoked_at timestamptz NOT NULL,
    reason text NOT NULL CHECK (
        reason IN (
            'evidence_invalidated',
            'risk_changed',
            'runtime_design_changed',
            'operator_safety_action'
        )
    ),
    live_trading_locked boolean NOT NULL DEFAULT true CHECK (
        live_trading_locked
    ),
    revocation_version text NOT NULL CHECK (
        revocation_version =
            'low-volatility-paper-candidate-revocation-v1'
    ),
    payload jsonb NOT NULL CHECK (
        jsonb_typeof(payload) = 'object'
        AND payload->>'live_trading_locked' = 'true'
    )
);

CREATE OR REPLACE FUNCTION
    autoquant_validate_low_volatility_paper_candidate()
RETURNS trigger
LANGUAGE plpgsql
AS $validate_low_volatility_paper_candidate$
DECLARE
    evidence low_volatility_forward_evaluation_runs%ROWTYPE;
BEGIN
    SELECT *
    INTO evidence
    FROM low_volatility_forward_evaluation_runs
    WHERE result_hash = NEW.evaluation_result_hash;

    IF NOT FOUND
       OR evidence.forward_spec_hash <> NEW.forward_spec_hash
       OR evidence.assessment_hash <>
            NEW.evaluation_assessment_hash
       OR evidence.evaluation_dataset_manifest_hash <>
            NEW.evaluation_dataset_manifest_hash
       OR evidence.source_spec_hash <> NEW.source_spec_hash
       OR evidence.evidence_status <> 'paper_candidate'
       OR NOT evidence.paper_trading_eligible
       OR evidence.paper_deployment_allowed
       OR NOT evidence.live_trading_locked
       OR NEW.approved_at < evidence.completed_at THEN
        RAISE EXCEPTION
            'low-volatility paper candidate evidence is invalid';
    END IF;
    RETURN NEW;
END;
$validate_low_volatility_paper_candidate$;

DO $create_low_volatility_paper_candidate_evidence_gate$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_paper_candidate_evidence_gate'
          AND tgrelid =
            'low_volatility_paper_candidate_approvals'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_paper_candidate_evidence_gate
        BEFORE INSERT
            ON low_volatility_paper_candidate_approvals
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_validate_low_volatility_paper_candidate();
    END IF;
END;
$create_low_volatility_paper_candidate_evidence_gate$;

CREATE INDEX IF NOT EXISTS
    low_volatility_paper_candidate_scope_idx
ON low_volatility_paper_candidate_approvals
    (account_id, strategy_id, approved_at DESC);

DO $create_low_volatility_paper_candidate_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_paper_candidate_approvals_immutable'
          AND tgrelid =
            'low_volatility_paper_candidate_approvals'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_paper_candidate_approvals_immutable
        BEFORE UPDATE OR DELETE
            ON low_volatility_paper_candidate_approvals
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_paper_candidate_revocations_immutable'
          AND tgrelid =
            'low_volatility_paper_candidate_revocations'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_paper_candidate_revocations_immutable
        BEFORE UPDATE OR DELETE
            ON low_volatility_paper_candidate_revocations
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_low_volatility_paper_candidate_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 46)
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
