CREATE TABLE IF NOT EXISTS
    low_volatility_paper_deployment_contracts
(
    contract_hash text PRIMARY KEY CHECK (
        contract_hash ~ '^[0-9a-f]{64}$'
    ),
    source_spec_hash text NOT NULL REFERENCES
        low_volatility_research_specs(spec_hash),
    forward_spec_hash text NOT NULL UNIQUE REFERENCES
        low_volatility_forward_evidence_specs(spec_hash),
    compatibility_spec_hash text NOT NULL UNIQUE REFERENCES
        low_volatility_execution_compatibility_specs(spec_hash),
    observed_forward_session_count integer NOT NULL CHECK (
        observed_forward_session_count BETWEEN 0 AND 125
    ),
    partial_outcome_observed_before_freeze boolean NOT NULL,
    terminal_outcome_observed_before_freeze boolean NOT NULL
        DEFAULT false CHECK (
            NOT terminal_outcome_observed_before_freeze
        ),
    minimum_forward_sessions integer NOT NULL CHECK (
        minimum_forward_sessions = 126
    ),
    minimum_paper_sessions integer NOT NULL CHECK (
        minimum_paper_sessions = 60
    ),
    required_forward_evidence_status text NOT NULL CHECK (
        required_forward_evidence_status = 'paper_candidate'
    ),
    required_compatibility_status text NOT NULL CHECK (
        required_compatibility_status = 'compatible'
    ),
    required_order_policy_version text NOT NULL CHECK (
        required_order_policy_version =
            'low-volatility-prior-close-order-intents-v1'
    ),
    required_daily_signal_policy_version text NOT NULL CHECK (
        required_daily_signal_policy_version =
            'low-volatility-decision-time-paper-signal-v2'
    ),
    candidate_approval_after_compatibility_required
        boolean NOT NULL CHECK (
            candidate_approval_after_compatibility_required
        ),
    exact_session_signal_required boolean NOT NULL CHECK (
        exact_session_signal_required
    ),
    decision_time_signal_required boolean NOT NULL CHECK (
        decision_time_signal_required
    ),
    preopen_signal_required boolean NOT NULL CHECK (
        preopen_signal_required
    ),
    point_in_time_universe_required boolean NOT NULL CHECK (
        point_in_time_universe_required
    ),
    held_position_valuation_coverage_required
        boolean NOT NULL CHECK (
            held_position_valuation_coverage_required
        ),
    exact_risk_policy_required boolean NOT NULL CHECK (
        exact_risk_policy_required
    ),
    kill_switch_active_at_authorization_required
        boolean NOT NULL CHECK (
            kill_switch_active_at_authorization_required
        ),
    exclusive_paper_deployment_required boolean NOT NULL CHECK (
        exclusive_paper_deployment_required
    ),
    fresh_runtime_unlock_evidence_required boolean NOT NULL CHECK (
        fresh_runtime_unlock_evidence_required
    ),
    runtime_authorization_separate boolean NOT NULL CHECK (
        runtime_authorization_separate
    ),
    historical_reclassification_allowed boolean NOT NULL
        DEFAULT false CHECK (
            NOT historical_reclassification_allowed
        ),
    paper_activation_authority_granted boolean NOT NULL
        DEFAULT false CHECK (
            NOT paper_activation_authority_granted
        ),
    runtime_activation_allowed boolean NOT NULL
        DEFAULT false CHECK (
            NOT runtime_activation_allowed
        ),
    live_trading_locked boolean NOT NULL DEFAULT true CHECK (
        live_trading_locked
    ),
    frozen_by text NOT NULL CHECK (
        length(btrim(frozen_by)) BETWEEN 1 AND 128
        AND frozen_by = btrim(frozen_by)
    ),
    frozen_at timestamptz NOT NULL,
    contract_version text NOT NULL CHECK (
        contract_version =
            'low-volatility-paper-deployment-contract-v1'
    ),
    payload jsonb NOT NULL CHECK (
        jsonb_typeof(payload) = 'object'
        AND payload->>'paper_activation_authority_granted' =
            'false'
        AND payload->>'runtime_activation_allowed' = 'false'
        AND payload->>'live_trading_locked' = 'true'
        AND payload->>'runtime_authorization_separate' = 'true'
    ),
    CHECK (
        partial_outcome_observed_before_freeze =
            (observed_forward_session_count > 0)
    )
);

CREATE OR REPLACE FUNCTION
    autoquant_validate_low_volatility_paper_deployment_contract()
RETURNS trigger
LANGUAGE plpgsql
AS $validate_low_volatility_paper_deployment_contract$
DECLARE
    forward_spec low_volatility_forward_evidence_specs%ROWTYPE;
    compatibility
        low_volatility_execution_compatibility_specs%ROWTYPE;
    frozen_session_count integer;
BEGIN
    SELECT *
    INTO forward_spec
    FROM low_volatility_forward_evidence_specs
    WHERE spec_hash = NEW.forward_spec_hash;

    SELECT *
    INTO compatibility
    FROM low_volatility_execution_compatibility_specs
    WHERE spec_hash = NEW.compatibility_spec_hash;

    SELECT count(*)
    INTO frozen_session_count
    FROM low_volatility_forward_sessions
    WHERE forward_spec_hash = NEW.forward_spec_hash;

    IF forward_spec.spec_hash IS NULL
       OR compatibility.spec_hash IS NULL
       OR forward_spec.source_spec_hash <>
            NEW.source_spec_hash
       OR compatibility.forward_spec_hash <>
            NEW.forward_spec_hash
       OR compatibility.source_spec_hash <>
            NEW.source_spec_hash
       OR frozen_session_count <>
            NEW.observed_forward_session_count
       OR frozen_session_count >= 126
       OR EXISTS (
            SELECT 1
            FROM low_volatility_forward_evaluation_runs
            WHERE forward_spec_hash = NEW.forward_spec_hash
       )
       OR EXISTS (
            SELECT 1
            FROM low_volatility_execution_compatibility_runs
            WHERE compatibility_spec_hash =
                NEW.compatibility_spec_hash
       )
       OR EXISTS (
            SELECT 1
            FROM low_volatility_paper_candidate_approvals
            WHERE forward_spec_hash = NEW.forward_spec_hash
       ) THEN
        RAISE EXCEPTION
            'paper deployment contract must precede terminal evidence';
    END IF;
    RETURN NEW;
END;
$validate_low_volatility_paper_deployment_contract$;

DO $create_low_volatility_paper_deployment_contract_gate$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_paper_deployment_contract_gate'
          AND tgrelid =
            'low_volatility_paper_deployment_contracts'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_paper_deployment_contract_gate
        BEFORE INSERT
            ON low_volatility_paper_deployment_contracts
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_validate_low_volatility_paper_deployment_contract();
    END IF;
END;
$create_low_volatility_paper_deployment_contract_gate$;

DO $create_low_volatility_paper_deployment_contract_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_paper_deployment_contracts_immutable'
          AND tgrelid =
            'low_volatility_paper_deployment_contracts'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_paper_deployment_contracts_immutable
        BEFORE UPDATE OR DELETE
            ON low_volatility_paper_deployment_contracts
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_low_volatility_paper_deployment_contract_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 50)
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
