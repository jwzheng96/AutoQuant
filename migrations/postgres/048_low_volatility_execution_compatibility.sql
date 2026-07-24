CREATE TABLE IF NOT EXISTS
    low_volatility_execution_compatibility_specs
(
    spec_hash text PRIMARY KEY CHECK (
        spec_hash ~ '^[0-9a-f]{64}$'
    ),
    source_spec_hash text NOT NULL REFERENCES
        low_volatility_research_specs(spec_hash),
    forward_spec_hash text NOT NULL UNIQUE REFERENCES
        low_volatility_forward_evidence_specs(spec_hash),
    observed_forward_session_count integer NOT NULL CHECK (
        observed_forward_session_count BETWEEN 0 AND 125
    ),
    partial_outcome_observed_before_freeze boolean NOT NULL,
    terminal_outcome_observed_before_freeze boolean NOT NULL
        DEFAULT false CHECK (
            NOT terminal_outcome_observed_before_freeze
        ),
    order_intent_invariance_required boolean NOT NULL CHECK (
        order_intent_invariance_required
    ),
    same_forward_window_required boolean NOT NULL CHECK (
        same_forward_window_required
    ),
    compatibility_can_only_disqualify boolean NOT NULL CHECK (
        compatibility_can_only_disqualify
    ),
    historical_reclassification_allowed boolean NOT NULL
        DEFAULT false CHECK (
            NOT historical_reclassification_allowed
        ),
    paper_activation_allowed boolean NOT NULL DEFAULT false CHECK (
        NOT paper_activation_allowed
    ),
    live_trading_locked boolean NOT NULL DEFAULT true CHECK (
        live_trading_locked
    ),
    decision_order_policy_version text NOT NULL CHECK (
        decision_order_policy_version =
            'low-volatility-prior-close-order-intents-v1'
    ),
    research_execution_version text NOT NULL CHECK (
        research_execution_version =
            'daily-open-conservative-v1'
    ),
    frozen_by text NOT NULL CHECK (
        length(btrim(frozen_by)) BETWEEN 1 AND 128
        AND frozen_by = btrim(frozen_by)
    ),
    frozen_at timestamptz NOT NULL,
    compatibility_version text NOT NULL CHECK (
        compatibility_version =
            'low-volatility-execution-compatibility-spec-v1'
    ),
    payload jsonb NOT NULL CHECK (
        jsonb_typeof(payload) = 'object'
        AND payload->>'paper_activation_allowed' = 'false'
        AND payload->>'live_trading_locked' = 'true'
        AND payload->>'compatibility_can_only_disqualify' =
            'true'
    ),
    CHECK (
        partial_outcome_observed_before_freeze =
            (observed_forward_session_count > 0)
    )
);

CREATE OR REPLACE FUNCTION
    autoquant_validate_low_volatility_execution_compatibility()
RETURNS trigger
LANGUAGE plpgsql
AS $validate_low_volatility_execution_compatibility$
DECLARE
    forward_spec low_volatility_forward_evidence_specs%ROWTYPE;
    frozen_session_count integer;
BEGIN
    SELECT *
    INTO forward_spec
    FROM low_volatility_forward_evidence_specs
    WHERE spec_hash = NEW.forward_spec_hash;

    SELECT count(*)
    INTO frozen_session_count
    FROM low_volatility_forward_sessions
    WHERE forward_spec_hash = NEW.forward_spec_hash;

    IF forward_spec.spec_hash IS NULL
       OR forward_spec.source_spec_hash <>
            NEW.source_spec_hash
       OR frozen_session_count <>
            NEW.observed_forward_session_count
       OR frozen_session_count >= 126
       OR EXISTS (
            SELECT 1
            FROM low_volatility_forward_evaluation_runs
            WHERE forward_spec_hash = NEW.forward_spec_hash
       ) THEN
        RAISE EXCEPTION
            'low-volatility execution compatibility was frozen too late';
    END IF;
    RETURN NEW;
END;
$validate_low_volatility_execution_compatibility$;

DO $create_low_volatility_execution_compatibility_gate$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_execution_compatibility_gate'
          AND tgrelid =
            'low_volatility_execution_compatibility_specs'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_execution_compatibility_gate
        BEFORE INSERT
            ON low_volatility_execution_compatibility_specs
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_validate_low_volatility_execution_compatibility();
    END IF;
END;
$create_low_volatility_execution_compatibility_gate$;

DO $create_low_volatility_execution_compatibility_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_execution_compatibility_specs_immutable'
          AND tgrelid =
            'low_volatility_execution_compatibility_specs'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_execution_compatibility_specs_immutable
        BEFORE UPDATE OR DELETE
            ON low_volatility_execution_compatibility_specs
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_low_volatility_execution_compatibility_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 48)
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
