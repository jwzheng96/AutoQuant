CREATE TABLE IF NOT EXISTS
    low_volatility_execution_compatibility_runs
(
    run_hash text PRIMARY KEY CHECK (
        run_hash ~ '^[0-9a-f]{64}$'
    ),
    compatibility_spec_hash text NOT NULL UNIQUE REFERENCES
        low_volatility_execution_compatibility_specs(spec_hash),
    original_evaluation_result_hash text NOT NULL UNIQUE REFERENCES
        low_volatility_forward_evaluation_runs(result_hash),
    corrected_forward_result_hash text NOT NULL UNIQUE CHECK (
        corrected_forward_result_hash ~ '^[0-9a-f]{64}$'
    ),
    corrected_assessment_hash text NOT NULL UNIQUE CHECK (
        corrected_assessment_hash ~ '^[0-9a-f]{64}$'
    ),
    forward_spec_hash text NOT NULL REFERENCES
        low_volatility_forward_evidence_specs(spec_hash),
    source_spec_hash text NOT NULL REFERENCES
        low_volatility_research_specs(spec_hash),
    evaluation_dataset_manifest_hash text NOT NULL REFERENCES
        research_dataset_manifests(manifest_hash),
    panel_hash text NOT NULL CHECK (
        panel_hash ~ '^[0-9a-f]{64}$'
    ),
    compatibility_status text NOT NULL CHECK (
        compatibility_status IN ('compatible', 'incompatible')
    ),
    execution_timing_compatible boolean NOT NULL,
    order_intent_invariance_verified boolean NOT NULL CHECK (
        order_intent_invariance_verified
    ),
    strategy_rejected_order_count integer NOT NULL CHECK (
        strategy_rejected_order_count >= 0
    ),
    strategy_unresolved_position_count integer NOT NULL CHECK (
        strategy_unresolved_position_count >= 0
    ),
    session_count integer NOT NULL CHECK (
        session_count = 126
    ),
    block_count integer NOT NULL CHECK (
        block_count = 6
    ),
    original_evidence_status text NOT NULL CHECK (
        original_evidence_status = 'paper_candidate'
    ),
    paper_activation_allowed boolean NOT NULL DEFAULT false CHECK (
        NOT paper_activation_allowed
    ),
    runtime_activation_allowed boolean NOT NULL DEFAULT false CHECK (
        NOT runtime_activation_allowed
    ),
    live_trading_locked boolean NOT NULL DEFAULT true CHECK (
        live_trading_locked
    ),
    decision_order_policy_version text NOT NULL CHECK (
        decision_order_policy_version =
            'low-volatility-prior-close-order-intents-v1'
    ),
    completed_by text NOT NULL CHECK (
        length(btrim(completed_by)) BETWEEN 1 AND 128
        AND completed_by = btrim(completed_by)
    ),
    completed_at timestamptz NOT NULL,
    run_version text NOT NULL CHECK (
        run_version =
            'low-volatility-execution-compatibility-run-v1'
    ),
    strategy_payload jsonb NOT NULL CHECK (
        jsonb_typeof(strategy_payload) = 'object'
    ),
    payload jsonb NOT NULL CHECK (
        jsonb_typeof(payload) = 'object'
        AND payload->>'paper_activation_allowed' = 'false'
        AND payload->>'runtime_activation_allowed' = 'false'
        AND payload->>'live_trading_locked' = 'true'
    ),
    CHECK (
        execution_timing_compatible =
            (compatibility_status = 'compatible')
        AND (
            compatibility_status = 'incompatible'
            OR (
                strategy_rejected_order_count = 0
                AND strategy_unresolved_position_count = 0
            )
        )
    )
);

CREATE OR REPLACE FUNCTION
    autoquant_validate_low_volatility_execution_compatibility_run()
RETURNS trigger
LANGUAGE plpgsql
AS $validate_low_volatility_execution_compatibility_run$
DECLARE
    compatibility
        low_volatility_execution_compatibility_specs%ROWTYPE;
    evaluation low_volatility_forward_evaluation_runs%ROWTYPE;
BEGIN
    SELECT *
    INTO compatibility
    FROM low_volatility_execution_compatibility_specs
    WHERE spec_hash = NEW.compatibility_spec_hash;

    SELECT *
    INTO evaluation
    FROM low_volatility_forward_evaluation_runs
    WHERE result_hash = NEW.original_evaluation_result_hash;

    IF compatibility.spec_hash IS NULL
       OR evaluation.result_hash IS NULL
       OR compatibility.forward_spec_hash <>
            NEW.forward_spec_hash
       OR compatibility.source_spec_hash <>
            NEW.source_spec_hash
       OR evaluation.forward_spec_hash <>
            NEW.forward_spec_hash
       OR evaluation.source_spec_hash <>
            NEW.source_spec_hash
       OR evaluation.evaluation_dataset_manifest_hash <>
            NEW.evaluation_dataset_manifest_hash
       OR evaluation.panel_hash <> NEW.panel_hash
       OR evaluation.evidence_status <> 'paper_candidate'
       OR NOT evaluation.paper_trading_eligible
       OR evaluation.paper_deployment_allowed
       OR NOT evaluation.live_trading_locked THEN
        RAISE EXCEPTION
            'execution compatibility requires one locked paper candidate';
    END IF;
    RETURN NEW;
END;
$validate_low_volatility_execution_compatibility_run$;

DO $create_low_volatility_execution_compatibility_run_gate$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_execution_compatibility_run_gate'
          AND tgrelid =
            'low_volatility_execution_compatibility_runs'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_execution_compatibility_run_gate
        BEFORE INSERT
            ON low_volatility_execution_compatibility_runs
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_validate_low_volatility_execution_compatibility_run();
    END IF;
END;
$create_low_volatility_execution_compatibility_run_gate$;

DO $create_low_volatility_execution_compatibility_run_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_execution_compatibility_runs_immutable'
          AND tgrelid =
            'low_volatility_execution_compatibility_runs'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_execution_compatibility_runs_immutable
        BEFORE UPDATE OR DELETE
            ON low_volatility_execution_compatibility_runs
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_low_volatility_execution_compatibility_run_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 49)
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
