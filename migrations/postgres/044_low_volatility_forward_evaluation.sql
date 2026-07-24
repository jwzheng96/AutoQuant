CREATE TABLE IF NOT EXISTS low_volatility_forward_evaluation_runs
(
    result_hash text PRIMARY KEY CHECK (
        result_hash ~ '^[0-9a-f]{64}$'
    ),
    forward_spec_hash text NOT NULL UNIQUE
        REFERENCES low_volatility_forward_evidence_specs(spec_hash),
    source_spec_hash text NOT NULL
        REFERENCES low_volatility_research_specs(spec_hash),
    predecessor_result_hash text NOT NULL
        REFERENCES low_volatility_validation_runs(result_hash),
    predecessor_assessment_hash text NOT NULL CHECK (
        predecessor_assessment_hash ~ '^[0-9a-f]{64}$'
    ),
    panel_hash text NOT NULL CHECK (
        panel_hash ~ '^[0-9a-f]{64}$'
    ),
    market_panel_hash text NOT NULL CHECK (
        market_panel_hash ~ '^[0-9a-f]{64}$'
    ),
    assessment_hash text NOT NULL UNIQUE CHECK (
        assessment_hash ~ '^[0-9a-f]{64}$'
    ),
    evidence_status text NOT NULL CHECK (
        evidence_status IN ('paper_candidate', 'rejected')
    ),
    session_count integer NOT NULL CHECK (
        session_count = 126
    ),
    block_count integer NOT NULL CHECK (
        block_count = 6
    ),
    paper_trading_eligible boolean NOT NULL,
    paper_deployment_allowed boolean NOT NULL DEFAULT false CHECK (
        NOT paper_deployment_allowed
    ),
    live_trading_locked boolean NOT NULL DEFAULT true CHECK (
        live_trading_locked
    ),
    strategy_rejected_order_count integer NOT NULL CHECK (
        strategy_rejected_order_count >= 0
    ),
    benchmark_rejected_order_count integer NOT NULL CHECK (
        benchmark_rejected_order_count >= 0
    ),
    strategy_unresolved_position_count integer NOT NULL CHECK (
        strategy_unresolved_position_count >= 0
    ),
    benchmark_unresolved_position_count integer NOT NULL CHECK (
        benchmark_unresolved_position_count >= 0
    ),
    evaluation_version text NOT NULL CHECK (
        evaluation_version =
            'low-volatility-forward-evaluation-v1'
    ),
    assessment_version text NOT NULL CHECK (
        assessment_version =
            'low-volatility-forward-assessment-v1'
    ),
    requested_by text NOT NULL CHECK (
        length(btrim(requested_by)) BETWEEN 1 AND 128
        AND requested_by = btrim(requested_by)
    ),
    as_of timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    strategy_payload jsonb NOT NULL CHECK (
        jsonb_typeof(strategy_payload) = 'object'
    ),
    benchmark_payload jsonb NOT NULL CHECK (
        jsonb_typeof(benchmark_payload) = 'object'
    ),
    summary_payload jsonb NOT NULL CHECK (
        jsonb_typeof(summary_payload) = 'object'
    ),
    assessment_payload jsonb NOT NULL CHECK (
        jsonb_typeof(assessment_payload) = 'object'
    ),
    CHECK (
        paper_trading_eligible =
            (evidence_status = 'paper_candidate')
    )
);

CREATE TABLE IF NOT EXISTS low_volatility_forward_evaluation_bindings
(
    result_hash text NOT NULL REFERENCES
        low_volatility_forward_evaluation_runs(result_hash),
    sequence integer NOT NULL CHECK (
        sequence BETWEEN 1 AND 126
    ),
    binding_hash text NOT NULL REFERENCES
        low_volatility_forward_sessions(binding_hash),
    session_date date NOT NULL,
    binding_payload jsonb NOT NULL CHECK (
        jsonb_typeof(binding_payload) = 'object'
    ),
    PRIMARY KEY (result_hash, sequence),
    UNIQUE (result_hash, binding_hash),
    UNIQUE (result_hash, session_date)
);

CREATE TABLE IF NOT EXISTS low_volatility_forward_evaluation_blocks
(
    result_hash text NOT NULL REFERENCES
        low_volatility_forward_evaluation_runs(result_hash),
    sequence integer NOT NULL CHECK (
        sequence BETWEEN 1 AND 6
    ),
    block_hash text NOT NULL CHECK (
        block_hash ~ '^[0-9a-f]{64}$'
    ),
    start_date date NOT NULL,
    end_date date NOT NULL,
    block_payload jsonb NOT NULL CHECK (
        jsonb_typeof(block_payload) = 'object'
    ),
    PRIMARY KEY (result_hash, sequence),
    UNIQUE (result_hash, block_hash),
    CHECK (start_date <= end_date)
);

DO $create_low_volatility_forward_evaluations_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_forward_evaluation_runs_immutable'
          AND tgrelid =
            'low_volatility_forward_evaluation_runs'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_forward_evaluation_runs_immutable
        BEFORE UPDATE OR DELETE
            ON low_volatility_forward_evaluation_runs
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_forward_evaluation_bindings_immutable'
          AND tgrelid =
            'low_volatility_forward_evaluation_bindings'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_forward_evaluation_bindings_immutable
        BEFORE UPDATE OR DELETE
            ON low_volatility_forward_evaluation_bindings
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_forward_evaluation_blocks_immutable'
          AND tgrelid =
            'low_volatility_forward_evaluation_blocks'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_forward_evaluation_blocks_immutable
        BEFORE UPDATE OR DELETE
            ON low_volatility_forward_evaluation_blocks
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_low_volatility_forward_evaluations_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 44)
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
