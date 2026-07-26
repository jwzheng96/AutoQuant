CREATE TABLE IF NOT EXISTS
    low_volatility_decision_time_paper_signals
(
    signal_hash text PRIMARY KEY CHECK (
        signal_hash ~ '^[0-9a-f]{64}$'
    ),
    deployment_contract_hash text NOT NULL REFERENCES
        low_volatility_paper_deployment_contracts(contract_hash),
    candidate_approval_hash text NOT NULL REFERENCES
        low_volatility_paper_candidate_approvals(approval_hash),
    compatibility_run_hash text NOT NULL REFERENCES
        low_volatility_execution_compatibility_runs(run_hash),
    observation_signal_hash text NOT NULL UNIQUE REFERENCES
        low_volatility_paper_daily_signals(signal_hash),
    reconciliation_report_hash text NOT NULL REFERENCES
        execution_reconciliation_reports(report_hash),
    internal_account_snapshot_hash text NOT NULL REFERENCES
        execution_account_snapshots(snapshot_hash),
    broker_account_snapshot_hash text NOT NULL REFERENCES
        execution_account_snapshots(snapshot_hash),
    kill_switch_event_hash text NOT NULL REFERENCES
        execution_control_events(event_hash),
    account_id text NOT NULL CHECK (
        length(btrim(account_id)) BETWEEN 1 AND 128
        AND account_id = btrim(account_id)
    ),
    strategy_id text NOT NULL CHECK (
        length(btrim(strategy_id)) BETWEEN 1 AND 128
        AND strategy_id = btrim(strategy_id)
    ),
    source_spec_hash text NOT NULL REFERENCES
        low_volatility_research_specs(spec_hash),
    forward_spec_hash text NOT NULL REFERENCES
        low_volatility_forward_evidence_specs(spec_hash),
    compatibility_spec_hash text NOT NULL REFERENCES
        low_volatility_execution_compatibility_specs(spec_hash),
    risk_policy_hash text NOT NULL CHECK (
        risk_policy_hash ~ '^[0-9a-f]{64}$'
    ),
    snapshot_hash text NOT NULL REFERENCES
        research_universe_snapshots(snapshot_hash),
    dataset_manifest_hash text NOT NULL REFERENCES
        research_dataset_manifests(manifest_hash),
    rule_set_hash text NOT NULL CHECK (
        rule_set_hash ~ '^[0-9a-f]{64}$'
    ),
    session_sequence integer NOT NULL CHECK (
        session_sequence > 0
    ),
    session_date date NOT NULL,
    signal_date date NOT NULL,
    account_evidence_at timestamptz NOT NULL,
    kill_switch_changed_at timestamptz NOT NULL,
    selected_count integer NOT NULL CHECK (
        selected_count IN (0, 20)
    ),
    held_position_count integer NOT NULL CHECK (
        held_position_count BETWEEN 0 AND 1000
    ),
    valuation_count integer NOT NULL CHECK (
        valuation_count BETWEEN 1 AND 1000
    ),
    prepared_by text NOT NULL CHECK (
        length(btrim(prepared_by)) BETWEEN 1 AND 128
        AND prepared_by = btrim(prepared_by)
    ),
    prepared_at timestamptz NOT NULL,
    point_in_time_universe_verified boolean NOT NULL CHECK (
        point_in_time_universe_verified
    ),
    held_position_valuation_coverage_verified
        boolean NOT NULL CHECK (
            held_position_valuation_coverage_verified
        ),
    exact_risk_policy_verified boolean NOT NULL CHECK (
        exact_risk_policy_verified
    ),
    exact_session_rules_verified boolean NOT NULL CHECK (
        exact_session_rules_verified
    ),
    decision_time_inputs_verified boolean NOT NULL CHECK (
        decision_time_inputs_verified
    ),
    account_reconciled boolean NOT NULL CHECK (
        account_reconciled
    ),
    no_open_orders_verified boolean NOT NULL CHECK (
        no_open_orders_verified
    ),
    kill_switch_active boolean NOT NULL CHECK (
        kill_switch_active
    ),
    execution_timing_compatible boolean NOT NULL CHECK (
        execution_timing_compatible
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
    decision_order_policy_version text NOT NULL CHECK (
        decision_order_policy_version =
            'low-volatility-prior-close-order-intents-v1'
    ),
    signal_version text NOT NULL CHECK (
        signal_version =
            'low-volatility-decision-time-paper-signal-v2'
    ),
    payload jsonb NOT NULL CHECK (
        jsonb_typeof(payload) = 'object'
        AND payload->>'execution_timing_compatible' = 'true'
        AND payload->>'paper_activation_authority_granted' =
            'false'
        AND payload->>'runtime_activation_allowed' = 'false'
        AND payload->>'live_trading_locked' = 'true'
    ),
    UNIQUE (candidate_approval_hash, session_sequence),
    UNIQUE (candidate_approval_hash, session_date),
    CHECK (
        signal_date < session_date
        AND account_evidence_at <= prepared_at
        AND account_evidence_at >=
            prepared_at - interval '5 minutes'
        AND kill_switch_changed_at <= prepared_at
        AND prepared_at <
            (
                session_date::timestamp + time '09:30'
            ) AT TIME ZONE 'Asia/Shanghai'
        AND held_position_count <= valuation_count
    )
);

CREATE OR REPLACE FUNCTION
    autoquant_validate_low_volatility_decision_time_signal()
RETURNS trigger
LANGUAGE plpgsql
AS $validate_low_volatility_decision_time_signal$
DECLARE
    contract
        low_volatility_paper_deployment_contracts%ROWTYPE;
    candidate
        low_volatility_paper_candidate_approvals%ROWTYPE;
    compatibility
        low_volatility_execution_compatibility_runs%ROWTYPE;
    observation low_volatility_paper_daily_signals%ROWTYPE;
    reconciliation execution_reconciliation_reports%ROWTYPE;
    internal_snapshot execution_account_snapshots%ROWTYPE;
    broker_snapshot execution_account_snapshots%ROWTYPE;
    kill_control execution_control_state%ROWTYPE;
    held_instruments jsonb;
    valuation_instruments jsonb;
BEGIN
    SELECT *
    INTO contract
    FROM low_volatility_paper_deployment_contracts
    WHERE contract_hash = NEW.deployment_contract_hash;

    SELECT a.*
    INTO candidate
    FROM low_volatility_paper_candidate_approvals a
    LEFT JOIN low_volatility_paper_candidate_revocations r
      ON r.approval_hash = a.approval_hash
    WHERE a.approval_hash = NEW.candidate_approval_hash
      AND r.revocation_hash IS NULL;

    SELECT *
    INTO compatibility
    FROM low_volatility_execution_compatibility_runs
    WHERE run_hash = NEW.compatibility_run_hash;

    SELECT *
    INTO observation
    FROM low_volatility_paper_daily_signals
    WHERE signal_hash = NEW.observation_signal_hash;

    SELECT *
    INTO reconciliation
    FROM execution_reconciliation_reports
    WHERE report_hash = NEW.reconciliation_report_hash;

    SELECT *
    INTO internal_snapshot
    FROM execution_account_snapshots
    WHERE snapshot_hash =
        NEW.internal_account_snapshot_hash;

    SELECT *
    INTO broker_snapshot
    FROM execution_account_snapshots
    WHERE snapshot_hash =
        NEW.broker_account_snapshot_hash;

    SELECT *
    INTO kill_control
    FROM execution_control_state
    WHERE account_id = NEW.account_id;

    SELECT COALESCE(
        jsonb_agg(instrument ORDER BY instrument),
        '[]'::jsonb
    )
    INTO held_instruments
    FROM (
        SELECT DISTINCT position->>'instrument' AS instrument
        FROM (
            SELECT internal_snapshot.payload AS payload
            UNION ALL
            SELECT broker_snapshot.payload AS payload
        ) snapshots
        CROSS JOIN LATERAL
            jsonb_array_elements(
                snapshots.payload->'positions'
            ) position
        WHERE (position->>'total_quantity')::bigint > 0
    ) held;

    SELECT COALESCE(
        jsonb_agg(instrument ORDER BY instrument),
        '[]'::jsonb
    )
    INTO valuation_instruments
    FROM (
        SELECT valuation->>'instrument' AS instrument
        FROM jsonb_array_elements(
            observation.payload->'valuations'
        ) valuation
    ) valued;

    IF contract.contract_hash IS NULL
       OR candidate.approval_hash IS NULL
       OR compatibility.run_hash IS NULL
       OR observation.signal_hash IS NULL
       OR reconciliation.report_hash IS NULL
       OR internal_snapshot.snapshot_hash IS NULL
       OR broker_snapshot.snapshot_hash IS NULL
       OR contract.forward_spec_hash <>
            NEW.forward_spec_hash
       OR contract.source_spec_hash <>
            NEW.source_spec_hash
       OR contract.compatibility_spec_hash <>
            NEW.compatibility_spec_hash
       OR compatibility.compatibility_spec_hash <>
            NEW.compatibility_spec_hash
       OR compatibility.forward_spec_hash <>
            NEW.forward_spec_hash
       OR compatibility.source_spec_hash <>
            NEW.source_spec_hash
       OR compatibility.original_evaluation_result_hash <>
            candidate.evaluation_result_hash
       OR compatibility.compatibility_status <> 'compatible'
       OR NOT compatibility.execution_timing_compatible
       OR compatibility.completed_at >
            candidate.approved_at
       OR contract.frozen_at >
            compatibility.completed_at
       OR candidate.account_id <> NEW.account_id
       OR candidate.strategy_id <> NEW.strategy_id
       OR candidate.forward_spec_hash <>
            NEW.forward_spec_hash
       OR candidate.source_spec_hash <>
            NEW.source_spec_hash
       OR candidate.risk_policy_hash <>
            NEW.risk_policy_hash
       OR observation.candidate_approval_hash <>
            NEW.candidate_approval_hash
       OR observation.account_id <> NEW.account_id
       OR observation.strategy_id <> NEW.strategy_id
       OR observation.source_spec_hash <>
            NEW.source_spec_hash
       OR observation.risk_policy_hash <>
            NEW.risk_policy_hash
       OR observation.session_sequence <>
            NEW.session_sequence
       OR observation.session_date <>
            NEW.session_date
       OR observation.signal_date <>
            NEW.signal_date
       OR observation.snapshot_hash <>
            NEW.snapshot_hash
       OR observation.dataset_manifest_hash <>
            NEW.dataset_manifest_hash
       OR observation.rule_set_hash <>
            NEW.rule_set_hash
       OR observation.prepared_at > NEW.prepared_at
       OR (observation.payload->'selected_instruments') <>
            (NEW.payload->'selected_instruments')
       OR valuation_instruments <>
            (NEW.payload->'valuation_instruments')
       OR held_instruments <>
            (NEW.payload->'held_instruments')
       OR NOT (
            (NEW.payload->'valuation_instruments') @>
            (NEW.payload->'held_instruments')
       )
       OR reconciliation.account_id <> NEW.account_id
       OR NOT reconciliation.reconciled
       OR reconciliation.internal_snapshot_hash <>
            NEW.internal_account_snapshot_hash
       OR reconciliation.broker_snapshot_hash <>
            NEW.broker_account_snapshot_hash
       OR reconciliation.evaluated_at <>
            NEW.account_evidence_at
       OR internal_snapshot.account_id <>
            NEW.account_id
       OR broker_snapshot.account_id <>
            NEW.account_id
       OR internal_snapshot.as_of >
            reconciliation.evaluated_at
       OR broker_snapshot.as_of >
            reconciliation.evaluated_at
       OR reconciliation.evaluated_at -
            internal_snapshot.as_of > interval '5 seconds'
       OR reconciliation.evaluated_at -
            broker_snapshot.as_of > interval '5 seconds'
       OR (internal_snapshot.payload->'open_client_order_ids')
            <> '[]'::jsonb
       OR (broker_snapshot.payload->'open_client_order_ids')
            <> '[]'::jsonb
       OR jsonb_array_length(held_instruments) <>
            NEW.held_position_count
       OR jsonb_array_length(valuation_instruments) <>
            NEW.valuation_count
       OR jsonb_array_length(
            NEW.payload->'selected_instruments'
          ) <> NEW.selected_count
       OR kill_control.account_id IS NULL
       OR NOT kill_control.active
       OR kill_control.last_event_hash <>
            NEW.kill_switch_event_hash
       OR kill_control.changed_at <>
            NEW.kill_switch_changed_at THEN
        RAISE EXCEPTION
            'decision-time paper signal evidence is invalid';
    END IF;
    RETURN NEW;
END;
$validate_low_volatility_decision_time_signal$;

DO $create_low_volatility_decision_time_signal_gate$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_decision_time_signal_gate'
          AND tgrelid =
            'low_volatility_decision_time_paper_signals'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_decision_time_signal_gate
        BEFORE INSERT
            ON low_volatility_decision_time_paper_signals
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_validate_low_volatility_decision_time_signal();
    END IF;
END;
$create_low_volatility_decision_time_signal_gate$;

DO $create_low_volatility_decision_time_signal_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_decision_time_paper_signals_immutable'
          AND tgrelid =
            'low_volatility_decision_time_paper_signals'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_decision_time_paper_signals_immutable
        BEFORE UPDATE OR DELETE
            ON low_volatility_decision_time_paper_signals
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_low_volatility_decision_time_signal_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 51)
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
