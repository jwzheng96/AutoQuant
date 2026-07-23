CREATE TABLE IF NOT EXISTS paper_strategy_registrations
(
    registration_hash text PRIMARY KEY CHECK (registration_hash ~ '^[0-9a-f]{64}$'),
    account_id text NOT NULL CHECK (char_length(account_id) BETWEEN 1 AND 128),
    strategy_id text NOT NULL CHECK (char_length(strategy_id) BETWEEN 1 AND 128),
    strategy_version text NOT NULL CHECK (char_length(strategy_version) BETWEEN 1 AND 128),
    experiment_id uuid NOT NULL REFERENCES validation_experiments(experiment_id),
    validation_result_hash text NOT NULL CHECK (validation_result_hash ~ '^[0-9a-f]{64}$'),
    validation_manifest_hash text NOT NULL REFERENCES dataset_manifests(manifest_hash),
    signal_manifest_hash text NOT NULL REFERENCES dataset_manifests(manifest_hash),
    signal_manifest_as_of timestamptz NOT NULL,
    instrument text NOT NULL CHECK (instrument ~ '^[0-9]{6}\.(XSHG|XSHE)$'),
    selected_fast integer NOT NULL CHECK (selected_fast >= 2),
    selected_slow integer NOT NULL CHECK (selected_slow > selected_fast),
    allocation numeric NOT NULL CHECK (allocation > 0 AND allocation <= 1),
    slippage_bps numeric NOT NULL CHECK (slippage_bps >= 0 AND slippage_bps <= 100),
    risk_policy_hash text NOT NULL CHECK (risk_policy_hash ~ '^[0-9a-f]{64}$'),
    rule_version text NOT NULL CHECK (char_length(rule_version) BETWEEN 1 AND 128),
    approved_by text NOT NULL CHECK (char_length(approved_by) BETWEEN 1 AND 128),
    approved_at timestamptz NOT NULL CHECK (approved_at >= signal_manifest_as_of),
    execution_mode text NOT NULL CHECK (execution_mode = 'paper'),
    parameter_selection_version text NOT NULL
        CHECK (parameter_selection_version = 'modal-training-selections-v1'),
    signal_policy_version text NOT NULL
        CHECK (signal_policy_version = 'prior-close-sma-target-v1'),
    artifact_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (account_id, strategy_id, strategy_version)
);

CREATE TABLE IF NOT EXISTS paper_strategy_activation_events
(
    event_hash text PRIMARY KEY CHECK (event_hash ~ '^[0-9a-f]{64}$'),
    account_id text NOT NULL CHECK (char_length(account_id) BETWEEN 1 AND 128),
    strategy_id text NOT NULL CHECK (char_length(strategy_id) BETWEEN 1 AND 128),
    sequence bigint NOT NULL CHECK (sequence > 0),
    action text NOT NULL CHECK (action IN ('approve', 'revoke')),
    registration_hash text REFERENCES paper_strategy_registrations(registration_hash),
    actor text NOT NULL CHECK (char_length(actor) BETWEEN 1 AND 128),
    reason text NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 128),
    occurred_at timestamptz NOT NULL,
    previous_hash text NOT NULL CHECK (previous_hash ~ '^[0-9a-f]{64}$'),
    event_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (account_id, strategy_id, sequence),
    CHECK (
        (action = 'approve' AND registration_hash IS NOT NULL)
        OR (action = 'revoke' AND registration_hash IS NULL)
    )
);

DO $create_paper_strategy_registrations_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'paper_strategy_registrations_immutable'
          AND tgrelid = 'paper_strategy_registrations'::regclass
    ) THEN
        CREATE TRIGGER paper_strategy_registrations_immutable
        BEFORE UPDATE OR DELETE ON paper_strategy_registrations
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_paper_strategy_registrations_immutable$;

DO $create_paper_strategy_activation_events_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'paper_strategy_activation_events_immutable'
          AND tgrelid = 'paper_strategy_activation_events'::regclass
    ) THEN
        CREATE TRIGGER paper_strategy_activation_events_immutable
        BEFORE UPDATE OR DELETE ON paper_strategy_activation_events
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_paper_strategy_activation_events_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 15)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
