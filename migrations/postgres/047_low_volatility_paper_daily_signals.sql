CREATE TABLE IF NOT EXISTS low_volatility_paper_daily_signals
(
    signal_hash text PRIMARY KEY CHECK (
        signal_hash ~ '^[0-9a-f]{64}$'
    ),
    candidate_approval_hash text NOT NULL REFERENCES
        low_volatility_paper_candidate_approvals(approval_hash),
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
    risk_policy_hash text NOT NULL CHECK (
        risk_policy_hash ~ '^[0-9a-f]{64}$'
    ),
    session_sequence integer NOT NULL CHECK (
        session_sequence > 0
    ),
    session_date date NOT NULL,
    signal_date date NOT NULL,
    window_start_date date NOT NULL,
    previous_signal_hash text NOT NULL CHECK (
        previous_signal_hash ~ '^[0-9a-f]{64}$'
    ),
    snapshot_hash text NOT NULL REFERENCES
        research_universe_snapshots(snapshot_hash),
    dataset_manifest_hash text NOT NULL REFERENCES
        research_dataset_manifests(manifest_hash),
    rule_set_hash text NOT NULL CHECK (
        rule_set_hash ~ '^[0-9a-f]{64}$'
    ),
    universe_member_count integer NOT NULL CHECK (
        universe_member_count BETWEEN 20 AND 1000
    ),
    evidence_instrument_count integer NOT NULL CHECK (
        evidence_instrument_count BETWEEN 1 AND 1000
    ),
    observation_count integer NOT NULL CHECK (
        observation_count BETWEEN 0 AND 1000
    ),
    selected_count integer NOT NULL CHECK (
        selected_count IN (0, 20)
    ),
    rebalance_due boolean NOT NULL,
    prepared_by text NOT NULL CHECK (
        length(btrim(prepared_by)) BETWEEN 1 AND 128
        AND prepared_by = btrim(prepared_by)
    ),
    prepared_at timestamptz NOT NULL,
    decision_policy text NOT NULL CHECK (
        decision_policy =
            'prior-close-preopen-observation-only-v1'
    ),
    execution_timing_compatible boolean NOT NULL DEFAULT false CHECK (
        NOT execution_timing_compatible
    ),
    runtime_activation_allowed boolean NOT NULL DEFAULT false CHECK (
        NOT runtime_activation_allowed
    ),
    live_trading_locked boolean NOT NULL DEFAULT true CHECK (
        live_trading_locked
    ),
    signal_version text NOT NULL CHECK (
        signal_version =
            'low-volatility-paper-daily-signal-v1'
    ),
    payload jsonb NOT NULL CHECK (
        jsonb_typeof(payload) = 'object'
        AND payload->>'execution_timing_compatible' = 'false'
        AND payload->>'runtime_activation_allowed' = 'false'
        AND payload->>'live_trading_locked' = 'true'
    ),
    UNIQUE (candidate_approval_hash, session_sequence),
    UNIQUE (candidate_approval_hash, session_date),
    CHECK (
        window_start_date <= signal_date
        AND signal_date < session_date
        AND evidence_instrument_count >= universe_member_count
        AND (
            (observation_count >= 60 AND selected_count = 20)
            OR (observation_count < 60 AND selected_count = 0)
            OR NOT rebalance_due
        )
        AND rebalance_due =
            (mod(session_sequence - 1, 21) = 0)
        AND prepared_at <
            (
                session_date::timestamp + time '09:30'
            ) AT TIME ZONE 'Asia/Shanghai'
    )
);

CREATE OR REPLACE FUNCTION
    autoquant_validate_low_volatility_paper_daily_signal()
RETURNS trigger
LANGUAGE plpgsql
AS $validate_low_volatility_paper_daily_signal$
DECLARE
    approval low_volatility_paper_candidate_approvals%ROWTYPE;
    source_spec low_volatility_research_specs%ROWTYPE;
    snapshot research_universe_snapshots%ROWTYPE;
    manifest research_dataset_manifests%ROWTYPE;
    previous low_volatility_paper_daily_signals%ROWTYPE;
    manifest_instruments text[];
BEGIN
    SELECT a.*
    INTO approval
    FROM low_volatility_paper_candidate_approvals a
    LEFT JOIN low_volatility_paper_candidate_revocations r
      ON r.approval_hash = a.approval_hash
    WHERE a.approval_hash = NEW.candidate_approval_hash
      AND r.revocation_hash IS NULL;

    SELECT *
    INTO source_spec
    FROM low_volatility_research_specs
    WHERE spec_hash = NEW.source_spec_hash;

    SELECT *
    INTO snapshot
    FROM research_universe_snapshots
    WHERE snapshot_hash = NEW.snapshot_hash;

    SELECT *
    INTO manifest
    FROM research_dataset_manifests
    WHERE manifest_hash = NEW.dataset_manifest_hash;

    SELECT array_agg(instrument ORDER BY instrument)
    INTO manifest_instruments
    FROM research_dataset_manifest_shards
    WHERE manifest_hash = NEW.dataset_manifest_hash;

    IF approval.approval_hash IS NULL
       OR approval.account_id <> NEW.account_id
       OR approval.strategy_id <> NEW.strategy_id
       OR approval.source_spec_hash <> NEW.source_spec_hash
       OR approval.risk_policy_hash <> NEW.risk_policy_hash
       OR source_spec.spec_hash IS NULL
       OR snapshot.snapshot_hash IS NULL
       OR snapshot.policy_hash <> source_spec.policy_hash
       OR snapshot.reference_date >= NEW.session_date
       OR snapshot.knowledge_as_of > NEW.prepared_at
       OR manifest.manifest_hash IS NULL
       OR manifest.policy_hash <> source_spec.policy_hash
       OR manifest.start_date <> NEW.window_start_date
       OR manifest.end_date <> NEW.signal_date
       OR manifest.snapshot_count <> 1
       OR manifest.payload->'snapshot_hashes' <>
            jsonb_build_array(NEW.snapshot_hash)
       OR to_jsonb(manifest_instruments) <>
            NEW.payload->'evidence_instruments'
       OR NOT (
            approval.payload->'instruments' @>
            NEW.payload->'evidence_instruments'
       ) THEN
        RAISE EXCEPTION
            'low-volatility paper daily signal evidence is invalid';
    END IF;

    SELECT *
    INTO previous
    FROM low_volatility_paper_daily_signals
    WHERE candidate_approval_hash =
        NEW.candidate_approval_hash
    ORDER BY session_sequence DESC
    LIMIT 1;

    IF previous.signal_hash IS NULL THEN
        IF NEW.session_sequence <> 1
           OR NEW.previous_signal_hash <>
                repeat('0', 64) THEN
            RAISE EXCEPTION
                'low-volatility paper signal genesis is invalid';
        END IF;
    ELSIF NEW.session_sequence <>
            previous.session_sequence + 1
       OR NEW.session_date <= previous.session_date
       OR NEW.previous_signal_hash <>
            previous.signal_hash THEN
        RAISE EXCEPTION
            'low-volatility paper signal chain is invalid';
    END IF;

    RETURN NEW;
END;
$validate_low_volatility_paper_daily_signal$;

DO $create_low_volatility_paper_daily_signal_gate$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_paper_daily_signal_gate'
          AND tgrelid =
            'low_volatility_paper_daily_signals'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_paper_daily_signal_gate
        BEFORE INSERT
            ON low_volatility_paper_daily_signals
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_validate_low_volatility_paper_daily_signal();
    END IF;
END;
$create_low_volatility_paper_daily_signal_gate$;

CREATE INDEX IF NOT EXISTS
    low_volatility_paper_daily_signal_scope_idx
ON low_volatility_paper_daily_signals
    (
        account_id, strategy_id,
        session_date DESC
    );

DO $create_low_volatility_paper_daily_signal_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname =
            'low_volatility_paper_daily_signals_immutable'
          AND tgrelid =
            'low_volatility_paper_daily_signals'::regclass
    ) THEN
        CREATE TRIGGER
            low_volatility_paper_daily_signals_immutable
        BEFORE UPDATE OR DELETE
            ON low_volatility_paper_daily_signals
        FOR EACH ROW EXECUTE FUNCTION
            autoquant_reject_immutable_change();
    END IF;
END;
$create_low_volatility_paper_daily_signal_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 47)
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
