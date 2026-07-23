CREATE TABLE IF NOT EXISTS research_universe_snapshots
(
    snapshot_hash text PRIMARY KEY CHECK (
        snapshot_hash ~ '^[0-9a-f]{64}$'
    ),
    policy_hash text NOT NULL CHECK (
        policy_hash ~ '^[0-9a-f]{64}$'
    ),
    index_code text NOT NULL CHECK (
        index_code ~ '^[0-9]{6}\.(SH|SZ)$'
    ),
    reference_date date NOT NULL,
    index_constituent_date date NOT NULL,
    liquidity_date date NOT NULL,
    knowledge_as_of timestamptz NOT NULL,
    index_response_hash text NOT NULL
        REFERENCES source_evidence(evidence_hash),
    liquidity_response_hash text NOT NULL
        REFERENCES source_evidence(evidence_hash),
    member_count integer NOT NULL CHECK (
        member_count BETWEEN 20 AND 1000
    ),
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    CHECK (
        index_constituent_date <= reference_date
        AND liquidity_date <= reference_date
        AND index_constituent_date <= liquidity_date
        AND index_response_hash <> liquidity_response_hash
    )
);

CREATE INDEX IF NOT EXISTS research_universe_snapshots_date_idx
ON research_universe_snapshots
    (index_code, reference_date DESC, knowledge_as_of DESC);

CREATE UNIQUE INDEX IF NOT EXISTS
    research_universe_snapshots_identity_idx
ON research_universe_snapshots (policy_hash, reference_date);

DO $create_research_universe_immutable$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'research_universe_snapshots_immutable'
          AND tgrelid = 'research_universe_snapshots'::regclass
    ) THEN
        CREATE TRIGGER research_universe_snapshots_immutable
        BEFORE UPDATE OR DELETE ON research_universe_snapshots
        FOR EACH ROW EXECUTE FUNCTION autoquant_reject_immutable_change();
    END IF;
END;
$create_research_universe_immutable$;

INSERT INTO schema_versions (component, version)
VALUES ('postgres', 23)
ON CONFLICT (component) DO UPDATE
SET version = GREATEST(schema_versions.version, EXCLUDED.version),
    applied_at = CASE
        WHEN schema_versions.version < EXCLUDED.version THEN clock_timestamp()
        ELSE schema_versions.applied_at
    END;
