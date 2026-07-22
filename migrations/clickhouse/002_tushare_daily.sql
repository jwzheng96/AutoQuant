CREATE TABLE IF NOT EXISTS daily_bar_revisions
(
    record_id UUID,
    source LowCardinality(String),
    instrument String,
    session_date Date,
    event_time DateTime64(6, 'UTC'),
    available_at DateTime64(6, 'UTC'),
    ingested_at DateTime64(6, 'UTC'),
    source_revision String,
    availability_policy String,
    evidence_hash FixedString(64),
    open_price Decimal(20, 6),
    high_price Decimal(20, 6),
    low_price Decimal(20, 6),
    close_price Decimal(20, 6),
    pre_close Decimal(20, 6),
    volume UInt64,
    turnover Decimal(24, 4),
    content_hash FixedString(64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(session_date)
ORDER BY (instrument, session_date, source, available_at, ingested_at, record_id);

CREATE TABLE IF NOT EXISTS adjustment_factor_revisions
(
    record_id UUID,
    source LowCardinality(String),
    instrument String,
    session_date Date,
    event_time DateTime64(6, 'UTC'),
    available_at DateTime64(6, 'UTC'),
    ingested_at DateTime64(6, 'UTC'),
    source_revision String,
    availability_policy String,
    evidence_hash FixedString(64),
    factor Decimal(24, 6),
    content_hash FixedString(64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(session_date)
ORDER BY (instrument, session_date, source, available_at, ingested_at, record_id);

INSERT INTO schema_versions (component, version)
SELECT 'clickhouse', 2
WHERE NOT EXISTS
(
    SELECT 1 FROM schema_versions WHERE component = 'clickhouse' AND version = 2
);
