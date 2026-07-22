CREATE TABLE IF NOT EXISTS trading_session_revisions
(
    record_id UUID,
    source LowCardinality(String),
    session_date Date,
    is_open Bool,
    available_at DateTime64(6, 'UTC'),
    response_hash FixedString(64),
    content_hash FixedString(64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(session_date)
ORDER BY (source, session_date, available_at, record_id);

CREATE TABLE IF NOT EXISTS instrument_lifecycle_revisions
(
    record_id UUID,
    source LowCardinality(String),
    instrument String,
    list_date Date,
    delist_date Nullable(Date),
    available_at DateTime64(6, 'UTC'),
    response_hash FixedString(64),
    content_hash FixedString(64)
)
ENGINE = MergeTree
ORDER BY (source, instrument, available_at, record_id);

CREATE TABLE IF NOT EXISTS daily_suspension_revisions
(
    record_id UUID,
    source LowCardinality(String),
    instrument String,
    session_date Date,
    suspended Bool,
    available_at DateTime64(6, 'UTC'),
    response_hash FixedString(64),
    content_hash FixedString(64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(session_date)
ORDER BY (source, instrument, session_date, available_at, record_id);

CREATE TABLE IF NOT EXISTS daily_price_limit_revisions
(
    record_id UUID,
    source LowCardinality(String),
    instrument String,
    session_date Date,
    pre_close Decimal(20, 6),
    up_limit Decimal(20, 6),
    down_limit Decimal(20, 6),
    available_at DateTime64(6, 'UTC'),
    response_hash FixedString(64),
    content_hash FixedString(64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(session_date)
ORDER BY (source, instrument, session_date, available_at, record_id);

INSERT INTO schema_versions (component, version)
SELECT 'clickhouse', 3
WHERE NOT EXISTS
(
    SELECT 1 FROM schema_versions WHERE component = 'clickhouse' AND version = 3
);
