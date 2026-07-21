CREATE TABLE IF NOT EXISTS minute_bar_revisions
(
    record_id UUID,
    source LowCardinality(String),
    instrument String,
    event_time DateTime64(6, 'UTC'),
    published_at Nullable(DateTime64(6, 'UTC')),
    available_at DateTime64(6, 'UTC'),
    ingested_at DateTime64(6, 'UTC'),
    source_revision String,
    availability_policy String,
    open_price Decimal(20, 6),
    high_price Decimal(20, 6),
    low_price Decimal(20, 6),
    close_price Decimal(20, 6),
    volume UInt64,
    turnover Decimal(24, 4),
    content_hash FixedString(64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (instrument, event_time, source, available_at, ingested_at, record_id);
