CREATE TABLE IF NOT EXISTS daily_valuation_revisions
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
    close_price Decimal(20, 6),
    free_float_turnover_rate_percent Nullable(Decimal(24, 8)),
    pe_ttm Nullable(Decimal(24, 8)),
    pb Nullable(Decimal(24, 8)),
    ps_ttm Nullable(Decimal(24, 8)),
    dividend_yield_ttm_percent Nullable(Decimal(24, 8)),
    total_market_value_cny Decimal(28, 4),
    circulating_market_value_cny Decimal(28, 4),
    content_hash FixedString(64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(session_date)
ORDER BY
(
    instrument,
    session_date,
    source,
    available_at,
    ingested_at,
    record_id
);

CREATE TABLE IF NOT EXISTS financial_indicator_revisions
(
    record_id UUID,
    source LowCardinality(String),
    instrument String,
    report_period Date,
    announced_date Date,
    updated Bool,
    event_time DateTime64(6, 'UTC'),
    available_at DateTime64(6, 'UTC'),
    ingested_at DateTime64(6, 'UTC'),
    source_revision String,
    availability_policy String,
    evidence_hash FixedString(64),
    roe_diluted_percent Nullable(Decimal(24, 8)),
    roa_percent Nullable(Decimal(24, 8)),
    gross_profit_margin_percent Nullable(Decimal(24, 8)),
    debt_to_assets_percent Nullable(Decimal(24, 8)),
    operating_cashflow_to_revenue_percent Nullable(Decimal(24, 8)),
    content_hash FixedString(64)
)
ENGINE = MergeTree
PARTITION BY toYear(report_period)
ORDER BY
(
    instrument,
    report_period,
    announced_date,
    updated,
    source,
    available_at,
    ingested_at,
    record_id
);

INSERT INTO schema_versions (component, version)
SELECT 'clickhouse', 4
WHERE NOT EXISTS
(
    SELECT 1 FROM schema_versions
    WHERE component = 'clickhouse' AND version = 4
);
