-- ClickHouse Raw Clickstream Layer
-- ReplacingMergeTree automatically deduplicates retries based on event_version.

CREATE DATABASE IF NOT EXISTS omniretail;

DROP TABLE IF EXISTS omniretail.raw_clickstream;

CREATE TABLE omniretail.raw_clickstream (
    event_id        UUID,
    event_time      DateTime('UTC') CODEC(DoubleDelta, LZ4),
    event_type      LowCardinality(String),
    user_id         UInt64,
    session_id      UUID,
    product_id      UInt64,
    category        LowCardinality(String),
    price           Decimal(10, 2),
    device_type     LowCardinality(String),
    region          LowCardinality(String),
    ip_address      IPv4,
    event_version   UInt64 DEFAULT 1,
    ingested_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(event_time)
-- ORDER BY sorted from low to high cardinality for sparse primary index pruning
ORDER BY (category, region, product_id, event_time, event_id)
SETTINGS index_granularity = 8192;
