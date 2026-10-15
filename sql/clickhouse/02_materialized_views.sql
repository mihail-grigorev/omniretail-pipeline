-- ClickHouse Materialized Views for Real-Time Funnels
-- Ingest trigger: raw_clickstream -> MV -> AggregatingMergeTree

CREATE DATABASE IF NOT EXISTS omniretail;

DROP TABLE IF EXISTS omniretail.agg_hourly_product_funnel;

CREATE TABLE omniretail.agg_hourly_product_funnel (
    hour_bucket      DateTime,
    product_id       UInt64,
    category         LowCardinality(String),
    region           LowCardinality(String),
    views_state      AggregateFunction(sum, UInt64),
    cart_adds_state  AggregateFunction(sum, UInt64),
    purchases_state  AggregateFunction(sum, UInt64),
    unique_users_st  AggregateFunction(uniqExact, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour_bucket)
ORDER BY (category, region, product_id, hour_bucket);

-- Triggered on batch insert into raw_clickstream
DROP VIEW IF EXISTS omniretail.mv_hourly_product_funnel;

CREATE MATERIALIZED VIEW omniretail.mv_hourly_product_funnel
TO omniretail.agg_hourly_product_funnel AS
SELECT
    toStartOfHour(event_time) AS hour_bucket,
    product_id,
    category,
    region,
    sumState(if(event_type = 'view_item', toUInt64(1), toUInt64(0))) AS views_state,
    sumState(if(event_type = 'add_to_cart', toUInt64(1), toUInt64(0))) AS cart_adds_state,
    sumState(if(event_type = 'purchase', toUInt64(1), toUInt64(0))) AS purchases_state,
    uniqExactState(user_id) AS unique_users_st
FROM omniretail.raw_clickstream
GROUP BY hour_bucket, product_id, category, region;

-- Serving view queried by dynamic pricing algorithm (<20ms SLA)
CREATE OR REPLACE VIEW omniretail.v_realtime_conversion_funnel AS
SELECT
    hour_bucket,
    product_id,
    category,
    region,
    sumMerge(views_state) AS views_count,
    sumMerge(cart_adds_state) AS cart_adds_count,
    sumMerge(purchases_state) AS purchases_count,
    uniqExactMerge(unique_users_st) AS unique_active_users,
    round(if(views_count = 0, 0, (cart_adds_count / views_count) * 100), 2) AS view_to_cart_pct,
    round(if(views_count = 0, 0, (purchases_count / views_count) * 100), 2) AS view_to_purchase_pct,
    round(if(cart_adds_count = 0, 0, (purchases_count / cart_adds_count) * 100), 2) AS cart_to_purchase_pct
FROM omniretail.agg_hourly_product_funnel
GROUP BY hour_bucket, product_id, category, region;
