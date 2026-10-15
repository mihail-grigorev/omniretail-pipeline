-- ClickHouse Wide Flat Pricing Mart
-- Denormalized flat table combining Greenplum unit economics with ClickHouse conversion funnels.

CREATE DATABASE IF NOT EXISTS omniretail;

DROP TABLE IF EXISTS omniretail.wide_product_pricing_mart;

CREATE TABLE omniretail.wide_product_pricing_mart (
    product_id             UInt64,
    snapshot_date          Date,
    category               LowCardinality(String),
    sku                    String,
    product_name           String,
    is_active              UInt8 DEFAULT 1,

    -- Financial attributes (Greenplum CDM)
    unit_cost_price        Decimal(10, 2),
    base_catalog_price     Decimal(10, 2),
    historical_net_margin  Decimal(12, 2),
    historical_margin_pct  Decimal(5, 2),
    delivered_orders_30d   UInt32,
    refund_rate_pct        Decimal(5, 2),

    -- Real-time funnel metrics (ClickHouse MV)
    views_last_1h          UInt32,
    views_last_24h         UInt32,
    cart_adds_last_1h      UInt32,
    cart_adds_last_24h     UInt32,
    purchases_last_1h      UInt32,
    purchases_last_24h     UInt32,
    unique_viewers_1h      UInt32,
    current_conv_rate_pct  Decimal(5, 2),
    cart_abandonment_pct   Decimal(5, 2),

    -- Dynamic pricing outputs
    competitor_min_price   Decimal(10, 2),
    inventory_stock_level  UInt32,
    stock_turnover_days    Decimal(5, 1),
    demand_surge_index     Decimal(4, 2),
    recommended_price      Decimal(10, 2),
    min_allowed_price      Decimal(10, 2),
    max_allowed_price      Decimal(10, 2),
    price_change_reason    LowCardinality(String),
    
    sync_batch_id          String,
    updated_at             DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (category, product_id, snapshot_date)
SETTINGS index_granularity = 8192;
