-- Greenplum Common Data Marts (CDM)
-- Unit economics financial mart with columnar ZSTD storage.

CREATE SCHEMA IF NOT EXISTS cdm;

DROP TABLE IF EXISTS cdm.dm_product_unit_economics CASCADE;
CREATE TABLE cdm.dm_product_unit_economics (
    report_date          DATE NOT NULL,
    product_id           BIGINT NOT NULL,
    category             VARCHAR(100) NOT NULL,
    total_orders_count   BIGINT DEFAULT 0,
    delivered_orders     BIGINT DEFAULT 0,
    refunded_orders      BIGINT DEFAULT 0,
    total_items_sold     BIGINT DEFAULT 0,
    gross_revenue        NUMERIC(16, 2) DEFAULT 0.00,
    cogs_total           NUMERIC(16, 2) DEFAULT 0.00,
    discounts_total      NUMERIC(16, 2) DEFAULT 0.00,
    tax_total            NUMERIC(16, 2) DEFAULT 0.00,
    delivery_costs_total NUMERIC(16, 2) DEFAULT 0.00,
    net_revenue          NUMERIC(16, 2) DEFAULT 0.00,
    net_margin           NUMERIC(16, 2) DEFAULT 0.00,
    margin_percentage    NUMERIC(6, 2) DEFAULT 0.00,
    calculated_at        TIMESTAMP WITHOUT TIME ZONE DEFAULT clock_timestamp()
)
WITH (
    APPENDOPTIMIZED = TRUE,
    ORIENTATION = COLUMN,
    COMPRESSTYPE = zstd,
    COMPRESSLEVEL = 5
)
DISTRIBUTED BY (product_id)
PARTITION BY RANGE (report_date)
(
    START (date '2026-01-01') INCLUSIVE
    END (date '2027-01-01') EXCLUSIVE
    EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION extra
);

-- Analytical aggregation view
CREATE OR REPLACE VIEW cdm.v_daily_unit_economics_calc AS
SELECT
    fo.order_timestamp::DATE AS report_date,
    fo.product_id,
    dp.category,
    COUNT(DISTINCT fo.order_id) AS total_orders_count,
    COUNT(DISTINCT CASE WHEN fo.status = 'DELIVERED' THEN fo.order_id END) AS delivered_orders,
    COUNT(DISTINCT CASE WHEN fo.status = 'REFUNDED' THEN fo.order_id END) AS refunded_orders,
    SUM(fo.quantity) AS total_items_sold,
    SUM(fo.gross_amount) AS gross_revenue,
    SUM(fo.cogs_amount) AS cogs_total,
    SUM(fo.discount_amount) AS discounts_total,
    SUM(fo.tax_amount) AS tax_total,
    SUM(fo.delivery_cost_share) AS delivery_costs_total,
    SUM(fo.net_revenue) AS net_revenue,
    SUM(fo.net_margin) AS net_margin,
    ROUND(
        (SUM(fo.net_margin) / NULLIF(SUM(fo.gross_amount), 0.0) * 100.0),
        2
    ) AS margin_percentage
FROM dds.fact_orders fo
JOIN dds.dim_products dp
    ON fo.product_id = dp.product_id
WHERE fo.status != 'CANCELLED'
GROUP BY fo.order_timestamp::DATE, fo.product_id, dp.category;
