-- Greenplum Detailed Data Store (DDS)
-- Star schema with SCD Type 2 history and colocated joins.

CREATE SCHEMA IF NOT EXISTS dds;

-- Dimension: Users (SCD Type 2)
-- DISTRIBUTED BY (user_id) matches fact_orders to enable colocated local segment joins.
DROP TABLE IF EXISTS dds.dim_users_scd2 CASCADE;
CREATE TABLE dds.dim_users_scd2 (
    user_surrogate_key BIGSERIAL,
    user_id            BIGINT NOT NULL,
    name               VARCHAR(255),
    email              VARCHAR(255),
    region             VARCHAR(100),
    loyalty_tier       VARCHAR(50),
    discount_rate      NUMERIC(5, 2),
    valid_from         TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    valid_to           TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT '9999-12-31 23:59:59',
    is_current         BOOLEAN NOT NULL DEFAULT TRUE,
    hash_diff          VARCHAR(64),
    created_at         TIMESTAMP WITHOUT TIME ZONE DEFAULT clock_timestamp()
)
DISTRIBUTED BY (user_id);

CREATE INDEX idx_dim_users_lookup ON dds.dim_users_scd2 (user_id, valid_from, valid_to);

-- Dimension: Products
-- Small master table replicated across all segment nodes.
DROP TABLE IF EXISTS dds.dim_products CASCADE;
CREATE TABLE dds.dim_products (
    product_id  BIGINT PRIMARY KEY,
    sku         VARCHAR(100) NOT NULL,
    title       VARCHAR(255) NOT NULL,
    category    VARCHAR(100) NOT NULL,
    cost_price  NUMERIC(12, 2) NOT NULL,
    base_price  NUMERIC(12, 2) NOT NULL,
    is_active   BOOLEAN DEFAULT TRUE,
    updated_at  TIMESTAMP WITHOUT TIME ZONE DEFAULT clock_timestamp()
)
DISTRIBUTED REPLICATED;

-- Fact: Orders and Line Items
-- Colocated with dim_users_scd2 on user_id. Monthly partitioned.
DROP TABLE IF EXISTS dds.fact_orders CASCADE;
CREATE TABLE dds.fact_orders (
    order_item_pk       BIGSERIAL,
    order_id            BIGINT NOT NULL,
    user_id             BIGINT NOT NULL,
    user_surrogate_key  BIGINT,
    product_id          BIGINT NOT NULL,
    order_timestamp     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    quantity            INTEGER NOT NULL,
    unit_price          NUMERIC(12, 2) NOT NULL,
    gross_amount        NUMERIC(14, 2) NOT NULL,
    discount_amount     NUMERIC(12, 2) DEFAULT 0.00,
    tax_amount          NUMERIC(12, 2) DEFAULT 0.00,
    delivery_cost_share NUMERIC(12, 2) DEFAULT 0.00,
    cogs_amount         NUMERIC(14, 2) NOT NULL,
    net_revenue         NUMERIC(14, 2) NOT NULL,
    net_margin          NUMERIC(14, 2) NOT NULL,
    status              VARCHAR(50) NOT NULL,
    created_at          TIMESTAMP WITHOUT TIME ZONE DEFAULT clock_timestamp()
)
DISTRIBUTED BY (user_id)
PARTITION BY RANGE (order_timestamp)
(
    START (date '2026-01-01') INCLUSIVE
    END (date '2027-01-01') EXCLUSIVE
    EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION extra
);

CREATE INDEX idx_fact_orders_user ON dds.fact_orders (user_id, order_timestamp);
CREATE INDEX idx_fact_orders_product ON dds.fact_orders (product_id, order_timestamp);
