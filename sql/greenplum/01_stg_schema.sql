-- Greenplum Staging Layer (STG)
-- Truncated before each batch ingest from S3 / PySpark.

CREATE SCHEMA IF NOT EXISTS stg;

DROP TABLE IF EXISTS stg.stg_orders;
CREATE TABLE stg.stg_orders (
    order_id        BIGINT,
    user_id         BIGINT,
    order_timestamp TIMESTAMP WITHOUT TIME ZONE,
    status          VARCHAR(50),
    delivery_cost   NUMERIC(10, 2),
    batch_id        VARCHAR(64),
    loaded_at       TIMESTAMP WITHOUT TIME ZONE DEFAULT clock_timestamp()
)
DISTRIBUTED RANDOMLY;

DROP TABLE IF EXISTS stg.stg_order_items;
CREATE TABLE stg.stg_order_items (
    item_id         BIGINT,
    order_id        BIGINT,
    product_id      BIGINT,
    quantity        INTEGER,
    unit_price      NUMERIC(12, 2),
    discount_amount NUMERIC(12, 2),
    tax_amount      NUMERIC(12, 2),
    batch_id        VARCHAR(64),
    loaded_at       TIMESTAMP WITHOUT TIME ZONE DEFAULT clock_timestamp()
)
DISTRIBUTED RANDOMLY;

DROP TABLE IF EXISTS stg.stg_users;
CREATE TABLE stg.stg_users (
    user_id         BIGINT,
    name            VARCHAR(255),
    email           VARCHAR(255),
    region          VARCHAR(100),
    loyalty_tier    VARCHAR(50),
    discount_rate   NUMERIC(5, 2),
    updated_at      TIMESTAMP WITHOUT TIME ZONE,
    batch_id        VARCHAR(64),
    loaded_at       TIMESTAMP WITHOUT TIME ZONE DEFAULT clock_timestamp()
)
DISTRIBUTED RANDOMLY;

DROP TABLE IF EXISTS stg.stg_products;
CREATE TABLE stg.stg_products (
    product_id      BIGINT,
    sku             VARCHAR(100),
    title           VARCHAR(255),
    category        VARCHAR(100),
    cost_price      NUMERIC(12, 2),
    base_price      NUMERIC(12, 2),
    is_active       BOOLEAN,
    updated_at      TIMESTAMP WITHOUT TIME ZONE,
    batch_id        VARCHAR(64),
    loaded_at       TIMESTAMP WITHOUT TIME ZONE DEFAULT clock_timestamp()
)
DISTRIBUTED RANDOMLY;
