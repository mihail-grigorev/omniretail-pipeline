/**
 * OmniRetail Pipeline & Mesh Topology Controller
 * Manages interactive node inspection, animated data flows, and live audit telemetry.
 */

// Pipeline nodes metadata, schemas, and production specifications
const NODES = {
  'node-postgres': {
    title: 'PostgreSQL 16 (OLTP Core)',
    badge: 'Ingress Source',
    engine: 'PostgreSQL 16 + Debezium CDC',
    role: 'Primary ACID database for transactional orders, checkout lines, user accounts, and product master data. Streams row-level changes directly through WAL replication slots without table locking.',
    optimization: 'Uses Debezium logical decoding to avoid polling overhead. Captures before/after states for downstream SCD Type 2 tracking.',
    specs: [
      { key: 'Replication Mode', val: 'Logical Decoding (pgoutput)' },
      { key: 'WAL Level', val: 'wal_level = logical' },
      { key: 'Batch Frequency', val: 'Real-time (streaming)' },
      { key: 'Throughput', val: '1,500 tx/sec baseline' }
    ],
    code: `-- PostgreSQL Orders Schema
CREATE TABLE core.orders (
    order_id        BIGSERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL,
    status          VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    order_timestamp TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    gross_amount    NUMERIC(14, 2) NOT NULL,
    discount_amount NUMERIC(12, 2) DEFAULT 0.00,
    tax_amount      NUMERIC(12, 2) DEFAULT 0.00,
    delivery_cost   NUMERIC(10, 2) DEFAULT 0.00
);`,
    sample: {
      op: 'c',
      ts_ms: 1771160400000,
      after: {
        order_id: 50123,
        user_id: 8812,
        status: 'DELIVERED',
        gross_amount: 1420.50,
        delivery_cost: 5.00
      }
    },
    runbook: [
      {
        issue: 'Replication lag under heavy transaction spikes',
        fix: 'Increase max_wal_senders and dedicate separate NVMe storage for WAL directories.'
      },
      {
        issue: 'CDC connector restart / replay',
        fix: 'Downstream Spark ETL uses order_id and order_timestamp as idempotent upsert keys.'
      }
    ]
  },

  'node-clickstream': {
    title: 'Clickstream Telemetry SDK',
    badge: 'High-Throughput Stream',
    engine: 'Client Event SDK (HTTP / gRPC)',
    role: 'Captures user behavioral telemetry (view_item, add_to_cart, checkout_step, purchase) across web and mobile apps at high throughput.',
    optimization: 'Lightweight client buffer that batches events locally before transmission. Out-of-order deliveries and network retries are handled downstream in ClickHouse.',
    specs: [
      { key: 'Event Volume', val: '20,000+ events/sec' },
      { key: 'Transport', val: 'gRPC over HTTP/2' },
      { key: 'Serialization', val: 'Protocol Buffers / JSON' },
      { key: 'P99 Latency', val: '< 15 ms' }
    ],
    code: `// Sample Client Event Payload
{
  "event_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "event_time": "2026-10-15 14:32:00.120",
  "event_type": "view_item",
  "user_id": 8812,
  "session_id": "a40c21ee-4cbb-46f3-9ff1-d70bfdbf3e5b",
  "product_id": 402,
  "category": "Electronics",
  "price": 299.90,
  "device_type": "ios",
  "region": "Central",
  "event_version": 1
}`,
    sample: {
      event_id: '9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d',
      event_type: 'view_item',
      user_id: 8812,
      product_id: 402,
      price: 299.90,
      event_version: 1
    },
    runbook: [
      {
        issue: 'Mobile network retries producing duplicate event_ids',
        fix: 'ClickHouse ReplacingMergeTree(event_version) discards older versions during background part merges.'
      }
    ]
  },

  'node-kafka': {
    title: 'Apache Kafka Event Backbone',
    badge: 'Stream Buffer',
    engine: 'Confluent Kafka 7.5 (Cluster)',
    role: 'Central buffer decoupling high-throughput producers from downstream batch engines and real-time analytical subscribers.',
    optimization: 'Partitioned by user_id for deterministic in-order event sequencing per customer account.',
    specs: [
      { key: 'Partitions', val: '12 per topic' },
      { key: 'Replication Factor', val: '3 (min.insync.replicas = 2)' },
      { key: 'Retention', val: '7 days (compacted)' },
      { key: 'Compression', val: 'lz4' }
    ],
    code: `# Topic Configuration
kafka-topics --create \\
  --bootstrap-server kafka:9092 \\
  --topic omni.clickstream.events \\
  --partitions 12 \\
  --replication-factor 3 \\
  --config compression.type=lz4 \\
  --config retention.ms=604800000`,
    sample: {
      topic: 'omni.clickstream.events',
      partition: 4,
      offset: 1482902,
      key: 'user_8812',
      timestamp: 1771160401500
    },
    runbook: [
      {
        issue: 'Consumer group lag during flash-sale campaigns',
        fix: 'Horizontally scale Spark executors and adjust max.poll.records to prevent partition starvation.'
      }
    ]
  },

  'node-minio': {
    title: 'MinIO S3 Object Store (Bronze Lake)',
    badge: 'Data Lake',
    engine: 'MinIO S3 (Distributed)',
    role: 'Serves as the raw immutable bronze layer. Archives raw clickstream batches and CDC changelogs in partitioned Parquet files for backfills and machine learning features.',
    optimization: 'Snappy-compressed columnar Parquet files with columnar projection to minimize I/O overhead.',
    specs: [
      { key: 'Storage Format', val: 'Apache Parquet' },
      { key: 'Compression', val: 'Snappy' },
      { key: 'Partition Key', val: 'year=YYYY/month=MM/day=DD' },
      { key: 'Target File Size', val: '128 MB to 256 MB' }
    ],
    code: `# S3 Bucket Layout
s3://omni-data-lake-bronze/
  ├── clickstream/
  │   └── year=2026/month=10/day=15/
  │       ├── part-00001.snappy.parquet
  │       └── part-00002.snappy.parquet
  └── orders_cdc/
      └── year=2026/month=10/`,
    sample: {
      bucket: 'omni-data-lake-bronze',
      path: 'clickstream/year=2026/month=10/day=15/part-001.parquet',
      records: 25000,
      size_mb: 18.4
    },
    runbook: [
      {
        issue: 'Small file proliferation degrades read throughput',
        fix: 'Spark compaction job periodically merges micro-batches into 256MB Parquet segments.'
      }
    ]
  },

  'node-spark': {
    title: 'Apache Spark Distributed Compute',
    badge: 'Compute Engine',
    engine: 'Apache Spark 3.5.1 / PySpark',
    role: 'Core distributed transformation engine. Reconstructs customer history via SCD Type 2 intervals, joins orders with historical loyalty rates, and computes unit economics before synchronizing the wide flat mart.',
    optimization: 'Uses window functions for forward-fill date intervals and broadcast joins for product master lookup.',
    specs: [
      { key: 'Engine Version', val: 'Spark 3.5.1 on Java 17' },
      { key: 'Master URL', val: 'spark://spark-master:7077' },
      { key: 'Worker Memory', val: '2 GB per executor' },
      { key: 'Local Fallback', val: 'Vectorized Pandas / PyArrow' }
    ],
    code: `# SCD Type 2 Window Calculation in PySpark
from pyspark.sql.window import Window
import pyspark.sql.functions as F

user_window = Window.partitionBy("user_id").orderBy("updated_at")

users_scd2_df = users_raw_df \\
    .withColumn("valid_from", F.to_timestamp("updated_at")) \\
    .withColumn("valid_to", F.coalesce(
        F.lead("valid_from", 1).over(user_window),
        F.to_timestamp(F.lit("9999-12-31 23:59:59"))
    )) \\
    .withColumn("is_current", F.col("valid_to") == F.to_timestamp(F.lit("9999-12-31 23:59:59")))`,
    sample: {
      spark_application_id: 'app-20261015143000-0042',
      tasks_completed: 48,
      scd2_records_processed: 116,
      fact_orders_enriched: 100,
      reconciliation_variance_pct: 0.003
    },
    runbook: [
      {
        issue: 'Data skew on power users during join',
        fix: 'Add salting to user_id keys or enable adaptive query execution (spark.sql.adaptive.enabled = true).'
      }
    ]
  },

  'node-gp-stg': {
    title: 'Greenplum STG Layer (stg_orders)',
    badge: 'Staging Store',
    engine: 'Greenplum MPP (External / Heap)',
    role: 'Landing zone for raw orders imported from S3 Parquet via PXF or COPY. Truncated before each batch execution.',
    optimization: 'No foreign key constraints or secondary indexes to maximize bulk insertion write throughput.',
    specs: [
      { key: 'Distribution', val: 'DISTRIBUTED RANDOMLY' },
      { key: 'Storage Model', val: 'Heap (Row-oriented)' },
      { key: 'Lifecycle', val: 'TRUNCATE before load' },
      { key: 'Indexes', val: 'None (Bulk staging)' }
    ],
    code: `-- STG Layer DDL
CREATE TABLE stg.stg_orders (
    order_id        BIGINT,
    user_id         BIGINT,
    order_timestamp TIMESTAMP WITHOUT TIME ZONE,
    status          VARCHAR(32),
    delivery_cost   NUMERIC(10, 2),
    raw_payload     TEXT
)
DISTRIBUTED RANDOMLY;`,
    sample: {
      order_id: 50123,
      user_id: 8812,
      order_timestamp: '2026-10-15 14:00:00',
      status: 'DELIVERED',
      delivery_cost: 5.00
    },
    runbook: [
      {
        issue: 'Master node CPU saturation during bulk COPY',
        fix: 'Use Greenplum PXF external tables to read Parquet directly from S3 across all segment workers in parallel.'
      }
    ]
  },

  'node-gp-dds': {
    title: 'Greenplum DDS Layer (fact_orders & dim_users)',
    badge: 'Core Star Schema',
    engine: 'Greenplum MPP (Colocated Joins)',
    role: 'Central enterprise data model. Contains fact_orders joined with dim_users_scd2 to preserve point-in-time loyalty tier, discount rates, and regional address.',
    optimization: 'Colocated joins: Both fact_orders and dim_users_scd2 are DISTRIBUTED BY (user_id). Segment nodes join locally in RAM without interconnect network motion.',
    specs: [
      { key: 'Distribution Key', val: 'DISTRIBUTED BY (user_id)' },
      { key: 'Dimension Distribution', val: 'dim_products: DISTRIBUTED REPLICATED' },
      { key: 'Partitioning', val: 'Monthly on order_timestamp' },
      { key: 'Interconnect Cost', val: 'Zero network broadcast (colocated)' }
    ],
    code: `-- Fact Table with Colocated Distribution
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
    cogs_amount         NUMERIC(14, 2) NOT NULL,
    net_margin          NUMERIC(14, 2) NOT NULL
)
DISTRIBUTED BY (user_id) -- Colocated with dim_users_scd2
PARTITION BY RANGE (order_timestamp)
(
    START (date '2026-01-01') INCLUSIVE
    END (date '2027-01-01') EXCLUSIVE
    EVERY (INTERVAL '1 month')
);`,
    sample: {
      order_item_pk: 10042,
      order_id: 50123,
      user_id: 8812,
      product_id: 402,
      gross_amount: 1420.50,
      cogs_amount: 952.30,
      net_margin: 468.20
    },
    runbook: [
      {
        issue: 'Uneven segment disk utilization (skew)',
        fix: 'Verify hash distribution on user_id. For small dimension tables, use DISTRIBUTED REPLICATED.'
      }
    ]
  },

  'node-gp-cdm': {
    title: 'Greenplum CDM (Unit Economics Mart)',
    badge: 'Financial Mart',
    engine: 'Greenplum Columnar (ZSTD)',
    role: 'Aggregated financial reporting mart. Calculates monthly product margins, tax liabilities, customer acquisition payback, and gross margins.',
    optimization: 'Stored with ORIENTATION=COLUMN and COMPRESSTYPE=zstd. Reduces storage footprint by 5x and accelerates column aggregations.',
    specs: [
      { key: 'Orientation', val: 'ORIENTATION = COLUMN' },
      { key: 'Compression', val: 'COMPRESSTYPE = zstd (level 5)' },
      { key: 'Append-Optimized', val: 'APPENDOPTIMIZED = TRUE' },
      { key: 'Distribution', val: 'DISTRIBUTED BY (product_id)' }
    ],
    code: `-- Columnar ZSTD Financial Mart
CREATE TABLE cdm.dm_product_unit_economics (
    report_date          DATE NOT NULL,
    product_id           BIGINT NOT NULL,
    gross_revenue        NUMERIC(16, 2),
    cogs_total           NUMERIC(16, 2),
    net_margin           NUMERIC(16, 2),
    margin_percentage    NUMERIC(6, 2)
)
WITH (
    APPENDOPTIMIZED = TRUE,
    ORIENTATION = COLUMN,
    COMPRESSTYPE = zstd
)
DISTRIBUTED BY (product_id);`,
    sample: {
      report_date: '2026-10-01',
      product_id: 402,
      gross_revenue: 91223.23,
      cogs_total: 67828.98,
      net_margin: 23394.25,
      margin_pct: 25.65
    },
    runbook: [
      {
        issue: 'Frequent row-level updates slow down columnar storage',
        fix: 'Never perform single-row UPDATE on append-optimized tables. Rebuild via partition exchange.'
      }
    ]
  },

  'node-ch-raw': {
    title: 'ClickHouse raw_clickstream',
    badge: 'Real-Time OLAP',
    engine: 'ClickHouse ReplacingMergeTree',
    role: 'Ingests clickstream telemetry directly from Kafka. Automatically deduplicates duplicate event IDs across mobile retry storms during background part merges.',
    optimization: 'Sorted sparse primary index arranged from lowest to highest cardinality (category, region, product_id, event_time).',
    specs: [
      { key: 'Engine', val: 'ReplacingMergeTree(event_version)' },
      { key: 'Partition Key', val: 'toYYYYMM(event_time)' },
      { key: 'Primary Sort Order', val: '(category, region, product_id, event_time, event_id)' },
      { key: 'Index Granularity', val: '8192 rows' }
    ],
    code: `-- ClickHouse Raw Telemetry Table
CREATE TABLE omniretail.raw_clickstream (
    event_id        UUID,
    event_time      DateTime('UTC') CODEC(DoubleDelta, LZ4),
    event_type      LowCardinality(String),
    user_id         UInt64,
    session_id      UUID,
    product_id      UInt64,
    category        LowCardinality(String),
    price           Decimal(10, 2),
    event_version   UInt64 DEFAULT 1
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(event_time)
ORDER BY (category, region, product_id, event_time, event_id);`,
    sample: {
      event_id: '9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d',
      event_type: 'view_item',
      product_id: 402,
      category: 'Electronics',
      event_version: 2
    },
    runbook: [
      {
        issue: 'Frequent small inserts triggering "Too many parts" error',
        fix: 'Buffer writes upstream in Spark/Kafka and insert in batches of 10,000 to 50,000 rows.'
      }
    ]
  },

  'node-ch-mv': {
    title: 'ClickHouse Materialized Views (Pre-aggregates)',
    badge: 'Materialized View',
    engine: 'ClickHouse AggregatingMergeTree',
    role: 'Automatically aggregates incoming event batches into hourly funnel states upon ingestion without running expensive ad-hoc GROUP BY operations.',
    optimization: 'Maintains intermediate binary aggregate states (uniqExactState, sumState). Sub-20ms dashboard queries using -Merge combinators.',
    specs: [
      { key: 'Destination Engine', val: 'AggregatingMergeTree' },
      { key: 'Aggregate States', val: 'uniqExactState, sumState' },
      { key: 'Query Latency', val: '12 ms to 18 ms' },
      { key: 'Trigger', val: 'Automatic on raw_clickstream insert' }
    ],
    code: `-- Materialized View Pre-aggregating Hourly Funnels
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
GROUP BY hour_bucket, product_id, category, region;`,
    sample: {
      hour_bucket: '2026-10-15 14:00:00',
      product_id: 402,
      views_count: 1420,
      cart_adds: 284,
      purchases: 85,
      conversion_pct: 5.98
    },
    runbook: [
      {
        issue: 'Materialized views do not retroactively populate historical rows',
        fix: 'Run manual INSERT INTO ... SELECT for date ranges preceding the view creation.'
      }
    ]
  },

  'node-ch-mart': {
    title: 'ClickHouse Wide Flat Mart (Pricing & Conversion)',
    badge: 'Wide Mart',
    engine: 'ClickHouse Flat Table (35+ Columns)',
    role: 'Denormalized wide mart uniting Greenplum CDM unit economics with ClickHouse real-time conversion metrics. Serves dynamic pricing APIs and executive dashboards.',
    optimization: 'Zero-JOIN query architecture. High throughput columnar reads with LZ4 compression.',
    specs: [
      { key: 'Columns', val: '35+ denormalized fields' },
      { key: 'Joins at Query Time', val: 'Zero (fully denormalized)' },
      { key: 'Engine', val: 'ReplacingMergeTree(updated_at)' },
      { key: 'Sync Frequency', val: 'Continuous / Hourly batch' }
    ],
    code: `-- 35+ Column Wide Pricing Mart
CREATE TABLE omniretail.wide_product_pricing_mart (
    snapshot_date          Date,
    product_id             UInt64,
    product_name           String,
    category               LowCardinality(String),
    unit_cost_price        Decimal(10, 2),
    base_catalog_price     Decimal(10, 2),
    recommended_price      Decimal(10, 2),
    demand_surge_index     Decimal(4, 2),
    current_conv_rate_pct  Decimal(5, 2),
    cart_abandonment_pct   Decimal(5, 2),
    historical_net_margin  Decimal(10, 2),
    price_change_reason    LowCardinality(String),
    updated_at             DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (category, product_id, snapshot_date);`,
    sample: {
      product_id: 402,
      product_name: 'Electronics Item #402',
      unit_cost_price: 185.00,
      base_catalog_price: 299.90,
      recommended_price: 323.89,
      demand_surge_index: 1.35,
      current_conv_rate_pct: 8.77,
      price_change_reason: 'HIGH_DEMAND_SURGE'
    },
    runbook: [
      {
        issue: 'ClickHouse mutations (ALTER UPDATE) are resource-intensive',
        fix: 'Avoid mutations; insert append-only rows with incremented updated_at timestamps.'
      }
    ]
  },

  'node-pricing': {
    title: 'Real-Time Pricing Engine',
    badge: 'Serving API',
    engine: 'Go / gRPC Microservice (< 25ms SLA)',
    role: 'Queries wide_product_pricing_mart to evaluate price elasticity rules and returns recommended catalog prices to the storefront.',
    optimization: 'Zero-join single-row lookup executed against ClickHouse sparse index in under 8 milliseconds.',
    specs: [
      { key: 'SLA Guarantee', val: '< 25 ms P99' },
      { key: 'Cache Layer', val: 'Redis read-through (60s TTL)' },
      { key: 'Protocol', val: 'gRPC / JSON REST' },
      { key: 'Safeguard', val: 'Min margin guardrails (COGS + 10%)' }
    ],
    code: `// Dynamic Pricing Rule Logic
if demandSurgeIndex >= 1.25 {
    recommendedPrice = basePrice * 1.08
    reason = "HIGH_DEMAND_SURGE"
} else if cartAbandonmentPct >= 75.0 {
    recommendedPrice = basePrice * 0.95
    reason = "STIMULATE_ABANDONED_CART"
} else {
    recommendedPrice = basePrice
    reason = "STEADY_DEMAND"
}

// Floor and ceiling price guardrails
recommendedPrice = clamp(recommendedPrice, costPrice * 1.10, basePrice * 1.30)`,
    sample: {
      product_id: 402,
      active_price: 323.89,
      base_price: 299.90,
      adjustment_pct: '+8.0%',
      guardrail_status: 'VALID'
    },
    runbook: [
      {
        issue: 'Runaway pricing algorithms selling below margin',
        fix: 'Hard constraint enforced in query: recommended_price cannot drop below cost_price * 1.10.'
      }
    ]
  },

  'node-bi': {
    title: 'Executive BI & Dashboards',
    badge: 'Serving BI',
    engine: 'Apache Superset & Grafana',
    role: 'Serves interactive category funnels, conversion heatmaps, and hourly gross revenue trends.',
    optimization: 'Queries against pre-aggregated ClickHouse Materialized Views with sub-50ms render times.',
    specs: [
      { key: 'Dashboard Tool', val: 'Superset 1.4 / Grafana' },
      { key: 'Query Mode', val: 'Direct ClickHouse SQL' },
      { key: 'Refresh Rate', val: '30 seconds (Live)' }
    ],
    code: `-- Analytical Funnel Query
SELECT
    category,
    sum(views_count) AS total_views,
    sum(purchases_count) AS total_purchases,
    round((sum(purchases_count) / sum(views_count)) * 100, 2) AS conv_pct
FROM omniretail.agg_hourly_product_funnel
WHERE hour_bucket >= now() - INTERVAL 24 HOUR
GROUP BY category
ORDER BY conv_pct DESC;`,
    sample: {
      category: 'Electronics',
      total_views: 148200,
      total_purchases: 8892,
      conversion_rate_pct: 6.00
    },
    runbook: [
      {
        issue: 'Dashboard queries overloading production OLAP server',
        fix: 'Direct BI queries to ClickHouse read-only replicas with query quotas.'
      }
    ]
  },

  'node-ledger': {
    title: 'Audited Financial Ledger (P&L)',
    badge: 'Accounting',
    engine: 'Greenplum CDM + Airflow DQ Gate',
    role: 'Maintains GAAP/IFRS audit trails for accounting, refunds, tax provisions, and verifies revenue consistency between the speed layer and financial ledger.',
    optimization: 'Automated reconciliation gate halts pricing synchronization if revenue discrepancy between Greenplum and ClickHouse exceeds 0.01%.',
    specs: [
      { key: 'Audit Compliance', val: 'GAAP / IFRS 15' },
      { key: 'Variance Threshold', val: '< 0.01% (Gate enforced)' },
      { key: 'Reconciliation Schedule', val: 'Hourly via Airflow DAG' }
    ],
    code: `# Airflow Data Quality Reconciliation Gate
def check_data_quality_reconciliation(**context):
    gp_revenue = get_greenplum_cdm_revenue()
    ch_revenue = get_clickhouse_mart_revenue()
    
    variance_pct = (abs(gp_revenue - ch_revenue) / gp_revenue) * 100.0
    if variance_pct >= 0.01:
        raise ReconciliationFailure(f"Variance {variance_pct:.4f}% exceeded 0.01% threshold!")`,
    sample: {
      audit_period: '2026-10-15 14:00:00',
      greenplum_cdm_revenue: 91223.23,
      clickhouse_mart_revenue: 91225.97,
      variance_percentage: 0.003,
      gate_status: 'PASSED'
    },
    runbook: [
      {
        issue: 'SCD2 interval boundary rounding causing financial variance',
        fix: 'Enforce UTC timestamps and synchronize server NTP clocks across clusters.'
      }
    ]
  }
};

// Topologic connection graph
const CONNECTIONS = [
  { from: 'node-postgres', to: 'node-kafka', type: 'fin' },
  { from: 'node-clickstream', to: 'node-kafka', type: 'spd' },
  { from: 'node-kafka', to: 'node-minio', type: 'fin' },
  { from: 'node-kafka', to: 'node-ch-raw', type: 'spd' },
  { from: 'node-minio', to: 'node-spark', type: 'fin' },
  { from: 'node-spark', to: 'node-gp-stg', type: 'fin' },
  { from: 'node-gp-stg', to: 'node-gp-dds', type: 'fin' },
  { from: 'node-gp-dds', to: 'node-gp-cdm', type: 'fin' },
  { from: 'node-ch-raw', to: 'node-ch-mv', type: 'spd' },
  { from: 'node-spark', to: 'node-ch-mart', type: 'sync' },
  { from: 'node-gp-cdm', to: 'node-ch-mart', type: 'sync' },
  { from: 'node-ch-mv', to: 'node-ch-mart', type: 'spd' },
  { from: 'node-ch-mart', to: 'node-pricing', type: 'spd' },
  { from: 'node-ch-mv', to: 'node-bi', type: 'spd' },
  { from: 'node-gp-cdm', to: 'node-ledger', type: 'fin' }
];

// App State
const state = {
  totalEvents: 1420500,
  dedupCount: 68184,
  scd2Count: 4971,
  isStreaming: false,
  streamTimer: null,
  activeNodeId: 'node-gp-dds',
  logEntries: 3
};

// Lifecycle
document.addEventListener('DOMContentLoaded', () => {
  initNodeListeners();
  initControlListeners();
  initDrawerTabs();
  initConsoleToggle();
  renderMeshConnections();

  window.addEventListener('resize', debounce(renderMeshConnections, 100));
  selectNode('node-gp-dds');
});

function initNodeListeners() {
  document.querySelectorAll('.node-card').forEach(card => {
    card.addEventListener('click', () => {
      selectNode(card.dataset.id);
    });
  });
}

function selectNode(nodeId) {
  const node = NODES[nodeId];
  if (!node) return;

  state.activeNodeId = nodeId;

  // Toggle active card
  document.querySelectorAll('.node-card').forEach(c => c.classList.remove('active'));
  const card = document.getElementById(nodeId);
  if (card) card.classList.add('active');

  // Fill drawer details
  document.getElementById('drawer-badge').textContent = node.badge;
  document.getElementById('drawer-title').textContent = node.title;
  document.getElementById('drawer-engine').textContent = node.engine;
  document.getElementById('drawer-role').textContent = node.role;
  document.getElementById('drawer-opt').textContent = node.optimization;
  document.getElementById('drawer-ddl').textContent = node.code;
  document.getElementById('drawer-json').textContent = JSON.stringify(node.sample, null, 2);

  // Render spec table
  const specsTbody = document.getElementById('drawer-specs');
  specsTbody.innerHTML = '';
  (node.specs || []).forEach(s => {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td class="spec-key">${escapeHtml(s.key)}</td><td class="spec-val">${escapeHtml(s.val)}</td>`;
    specsTbody.appendChild(tr);
  });

  // Render runbook
  const runbookUl = document.getElementById('drawer-runbook');
  runbookUl.innerHTML = '';
  (node.runbook || []).forEach(r => {
    const li = document.createElement('li');
    li.className = 'runbook-item';
    li.innerHTML = `
      <span class="runbook-issue">${escapeHtml(r.issue)}</span>
      <span class="runbook-fix">${escapeHtml(r.fix)}</span>
    `;
    runbookUl.appendChild(li);
  });

  document.getElementById('inspector-drawer').classList.add('open');
}

function initDrawerTabs() {
  const tabs = document.querySelectorAll('.tab-btn');
  tabs.forEach(tab => {
    tab.addEventListener('click', () => {
      tabs.forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));

      tab.classList.add('active');
      const targetPane = document.getElementById(tab.dataset.tab);
      if (targetPane) targetPane.classList.add('active');
    });
  });

  document.getElementById('drawer-close').addEventListener('click', () => {
    document.getElementById('inspector-drawer').classList.remove('open');
  });

  // Copy buttons
  document.getElementById('btn-copy-ddl').addEventListener('click', () => {
    const code = document.getElementById('drawer-ddl').textContent;
    navigator.clipboard.writeText(code).then(() => showToast('SQL DDL copied to clipboard.'));
  });

  document.getElementById('btn-copy-json').addEventListener('click', () => {
    const code = document.getElementById('drawer-json').textContent;
    navigator.clipboard.writeText(code).then(() => showToast('Sample JSON copied to clipboard.'));
  });
}

function initControlListeners() {
  const btnBatch = document.getElementById('btn-generate-batch');
  const btnStream = document.getElementById('btn-toggle-stream');
  const btnAudit = document.getElementById('btn-audit');
  const filter = document.getElementById('flow-filter');

  btnBatch.addEventListener('click', () => {
    injectBatch(10000);
    pulsePackets();
    appendAuditLog('INGEST', '10,000 telemetry events ingested and dispatched to Kafka.');
    showToast('Injected 10,000 synthetic events into pipeline mesh.');
  });

  btnStream.addEventListener('click', () => {
    state.isStreaming = !state.isStreaming;
    const streamText = document.getElementById('stream-btn-text');
    const body = document.body;

    if (state.isStreaming) {
      body.classList.add('streaming');
      streamText.textContent = 'Pause Stream';
      state.streamTimer = setInterval(() => {
        injectBatch(2500);
      }, 1200);
      appendAuditLog('STREAM', 'Continuous telemetry ingestion started (~2,100 events/sec).');
      showToast('Live stream ingestion active.');
    } else {
      body.classList.remove('streaming');
      streamText.textContent = 'Live Ingestion';
      clearInterval(state.streamTimer);
      appendAuditLog('STREAM', 'Continuous telemetry ingestion paused.');
      showToast('Live ingestion paused.');
    }
  });

  btnAudit.addEventListener('click', () => {
    const gpRev = 91223.23 + (Math.random() * 50 - 25);
    const chRev = gpRev * (1.0 + (Math.random() * 0.00004));
    const variance = (Math.abs(gpRev - chRev) / gpRev * 100).toFixed(4);
    document.getElementById('val-variance').textContent = `${variance}%`;
    appendAuditLog('AUDIT', `Reconciliation gate: Greenplum $${gpRev.toLocaleString(undefined, {minimumFractionDigits: 2})} vs ClickHouse $${chRev.toLocaleString(undefined, {minimumFractionDigits: 2})} (Delta ${variance}% < 0.01% - PASS)`);
    showToast(`Reconciliation verified: ${variance}% variance (PASS)`);
  });

  filter.addEventListener('change', () => {
    renderMeshConnections();
  });
}

function injectBatch(count) {
  state.totalEvents += count;
  state.dedupCount += Math.floor(count * 0.048);
  state.scd2Count += Math.floor(count * 0.0035);

  document.getElementById('val-events').textContent = state.totalEvents.toLocaleString();
  document.getElementById('val-dedup').textContent = state.dedupCount.toLocaleString();
  document.getElementById('val-scd2').textContent = state.scd2Count.toLocaleString();

  const variance = (0.0018 + Math.random() * 0.0025).toFixed(4);
  document.getElementById('val-variance').textContent = `${variance}%`;
}

function renderMeshConnections() {
  const connGroup = document.getElementById('svg-connections');
  const packetGroup = document.getElementById('svg-packets');
  const container = document.getElementById('canvas-container');
  const filterVal = document.getElementById('flow-filter').value;

  if (!connGroup || !container) return;

  connGroup.innerHTML = '';
  packetGroup.innerHTML = '';

  const cRect = container.getBoundingClientRect();
  const scrollX = container.scrollLeft;
  const scrollY = container.scrollTop;

  CONNECTIONS.forEach((conn, index) => {
    if (filterVal === 'financial' && conn.type !== 'fin') return;
    if (filterVal === 'speed' && conn.type !== 'spd') return;
    if (filterVal === 'sync' && conn.type !== 'sync') return;

    const fromEl = document.getElementById(conn.from);
    const toEl = document.getElementById(conn.to);
    if (!fromEl || !toEl) return;

    const rFrom = fromEl.getBoundingClientRect();
    const rTo = toEl.getBoundingClientRect();

    const x1 = rFrom.right - cRect.left + scrollX;
    const y1 = rFrom.top + rFrom.height / 2 - cRect.top + scrollY;
    const x2 = rTo.left - cRect.left + scrollX;
    const y2 = rTo.top + rTo.height / 2 - cRect.top + scrollY;

    const dx = Math.abs(x2 - x1);
    const cx1 = x1 + Math.max(dx * 0.45, 24);
    const cy1 = y1;
    const cx2 = x2 - Math.max(dx * 0.45, 24);
    const cy2 = y2;

    const pathD = `M ${x1} ${y1} C ${cx1} ${cy1}, ${cx2} ${cy2}, ${x2} ${y2}`;

    const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    path.setAttribute('d', pathD);
    path.setAttribute('class', `mesh-connection-path path-${conn.type}`);
    connGroup.appendChild(path);

    // Particle
    const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    circle.setAttribute('r', '2.5');
    circle.setAttribute('fill', conn.type === 'fin' ? '#10b981' : (conn.type === 'spd' ? '#0ea5e9' : '#f59e0b'));

    const anim = document.createElementNS('http://www.w3.org/2000/svg', 'animateMotion');
    anim.setAttribute('repeatCount', 'indefinite');
    anim.setAttribute('dur', `${2.2 + (index % 4) * 0.35}s`);
    anim.setAttribute('path', pathD);

    circle.appendChild(anim);
    packetGroup.appendChild(circle);
  });
}

function pulsePackets() {
  document.querySelectorAll('#svg-packets circle').forEach(p => {
    p.setAttribute('r', '5');
    setTimeout(() => p.setAttribute('r', '2.5'), 300);
  });
}

function initConsoleToggle() {
  const toggleBtn = document.getElementById('console-toggle');
  const consoleEl = document.getElementById('audit-console');

  toggleBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    consoleEl.classList.toggle('minimized');
    toggleBtn.textContent = consoleEl.classList.contains('minimized') ? 'Expand' : 'Minimize';
  });

  document.getElementById('console-header').addEventListener('click', () => {
    consoleEl.classList.toggle('minimized');
    toggleBtn.textContent = consoleEl.classList.contains('minimized') ? 'Expand' : 'Minimize';
  });
}

function appendAuditLog(tag, msg) {
  const body = document.getElementById('console-body');
  if (!body) return;

  const now = new Date();
  const ts = now.toTimeString().split(' ')[0];

  const entry = document.createElement('div');
  entry.className = 'log-entry';

  let tagClass = 'tag-info';
  if (tag === 'AUDIT') tagClass = 'tag-success';
  if (tag === 'SPARK') tagClass = 'tag-spark';

  entry.innerHTML = `
    <span class="log-ts">${ts}</span>
    <span class="log-tag ${tagClass}">${escapeHtml(tag)}</span>
    <span class="log-msg">${escapeHtml(msg)}</span>
  `;

  body.appendChild(entry);
  body.scrollTop = body.scrollHeight;

  state.logEntries += 1;
  const countEl = document.getElementById('log-count');
  if (countEl) countEl.textContent = `${state.logEntries} entries`;
}

function showToast(msg) {
  const toast = document.getElementById('toast');
  toast.textContent = msg;
  toast.classList.add('show');
  setTimeout(() => toast.classList.remove('show'), 2500);
}

function debounce(func, wait) {
  let timeout;
  return function(...args) {
    clearTimeout(timeout);
    timeout = setTimeout(() => func.apply(this, args), wait);
  };
}

function escapeHtml(str) {
  if (typeof str !== 'string') return str;
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
