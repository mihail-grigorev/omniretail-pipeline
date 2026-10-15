# OmniRetail • Real-Time Pipeline & Hybrid DWH

A hybrid data platform architecture uniting an **ACID financial core (Greenplum MPP)** with a **real-time OLAP speed layer (ClickHouse)**, coordinated via **Apache Spark**, **Kafka**, and **MinIO S3**.

---

## 1. Architecture & Design Rationale

Retail analytics typically faces two conflicting requirements:

1. **Finance & Accounting (Strict ACID & Depth):**
   - Complex multi-table joins across orders, line items, refunds, taxes, and customer loyalty tiers.
   - Requires auditability and point-in-time accuracy (Slowly Changing Dimension Type 2).
   - **Solution:** **Greenplum MPP** with colocated hash joins on `user_id` and columnar append-optimized marts for unit economics.

2. **Storefront & Dynamic Pricing (Throughput & Latency):**
   - High-volume clickstream events (`view_item`, `add_to_cart`, `purchase`) exceeding 20,000 events/sec.
   - Requires sub-25ms lookups for price elasticity and category conversion rates.
   - **Solution:** **ClickHouse OLAP** using `ReplacingMergeTree` for deduplication and a wide denormalized flat mart (35+ columns) requiring zero table joins at query time.

```
                      [ Sources: Postgres CDC + Clickstream SDK ]
                                           │
                                           ▼
                             [ Apache Kafka Event Backbone ]
                                    /              \
                                   ▼                ▼
                     [ MinIO S3 (Bronze Lake) ]  [ ClickHouse Raw Telemetry ]
                                   │                        │
                                   ▼                        ▼
                        [ Apache Spark Engine ]    [ Materialized Views ]
                               /          \                 │
                              ▼            \                │
                   [ Greenplum MPP ]        \               │
                   • STG: Raw landing        \              │
                   • DDS: Colocated star      \             │
                   • CDM: Unit economics       ▼            ▼
                             │            [ Wide Pricing Mart (35+ cols) ]
                             └────────────►         │
                                                    ▼
                                         [ Dynamic Pricing API (<25ms) ]
                                         [ Executive Analytics Dashboards ]
```

---

## 2. Key Data Engineering Patterns

### Colocated Hash Joins (Greenplum)
In distributed MPP databases, cross-node data shuffling across the interconnect is the primary performance bottleneck.
- `dds.fact_orders` and `dds.dim_users_scd2` are distributed on the same key: `DISTRIBUTED BY (user_id)`.
- Segment nodes join orders and historical user revisions locally in memory without network transfer.
- Smaller dimension tables (`dds.dim_products`) use `DISTRIBUTED REPLICATED` across all segment nodes.

### Automated Deduplication (ClickHouse)
Mobile telemetry frequently introduces duplicate events caused by client timeouts and network retries.
- Raw clickstream uses `ReplacingMergeTree(event_version)`.
- Duplicate `event_id` rows are discarded during background part merges.
- Sparse primary index is sorted from low to high cardinality: `ORDER BY (category, region, product_id, event_time, event_id)`.

### Point-in-Time SCD Type 2 Join (Spark)
To determine the correct loyalty tier and discount rate applied when an order was placed:
- Spark computes non-overlapping validity windows (`valid_from` to `valid_to`) using window functions.
- Orders are joined on `order_lines.user_id = users_scd2.user_id` where `order_timestamp >= valid_from AND order_timestamp < valid_to`.

### Financial Reconciliation Gate
Before dynamic pricing weights are refreshed, an automated audit verifies that revenue calculated in the ClickHouse speed mart matches the audited Greenplum CDM ledger within a strict 0.01% tolerance:

$$\text{Variance} = \frac{|\text{Revenue}_{\text{Greenplum}} - \text{Revenue}_{\text{ClickHouse}}|}{\text{Revenue}_{\text{Greenplum}}} \times 100\% < 0.01\%$$

---

## 3. Repository Structure

```
├── airflow/
│   └── dags/
│       └── omniretail_ledger_dag.py       # Orchestration DAG & DQ reconciliation gate
├── data_sample/                           # Seed datasets and parquet exports
├── scripts/
│   ├── generator/
│   │   └── traffic_generator.py           # Synthetic telemetry & CDC generator
│   ├── spark/
│   │   ├── etl_greenplum_scd2.py          # SCD2 history & point-in-time fact orders ETL
│   │   └── etl_clickhouse_wide_mart.py    # Wide flat mart & dynamic pricing ETL
│   └── test_pipeline.py                   # Automated end-to-end test suite
├── sql/
│   ├── clickhouse/                        # ClickHouse DDL & materialized views
│   │   ├── 01_raw_clickstream.sql
│   │   ├── 02_materialized_views.sql
│   │   └── 03_wide_flat_mart.sql
│   └── greenplum/                         # Greenplum DWH DDL (STG, DDS, CDM)
│       ├── 01_stg_schema.sql
│       ├── 02_dds_star_schema.sql
│       └── 03_cdm_unit_economics.sql
├── docker-compose.yml                     # Local cluster infrastructure
├── index.html                             # Interactive pipeline topology console
├── styles.css                             # Production UI stylesheet
└── app.js                                 # Console interaction & telemetry logic
```

---

## 4. Quickstart

### Prerequisites
- Python 3.9+ with `pandas` and `pyarrow`
- Docker & Docker Compose (optional, for running infrastructure containers)

### 1. Run Pipeline & Verification Test Suite
Execute the end-to-end pipeline locally (generates data, builds SCD2 history, compiles the wide mart, and runs the reconciliation gate):

```bash
python scripts/test_pipeline.py
```

Expected output:
```
[*] Step 1: Generating synthetic traffic & CDC logs...
[*] Step 2: Greenplum SCD2 & fact orders ETL...
    [OK] SCD2 Dimension built: 116 user revisions.
    [OK] Fact Orders enriched: 100 line items with unit economics.
[*] Step 3: ClickHouse Wide Mart & pricing ETL...
    [OK] ClickHouse Wide Mart assembled: 30 products.
[*] Step 4: Validating output parquets & financial reconciliation...
    [CHECK] SCD2 interval check passed (116 user revisions).
    [CHECK] Non-null integrity check on pricing columns passed.
    [CHECK] Financial ledger reconciliation: PASSED (Variance 0.0030% < 0.01%)
```

### 2. Launch Local Docker Services
Start the distributed services (ClickHouse, PostgreSQL/Greenplum, Kafka, Spark):

```bash
docker compose up -d
```

Service endpoints:
- **ClickHouse HTTP:** `http://localhost:8123`
- **Greenplum (PG16):** `localhost:5435` (`gpadmin` / `pivotal`)
- **Spark Web UI:** `http://localhost:8085`
- **MinIO Console:** `http://localhost:9001` (`minioadmin` / `minioadmin`)

### 3. Open Interactive Pipeline Visualizer
Serve the console locally using Python's built-in HTTP server:

```bash
python -m http.server 8080
```
Open `http://localhost:8080` in your browser to inspect nodes, view production DDL, simulate event batches, and review live audit logs.

---

## 5. Production Mitigations Summary

| Problem | Root Cause | Architectural Mitigation |
|---|---|---|
| **Greenplum Master Bottleneck** | Routing all ETL writes through the master coordinator exhausts CPU. | Spark writes partitioned Parquet to S3; Greenplum segments read in parallel via PXF. |
| **ClickHouse "Too Many Parts"** | High-frequency single-row inserts create hundreds of tiny unmerged parts on disk. | Upstream Spark buffers writes and flushes in atomic batches of 10,000–50,000 rows. |
| **Cross-Node Network Motion** | Joining billion-row order facts with customer dimensions saturates the network interconnect. | Both tables are defined with `DISTRIBUTED BY (user_id)` for local in-memory segment joins. |
| **ClickHouse Mutation Overhead** | `ALTER UPDATE` rewrites entire compressed parts asynchronously on disk. | Replaced by `ReplacingMergeTree` with incremented `updated_at` / `event_version` keys. |

---

## License
MIT
