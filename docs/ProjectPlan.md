# Revised Project Plan: Spark Telecom Lakehouse With MinIO + Iceberg

## Summary

Build an end-to-end India-style telecom lakehouse using your existing cluster:

- Spark master on current device
- 3 Spark workers on another device
- MinIO as object storage
- Apache Iceberg as the table format from Bronze onward
- Spark for batch, streaming, and ML processing
- Trino later for SQL search/query over Iceberg tables
- DeepMIMO later for realistic network/radio simulation

Correct architecture:

```text
MinIO = storage
Iceberg = table format
Spark = processing/ML engine
Trino = query/search engine
```

Overall flow:

```text
Raw files on MinIO
      ↓
Bronze Iceberg tables
      ↓
Silver Iceberg tables
      ↓
Gold Iceberg tables
      ↓
Spark ML + Trino Queries + Dashboards
```

## Key Architecture Changes

- Keep `raw/` as normal files on MinIO: CSV, JSON, Parquet, generated files, OpenCelliD data, DeepMIMO exports.
- Use Iceberg tables for `bronze`, `silver`, and `gold`.
- Store Iceberg warehouse metadata and data files on MinIO, for example:
  ```text
  s3a://airtel-spark/warehouse
  ```
- Use a Spark Iceberg catalog, for example:
  ```text
  telecom.bronze_customer
  telecom.silver_customer
  telecom.gold_customer_360
  ```
- Use Parquet as the Iceberg data file format.
- Add Trino later using an Iceberg catalog pointed at the same MinIO warehouse.

## Implementation Stages

### Stage 1: Cluster + Iceberg Foundation

Set up configs and smoke tests for:

- Spark master URL
- MinIO S3A endpoint
- Iceberg Spark runtime package
- Iceberg warehouse path on MinIO
- catalog name, recommended: `telecom`
- environment-based MinIO credentials
- Spark cluster smoke test
- MinIO S3A read/write smoke test
- Iceberg create/insert/read smoke test

Phase 1 is complete only when Spark can create an Iceberg table on MinIO, insert data, and read it back.

### Stage 2: Raw Data Generation

Generate raw telecom data and write it to MinIO raw paths.

Raw datasets:

- `raw/customer`
- `raw/tower`
- `raw/usage_daily`
- `raw/recharge`
- `raw/complaint`
- `raw/network_events`

These remain file-based because raw data should preserve source shape.

### Stage 3: Bronze Iceberg Ingestion

Read raw files with explicit schemas and write Bronze Iceberg tables.

Bronze tables:

- `telecom.bronze_customer`
- `telecom.bronze_tower`
- `telecom.bronze_usage_daily`
- `telecom.bronze_recharge`
- `telecom.bronze_complaint`
- `telecom.bronze_network_event_seed`

Practice:

- explicit schemas
- corrupt record handling
- ingestion timestamps
- source file tracking
- duplicate detection
- Iceberg table creation
- append writes
- snapshot inspection

### Stage 4: Silver Iceberg Cleaning

Clean, validate, and enrich Bronze tables into Silver tables.

Silver tables:

- `telecom.silver_customer`
- `telecom.silver_tower`
- `telecom.silver_usage_daily`
- `telecom.silver_recharge`
- `telecom.silver_complaint`
- `telecom.silver_customer_tower_quality`

Practice:

- joins
- broadcast joins
- referential integrity checks
- null handling
- deduplication
- derived columns
- partitioning strategy
- Iceberg schema evolution
- Iceberg overwrite/merge patterns

### Stage 5: Gold Iceberg Customer 360

Create business-ready Gold Iceberg tables.

Gold tables:

- `telecom.gold_customer_360`
- `telecom.gold_ml_customer_features`
- `telecom.gold_circle_daily_kpis`
- `telecom.gold_tower_quality_kpis`
- `telecom.gold_next_best_offer_population`

Practice:

- complex aggregations
- rolling windows
- customer-level feature engineering
- partitioned Iceberg writes
- table compaction concepts
- Spark UI performance tuning
- AQE and skew handling

### Stage 6: Structured Streaming Network Intelligence

Start with file-based streaming, then optionally move to Kafka.

Streaming outputs should be Iceberg tables where practical:

- `telecom.gold_tower_health_10min`
- `telecom.gold_circle_network_kpis`
- `telecom.gold_network_alerts`
- `telecom.gold_ml_network_features`

Practice:

- event-time processing
- watermarks
- deduplication
- stream-static joins
- checkpointing on MinIO
- writing streaming results to lake tables
- alert classification

If direct streaming writes to Iceberg become tricky, use a two-step pattern:

```text
Streaming output to checkpointed Parquet landing path
      ↓
micro-batch or scheduled Spark job
      ↓
Iceberg gold table
```

### Stage 7: ML and Advanced Analytics

Use Spark MLlib on Gold Iceberg tables.

Models:

- churn prediction
- customer segmentation
- next-best-offer recommendation
- tower anomaly detection
- optional complaint sentiment model

Outputs:

- `telecom.gold_customer_churn_scores`
- `telecom.gold_customer_segments`
- `telecom.gold_network_anomaly_scores`
- `telecom.gold_offer_recommendations`

Store trained models in:

```text
s3a://airtel-spark/models
```

### Stage 8: DeepMIMO Extension

Use DeepMIMO later to enrich network data.

DeepMIMO outputs should be converted into the same network event schema used by the streaming pipeline.

Add fields such as:

- SINR
- path loss
- channel quality
- beam/user scenario features
- RSRP proxy
- packet loss proxy

Keep Spark streaming logic mostly unchanged.

### Stage 9: Kafka Upgrade

After file-based streaming works, add Kafka topics:

- `network-events`
- `complaint-events`
- optional `recharge-events`

Use Kafka for ingestion, then continue writing curated results into Iceberg Gold tables.

### Stage 10: Trino Query/Search Layer

Add Trino after Gold Iceberg tables are stable.

Trino should query the same Iceberg catalog over MinIO.

Use cases:

- churn-risk customer search
- tower degradation search
- complaint SLA breach search
- next-best-offer targeting
- network anomaly analysis
- dashboard SQL queries

Trino should query Iceberg tables, not raw files.

## Test Plan

Required validations:

- Spark workers appear in master UI
- Spark can write/read MinIO using `s3a://`
- Spark can create/read/drop a test Iceberg table
- Bronze Iceberg tables match raw row counts
- Silver tables remove invalid or duplicate data correctly
- Gold customer 360 metrics pass manual checks
- Iceberg snapshots are created after writes
- schema evolution test works on a small table
- streaming checkpoint recovery works
- ML jobs can read Gold Iceberg tables
- Trino can query Iceberg Gold tables later

## Assumptions

- MinIO remains the object storage layer.
- Iceberg is added as the lakehouse table format, not a replacement for MinIO.
- Raw data stays file-based.
- Bronze, Silver, and Gold are Iceberg tables.
- Spark jobs are submitted from the Spark master container.
- Secrets are stored in environment variables, not committed configs.
- Trino is added only after Iceberg tables are stable.
- No project files are edited until explicitly requested.
