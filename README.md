# Spark Telecom Lakehouse With MinIO and Iceberg

This project is an end-to-end India-style telecom data platform built to practice advanced Apache Spark concepts in a realistic environment. It uses Spark for batch processing, streaming, and machine learning; MinIO as the object storage layer; and Apache Iceberg as the lakehouse table format from Bronze onward.

The project is designed around an existing distributed Spark setup:

- Spark master running on the current device
- Three Spark worker nodes running on another device
- MinIO object storage running on another device
- Spark jobs submitted from the Spark master container
- Trino planned later for interactive SQL search over Iceberg tables
- DeepMIMO planned later for realistic wireless network simulation

## Architecture

The architecture separates storage, table management, processing, and querying:

```text
MinIO   = object storage
Iceberg = lakehouse table format
Spark   = processing, streaming, and ML engine
Trino   = query/search engine added later
```

Overall data flow:

```text
Raw files on MinIO
      ↓
Bronze Iceberg tables
      ↓
Silver Iceberg tables
      ↓
Gold Iceberg tables
      ↓
Spark ML, Trino queries, and dashboards
```

Raw data remains file-based so source data is preserved. Bronze, Silver, and Gold layers are stored as Iceberg tables on MinIO.

## Lake Layout

The MinIO bucket should use a clear layout:

```text
s3a://airtel-spark/raw
s3a://airtel-spark/warehouse
s3a://airtel-spark/checkpoints
s3a://airtel-spark/models
```

Recommended usage:

- `raw`: source files such as generated CSV/JSON, OpenCelliD extracts, and DeepMIMO exports
- `warehouse`: Iceberg table warehouse for Bronze, Silver, and Gold tables
- `checkpoints`: Spark Structured Streaming checkpoints
- `models`: trained Spark ML models and scoring artifacts

## Project Layers

### Raw Layer

The Raw layer stores source-shaped files on MinIO.

Planned raw datasets:

- `customer`
- `tower`
- `usage_daily`
- `recharge`
- `complaint`
- `network_events`

Raw files should not be heavily transformed. They exist to preserve source data and support replay.

### Bronze Layer

The Bronze layer contains Iceberg tables created from raw files with explicit schemas.

Planned Bronze tables:

- `telecom.bronze_customer`
- `telecom.bronze_tower`
- `telecom.bronze_usage_daily`
- `telecom.bronze_recharge`
- `telecom.bronze_complaint`
- `telecom.bronze_network_event_seed`

Bronze responsibilities:

- apply schemas
- capture ingestion timestamps
- capture source file names
- handle corrupt records
- preserve raw business fields
- support row count validation

### Silver Layer

The Silver layer contains cleaned and enriched telecom domain tables.

Planned Silver tables:

- `telecom.silver_customer`
- `telecom.silver_tower`
- `telecom.silver_usage_daily`
- `telecom.silver_recharge`
- `telecom.silver_complaint`
- `telecom.silver_customer_tower_quality`

Silver responsibilities:

- remove duplicates
- standardize data types
- validate dates and identifiers
- handle nulls
- enforce customer and tower relationships
- create reusable enriched domain tables

### Gold Layer

The Gold layer contains business-ready analytics and ML feature tables.

Planned Gold tables:

- `telecom.gold_customer_360`
- `telecom.gold_ml_customer_features`
- `telecom.gold_circle_daily_kpis`
- `telecom.gold_tower_quality_kpis`
- `telecom.gold_next_best_offer_population`
- `telecom.gold_tower_health_10min`
- `telecom.gold_circle_network_kpis`
- `telecom.gold_network_alerts`
- `telecom.gold_ml_network_features`

Gold responsibilities:

- customer 360 analytics
- network health monitoring
- KPI generation
- feature engineering
- alert generation
- model-ready outputs

## Implementation Stages

### Stage 1: Cluster and Iceberg Foundation

Set up the base project structure, configuration files, Spark session helper, path helpers, schemas, and smoke tests.

Stage 1 is complete when Spark can:

- connect to the Spark master
- run work on the worker nodes
- write and read from MinIO using `s3a://`
- create an Iceberg table on MinIO
- insert data into the Iceberg table
- read the Iceberg table back successfully

### Stage 2: Raw Data Generation

Generate realistic telecom source data and write it to MinIO raw paths.

Datasets to generate:

- customer profiles
- tower metadata
- daily usage
- recharge transactions
- complaints
- network event records

The generator should include realistic issues such as duplicates, missing values, skewed city/circle distribution, late events, and churn class imbalance.

### Stage 3: Bronze Ingestion

Read raw files with explicit schemas and write Bronze Iceberg tables.

Key Spark topics:

- explicit schemas
- corrupt record handling
- append writes
- ingestion timestamps
- source tracking
- Iceberg table creation
- Iceberg snapshots

### Stage 4: Silver Cleaning and Enrichment

Clean Bronze data and create validated Silver tables.

Key Spark topics:

- joins
- broadcast joins
- deduplication
- null handling
- data validation
- derived columns
- partitioning strategy
- schema evolution

### Stage 5: Gold Customer 360

Create customer-level and business-level analytics.

Example metrics:

- average daily data usage
- recharge frequency
- total recharge amount
- days since last recharge
- complaint count
- SLA breach count
- rolling dropped-call metrics
- CLV proxy
- next-best-offer category
- churn feature inputs

Key Spark topics:

- aggregations
- window functions
- rolling metrics
- wide-table joins
- partitioned Iceberg writes
- Spark UI performance analysis
- adaptive query execution
- skew handling

### Stage 6: Structured Streaming Network Intelligence

Process tower network events using Spark Structured Streaming.

Streaming outputs:

- tower health windows
- circle network KPIs
- network alerts
- network ML features

Alert categories:

- `normal`
- `network_degradation`
- `capacity_congestion`
- `probable_outage`

Key Spark topics:

- event-time processing
- watermarks
- tumbling windows
- sliding windows
- deduplication
- stream-static joins
- checkpointing
- recovery after restart

### Stage 7: ML and Advanced Analytics

Use Spark MLlib on Gold Iceberg tables.

Planned models:

- churn prediction
- customer segmentation
- next-best-offer recommendation
- tower anomaly detection
- optional complaint sentiment classification

Model outputs:

- `telecom.gold_customer_churn_scores`
- `telecom.gold_customer_segments`
- `telecom.gold_network_anomaly_scores`
- `telecom.gold_offer_recommendations`

Models should be stored in MinIO under the `models` path.

### Stage 8: DeepMIMO Extension

DeepMIMO will be added after the baseline network streaming pipeline works.

Purpose:

- generate more realistic wireless network features
- enrich network events with radio-level behavior
- simulate signal quality and channel conditions

DeepMIMO-derived fields may include:

- SINR
- path loss
- channel quality
- beam/user scenario features
- RSRP proxy
- packet loss proxy

The converted DeepMIMO events should match the existing network event schema so the Spark streaming job does not need a major rewrite.

### Stage 9: Kafka Streaming Upgrade

After file-based streaming works, add Kafka.

Planned topics:

- `network-events`
- `complaint-events`
- optional `recharge-events`

Kafka will become the streaming source, while curated outputs continue to land in Iceberg tables.

### Stage 10: Trino Query and Search Layer

Add Trino after Gold Iceberg tables are stable.

Trino should query the same Iceberg catalog backed by MinIO.

Example query use cases:

- high-risk churn customers by circle
- network degradation hotspots
- repeated SLA breach clusters
- next-best-offer targeting
- tower congestion trends
- anomaly investigation

Trino should query Iceberg tables, not raw files.

## Expected Project Structure

```text
Sproj/
  README.md
  configs/
  docs/
  src/
    telecom_project/
  jobs/
    smoke_tests/
    batch/
    streaming/
    ml/
  tests/
    unit/
    integration/
  scripts/
  data/
    sample/
```

Local `data/sample` is only for tiny test files. Main project data should live in MinIO.

## Configuration Principles

Do not commit secrets.

Cluster and storage configuration should include:

- Spark master URL
- MinIO endpoint
- MinIO bucket
- Iceberg warehouse path
- catalog name
- checkpoint path
- model path

MinIO credentials should come from environment variables, not config files.

## Testing Strategy

Required validations:

- Spark workers appear in Spark master UI
- Spark cluster smoke test runs successfully
- Spark writes to MinIO using `s3a://`
- Spark reads from MinIO using `s3a://`
- Spark creates an Iceberg table on MinIO
- Spark inserts into and reads from the Iceberg table
- Bronze row counts match Raw row counts
- Silver tables remove invalid and duplicate data correctly
- Gold customer metrics pass manual checks
- Iceberg snapshots are created after table writes
- streaming checkpoints recover after restart
- ML jobs can read Gold Iceberg tables
- Trino can query Gold Iceberg tables later

## Success Criteria

This project is successful when it demonstrates:

- distributed Spark execution across the cluster
- MinIO-backed data lake storage
- Iceberg table management
- batch ETL from Raw to Bronze to Silver to Gold
- streaming network analytics
- Spark ML feature engineering and scoring
- realistic telecom business use cases
- future Trino query access over the same lakehouse tables
