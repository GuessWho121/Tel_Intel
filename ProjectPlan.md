# Spark Telecom Intelligence Project Plan

# Complete Revised Project Plan: Spark Telecom Intelligence on Cluster + MinIO

## Summary

Build an end-to-end India-style telecom data platform using your existing distributed setup:

- 1 Spark master node on current device
- 3 Spark worker nodes
- 2 MinIO nodes on another device
- Spark jobs submitted from the Spark master container
- MinIO used as the main S3A data lake
- Future stages for Kafka, DeepMIMO, ML/AI, Iceberg, and Trino

The project will demonstrate mastery of Spark batch processing, Structured Streaming, lakehouse-style design, performance tuning, feature engineering, ML, and distributed query/search.

## Architecture

Use a medallion-style lake on MinIO:

```text
Synthetic/Open/DeepMIMO Data
        ↓
s3a://airtel-spark/raw
        ↓
s3a://airtel-spark/bronze
        ↓
s3a://airtel-spark/silver
        ↓
s3a://airtel-spark/gold
        ↓
ML Models + Trino + Dashboards
```

Primary storage paths:

- `raw`: original generated/open input data
- `bronze`: schema-applied raw Spark tables
- `silver`: cleaned and enriched telecom entities
- `gold`: customer 360, network KPIs, churn features, alerts
- `checkpoints`: streaming checkpoints
- `models`: Spark ML models

Use Parquet first. Add Iceberg later before Trino if you want stronger table metadata and schema evolution.

## Stage 1: Cluster Project Foundation

Create the project structure, config files, schema definitions, Spark session helper, and smoke tests.

Key components:

- `cluster.yaml`: Spark master URL, MinIO endpoint, bucket paths, S3A config.
- `project_config.yaml`: data volume, seed, date range, telecom circles.
- reusable Spark session builder with MinIO S3A support.
- path helpers for raw, bronze, silver, gold, checkpoints, and models.
- schemas for all customer, tower, usage, recharge, complaint, and network event datasets.

Smoke tests:

- Spark cluster distributed count job.
- MinIO write/read job using `s3a://`.
- Verify Spark UI shows worker execution.
- Verify MinIO contains written test data.

## Stage 2: Telecom Data Generation

Generate realistic India-style telecom data and write it to MinIO raw paths.

Datasets:

- `dim_customer`: customer profile, circle, region, plan type, tenure, ARPU band, device type, KYC status, home tower, churn label.
- `dim_tower`: tower ID, radio type, MCC/MNC, circle, region, latitude, longitude, capacity score.
- `fact_usage_daily`: daily data usage, voice minutes, SMS, roaming, dropped calls.
- `fact_recharge`: recharge amount, channel, pack type, validity, discount, payment failure.
- `fact_complaint`: issue type, severity, SLA status, resolution time, complaint text.
- `fact_network_event_stream`: JSON tower events for streaming.

Add intentional real-world issues:

- skewed metro circles
- duplicate records
- missing tower IDs
- late network events
- malformed records
- high-usage customers
- SLA breaches
- churn class imbalance

Later enrich tower/network data with OpenCelliD and DeepMIMO.

## Stage 3: Bronze Ingestion

Build Spark batch jobs that read raw data from MinIO and write bronze tables.

Practice:

- explicit schemas
- corrupt record handling
- schema validation
- ingestion timestamps
- source file tracking
- deduplication basics
- partitioning by date and circle where applicable

Bronze outputs:

- `bronze.dim_customer`
- `bronze.dim_tower`
- `bronze.fact_usage_daily`
- `bronze.fact_recharge`
- `bronze.fact_complaint`
- `bronze.fact_network_event_seed`

## Stage 4: Silver Cleaning and Enrichment

Build cleaned telecom domain tables.

Silver outputs:

- `silver.customer`
- `silver.tower`
- `silver.usage_daily`
- `silver.recharge`
- `silver.complaint`
- `silver.customer_tower_quality`
- `silver.circle_daily_usage`

Practice:

- joins
- broadcast joins
- null handling
- date parsing
- deduplication
- referential integrity checks
- derived columns
- Spark SQL views
- explain plans
- AQE and shuffle tuning

Important checks:

- no orphan customer IDs
- no invalid tower IDs unless intentionally quarantined
- valid date ranges
- valid recharge amounts
- valid churn label values

## Stage 5: Batch Customer 360 and Gold Analytics

Build gold business outputs.

Gold tables:

- `gold.customer_360`
- `gold.ml_customer_features`
- `gold.circle_daily_kpis`
- `gold.tower_quality_kpis`
- `gold.next_best_offer_population`

Customer 360 metrics:

- average daily data usage
- average voice usage
- recharge count
- total recharge amount
- days since last recharge
- complaint count
- SLA breach count
- rolling 7-day dropped calls
- CLV proxy
- churn risk input features
- next-best-offer category

Practice:

- window functions
- rolling aggregates
- wide-table joins
- partitioned writes
- bucketing discussion or experiment
- skew mitigation
- caching and persistence
- Spark UI performance analysis

## Stage 6: Structured Streaming Network Intelligence

Start with file-based streaming from MinIO or mounted input folders. Later upgrade to Kafka.

Streaming input:

- tower network events with event ID, event time, tower ID, radio type, signal strength, latency, packet loss, call drop rate, active users, congestion flag, outage flag.

Streaming outputs:

- `gold.tower_health_10min`
- `gold.circle_network_kpis`
- `gold.network_alerts`
- `gold.ml_network_features`

Practice:

- event-time processing
- watermarks
- tumbling windows
- sliding windows
- deduplication
- late-event handling
- stream-static joins with tower metadata
- checkpointing to MinIO
- recovery after restart

Alert categories:

- `normal`
- `network_degradation`
- `capacity_congestion`
- `probable_outage`

## Stage 7: ML and Advanced Analytics

Use Spark MLlib first.

Models:

- churn prediction from `gold.ml_customer_features`
- customer segmentation using clustering
- tower anomaly detection from `gold.ml_network_features`
- next-best-offer classifier or rule-based recommender
- complaint sentiment classification as optional AI extension

Practice:

- `StringIndexer`
- `OneHotEncoder`
- `VectorAssembler`
- train/test split by time
- binary classification metrics
- clustering evaluation
- model persistence to MinIO
- batch scoring
- feature leakage checks

Model outputs:

- `gold.customer_churn_scores`
- `gold.customer_segments`
- `gold.network_anomaly_scores`
- `gold.offer_recommendations`

## Stage 8: DeepMIMO Network Simulation Extension

Add DeepMIMO after the baseline streaming pipeline works.

Purpose:

- make network data more realistic
- simulate radio conditions
- enrich tower health events with wireless features

DeepMIMO-derived fields:

- path loss
- SINR
- RSRP/RSRQ proxy
- beam/user scenario features
- channel quality
- congestion proxy
- packet loss proxy

Implementation approach:

- run DeepMIMO separately
- convert DeepMIMO outputs into the existing network event schema
- feed converted events into the same streaming pipeline
- avoid rewriting the Spark streaming analytics job

## Stage 9: Kafka Streaming Upgrade

After file-based streaming is stable, add Kafka.

Kafka topics:

- `network-events`
- `complaint-events`
- optional `recharge-events`

Practice:

- Kafka source
- Kafka sink
- JSON parsing
- event-time extraction
- offsets
- checkpointing
- replay behavior
- exactly-once discussion with Spark sinks

Keep the file-based source as a fallback and learning baseline.

## Stage 10: Trino Search and Query Layer

Add Trino after Spark gold tables are stable.

Purpose:

- interactive SQL search over lake outputs
- analyst-style querying
- dashboard-ready access

Query targets:

- `gold.customer_360`
- `gold.ml_customer_features`
- `gold.customer_churn_scores`
- `gold.network_alerts`
- `gold.tower_health_10min`
- `gold.circle_daily_kpis`

Example use cases:

- find high-risk churn customers by circle
- search network degradation hotspots
- identify repeated SLA breach clusters
- query next-best-offer populations
- inspect tower congestion trends

Recommended path:

- start with Parquet + Hive-compatible catalog
- later upgrade to Iceberg
- use Trino as query/search layer, not as the processing engine

## Testing and Validation

Core tests:

- config loading test
- Spark session smoke test
- MinIO read/write smoke test
- schema validation test
- raw-to-bronze row count checks
- bronze-to-silver deduplication checks
- orphan customer/tower checks
- gold aggregate correctness checks
- streaming duplicate and late-event tests
- checkpoint recovery test
- ML feature leakage checks
- model metric checks
- Trino query smoke test

Acceptance criteria:

- Spark jobs run on cluster workers
- all major outputs write to MinIO
- batch pipeline produces customer 360
- streaming pipeline produces network alerts
- ML pipeline produces churn scores
- Trino can query final gold tables
- documentation explains architecture, schemas, and learning outcomes

## Assumptions

- You will write the code yourself for practice.
- I will provide architecture, schemas, pseudocode, explanations, reviews, and debugging support.
- No files will be edited unless you explicitly ask.
- Spark submit will run from the Spark master container.
- MinIO is the main data lake.
- Parquet is the first table format.
- Iceberg, Kafka, DeepMIMO, and Trino are later additions.
- The project is India-style telecom, not real Airtel internal data.
