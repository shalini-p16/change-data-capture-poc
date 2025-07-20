# Postgres CDC Pipeline with Kafka, S3, and DuckDB

## Objective
Capture every change to the `commerce.products` and `commerce.users` tables in a Postgres database and make it available for analytics.

---

## High-Level Architecture
- **Source Database:** PostgreSQL (`commerce.products`, `commerce.users`)
- **CDC Capture:** Debezium (via Kafka Connect source connector)
- **Message Broker:** Apache Kafka  
  - One topic per table (e.g., `cdc.commerce.products`, `cdc.commerce.users`)
- **Downstream Sink:** Kafka Connect S3 Sink Connector  
  - Writes JSON/Avro/Parquet data to S3 bucket  
  - Table-specific prefixes (e.g., `s3://bucket/products/`, `s3://bucket/users/`)
- **Analytics Layer:** DuckDB  
  - Reads data from S3  
  - Applies Slowly Changing Dimension Type 2 (SCD2) logic for historical tracking.

---

## 1. Setup Postgres with Logical Replication
*(Add your Postgres logical replication setup steps here)*

---

## 2. Deploy Kafka + Debezium Connectors
- Run Kafka cluster (Zookeeper, Kafka broker).
- Run Kafka Connect with Debezium Postgres Connector.

**Example Debezium connector config (`postgres-source.json`):**
```json
{
  "name": "postgres-commerce-cdc",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "cdc_user",
    "database.password": "cdc_password",
    "database.dbname": "commerce_db",
    "database.server.name": "commerce",
    "table.include.list": "commerce.products,commerce.users",
    "publication.name": "commerce_pub",
    "slot.name": "commerce_slot",
    "include.schema.changes": "false",
    "topic.prefix": "cdc",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "true",
    "transforms.unwrap.delete.handling.mode": "rewrite"
  }
}```

---

## 3. Configure Kafka Connect S3 Sink
- Install the S3 sink connector (Confluent or open-source).

**Example config (`s3-sink.json`):**
```json
{
  "name": "s3-sink",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "2",
    "topics": "cdc.commerce.products,cdc.commerce.users",
    "s3.bucket.name": "my-cdc-bucket",
    "s3.part.size": 5242880,
    "flush.size": 1000,
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "schema.compatibility": "NONE",
    "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
    "topics.dir": "cdc_data",
    "aws.access.key.id": "<YOUR_KEY>",
    "aws.secret.access.key": "<YOUR_SECRET>"
  }
}```


## Resulting S3 paths
```plaintext
s3://my-cdc-bucket/cdc_data/cdc.commerce.products/...
s3://my-cdc-bucket/cdc_data/cdc.commerce.users/...
```


## 4. Read Data from S3 with DuckDB
- DuckDB supports reading Parquet/CSV files directly from S3.


## 5. Apply SCD2 Logic in DuckDB

Assuming your CDC data includes:
```
- `op` or `__op` (insert/update/delete indicator)
- `ts_ms` (change timestamp)
- Primary key (`id`)
```

## SCD2 Approach

- Maintain `effective_from`, `effective_to`, and `is_current` columns.
- On update:
  - Close old record (`effective_to = ts_ms`, `is_current = false`)
  - Insert new row with `effective_from = ts_ms`
















