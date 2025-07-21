#!/bin/bash
set -e

DB_FILE="/duckdb_data/analytics.duckdb"

echo "[INFO] Initializing DuckDB and applying SCD2 transformations..."
echo "[INFO] Using database file: $DB_FILE"

# Ensure DuckDB CLI is installed (if using container image, it's already there)
# Create or connect to DuckDB database
duckdb $DB_FILE <<SQL
-- Load the S3 extension
INSTALL httpfs;
LOAD httpfs;

-- Configure S3 access (MinIO)
SET s3_region='${AWS_REGION}';
SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint='http://minio:9000'; -- Internal docker name for MinIO

-- Create a raw CDC table from JSON files in S3
CREATE OR REPLACE TABLE products_raw AS
SELECT *
FROM read_json_auto('s3://my-cdc-bucket/topics/products/*.json.gz');

-- Create the SCD2 table (dimension table)
CREATE TABLE IF NOT EXISTS products_scd2 (
    product_id VARCHAR,
    name VARCHAR,
    price DOUBLE,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
);

-- Insert new records with SCD2 logic
-- This is simplified: updates close the previous record and insert a new one
INSERT INTO products_scd2
SELECT
    p.product_id,
    p.name,
    p.price,
    CURRENT_TIMESTAMP AS valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS valid_to,
    TRUE AS is_current
FROM products_raw p
WHERE NOT EXISTS (
    SELECT 1 FROM products_scd2 s
    WHERE s.product_id = p.product_id
      AND s.is_current = TRUE
      AND (s.name != p.name OR s.price != p.price)
);

-- Mark previous versions as inactive if updated
UPDATE products_scd2
SET valid_to = CURRENT_TIMESTAMP,
    is_current = FALSE
WHERE product_id IN (
    SELECT product_id FROM products_raw
)
AND is_current = TRUE
AND valid_to = TIMESTAMP '9999-12-31 23:59:59'
AND EXISTS (
    SELECT 1 FROM products_raw r
    WHERE r.product_id = products_scd2.product_id
      AND (r.name != products_scd2.name OR r.price != products_scd2.price)
);

SQL

echo "[INFO] DuckDB SCD2 transformation complete."
