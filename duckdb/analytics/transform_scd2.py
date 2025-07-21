import duckdb

# Connect to DuckDB database file (creates if not exists)
conn = duckdb.connect('/analytics/products_scd2.duckdb')

# Install and load HTTPFS extension for S3 access
conn.execute("INSTALL httpfs")
conn.execute("LOAD httpfs")

# Configure S3 access for MinIO
conn.execute("SET s3_region='us-east-1'")
conn.execute("SET s3_url_style='path'")
conn.execute("SET s3_access_key_id='minioadmin'")
conn.execute("SET s3_secret_access_key='minioadmin'")
conn.execute("SET s3_endpoint='minio:9000'")  # MinIO endpoint
conn.execute("SET s3_use_ssl=false")

# SCD2 query adapted for DuckDB JSON and timestamp functions
query = """
CREATE OR REPLACE TABLE products_scd2 AS
WITH cdc_events AS (
    SELECT
        COALESCE(
            CAST(json_extract(json, '$.payload.after.id') AS INT),
            CAST(json_extract(json, '$.payload.before.id') AS INT)
        ) AS id,
        json_extract(json, '$.payload.before') AS before_row_value,
        json_extract(json, '$.payload.after') AS after_row_value,
        CASE json_extract(json, '$.payload.op')
            WHEN '"c"' THEN 'CREATE'
            WHEN '"u"' THEN 'UPDATE'
            WHEN '"d"' THEN 'DELETE'
            WHEN '"r"' THEN 'SNAPSHOT'
            ELSE 'INVALID'
        END AS operation_type,
        CAST(json_extract(json, '$.payload.source.lsn') AS BIGINT) AS log_seq_num,
        to_timestamp(CAST(json_extract(json, '$.payload.ts_ms') AS BIGINT) / 1000) AS source_timestamp
    FROM read_ndjson_objects('s3://my-cdc-bucket/topics/cdc.commerce.products/*/*/*/*.json')
    WHERE json_extract(json, '$.payload.op') IS NOT NULL
),
ranked_events AS (
    SELECT
        id,
        after_row_value,
        log_seq_num,
        source_timestamp,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY log_seq_num) AS row_num,
        LEAD(source_timestamp) OVER (PARTITION BY id ORDER BY log_seq_num) AS next_change_timestamp
    FROM cdc_events
    WHERE id IS NOT NULL
)
SELECT
    id,
    CAST(json_extract(after_row_value, '$.name') AS VARCHAR(255)) AS name,
    CAST(json_extract(after_row_value, '$.description') AS TEXT) AS description,
    CAST(json_extract(after_row_value, '$.price') AS DOUBLE) AS price,
    source_timestamp AS row_valid_start_timestamp,
    COALESCE(next_change_timestamp, TIMESTAMP '9999-01-01') AS row_valid_expiration_timestamp
FROM ranked_events
ORDER BY id, row_valid_start_timestamp;
"""

# Execute the query and fetch some sample rows to verify
conn.execute(query)

sample_rows = conn.execute("SELECT * FROM products_scd2 LIMIT 5").fetchall()
print("SCD2 transformation completed and saved to products_scd2.duckdb")

print("Sample rows from products_scd2:")
for row in sample_rows:
    print(row)


conn.close()


# test = conn.execute("""
# SELECT * FROM read_ndjson_objects('s3://my-cdc-bucket/topics/cdc.commerce.products/*/*/*/*.json') LIMIT 5
# """).fetchall()

# print("Sample JSON data:", test)
# conn.close()




