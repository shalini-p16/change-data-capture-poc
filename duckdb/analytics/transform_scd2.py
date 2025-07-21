import duckdb

# Create or connect to DuckDB database file
conn = duckdb.connect('/analytics/products_scd2.duckdb')
conn.execute("INSTALL httpfs")
conn.execute("LOAD httpfs")
conn.execute("SET s3_region='us-east-1'")
conn.execute("SET s3_url_style='path'")
conn.execute("SET s3_access_key_id='minioadmin'")
conn.execute("SET s3_secret_access_key='minioadmin'")
conn.execute("SET s3_endpoint='minio:9000'")  # your MinIO endpoint
conn.execute("SET s3_use_ssl=false")

conn.execute("""
CREATE OR REPLACE TABLE products_scd2 AS
WITH products_create_update_delete AS (
    SELECT
        COALESCE(
            CAST(json->'value'->'after'->'id' AS INT),
            CAST(json->'value'->'before'->'id' AS INT)
        ) AS id,
        json->'value'->'before' AS before_row_value,
        json->'value'->'after' AS after_row_value,
        CASE
            WHEN CAST(json->'value'->'$.op' AS CHAR(1)) = '"c"' THEN 'CREATE'
            WHEN CAST(json->'value'->'$.op' AS CHAR(1)) = '"d"' THEN 'DELETE'
            WHEN CAST(json->'value'->'$.op' AS CHAR(1)) = '"u"' THEN 'UPDATE'
            WHEN CAST(json->'value'->'$.op' AS CHAR(1)) = '"r"' THEN 'SNAPSHOT'
            ELSE 'INVALID'
        END AS operation_type,
        CAST(json->'value'->'source'->'lsn' AS BIGINT) AS log_seq_num,
        epoch_ms(CAST(json->'value'->'source'->'ts_ms' AS BIGINT)) AS source_timestamp
    FROM
        read_ndjson_objects('s3://my-cdc-bucket/topics/cdc.commerce.products/*/*/*/*.json')
    WHERE
        log_seq_num IS NOT NULL
)
SELECT
    id,
    CAST(after_row_value->'name' AS VARCHAR(255)) AS name,
    CAST(after_row_value->'description' AS TEXT) AS description,
    CAST(after_row_value->'price' AS NUMERIC(10, 2)) AS price,
    source_timestamp AS row_valid_start_timestamp,
    COALESCE(
        LEAD(source_timestamp) OVER (PARTITION BY id ORDER BY log_seq_num),
        CAST('9999-01-01' AS TIMESTAMP)
    ) AS row_valid_expiration_timestamp
FROM
    products_create_update_delete
WHERE
    id IN (
        SELECT id
        FROM products_create_update_delete
        GROUP BY id
        HAVING COUNT(*) > 1
    )
ORDER BY
    id, row_valid_start_timestamp
""")

print("SCD2 transformation completed and saved to products_scd2.duckdb")

conn.close()



# import duckdb
# conn = duckdb.connect()
# conn.execute("INSTALL httpfs")
# conn.execute("LOAD httpfs")
# conn.execute("SET s3_region='us-east-1'")
# conn.execute("SET s3_url_style='path'")
# conn.execute("SET s3_access_key_id='minioadmin'")
# conn.execute("SET s3_secret_access_key='minioadmin'")
# conn.execute("SET s3_endpoint='minio:9000'")  # your MinIO endpoint
# conn.execute("SET s3_use_ssl=false")
# result = conn.execute("""
#     SELECT * FROM read_ndjson_objects('s3://my-cdc-bucket/topics/cdc.commerce.products/year=2025/month=07/day=21/*.json') LIMIT 5
# """).fetchall()
# print(result)



