INSTALL httpfs;
LOAD httpfs;

SET s3_region='us-east-1';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_endpoint='http://minio:9000';

-- Read CDC data
CREATE VIEW products AS
SELECT * FROM parquet_scan('s3://my-cdc-bucket/cdc_data/cdc.commerce.products/*.parquet');

CREATE VIEW users AS
SELECT * FROM parquet_scan('s3://my-cdc-bucket/cdc_data/cdc.commerce.users/*.parquet');

-- Example SCD2 Table for Products
CREATE TABLE products_scd2 AS
SELECT id,
       name,
       price,
       ts_ms AS effective_from,
       NULL AS effective_to,
       TRUE AS is_current
FROM products;

SELECT * FROM products_scd2;