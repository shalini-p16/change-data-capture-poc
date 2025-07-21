-- Create table with history (SCD Type 2)
CREATE TABLE IF NOT EXISTS products_scd2 AS
SELECT *
FROM read_parquet('s3://bucket/products/*.parquet');

-- Add historical tracking logic (simplified)
INSERT INTO products_scd2
SELECT p.*
FROM read_parquet('s3://bucket/products/*.parquet') p
LEFT JOIN products_scd2 h
  ON p.id = h.id AND p.updated_at <= h.updated_at
WHERE h.id IS NULL;
