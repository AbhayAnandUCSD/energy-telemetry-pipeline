-- Example data quality SQL checks for a warehouse table `silver.telemetry`
-- Adapt table names/paths to your environment (e.g., Hive Metastore, Delta, Iceberg, or DuckDB).

-- 1) Null counts on critical fields
SELECT
  COUNT(*) AS rows_total,
  SUM(CASE WHEN power_w IS NULL THEN 1 ELSE 0 END) AS null_power,
  SUM(CASE WHEN soc_percent IS NULL THEN 1 ELSE 0 END) AS null_soc
FROM silver.telemetry
WHERE event_date = DATE '{{ ds }}';

-- 2) Bounds checks on soc_percent
SELECT
  COUNT(*) AS out_of_bounds_soc
FROM silver.telemetry
WHERE event_date = DATE '{{ ds }}'
  AND (soc_percent < 0 OR soc_percent > 100);

-- 3) Duplicate key detection on (device_id, ts)
SELECT device_id, ts, COUNT(*) AS cnt
FROM silver.telemetry
WHERE event_date = DATE '{{ ds }}'
GROUP BY device_id, ts
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 50;
