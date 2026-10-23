-- Run with: docker compose exec -T warehouse psql -U warehouse -d air_quality < sql/data_quality.sql
\pset pager off
SET TIME ZONE 'UTC';

SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT location_id) AS stations,
       COUNT(DISTINCT sensor_id) AS sensors,
       COUNT(DISTINCT parameter) AS parameters,
       MIN(measured_at) AS earliest_measurement,
       MAX(measured_at) AS latest_measurement
FROM raw_air_quality;

-- Expected: no rows.
SELECT sensor_id, measured_at, COUNT(*) AS occurrences
FROM raw_air_quality
GROUP BY sensor_id, measured_at
HAVING COUNT(*) > 1;

SELECT location_name, parameter, unit,
       COUNT(*) AS measurement_count,
       COUNT(DISTINCT sensor_id) AS sensor_count,
       MIN(measured_at) AS earliest_measurement,
       MAX(measured_at) AS latest_measurement
FROM raw_air_quality
GROUP BY location_name, parameter, unit
ORDER BY location_name, parameter, unit;

SELECT COUNT(*) FILTER (WHERE sensor_id IS NULL) AS missing_sensor_ids,
       COUNT(*) FILTER (WHERE measured_at IS NULL) AS missing_timestamps,
       COUNT(*) FILTER (WHERE value IS NULL) AS null_values,
       COUNT(*) FILTER (WHERE value < 0) AS negative_values
FROM raw_air_quality;

SELECT parameter, unit, COUNT(*) AS rows,
       MIN(value) AS minimum_value, MAX(value) AS maximum_value
FROM raw_air_quality
GROUP BY parameter, unit
ORDER BY parameter, unit;
