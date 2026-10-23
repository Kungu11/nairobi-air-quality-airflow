-- What uniquely identifies one pollutant measurement?
-- One sensor measures one pollutant. That sensor produces at most one
-- reading for a given hour. So (sensor_id, measured_at) together are
-- the natural key -- NOT an auto-incrementing id. This is what makes
-- reruns idempotent: ON CONFLICT can target this exact pair.

CREATE TABLE IF NOT EXISTS raw_air_quality (
    location_id         INTEGER NOT NULL,
    location_name       TEXT NOT NULL,
    sensor_id           INTEGER NOT NULL,
    parameter           TEXT NOT NULL,
    unit                TEXT NOT NULL,
    value               NUMERIC NOT NULL,
    measured_at         TIMESTAMPTZ NOT NULL,   -- store in UTC only
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sensor_id, measured_at)
);

CREATE INDEX IF NOT EXISTS idx_raw_air_quality_location_param
    ON raw_air_quality (location_name, parameter);
