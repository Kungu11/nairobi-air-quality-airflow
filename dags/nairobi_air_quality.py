"""
Nairobi Air Quality batch pipeline.

Flow:

    extract_locations
          |
          v   (.expand() -- one mapped task instance per location)
    extract_sensors            (keeps only the six target pollutants)
          |
          v   (flatten, then .expand() -- one mapped instance per sensor)
    extract_measurements       (time-windowed, rate-limit aware, one at a time)
          |
          v   (flatten)
    validate_measurements
          |
          v
    load_to_postgres           (idempotent upsert)

Why two flatten steps: extract_sensors.expand(location=locations) returns ONE
XCom list PER mapped instance, i.e. a list-of-lists. To map
extract_measurements once per INDIVIDUAL sensor, we first flatten that into a
flat list of sensor dicts, then .expand() over it. Same again for measurements.

Fixes versus the first version (found from the failed run's 429 log):
  1. Measurements are requested for a TIME WINDOW, not a sensor's full history.
  2. Page size is 1000 (the API maximum) instead of 100, so ~10x fewer requests.
  3. Only the six target pollutants are extracted (temperature, humidity, etc.
     are skipped), which roughly halves the number of sensors.
  4. Requests are paced (~54/minute) and extract_measurements runs one mapped
     instance at a time, to stay under OpenAQ's 60/minute limit.
  5. A 429 response waits for the rate-limit reset and retries in place.
  6. A timeout now FAILS the task (so Airflow retries it) instead of silently
     returning partial data.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

OPENAQ_CONN_ID = "openaq_api"
WAREHOUSE_CONN_ID = "air_quality_warehouse"

# Nairobi centre, ~25km radius
NAIROBI_LAT = -1.286389
NAIROBI_LON = 36.817223
RADIUS_METERS = 25_000

# The six pollutants the project cares about
TARGET_PARAMETERS = {"pm25", "pm10", "no2", "co", "o3", "so2"}

# OpenAQ free tier: 60 requests/minute, 2,000/hour.
# 1.1s between requests => at most ~54 requests/minute.
REQUEST_PAUSE_SECONDS = 1.1
PAGE_LIMIT = 1000           # API maximum per page
MAX_PAGES = 30              # safety cap per sensor per run
MAX_RATE_LIMIT_WAITS = 5    # how many times to wait out a 429 before failing
MAX_WAIT_SECONDS = 120

# Loose sanity bounds for validate_measurements. These catch obviously broken
# readings (sensor glitches, unit mix-ups, negative values); they are not
# health thresholds.
IMPOSSIBLE_VALUE_BOUNDS = {
    "pm25": (0, 1000),
    "pm10": (0, 1000),
    "no2": (0, 2000),
    "co": (0, 50000),
    "o3": (0, 2000),
    "so2": (0, 2000),
}

default_args = {
    "owner": "nairobi-air-quality",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}


def _openaq_request(path: str, params: dict | None = None) -> dict:
    """
    Shared helper. Builds the request from the openaq_api Connection so the
    API key never appears in DAG code.

    - 401 (bad/missing key) and other HTTP errors raise immediately, and
      Airflow's task-level retry/backoff (default_args) takes over.
    - 429 (rate limit) waits for the limit to reset, then retries in place.
    - Every successful call pauses briefly to stay under the per-minute limit.
    """
    conn = BaseHook.get_connection(OPENAQ_CONN_ID)
    headers = conn.extra_dejson  # {"X-API-Key": "..."} from the Connection's Extra field
    url = f"https://{conn.host}/v3{path}"

    for attempt in range(MAX_RATE_LIMIT_WAITS + 1):
        response = requests.get(url, headers=headers, params=params or {}, timeout=30)

        if response.status_code == 429 and attempt < MAX_RATE_LIMIT_WAITS:
            reset = response.headers.get("x-ratelimit-reset", "")
            wait = int(reset) if reset.isdigit() else 60
            wait = max(1, min(wait, MAX_WAIT_SECONDS))
            logger.warning(
                "429 from OpenAQ on %s; waiting %ds before retry (%d/%d)",
                path, wait, attempt + 1, MAX_RATE_LIMIT_WAITS,
            )
            time.sleep(wait)
            continue

        response.raise_for_status()
        time.sleep(REQUEST_PAUSE_SECONDS)
        return response.json()

    # Unreachable in practice: the last loop iteration either returns or raises.
    raise RuntimeError(f"OpenAQ request to {path} failed after rate-limit waits")


@dag(
    dag_id="nairobi_air_quality",
    description="Batch pipeline: OpenAQ -> validate -> Postgres, for Nairobi air quality",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,  # never overlap runs: overlapping runs caused the rate-limit burst
    default_args=default_args,
    params={
        "lookback_days": Param(
            30,
            type="integer",
            minimum=1,
            maximum=3650,
            description=(
                "How many days back from the end of the run's data interval to "
                "request. Many Nairobi stations report irregularly, so a short "
                "window can return few stations. Use 1 for a strict daily window."
            ),
        )
    },
    tags=["air-quality", "openaq"],
)
def nairobi_air_quality():

    @task
    def extract_locations() -> list[dict]:
        """One row per OpenAQ location near Nairobi: {id, name}."""
        data = _openaq_request(
            "/locations",
            params={
                "coordinates": f"{NAIROBI_LAT},{NAIROBI_LON}",
                "radius": RADIUS_METERS,
                "limit": 100,
            },
        )
        locations = [{"id": loc["id"], "name": loc["name"]} for loc in data["results"]]
        logger.info("extract_locations: found %d locations near Nairobi", len(locations))
        return locations

    @task
    def extract_sensors(location: dict) -> list[dict]:
        """
        Target-pollutant sensors for ONE location (runs once per mapped
        instance). Locations differ: some have 1 sensor, some have 8, and
        many of those are temperature/humidity/etc., which we skip.
        """
        data = _openaq_request(f"/locations/{location['id']}/sensors")
        sensors = [
            {
                "sensor_id": s["id"],
                "parameter": s["parameter"]["name"],
                "unit": s["parameter"]["units"],
                "location_id": location["id"],
                "location_name": location["name"],
            }
            for s in data["results"]
            if s["parameter"]["name"] in TARGET_PARAMETERS
        ]
        logger.info(
            "extract_sensors: location %s (%s) has %d target-pollutant sensors",
            location["id"], location["name"], len(sensors),
        )
        return sensors

    @task
    def flatten_sensors(nested_sensor_lists: list[list[dict]]) -> list[dict]:
        """Turn [[sensors for loc A], [sensors for loc B], ...] into one flat list."""
        flat = [sensor for sensor_list in nested_sensor_lists for sensor in sensor_list]
        logger.info("flatten_sensors: %d target sensors across all locations", len(flat))
        return flat

    # max_active_tis_per_dag=1: only one mapped instance runs at a time (even
    # across overlapping DAG runs), so the request pacing above actually holds.
    @task(max_active_tis_per_dag=1)
    def extract_measurements(
        sensor: dict,
        params: dict | None = None,
        data_interval_end=None,
    ) -> list[dict]:
        """
        Measurements for ONE sensor within the run's time window:
            [data_interval_end - lookback_days, data_interval_end]
        Rerunning the same window returns the same rows, and the upsert in
        load_to_postgres makes that harmless (idempotent).
        """
        lookback_days = int((params or {}).get("lookback_days", 30))
        window_end = data_interval_end.in_timezone("UTC")
        window_start = window_end - timedelta(days=lookback_days)
        fmt = "%Y-%m-%dT%H:%M:%SZ"

        all_rows: list[dict] = []
        pages_read = 0

        for page in range(1, MAX_PAGES + 1):
            data = _openaq_request(
                f"/sensors/{sensor['sensor_id']}/measurements",
                params={
                    "datetime_from": window_start.strftime(fmt),
                    "datetime_to": window_end.strftime(fmt),
                    "page": page,
                    "limit": PAGE_LIMIT,
                },
            )
            results = data.get("results", [])
            pages_read += 1
            if not results:
                break

            for row in results:
                parameter = row.get("parameter") or {}
                period = row.get("period") or {}
                datetime_to = period.get("datetimeTo") or {}
                all_rows.append(
                    {
                        "sensor_id": sensor["sensor_id"],
                        "location_id": sensor["location_id"],
                        "location_name": sensor["location_name"],
                        "parameter": parameter.get("name") or sensor["parameter"],
                        "unit": parameter.get("units") or sensor["unit"],
                        "value": row.get("value"),
                        # measured_at = end of the hourly bucket, in UTC
                        "measured_at": datetime_to.get("utc"),
                    }
                )

            if len(results) < PAGE_LIMIT:
                break  # last page

        logger.info(
            "extract_measurements: sensor %s (%s, %s) window %s -> %s returned %d rows in %d page(s)",
            sensor["sensor_id"], sensor["parameter"], sensor["location_name"],
            window_start.strftime(fmt), window_end.strftime(fmt),
            len(all_rows), pages_read,
        )
        return all_rows

    @task
    def flatten_measurements(nested_measurement_lists: list[list[dict]]) -> list[dict]:
        flat = [m for m_list in nested_measurement_lists for m in m_list]
        logger.info("flatten_measurements: %d total raw measurements", len(flat))
        return flat

    @task
    def validate_measurements(raw_measurements: list[dict]) -> list[dict]:
        """
        Rejects null/non-numeric values, missing sensor id/timestamp, in-batch
        duplicates, and out-of-range values. Always logs
        extracted / valid / rejected counts; nothing is dropped silently.
        """
        valid: list[dict] = []
        rejected = 0
        seen_keys: set[tuple] = set()

        for m in raw_measurements:
            key = (m.get("sensor_id"), m.get("measured_at"))
            value = m.get("value")

            if m.get("sensor_id") is None or m.get("measured_at") is None:
                rejected += 1
                continue
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                rejected += 1
                continue
            if key in seen_keys:
                rejected += 1
                continue

            bounds = IMPOSSIBLE_VALUE_BOUNDS.get(m.get("parameter"))
            if bounds and not (bounds[0] <= value <= bounds[1]):
                rejected += 1
                continue

            seen_keys.add(key)
            valid.append(m)

        logger.info(
            "validate_measurements: extracted=%d valid=%d rejected=%d",
            len(raw_measurements), len(valid), rejected,
        )
        return valid

    @task
    def load_to_postgres(valid_measurements: list[dict]) -> None:
        """
        Idempotent upsert keyed on (sensor_id, measured_at), matching the
        raw_air_quality primary key. Rerunning the same batch updates existing
        rows in place instead of duplicating them.
        """
        if not valid_measurements:
            logger.info("load_to_postgres: nothing to load")
            return

        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        rows = [
            (
                m["location_id"],
                m["location_name"],
                m["sensor_id"],
                m["parameter"],
                m["unit"],
                m["value"],
                m["measured_at"],
            )
            for m in valid_measurements
        ]

        sql = """
            INSERT INTO raw_air_quality
                (location_id, location_name, sensor_id, parameter, unit, value, measured_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sensor_id, measured_at)
            DO UPDATE SET
                value = EXCLUDED.value,
                unit = EXCLUDED.unit,
                location_name = EXCLUDED.location_name,
                loaded_at = now();
        """
        conn = hook.get_conn()
        try:
            cur = conn.cursor()
            cur.executemany(sql, rows)
            conn.commit()
            cur.close()
        finally:
            conn.close()

        logger.info("load_to_postgres: upserted %d rows", len(rows))

    # ---- wiring: the fan-out / fan-in from the Day 1 diagram ----
    locations = extract_locations()
    sensors_nested = extract_sensors.expand(location=locations)
    sensors_flat = flatten_sensors(sensors_nested)
    measurements_nested = extract_measurements.expand(sensor=sensors_flat)
    measurements_flat = flatten_measurements(measurements_nested)
    valid = validate_measurements(measurements_flat)
    load_to_postgres(valid)


nairobi_air_quality()
