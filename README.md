# Nairobi Air Quality Batch Pipeline with Apache Airflow

**Author:** David Kungu · GitHub: Kungu11 · 
**Stack:** Airflow 2.10.4 / Python 3.12 / Docker Compose / PostgreSQL 16 / OpenAQ v3

A scheduled batch pipeline that discovers air-quality locations within 25 km of
Nairobi, discovers their pollutant sensors, retrieves windowed measurements,
validates them, and upserts them into a separate PostgreSQL warehouse.

## Observed results

The documented 365-day manual run queried 2025-10-21 00:00 UTC through
2026-10-21 00:00 UTC. Its warehouse evidence contains **9,938 rows**, **five
stations**, **five sensors**, and **one parameter (PM2.5)**. Actual stored
measurement timestamps span 2025-10-21 01:00 UTC to 2026-10-21 00:00 UTC.
These are captured results, not a promise that future API responses remain identical.

| Station | PM2.5 rows | Unit |
| --- | ---: | --- |
| Kihumo Village \|\| Antenna Array | 7,494 | µg/m³ |
| Kitisuru | 1,049 | µg/m³ |
| Providence Academy | 841 | µg/m³ |
| Marurui | 291 | µg/m³ |
| Mukuru- Pipeline | 263 | µg/m³ |

Discovery produced **16 locations and 22 target sensors**: 16 PM2.5, five PM10,
and one NO₂. Five PM2.5 sensors returned measurements. The other 17 sensors,
including all PM10 and NO₂ sensors, returned zero records for this window.
No CO, O₃, or SO₂ sensors appear in the target sensor list for this run.
This describes the queried response, not the absence of those pollutants in Nairobi.

Kitisuru returned 1,049 rows in two pages; Kihumo returned 7,494 in eight.
Validation logged `extracted=9938 valid=9938 rejected=0`. The duplicate query
returned zero rows. Stored-data checks found no null values, missing sensor IDs,
missing timestamps, or negative concentrations.

## Repository layout

- `dags/nairobi_air_quality.py`: working DAG supplied with the project.
- `sql/create_tables.sql`: warehouse schema and composite primary key.
- `sql/data_quality.sql`: coverage and data-quality checks.
- `config/`: optional Airflow configuration; placeholder tracked.
- `logs/`: local task logs; only the placeholder is tracked.
- `tests/`: repeatable unit and DAG-definition tests, with a real normalized fixture.
- `docs/architecture.md`: architecture and mapping diagram.
- `docs/evidence.md`: experiment outcomes and screenshots.
- `docs/technical_writeup.md`: engineering decisions and limitations.
- `docs/presentation.md`: live demonstration sequence and speaker notes.
- `docker-compose.yaml`, `requirements.txt`, `.env.example`, `.gitignore`.

## Run the project

### Existing working installation

Start Docker Desktop with WSL integration, then run in WSL:

```bash
cd ~/nairobi-air-quality-airflow
docker compose up -d
docker compose ps
```

If only the webserver service has exited:

```bash
docker compose up -d --force-recreate airflow-apiserver
```

Keep the existing `.env`, Airflow Connections, and named volumes. Never run
`docker compose down -v`: that removes the metadata and warehouse volumes.

### New checkout

Use Docker Desktop with WSL2 integration, Docker Compose v2, and sufficient
Docker memory (at least 4 GB; 8 GB is more comfortable). Clone this repository,
then run the following from its root. Only copy `.env.example` when `.env` does
not already exist, and edit `AIRFLOW_UID` to match `id -u` on WSL.

```bash
mkdir -p dags sql config logs tests
cp -n .env.example .env
id -u
```

After setting `AIRFLOW_UID` in `.env`, initialize once:

```bash
docker compose up airflow-init
docker compose up -d
docker compose ps
```

`airflow-init` should exit successfully. Long-running services should be up.
The warehouse mounts `sql/` as initialization scripts; these run only on a new
empty warehouse volume. For an existing volume that needs the table created:

```bash
docker compose exec -T warehouse psql -v ON_ERROR_STOP=1 -U warehouse -d air_quality < sql/create_tables.sql
```

The Compose setup is for a local assignment/demo. Its default credentials and
empty Fernet key require hardening before production use. Repeated init can
encounter an existing-admin-user error; do not delete volumes to fix this.
Inspect `docker compose logs airflow-init` and create the admin only if absent.

### Connections and access

Open http://localhost:8080 and log in with the local demo account `airflow` /
`airflow`. Although the service is named `airflow-apiserver`, its command is
`webserver`, as appropriate for this Airflow 2.10.4 stack. LocalExecutor executes
tasks in scheduler-managed local processes; no separate Celery worker is needed.

Create these in **Admin → Connections** on a new installation:

| Connection ID | Type | Host | Database/schema | User | Port |
| --- | --- | --- | --- | --- | ---: |
| `openaq_api` | HTTP | `api.openaq.org` | — | — | — |
| `air_quality_warehouse` | Postgres | `warehouse` | `air_quality` | `warehouse` | 5432 |

For `openaq_api`, enter the real key privately in Extra as
`{"X-API-Key": "YOUR_PRIVATE_KEY"}`. For the warehouse, use the local demo
password `warehouse`. Do not commit a real key or export real Connections to Git.
The warehouse is reachable from WSL on **localhost:5434**; containers use
**warehouse:5432**. The old Compose comment mentioning host port 5433 is stale.

### Trigger and inspect

Unpause the DAG, then trigger the default 30-day run:

```bash
docker compose exec airflow-scheduler airflow dags unpause nairobi_air_quality
docker compose exec airflow-scheduler airflow dags trigger nairobi_air_quality
```

For the larger demonstration window:

```bash
docker compose exec airflow-scheduler airflow dags trigger nairobi_air_quality --conf '{"lookback_days": 365}'
```

Airflow's normal `dag_run_conf_overrides_params` setting permits the trigger
configuration to override `lookback_days`. Check task logs for the actual dates.

Open Graph for dependencies, Mapped Tasks for individual map indexes, and Logs
for counts, errors and retries. One initial attempt plus three retries means
up to **four attempts**, not four retries.

```bash
docker compose exec -T warehouse psql -U warehouse -d air_quality < sql/data_quality.sql
```

### Run the saved tests

Tests use the existing scheduler container and its installed Airflow libraries.
They mock HTTP and Connection lookup, so they need neither an API key nor a
running warehouse. Copy the tests because the current Compose file mounts
`dags/`, `logs/`, and `config/`, but does not mount `tests/`.

```bash
docker compose exec airflow-scheduler mkdir -p /tmp/nairobi-project-tests
docker compose cp ./tests/. airflow-scheduler:/tmp/nairobi-project-tests/
docker compose exec -T airflow-scheduler python -m unittest discover -s /tmp/nairobi-project-tests -p 'test_*.py' -v
```

Verified on 22 October 2026: all 17 tests passed in the Airflow scheduler container in 0.069 seconds. See [test results](docs/test-results.md).

## 1. Architecture

OpenAQ is the source, Airflow orchestrates the Python tasks, and the warehouse
stores analyst-facing data. The separate metadata Postgres stores DAG runs,
task states, scheduling information, Connections and XComs. Keeping business
data separate protects orchestration from analytics queries and allows each
database to be managed independently.

The seven tasks implement the five required stages plus two flatten steps:
`extract_locations → extract_sensors → flatten_sensors → extract_measurements
→ flatten_measurements → validate_measurements → load_to_postgres`.
See [the architecture diagram](docs/architecture.md).

A location represents a monitoring site. A location can contain multiple
sensors; each sensor measures a parameter. Measurements contain values, units,
and timestamps. The extractor uses `period.datetimeTo.utc` as `measured_at`,
which represents the reported period end. It does not independently enforce
that every source reading is hourly.

## 2. Why batch processing

Each DAG run asks: “What measurements are available for sensors around Nairobi
within a bounded time window ending at this run's data interval end?” The
source is periodically queried over HTTP and rows are loaded as a batch.
Daily monitoring does not require an always-running Kafka streaming system.
Airflow coordinates finite work; it is not the measurement database or a stream broker.

## 3. Dynamic task mapping

`extract_sensors.expand(location=locations)` creates one instance per discovered
location. Different locations have different sensors. Each mapped instance
returns a list; `flatten_sensors` converts the list of lists into individual
sensor dictionaries. `extract_measurements.expand(sensor=sensors_flat)` then
creates one task per sensor. `flatten_measurements` collects the results before
validation. TaskFlow return values travel through XCom.

Runtime mapping avoids hard-coding station/sensor IDs. Mapped tasks need not run
concurrently: `max_active_tis_per_dag=1` serializes measurement instances to
reduce pressure on OpenAQ. Sensor-discovery tasks currently remain concurrent.

## 4. Connections and credentials

`BaseHook.get_connection("openaq_api")` reads the host and API-key header from
Airflow. `PostgresHook(postgres_conn_id="air_quality_warehouse")` obtains the
warehouse connection. Keeping credentials out of Python and Git makes rotation
possible without changing DAG code. Connections still require restricted UI
access and appropriate encryption/secret management for production.

## 5. Idempotency and warehouse design

`PRIMARY KEY (sensor_id, measured_at)` identifies the reading represented by a
sensor and period end. `INSERT ... ON CONFLICT ... DO UPDATE` updates value,
unit, location name and `loaded_at`, rather than inserting another copy.
A transaction commits the batch together. A rerun after an interrupted load can
therefore safely reapply the data.

Observed proof: counts before and after the 365-day rerun were both **9,938**.
All rows' `loaded_at` values changed to the rerun load time, **12:19 UTC**.
The later duplicate check returned no rows. This demonstrates that existing keys
were updated; count alone would not prove that the task processed the data.
`loaded_at` is last-load/update time, not immutable first-ingestion time.

New data or source corrections can change a later run's contents; idempotency
means repeated keys do not multiply, not that a live source can never add rows.
The window is anchored to `data_interval_end`, so re-executing the same run uses
the same dates. Fresh manual runs may share or differ in inferred intervals.

## 6. Reliability and failure handling

Requests use a 30-second timeout. HTTP errors and timeouts raise exceptions,
which let Airflow retry the task. A 429 has bounded in-request waiting and retry.
Tasks allow three retries with a two-minute base delay, exponential backoff,
and a 15-minute maximum retry delay. `max_active_runs=1` prevents overlapping
runs of this DAG. Measurement pages request up to 1,000 rows, with about 1.1
seconds of successful-call pacing.

The first implementation hit 429 rate limiting and an account suspension that
returned 401. After the suspension was lifted, the extraction design introduced
time windows, larger pages, target-pollutant filtering, pacing, serialization
and bounded 429 handling. The controlled invalid-key test is separate evidence:
its four attempts all returned 401, then the run failed. Restoring the real key
allowed a subsequent run to succeed.

The warehouse-outage experiment failed at database connection lookup, entered
retry, and succeeded on attempt four after PostgreSQL restarted, upserting
523 batch rows. That is a batch count, not the warehouse total.
See [experiment evidence](docs/evidence.md).

## 7. Data quality

Validation rejects missing (`None`) sensor IDs/timestamps, null/non-numeric or
boolean values, duplicates within a batch, and values outside configured
pollutant bounds. For target pollutants those bounds also reject NaN/infinity.
It logs extracted, valid and rejected counts. The controlled fixture test
accepted one real reading and rejected null, negative and duplicate variants:
`extracted=4 valid=1 rejected=3`.

The numeric bounds are configurable engineering sanity rules, not medical or
regulatory thresholds. Current bounds are PM2.5/PM10 0–1,000; NO₂/O₃/SO₂ 0–2,000;
CO 0–50,000. Units must be checked before extending these rules to different
sensor units. Negative values are deliberately rejected for this assignment;
other scientific workflows may preserve flagged values for analysis.

Location/sensor IDs, names, parameter, unit, value and measurement time support
analysis. `loaded_at` provides loading lineage. `TIMESTAMPTZ` represents an
instant; SQL explicitly sets UTC for consistent displayed dates.

## 8. Scheduling and scalability

The DAG uses `@daily`, `catchup=False`, and one active run. Scheduled intervals
are daily in the current UTC environment. The default rolling 30-day lookback
intentionally rereads older data to accommodate delayed/irregular reporting;
upserts handle overlap. `lookback_days=1` narrows the extraction to one day.

Moving to hourly with the same rolling window would multiply discovery work
and reread much of the same history up to 24 times per day. It would not imply
24 times as many unique warehouse rows. Evaluate runtime, provider quotas,
reporting freshness and API pagination before making that change. The present
integer-day parameter cannot express a one-hour window. An hourly version
should use `data_interval_start` and `data_interval_end`, with an intentional
overlap for late readings, or a successful watermark advanced only after the
load commits. Per-sensor watermarks accommodate irregular reporting; retain
periodic backfills for late historical corrections.

Across Africa, task count, HTTP volume, XCom size and warehouse size would grow.
Use a shared quota limiter across all API callers, batched mapping, shared object
storage for measurement payloads, indexes/partitioning based on queries, bulk
loading, and incremental windows. Map lightweight references instead of large
measurement lists. Do not increase parallelism without considering source quotas.

## Known limitations and follow-up improvements

These are limitations of the demonstrated version, not claims of implemented fixes:

- Only measurements paginate. Location discovery requests the first 100 locations;
  sensor discovery also reads one response page. The captured 16-location Nairobi
  run fits within the location limit; larger coverage needs pagination everywhere.
- The measurement loop caps at 30 pages per sensor and currently does not fail
  if the last allowed page is full. Add an explicit completeness guard before
  using broad windows or dense sensors.
- Successful-call sleeps are per process. Parallel sensor discovery and other
  clients can still exhaust shared quotas. Proactive remaining-quota checks and
  correct handling of reset/retry headers should use the provider's current contract.
- Validation does not parse or normalize timestamps, reject blank timestamps,
  validate every metadata field, or interpret bounds by unit. Equivalent timestamp
  strings can evade in-memory deduplication even though Postgres normalizes instants.
- Large payloads are returned through XCom and logged as return values; use shared
  storage references and suppress bulk return logging as volume grows.
- Dependency installation uses a broad requests range and the image's bundled
  providers; a built image with tested version locks would improve reproducibility.
- Invalid-key 401 responses currently consume task retries. A future version can
  fail fast on permanent credential errors while retrying transient faults.

## References

- [Airflow 2.10.4 Docker guide](https://airflow.apache.org/docs/apache-airflow/2.10.4/howto/docker-compose/index.html)
- [Airflow 2.10.4 dynamic task mapping](https://airflow.apache.org/docs/apache-airflow/2.10.4/authoring-and-scheduling/dynamic-task-mapping.html)
- [Airflow 2.10.4 task states](https://airflow.apache.org/docs/apache-airflow/2.10.4/core-concepts/tasks.html)
- [OpenAQ documentation](https://docs.openaq.org/)
- [PostgreSQL INSERT and ON CONFLICT](https://www.postgresql.org/docs/16/sql-insert.html)
