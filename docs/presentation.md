# Live demonstration and speaker notes

Target: 7–10 minutes plus the time needed for a real rerun. Use the existing
working installation. Explain decisions and show evidence rather than reading
Python line by line. All four failure experiments have already been performed;
show their captured evidence instead of waiting through backoff during the talk.

## Before presenting

- Start Docker Desktop and run `docker compose up -d`.
- Confirm the real key is restored and the warehouse is accepting connections.
- Open the README diagram, successful Graph, mapped instances and SQL terminal.
- Keep evidence images ready for the authentication and database failures.
- Run the saved tests and retain the real output.
- Choose the successful 365-day run if demonstrating exact batch reloading.
- Avoid overlapping scheduled/manual work while comparing row counts.

## 1. Problem and architecture — about 60 seconds

“Analysts were retrieving Nairobi air-quality data manually. I built a daily
batch pipeline to discover stations and sensors, collect available readings,
validate them, and load a PostgreSQL warehouse that can safely be updated again.”

Show `architecture.md`. Explain that Airflow's metadata database stores task
state, while the separate warehouse stores business measurements. LocalExecutor
runs task processes; this setup does not need a separate Celery worker.

## 2. Source and mapping — about 60 seconds

Show the chain location → sensors → measurements. A location can contain several
sensors, so neither station IDs nor sensor counts are hard-coded. Show Graph:
16 mapped discovery instances and 22 mapped measurement instances. Open Mapped
Tasks and one instance's log. Explain flatten steps and XCom return values.

“Mapping creates runtime task instances; I deliberately serialize measurement
instances to reduce API pressure.”

## 3. Results and validation — about 60 seconds

Run `sql/data_quality.sql`. Explain 9,938 is the historical captured total;
new runs may legitimately add records. Show real station names, parameter, units
and measurement time ranges. Explain that PM10 and NO2 sensors were queried
but returned no records for the documented window. Show validation counts.

## 4. Live idempotency — allow the actual run time

The rubric requires a live comparison. Record counts immediately before and
after the repeat operation; do not substitute historical screenshots alone.

```bash
docker compose exec warehouse psql -U warehouse -d air_quality -c "SELECT COUNT(*) AS total_rows, MIN(loaded_at), MAX(loaded_at) FROM raw_air_quality;"
```

For an exact repeat load without new API calls, select the **existing successful
365-day run**, click its `load_to_postgres` instance and clear **only that task**.
In the confirmation, exclude upstream tasks, other runs, past/future runs and
other date ranges. Keep the DAG unpaused and let the scheduler execute the task.
It reloads the same validated XCom batch. Show its new load log and rerun the SQL.
This is an exact batch-load demonstration, not a new full API extraction.

Also show the completed full-pipeline rerun evidence (9,938 before and after).
If the assessor requests a fresh full pipeline rerun, trigger it with the same
lookback and inspect the logged date boundaries, then compare counts after it
finishes. Live API updates can add new keys even when upserts are correct.

```bash
docker compose exec airflow-scheduler airflow dags trigger nairobi_air_quality --conf '{"lookback_days": 365}'
docker compose exec warehouse psql -U warehouse -d air_quality -c "SELECT sensor_id, measured_at, COUNT(*) FROM raw_air_quality GROUP BY 1, 2 HAVING COUNT(*) > 1;"
```

Wait for the run to complete before interpreting the after-count. Explain the
composite primary key and `ON CONFLICT`: same-key rows update, rather than multiply.
`loaded_at` records the latest load, so it can change on an idempotent rerun.

## 5. Failure evidence — about 90 seconds

Show the invalid-key 401, retry state and fourth-attempt failure. Explain why
retries did not fix a bad credential and why the restored key did.
Show the warehouse hostname error and attempt-four recovery with 523 batch rows.
Show the controlled validation output: one accepted, three rejected.

“The important property is that a failed task can retry safely, and an
unrecoverable failure is visible rather than silently dropping data.”

## 6. Schedule and limitations — about 60 seconds

Explain daily scheduling, disabled catchup and one active run. The default
30-day lookback intentionally overlaps previous runs. Hourly scheduling should
use narrower intervals and late-arrival overlap, rather than rereading 30 days
24 times per day. Larger coverage needs shared quota controls, full discovery
pagination, a page-cap completeness guard, unit-aware validation and shared
storage references for large payloads.

## Questions to rehearse

- Why batch? Periodic HTTP retrieval and finite workloads fit the requirement.
- Why dynamic mapping? Stations and sensors differ and are discovered at runtime.
- What prevents duplicates? Sensor/time primary key plus transactional upsert.
- Can a 2 AM failure rerun at 8 AM? Yes, repeated keys update safely; the run's interval fixes dates.
- Why only PM2.5? Other discovered target sensors returned no rows in that window.
- What is `loaded_at`? Last insert/update time, not measurement time or immutable first ingestion.
- What does the validation test prove? Rejection behavior and counts, independently of the database.
- What do the unit tests not prove? Live API availability, scheduler retry timing, or database idempotency.
- How do rate limits affect scale? Discovery concurrency and all clients sharing a key must be considered.
