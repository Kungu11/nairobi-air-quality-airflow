# Evidence and experiment results

Evidence was collected on 21–22 October 2026. Airflow log timestamps below are
UTC; Nairobi time is UTC+3. Screenshots come from more than one run, identified
separately. They must not be described as one single execution.

## Successful pipeline, mapping, and warehouse checks

- [Successful Graph with 16 location instances and 22 sensor instances](evidence/successful-graph.png).
- [Individual mapped sensor-discovery instances](evidence/mapped-sensor-instances.png).
- [9,938 rows, five stations, five sensors, one parameter](evidence/total-coverage.png).
- [Counts and time ranges by station, parameter and unit](evidence/station-parameter-coverage.png).
- [Duplicate query: zero rows](evidence/duplicate-check.png).
- [Stored-data null and negative checks: all zero](evidence/stored-data-check.png).

The individual mapped-instance screenshot is from a scheduled run. The original
365-day validation log is from `manual__2026-10-21T11:09:14+00:00`:

```text
2026-10-21 12:05:15.251 UTC
validate_measurements: extracted=9938 valid=9938 rejected=0
```

Sensor extraction logs for that manual run report 22 sensors across 16 locations:
16 PM2.5, five PM10 and one NO2. Five PM2.5 sensors returned 9,938 rows; all other
target sensors returned zero rows for the requested window. The Graph screenshot
already shows both mapped-task counts. A separate measurement-instance table
screenshot is optional additional evidence and is not included in this package.

## Scenario 1: invalid API key

Run: `invalid_key_test_01`, 22 October 2026.

| Attempt | Started (UTC) | Observed result |
| --- | --- | --- |
| 1 | 09:27:44 | HTTP 401; `UP_FOR_RETRY` at 09:27:47 |
| 2 | 09:31:09 | HTTP 401; `UP_FOR_RETRY` at 09:31:10 |
| 3 | 09:38:53 | HTTP 401; `UP_FOR_RETRY` at 09:38:54 |
| 4 | 09:49:32 | HTTP 401; `FAILED` at 09:49:33 |

Diagnosis: the intentionally invalid key caused `requests.raise_for_status()`
to raise `HTTPError: 401 Unauthorized` during `extract_locations`. Retrying
could not repair the credential. The task and DAG run failed after the initial
attempt plus three retries. Dependent tasks became `upstream_failed`.
Restoring the real key allowed a subsequent run to discover 16 locations and
complete successfully. Its screenshot identifies the new run at 10:02:18 UTC.

- [HTTP 401 and retry log](evidence/invalid-key-http-401.png).
- [Waiting for retry](evidence/invalid-key-up-for-retry.png).
- [Fourth attempt failed](evidence/invalid-key-attempt-four-failed.png).
- [Failed run and final error](evidence/invalid-key-failed-run.png).
- [Success after restoring credentials](evidence/restored-key-success.png).

This is separate from the September account-suspension incident. A historical
suspension-related 401 and an intentionally invalid-key 401 have different causes.

## Scenario 2: warehouse stopped mid-run

Run: `manual__2026-10-22T08:24:25+00:00`.

The warehouse container was stopped while the run was in progress. Extraction
and validation completed, but `load_to_postgres` failed at `hook.get_conn()`:

```text
psycopg2.OperationalError: could not translate host name "warehouse" to address:
Name or service not known
```

The stopped container's hostname was unavailable to the connection attempt.
Airflow marked the task `UP_FOR_RETRY`. The first three attempts failed. After
`docker compose start warehouse`, `pg_isready` reported accepting connections.
Attempt four completed at about 08:42:26 UTC and logged:

```text
load_to_postgres: upserted 523 rows
```

The task and run recovered. The 523 rows were this run's batch, not the warehouse
total, and can include updates to previously loaded keys.

- [Container stop, restart and readiness](evidence/warehouse-stop-and-restart.png).
- [Actual connection error](evidence/warehouse-connection-error.png).
- [Retry state](evidence/warehouse-up-for-retry.png).
- [Successful fourth-attempt load](evidence/warehouse-retry-loaded-523.png).
- [Task success with attempt history](evidence/warehouse-attempt-four-success.png).

## Scenario 3: null/negative measurement and duplicate

A direct call to the actual DAG's validation callable was run in the scheduler
container at 08:15:16 UTC on 22 October. Input was one real normalized Kitisuru
measurement plus controlled null, negative and duplicate variants. Terminal
output supplied by the author was:

```text
validate_measurements: extracted=4 valid=1 rejected=3
PASS: valid measurement retained.
PASS: null value rejected.
PASS: negative value rejected.
PASS: duplicate rejected.
SUMMARY: extracted=4 valid=1 rejected=3
```

This was a function-level controlled test, not a scheduled DAG run. No invalid
records were written to the warehouse. The repeatable version is included in
`tests/test_pipeline.py`; run the packaged suite and save its actual output.
The supplied screenshot set does not include a screenshot of this direct test.

## Scenario 4: plain rerun and idempotency

The author's recorded before/after observations for the 365-day rerun were:

| Observation | Before | After |
| --- | --- | --- |
| Warehouse row count | 9,938 | 9,938 |
| `loaded_at` | approximately 12:05:18–12:07:26 UTC | all rows approximately 12:19:00 UTC |

The load rewrote existing keys without increasing their count. A subsequent
[duplicate query returned zero rows](evidence/duplicate-check.png). These
before/after figures are author-recorded observations; separate screenshots of
both original counts were not supplied in this package. The live demo should
repeat the count comparison and show the load log, not merely a green DAG.

## Additional reliability history

The author reports a run recovering after a machine shutdown, and a September
429/account-suspension incident followed by provider reinstatement. These are
background history; the controlled October screenshots above provide the
reviewable failure evidence included here.

## Final evidence still to record locally

1. Completed: all 17 tests passed; see test-results.md.
2. Live idempotency before/after counts and explanation during presentation.
3. GitHub repository URL after pushing. For a private repo, give the assessor access.
