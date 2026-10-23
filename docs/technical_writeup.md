# Building a rerunnable Nairobi air-quality batch pipeline

David Kungu · LuxDevHQ Cohort 7

## Problem and design

Manual API retrieval makes environmental analysis repetitive and difficult to
reproduce. I built a daily Airflow pipeline that discovers Nairobi monitoring
locations and their sensors, retrieves measurements for a bounded period,
validates the records, and loads a separate PostgreSQL warehouse.

Airflow 2.10.4 uses TaskFlow tasks with LocalExecutor. Discovery maps one task
per location, then a flatten step produces a sensor list for the second mapped
stage. A final flatten step collects measurements for validation and loading.
The metadata database remains separate from the measurement warehouse.

## Extraction and the rate-limit redesign

The first implementation encountered HTTP 429 and a provider suspension that
returned 401. This exposed the cost of small pages, broad historical retrieval
and parallel API calls. The revised pipeline requests a bounded time window,
uses 1,000-row measurement pages, filters to six target pollutants, serializes
measurement tasks, pauses after successful requests and waits on 429 responses.
Task-level retries and exponential backoff handle raised errors and timeouts.
This reduces API pressure but is not a shared quota guarantee: concurrent
sensor discovery still needs stronger coordination for larger deployments.

## What the live run found

The documented 365-day run discovered 16 locations and 22 target sensors.
Five PM2.5 sensors returned 9,938 readings. Five PM10 sensors and one NO2 sensor
were queried but returned no measurements for the requested window. Other
PM2.5 sensors were also empty. This explains why the warehouse contains only
PM2.5 despite discovery supporting multiple pollutants. It does not establish
that those pollutants have no measurements outside the chosen window.

Kihumo returned 7,494 rows across eight pages, demonstrating pagination.
Validation accepted all 9,938 extracted records. The station/parameter SQL
confirmed real Nairobi locations and units of µg/m³.

## Safe repetition and controlled failure

The table primary key `(sensor_id, measured_at)` and `ON CONFLICT DO UPDATE`
prevent duplicate keys. The row count remained 9,938 after the documented rerun,
while `loaded_at` changed, demonstrating an actual update of existing rows.
A later duplicate query returned zero rows. This supports safe retries after
interruption without manually cleaning the warehouse.

Four scenarios were demonstrated. An invalid key produced 401 on all four
attempts before the DAG failed; restoring the key allowed another successful
run. Stopping the warehouse caused hostname resolution failure in the load
task. After PostgreSQL restarted, attempt four upserted 523 batch rows. A direct
validation test accepted one real record and rejected null, negative and
duplicate variants, logging `extracted=4 valid=1 rejected=3`. Repeating the
pipeline retained the warehouse key count.

## Tradeoffs and next steps

The rolling 30-day default deliberately rereads history to pick up delayed
measurements. An hourly schedule should narrow the window and account for late
arrivals rather than rereading 30 days every hour. For continental coverage,
I would use shared quota enforcement, incremental per-sensor watermarks, object
storage references instead of large XCom payloads, and bulk warehouse loads.

Before scaling, the current implementation also needs completeness checks at
the measurement page cap, pagination for discovery, timestamp normalization and
unit-aware validation. These are documented limitations of the tested version.
The repeatable suite is provided in `tests/`; its actual container-run result
must be captured before submission. The live experiments demonstrate scheduler
behavior and warehouse idempotency beyond what mocked tests can establish.
