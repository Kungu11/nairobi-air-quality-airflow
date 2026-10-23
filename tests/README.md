# Automated tests

These tests load the actual `dags/nairobi_air_quality.py` with Airflow installed.
They call its task functions and mock external HTTP requests and connection lookup.
They do not contact OpenAQ, write warehouse rows, or create scheduled task runs.

Run from the WSL project folder with the scheduler running:

```bash
docker compose exec airflow-scheduler mkdir -p /tmp/nairobi-project-tests
docker compose cp ./tests/. airflow-scheduler:/tmp/nairobi-project-tests/
docker compose exec -T airflow-scheduler python -m unittest discover -s /tmp/nairobi-project-tests -p 'test_*.py' -v
```

Expected: **17 tests**, followed by `OK`. Capture the real output before claiming
the full suite passed. Tests cover validation, log counts, discovery filtering,
empty responses, pagination and fixed windows, 401 propagation, timeout
propagation, 429 waits, DAG dependencies, mapping and retry settings.

`fixtures/kitisuru_normalized.json` is a real normalized record from the successful
manual run's validation log, not a downloaded raw API response. API-shaped
fixtures in extraction tests are synthetic. This distinction is intentional.

These are unit and DAG-definition tests. Live idempotency and scheduler retry
behavior are demonstrated separately in `docs/evidence.md`; mocked tests alone
do not prove either behavior.

Known gaps in the existing validator are documented in the README. These tests
verify the supported behavior; they do not establish full timestamp or unit validation.
