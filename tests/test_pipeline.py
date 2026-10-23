"""Run against the real DAG inside the project's Airflow 2.10.4 container.

External requests are mocked. No API key or warehouse is required by this suite.
The fixture is a real normalized record from the successful validation log.
"""
import importlib.util
import json
import os
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

import pendulum
import requests


DAG_PATH = Path(os.environ.get(
    "NAIROBI_DAG_PATH", "/opt/airflow/dags/nairobi_air_quality.py"
))
spec = importlib.util.spec_from_file_location("nairobi_pipeline_under_test", DAG_PATH)
pipeline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pipeline)
dag = pipeline.nairobi_air_quality()


def fixture():
    path = Path(__file__).parent / "fixtures" / "kitisuru_normalized.json"
    return json.loads(path.read_text(encoding="utf-8"))


class ValidationTests(unittest.TestCase):
    def setUp(self):
        self.validate = dag.get_task("validate_measurements").python_callable
        self.good = fixture()

    def test_real_measurement_is_accepted(self):
        self.assertEqual(self.validate([self.good]), [self.good])

    def test_null_and_negative_values_are_rejected(self):
        for value in (None, -5):
            with self.subTest(value=value):
                self.assertEqual(self.validate([{**self.good, "value": value}]), [])

    def test_missing_sensor_and_timestamp_are_rejected(self):
        for field in ("sensor_id", "measured_at"):
            with self.subTest(field=field):
                row = dict(self.good)
                row.pop(field)
                self.assertEqual(self.validate([row]), [])

    def test_non_numeric_and_boolean_values_are_rejected(self):
        for value in ("17.3", True, False):
            with self.subTest(value=value):
                self.assertEqual(self.validate([{**self.good, "value": value}]), [])

    def test_non_finite_values_are_rejected_for_target_pollutant(self):
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value):
                self.assertEqual(self.validate([{**self.good, "value": value}]), [])

    def test_duplicate_is_rejected_but_other_sensor_is_kept(self):
        other_sensor = {**self.good, "sensor_id": self.good["sensor_id"] + 1}
        self.assertEqual(
            self.validate([self.good, dict(self.good), other_sensor]),
            [self.good, other_sensor],
        )

    def test_pm25_configured_boundaries(self):
        for value, accepted in ((0, True), (1000, True), (1000.01, False)):
            with self.subTest(value=value):
                row = {**self.good, "value": value}
                self.assertEqual(self.validate([row]), [row] if accepted else [])

    def test_empty_input(self):
        self.assertEqual(self.validate([]), [])

    def test_rejection_counts_are_logged(self):
        rows = [
            self.good,
            {**self.good, "value": None, "measured_at": "2025-10-21T02:00:00Z"},
            {**self.good, "value": -5, "measured_at": "2025-10-21T03:00:00Z"},
            dict(self.good),
        ]
        with self.assertLogs(pipeline.logger, level="INFO") as logs:
            self.assertEqual(self.validate(rows), [self.good])
        self.assertTrue(any("extracted=4 valid=1 rejected=3" in line
                            for line in logs.output))


class ExtractionTests(unittest.TestCase):
    def test_sensor_discovery_filters_unwanted_parameters(self):
        # Synthetic API-shape fixture; the normalized measurement fixture is real.
        response = {"results": [
            {"id": 1, "parameter": {"name": "pm25", "units": "µg/m³"}},
            {"id": 2, "parameter": {"name": "pm10", "units": "µg/m³"}},
            {"id": 3, "parameter": {"name": "temperature", "units": "c"}},
        ]}
        function = dag.get_task("extract_sensors").python_callable
        with patch.object(pipeline, "_openaq_request", return_value=response):
            result = function({"id": 99, "name": "Test location"})
        self.assertEqual([row["parameter"] for row in result], ["pm25", "pm10"])
        self.assertTrue(all(row["location_id"] == 99 for row in result))

    def test_empty_measurements_are_handled(self):
        function = dag.get_task("extract_measurements").python_callable
        with patch.object(pipeline, "_openaq_request", return_value={"results": []}):
            result = function(fixture(), params={"lookback_days": 30},
                              data_interval_end=pendulum.datetime(2026, 10, 21, tz="UTC"))
        self.assertEqual(result, [])

    def test_pagination_keeps_time_window_and_reads_next_page(self):
        # Small page size exercises the real pagination loop without large fixtures.
        def raw(value, timestamp):
            return {"value": value, "parameter": {"name": "pm25", "units": "µg/m³"},
                    "period": {"datetimeTo": {"utc": timestamp}}}
        pages = [
            {"results": [raw(10, "2026-10-20T01:00:00Z"),
                         raw(11, "2026-10-20T02:00:00Z")]},
            {"results": [raw(12, "2026-10-20T03:00:00Z")]},
        ]
        function = dag.get_task("extract_measurements").python_callable
        with patch.object(pipeline, "PAGE_LIMIT", 2), patch.object(
                pipeline, "_openaq_request", side_effect=pages) as request:
            result = function(fixture(), params={"lookback_days": 1},
                              data_interval_end=pendulum.datetime(2026, 10, 21, tz="UTC"))
        self.assertEqual([row["value"] for row in result], [10, 11, 12])
        self.assertEqual(request.call_count, 2)
        for page, call in enumerate(request.call_args_list, start=1):
            self.assertEqual(call.kwargs["params"]["page"], page)
            self.assertEqual(call.kwargs["params"]["datetime_from"], "2026-10-20T00:00:00Z")
            self.assertEqual(call.kwargs["params"]["datetime_to"], "2026-10-21T00:00:00Z")


class RequestTests(unittest.TestCase):
    def setUp(self):
        connection = Mock(host="api.openaq.org", extra_dejson={"X-API-Key": "dummy-test-key"})
        self.connection_patch = patch.object(pipeline.BaseHook, "get_connection", return_value=connection)
        self.connection_patch.start()
        self.addCleanup(self.connection_patch.stop)

    def test_invalid_key_http_error_propagates(self):
        response = Mock(status_code=401)
        response.raise_for_status.side_effect = requests.HTTPError("401 Unauthorized")
        with patch.object(pipeline.requests, "get", return_value=response) as request:
            with self.assertRaises(requests.HTTPError):
                pipeline._openaq_request("/locations")
        self.assertEqual(request.call_count, 1)

    def test_timeout_propagates_instead_of_returning_partial_data(self):
        with patch.object(pipeline.requests, "get", side_effect=requests.Timeout("test timeout")):
            with self.assertRaises(requests.Timeout):
                pipeline._openaq_request("/locations")

    def test_429_waits_then_returns_success(self):
        limited = Mock(status_code=429, headers={"x-ratelimit-reset": "2"})
        success = Mock(status_code=200)
        success.json.return_value = {"results": []}
        with patch.object(pipeline.requests, "get", side_effect=[limited, success]) as request, \
                patch.object(pipeline.time, "sleep") as sleep:
            self.assertEqual(pipeline._openaq_request("/locations"), {"results": []})
        self.assertEqual(request.call_count, 2)
        sleep.assert_any_call(2)


class DagTests(unittest.TestCase):
    def test_task_chain_and_mapping(self):
        chain = ["extract_locations", "extract_sensors", "flatten_sensors",
                 "extract_measurements", "flatten_measurements",
                 "validate_measurements", "load_to_postgres"]
        self.assertEqual(set(dag.task_ids), set(chain))
        for upstream, downstream in zip(chain, chain[1:]):
            self.assertEqual(dag.get_task(upstream).downstream_task_ids, {downstream})
        from airflow.models.mappedoperator import MappedOperator
        self.assertIsInstance(dag.get_task("extract_sensors"), MappedOperator)
        self.assertIsInstance(dag.get_task("extract_measurements"), MappedOperator)

    def test_schedule_and_retry_controls(self):
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(dag.schedule_interval, "@daily")
        self.assertEqual(dag.get_task("load_to_postgres").retries, 3)
        self.assertTrue(dag.get_task("load_to_postgres").retry_exponential_backoff)


if __name__ == "__main__":
    unittest.main(verbosity=2)
