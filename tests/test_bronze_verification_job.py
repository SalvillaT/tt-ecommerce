"""Local contract tests for the isolated, read-only DEMO-001 verifier."""

from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "verify_bronze_demo_001.py"
JOB_YML = (ROOT / "resources" / "bronze_verification_job.yml").read_text(encoding="utf-8")
BUNDLE_YML = (ROOT / "databricks.yml").read_text(encoding="utf-8")


def load_verifier():
    spec = importlib.util.spec_from_file_location("verify_bronze_demo_001", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class FakeRow:
    def __init__(self, values):
        self.values = values

    def asDict(self, recursive=True):
        assert recursive is True
        return self.values


class FakeFrame:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return [FakeRow(row) for row in self.rows]


class FakeSpark:
    def __init__(self, responses):
        self.responses = responses
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)
        upper = statement.upper()
        for forbidden in ("INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "ALTER",
                          "OPTIMIZE", "VACUUM", "WRITE", "STREAM"):
            if forbidden in upper:
                raise AssertionError(f"mutating SQL submitted: {statement}")
        return FakeFrame(self.responses[statement])


class BronzeVerificationScriptTests(unittest.TestCase):
    def setUp(self):
        self.verifier = load_verifier()

    def _responses(self):
        verifier = self.verifier
        events = [
            {"attempt_id": "attempt-1", "pipeline_run_id": "run-1", "event": "STARTED", "source": None,
             "event_at": "2026-11-10T10:00:00Z", "files_discovered": 0, "rows_declared": 0,
             "rows_read": 0, "metrics_observed": False, "error_class": None},
            {"attempt_id": "attempt-1", "pipeline_run_id": "run-1", "event": "MANIFEST_VALIDATED", "source": None,
             "event_at": "2026-11-10T10:00:01Z", "files_discovered": 3, "rows_declared": 44,
             "rows_read": 0, "metrics_observed": False, "error_class": None},
            *[
                {"attempt_id": "attempt-1", "pipeline_run_id": "run-1", "event": "SOURCE_COMMITTED", "source": source,
                 "event_at": f"2026-11-10T10:00:0{index + 2}Z", "files_discovered": 0, "rows_declared": 0,
                 "rows_read": expected["raw_rows"], "metrics_observed": True, "error_class": None}
                for index, (source, expected) in enumerate(verifier.RAW_EXPECTATIONS.items())
            ],
        ]
        responses = {verifier.RUN_LOG_SQL: events}
        for source, expected in verifier.RAW_EXPECTATIONS.items():
            responses[verifier.raw_summary_sql(expected["table"])] = [{
                "batch_id": verifier.BATCH_ID,
                "_pipeline_run_id": "run-1",
                "raw_rows": expected["raw_rows"],
                "rescued_rows": expected["rescued_rows"],
                "corrupt_rows": expected["corrupt_rows"],
            }]
        responses[verifier.ORDERS_PARSE_STATUS_SQL] = [
            {"parse_status": status, "row_count": count}
            for status, count in verifier.EXPECTED_ORDERS_PARSE_STATUS.items()
        ]
        return responses

    def test_fixed_read_only_statements_produce_passing_machine_readable_report(self):
        spark = FakeSpark(self._responses())
        report = self.verifier.collect_report(spark)

        expected_statements = [
            *(self.verifier.raw_summary_sql(expected["table"])
              for expected in self.verifier.RAW_EXPECTATIONS.values()),
            self.verifier.RUN_LOG_SQL,
            self.verifier.ORDERS_PARSE_STATUS_SQL,
        ]
        self.assertEqual(spark.statements, expected_statements)
        self.assertEqual(report["batch_id"], "DEMO-001")
        self.assertTrue(report["gate"]["passed"], report["gate"]["fail_reasons"])
        self.assertEqual(report["gate"]["fail_reasons"], [])
        rendered = self.verifier.report_json(report)
        self.assertEqual(rendered, self.verifier.report_json(report))
        self.assertEqual(json.loads(rendered)["gate"]["passed"], True)
        self.assertNotIn("abfss://", rendered)

    def test_gate_reports_exact_count_failure_without_claiming_completion_from_metrics(self):
        responses = self._responses()
        orders_sql = self.verifier.raw_summary_sql("orders_raw")
        responses[orders_sql][0]["raw_rows"] = 27
        report = self.verifier.collect_report(FakeSpark(responses))

        self.assertFalse(report["gate"]["passed"])
        failed = {check["name"] for check in report["gate"]["checks"] if not check["passed"]}
        self.assertEqual(failed, {"orders_raw_exact_counts"})
        self.assertNotIn("rows_written", self.verifier.RUN_LOG_SQL)

    def test_unknown_raw_table_is_rejected_without_spark(self):
        with self.assertRaises(ValueError):
            self.verifier.raw_summary_sql("orders_raw; DELETE FROM x")


class BronzeVerificationJobBundleTests(unittest.TestCase):
    def test_verification_job_is_isolated_manual_and_read_only(self):
        self.assertIn("name: tt-ecommerce-bronze-demo-001-verification-read-only", JOB_YML)
        self.assertIn("max_concurrent_runs: 1", JOB_YML)
        self.assertIn("max_retries: 0", JOB_YML)
        self.assertIn("timeout_seconds: 900", JOB_YML)
        self.assertIn("service_principal_name: ${var.run_as_service_principal_name}", JOB_YML)
        self.assertIn("scripts/verify_bronze_demo_001.py", JOB_YML)
        self.assertIn("source: WORKSPACE", JOB_YML)
        for forbidden in ("schedule:", "continuous:", "parameters:", "run_bronze_job.py"):
            self.assertNotIn(forbidden, JOB_YML)

    def test_verifier_is_synced_and_has_serverless_and_classic_target_wiring(self):
        self.assertIn("- scripts/verify_bronze_demo_001.py", BUNDLE_YML)
        self.assertIn("bronze_demo_001_verification_job:", BUNDLE_YML)
        self.assertIn("task_key: bronze_demo_001_verification", BUNDLE_YML)
        self.assertIn("job_cluster_key: bronze_verification_classic", BUNDLE_YML)


if __name__ == "__main__":
    unittest.main()
