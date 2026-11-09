from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from pipelines.bronze.contracts import AUDIT_COLUMNS, BRONZE_CONTRACTS
from pipelines.bronze.ddl import (
    bootstrap_bronze_tables, bronze_table_ddls, managed_source_table_ddl,
    ownership_invariant, run_log_table_ddl,
)
from pipelines.bronze.job import BronzeJobRequest, preflight_request
from pipelines.bronze.layout import LandingLayout
from pipelines.bronze.manifest import (
    ManifestPreflightError,
    validate_manifest_batch,
    validate_ready_marker,
    validate_receipt_consistency,
    validate_source_directory,
)
from pipelines.bronze.run_log import RunLogEvent, RunMetrics
from pipelines.bronze.spark import (
    append_run_log, autoloader_options, execute_source_receipt,
    idempotency_limitations, provenance_columns,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "data" / "sample" / "DEMO-001"


def _fixture_copy(destination: Path) -> Path:
    """Copy only landing artifacts byte-for-byte; never normalize fixture bytes."""
    copied = destination / "DEMO-001"
    copied.mkdir()
    for name in (
        "orders_DEMO-001.jsonl", "customers_DEMO-001.csv",
        "products_DEMO-001.csv", "manifest_DEMO-001.json",
    ):
        shutil.copyfile(FIXTURE / name, copied / name)
    return copied


class BronzeCoreTests(unittest.TestCase):
    def test_layout_is_safe_and_exact(self):
        layout = LandingLayout()
        self.assertEqual(layout.final_file("orders", "DEMO-001", "2026-01-10"), "landing/orders/arrival_date=2026-01-10/batch_id=DEMO-001/orders_DEMO-001.jsonl")
        self.assertEqual(layout.final_directory("orders", "DEMO-001", "2026-01-10"), "landing/orders/arrival_date=2026-01-10/batch_id=DEMO-001")
        self.assertEqual(layout.ready("DEMO-001"), "landing/_control/ready/DEMO-001.json")
        self.assertEqual(layout.manifest("DEMO-001"), "landing/_control/manifests/DEMO-001/manifest_DEMO-001.json")
        self.assertIn("/checkpoints/DEMO-001", layout.checkpoint("orders", "DEMO-001"))
        self.assertIn("/pipeline_state/ingestion_state/orders/schema", layout.schema_location("orders"))
        for unsafe in ("../bad", "a/b", "", "white space"):
            with self.assertRaises(ValueError):
                layout.ready(unsafe)

    def test_fixture_manifest_receipts_and_counts(self):
        with tempfile.TemporaryDirectory() as temp:
            # Original fixture bytes are copied unchanged into the strict landing shape.
            receipts = validate_manifest_batch(_fixture_copy(Path(temp)), "DEMO-001")
            self.assertEqual([r.source for r in receipts], ["orders", "customers", "products"])
            self.assertEqual([r.rows_declared for r in receipts], [26, 11, 7])
            self.assertEqual(sum(r.rows_declared for r in receipts), 44)
            validate_receipt_consistency(receipts[0], {(receipts[0].batch_id, receipts[0].source): receipts[0].sha256})
            with self.assertRaises(ManifestPreflightError):
                validate_receipt_consistency(receipts[0], {(receipts[0].batch_id, receipts[0].source): "0" * 64})

    def test_manifest_rejects_modified_source_and_total(self):
        with tempfile.TemporaryDirectory() as temp:
            copied = _fixture_copy(Path(temp))
            orders = copied / "orders_DEMO-001.jsonl"
            orders.write_bytes(orders.read_bytes() + b"\n")
            with self.assertRaises(ManifestPreflightError):
                validate_manifest_batch(copied)
            orders.write_bytes((FIXTURE / "orders_DEMO-001.jsonl").read_bytes())
            manifest = copied / "manifest_DEMO-001.json"
            body = json.loads(manifest.read_text(encoding="utf-8"))
            body["total_rows"] = 0
            manifest.write_text(json.dumps(body), encoding="utf-8")
            with self.assertRaises(ManifestPreflightError):
                validate_manifest_batch(copied)

    def test_manifest_hardening_rejects_duplicate_bool_hash_and_filesystem_entries(self):
        with tempfile.TemporaryDirectory() as temp:
            copied = _fixture_copy(Path(temp))
            manifest_path = copied / "manifest_DEMO-001.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["files"].append(dict(manifest["files"][0]))
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaises(ManifestPreflightError):
                validate_manifest_batch(copied)
            manifest["files"] = manifest["files"][:3]
            manifest["files"][0]["rows"] = True
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaises(ManifestPreflightError):
                validate_manifest_batch(copied)
            manifest["files"][0]["rows"] = 26
            manifest["files"][0]["sha256"] = "not-a-sha"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaises(ManifestPreflightError):
                validate_manifest_batch(copied)
            (copied / "unexpected.txt").write_text("x", encoding="utf-8")
            with self.assertRaises(ManifestPreflightError):
                validate_manifest_batch(copied)

    def test_fixture_has_explicit_duplicate_drift_and_malformed_expectations(self):
        text = (FIXTURE / "orders_DEMO-001.jsonl").read_text(encoding="utf-8")
        self.assertEqual(text.count('"order_id":"ORD-DUP-01"'), 2)  # Bronze preserves both physical lines.
        self.assertIn('"source_campaign"', text)
        self.assertTrue(text.rstrip("\r\n").endswith('"record_version":1,'))

    def test_contracts_and_options_are_append_only_safe(self):
        layout = LandingLayout()
        self.assertEqual(set(BRONZE_CONTRACTS), {"orders", "customers", "products"})
        self.assertIn("_rescued_data", AUDIT_COLUMNS)
        self.assertIn("parsed_payload_canonical", AUDIT_COLUMNS)
        self.assertNotIn("raw_payload_canonical", AUDIT_COLUMNS)
        self.assertEqual(provenance_columns(), AUDIT_COLUMNS)
        for contract in BRONZE_CONTRACTS.values():
            self.assertIn("_corrupt_record", contract.bronze_columns)
            options = autoloader_options(contract, layout, "DEMO-001")
            self.assertEqual(options["cloudFiles.schemaEvolutionMode"], "rescue")
            self.assertEqual(options["mode"], "PERMISSIVE")
            self.assertEqual(options["cloudFiles.cleanSource"], "OFF")
            self.assertNotIn("mergeSchema", options)
            self.assertIn("/Volumes/", options["checkpointLocation"])
            self.assertIn("/pipeline_state/ingestion_state/", options["checkpointLocation"])
        self.assertIn("no checkpoint-loss guarantee", idempotency_limitations())

    def test_managed_ddl_is_pathless_partitioned_and_job_owned(self):
        self.assertIn("sole first creator and owner", ownership_invariant())
        ddls = bronze_table_ddls()
        self.assertEqual(len(ddls), 4)
        for contract in BRONZE_CONTRACTS.values():
            ddl = managed_source_table_ddl(contract)
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {contract.table}", ddl)
            self.assertIn("PARTITIONED BY (ingest_date)", ddl)
            self.assertIn("append-only", ddl)
            self.assertNotIn("LOCATION", ddl)
            self.assertNotIn("/Volumes/", ddl)
            for column in contract.data_columns:
                self.assertIn(f"`{column}` STRING", ddl)
        run_log_ddl = run_log_table_ddl()
        self.assertIn("tt_ecommerce_synthetic.bronze.ingestion_run_log", run_log_ddl)
        self.assertIn("`event_at` TIMESTAMP", run_log_ddl)
        self.assertNotIn("LOCATION", run_log_ddl)

    def test_spark_execution_interfaces_are_lazy(self):
        self.assertTrue(callable(bootstrap_bronze_tables))
        self.assertTrue(callable(append_run_log))
        self.assertTrue(callable(execute_source_receipt))

    def test_local_batch_directory_is_exact_and_stream_has_no_source_root(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            expected = directory / "orders_DEMO-001.jsonl"
            expected.write_bytes(b"{}\n")
            self.assertEqual(validate_source_directory(directory, "orders", "DEMO-001", "2026-01-10"), expected)
            (directory / "other.jsonl").write_text("{}", encoding="utf-8")
            with self.assertRaises(ManifestPreflightError):
                validate_source_directory(directory, "orders", "DEMO-001", "2026-01-10")
        self.assertFalse(hasattr(LandingLayout(), "source_root"))

    def test_ready_marker_is_required_control_artifact(self):
        with tempfile.TemporaryDirectory() as temp:
            marker = Path(temp) / "DEMO-001.json"
            marker.write_text('{"batch_id":"DEMO-001"}', encoding="utf-8")
            validate_ready_marker(marker, "DEMO-001")
            marker.write_text('{"batch_id":"OTHER"}', encoding="utf-8")
            with self.assertRaises(ManifestPreflightError):
                validate_ready_marker(marker, "DEMO-001")

    def test_run_log_requires_injected_timestamp_and_failure_class(self):
        metric = RunMetrics("a", "p", "DEMO-001", "orders", RunLogEvent.SOURCE_COMMITTED, "2026-01-01T00:00:00Z", rows_read=26, rows_written=26, rows_rescued=1, rows_malformed=1, metrics_observed=True)
        self.assertEqual(metric.as_dict()["event"], "SOURCE_COMMITTED")
        self.assertFalse(metric.as_dict()["metrics_observed"] is False and metric.rows_read > 0)
        with self.assertRaises(ValueError):
            RunMetrics("a", "p", "DEMO-001", "orders", RunLogEvent.FAILED, "2026-01-01T00:00:00Z")

    def test_orchestration_is_local_preflight_only(self):
        with tempfile.TemporaryDirectory() as temp:
            request = BronzeJobRequest("DEMO-001", "2026-01-10", _fixture_copy(Path(temp)))
            self.assertEqual(len(preflight_request(request)), 3)


if __name__ == "__main__":
    unittest.main()
