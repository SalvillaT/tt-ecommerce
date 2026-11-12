"""Focused dependency-free tests for the portfolio Silver/Gold core."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import unittest

from pipelines.silver import run_pipeline


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "data" / "sample" / "DEMO-001"
NOW = datetime(2026, 1, 20, 12, tzinfo=timezone.utc)


def _audit(source: str, payload: dict | None = None, *, status: str = "PARSED", corrupt: str | None = None) -> dict:
    name = {"orders": "orders_DEMO-001.jsonl", "customers": "customers_DEMO-001.csv", "products": "products_DEMO-001.csv"}[source]
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")) if payload else ""
    return {"batch_id": "DEMO-001", "contract_version": "v1", "manifest_file_sha256": "fixture-sha",
            "_source_file": f"abfss://landing/{source}/{name}", "_source_file_size": 1,
            "_source_file_modification_time": "2026-01-10T00:00:00Z", "_ingested_at": "2026-01-10T01:00:00Z",
            "_pipeline_run_id": "test-run", "parsed_payload_canonical": canonical,
            "_rescued_data": None, "_corrupt_record": corrupt, "parse_status": status, "ingest_date": "2026-01-10"}


def _inputs():
    orders = []
    for line in (FIXTURE / "orders_DEMO-001.jsonl").read_text("utf-8").splitlines():
        try:
            payload = json.loads(line)
            status = "RESCUED" if payload.get("order_id") == "ORD-EXTRA-01" else "PARSED"
            row = {**payload, **_audit("orders", payload, status=status)}
        except json.JSONDecodeError:
            row = _audit("orders", status="MALFORMED", corrupt=line)
        orders.append(row)
    def csv_rows(name, source):
        with (FIXTURE / name).open(encoding="utf-8", newline="") as handle:
            return [{**row, **_audit(source, row)} for row in csv.DictReader(handle)]
    return orders, csv_rows("customers_DEMO-001.csv", "customers"), csv_rows("products_DEMO-001.csv", "products")


class SilverCore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.inputs = _inputs()
        cls.result = run_pipeline(orders=cls.inputs[0], customers=cls.inputs[1], products=cls.inputs[2], audit_timestamp=NOW)

    def test_controls_and_published_cell(self):
        self.assertEqual(dict(self.result.pre_suppression_controls), {
            "total_revenue_eur": Decimal("812.20"), "order_count": 13, "refunded_line_count": 2})
        self.assertEqual(len(self.result.gold), 1)
        row = self.result.gold[0]
        self.assertEqual((row.business_date.isoformat(), row.category, row.region_code, row.channel), ("2026-01-10", "paddle", "ES-MD", "web"))
        self.assertEqual((str(row.total_revenue_eur), row.order_count, row.refunded_line_count), ("640.00", 5, 0))
        self.assertIsInstance(row.total_revenue_eur, Decimal)
        self.assertIn(("2026-01-11", "rubber", "ES-MD", "web"), {(x.business_date.isoformat(), x.category, x.region_code, x.channel) for x in self.result.suppressed_cells})

    def test_known_quality_scenarios(self):
        order_ids = {r.as_dict()["order_id"] for r in self.result.orders}
        self.assertEqual(len(order_ids), 15)
        self.assertIn("ORD-EXTRA-01", order_ids)
        self.assertNotIn("ORD-CONF-01", order_ids)
        self.assertNotIn("ORD-NULL-01", order_ids)
        self.assertNotIn("ORD-ORPH-01", order_ids)
        self.assertEqual(next(r for r in self.result.orders if r.as_dict()["order_id"] == "ORD-EXTRA-01").quality_status, "VALID_WITH_WARNINGS")
        self.assertEqual(next(r for r in self.result.customers if r.as_dict()["customer_id"] == "CUS-SOS-NOPHONE").quality_status, "VALID_WITH_WARNINGS")
        rules = {rule for q in self.result.quarantine for rule in q.rule_ids}
        self.assertTrue({"SCH-002", "ORD-002", "ORD-001", "CUS-001", "ORD-005"}.issubset(rules))
        self.assertEqual(sum(1 for r in self.result.orders if r.as_dict()["order_id"] == "ORD-DUP-01"), 1)
        self.assertEqual(dict(self.result.quality_report.by_rule)["ORD-006"], 1)
        self.assertEqual(dict(self.result.quality_report.by_rule)["CUS-002"], 5)

    def test_order_independence_and_gold_has_no_pii(self):
        reversed_result = run_pipeline(orders=reversed(self.inputs[0]), customers=reversed(self.inputs[1]), products=reversed(self.inputs[2]), audit_timestamp=NOW)
        self.assertEqual(self.result, reversed_result)
        rendered = json.dumps(self.result.gold[0].as_dict(), sort_keys=True)
        self.assertNotIn("customer", rendered)
        self.assertNotIn("order_id", rendered)
        self.assertNotIn("email", rendered)
        self.assertNotIn("phone", rendered)

    def test_master_equal_version_conflict_is_fail_closed(self):
        customers = list(self.inputs[1])
        duplicate = dict(customers[0])
        duplicate["region_code"] = "ES-XX"
        duplicate.update(_audit("customers", {k: duplicate[k] for k in ("customer_id", "record_version", "updated_at", "region_code", "email", "phone", "deleted_at")}))
        result = run_pipeline(orders=self.inputs[0], customers=customers + [duplicate], products=self.inputs[2], audit_timestamp=NOW)
        self.assertNotIn("CUS-001", {r.as_dict()["customer_id"] for r in result.customers})
        self.assertTrue(any(q.source == "customers" and "CUS-001" in q.rule_ids for q in result.quarantine))

    def test_unknown_parse_status_fails_closed(self):
        broken = list(self.inputs[0])
        broken[0] = {**broken[0], "parse_status": "MYSTERY"}
        with self.assertRaisesRegex(ValueError, "unknown parse_status"):
            run_pipeline(orders=broken, customers=self.inputs[1], products=self.inputs[2], audit_timestamp=NOW)


if __name__ == "__main__":
    unittest.main()
