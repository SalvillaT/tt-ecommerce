"""Fake-Spark tests for the read-only Silver DEMO-001 evidence verifier.

The verifier must stay provably read-only (only fixed SELECT/DESCRIBE
statements), produce one redacted deterministic JSON report, and never invoke
materialization, raw-layer, landing, or checkpoint code.
"""

from __future__ import annotations

import importlib.util
import json
from datetime import date
from decimal import Decimal
from pathlib import Path
import sys
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
_VERIFIER_PATH = ROOT / "scripts" / "verify_silver_demo_001.py"


def _load_verifier():
    spec = importlib.util.spec_from_file_location("verify_silver_demo_001", _VERIFIER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeRow:
    def __init__(self, data):
        self._data = dict(data)

    def asDict(self, recursive=True):
        return dict(self._data)


class FakeFrame:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return [FakeRow(row) for row in self._rows]


class FakeSpark:
    """Rejects mutating SQL; answers only the verifier's fixed statements."""

    def __init__(self, responses):
        self.responses = responses
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)
        upper = statement.upper()
        for forbidden in ("INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP",
                          "ALTER", "OPTIMIZE", "VACUUM", "WRITE", "STREAM"):
            if forbidden in upper:
                raise AssertionError(f"mutating SQL submitted: {statement}")
        return FakeFrame(self.responses[statement])


class SilverVerifierTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.verifier = _load_verifier()

    def _responses(self):
        verifier = self.verifier
        responses = {
            verifier.ORDERS_CURRENT_SQL: [{"materialization_scope": "DEMO-001",
                                           "row_count": 15, "extra_rows": 1, "rejected_rows": 0}],
            verifier.CUSTOMERS_CURRENT_SQL: [{"materialization_scope": "DEMO-001",
                                              "row_count": 10, "nophone_rows": 1,
                                              "null_email_rows": 0}],
            verifier.PRODUCTS_CURRENT_SQL: [{"materialization_scope": "DEMO-001", "row_count": 7}],
            verifier.QUARANTINE_COUNTS_SQL: [{"materialization_scope": "DEMO-001",
                                              "rejection_rows": 6, "orders_rows": 5,
                                              "customers_rows": 1}],
            verifier.QUARANTINE_RULES_SQL: [
                {"rule_id": rule, "rule_rows": rows}
                for rule, rows in sorted(verifier.EXPECTED_QUARANTINE_RULE_ROWS.items())
            ],
            verifier.GOLD_PUBLISHED_CELLS_SQL: [{
                # Real Spark shapes: DATE as datetime.date and DECIMAL(18,2)
                # as Python Decimal — both normalized deterministically in the
                # gold evaluation.
                "business_date": date(2026, 1, 10), "category": "paddle",
                "region_code": "ES-MD", "channel": "web",
                "total_revenue_eur": Decimal("640.00"), "order_count": 5,
                "refunded_line_count": 0,
            }],
            verifier.MATERIALIZATION_REPORT_SQL: [{
                "materialization_scope": "DEMO-001", "batch_id": "DEMO-001",
                "status": "COMPLETED_WITH_QUARANTINE", "audit_timestamp": "2026-01-20T12:00:00Z",
                "recorded_at": "2026-01-20T12:00:05Z", "published_cells": 1,
                "suppressed_cells": 8, "duplicate_count": 1,
                "control_total_revenue_eur": "812.20", "control_order_count": 13,
                "control_refunded_line_count": 2,
            }],
            verifier.GOLD_SCHEMA_SQL: [
                {"col_name": column, "data_type": data_type, "comment": None}
                for column, data_type in (
                    ("materialization_scope", "string"), ("business_date", "date"),
                    ("category", "string"), ("region_code", "string"), ("channel", "string"),
                    ("total_revenue_eur", "decimal(18,2)"), ("order_count", "bigint"),
                    ("refunded_line_count", "bigint"), ("refreshed_at", "timestamp"),
                    ("source_system", "string"), ("quality_status", "string"),
                )
            ],
        }
        return responses

    def test_fixed_read_only_statements_produce_passing_report(self):
        spark = FakeSpark(self._responses())
        report = self.verifier.collect_report(spark)
        self.assertEqual(spark.statements, list(self.verifier._FIXED_STATEMENTS))
        self.assertEqual(report["batch_id"], "DEMO-001")
        self.assertTrue(report["gate"]["passed"], report["gate"]["fail_reasons"])
        self.assertEqual(report["gate"]["fail_reasons"], [])

    def test_report_is_redacted_and_deterministic(self):
        spark = FakeSpark(self._responses())
        rendered = self.verifier.report_json(self.verifier.collect_report(spark))
        self.assertEqual(rendered, self.verifier.report_json(
            self.verifier.collect_report(FakeSpark(self._responses()))))
        self.assertNotIn("abfss://", rendered)
        self.assertNotIn("@", rendered)
        self.assertNotIn("example.test", rendered)
        self.assertNotIn("+34", rendered)
        self.assertNotIn("parsed_payload", rendered)
        self.assertNotIn("CUS-NULL-EMAIL", rendered)
        self.assertNotIn("CUS-SOS-NOPHONE", rendered)
        parsed = json.loads(rendered)
        self.assertEqual(parsed["gate"]["passed"], True)

    def test_gate_reports_exact_count_failure(self):
        responses = self._responses()
        responses[self.verifier.ORDERS_CURRENT_SQL][0]["row_count"] = 14
        report = self.verifier.collect_report(FakeSpark(responses))
        self.assertFalse(report["gate"]["passed"])
        failed = {check["name"] for check in report["gate"]["checks"] if not check["passed"]}
        self.assertEqual(failed, {"orders_current_scenarios"})

    def test_gate_reports_rubber_or_missing_gold_cell_failure(self):
        responses = self._responses()
        responses[self.verifier.GOLD_PUBLISHED_CELLS_SQL] = [{
            "business_date": "2026-01-11", "category": "rubber", "region_code": "ES-MD",
            "channel": "web", "total_revenue_eur": "37.50", "order_count": 2,
            "refunded_line_count": 0,
        }]
        report = self.verifier.collect_report(FakeSpark(responses))
        self.assertFalse(report["gate"]["passed"])
        failed = {check["name"] for check in report["gate"]["checks"] if not check["passed"]}
        self.assertEqual(failed, {"gold_published_paddle_cell_no_rubber"})

    def test_gold_physical_duplicate_identical_row_fails_the_gate(self):
        # No GROUP BY collapsing: two identical physical Gold rows are
        # duplicate published evidence and must fail the evidence gate.
        responses = self._responses()
        row = dict(responses[self.verifier.GOLD_PUBLISHED_CELLS_SQL][0])
        responses[self.verifier.GOLD_PUBLISHED_CELLS_SQL] = [row, dict(row)]
        report = self.verifier.collect_report(FakeSpark(responses))
        self.assertFalse(report["gate"]["passed"])
        failed = {check["name"] for check in report["gate"]["checks"] if not check["passed"]}
        self.assertEqual(failed, {"gold_published_paddle_cell_no_rubber"})

    def test_gold_date_value_compared_as_normalized_iso(self):
        # The single fixture-published paddle cell passes with a real Spark
        # DATE value (datetime.date) instead of ISO text.
        responses = self._responses()
        report = self.verifier.collect_report(FakeSpark(responses))
        gold_check = next(check for check in report["gate"]["checks"]
                          if check["name"] == "gold_published_paddle_cell_no_rubber")
        self.assertTrue(gold_check["passed"], gold_check["reason"])
        # A date-shaped value that mismatches the expected ISO day must fail.
        responses[self.verifier.GOLD_PUBLISHED_CELLS_SQL][0]["business_date"] = date(2026, 1, 11)
        report = self.verifier.collect_report(FakeSpark(responses))
        self.assertFalse(report["gate"]["passed"])

    def test_gold_total_revenue_decimal_normalized_deterministically(self):
        # Real Spark DECIMAL(18,2) arrives as Python Decimal: Decimal("640.00")
        # must pass the gold check, and an incorrect Decimal must fail it.
        responses = self._responses()
        report = self.verifier.collect_report(FakeSpark(responses))
        gold_check = next(check for check in report["gate"]["checks"]
                          if check["name"] == "gold_published_paddle_cell_no_rubber")
        self.assertTrue(gold_check["passed"], gold_check["reason"])

        responses[self.verifier.GOLD_PUBLISHED_CELLS_SQL][0]["total_revenue_eur"] = Decimal("37.50")
        report = self.verifier.collect_report(FakeSpark(responses))
        gold_check = next(check for check in report["gate"]["checks"]
                          if check["name"] == "gold_published_paddle_cell_no_rubber")
        self.assertFalse(gold_check["passed"], gold_check["reason"])
        self.assertFalse(report["gate"]["passed"])

    def test_gate_reports_pii_columns_in_gold_schema(self):
        responses = self._responses()
        responses[self.verifier.GOLD_SCHEMA_SQL].append(
            {"col_name": "customer_id", "data_type": "string", "comment": None})
        report = self.verifier.collect_report(FakeSpark(responses))
        self.assertFalse(report["gate"]["passed"])
        failed = {check["name"] for check in report["gate"]["checks"] if not check["passed"]}
        self.assertEqual(failed, {"gold_schema_has_no_pii_identifier_columns"})

    def test_gate_reports_clean_completion_without_quarantine_label(self):
        responses = self._responses()
        responses[self.verifier.MATERIALIZATION_REPORT_SQL][0]["status"] = "COMPLETED"
        report = self.verifier.collect_report(FakeSpark(responses))
        self.assertFalse(report["gate"]["passed"])
        failed = {check["name"] for check in report["gate"]["checks"] if not check["passed"]}
        self.assertEqual(failed, {"materialization_report_completion_status"})

    def test_verifier_never_invokes_materialization_code(self):
        # Dynamically prove read-only intent: even if the adapter were present
        # and tampered, the verifier cannot call it (it never imports it).
        materialization = importlib.import_module("pipelines.silver.spark")

        def _booby_trap(*args, **kwargs):
            raise AssertionError("materialization invoked from the verifier")

        with unittest.mock.patch.object(materialization, "materialize", _booby_trap):
            report = self.verifier.collect_report(FakeSpark(self._responses()))
        self.assertTrue(report["gate"]["passed"], report["gate"]["fail_reasons"])

    def test_main_prints_json_then_returns_naturally_on_passing_gate(self):
        verifier = self.verifier
        with unittest.mock.patch.object(verifier, "get_default_spark",
                                        lambda: FakeSpark(self._responses())):
            with unittest.mock.patch("builtins.print") as printed:
                result = verifier.main()  # success returns normally: never SystemExit(0)
        self.assertIsNone(result)
        self.assertEqual(printed.call_count, 1)
        rendered = printed.call_args[0][0]
        self.assertEqual(json.loads(rendered)["gate"]["passed"], True)

    def test_main_prints_report_first_then_raises_on_failed_gate(self):
        verifier = self.verifier
        responses = self._responses()
        responses[verifier.ORDERS_CURRENT_SQL][0]["row_count"] = 14  # failing gate
        printed: list[str] = []

        with unittest.mock.patch.object(verifier, "get_default_spark", lambda: FakeSpark(responses)):
            with unittest.mock.patch("builtins.print",
                                     side_effect=lambda text: printed.append(text)):
                with self.assertRaises(verifier.SilverVerificationFailure) as assertion:
                    verifier.main()

        # The redacted report reached stdout BEFORE the failure was raised,
        # and it reports the failed gate.
        self.assertEqual(len(printed), 1)
        payload = json.loads(printed[0])
        self.assertEqual(payload["gate"]["passed"], False)
        self.assertNotIn("@", printed[0])
        self.assertNotIn("abfss://", printed[0])
        self.assertIn("verification failed with 1 failed check(s)", str(assertion.exception))
        self.assertIn("row_count': 14", str(assertion.exception))


if __name__ == "__main__":
    unittest.main()
