"""Read-only evidence verifier for the materialized Silver ``DEMO-001`` scope.

Deliberately separate from the materialization job (a later task in the same
manual job): it executes only fixed SELECT/DESCRIBE statements against the
managed output tables and prints one redacted deterministic JSON report. It
NEVER imports or invokes the materialization core/adapter, the raw ingestion
layer, external locations, volumes or pipeline-state code, and it never
mutates anything.

Expected values are anchored to the tested DEMO-001 fixture semantics: valid
current Silver scenarios, the expected quarantine rule ids, the published
k-anonymous paddle Gold cell, the pre-suppression controls, the completion
status, and a privacy-safe Gold schema.
"""

from __future__ import annotations

from decimal import Decimal
from datetime import date as date_type, datetime as datetime_type
import json
from typing import Any

BATCH_ID = "DEMO-001"
SCOPE_PREDICATE = "materialization_scope = 'DEMO-001'"

EXPECTED_ORDERS_CURRENT_ROWS = 15
EXPECTED_CUSTOMERS_CURRENT_ROWS = 10
EXPECTED_PRODUCTS_CURRENT_ROWS = 7
EXPECTED_QUARANTINE_ROWS = 6
EXPECTED_QUARANTINE_RULE_ROWS = {
    "CUS-001": 1, "ORD-001": 1, "ORD-002": 2, "ORD-005": 1, "SCH-002": 1,
}
EXPECTED_GOLD_CELL = {
    "business_date": "2026-01-10", "category": "paddle", "region_code": "ES-MD",
    "channel": "web", "total_revenue_eur": "640.00", "order_count": 5,
    "refunded_line_count": 0,
}
EXPECTED_STATUS = "COMPLETED_WITH_QUARANTINE"
EXPECTED_REPORT_COUNTS = {"published_cells": 1, "suppressed_cells": 8, "duplicate_count": 1}
EXPECTED_PRE_SUPPRESSION_CONTROLS = {
    "control_total_revenue_eur": "812.20", "control_order_count": 13,
    "control_refunded_line_count": 2,
}
# Prohibited PII/identifier columns must never exist in the Gold schema.
GOLD_FORBIDDEN_COLUMNS = frozenset({
    "customer", "customer_id", "order_id", "line_id", "email", "phone",
    "payload_reference", "parsed_payload_canonical", "subject_count",
})

ORDERS_CURRENT_SQL = f"""
SELECT
  materialization_scope,
  COUNT(*) AS row_count,
  SUM(CASE WHEN order_id = 'ORD-EXTRA-01' THEN 1 ELSE 0 END) AS extra_rows,
  SUM(CASE WHEN order_id IN ('ORD-CONF-01', 'ORD-NULL-01', 'ORD-ORPH-01')
           THEN 1 ELSE 0 END) AS rejected_rows
FROM tt_ecommerce_synthetic.silver.orders_current
WHERE {SCOPE_PREDICATE}
GROUP BY materialization_scope
""".strip()

CUSTOMERS_CURRENT_SQL = f"""
SELECT
  materialization_scope,
  COUNT(*) AS row_count,
  SUM(CASE WHEN customer_id = 'CUS-SOS-NOPHONE' THEN 1 ELSE 0 END) AS nophone_rows,
  SUM(CASE WHEN customer_id = 'CUS-NULL-EMAIL' THEN 1 ELSE 0 END) AS null_email_rows
FROM tt_ecommerce_synthetic.silver.customers_current
WHERE {SCOPE_PREDICATE}
GROUP BY materialization_scope
""".strip()

PRODUCTS_CURRENT_SQL = f"""
SELECT materialization_scope, COUNT(*) AS row_count
FROM tt_ecommerce_synthetic.silver.products_current
WHERE {SCOPE_PREDICATE}
GROUP BY materialization_scope
""".strip()

QUARANTINE_COUNTS_SQL = f"""
SELECT
  materialization_scope,
  COUNT(*) AS rejection_rows,
  SUM(CASE WHEN source = 'orders' THEN 1 ELSE 0 END) AS orders_rows,
  SUM(CASE WHEN source = 'customers' THEN 1 ELSE 0 END) AS customers_rows
FROM tt_ecommerce_synthetic.quarantine.data_quality_rejections
WHERE {SCOPE_PREDICATE}
GROUP BY materialization_scope
""".strip()

QUARANTINE_RULES_SQL = f"""
SELECT rule_id, COUNT(*) AS rule_rows
FROM (
  SELECT explode(rule_ids) AS rule_id
  FROM tt_ecommerce_synthetic.quarantine.data_quality_rejections
  WHERE {SCOPE_PREDICATE}
)
GROUP BY rule_id
ORDER BY rule_id ASC
""".strip()

# Gold is read as PHYSICAL rows (no GROUP BY collapsing): a duplicate Gold row
# is duplicate published evidence and must fail the gate. Stable deterministic
# order; the only mutation-free catalog statement is DESCRIBE below.
GOLD_PUBLISHED_CELLS_SQL = f"""
SELECT
  business_date, category, region_code, channel, total_revenue_eur,
  order_count, refunded_line_count
FROM tt_ecommerce_synthetic.gold.daily_sales_segment
WHERE {SCOPE_PREDICATE}
ORDER BY business_date ASC, category ASC, region_code ASC, channel ASC,
         total_revenue_eur ASC, order_count ASC, refunded_line_count ASC
""".strip()

MATERIALIZATION_REPORT_SQL = f"""
SELECT
  materialization_scope, batch_id, status, audit_timestamp, recorded_at,
  published_cells, suppressed_cells, duplicate_count,
  control_total_revenue_eur, control_order_count, control_refunded_line_count
FROM tt_ecommerce_synthetic.silver.materialization_run_report
WHERE {SCOPE_PREDICATE}
ORDER BY recorded_at ASC
""".strip()

# DESCRIBE is a read-only catalog statement; no mutation is possible here.
GOLD_SCHEMA_SQL = "DESCRIBE TABLE tt_ecommerce_synthetic.gold.daily_sales_segment"

_FIXED_STATEMENTS = (
    ORDERS_CURRENT_SQL, CUSTOMERS_CURRENT_SQL, PRODUCTS_CURRENT_SQL,
    QUARANTINE_COUNTS_SQL, QUARANTINE_RULES_SQL, GOLD_PUBLISHED_CELLS_SQL,
    MATERIALIZATION_REPORT_SQL, GOLD_SCHEMA_SQL,
)


def _row_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "asDict"):
        return dict(row.asDict(recursive=True))
    if isinstance(row, dict):
        return dict(row)
    raise TypeError("Spark verification query returned an unsupported row type")


def _collect(spark, statement: str) -> list[dict[str, Any]]:
    """Execute exactly one read-only statement and detach its collected rows."""
    return [_row_dict(row) for row in spark.sql(statement).collect()]


def _check(name: str, passed: bool, reason: str) -> dict[str, Any]:
    return {"name": name, "passed": passed, "reason": reason}


def _revenue(value: Any) -> str:
    return str(Decimal(str(value)))


def _iso_date(value: Any) -> str:
    """Normalize a Gold DATE cell (Spark returns ``datetime.date``) to ISO text."""
    if isinstance(value, datetime_type):
        return value.date().isoformat()
    if isinstance(value, date_type):
        return value.isoformat()
    return str(value)


def evaluate_report(report: dict[str, Any]) -> dict[str, Any]:
    """Evaluate only the evidence actually returned by the fixed read queries."""
    checks: list[dict[str, Any]] = []

    orders = report["current_scenarios"]["orders"]
    expected = {"row_count": EXPECTED_ORDERS_CURRENT_ROWS, "extra_rows": 1, "rejected_rows": 0}
    checks.append(_check(
        "orders_current_scenarios",
        len(orders) == 1 and orders[0].get("materialization_scope") == BATCH_ID
        and all(orders[0].get(key) == value for key, value in expected.items()),
        f"expected one scope group {expected}; observed {orders}",
    ))

    customers = report["current_scenarios"]["customers"]
    expected = {"row_count": EXPECTED_CUSTOMERS_CURRENT_ROWS, "nophone_rows": 1, "null_email_rows": 0}
    checks.append(_check(
        "customers_current_scenarios",
        len(customers) == 1 and customers[0].get("materialization_scope") == BATCH_ID
        and all(customers[0].get(key) == value for key, value in expected.items()),
        f"expected one scope group {expected}; observed {customers}",
    ))

    products = report["current_scenarios"]["products"]
    checks.append(_check(
        "products_current_scenarios",
        len(products) == 1 and products[0].get("materialization_scope") == BATCH_ID
        and products[0].get("row_count") == EXPECTED_PRODUCTS_CURRENT_ROWS,
        f"expected one scope group with {EXPECTED_PRODUCTS_CURRENT_ROWS} rows; observed {products}",
    ))

    quarantine = report["quarantine_counts"]
    expected = {"rejection_rows": EXPECTED_QUARANTINE_ROWS, "orders_rows": 5, "customers_rows": 1}
    checks.append(_check(
        "quarantine_counts",
        len(quarantine) == 1 and quarantine[0].get("materialization_scope") == BATCH_ID
        and all(quarantine[0].get(key) == value for key, value in expected.items()),
        f"expected one scope group {expected}; observed {quarantine}",
    ))

    rule_rows = {row["rule_id"]: row["rule_rows"] for row in report["quarantine_rules"]}
    checks.append(_check(
        "quarantine_rule_ids",
        rule_rows == EXPECTED_QUARANTINE_RULE_ROWS,
        f"expected {EXPECTED_QUARANTINE_RULE_ROWS}; observed {rule_rows}",
    ))

    gold_cells = report["gold_cells"]
    gold_passed = (
        len(gold_cells) == 1
        # Spark DATE columns arrive as datetime.date: compare deterministically
        # via normalized ISO text.
        and _iso_date(gold_cells[0].get("business_date")) == EXPECTED_GOLD_CELL["business_date"]
        # Spark DECIMAL(18,2) arrives as Python Decimal: compare
        # deterministically via the same _revenue normalizer as the controls.
        and _revenue(gold_cells[0].get("total_revenue_eur"))
        == EXPECTED_GOLD_CELL["total_revenue_eur"]
        and all(gold_cells[0].get(key) == value
                for key, value in EXPECTED_GOLD_CELL.items()
                if key not in ("business_date", "total_revenue_eur"))
        and not any(str(row.get("category", "")).strip().lower() == "rubber"
                    for row in gold_cells)
    )
    checks.append(_check(
        "gold_published_paddle_cell_no_rubber",
        gold_passed,
        f"expected exactly one physical row {EXPECTED_GOLD_CELL} and no rubber row; "
        f"observed {gold_cells}",
    ))

    reports = report["materialization_report"]
    report_passed = (
        len(reports) == 1
        and reports[0].get("materialization_scope") == BATCH_ID
        and reports[0].get("batch_id") == BATCH_ID
        and reports[0].get("status") == EXPECTED_STATUS
        and all(reports[0].get(key) == value
                for key, value in EXPECTED_REPORT_COUNTS.items())
    )
    checks.append(_check(
        "materialization_report_completion_status",
        report_passed,
        f"expected one report row with status={EXPECTED_STATUS} and "
        f"{EXPECTED_REPORT_COUNTS}; observed {reports}",
    ))

    controls_passed = (
        len(reports) == 1
        and _revenue(reports[0].get("control_total_revenue_eur")) == "812.20"
        and reports[0].get("control_order_count") == EXPECTED_PRE_SUPPRESSION_CONTROLS["control_order_count"]
        and reports[0].get("control_refunded_line_count") == EXPECTED_PRE_SUPPRESSION_CONTROLS["control_refunded_line_count"]
    )
    checks.append(_check(
        "pre_suppression_controls",
        controls_passed,
        "expected one report row with pre-suppression controls 812.20/13/2; "
        f"observed {reports}",
    ))

    gold_columns = {str(row.get("col_name", "")).strip().lower()
                    for row in report["gold_schema"]}
    forbidden_present = sorted(gold_columns & GOLD_FORBIDDEN_COLUMNS)
    substring_hit = sorted(
        column for column in gold_columns
        if any(marker in column for marker in ("email", "phone", "customer"))
    )
    checks.append(_check(
        "gold_schema_has_no_pii_identifier_columns",
        not forbidden_present and not substring_hit,
        f"forbidden exact columns {forbidden_present}; privacy-marker "
        f"substrings {substring_hit}; observed columns {sorted(gold_columns)}",
    ))

    failures = [check["reason"] for check in checks if not check["passed"]]
    return {"passed": not failures, "checks": checks, "fail_reasons": failures}


def collect_report(spark) -> dict[str, Any]:
    """Collect the fixed read-only evidence; no mutation and no inference."""
    report = {
        "batch_id": BATCH_ID,
        "current_scenarios": {
            "orders": _collect(spark, ORDERS_CURRENT_SQL),
            "customers": _collect(spark, CUSTOMERS_CURRENT_SQL),
            "products": _collect(spark, PRODUCTS_CURRENT_SQL),
        },
        "quarantine_counts": _collect(spark, QUARANTINE_COUNTS_SQL),
        "quarantine_rules": _collect(spark, QUARANTINE_RULES_SQL),
        "gold_cells": _collect(spark, GOLD_PUBLISHED_CELLS_SQL),
        "materialization_report": _collect(spark, MATERIALIZATION_REPORT_SQL),
        "gold_schema": _collect(spark, GOLD_SCHEMA_SQL),
    }
    report["gate"] = evaluate_report(report)
    return report


def report_json(report: dict[str, Any]) -> str:
    """One stable, machine-readable line suitable for redacted evidence capture."""
    return json.dumps(report, sort_keys=True, separators=(",", ":"), default=str)


def get_default_spark():
    """Lazy runtime-only import preserves local importability without PySpark."""
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


class SilverVerificationFailure(RuntimeError):
    """Raised AFTER the redacted report is printed, so the manual job task
    fails with a nonzero exit while the evidence remains on the task log."""


def main() -> None:
    """Print the redacted JSON report FIRST, then fail the task on a failed gate.

    A failed evidence gate must make the verifying task fail visibly: the
    report is printed before raising (nonzero-failing exception), and a
    passing gate returns naturally (never a zero-false-success exit).
    """
    report = collect_report(get_default_spark())
    print(report_json(report))
    if not report["gate"]["passed"]:
        raise SilverVerificationFailure(
            f"silver {BATCH_ID} verification failed with "
            f"{len(report['gate']['fail_reasons'])} failed check(s): "
            + " | ".join(report["gate"]["fail_reasons"]))


if __name__ == "__main__":
    main()
