"""Read-only evidence verifier for the completed Bronze ``DEMO-001`` batch.

This is a deliberately separate manual verification task, not an ingestion
mode.  It executes only fixed SELECT statements and prints one JSON report.
It is safe only for assessing the already completed DEMO-001 run; it neither
reprocesses data nor substitutes for separately listing the retained
per-source checkpoints under the UC volume.
"""

from __future__ import annotations

import json
from collections import Counter
from typing import Any

BATCH_ID = "DEMO-001"
CATALOG_SCHEMA = "tt_ecommerce_synthetic.bronze"

RAW_EXPECTATIONS = {
    "orders": {"table": "orders_raw", "raw_rows": 26, "rescued_rows": 1, "corrupt_rows": 1},
    "customers": {"table": "customers_raw", "raw_rows": 11, "rescued_rows": 0, "corrupt_rows": 0},
    "products": {"table": "products_raw", "raw_rows": 7, "rescued_rows": 0, "corrupt_rows": 0},
}
EXPECTED_ORDERS_PARSE_STATUS = {"MALFORMED": 1, "PARSED": 24, "RESCUED": 1}
EXPECTED_EVENTS = Counter({
    ("STARTED", None): 1,
    ("MANIFEST_VALIDATED", None): 1,
    ("SOURCE_COMMITTED", "orders"): 1,
    ("SOURCE_COMMITTED", "customers"): 1,
    ("SOURCE_COMMITTED", "products"): 1,
})

RUN_LOG_SQL = """
SELECT
  attempt_id, pipeline_run_id, event, source, event_at,
  files_discovered, rows_declared, rows_read, metrics_observed, error_class
FROM tt_ecommerce_synthetic.bronze.ingestion_run_log
WHERE batch_id = 'DEMO-001'
ORDER BY
  event_at ASC,
  attempt_id ASC,
  CASE event
    WHEN 'STARTED' THEN 1
    WHEN 'MANIFEST_VALIDATED' THEN 2
    WHEN 'SOURCE_COMMITTED' THEN 3
    WHEN 'FAILED' THEN 4
    ELSE 5
  END ASC,
  CASE source
    WHEN 'orders' THEN 1
    WHEN 'customers' THEN 2
    WHEN 'products' THEN 3
    ELSE 0
  END ASC
""".strip()

ORDERS_PARSE_STATUS_SQL = """
SELECT parse_status, COUNT(*) AS row_count
FROM tt_ecommerce_synthetic.bronze.orders_raw
WHERE batch_id = 'DEMO-001'
GROUP BY parse_status
ORDER BY parse_status ASC
""".strip()


def raw_summary_sql(table: str) -> str:
    """Return a fixed, read-only summary for one contract table."""
    if table not in {expected["table"] for expected in RAW_EXPECTATIONS.values()}:
        raise ValueError("unknown verification table")
    return f"""
SELECT
  batch_id, _pipeline_run_id,
  COUNT(*) AS raw_rows,
  SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END) AS rescued_rows,
  SUM(CASE WHEN _corrupt_record IS NOT NULL THEN 1 ELSE 0 END) AS corrupt_rows
FROM {CATALOG_SCHEMA}.{table}
WHERE batch_id = 'DEMO-001'
GROUP BY batch_id, _pipeline_run_id
ORDER BY batch_id ASC, _pipeline_run_id ASC
""".strip()


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


def evaluate_report(report: dict[str, Any]) -> dict[str, Any]:
    """Evaluate only evidence actually returned by the fixed read queries."""
    checks: list[dict[str, Any]] = []
    raw_tables = report["raw_tables"]
    for source, expected in RAW_EXPECTATIONS.items():
        groups = raw_tables[source]
        expected_values = {
            "raw_rows": expected["raw_rows"],
            "rescued_rows": expected["rescued_rows"],
            "corrupt_rows": expected["corrupt_rows"],
        }
        passed = (len(groups) == 1 and groups[0].get("batch_id") == BATCH_ID
                  and all(groups[0].get(key) == value
                          for key, value in expected_values.items()))
        checks.append(_check(
            f"{source}_raw_exact_counts",
            passed,
            f"expected one pipeline group with {expected_values}; observed {groups}",
        ))

    parse_status = {row["parse_status"]: row["row_count"]
                    for row in report["orders_parse_status"]}
    checks.append(_check(
        "orders_parse_status_distribution",
        parse_status == EXPECTED_ORDERS_PARSE_STATUS,
        f"expected {EXPECTED_ORDERS_PARSE_STATUS}; observed {parse_status}",
    ))

    events = report["run_log_events"]
    observed_events = Counter((row.get("event"), row.get("source")) for row in events)
    checks.append(_check(
        "run_log_exact_expected_events_no_failed",
        len(events) == 5 and observed_events == EXPECTED_EVENTS and not any(
            row.get("event") == "FAILED" for row in events),
        f"expected five events {dict(EXPECTED_EVENTS)} and no FAILED; observed {events}",
    ))
    manifest_events = [row for row in events if row.get("event") == "MANIFEST_VALIDATED"]
    checks.append(_check(
        "manifest_validated_three_files_44_rows",
        len(manifest_events) == 1
        and manifest_events[0].get("files_discovered") == 3
        and manifest_events[0].get("rows_declared") == 44,
        f"expected one MANIFEST_VALIDATED event with files_discovered=3 and rows_declared=44; "
        f"observed {manifest_events}",
    ))
    committed_sources = Counter(row.get("source") for row in events
                                if row.get("event") == "SOURCE_COMMITTED")
    checks.append(_check(
        "all_source_commit_events",
        committed_sources == Counter({"orders": 1, "customers": 1, "products": 1}),
        f"expected one SOURCE_COMMITTED event per source; observed {dict(committed_sources)}",
    ))

    log_pipeline_ids = {row.get("pipeline_run_id") for row in events if row.get("pipeline_run_id")}
    raw_pipeline_ids = {
        group.get("_pipeline_run_id")
        for groups in raw_tables.values() for group in groups
        if group.get("_pipeline_run_id")
    }
    checks.append(_check(
        "one_matching_run_log_pipeline_id",
        len(log_pipeline_ids) == 1 and raw_pipeline_ids == log_pipeline_ids,
        f"expected one matching pipeline_run_id; run_log={sorted(log_pipeline_ids)}, "
        f"raw={sorted(raw_pipeline_ids)}",
    ))

    failures = [check["reason"] for check in checks if not check["passed"]]
    return {"passed": not failures, "checks": checks, "fail_reasons": failures}


def collect_report(spark) -> dict[str, Any]:
    """Collect fixed evidence and evaluate it without any mutation or inference."""
    raw_tables = {
        source: _collect(spark, raw_summary_sql(expected["table"]))
        for source, expected in RAW_EXPECTATIONS.items()
    }
    report = {
        "batch_id": BATCH_ID,
        "run_log_events": _collect(spark, RUN_LOG_SQL),
        "raw_tables": raw_tables,
        "orders_parse_status": _collect(spark, ORDERS_PARSE_STATUS_SQL),
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


def main() -> None:
    print(report_json(collect_report(get_default_spark())))


if __name__ == "__main__":
    main()
