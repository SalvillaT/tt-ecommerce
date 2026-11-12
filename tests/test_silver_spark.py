"""Fake-Spark tests for the lazy PySpark Silver materialization adapter.

No real Spark, no PySpark installation, no cloud: a local fake-Spark suite
exercises the adapter/DDL/job boundary. The pure core stays the sole
quality/version/metric engine; these tests only assert the boundary contract:
exact Bronze read-only surface, DEMO-only scope, DDL shapes, typed
serializations with explicit schemas, ``replaceWhere`` scoped overwrite only,
report written last, no writes to the raw layer, and output idempotence under
the same audit timestamp.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import csv
import importlib
import json
from pathlib import Path
import re
import sys
import unittest
from unittest import mock

from pipelines.bronze.contracts import AUDIT_COLUMNS, BRONZE_CONTRACTS
from pipelines.silver import run_pipeline
from pipelines.silver import job as silver_job
from pipelines.silver import spark as silver_spark
from pipelines.silver import tables as silver_tables


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "data" / "sample" / "DEMO-001"
NOW = datetime(2026, 1, 20, 12, tzinfo=timezone.utc)
SCOPE = silver_tables.MATERIALIZATION_SCOPE

SIX_TABLES = {
    silver_tables.ORDERS_CURRENT_TABLE,
    silver_tables.CUSTOMERS_CURRENT_TABLE,
    silver_tables.PRODUCTS_CURRENT_TABLE,
    silver_tables.QUARANTINE_TABLE,
    silver_tables.GOLD_TABLE,
    silver_tables.MATERIALIZATION_REPORT_TABLE,
}


def _audit(source: str, payload: dict | None = None, *, status: str = "PARSED",
           corrupt: str | None = None) -> dict:
    name = {"orders": "orders_DEMO-001.jsonl", "customers": "customers_DEMO-001.csv",
            "products": "products_DEMO-001.csv"}[source]
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")) if payload else ""
    return {"batch_id": "DEMO-001", "contract_version": "v1", "manifest_file_sha256": "fixture-sha",
            "_source_file": f"abfss://landing/{source}/{name}", "_source_file_size": 1,
            "_source_file_modification_time": "2026-01-10T00:00:00Z",
            "_ingested_at": "2026-01-10T01:00:00Z", "_pipeline_run_id": "test-run",
            "parsed_payload_canonical": canonical, "_rescued_data": None,
            "_corrupt_record": corrupt, "parse_status": status, "ingest_date": "2026-01-10"}


def _fixture_bronze_rows() -> dict[str, list[dict]]:
    """Fixture-derived Bronze-shaped mappings (like collected raw rows)."""
    orders = []
    for line in (FIXTURE / "orders_DEMO-001.jsonl").read_text("utf-8").splitlines():
        try:
            payload = json.loads(line)
            status = "RESCUED" if payload.get("order_id") == "ORD-EXTRA-01" else "PARSED"
            orders.append({**payload, **_audit("orders", payload, status=status)})
        except json.JSONDecodeError:
            orders.append(_audit("orders", status="MALFORMED", corrupt=line))

    def csv_rows(name, source):
        with (FIXTURE / name).open(encoding="utf-8", newline="") as handle:
            return [{**row, **_audit(source, row)} for row in csv.DictReader(handle)]

    # Keyed exactly as the adapter reads them: full managed table names.
    return {
        BRONZE_CONTRACTS["orders"].table: orders,
        BRONZE_CONTRACTS["customers"].table: csv_rows("customers_DEMO-001.csv", "customers"),
        BRONZE_CONTRACTS["products"].table: csv_rows("products_DEMO-001.csv", "products"),
    }


def _pipeline_result_from(rows: dict[str, list[dict]]):
    return run_pipeline(orders=rows[BRONZE_CONTRACTS["orders"].table],
                        customers=rows[BRONZE_CONTRACTS["customers"].table],
                        products=rows[BRONZE_CONTRACTS["products"].table],
                        audit_timestamp=NOW)


class FakeRow:
    def __init__(self, data: dict):
        self._data = dict(data)

    def asDict(self, recursive: bool = True) -> dict:
        return dict(self._data)


class FakeFrame:
    def __init__(self, spark: "FakeSpark", rows: list[dict]):
        self._spark = spark
        self._rows = rows

    def where(self, predicate: str) -> "FakeFrame":
        self._spark.predicates.append(predicate)
        return self

    def select(self, *columns) -> "FakeFrame":
        self._spark.selections.append(list(columns))
        return self

    def collect(self) -> list[FakeRow]:
        return [FakeRow(row) for row in self._rows]


class _FakeSchema:
    """Explicit-schema stand-in recording the column contract."""

    def __init__(self, columns: tuple[tuple[str, str], ...]):
        self.columns = tuple(columns)


_TYPE_CHECKS = {
    "STRING": lambda value: value is None or isinstance(value, str),
    "STRING NOT NULL": lambda value: isinstance(value, str),
    "BIGINT": lambda value: value is None or (isinstance(value, int) and not isinstance(value, bool)),
    "TIMESTAMP": lambda value: value is None or isinstance(value, datetime),
    "DATE": lambda value: value is None or (isinstance(value, date) and not isinstance(value, datetime)),
    "BOOLEAN": lambda value: value is None or isinstance(value, bool),
    "DECIMAL(18,2)": lambda value: value is None or isinstance(value, Decimal),
    "ARRAY<STRING>": lambda value: value is None or (isinstance(value, (list, tuple)) and all(isinstance(item, str) for item in value)),
}


class FakeSpark:
    """Minimal read/write recorder with typed-value validation."""

    def __init__(self, bronze_tables: dict[str, list[dict]]):
        self.tables = bronze_tables
        self.table_names: list[str] = []
        self.predicates: list[str] = []
        self.selections: list[list[str]] = []
        self.sql_statements: list[str] = []
        self.created: list[dict] = []
        self.writes: list[dict] = []

    # --- read side ---------------------------------------------------------
    def table(self, name: str) -> FakeFrame:
        self.table_names.append(name)
        return FakeFrame(self, self.tables.get(name, []))

    # --- DDL side ----------------------------------------------------------
    def sql(self, statement: str) -> None:
        self.sql_statements.append(statement)

    # --- write side --------------------------------------------------------
    def createDataFrame(self, data, schema=None):
        columns = tuple(schema.columns) if isinstance(schema, _FakeSchema) else None
        if columns is None:
            raise AssertionError("output DataFrames must carry an explicit schema")
        for row in data:
            for name, sql_type in columns:
                if not _TYPE_CHECKS[sql_type](row.get(name)):
                    raise AssertionError(
                        f"bad serialized value for {name} ({sql_type}): {row.get(name)!r}")
        self.created.append({"columns": columns, "rows": [dict(row) for row in data]})
        return FakeDataFrame(self, len(self.created) - 1)

    def _record_write(self, index: int, table: str) -> None:
        self.writes.append({"table": table, **self.created[index]})


class FakeDataFrame:
    def __init__(self, spark: FakeSpark, index: int):
        self._spark = spark
        self._index = index

    @property
    def write(self) -> "_FakeWriter":
        return _FakeWriter(self._spark, self._index)


class _FakeWriter:
    def __init__(self, spark: FakeSpark, index: int):
        self._spark = spark
        self._index = index
        self._format = None
        self._mode = None
        self.options: dict[str, str] = {}

    def format(self, value: str) -> "_FakeWriter":
        self._format = value
        return self

    def mode(self, value: str) -> "_FakeWriter":
        self._mode = value
        return self

    def option(self, key: str, value: str) -> "_FakeWriter":
        self.options[key] = value
        return self

    def saveAsTable(self, table: str) -> None:
        self._spark.writes.append({
            "table": table, "format": self._format, "mode": self._mode,
            "options": dict(self.options),
            **self._spark.created[self._index],
        })


def _fake_schema_factory(columns):
    return _FakeSchema(columns)


def _patched_schema():
    return mock.patch.object(silver_spark, "_spark_schema", _fake_schema_factory)


def _materialize(spark, result, audit_timestamp=NOW):
    with _patched_schema():
        return silver_spark.materialize(spark, result, audit_timestamp=audit_timestamp)


class AdapterImportsWithoutPySparkTests(unittest.TestCase):
    def test_adapter_modules_import_without_pyspark(self):
        for name in ("pipelines.silver.tables", "pipelines.silver.spark", "pipelines.silver.job"):
            module = importlib.import_module(name)
            self.assertIsNotNone(module)
        self.assertNotIn("pyspark", sys.modules)
        self.assertNotIn("pyspark.sql", sys.modules)

    def test_real_schema_builder_requires_pyspark_lazily(self):
        # The lazy builder raises a clear runtime error (not an ImportError)
        # only when actually invoked without PySpark; import stays clean.
        if "pyspark" in sys.modules:  # pragma: no cover - environment guard
            self.skipTest("PySpark unexpectedly present")
        with self.assertRaisesRegex(RuntimeError, "PySpark is required"):
            silver_spark._spark_schema(silver_tables.GOLD_COLUMNS)


class BronzeReadOnlySurfaceTests(unittest.TestCase):
    def setUp(self):
        self.spark = FakeSpark(_fixture_bronze_rows())

    def test_reads_exact_bronze_tables_with_contract_columns(self):
        for source in ("orders", "customers", "products"):
            rows = silver_spark.bronze_batch_rows(self.spark, source)
            contract = BRONZE_CONTRACTS[source]
            self.assertEqual(rows[0]["batch_id"], "DEMO-001")
        self.assertEqual(self.spark.table_names, [
            "tt_ecommerce_synthetic.bronze.orders_raw",
            "tt_ecommerce_synthetic.bronze.customers_raw",
            "tt_ecommerce_synthetic.bronze.products_raw",
        ])
        self.assertEqual(self.spark.predicates, ["batch_id = 'DEMO-001'"] * 3)
        for index, source in enumerate(("orders", "customers", "products")):
            contract = BRONZE_CONTRACTS[source]
            expected_columns = [f"`{column}`" for column in contract.bronze_columns]
            self.assertEqual(self.spark.selections[index], expected_columns)
            # Selected columns are exactly the source contract fields plus ALL
            # audit columns (including parsed_payload_canonical/parse_status).
            self.assertEqual(contract.bronze_columns,
                             contract.data_columns + AUDIT_COLUMNS)

    def test_rows_pass_unchanged_business_and_audit_content(self):
        rows = silver_spark.bronze_batch_rows(self.spark, "orders")
        self.assertEqual(rows[0]["order_id"], "ORD-BASE-A01")
        self.assertEqual(rows[-1]["_corrupt_record"],
                         (FIXTURE / "orders_DEMO-001.jsonl").read_text("utf-8").splitlines()[-1])

    def test_other_batch_ids_are_rejected_before_spark(self):
        with self.assertRaisesRegex(ValueError, "only accepts batch_id = 'DEMO-001'"):
            silver_spark.bronze_batch_rows(self.spark, "orders", batch_id="DEMO-002")
        self.assertEqual(self.spark.table_names, [])


class DdlShapeTests(unittest.TestCase):
    def test_exactly_six_managed_output_tables(self):
        self.assertEqual(silver_tables.output_table_names(), SIX_TABLES)

    def test_ddl_shape_managed_partitioned_scoped(self):
        ddls = silver_tables.output_table_ddls()
        self.assertEqual(len(ddls), 6)
        for ddl in ddls:
            self.assertIn("CREATE TABLE IF NOT EXISTS tt_ecommerce_synthetic.", ddl)
            self.assertIn("USING DELTA", ddl)
            self.assertIn("PARTITIONED BY (materialization_scope)", ddl)
            self.assertIn("`materialization_scope` STRING NOT NULL", ddl)
            self.assertIn("'tt_ecommerce.write_semantics' = 'demo-001-scoped-overwrite'", ddl)
            self.assertIn("'tt_ecommerce.layer'", ddl)
            # Managed tables only: no external LOCATION, no Bronze DDL.
            self.assertNotIn("LOCATION", ddl)
            self.assertNotIn("bronze", ddl.lower())

    def test_table_layers_match_schemas(self):
        expected_layers = {
            silver_tables.ORDERS_CURRENT_TABLE: "silver",
            silver_tables.CUSTOMERS_CURRENT_TABLE: "silver",
            silver_tables.PRODUCTS_CURRENT_TABLE: "silver",
            silver_tables.QUARANTINE_TABLE: "quarantine",
            silver_tables.GOLD_TABLE: "gold",
            silver_tables.MATERIALIZATION_REPORT_TABLE: "silver",
        }
        for ddl in silver_tables.output_table_ddls():
            for table, layer in expected_layers.items():
                if table in ddl:
                    self.assertIn(f"'tt_ecommerce.layer' = '{layer}'", ddl)

    def test_unknown_table_rejected(self):
        with self.assertRaisesRegex(ValueError, "unknown output table"):
            silver_tables.output_table_ddl("tt_ecommerce_synthetic.bronze.orders_raw",
                                           silver_tables.GOLD_COLUMNS, layer="silver")

    def test_output_column_names_are_unique_per_table(self):
        # Delta DDL cannot create duplicate column names; every contract must
        # declare each column exactly once.
        contracts = (
            silver_tables.ORDERS_CURRENT_COLUMNS,
            silver_tables.CUSTOMERS_CURRENT_COLUMNS,
            silver_tables.PRODUCTS_CURRENT_COLUMNS,
            silver_tables.QUARANTINE_COLUMNS,
            silver_tables.GOLD_COLUMNS,
            silver_tables.MATERIALIZATION_REPORT_COLUMNS,
        )
        for columns in contracts:
            names = [name for name, _ in columns]
            self.assertEqual(len(names), len(set(names)),
                             msg=f"duplicate columns in contract: {sorted(names)}")

    def test_silver_current_tables_declare_deleted_at_exactly_once(self):
        # Customers: business deleted_at == SilverRow.deleted_at, emitted once
        # through the row-audit column. Products/orders: row-audit only.
        for columns in (silver_tables.ORDERS_CURRENT_COLUMNS,
                        silver_tables.CUSTOMERS_CURRENT_COLUMNS,
                        silver_tables.PRODUCTS_CURRENT_COLUMNS):
            self.assertEqual([name for name, _ in columns].count("deleted_at"), 1)

    def test_ddl_column_names_are_unique_per_table(self):
        # Regression guard at the rendered-DDL level: the backtick-quoted
        # column list inside each CREATE TABLE must have no repeated names.
        for ddl in silver_tables.output_table_ddls():
            table = ddl.split("(", 1)[0].split()[-1]
            body = ddl[ddl.index("(") + 1:ddl.index(")\nUSING DELTA")]
            names = re.findall(r"`([^`]+)`", body)
            self.assertEqual(len(names), len(set(names)),
                             msg=f"rendered DDL for {table} has duplicate columns")


class TypedSerializationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        rows = _fixture_bronze_rows()
        cls.result = _pipeline_result_from(rows)

    def test_silver_row_business_lineage_and_audit_values(self):
        order = self.result.orders[0]
        row = silver_spark.silver_output_row(order)
        business = order.as_dict()
        for name, value in business.items():
            if name == "updated_at":
                continue
            self.assertEqual(row[name], value, msg=name)
        lineage = order.lineage
        self.assertEqual(row["source_file"], lineage.source_file)
        self.assertEqual(row["source_file_size"], lineage.source_file_size)
        self.assertEqual(row["manifest_file_sha256"], lineage.manifest_file_sha256)
        self.assertEqual(row["created_at"], order.created_at)
        self.assertEqual(row["updated_at"], order.updated_at)
        self.assertEqual(row["source_system"], order.source_system)
        self.assertEqual(row["quality_status"], order.quality_status)
        self.assertEqual(row["materialization_scope"], "DEMO-001")
        # Typed serializations keep the core's python types.
        self.assertIsInstance(row["net_revenue_eur"], Decimal)
        self.assertIsInstance(row["record_version"], int)
        self.assertIsInstance(row["event_at"], datetime)
        self.assertIsInstance(row["business_date"], date)

    def test_customers_and_products_rename_record_updated_at(self):
        customer = self.result.customers[0]
        row = silver_spark.silver_output_row(customer)
        self.assertEqual(row["record_updated_at"], customer.as_dict()["updated_at"])
        self.assertEqual(row["updated_at"], customer.updated_at)
        product = self.result.products[0]
        prow = silver_spark.silver_output_row(product)
        self.assertEqual(prow["record_updated_at"], product.as_dict()["updated_at"])
        self.assertIsInstance(prow["active"], bool)
        self.assertIsInstance(prow["list_price_eur"], Decimal)

    def test_quarantine_row_contract_without_raw_payload(self):
        record = self.result.quarantine[0]
        row = silver_spark.quarantine_output_row(record)
        self.assertEqual(row["rule_ids"], list(record.rule_ids))
        self.assertEqual(row["details"], list(record.details))
        self.assertEqual(row["available_key"], list(record.available_key))
        self.assertTrue(all(isinstance(item, str) for item in row["rule_ids"]))
        self.assertTrue(row["payload_reference"].startswith("sha256:"))
        self.assertIsInstance(row["quarantined_at"], datetime)
        self.assertNotIn("parsed_payload_canonical", row)
        self.assertNotIn("_corrupt_record", row)

    def test_gold_row_carries_only_canonical_fields(self):
        segment = self.result.gold[0]
        row = silver_spark.gold_output_row(segment)
        self.assertEqual(set(row), {name for name, _ in silver_tables.GOLD_COLUMNS})
        self.assertIsInstance(row["total_revenue_eur"], Decimal)
        self.assertNotIn("customer", " ".join(row))
        self.assertNotIn("order_id", " ".join(row))

    def test_report_row_controls_and_no_suppressed_coordinates(self):
        row = silver_spark.report_output_row(self.result, audit_timestamp=NOW)
        self.assertEqual(row["status"], "COMPLETED_WITH_QUARANTINE")
        self.assertEqual(row["control_total_revenue_eur"], Decimal("812.20"))
        self.assertEqual(row["control_order_count"], 13)
        self.assertEqual(row["control_refunded_line_count"], 2)
        self.assertEqual(row["published_cells"], 1)
        self.assertEqual(row["suppressed_cells"], 8)
        self.assertEqual(row["duplicate_count"], 1)
        self.assertEqual(json.loads(row["by_rule_json"])["ORD-006"], 1)
        # Suppressed-cell coordinates and subject counts are never persisted.
        for forbidden in ("business_date", "category", "region_code", "channel", "subject"):
            self.assertNotIn(forbidden, row)

    def test_missing_column_value_fails_closed(self):
        order = self.result.orders[0]
        with mock.patch.object(
                silver_spark, "_SILVER_BUSINESS_COLUMNS",
                {"orders": (("order_id", "STRING"), ("missing", "STRING"))}):
            with self.assertRaisesRegex(ValueError, "missing output value"):
                silver_spark.silver_output_row(order)


class MaterializationBehaviorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        rows = _fixture_bronze_rows()
        cls.result = _pipeline_result_from(rows)

    def test_full_write_order_report_last_and_ddl_only(self):
        spark = FakeSpark(_fixture_bronze_rows())
        _materialize(spark, self.result)
        self.assertEqual([write["table"] for write in spark.writes], [
            silver_tables.ORDERS_CURRENT_TABLE,
            silver_tables.CUSTOMERS_CURRENT_TABLE,
            silver_tables.PRODUCTS_CURRENT_TABLE,
            silver_tables.QUARANTINE_TABLE,
            silver_tables.GOLD_TABLE,
            silver_tables.MATERIALIZATION_REPORT_TABLE,
        ])
        # The report (completion/evidence marker) is written LAST.
        self.assertEqual(spark.writes[-1]["table"], silver_tables.MATERIALIZATION_REPORT_TABLE)
        self.assertEqual(len(spark.sql_statements), 6)
        for statement in spark.sql_statements:
            self.assertTrue(statement.startswith("CREATE TABLE IF NOT EXISTS"))
            self.assertNotIn("bronze", statement.lower())
            self.assertIsNone(re.search(
                r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|VACUUM|OPTIMIZE)\b",
                statement.upper()),
                msg=statement)

    def test_no_writes_touch_the_raw_layer(self):
        spark = FakeSpark(_fixture_bronze_rows())
        _materialize(spark, self.result)
        written_tables = {write["table"] for write in spark.writes}
        self.assertTrue(written_tables <= SIX_TABLES)
        self.assertFalse(any("bronze" in table for table in written_tables))
        self.assertFalse(any("ingestion_run_log" in table for table in written_tables))

    def test_replace_where_scope_only_on_every_write(self):
        spark = FakeSpark(_fixture_bronze_rows())
        _materialize(spark, self.result)
        for write in spark.writes:
            self.assertEqual(write["format"], "delta")
            self.assertEqual(write["mode"], "overwrite")
            self.assertEqual(write["options"],
                             {"replaceWhere": "materialization_scope = 'DEMO-001'"})

    def test_scoped_rows_carry_the_demo_scope(self):
        spark = FakeSpark(_fixture_bronze_rows())
        _materialize(spark, self.result)
        for write in spark.writes:
            for row in write["rows"]:
                self.assertEqual(row["materialization_scope"], "DEMO-001")

    def test_explicit_schema_per_output_table(self):
        spark = FakeSpark(_fixture_bronze_rows())
        _materialize(spark, self.result)
        expected_columns = {
            silver_tables.ORDERS_CURRENT_TABLE: silver_tables.ORDERS_CURRENT_COLUMNS,
            silver_tables.CUSTOMERS_CURRENT_TABLE: silver_tables.CUSTOMERS_CURRENT_COLUMNS,
            silver_tables.PRODUCTS_CURRENT_TABLE: silver_tables.PRODUCTS_CURRENT_COLUMNS,
            silver_tables.QUARANTINE_TABLE: silver_tables.QUARANTINE_COLUMNS,
            silver_tables.GOLD_TABLE: silver_tables.GOLD_COLUMNS,
            silver_tables.MATERIALIZATION_REPORT_TABLE: silver_tables.MATERIALIZATION_REPORT_COLUMNS,
        }
        for write in spark.writes:
            self.assertEqual(tuple(write["columns"]), expected_columns[write["table"]])

    def test_output_rows_map_exactly_every_declared_column(self):
        # After the customers/products deleted_at dedup: every serialized row
        # must carry exactly the contract's column keys (no missing, no extra).
        spark = FakeSpark(_fixture_bronze_rows())
        _materialize(spark, self.result)
        expected_columns = {
            silver_tables.ORDERS_CURRENT_TABLE: silver_tables.ORDERS_CURRENT_COLUMNS,
            silver_tables.CUSTOMERS_CURRENT_TABLE: silver_tables.CUSTOMERS_CURRENT_COLUMNS,
            silver_tables.PRODUCTS_CURRENT_TABLE: silver_tables.PRODUCTS_CURRENT_COLUMNS,
            silver_tables.QUARANTINE_TABLE: silver_tables.QUARANTINE_COLUMNS,
            silver_tables.GOLD_TABLE: silver_tables.GOLD_COLUMNS,
            silver_tables.MATERIALIZATION_REPORT_TABLE: silver_tables.MATERIALIZATION_REPORT_COLUMNS,
        }
        for write in spark.writes:
            names = {name for name, _ in expected_columns[write["table"]]}
            for row in write["rows"]:
                self.assertEqual(set(row), names,
                                 msg=f"{write['table']} row keys drift from contract")

    def test_customers_current_row_deleted_at_is_the_row_audit_value(self):
        # Emitted exactly once, equal to both the SilverRow audit stamp and the
        # customers business deleted_at (they are the same value by contract;
        # in the DEMO-001 fixture every master deleted_at is null).
        customer = self.result.customers[0]
        row = silver_spark.silver_output_row(customer)
        business = customer.as_dict()
        self.assertEqual(row["deleted_at"], customer.deleted_at)
        self.assertEqual(row["deleted_at"], business["deleted_at"])
        self.assertEqual([name for name in row if name == "deleted_at"], ["deleted_at"])

    def test_typed_values_survive_fake_dataframe_validation(self):
        # FakeSpark.createDataFrame validates every serialized value against
        # the explicit column contract; a successful materialization proves
        # Decimal/datetime/date/int/bool/list[str] serializations are correct.
        spark = FakeSpark(_fixture_bronze_rows())
        _materialize(spark, self.result)
        gold_write = next(w for w in spark.writes if w["table"] == silver_tables.GOLD_TABLE)
        self.assertEqual(len(gold_write["rows"]), 1)
        self.assertEqual(gold_write["rows"][0]["total_revenue_eur"], Decimal("640.00"))
        self.assertEqual(gold_write["rows"][0]["order_count"], 5)

    def test_output_idempotence_with_same_timestamp(self):
        first = FakeSpark(_fixture_bronze_rows())
        second = FakeSpark(_fixture_bronze_rows())
        _materialize(first, self.result)
        _materialize(second, self.result)
        self.assertEqual(first.writes, second.writes)

    def test_rerun_on_same_spark_writes_identical_scope_rows(self):
        spark = FakeSpark(_fixture_bronze_rows())
        _materialize(spark, self.result)
        first_pass = list(spark.writes)
        _materialize(spark, self.result)
        second_pass = spark.writes[len(first_pass):]
        self.assertEqual(first_pass, second_pass)

    def test_timezone_naive_audit_timestamp_rejected(self):
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            silver_spark.materialize(FakeSpark({}), self.result,
                                     audit_timestamp=datetime(2026, 1, 20, 12))


class JobBoundaryTests(unittest.TestCase):
    def test_default_batch_id_is_demo_001(self):
        config = silver_job.parse_job_args([
            "--audit-timestamp=2026-01-20T12:00:00Z"])
        self.assertEqual(config.batch_id, "DEMO-001")
        self.assertEqual(config.audit_timestamp, NOW)

    def test_explicit_demo_001_accepted(self):
        config = silver_job.parse_job_args([
            "--batch-id=DEMO-001", "--audit-timestamp=2026-01-20T12:00:00Z"])
        self.assertEqual(config.batch_id, "DEMO-001")

    def test_other_batch_ids_rejected(self):
        for batch in ("DEMO-002", "demo-001", "PROD-001", ""):
            with self.assertRaises(SystemExit):
                silver_job.parse_job_args([
                    f"--batch-id={batch}", "--audit-timestamp=2026-01-20T12:00:00Z"])

    def test_audit_timestamp_mandatory(self):
        with self.assertRaises(SystemExit):
            silver_job.parse_job_args(["--batch-id=DEMO-001"])

    def test_audit_timestamp_must_be_utc_rfc3339_z(self):
        for bad in ("2026-01-20T12:00:00+00:00", "2026-01-20 12:00:00Z",
                    "2026-01-20T12:00:00", "2026-01-20T12:00:00.000Z",
                    "2026-13-20T12:00:00Z"):
            with self.assertRaises(SystemExit):
                silver_job.parse_job_args(["--audit-timestamp=" + bad])

    def test_catalog_and_scope_are_not_cli_inputs(self):
        with self.assertRaises(SystemExit):
            silver_job.parse_job_args([
                "--batch-id=DEMO-001", "--audit-timestamp=2026-01-20T12:00:00Z",
                "--catalog=other", "--schema=silver", "--scope=OTHER"])

    def test_execute_materialization_composes_read_transform_write(self):
        rows = _fixture_bronze_rows()
        spark = FakeSpark(rows)
        reads: list[tuple[str, str]] = []

        def recording_read(spark, source, *, batch_id):
            reads.append((source, batch_id))
            return silver_job.default_read_bronze(spark, source, batch_id=batch_id)

        with _patched_schema():
            report = silver_job.execute_materialization(
                spark, silver_job.SilverJobConfig(
                    batch_id="DEMO-001", audit_timestamp=NOW),
                read_bronze=recording_read)
        self.assertEqual(reads, [("orders", "DEMO-001"), ("customers", "DEMO-001"),
                                 ("products", "DEMO-001")])
        self.assertEqual(report["status"], "COMPLETED_WITH_QUARANTINE")
        self.assertEqual(spark.writes[-1]["table"],
                         silver_tables.MATERIALIZATION_REPORT_TABLE)

    def test_execute_materialization_rejects_other_batch(self):
        spark = FakeSpark(_fixture_bronze_rows())
        with self.assertRaisesRegex(ValueError, "only accepts batch_id"):
            silver_job.execute_materialization(spark, silver_job.SilverJobConfig(
                batch_id="DEMO-002", audit_timestamp=NOW))
        self.assertEqual(spark.table_names, [])

    def test_get_default_spark_is_lazy_and_unavailable_locally(self):
        if "pyspark" in sys.modules:  # pragma: no cover - environment guard
            self.skipTest("PySpark unexpectedly present")
        with self.assertRaises(ImportError):
            silver_job.get_default_spark()


if __name__ == "__main__":
    unittest.main()
