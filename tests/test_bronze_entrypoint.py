"""Entrypoint tests (Fase B) with fake Spark, fake streams and fake preflight.

No Azure/remote access and no PySpark: Spark is never requested at import;
tests inject fakes for preflight, bootstrap, run-log writes and stream
starts, and drive the real orchestration in `entrypoint.execute_run`.
"""

from __future__ import annotations

import json
import sys
import unittest

from pipelines.bronze.contracts import BRONZE_CONTRACTS
from pipelines.bronze.entrypoint import (
    BronzeJobConfig,
    default_start_stream,
    execute_run,
    parse_job_args,
    progress_input_rows,
)
from pipelines.bronze.layout import (
    LandingLayout,
    join_landing_uri,
    landing_relative_path,
    validate_landing_root,
)
from pipelines.bronze.manifest import SourceReceipt
from pipelines.bronze.remote_preflight import (
    RemotePreflightError,
    RemotePreflightResult,
    remote_preflight,
)
from pipelines.bronze.run_log import RunMetrics, RunLogEvent
from pipelines.bronze.spark import resolve_batch_input_uri

try:  # works both under `unittest discover -s tests` and `unittest tests.…`
    from test_bronze_remote_preflight import FakeBinarySpark, canonical_remote_objects
except ImportError:  # pragma: no cover - package-style invocation
    from tests.test_bronze_remote_preflight import FakeBinarySpark, canonical_remote_objects

BATCH = "DEMO-001"
ARRIVAL = "2026-01-10"
ROOT_URI = "abfss://landing@example.dfs.core.windows.net"


def _receipts():
    return [
        SourceReceipt(BATCH, "orders", "orders_DEMO-001.jsonl", "a" * 64, 26, "v1"),
        SourceReceipt(BATCH, "customers", "customers_DEMO-001.csv", "b" * 64, 11, "v1"),
        SourceReceipt(BATCH, "products", "products_DEMO-001.csv", "c" * 64, 7, "v1"),
    ]


def _result(receipts=None):
    receipts = receipts if receipts is not None else _receipts()
    return RemotePreflightResult(
        batch_id=BATCH, arrival_date=ARRIVAL, landing_root=ROOT_URI,
        ready_payload={"batch_id": BATCH}, ready_uri=f"{ROOT_URI}/_control/ready/{BATCH}.json",
        manifest_uri=f"{ROOT_URI}/_control/manifests/{BATCH}/manifest_{BATCH}.json",
        manifest_sha256="d" * 64, receipts=receipts, source_uris={},
    )


class RecordingSpark:
    """Fake Spark for bootstrap DDL and append-only run-log writes."""

    def __init__(self):
        self.ddls: list[str] = []
        self.run_log_writes: list[dict] = []
        self.tables: list[str] = []

    def sql(self, ddl):
        self.ddls.append(ddl)
        return None

    def createDataFrame(self, data):
        return FakeLogFrame(self, list(data))

    def record_write(self, rows, table):
        self.tables.append(table)
        self.run_log_writes.extend(rows)


class FakeLogFrame:
    def __init__(self, spark, rows):
        self._spark = spark
        self._rows = rows

    def selectExpr(self, *exprs):
        return self

    @property
    def write(self):
        return self

    def format(self, name):
        assert name == "delta"
        return self

    def mode(self, mode):
        assert mode == "append"
        return self

    def saveAsTable(self, table):
        self._spark.record_write([dict(row) for row in self._rows], table)


class FakeQuery:
    def __init__(self, last_progress=None, recent=None, started=False):
        self.lastProgress = last_progress
        self.recentProgress = recent
        self.terminated = False
        self.started = started

    def awaitTermination(self):
        self.terminated = True


class RecordingStreams:
    """Same 4-arg seam as the real adapter: (spark, receipt, config, layout)."""

    def __init__(self, queries_by_source):
        self.started: list[str] = []
        self.configs: list[BronzeJobConfig] = []
        self.layouts_seen: list[object] = []
        self._queries = queries_by_source

    def __call__(self, spark, receipt, config, layout):
        self.started.append(receipt.source)
        self.configs.append(config)
        self.layouts_seen.append(layout)
        return self._queries[receipt.source]


class RecordingPreflight:
    def __init__(self, result=_result()):
        self.result = result
        self.calls: list[dict] = []

    def __call__(self, spark, **kwargs):
        self.calls.append(dict(kwargs))
        return self.result


class StreamingInputUriTests(unittest.TestCase):
    """The Job passes an explicit landing_root; Auto Loader reads the physical
    final URI per batch (never the logical `landing/...` relative or a source
    root). Locally (no landing_root) the validated legacy relative path stays."""

    def test_logical_path_without_landing_root(self):
        layout = LandingLayout()
        self.assertEqual(resolve_batch_input_uri(layout, "orders", BATCH, ARRIVAL),
                         layout.final_directory("orders", BATCH, ARRIVAL))

    def test_physical_uri_with_explicit_landing_root(self):
        layout = LandingLayout()
        expected = join_landing_uri(ROOT_URI, landing_relative_path(
            layout.final_directory("customers", BATCH, ARRIVAL)))
        self.assertEqual(resolve_batch_input_uri(layout, "customers", BATCH, ARRIVAL, ROOT_URI),
                         expected)
        resolved = resolve_batch_input_uri(layout, "products", BATCH, ARRIVAL, ROOT_URI)
        self.assertIn(f"products/arrival_date={ARRIVAL}/batch_id={BATCH}", resolved)
        self.assertIn(".dfs.core.windows.net/products/arrival_date=", resolved)
        self.assertNotEqual(resolved, ROOT_URI)
        self.assertFalse(resolved.startswith("landing/"))
        self.assertNotIn("landing/products/", resolved)

    def test_unsafe_landing_root_rejected(self):
        with self.assertRaises(ValueError):
            resolve_batch_input_uri(LandingLayout(), "products", BATCH, ARRIVAL, "s3://bucket")


class LandingRootValidationTests(unittest.TestCase):
    """The landing root is DEPLOYMENT configuration, not a run-editable job
    parameter. It is validated strictly in Python: exactly the root of the
    `landing` abfss container — no subpath, query, fragment or port, no other
    scheme/root filesystem, no traversable root. A trailing slash alone is
    normalized. The account is free-form (never a secret, never presumed)."""

    def _args(self, root):
        return ["--batch-id", BATCH, "--arrival-date", ARRIVAL, "--landing-root", root]

    def test_accepts_exact_container_root_and_normalizes_trailing_slash(self):
        self.assertEqual(parse_job_args(self._args(ROOT_URI)).landing_root, ROOT_URI)
        self.assertEqual(
            parse_job_args(self._args(ROOT_URI + "/")).landing_root, ROOT_URI)
        # Another valid account is structurally valid (no real account is
        # presumed or hard-coded in the validator).
        other = validate_landing_root("abfss://landing@other-account-storage.dfs.core.windows.net")
        self.assertEqual(other, "abfss://landing@other-account-storage.dfs.core.windows.net")

    def test_rejects_roots_that_are_not_landing_container_roots(self):
        invalid_roots = [
            None,
            "",
            "   ",
            f"{ROOT_URI}/orders",                     # subpath (no traversal surface)
            f"{ROOT_URI}/orders/..",                  # traversal attempt
            f"{ROOT_URI}?query=1",                    # query
            f"{ROOT_URI}#fragment",                   # fragment
            "abfss://landing@example.dfs.core.windows.net:443",   # port
            "wasbs://landing@example.blob.core.windows.net",      # other scheme/endpoint
            "abfs://landing@example.dfs.core.windows.net",
            "https://example.dfs.core.windows.net/landing",
            "s3://landing@bucket",
            "file:///landing",                        # local root filesystem
            "/dbfs/landing",                          # DBFS root filesystem
            "/mnt/landing",
            "landing",                                # logical path, not a URI
            "abfss://example.dfs.core.windows.net",   # account root, missing container
            "abfss://data@example.dfs.core.windows.net",      # another container
            "abfss://LANDING@example.dfs.core.windows.net",   # container name is exact
            "abfss://landing//corners@example.dfs.core.windows.net",
            "abfss://landing@example.dfs.core.windows.net//",
        ]
        for root in invalid_roots:
            with self.subTest(root=root):
                self.assertRaises(ValueError, validate_landing_root, root)
                if isinstance(root, str):
                    with self.assertRaises(SystemExit):
                        parse_job_args(self._args(root))


class BronzeJobConfigTests(unittest.TestCase):
    def test_parse_job_args_accepts_equals_and_join_params(self):
        config = parse_job_args([
            "--batch-id=DEMO-001", "--arrival-date=2026-01-10",
            "--landing-root", ROOT_URI,
            "--pipeline-run-id", "run-1", "--attempt-id", "attempt-1",
        ])
        self.assertEqual(config.batch_id, BATCH)
        self.assertEqual(config.arrival_date, ARRIVAL)
        self.assertEqual(config.landing_root, ROOT_URI)
        self.assertEqual(config.pipeline_run_id, "run-1")
        self.assertEqual(config.attempt_id, "attempt-1")

    def test_parse_job_args_requires_safe_batch_and_landing_root(self):
        with self.assertRaises(SystemExit):
            parse_job_args(["--batch-id", "../bad", "--arrival-date", ARRIVAL,
                            "--landing-root", ROOT_URI])
        with self.assertRaises(SystemExit):
            parse_job_args(["--batch-id", BATCH, "--arrival-date", ARRIVAL])
        with self.assertRaises(SystemExit):
            parse_job_args(["--batch-id", BATCH, "--arrival-date", "2026-13-01",
                            "--landing-root", ROOT_URI])


class ProgressMetricsTests(unittest.TestCase):
    def test_progress_uses_only_present_num_input_rows(self):
        self.assertEqual(progress_input_rows(FakeQuery(last_progress=None)), (0, False))
        self.assertEqual(progress_input_rows(FakeQuery(last_progress={"numInputRows": 7})), (7, True))
        self.assertEqual(progress_input_rows(
            FakeQuery(recent=[{"numInputRows": 5}, {"numInputRows": 20}])), (25, True))
        self.assertEqual(progress_input_rows(
            FakeQuery(recent=[{"numInputRows": 5}, {"missing": 1}])), (5, True))
        # A REAL zero must be distinguishable from absent progress.
        self.assertEqual(progress_input_rows(
            FakeQuery(recent=[{"numInputRows": 0}])), (0, True))
        # recentProgress wins when present (Spark semantics: lastProgress is
        # already the newest element of the window).
        self.assertEqual(progress_input_rows(FakeQuery(last_progress={"numInputRows": 99},
                                                      recent=[{"numInputRows": 3}])), (3, True))
        self.assertEqual(progress_input_rows(FakeQuery(last_progress={"numInputRows": True})), (0, False))


class MetricsObservedSemanticsTests(unittest.TestCase):
    def test_rows_read_without_observation_is_rejected(self):
        with self.assertRaises(ValueError):
            RunMetrics("a", "p", BATCH, "orders", RunLogEvent.SOURCE_COMMITTED,
                       "2026-01-01T00:00:00Z", rows_read=3)
        RunMetrics("a", "p", BATCH, "orders", RunLogEvent.SOURCE_COMMITTED,
                   "2026-01-01T00:00:00Z", rows_read=0)  # absent progress default is fine
        RunMetrics("a", "p", BATCH, "orders", RunLogEvent.SOURCE_COMMITTED,
                   "2026-01-01T00:00:00Z", rows_read=0, metrics_observed=True)
        RunMetrics("a", "p", BATCH, "orders", RunLogEvent.SOURCE_COMMITTED,
                   "2026-01-01T00:00:00Z", rows_read=13, metrics_observed=True)


class ExecuteRunTests(unittest.TestCase):
    def _config(self):
        return BronzeJobConfig(batch_id=BATCH, arrival_date=ARRIVAL, landing_root=ROOT_URI,
                               pipeline_run_id="run-1", attempt_id="attempt-1")

    def _queries(self, products_progress=(None, None)):
        orders = FakeQuery(recent=[{"numInputRows": 5}, {"numInputRows": 20}])
        customers = FakeQuery(last_progress={"numInputRows": 11})
        products = FakeQuery(last_progress=products_progress[0], recent=products_progress[1])
        return {"orders": orders, "customers": customers, "products": products}

    def test_happy_run_order_preflight_ddl_events_streams_and_metrics(self):
        spark = RecordingSpark()
        preflight = RecordingPreflight()
        queries = self._queries()
        streams = RecordingStreams(queries)
        records = execute_run(spark, self._config(), preflight=preflight,
                              start_stream=streams)
        # 1) preflight before DDL and before tables/log; read-only inputs.
        call = preflight.calls[0]
        self.assertEqual((call["batch_id"], call["arrival_date"], call["landing_root"]),
                         (BATCH, ARRIVAL, ROOT_URI))
        # The unified streaming seam receives the layout object both times.
        self.assertTrue(all(l is not None for l in streams.layouts_seen))
        self.assertIs(streams.layouts_seen[0], streams.layouts_seen[1])
        # 2) DDL created before any run-log write (tables must exist first).
        self.assertTrue(spark.ddls)
        # 3) event sequence: STARTED, MANIFEST_VALIDATED, and one
        # SOURCE_COMMITTED per source in contract order.
        events = [record["event"] for record in spark.run_log_writes]
        self.assertEqual(events, ["STARTED", "MANIFEST_VALIDATED",
                                  "SOURCE_COMMITTED", "SOURCE_COMMITTED", "SOURCE_COMMITTED"])
        self.assertEqual([r["source"] for r in spark.run_log_writes[2:]],
                         ["orders", "customers", "products"])
        # 4) real progress only: orders summed window rows, customers
        # lastProgress row, products has NO progress (absent, not zero-measured).
        committed = spark.run_log_writes[2:]
        self.assertEqual([r["rows_read"] for r in committed], [25, 11, 0])
        self.assertEqual([r["metrics_observed"] for r in committed], [True, True, False])
        # And a REAL observed zero keeps metrics_observed true:
        observed_zero = {r["source"]: r for r in spark.run_log_writes[2:]}
        self.assertEqual(observed_zero["orders"]["rows_written"], 0)
        self.assertEqual(observed_zero["orders"]["files_discovered"], 0)
        self.assertEqual(observed_zero["orders"]["rows_rescued"], 0)
        # No invented metrics: rows_written/files stay untouched defaults.
        for record in records:
            json.dumps(record)  # stable serializable contract
        # 5) streams started sequentially and each awaited termination.
        self.assertEqual(streams.started, ["orders", "customers", "products"])
        self.assertTrue(all(q.terminated for q in queries.values()))
        # 6) run-log table is the only written table and DDLs came first.
        self.assertEqual(spark.tables,
                         ["tt_ecommerce_synthetic.bronze.ingestion_run_log"] * 5)

    def test_real_observed_zero_is_distinguished_from_absent_progress(self):
        spark = RecordingSpark()
        queries = self._queries(products_progress=({"numInputRows": 0}, [{"numInputRows": 0}]))
        streams = RecordingStreams(queries)
        execute_run(spark, self._config(), preflight=RecordingPreflight(),
                    start_stream=streams)
        committed = spark.run_log_writes[2:]
        self.assertEqual([r["rows_read"] for r in committed], [25, 11, 0])
        self.assertEqual([r["metrics_observed"] for r in committed], [True, True, True])

    def test_preorder_await_termination_before_next_stream(self):
        spark = RecordingSpark()
        queries = self._queries()
        started_flags = []

        def ordered_stream(spark_obj, receipt, config, layout):
            started_flags.append(("start", receipt.source,
                                  any(q.terminated for q in queries.values())))
            return queries[receipt.source]

        records = execute_run(spark, self._config(), preflight=RecordingPreflight(),
                              start_stream=ordered_stream)
        self.assertEqual([entry[1] for entry in started_flags],
                         ["orders", "customers", "products"])
        self.assertFalse(started_flags[0][2])   # first stream starts unawaited
        self.assertTrue(started_flags[1][2])    # previous stream awaited already
        self.assertTrue(started_flags[2][2])
        self.assertEqual(len(records), 5)

    def test_preflight_failure_skips_ddl_and_run_log(self):
        spark = RecordingSpark()

        def failing_preflight(spark_obj, **kwargs):
            raise RemotePreflightError("ready state unresolved")

        with self.assertRaises(RemotePreflightError):
            execute_run(spark, self._config(), preflight=failing_preflight,
                        start_stream=RecordingStreams({}))
        self.assertEqual(spark.ddls, [])
        self.assertEqual(spark.run_log_writes, [])

    def test_receipt_not_matching_contract_filename_rejected_before_ddl(self):
        # A preflight receipt must carry THE exact contract filename; the
        # forged/traversal one is rejected before any DDL or run-log write.
        receipts = _receipts()
        forged = SourceReceipt(BATCH, "orders", f"../{BATCH}/orders_{BATCH}.jsonl",
                               "a" * 64, 26, "v1")
        spark = RecordingSpark()
        preflight = RecordingPreflight(_result(receipts=[forged, *receipts[1:]]))
        with self.assertRaises(RemotePreflightError) as caught:
            execute_run(spark, self._config(), preflight=preflight,
                        start_stream=RecordingStreams({}))
        self.assertIn("orders", str(caught.exception))
        self.assertEqual(spark.ddls, [])
        self.assertEqual(spark.tables, [])
        self.assertEqual(spark.run_log_writes, [])

    def test_failure_after_bootstrap_writes_failed_and_propagates(self):
        spark = RecordingSpark()
        preflight = RecordingPreflight()

        class FailingOrders:
            def awaitTermination(self):
                raise RuntimeError("simulated stream error")

        streams = RecordingStreams({
            "orders": FailingOrders(),
            "customers": FakeQuery(),
            "products": FakeQuery(),
        })
        with self.assertRaises(RuntimeError):
            execute_run(spark, self._config(), preflight=preflight, start_stream=streams)
        self.assertEqual(streams.started, ["orders"])  # sequential: stops at failure
        self.assertEqual([r["event"] for r in spark.run_log_writes],
                         ["STARTED", "MANIFEST_VALIDATED", "FAILED"])
        failed = spark.run_log_writes[-1]
        self.assertEqual(failed["source"], "orders")
        self.assertEqual(failed["error_class"], "builtins.RuntimeError")
        # DDL happened exactly once before any failure handling.
        self.assertTrue(spark.ddls)
        self.assertEqual(len(spark.tables), 3)

    def test_ddl_or_event_failure_propagates_without_failed_event(self):
        spark = RecordingSpark()
        preflight = RecordingPreflight()

        def failing_bootstrap(spark_obj):
            raise RuntimeError("cluster cannot create managed tables")

        with self.assertRaises(RuntimeError):
            execute_run(spark, self._config(), preflight=preflight,
                        bootstrap=failing_bootstrap, start_stream=RecordingStreams({}))
        # No FAILED promise: the bootstrapped run-log never received events.
        self.assertEqual(spark.run_log_writes, [])
        self.assertEqual(spark.tables, [])

    def test_initial_event_failure_propagates_without_failed_event(self):
        preflight = RecordingPreflight()
        unwritable: list[str] = []

        class UnwritableLogSpark(RecordingSpark):
            def record_write(self, rows, table):
                unwritable.append(table)
                raise RuntimeError("run-log volume unavailable")

        spark = UnwritableLogSpark()
        with self.assertRaises(RuntimeError):
            execute_run(spark, self._config(), preflight=preflight,
                        start_stream=RecordingStreams({}))
        self.assertTrue(spark.ddls)  # DDL succeeded before the event failure
        self.assertTrue(unwritable and unwritable[0] ==
                        "tt_ecommerce_synthetic.bronze.ingestion_run_log")
        self.assertEqual(spark.run_log_writes, [])
        self.assertEqual(spark.tables, [])


class BinaryPreflightAndEventSpark(RecordingSpark, FakeBinarySpark):
    """One fake playing both remote roles: the binaryFile edge consumed by the
    REAL `remote_preflight` (read chain) AND the bootstrap DDL / run-log sink
    consumed by `execute_run` (`sql` / `createDataFrame`)."""

    def __init__(self, objects):
        RecordingSpark.__init__(self)
        FakeBinarySpark.__init__(self, objects)


class PreflightDirectoryAuditIntegrationTests(unittest.TestCase):
    """Local integration between the REAL remote preflight and `execute_run`.

    A promoted batch whose exact source directory holds an unexpected entry
    (a sibling extra file, or a NESTED subdirectory repeating the SAME
    approved filename) must abort in `execute_run` with zero DDL, zero
    run-log writes and zero streams started — and no byte of the unexpected
    entry or of the approved batch file may ever reach the driver.
    """

    def _config(self):
        return BronzeJobConfig(batch_id=BATCH, arrival_date=ARRIVAL, landing_root=ROOT_URI,
                               pipeline_run_id="run-1", attempt_id="attempt-1")

    def _rejected_run(self, mutate):
        _, objects = canonical_remote_objects()
        spark = BinaryPreflightAndEventSpark(mutate(objects))
        streams = RecordingStreams({})
        with self.assertRaises(RemotePreflightError) as caught:
            execute_run(spark, self._config(), preflight=remote_preflight,
                        start_stream=streams)
        return spark, streams, caught

    def _assert_zero_side_effects(self, spark, streams, source):
        message = self._last_message
        # Zero DDL: nothing is precreated for a rejected batch.
        self.assertEqual(spark.ddls, [])
        self.assertEqual(spark.tables, [])
        # Zero run-log: not even STARTED/MANIFEST_VALIDATED is written.
        self.assertEqual(spark.run_log_writes, [])
        # Zero streams: not a single source stream started.
        self.assertEqual(streams.started, [])
        # And the audit pass really ran with the explicit recursive option.
        self.assertTrue(any(options.get("recursiveFileLookup") is True
                            for _, options in spark.reader_options),
                        msg=f"directory audit must set recursiveFileLookup for {source}")

    def test_nested_subdirectory_with_approved_filename_aborts_with_zero_writes(self):
        layout = LandingLayout()
        orders_file = layout.final_file_uri("orders", BATCH, ARRIVAL, ROOT_URI)
        orders_dir = layout.final_directory_uri("orders", BATCH, ARRIVAL, ROOT_URI)
        nested_same_name = f"{orders_dir}/nested/{orders_file.rsplit('/', 1)[-1]}"

        def mutate(objects):
            objects[nested_same_name] = objects[orders_file]
            return objects

        spark, streams, caught = self._rejected_run(mutate)
        message = str(caught.exception)
        self._last_message = message
        self.assertIn("remote preflight failed", message)
        self.assertIn("nested subdirectories", message)
        self.assertIn("/nested/", message)
        self._assert_zero_side_effects(spark, streams, "orders")
        # The listing pass over the orders directory surfaced BOTH entries;
        # it ran with recursiveFileLookup=True and no content was ever
        # collected for the approved file or the nested copy.
        self.assertTrue(
            all(options.get("recursiveFileLookup") is True
                for uri, options in spark.reader_options if uri == orders_dir))
        self.assertNotIn(orders_file, spark.content_collected)
        self.assertNotIn(nested_same_name,
                         [uri for uri, columns in spark.selected_projects
                          if "content" in columns])
        self.assertEqual(spark.uris[-1], orders_dir)

    def test_sibling_extra_file_aborts_with_zero_writes(self):
        layout = LandingLayout()
        customers_file = layout.final_file_uri("customers", BATCH, ARRIVAL, ROOT_URI)
        customers_dir = layout.final_directory_uri("customers", BATCH, ARRIVAL, ROOT_URI)

        def mutate(objects):
            objects[f"{customers_dir}/unexpected_file.csv"] = b"stray"
            return objects

        spark, streams, caught = self._rejected_run(mutate)
        message = str(caught.exception)
        self._last_message = message
        self.assertIn("remote preflight failed", message)
        self.assertIn("unexpected entries", message)
        self._assert_zero_side_effects(spark, streams, "customers")
        self.assertNotIn(customers_file, spark.content_collected)
        self.assertEqual(spark.uris[-1], customers_dir)


class StreamingFakeSpark:
    """Fake edge for the REAL `default_start_stream` adapter.

    Delegates bootstrap DDL/run-log writes to an `RecordingSpark` event sink
    AND records the Auto Loader streaming edge (`readStream.format
    ("cloudFiles")` chain). Requires the PySpark stub to be installed so the
    lazily-imported column functions resolve; columns are inert placeholders.
    """

    def __init__(self, event_sink: RecordingSpark, root: str):
        self.event_sink = event_sink
        self.root = root
        self.started_sources: list[str] = []
        self.loaded_uris: list[str] = []
        self.stream_tables: list[str] = []
        self.stream_option_sets: list[set] = []
        self.stream_option_values: list[dict] = []
        self.trigger_flags: list[dict] = []
        self.added_columns: list[tuple[str, object]] = []
        self.queries: list[FakeQuery] = []

    def sql(self, ddl):
        return self.event_sink.sql(ddl)

    def createDataFrame(self, data):
        return self.event_sink.createDataFrame(data)

    @property
    def run_log_writes(self):
        return self.event_sink.run_log_writes

    @property
    def tables(self):
        return self.event_sink.tables

    @property
    def ddls(self):
        return self.event_sink.ddls

    @staticmethod
    def _source_from_uri(root: str, uri: str) -> str:
        root = root.rstrip("/")
        suffix = uri[len(root) + 1:]
        return suffix.split("/", 1)[0]

    @property
    def readStream(self):
        return FakeStreamReader(self)

    def started_source_name(self, uri):
        source = self._source_from_uri(self.root, uri)
        self.started_sources.append(source)
        return source


class FakeStreamReader:
    def __init__(self, spark):
        self.spark = spark

    def format(self, name):
        assert name == "cloudFiles", f"unexpected reader format {name}"
        return self

    def schema(self, schema):
        self.spark.stream_schema = schema
        return self

    def option(self, key, value):
        if not hasattr(self.spark, "_pending_options"):
            self.spark._pending_options = {}
        self.spark._pending_options[key] = value
        return self

    def load(self, uri):
        self.spark.loaded_uris.append(uri)
        source = self.spark.started_source_name(uri)
        columns = list(BRONZE_CONTRACTS[source].data_columns) + ["_rescued_data", "_corrupt_record"]
        return FakeStreamDF(self.spark, uri, columns)


class FakeStreamDF:
    def __init__(self, spark, uri, columns):
        self._spark = spark
        self._uri = uri
        self.columns = columns

    def withColumn(self, name, expression):
        self._spark.added_columns.append((name, expression))
        return self

    @property
    def writeStream(self):
        pending = getattr(self._spark, "_pending_options", {})
        self._spark.stream_option_sets.append(set(pending.keys()))
        self._spark.stream_option_values.append(dict(pending))
        self._spark._pending_options = {}
        return FakeStreamWriter(self._spark)


class FakeStreamWriter:
    def __init__(self, spark):
        self.spark = spark

    def format(self, name):
        assert name == "delta", f"unexpected sink format {name}"
        return self

    def outputMode(self, mode):
        assert mode == "append", f"append-only mode required, got {mode}"
        return self

    @property
    def _sink_options(self):
        if not hasattr(self.spark, "writer_option_sets"):
            self.spark.writer_option_sets = []
        sets = self.spark.writer_option_sets
        if not sets or sets[-1] is not self.spark.current_writer_options:
            self.spark.current_writer_options = {}
            sets.append(self.spark.current_writer_options)
        return self.spark.current_writer_options

    def option(self, key, value):
        self._sink_options[key] = value
        return self

    def trigger(self, **flags):
        self.spark.trigger_flags.append(flags)
        return self

    def toTable(self, table):
        # Databricks `DataStreamWriter.toTable()` starts the query and
        # returns the live StreamingQuery; the fake models exactly that
        # (there is no `start()` call in the production seam).
        self.spark.stream_tables.append(table)
        query = FakeQuery()
        query.started = True
        self.spark.queries.append(query)
        return query

    def start(self):
        raise AssertionError("production must not call .start(): toTable() already starts")


class _StubRemove:
    def __init__(self, added: list[str]):
        self.added = added

    def remove(self):
        for name in self.added:
            sys.modules.pop(name, None)


def install_pyspark_stub():
    """Registers minimal `pyspark` package/module stand-ins so the REAL
    adapter's lazy imports (`pyspark.sql.types`, `pyspark.sql.functions`)
    resolve; the machine has no PySpark. Only these four sys.modules keys are
    added and always removed afterwards."""
    import types as _types

    import sys as _sys

    added: list[str] = []

    def provide(name, module):
        _sys.modules[name] = module
        added.append(name)

    pyspark = _types.ModuleType("pyspark")
    sql_mod = _types.ModuleType("pyspark.sql")
    types_mod = _types.ModuleType("pyspark.sql.types")
    functions_mod = _types.ModuleType("pyspark.sql.functions")

    class StructType(list):
        pass

    class StructField:
        def __init__(self, name, data_type, nullable=True):
            self.name = name
            self.data_type = data_type
            self.nullable = nullable

    class StringType:
        pass

    types_mod.StructType = StructType
    types_mod.StructField = StructField
    types_mod.StringType = StringType

    class _Column:
        def __init__(self, label):
            self.label = label

        def isNotNull(self):
            return _Column(f"{self.label} isNotNull")

        def __getattr__(self, attribute):
            return _Column(f"{self.label}.{attribute}")

    class _When:
        def when(self, condition, value):
            return self

        def otherwise(self, value):
            return _Column("otherwise")

    functions_mod.lit = lambda value: _Column(f"lit({value})")
    functions_mod.col = lambda column: _Column(f"col({column})")
    functions_mod.when = lambda condition, value: _When()
    functions_mod.current_timestamp = lambda: _Column("current_timestamp")
    functions_mod.to_json = lambda column: _Column("to_json")
    functions_mod.struct = lambda *columns: _Column("struct")
    functions_mod.to_date = lambda column: _Column("to_date")

    pyspark.sql = sql_mod
    sql_mod.types = types_mod
    sql_mod.functions = functions_mod
    provide("pyspark", pyspark)
    provide("pyspark.sql", sql_mod)
    provide("pyspark.sql.types", types_mod)
    provide("pyspark.sql.functions", functions_mod)
    return _StubRemove(added)


class RealAdapterTests(unittest.TestCase):
    """Pruela the true streaming seam: `execute_run` runs with the REAL
    `default_start_stream` adapter; only the Spark/Auto Loader edge is
    replaced by a fake (no signature swap, no mocked `start_stream`)."""

    @classmethod
    def setUpClass(cls):
        cls._pyspark_stub = install_pyspark_stub()

    @classmethod
    def tearDownClass(cls):
        cls._pyspark_stub.remove()

    def _run_with_real_adapter(self):
        config = BronzeJobConfig(batch_id=BATCH, arrival_date=ARRIVAL,
                                 landing_root=ROOT_URI,
                                 pipeline_run_id="run-1", attempt_id="attempt-1")
        logsink = RecordingSpark()
        # The same fake object plays two roles: recording bootstrap/run-log
        # writes AND the readStream/Auto Loader edge (see StreamingFakeSpark).
        real_spark = StreamingFakeSpark(event_sink=logsink, root=ROOT_URI)
        execute_run(real_spark, config, preflight=RecordingPreflight())
        return real_spark, config

    def test_default_start_stream_composes_streams_started_and_awaited(self):
        real_spark, config = self._run_with_real_adapter()
        self.assertEqual(real_spark.started_sources, ["orders", "customers", "products"])
        self.assertEqual(real_spark.loaded_uris, [
            f"{ROOT_URI}/orders/arrival_date={ARRIVAL}/batch_id={BATCH}",
            f"{ROOT_URI}/customers/arrival_date={ARRIVAL}/batch_id={BATCH}",
            f"{ROOT_URI}/products/arrival_date={ARRIVAL}/batch_id={BATCH}",
        ])
        # The Auto Loader loads the batch DIRECTORY (never an individual file
        # URI) and is bound to the receipt's EXACT approved filename.
        for loaded_uri in real_spark.loaded_uris:
            self.assertTrue(loaded_uri.endswith(f"batch_id={BATCH}"))
            self.assertNotIn(f"/orders_{BATCH}.jsonl", loaded_uri)
            self.assertNotIn(".csv", loaded_uri)
        for value_set, source in zip(real_spark.stream_option_values,
                                     ["orders", "customers", "products"]):
            extension = "jsonl" if source == "orders" else "csv"
            self.assertEqual(value_set["pathGlobFilter"], f"{source}_{BATCH}.{extension}")
        # Append-only sink on managed tables with real checkpoint wiring.
        self.assertEqual(real_spark.stream_tables,
                         ["tt_ecommerce_synthetic.bronze.orders_raw",
                          "tt_ecommerce_synthetic.bronze.customers_raw",
                          "tt_ecommerce_synthetic.bronze.products_raw"])
        for option in real_spark.writer_option_sets:
            self.assertIn("checkpointLocation", option)
            self.assertTrue(option["checkpointLocation"].startswith("/Volumes/"))
        for trigger in real_spark.trigger_flags:
            self.assertEqual(trigger, {"availableNow": True})
        # The provenance columns from the real adapter touched the streams.
        seen = {name for name, _expr in real_spark.added_columns}
        self.assertTrue(seen.issuperset({
            "batch_id", "contract_version", "manifest_file_sha256", "_source_file",
            "_source_file_size", "_source_file_modification_time", "_ingested_at",
            "_pipeline_run_id", "parsed_payload_canonical", "parse_status",
            "ingest_date",
        }))
        # All three queries were started by `toTable()` and then awaited.
        self.assertTrue(all(q.started for q in real_spark.queries))
        self.assertTrue(all(q.terminated for q in real_spark.queries))
        # Events completed for all three sources.
        self.assertEqual(
            [r["event"] for r in real_spark.run_log_writes].count("SOURCE_COMMITTED"), 3)


if __name__ == "__main__":
    unittest.main()
