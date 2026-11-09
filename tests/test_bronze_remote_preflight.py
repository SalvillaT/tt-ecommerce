"""Remote preflight tests (Fase B) with a fake Spark binaryFile reader.

No Azure, no Databricks remote: objects are simulated per abfss URI; the fake
Spark validates the `binaryFile` contract (`read.format("binaryFile").load(uri).select("length","content").limit(1).collect()`),
and the preflight must read ONLY the ready marker, the control manifest and
the three canonical final URIs (never `_staging`, never a source root).
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Mapping

from pipelines.bronze.layout import LandingLayout, join_landing_uri, landing_relative_path
from pipelines.bronze.manifest import ManifestPreflightError
from pipelines.bronze.remote_preflight import (
    MAX_DIRECTORY_ENTRIES,
    MAX_OBJECT_BYTES,
    RemotePreflightError,
    list_batch_directory,
    read_binary_object,
    remote_preflight,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "data" / "sample" / "DEMO-001"
ROOT_URI = "abfss://landing@example.dfs.core.windows.net"
BATCH = "DEMO-001"
ARRIVAL = "2026-01-10"


def _canonical_ready_bytes(manifest_sha256: str, *, include_sha: bool = True) -> bytes:
    payload = {"batch_id": BATCH, "arrival_date": ARRIVAL}
    if include_sha:
        payload["manifest_sha256"] = manifest_sha256
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


class FakeReader:
    def __init__(self, spark):
        self._spark = spark
        self._filter_expression: str | None = None
        self._current = None
        self._listing: list[str] | None = None
        self._limit: int | None = None
        self._pending_options: dict = {}

    def format(self, name):
        self._spark.formats.append(name)
        if name != "binaryFile":
            raise AssertionError(f"preflight must use binaryFile, got {name}")
        return self

    def option(self, key, value):
        self._pending_options[key] = value
        return self

    def load(self, uri):
        spark = self._spark
        history = spark._read_history.setdefault(uri, [])
        if history and history[-1] == ("length",) and uri not in spark._hook_fired:
            # The BETWEEN-passes mutation hook: exactly the race the guarded
            # second read exists to contain.
            spark._hook_fired.add(uri)
            spark._mutate_between_passes(uri, spark.objects)
        spark.uris.append(uri)
        self._current = spark.objects.get(uri)  # None -> missing object
        self._listing = None
        self._limit = None
        spark.reader_options.append((uri, dict(self._pending_options)))
        self._pending_options = {}
        if self._current is None:
            # Directory listing pass (metadata-only): the fake mirrors the
            # Spark semantics the option contract relies on. WITHOUT the
            # explicit `recursiveFileLookup` option the directory read is
            # NON-recursive (only direct children); WITH it, keys under
            # nested subdirectories surface, exactly like the guaranteed
            # recursive listing the preflight audit requires.
            recursive = self._recursive_file_lookup
            prefix = uri.rstrip("/") + "/"
            if recursive:
                keys = [key for key in spark.objects if key.startswith(prefix)]
            else:
                keys = [key for key in spark.objects
                        if key.startswith(prefix) and "/" not in key[len(prefix):]]
            self._listing = sorted(keys)
            spark.listing_uris.append(uri)
        return self

    @property
    def _recursive_file_lookup(self) -> bool:
        value = self._spark.reader_options[-1][1].get("recursiveFileLookup")
        return value in (True, "true", "True", 1, "1")

    def filter(self, expression):
        self._filter_expression = expression
        self._spark.filters.append((self._spark.uris[-1], expression))
        return self

    def select(self, *columns):
        uri = self._spark.uris[-1]
        self._spark._read_history.setdefault(uri, []).append(columns)
        if self._filter_expression is not None:
            self._spark.filtered_projects.append((uri, self._filter_expression, columns))
        self._spark.selected_projects.append((uri, columns))
        if set(columns) - {"length", "content"}:
            if set(columns) != {"_metadata.file_path"}:
                raise AssertionError(f"unexpected binaryFile projection: {columns}")
        return self

    def limit(self, count):
        self._limit = count
        return self

    def collect(self):
        if self._listing is not None:
            # Metadata-only directory listing pass: driver receives ONLY path
            # strings of the listed entries (after the applied limit bound).
            uri = self._spark.uris[-1]
            entries = self._listing if self._limit is None else self._listing[:self._limit]
            self._spark.listings.append((uri, tuple(entries)))
            projection = set(self._spark.selected_projects[-1][1])
            rows = [{key: value for key, value in
                     {"_metadata.file_path": path}.items() if key in projection}
                    for path in entries]
            return rows
        if self._current is None:
            return []
        uri = self._spark.uris[-1]
        projection = set(self._spark.selected_projects[-1][1])
        row_length = len(self._current)
        if self._filter_expression is not None:
            match = re.fullmatch(r"length\s*<=\s*(\d+)", self._filter_expression)
            if not match:
                raise AssertionError(
                    f"fake only understands 'length <= N' filters, got {self._filter_expression!r}")
            if row_length > int(match.group(1)):
                self.spark_note_blocked()
                return []  # guarded out BEFORE content projection/collect
        if "content" in projection:
            self._spark._content_collected.append(uri)
        return [{key: value for key, value in
                 {"length": row_length, "content": self._current}.items()
                 if key in projection}]

    def spark_note_blocked(self):
        """Record that the guard dropped the row before content projection."""
        self._spark.guarded_out.append((self._spark.uris[-1], self._filter_expression))


class FakeBinarySpark:
    def __init__(self, objects: dict[str, bytes], mutate_between_passes=None):
        self.objects = objects
        self.formats: list[str] = []
        self.uris: list[str] = []
        self.listing_uris: list[str] = []  # directory listing loads (metadata-only)
        self.reader_options: list[tuple[str, dict]] = []  # (uri, options set before a load)
        self.selected_projects: list[tuple[str, tuple]] = []
        self.filtered_projects: list[tuple[str, str, tuple]] = []
        self.filters: list[tuple[str, str]] = []
        self.listings: list[tuple[str, tuple]] = []  # (directory_uri, collected paths)
        self.guarded_out: list[tuple[str, str]] = []
        self._read_history: dict[str, list] = {}
        self._hook_fired: set[str] = set()
        self._content_collected: list[str] = []
        self._mutate_between_passes = mutate_between_passes or (lambda uri, objects: None)

    @property
    def read(self):
        # spark.read chain accessor, like SparkSession.read.
        return FakeReader(self)

    @property
    def content_selected_uris(self) -> list[str]:
        """URIs whose REQUESTED projection includes `content` (the guard may
        still drop the row before materialization; see `content_collected`)."""
        return [uri for uri, columns in self.selected_projects if "content" in columns]

    @property
    def content_collected(self) -> list[str]:
        """URIs for which `content` was actually returned to the driver (the
        real transfer guarantee: the guarded pass must never add a URI here
        when its row was filtered out by the length guard)."""
        return list(self._content_collected)


class SparkConnectRow:
    """Test double for a serverless Spark Connect `Row`.

    Mimics the exact shape from the reported cloud failure: index access
    works, `asDict(recursive=True)` works, but `dict(row)` fails (the Row
    iterates tuple-like over bare values) and `.get` is absent.
    """

    def __init__(self, values: dict):
        self._values = dict(values)

    def __getitem__(self, key):
        return self._values[key]

    def __iter__(self):
        # Tuple-like Row iteration (bare values): `dict(row)` then fails with
        # the reported "cannot convert dictionary update sequence element #0
        # to a sequence" TypeError, exactly like the Spark Connect run.
        return iter(self._values.values())

    def asDict(self, recursive: bool = False):
        if recursive:
            return {key: dict(value) if isinstance(value, Mapping) else value
                    for key, value in self._values.items()}
        return dict(self._values)


class SparkConnectRowsSpark(FakeBinarySpark):
    """FakeBinarySpark whose collect() returns Spark Connect-style rows."""

    @property
    def read(self):
        class ConnectReader(FakeReader):
            def collect(self):
                return [SparkConnectRow(row) for row in super().collect()]

        return ConnectReader(self)


class FlatFilePathSpark(FakeBinarySpark):
    """Fake whose metadata-only listing pass emits REAL-Spark-shaped rows.

    Real Spark commonly ALIASES `select("_metadata.file_path")` to a plain
    top-level `file_path` column, so the listing rows normalize to just
    {"file_path": <uri>}. All other passes behave like FakeBinarySpark
    ({"length": n, "content": b} object rows). Only the flatten LISTING
    representation differs.
    """

    @property
    def read(self):
        class FlatRowsReader(FakeReader):
            def collect(self):
                if self._listing is not None:
                    uri = self._spark.uris[-1]
                    entries = self._listing if self._limit is None else self._listing[:self._limit]
                    self._spark.listings.append((uri, tuple(entries)))
                    return [{"file_path": path} for path in entries]
                return super().collect()

        return FlatRowsReader(self)


class SparkConnectFlatFilePathSpark(FakeBinarySpark):
    """Serverless-regression shape: Spark Connect rows AND the real-Spark
    flat `file_path` listing alias (the exact reported cloud conditions).
    """

    @property
    def read(self):
        class ConnectFlatReader(FakeReader):
            def collect(self):
                if self._listing is not None:
                    uri = self._spark.uris[-1]
                    entries = self._listing if self._limit is None else self._listing[:self._limit]
                    self._spark.listings.append((uri, tuple(entries)))
                    return [SparkConnectRow({"file_path": path}) for path in entries]
                return [SparkConnectRow(row) for row in super().collect()]

        return ConnectFlatReader(self)


def canonical_remote_objects(ready_bytes: bytes | None = None) -> tuple[str, dict[str, bytes]]:
    """The approved promoted batch as fake binaryFile objects under ROOT_URI.

    Module-level so other suites (e.g. the entrypoint integration tests) can
    reuse the exact same fixture without instantiating the TestCase.
    """
    manifest_sha = hashlib.sha256((FIXTURE / f"manifest_{BATCH}.json").read_bytes()).hexdigest()
    ready = ready_bytes if ready_bytes is not None else _canonical_ready_bytes(manifest_sha, include_sha=True)
    objects = {
        LandingLayout().ready_uri(ROOT_URI, BATCH): ready,
        LandingLayout().manifest_uri(ROOT_URI, BATCH): (FIXTURE / f"manifest_{BATCH}.json").read_bytes(),
    }
    layout = LandingLayout()
    for source, filename in (("orders", "orders_DEMO-001.jsonl"),
                             ("customers", "customers_DEMO-001.csv"),
                             ("products", "products_DEMO-001.csv")):
        uri = layout.final_file_uri(source, BATCH, ARRIVAL, ROOT_URI)
        objects[uri] = (FIXTURE / filename).read_bytes()
    return manifest_sha, objects


class RemotePreflightContract(unittest.TestCase):
    def _remote_bytes(self, ready_bytes: bytes | None = None) -> dict[str, bytes]:
        return canonical_remote_objects(ready_bytes)[1]

    def _preflight(self, mutate=None, *, spark=None, batch=BATCH, arrival=ARRIVAL, ready_bytes=None):
        objects = self._remote_bytes(ready_bytes=ready_bytes)
        if mutate is not None:
            objects = mutate(objects)
        fake_spark = spark or FakeBinarySpark(objects)
        return remote_preflight(fake_spark, landing_root=ROOT_URI, batch_id=batch,
                                arrival_date=arrival), fake_spark

    def test_valid_preflight_reads_only_approved_paths_and_verifies_bytes(self):
        result, fake_spark = self._preflight()
        self.assertEqual([r.source for r in result.receipts], ["orders", "customers", "products"])
        self.assertEqual([r.rows_declared for r in result.receipts], [26, 11, 7])
        self.assertEqual(result.total_rows, 44)
        self.assertEqual(result.ready_payload["batch_id"], BATCH)
        layout = LandingLayout()
        self.assertEqual(result.ready_uri, layout.ready_uri(ROOT_URI, BATCH))
        self.assertEqual(result.manifest_uri, layout.manifest_uri(ROOT_URI, BATCH))
        expected_final = {source: layout.final_file_uri(source, BATCH, ARRIVAL, ROOT_URI)
                          for source in ("orders", "customers", "products")}
        self.assertEqual(dict(result.source_uris), expected_final)
        requested = set(fake_spark.uris)
        self.assertTrue(requested)
        for uri in requested:
            self.assertNotIn("_staging", uri)
            self.assertTrue(uri.startswith(ROOT_URI))
        self.assertEqual(len(set(fake_spark.uris)), 8)  # 5 objects + 3 batch directories
        self.assertEqual(len(fake_spark.uris), 13)  # 2 object reads each + 1 directory listing per source
        # Per-batch canonical grain, never a source root.
        orders_uri = expected_final["orders"]
        self.assertIn(f"arrival_date={ARRIVAL}", orders_uri)
        self.assertIn(f"batch_id={BATCH}", orders_uri)
        # Exactly one metadata-only listing per source, over the batch dir,
        # and every returned entry is exactly the approved file.
        file_by_dir = {}
        for source in ("orders", "customers", "products"):
            file_by_dir[layout.final_directory_uri(source, BATCH, ARRIVAL, ROOT_URI)] = (
                layout.final_file_uri(source, BATCH, ARRIVAL, ROOT_URI))
        self.assertEqual(dict(fake_spark.listings),
                         {dir_uri: (file_uri,) for dir_uri, file_uri in file_by_dir.items()})

    def test_logical_paths_preserved_separately_from_physical_uri(self):
        layout = LandingLayout()
        logical = layout.final_file("orders", BATCH, ARRIVAL)
        self.assertTrue(logical.startswith("landing/"))
        self.assertEqual(landing_relative_path(logical),
                         f"orders/arrival_date={ARRIVAL}/batch_id={BATCH}/orders_{BATCH}.jsonl")
        self.assertEqual(join_landing_uri(ROOT_URI, landing_relative_path(logical)),
                         f"{ROOT_URI}/orders/arrival_date={ARRIVAL}/batch_id={BATCH}/orders_{BATCH}.jsonl")

    def test_join_rejects_unsafe_roots_and_traversal(self):
        for bad_root in ("s3://landing@bucket", "abfss://", "abfss://host/..",
                         "abfss://host/x\\y", "", "abfss://host/orders/.."):
            with self.assertRaises(ValueError):
                join_landing_uri(bad_root, "orders/x.json")
        for bad_rel in ("../orders", "orders\\x", "/orders", "orders/..", ""):
            with self.assertRaises(ValueError):
                join_landing_uri(ROOT_URI, bad_rel)
        with self.assertRaises(ValueError):
            landing_relative_path("landing/../x")
        with self.assertRaises(ValueError):
            landing_relative_path("not_landing/x")

    def test_reads_and_verifies_remote_sha_and_rows(self):
        result, _ = self._preflight()
        expected_sha = hashlib.sha256(
            (FIXTURE / "customers_DEMO-001.csv").read_bytes()).hexdigest()
        receipt = next(r for r in result.receipts if r.source == "customers")
        self.assertEqual(receipt.sha256, expected_sha)
        self.assertEqual(receipt.rows_declared, 11)

    def test_ready_batch_mismatch_rejected(self):
        bad = (json.dumps({"batch_id": "OTHER"}, sort_keys=True,
                          separators=(",", ":")) + "\n").encode("utf-8")

        def mutate(objects):
            objects[LandingLayout().ready_uri(ROOT_URI, BATCH)] = bad
            return objects

        with self.assertRaises(RemotePreflightError) as caught:
            self._preflight(mutate)
        self.assertIn("batch_id mismatch", str(caught.exception))

    def test_missing_ready_object_is_an_error(self):
        def mutate(objects):
            objects.pop(LandingLayout().ready_uri(ROOT_URI, BATCH))
            return objects

        with self.assertRaises(RemotePreflightError):
            self._preflight(mutate)

    def test_sha_divergence_rejected(self):
        def mutate(objects):
            orders_uri = LandingLayout().final_file_uri("orders", BATCH, ARRIVAL, ROOT_URI)
            objects[orders_uri] = objects[orders_uri] + b"\n"
            return objects

        with self.assertRaises(RemotePreflightError) as caught:
            self._preflight(mutate)
        self.assertIn("checksum mismatch", str(caught.exception))
        self.assertIn("remote preflight rejected", str(caught.exception))

    def test_manifest_reparsed_names_still_canonical(self):
        # A control manifest whose declared file set deviates from the v1
        # contract is rejected in the pure header audit, after the ready
        # marker (attesting THIS mutated manifest) passes.
        manifest_json = (FIXTURE / f"manifest_{BATCH}.json").read_bytes()
        manifest = json.loads(manifest_json.decode("utf-8"))
        manifest["files"][2]["file"] = "products_DEMO-999.csv"
        mutated_manifest = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
        mutated_sha = hashlib.sha256(mutated_manifest).hexdigest()
        ready_attesting_mutation = _canonical_ready_bytes(mutated_sha, include_sha=True)
        layout = LandingLayout()
        objects = {
            layout.ready_uri(ROOT_URI, BATCH): ready_attesting_mutation,
            layout.manifest_uri(ROOT_URI, BATCH): mutated_manifest,
        }
        objects[layout.final_file_uri("orders", BATCH, ARRIVAL, ROOT_URI)] = (
            FIXTURE / f"orders_{BATCH}.jsonl").read_bytes()
        objects[layout.final_file_uri("customers", BATCH, ARRIVAL, ROOT_URI)] = (
            FIXTURE / f"customers_{BATCH}.csv").read_bytes()
        objects[layout.final_file_uri("products", BATCH, ARRIVAL, ROOT_URI)] = (
            FIXTURE / f"products_{BATCH}.csv").read_bytes()
        spark = FakeBinarySpark(objects)
        with self.assertRaises(RemotePreflightError) as caught:
            remote_preflight(spark, landing_root=ROOT_URI, batch_id=BATCH, arrival_date=ARRIVAL)
        self.assertIn("file set differs", str(caught.exception))

    def test_ready_manifest_sha_attestation_checked(self):
        def mutate(objects):
            ready_uri = LandingLayout().ready_uri(ROOT_URI, BATCH)
            payload = json.loads(objects[ready_uri].decode("utf-8"))
            payload["manifest_sha256"] = "1" * 64
            objects[ready_uri] = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
            return objects

        with self.assertRaises(RemotePreflightError) as caught:
            self._preflight(mutate)
        self.assertIn("attest", str(caught.exception))

    def test_row_count_mismatch_rejected(self):
        # The control manifest declares fewer customers rows than the bytes
        # hold. The ready marker attests THIS manifest (so the attestation
        # check passes), and the pure row-count audit is what rejects the
        # batch. The control manifest is what carries the mutation.
        manifest_json = (FIXTURE / f"manifest_{BATCH}.json").read_bytes()
        manifest = json.loads(manifest_json.decode("utf-8"))
        for item in manifest["files"]:
            if item["file"].startswith("customers"):
                item["rows"] = 10
        mutated_manifest = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
        mutated_sha = hashlib.sha256(mutated_manifest).hexdigest()
        ready_attesting_mutation = _canonical_ready_bytes(mutated_sha, include_sha=True)
        layout = LandingLayout()
        objects = {
            layout.ready_uri(ROOT_URI, BATCH): ready_attesting_mutation,
            layout.manifest_uri(ROOT_URI, BATCH): mutated_manifest,
        }
        objects[layout.final_file_uri("orders", BATCH, ARRIVAL, ROOT_URI)] = (
            FIXTURE / f"orders_{BATCH}.jsonl").read_bytes()
        objects[layout.final_file_uri("customers", BATCH, ARRIVAL, ROOT_URI)] = (
            FIXTURE / f"customers_{BATCH}.csv").read_bytes()
        objects[layout.final_file_uri("products", BATCH, ARRIVAL, ROOT_URI)] = (
            FIXTURE / f"products_{BATCH}.csv").read_bytes()
        spark = FakeBinarySpark(objects)
        with self.assertRaises(RemotePreflightError) as caught:
            remote_preflight(spark, landing_root=ROOT_URI, batch_id=BATCH, arrival_date=ARRIVAL)
        self.assertIn("row count mismatch", str(caught.exception))

    def test_oversized_object_rejected_before_driver(self):
        def mutate(objects):
            orders_uri = LandingLayout().final_file_uri("orders", BATCH, ARRIVAL, ROOT_URI)
            objects[orders_uri] = b"x" * (MAX_OBJECT_BYTES + 1)
            return objects

        with self.assertRaises(RemotePreflightError) as caught:
            self._preflight(mutate)
        self.assertIn("driver limit", str(caught.exception))

    def test_length_read_precedes_content_read(self):
        result_spark = self._preflight()[1]
        # Each canonical OBJECT is read exactly twice: length first, then a
        # GUARDED length+content pass only after the limit check. Directory
        # audits are separate metadata-only (path-only) listing passes.
        object_requests: dict[str, list] = {}
        listing_requests: dict[str, list] = {}
        for uri, columns in result_spark.selected_projects:
            if columns == ("_metadata.file_path",):
                listing_requests.setdefault(uri, []).append(columns)
            else:
                object_requests.setdefault(uri, []).append(columns)
        self.assertEqual(len(object_requests), 5)
        for uri, projections in object_requests.items():
            self.assertEqual(projections, [("length",), ("length", "content")])
        # The guarded pass applies the length filter BEFORE the projection,
        # so a mutation between passes cannot transfer content to the driver.
        self.assertEqual(len(result_spark.filters), 5)
        for uri, expression in result_spark.filters:
            self.assertEqual(expression, f"length <= {MAX_OBJECT_BYTES}")
        # The directory audits never request content or any other column.
        self.assertEqual(len(listing_requests), 3)
        for uri, projections in listing_requests.items():
            self.assertEqual(projections, [("_metadata.file_path",)])

    def test_directory_listing_requires_explicit_recursive_file_lookup(self):
        # The audit's listing pass MUST explicitly set recursiveFileLookup:
        # without it the binaryFile directory read is not guaranteed to
        # descend into nested subdirectories, and an extra file under one
        # could vanish from the listing and pass the exact-one audit. The
        # metadata-only, never-content property still holds: only paths are
        # requested, and the guarded object reads set no options.
        _, fake_spark = self._preflight()
        listing_uris = set(fake_spark.listing_uris)
        self.assertEqual(len(listing_uris), 3)
        options_by_uri = dict(fake_spark.reader_options)
        for uri in listing_uris:
            self.assertEqual(options_by_uri[uri], {"recursiveFileLookup": True})
        for uri in set(fake_spark.uris) - listing_uris:
            self.assertEqual(options_by_uri[uri], {})
        self.assertEqual(len(fake_spark.reader_options), len(fake_spark.uris))

    def test_nested_subdirectory_listing_depends_on_the_option(self):
        # The fake mirrors Spark's semantics: without recursiveFileLookup the
        # directory read lists only direct children, so the nested copy of
        # the SAME approved filename would be invisible. Probes that the
        # option is load-bearing for this rejection (a listing missing the
        # option keeps the single direct-child entry and the preflight
        # proceeds into the byte audits, hitting the concept drift the fix
        # exists to close).
        def boot(objects):
            directory = LandingLayout().final_directory_uri("products", BATCH, ARRIVAL, ROOT_URI)
            nested_name = objects[
                LandingLayout().final_file_uri("products", BATCH, ARRIVAL, ROOT_URI)
            ]
            objects[f"{directory}/nested/{'products_' + BATCH + '.csv'}"] = nested_name
            return objects

        objects = canonical_remote_objects()[1]
        spark = FakeBinarySpark(boot(objects))
        directory = LandingLayout().final_directory_uri("products", BATCH, ARRIVAL, ROOT_URI)
        # Simulate the regression: the same listing read WITHOUT the option
        # must NOT see the nested entry (non-recursive semantics).
        listing = FakeBinarySpark(boot(canonical_remote_objects()[1]))
        reader = listing.read.format("binaryFile").load(directory)
        paths = [row["_metadata.file_path"]
                 for row in reader.select("_metadata.file_path").limit(8).collect()]
        self.assertNotIn(
            f"{directory}/nested/products_{BATCH}.csv", paths)
        # The real preflight (with the option) DOES surface and reject it.
        with self.assertRaises(RemotePreflightError) as caught:
            remote_preflight(spark, landing_root=ROOT_URI, batch_id=BATCH, arrival_date=ARRIVAL)
        self.assertIn("nested subdirectories", str(caught.exception))
        self.assertIn("/nested/", str(caught.exception))

    def test_object_mutating_between_passes_is_rejected_without_content(self):
        objects = self._remote_bytes()
        orders_uri = LandingLayout().final_file_uri("orders", BATCH, ARRIVAL, ROOT_URI)

        def mutate_to_oversized(uri, objects):
            if uri == orders_uri:
                objects[uri] = (b"!oversized!" * ((MAX_OBJECT_BYTES // 10) + 2))[:MAX_OBJECT_BYTES + 11]

        spark = FakeBinarySpark(objects, mutate_between_passes=mutate_to_oversized)
        with self.assertRaises(RemotePreflightError) as caught:
            remote_preflight(spark, landing_root=ROOT_URI, batch_id=BATCH, arrival_date=ARRIVAL)
        self.assertIn("changed between reads", str(caught.exception))
        self.assertIn("no content was transferred", str(caught.exception))
        # Content was NEVER returned/collected for the mutated URI: the guard
        # dropped the row before projection materialized content.
        self.assertNotIn(orders_uri, spark.content_collected)
        self.assertTrue(spark.guarded_out)

    def test_object_resized_between_passes_under_limit_is_rejected(self):
        # A mutation between passes that stays WITHIN the driver limit still
        # produces a different second-pass length; the guarded pass must
        # re-verify it against the first try pass and reject WITHOUT using
        # or accepting the content (no downstream audit, no later sources).
        objects = self._remote_bytes()
        orders_uri = LandingLayout().final_file_uri("orders", BATCH, ARRIVAL, ROOT_URI)
        first_length = len(objects[orders_uri])
        assert first_length + 5 <= MAX_OBJECT_BYTES

        def resize_within_limit(uri, objects):
            if uri == orders_uri:
                objects[uri] = b"Q" * (first_length + 5)

        spark = FakeBinarySpark(objects, mutate_between_passes=resize_within_limit)
        with self.assertRaises(RemotePreflightError) as caught:
            remote_preflight(spark, landing_root=ROOT_URI, batch_id=BATCH, arrival_date=ARRIVAL)
        self.assertIn("changed between reads", str(caught.exception))
        self.assertIn(
            f"first pass reported {first_length} bytes, guarded pass reports {first_length + 5}",
            str(caught.exception))
        self.assertIn("content is not used", str(caught.exception))
        self.assertNotIn("vanished between reads", str(caught.exception))
        # The rejection happens at the orders URI: no later source is read
        # and the object is not accepted into any audit.
        self.assertEqual(spark.uris[-1], orders_uri)
        self.assertNotIn(orders_uri, spark.guarded_out)  # row passed the size guard

    def test_object_vanishing_between_passes_rejected_differently(self):
        objects = self._remote_bytes()
        orders_uri = LandingLayout().final_file_uri("orders", BATCH, ARRIVAL, ROOT_URI)

        def delete_between_passes(uri, objects):
            if uri == orders_uri:
                objects.pop(uri, None)

        spark = FakeBinarySpark(objects, mutate_between_passes=delete_between_passes)
        with self.assertRaises(RemotePreflightError) as caught:
            remote_preflight(spark, landing_root=ROOT_URI, batch_id=BATCH, arrival_date=ARRIVAL)
        self.assertEqual(len(spark.guarded_out), 0)
        self.assertIn("vanished between reads", str(caught.exception))
        self.assertNotIn(orders_uri, spark.content_collected)
        self.assertNotIn("changed between reads", str(caught.exception))

    def test_extra_file_in_batch_directory_is_rejected_before_content(self):
        def mutate(objects):
            directory = LandingLayout().final_directory_uri("orders", BATCH, ARRIVAL, ROOT_URI)
            objects[f"{directory}/unexpected_file.csv"] = b"stray"
            return objects

        objects = self._remote_bytes()
        spark = FakeBinarySpark(mutate(objects))
        orders_uri = LandingLayout().final_file_uri("orders", BATCH, ARRIVAL, ROOT_URI)
        orders_dir = LandingLayout().final_directory_uri("orders", BATCH, ARRIVAL, ROOT_URI)
        with self.assertRaises(RemotePreflightError) as caught:
            remote_preflight(spark, landing_root=ROOT_URI, batch_id=BATCH, arrival_date=ARRIVAL)
        self.assertIn("unexpected entries", str(caught.exception))
        self.assertIn("extra files or nested subdirectories", str(caught.exception))
        # The audit runs BEFORE the approved file bytes are read: rejection
        # happens at the listing pass and no content ever reached the driver.
        self.assertEqual(spark.uris[-1], orders_dir)
        self.assertNotIn(orders_uri, spark.content_collected)

    def test_nested_subdirectory_entry_is_rejected(self):
        def mutate(objects):
            directory = LandingLayout().final_directory_uri("products", BATCH, ARRIVAL, ROOT_URI)
            objects[f"{directory}/nested/sub/products_{BATCH}.csv"] = (
                objects[LandingLayout().final_file_uri("products", BATCH, ARRIVAL, ROOT_URI)])
            return objects

        objects = self._remote_bytes()
        spark = FakeBinarySpark(mutate(objects))
        product_dir = LandingLayout().final_directory_uri("products", BATCH, ARRIVAL, ROOT_URI)
        with self.assertRaises(RemotePreflightError) as caught:
            remote_preflight(spark, landing_root=ROOT_URI, batch_id=BATCH, arrival_date=ARRIVAL)
        self.assertIn("nested subdirectories", str(caught.exception))
        # The unexpected nested path is named in the rejection.
        self.assertIn("/nested/sub/", str(caught.exception))
        self.assertEqual(spark.uris[-1], product_dir)

    def test_exactly_one_approved_file_directory_passes(self):
        # The listing must be satisfied by exactly the canonical entry: pin
        # the single-entry equality of every per-source listing explicitly.
        result, fake_spark = self._preflight()
        self.assertEqual(len(result.receipts), 3)
        self.assertTrue(all(len(entries) == 1 for _, entries in fake_spark.listings))

    def test_non_root_landing_root_rejected_before_any_read(self):
        invalid_roots = [
            f"{ROOT_URI}/orders",                                       # subpath / traversal surface
            f"abfss://landing@example.dfs.core.windows.net/orders/..",  # traversal attempt
            f"{ROOT_URI}?query=1",                                      # query
            f"{ROOT_URI}#fragment",                                     # fragment
            "abfss://landing@example.dfs.core.windows.net:443",         # port
            "wasbs://landing@example.blob.core.windows.net",            # other scheme/endpoint
            "abfs://landing@example.dfs.core.windows.net",
            "https://example.dfs.core.windows.net/landing",
            "s3://landing@bucket",
            "file:///landing",                                          # local root filesystem
            "/dbfs/landing",                                            # DBFS root filesystem
            "landing",                                                  # logical path, not a URI
            "abfss://example.dfs.core.windows.net",                     # account rootless container
            "abfss://data@example.dfs.core.windows.net",                # another container
            "abfss://LANDING@example.dfs.core.windows.net",             # container must be exact
        ]
        for root in invalid_roots:
            with self.subTest(root=root):
                objects = self._remote_bytes()
                spark = FakeBinarySpark(objects)
                with self.assertRaises(RemotePreflightError) as caught:
                    remote_preflight(spark, landing_root=root, batch_id=BATCH,
                                     arrival_date=ARRIVAL)
                self.assertEqual(spark.uris, [])
                self.assertFalse(spark.content_collected)

    def test_trailing_slash_root_is_normalized(self):
        objects = self._remote_bytes()
        spark = FakeBinarySpark(objects)
        result = remote_preflight(spark, landing_root=ROOT_URI + "/", batch_id=BATCH,
                                  arrival_date=ARRIVAL)
        self.assertEqual(result.landing_root, ROOT_URI)
        self.assertEqual(result.ready_uri, LandingLayout().ready_uri(ROOT_URI, BATCH))
        self.assertEqual(result.source_uris,
                         {source: LandingLayout().final_file_uri(
                             source, BATCH, ARRIVAL, ROOT_URI)
                          for source in ("orders", "customers", "products")})

    def test_ready_marker_without_manifest_hash_is_rejected(self):
        ready = _canonical_ready_bytes("", include_sha=False)
        with self.assertRaises(RemotePreflightError) as caught:
            self._preflight(ready_bytes=ready)
        self.assertIn("64-hex sha256", str(caught.exception))

    def test_ready_marker_with_invalid_hash_is_rejected(self):
        ready = _canonical_ready_bytes("z" * 64, include_sha=True)
        with self.assertRaises(RemotePreflightError) as caught:
            self._preflight(ready_bytes=ready)
        self.assertIn("64-hex sha256", str(caught.exception))

    def test_ready_marker_without_arrival_date_is_rejected(self):
        manifest_sha = hashlib.sha256((FIXTURE / f"manifest_{BATCH}.json").read_bytes()).hexdigest()
        payload = {"batch_id": BATCH, "manifest_sha256": manifest_sha}
        ready = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        with self.assertRaises(RemotePreflightError) as caught:
            self._preflight(ready_bytes=ready)
        self.assertIn("arrival_date", str(caught.exception))

    def test_ready_marker_with_differing_arrival_date_is_rejected(self):
        manifest_sha = hashlib.sha256((FIXTURE / f"manifest_{BATCH}.json").read_bytes()).hexdigest()
        payload = {"batch_id": BATCH, "manifest_sha256": manifest_sha, "arrival_date": "2026-01-11"}
        ready = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        with self.assertRaises(RemotePreflightError) as caught:
            self._preflight(ready_bytes=ready)
        self.assertIn("arrival_date", str(caught.exception))

    def test_read_binary_object_enforces_limit_and_integrity(self):
        spark = FakeBinarySpark({"abfss://fs@host/a": b"z" * 10})
        self.assertEqual(read_binary_object(spark, "abfss://fs@host/a"), b"z" * 10)
        spark_oversized = FakeBinarySpark({"abfss://fs@host/b": b"y" * 700_000_000})
        with self.assertRaises(RemotePreflightError):
            read_binary_object(spark_oversized, "abfss://fs@host/b", max_bytes=1_000)
        with self.assertRaises(RemotePreflightError):
            read_binary_object(spark, "abfss://fs@host/missing")

    def test_binary_file_format_locked(self):
        class AlarmReader:
            def format(self, name):
                raise AssertionError(f"preflight must use binaryFile, got {name}")

        class AlarmSpark(FakeBinarySpark):
            @property
            def read(self):  # noqa: D102 - swap the reader for one that alarms
                return AlarmReader()

        with self.assertRaises(AssertionError):
            read_binary_object(AlarmSpark({}), "abfss://fs@host/x")

    # --- Regression: serverless Spark Connect Row shape ---------------------

    def test_spark_connect_row_double_matches_reported_failure_shape(self):
        # The double reproduces the exact cloud failure surface: index access
        # and asDict work, dict(row) raises the reported TypeError, .get is
        # absent.
        row = SparkConnectRow({"length": 3, "content": b"abc"})
        self.assertEqual(row["length"], 3)
        self.assertEqual(row.asDict(recursive=True)["content"], b"abc")
        self.assertEqual(row.asDict()["content"], b"abc")
        with self.assertRaises(TypeError) as caught:
            dict(row)
        self.assertIn("cannot convert dictionary update sequence element #0",
                      str(caught.exception))
        with self.assertRaises(AttributeError):
            row.get("content")  # .get absent, like the Spark Connect Row

    def test_read_binary_object_supports_spark_connect_rows(self):
        payload = b"connect-row-payload"
        spark = SparkConnectRowsSpark({"abfss://fs@host/connect": payload})
        self.assertEqual(read_binary_object(spark, "abfss://fs@host/connect"), payload)
        # The guarded content pass ran to completion on the normalized row.
        self.assertEqual(spark.content_collected, ["abfss://fs@host/connect"])

    def test_full_remote_preflight_supports_spark_connect_rows(self):
        # Object reads AND metadata-only directory listings both return
        # Spark Connect-style rows; the preflight must succeed end to end
        # (proves the guarded content read and the listing normalization).
        spark = SparkConnectRowsSpark(canonical_remote_objects()[1])
        result = remote_preflight(spark, landing_root=ROOT_URI, batch_id=BATCH,
                                  arrival_date=ARRIVAL)
        self.assertEqual([r.source for r in result.receipts],
                         ["orders", "customers", "products"])
        self.assertEqual(result.total_rows, 44)
        self.assertEqual(len(spark.listings), 3)

    def test_unsupported_row_object_is_rejected_with_clear_error(self):
        class BogusRow:
            """No asDict, not a mapping: unsupported by the normalization."""

        class BogusRowsSpark(FakeBinarySpark):
            @property
            def read(self):
                class BogusReader(FakeReader):
                    def collect(self):
                        return [BogusRow()]

                return BogusReader(self)

        spark = BogusRowsSpark({"abfss://fs@host/bogus": b"x"})
        with self.assertRaises(RemotePreflightError) as caught:
            read_binary_object(spark, "abfss://fs@host/bogus")
        self.assertIn("unsupported row object", str(caught.exception))
        self.assertIn("BogusRow", str(caught.exception))

    # --- Regression: real-Spark flat `file_path` listing rows ---------------

    def test_listing_accepts_flat_file_path_row_representation(self):
        # Real Spark aliases `select("_metadata.file_path")` to a top-level
        # `file_path` column: the rows normalize to {"file_path": <uri>} with
        # NO `_metadata.file_path` and NO nested `_metadata`. This is the
        # third documented representation and it FAILED before the fix
        # (`returned a non-path entry`).
        layout = LandingLayout()
        directory = layout.final_directory_uri("orders", BATCH, ARRIVAL, ROOT_URI)
        approved = layout.final_file_uri("orders", BATCH, ARRIVAL, ROOT_URI)
        spark = FlatFilePathSpark(canonical_remote_objects()[1])
        paths = list_batch_directory(spark, directory)
        self.assertEqual(paths, (approved,))
        # Prove the fake really emitted only the flat representation (the
        # pre-fix lookup would see nothing at all in these rows).
        self.assertEqual(spark.listings, [(directory, (approved,))])

    def test_full_remote_preflight_with_spark_connect_flat_file_path_rows(self):
        # Full cloud-regression shape: Spark Connect Row objects on object
        # reads AND flat `file_path` rows on the metadata-only listings — the
        # exact condition where the run previously died in list_batch_directory.
        spark = SparkConnectFlatFilePathSpark(canonical_remote_objects()[1])
        result = remote_preflight(spark, landing_root=ROOT_URI, batch_id=BATCH,
                                  arrival_date=ARRIVAL)
        self.assertEqual([r.source for r in result.receipts],
                         ["orders", "customers", "products"])
        self.assertEqual(result.total_rows, 44)
        # The directory audits ran and satisfied exactly-one per source.
        self.assertEqual(len(spark.listings), 3)
        self.assertTrue(all(len(entries) == 1 for _, entries in spark.listings))

    def test_listing_rejects_missing_invalid_or_empty_paths_in_all_shapes(self):
        # The representation widening must NOT soften the contract: a row
        # whose every representation is absent, non-string or empty is still
        # a `non-path entry` rejection.
        class StaticListingReader:
            def __init__(self, rows: dict):
                self._rows = rows
                self._options: dict = {}

            def format(self, name):
                if name != "binaryFile":
                    raise AssertionError(f"preflight must use binaryFile, got {name}")
                return self

            def option(self, key, value):
                self._options[key] = value
                return self

            def load(self, uri):
                self._loaded = uri
                return self

            def select(self, *columns):
                if list(columns) != ["_metadata.file_path"]:
                    raise AssertionError(f"unexpected listing projection: {columns}")
                return self

            def limit(self, count):
                if count != MAX_DIRECTORY_ENTRIES:
                    raise AssertionError("listing bound must reach the driver pass")
                return self

            def collect(self):
                if self._options.get("recursiveFileLookup") is not True:
                    raise AssertionError("listing must keep recursiveFileLookup=True")
                return self._rows

        class StaticListingsSpark(FakeBinarySpark):
            def __init__(self, rows: dict):
                super().__init__({})
                self._rows = rows

            @property
            def read(self):
                return StaticListingReader(self._rows)

        directory = "abfss://landing@example.dfs.core.windows.net/orders/x"
        rejected = ([{"length": 5}],                              # absent everywhere
                    [{"file_path": 7}],                           # non-string flat
                    [{"file_path": ""}],                          # empty flat
                    [{"_metadata.file_path": 7}],                 # non-string dotted
                    [{"_metadata.file_path": ""}],                # empty dotted
                    [{"_metadata": {"file_path": None}}])         # non-string nested
        for rows in rejected:
            with self.subTest(rows=rows):
                with self.assertRaises(RemotePreflightError) as caught:
                    list_batch_directory(StaticListingsSpark(rows), directory)
                self.assertIn("non-path entry", str(caught.exception))
                self.assertIn(directory, str(caught.exception))


if __name__ == "__main__":
    unittest.main()
