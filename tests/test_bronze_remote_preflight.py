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

from pipelines.bronze.layout import LandingLayout, join_landing_uri, landing_relative_path
from pipelines.bronze.manifest import ManifestPreflightError
from pipelines.bronze.remote_preflight import (
    MAX_OBJECT_BYTES,
    RemotePreflightError,
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

    def format(self, name):
        self._spark.formats.append(name)
        if name != "binaryFile":
            raise AssertionError(f"preflight must use binaryFile, got {name}")
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
        return self

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
            raise AssertionError(f"unexpected binaryFile projection: {columns}")
        return self

    def limit(self, count):
        return self

    def collect(self):
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
        self.selected_projects: list[tuple[str, tuple]] = []
        self.filtered_projects: list[tuple[str, str, tuple]] = []
        self.filters: list[tuple[str, str]] = []
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


class RemotePreflightContract(unittest.TestCase):
    def _remote_bytes(self, ready_bytes: bytes | None = None) -> dict[str, bytes]:
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

    def _preflight(self, mutate=None, *, spark=None, batch=BATCH, arrival=ARRIVAL, ready_bytes=None):
        _, objects = self._remote_bytes(ready_bytes=ready_bytes)
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
        self.assertEqual(len(set(fake_spark.uris)), 5)
        self.assertEqual(len(fake_spark.uris), 10)  # two reads per object: length → length+content
        # Per-batch canonical grain, never a source root.
        orders_uri = expected_final["orders"]
        self.assertIn(f"arrival_date={ARRIVAL}", orders_uri)
        self.assertIn(f"batch_id={BATCH}", orders_uri)

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
        # Each canonical object is read exactly twice: length first, then a
        # GUARDED length+content pass only after the limit check.
        requests: dict[str, list] = {}
        for uri, columns in result_spark.selected_projects:
            requests.setdefault(uri, []).append(columns)
        self.assertEqual(len(requests), 5)
        for uri, projections in requests.items():
            self.assertEqual(projections, [("length",), ("length", "content")])
        # The guarded pass applies the length filter BEFORE the projection,
        # so a mutation between passes cannot transfer content to the driver.
        self.assertEqual(len(result_spark.filters), 5)
        for uri, expression in result_spark.filters:
            self.assertEqual(expression, f"length <= {MAX_OBJECT_BYTES}")

    def test_object_mutating_between_passes_is_rejected_without_content(self):
        _, objects = self._remote_bytes()
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
        _, objects = self._remote_bytes()
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
        _, objects = self._remote_bytes()
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


if __name__ == "__main__":
    unittest.main()
