"""Preflight cloud de bytes sobre las rutas ya concedidas del lote.

Con una `landing_root` `abfss://...` (bajo las external locations ya
concedidas: `landing/orders`, `landing/customers`, `landing/products` y
`landing/_control`), este adaptador lee con Spark `binaryFile` SOLO:

* el ready marker (`landing/_control/ready/<batch>.json`),
* el manifest de control (`landing/_control/manifests/<batch>/manifest_<batch>.json`),
* las tres rutas finales canónicas del lote (grano por
  `arrival_date`/`batch_id`), nunca el root de una fuente ni `_staging`.

Verifica ready + nombres + SHA-256 + recuentos/total contra los bytes
originales, reutilizando los validadores puros de `manifest.py`. Sin `dbutils`,
sin secretos: el root físico se construye con el join seguro de `layout.py`.
Los objetos que llegan al driver están limitados por `MAX_OBJECT_BYTES`; los
inputs mayores se rechazan antes de acumularse en el driver.
El preflight es additivo: no escribe, no muta y no sustituye al preflight
local (Gate GF2-6 precede a cualquier ejecución del Job).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Mapping

from .contracts import BRONZE_CONTRACTS
from .layout import LandingLayout, validate_arrival_date, validate_batch_id
from .manifest import (
    ManifestPreflightError,
    READY_MARKER_MANIFEST_KEY,
    SourceReceipt,
    audit_source_bytes,
    expected_source_names,
    parse_cloud_ready_payload,
    parse_manifest_bytes,
    verify_manifest_total,
)

MAX_OBJECT_BYTES = 1_000_000  # ~1 MiB: los objetos del lote son pequeños y controlados.
BINARY_FILE_FORMAT = "binaryFile"
READY_MANIFEST_SHA_KEY = "manifest_sha256"


class RemotePreflightError(ValueError):
    """Cualquier desviación del preflight remoto aborta el Job: nada se escribe."""


@dataclass(frozen=True)
class RemotePreflightResult:
    batch_id: str
    arrival_date: str
    landing_root: str
    ready_payload: dict
    ready_uri: str
    manifest_uri: str
    manifest_sha256: str
    receipts: tuple[SourceReceipt, ...]
    source_uris: Mapping[str, str]

    @property
    def total_rows(self) -> int:
        return sum(receipt.rows_declared for receipt in self.receipts)


def read_binary_object(spark, uri: str, *, max_bytes: int = MAX_OBJECT_BYTES) -> bytes:
    """Read one binaryFile object into the driver with an explicit size limit.

    Protocol so the driver never receives oversized `content`:

    1. First pass selects ONLY `length` (a single bounded row): a missing row,
       a non-numeric length, or `length > max_bytes` is rejected before any
       `content` is ever requested.
    2. Guarded second pass: the SAME URI is re-read with a length filter
       (`length <= max_bytes`) applied BEFORE `content` is selected or
       collected. If an object MUTATES between passes and now exceeds the
       limit, the guarded projection yields no row and `content` is never
       selected or transferred. A filter-less second read would be a race:
       a mutated object could then push oversized content to the driver.
    3. If the guarded pass returns no row, the object is diagnosed with a
       length-ONLY read (never content) and rejected with an explicit
       difference between "changed (now larger than the limit)" and
       "vanished (absent)"; updated objects are never accepted as-is.
    4. The guarded pass re-verifies its own `length` against the first read;
       a DIFFERENT reported length is rejected as a changed object without
       using/accepting its content (this closes race window (2) for
       mutations that stay within the limit).

    What is actually checked, nothing stronger (no complete-detection claim):
    (a) first-pass length must be numeric and within the driver limit;
    (b) the guarded pass must re-confirm the same first-pass length;
    (c) `len(content) == reported length` of the guarded pass;
    (d) afterwards, the audited SHA-256/row counts of the returned bytes
        against the control manifest remain the real integrity check for the
        three final objects (not for ready/manifest themselves beyond the
        ready marker's own attestation). A between-pass mutation that keeps
        the guarded length equal to the first pass is therefore only caught
        downstream by (d) on the final objects, never promised detected here.

    Exact residual limit: binaryFile may still materialize the whole object
    inside the executor during the scan (that cost is bounded by cluster
    memory, NOT by this function and not promised here); the guarantees of
    this function are about what reaches/gets used by the driver: oversized
    `content` is never transferred, and any cross-pass length divergence is
    rejected without accepting the content. Uses `spark.read.format
    ("binaryFile")`: no `dbutils`, no extra privileges.
    """
    if max_bytes <= 0:
        raise RemotePreflightError("max_bytes must be positive")
    length = _collect_first_row(spark, uri, select=("length",))["length"]
    if not isinstance(length, int) or isinstance(length, bool):
        raise RemotePreflightError(f"binaryFile did not report numeric length for {uri}")
    if length > max_bytes:
        raise RemotePreflightError(
            f"object at {uri} is {length} bytes, exceeding the driver limit {max_bytes}")
    guarded = (spark.read.format(BINARY_FILE_FORMAT).load(uri)
               .filter(f"length <= {int(max_bytes)}")
               .select("length", "content").limit(1))
    rows = guarded.collect()
    if not rows:
        # The object changed or vanished between passes; diagnose WITHOUT
        # content so the driver never sees anything beyond metadata.
        try:
            diagnostic = _collect_first_row(spark, uri, select=("length",))["length"]
        except RemotePreflightError as exc:
            raise RemotePreflightError(
                f"object vanished between reads at {uri}: the guarded content "
                "read found nothing and the diagnostic length-only read "
                "confirms absence (the object is not accepted).") from exc
        raise RemotePreflightError(
            f"object changed between reads at {uri}: first read reported "
            f"{length} bytes, diagnostic length-only read reports {diagnostic}; "
            "changed objects are not accepted and no content was transferred.")
    verified_length = rows[0]["length"]
    if verified_length != length:
        # Guarded read raced with a mutation that still fits the limit: catch
        # it here and refuse to accept/use the content.
        raise RemotePreflightError(
            f"object changed between reads at {uri}: first pass reported "
            f"{length} bytes, guarded pass reports {verified_length}; "
            "changed objects are not accepted and content is not used.")
    content = rows[0].get("content")
    if content is None:
        raise RemotePreflightError(f"binaryFile returned no content for {uri}")
    if len(content) != verified_length:
        raise RemotePreflightError(
            f"binaryFile content mismatch for {uri}: reported {verified_length}, got {len(content)} bytes")
    return bytes(content)


def _collect_first_row(spark, uri: str, select: tuple[str, ...]) -> dict:
    """One binaryFile read limited to a single row with the given projection."""
    dataframe = spark.read.format(BINARY_FILE_FORMAT).load(uri).select(*select).limit(1)
    rows = dataframe.collect()
    if len(rows) != 1:
        raise RemotePreflightError(f"no object found at {uri}")
    return dict(rows[0])


def remote_preflight(spark, *, landing_root: str, batch_id: str, arrival_date: str,
                     layout: LandingLayout | None = None,
                     max_bytes: int = MAX_OBJECT_BYTES) -> RemotePreflightResult:
    """Validate the promoted batch, byte-for-byte, before any Bronze write.

    Read-only steps: ready marker (exists/valid; MUST attest the control
    manifest SHA-256 with a 64-hex hash and carry an `arrival_date` exactly
    equal to the requested one), control manifest (v1 header + contract file
    set), and each
    canonical final file (SHA-256 + physical row count + manifest total).
    Every failure — including contract errors surfaced by the pure manifest
    validators — is uniformly re-raised as `RemotePreflightError` with the
    detail preserved as its cause/message.

    Reads are limited to ready + control manifest + the three final URIs
    composed by the layout under the granted landing sublocations; `_staging`
    and source roots are unreachable by construction.
    """
    batch_id = validate_batch_id(batch_id)
    arrival_date = validate_arrival_date(arrival_date)
    layout = layout or LandingLayout()
    if max_bytes <= 0 or not isinstance(max_bytes, int) or isinstance(max_bytes, bool):
        raise RemotePreflightError("max_bytes must be a positive integer limit")
    try:
        return _preflight_bytes(spark, landing_root=landing_root, batch_id=batch_id,
                                arrival_date=arrival_date, layout=layout, max_bytes=max_bytes)
    except RemotePreflightError:
        raise
    except (ManifestPreflightError, ValueError, OSError) as exc:
        raise RemotePreflightError(f"remote preflight rejected the promoted batch: {exc}") from exc


def _preflight_bytes(spark, *, landing_root: str, batch_id: str, arrival_date: str,
                     layout: LandingLayout, max_bytes: int) -> RemotePreflightResult:
    ready_uri = layout.ready_uri(landing_root, batch_id)
    ready_payload = parse_cloud_ready_payload(
        read_binary_object(spark, ready_uri, max_bytes=max_bytes),
        batch_id=batch_id, arrival_date=arrival_date)

    manifest_uri = layout.manifest_uri(landing_root, batch_id)
    manifest_bytes = read_binary_object(spark, manifest_uri, max_bytes=max_bytes)
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    attested = ready_payload[READY_MARKER_MANIFEST_KEY]
    if attested != manifest_sha256:
        raise RemotePreflightError(
            "ready marker does not attest the control manifest: "
            f"expected {manifest_sha256}, ready declared {attested}")
    manifest_batch, manifest = parse_manifest_bytes(manifest_bytes, batch_id)
    expected = expected_source_names(manifest_batch)

    receipts: list[SourceReceipt] = []
    source_uris: dict[str, str] = {}
    by_name = {item["file"]: item for item in manifest["files"]}
    for source, filename in expected.items():
        uri = layout.final_file_uri(source, batch_id, arrival_date, landing_root)
        raw = read_binary_object(spark, uri, max_bytes=max_bytes)
        receipts.append(audit_source_bytes(by_name[filename], raw, source, manifest_batch))
        source_uris[source] = uri
    verify_manifest_total(manifest, tuple(receipts))
    return RemotePreflightResult(
        batch_id=batch_id,
        arrival_date=arrival_date,
        landing_root=landing_root,
        ready_payload=ready_payload,
        ready_uri=ready_uri,
        manifest_uri=manifest_uri,
        manifest_sha256=manifest_sha256,
        receipts=tuple(receipts),
        source_uris=source_uris,
    )
