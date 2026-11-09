"""Preflight del manifest; verifica bytes antes de cualquier Spark action.

Los validadores puros sobre bytes (`parse_ready_payload_bytes`,
`parse_manifest_bytes`, `expected_source_names`, `audit_source_bytes`,
`verify_manifest_total`) son reutilizables por el preflight cloud
(`remote_preflight.py`) y por la validación local (`validate_manifest_batch`,
`validate_ready_marker`), sin duplicar la lógica del contrato v1.
"""

from __future__ import annotations

import hashlib
import csv
import io
import json
import re
from dataclasses import dataclass
from pathlib import Path

from .contracts import BRONZE_CONTRACTS
from .layout import validate_arrival_date, validate_batch_id


class ManifestPreflightError(ValueError):
    pass


@dataclass(frozen=True)
class SourceReceipt:
    batch_id: str
    source: str
    filename: str
    sha256: str
    rows_declared: int
    contract_version: str

    @property
    def immutable_key(self) -> tuple[str, str, str]:
        return self.batch_id, self.source, self.sha256


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
READY_MARKER_BATCH_KEY = "batch_id"
READY_MARKER_MANIFEST_KEY = "manifest_sha256"
READY_MARKER_ARRIVAL_KEY = "arrival_date"
MANIFEST_CONTRACT_VERSION = "v1"


def _rows_from_bytes(raw: bytes, source: str) -> int:
    if raw is None or not isinstance(raw, (bytes, bytearray)):
        raise ManifestPreflightError("source bytes are required to audit rows")
    raw = bytes(raw)
    if source == "orders":
        # Physical JSONL records: malformed lines are still Bronze evidence.
        return len(raw.splitlines())
    try:
        rows = list(csv.reader(io.TextIOWrapper(io.BytesIO(raw), encoding="utf-8", newline="")))
    except (UnicodeDecodeError, csv.Error) as exc:
        raise ManifestPreflightError(f"unparseable CSV: {source}") from exc
    return max(0, len(rows) - 1)


def _row_count(path: Path, source: str) -> int:
    return _rows_from_bytes(Path(path).read_bytes(), source)


def validate_source_directory(directory: Path, source: str, batch_id: str, arrival_date: str) -> Path:
    """Require exactly the approved physical file in an isolated batch directory."""
    from .layout import LandingLayout

    if source not in BRONZE_CONTRACTS:
        raise ManifestPreflightError("unknown source")
    expected = BRONZE_CONTRACTS[source].filename(batch_id)
    directory = Path(directory)
    if not directory.is_dir() or {entry.name for entry in directory.iterdir()} != {expected}:
        raise ManifestPreflightError("batch source directory must contain exactly its contract file")
    # Assert the caller selected the canonical batch path, not `landing/<source>`.
    LandingLayout().final_directory(source, batch_id, arrival_date)
    return directory / expected


def parse_ready_payload_bytes(data: bytes, batch_id: str | None = None) -> dict:
    """Pure ready-marker validator over bytes (UTF-8 JSON object)."""
    try:
        payload = json.loads(bytes(data).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ManifestPreflightError("ready marker must be valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise ManifestPreflightError("ready marker must be a JSON object")
    marker_batch = payload.get(READY_MARKER_BATCH_KEY)
    try:
        validate_batch_id(marker_batch)
    except ValueError as exc:
        raise ManifestPreflightError("ready marker batch_id is unsafe") from exc
    if batch_id is not None and marker_batch != validate_batch_id(batch_id):
        raise ManifestPreflightError("ready marker batch_id mismatch")
    return payload


def parse_cloud_ready_payload(data: bytes, *, batch_id: str, arrival_date: str) -> dict:
    """Strict cloud ready-marker contract over bytes.

    Requires (beyond the local shape): a valid 64-hex `manifest_sha256`
    attestation and a valid `arrival_date` exactly matching the requested
    one. The SHA equality against the control manifest bytes is checked
    separately by the remote preflight (after reading the manifest).
    """
    payload = parse_ready_payload_bytes(data, batch_id)
    declared_sha = payload.get(READY_MARKER_MANIFEST_KEY)
    if not isinstance(declared_sha, str) or not SHA256_RE.fullmatch(declared_sha):
        raise ManifestPreflightError(
            "ready marker must attest the control manifest as a 64-hex sha256")
    requested_date = validate_arrival_date(arrival_date)
    if payload.get(READY_MARKER_ARRIVAL_KEY) != requested_date:
        raise ManifestPreflightError(
            "ready marker arrival_date must exist and match the requested date")
    return payload


def validate_ready_marker(marker: Path, batch_id: str) -> None:
    """Validate the producer's final control artifact without performing I/O elsewhere.

    The marker is written after immutable source files and manifest are placed;
    its presence alone is never a substitute for checksum preflight.
    """
    try:
        data = Path(marker).read_bytes()
    except OSError as exc:
        raise ManifestPreflightError("ready marker must be valid UTF-8 JSON") from exc
    parse_ready_payload_bytes(data, batch_id)


def parse_manifest_bytes(data: bytes, batch_id: str | None = None) -> tuple[str, dict]:
    """Pure manifest v1 header validator over bytes.

    Returns `(manifest_batch_id, manifest_dict)` once the header is proven: one
    object, safe and matching `batch_id`, contract v1, exactly the three
    contract file entries (by name, no duplicates) declared as objects.
    Byte-level evidence (SHAs, rows, disc-set) is audited separately by
    `audit_source_bytes`/`verify_manifest_total`.
    """
    try:
        manifest = json.loads(bytes(data).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ManifestPreflightError("manifest must be valid UTF-8 JSON") from exc
    if not isinstance(manifest, dict):
        raise ManifestPreflightError("manifest must be a JSON object")
    manifest_batch = manifest.get("batch_id")
    try:
        validate_batch_id(manifest_batch)
    except ValueError as exc:
        raise ManifestPreflightError("manifest batch_id is unsafe") from exc
    if batch_id is not None and manifest_batch != validate_batch_id(batch_id):
        raise ManifestPreflightError("requested batch_id does not match manifest")
    if manifest.get("contract_version") != MANIFEST_CONTRACT_VERSION:
        raise ManifestPreflightError("unsupported contract_version")
    entries = manifest.get("files")
    if not isinstance(entries, list) or len(entries) != len(BRONZE_CONTRACTS):
        raise ManifestPreflightError(f"manifest must declare exactly {len(BRONZE_CONTRACTS)} source files")
    if any(not isinstance(item, dict) for item in entries):
        raise ManifestPreflightError("manifest entries must be objects")
    names = [item.get("file") for item in entries]
    if len(set(names)) != len(names):
        raise ManifestPreflightError("manifest must not contain duplicate entries")
    expected = expected_source_names(manifest_batch)
    if set(names) != set(expected.values()):
        raise ManifestPreflightError("manifest file set differs from v1 contract")
    return manifest_batch, manifest


def expected_source_names(manifest_batch: str) -> dict[str, str]:
    """Canonical `<source>/<filename>` mapping for a validated batch id."""
    return {
        source: contract.filename(manifest_batch)
        for source, contract in BRONZE_CONTRACTS.items()
    }


def audit_source_bytes(entry: dict, raw: bytes, source: str, manifest_batch: str) -> SourceReceipt:
    """Pure per-source byte audit: SHA-256 over the original bytes and the
    declared-vs-actual row count, returning the immutable receipt."""
    name = entry.get("file")
    if not isinstance(name, str):
        raise ManifestPreflightError(f"invalid name for source: {source}")
    declared_sha = entry.get("sha256")
    if not isinstance(declared_sha, str) or not SHA256_RE.fullmatch(declared_sha):
        raise ManifestPreflightError(f"invalid checksum: {source}")
    if not isinstance(entry.get("rows"), int) or isinstance(entry.get("rows"), bool):
        raise ManifestPreflightError(f"declared rows must be an integer: {source}")
    actual_sha = hashlib.sha256(bytes(raw)).hexdigest()
    if declared_sha != actual_sha:
        raise ManifestPreflightError(f"checksum mismatch: {name or source}")
    rows = _rows_from_bytes(raw, source)
    if entry["rows"] != rows:
        raise ManifestPreflightError(f"row count mismatch: {name or source}")
    return SourceReceipt(manifest_batch, source, name, actual_sha, rows, MANIFEST_CONTRACT_VERSION)


def verify_manifest_total(manifest: dict, receipts: tuple[SourceReceipt, ...]) -> int:
    """Declared `total_rows` must equal the sum of audited receipts."""
    total = sum(receipt.rows_declared for receipt in receipts)
    if (
        isinstance(manifest.get("total_rows"), bool)
        or not isinstance(manifest.get("total_rows"), int)
        or manifest.get("total_rows") != total
    ):
        raise ManifestPreflightError("total_rows mismatch")
    return total


def validate_manifest_batch(batch_dir: Path, batch_id: str | None = None) -> tuple[SourceReceipt, ...]:
    """Validate v1 file set, SHA-256, declared rows and total without mutation."""
    batch_dir = Path(batch_dir)
    entries_on_disk = {entry.name for entry in batch_dir.iterdir()} if batch_dir.is_dir() else set()
    manifest_paths = [batch_dir / f"manifest_{batch_id}.json"] if batch_id else list(batch_dir.glob("manifest_*.json"))
    if len(manifest_paths) != 1 or not manifest_paths[0].is_file():
        raise ManifestPreflightError("exactly one manifest is required")
    try:
        data = manifest_paths[0].read_bytes()
    except OSError as exc:
        raise ManifestPreflightError("manifest must be valid UTF-8 JSON") from exc
    manifest_batch, manifest = parse_manifest_bytes(data, batch_id)
    expected = expected_source_names(manifest_batch)
    if entries_on_disk != set(expected.values()) | {manifest_paths[0].name}:
        raise ManifestPreflightError("batch directory contains unexpected entries")
    receipts: list[SourceReceipt] = []
    by_name = {item["file"]: item for item in manifest["files"]}
    for source, filename in expected.items():
        path = batch_dir / filename
        if not path.is_file():
            raise ManifestPreflightError(f"missing source file: {filename}")
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise ManifestPreflightError(f"unreadable source file: {filename}") from exc
        receipts.append(audit_source_bytes(by_name[filename], raw, source, manifest_batch))
    verify_manifest_total(manifest, tuple(receipts))
    return tuple(receipts)


def validate_receipt_consistency(receipt: SourceReceipt, known_receipts: dict[tuple[str, str], str]) -> None:
    """Reject conflicting historical checksums; this is not a filtering mechanism.

    Cross-table atomicity is not claimed. A retry can append another run-log
    attempt; Bronze stream idempotency relies on immutable input, the retained
    same checkpoint, and the Delta streaming sink.
    """
    key = (receipt.batch_id, receipt.source)
    known = known_receipts.get(key)
    if known is None:
        return
    if known != receipt.sha256:
        raise ManifestPreflightError("batch/source already accepted with another checksum")
