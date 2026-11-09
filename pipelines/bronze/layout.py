"""Convenciones puras para rutas landing y estado durable."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import PurePosixPath

from .contracts import BRONZE_CONTRACTS

BATCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
ARRIVAL_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# Contract-internal exact file names only (glob metacharacters are excluded so
# a validated name is also a safe exact `pathGlobFilter` pattern).
SOURCE_FILENAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
LANDING_PREFIX = "landing/"

LANDING_SCHEME_PREFIX = "abfss://"

# The operational landing root is a DEPLOYMENT configuration value, not a
# run-editable parameter: exactly the ROOT of the `landing` container, with a
# free-form (never secret) account. Anything beyond a pure container root —
# subpath, query, fragment, port, other scheme/endpoint, another container, or
# the account root without the container — is invalid by construction, so no
# traversal surface exists at the root level either.
LANDING_ROOT_RE = re.compile(
    r"^abfss://landing@"
    r"(?P<account>[a-z0-9][a-z0-9-]{1,21}[a-z0-9])"  # storage account: 3-24 lowercase alnum/hyphen
    r"\.dfs\.core\.windows\.net/?"                    # exactly the DFS endpoint (+ optional slash)
)


def validate_landing_root(landing_root: str) -> str:
    """Strictly validate the operational landing root as a `landing` container root.

    Accepts ONLY `abfss://landing@<account>.dfs.core.windows.net` (optionally
    with a single trailing slash, which is normalized off). Rejects a subpath,
    query, fragment or port, other schemes/endpoints (wasbs/abfs/https/s3...),
    a local/DBFS root filesystem, another container name, an empty value, and
    therefore any traversal attempt at the root level. The account is not
    matched against a configured truth (no real account is presumed here);
    this validator enforces the structural landing contract only.
    """
    if not isinstance(landing_root, str):
        raise ValueError("landing_root must be a string abfss:// URI")
    stripped = landing_root.strip()
    if not stripped:
        raise ValueError("landing_root is required")
    if not LANDING_ROOT_RE.fullmatch(stripped):
        raise ValueError(
            "landing_root must be exactly the abfss://landing@<account>.dfs.core.windows.net "
            "container root (no subpath, query, fragment, port, another container or scheme)")
    return stripped.rstrip("/")


def validate_source_filename(filename: str) -> str:
    """Validate an exact contract file name, safe to use as a path glob filter.

    Accepted names contain no `/`, `..`, backslash or glob metacharacter, so
    the value is both an exact name and a literal-only `pathGlobFilter`.
    """
    if not isinstance(filename, str) or not SOURCE_FILENAME_RE.fullmatch(filename):
        raise ValueError("source filename must be a safe exact contract file name")
    return filename


def validate_batch_id(batch_id: str) -> str:
    if not isinstance(batch_id, str) or not BATCH_ID_RE.fullmatch(batch_id):
        raise ValueError("batch_id must be a safe 1-64 character identifier")
    return batch_id


def validate_arrival_date(arrival_date: str) -> str:
    if not isinstance(arrival_date, str) or not ARRIVAL_DATE_RE.fullmatch(arrival_date):
        raise ValueError("arrival_date must use YYYY-MM-DD")
    try:
        date.fromisoformat(arrival_date)
    except ValueError as exc:
        raise ValueError("arrival_date must be a calendar date") from exc
    return arrival_date


@dataclass(frozen=True)
class LandingLayout:
    """Logical paths only; no filesystem or cloud operation occurs here.

    A batch gets an isolated input directory and a retained, batch-scoped
    checkpoint. This supports Spark's normal streaming idempotency only while
    that exact checkpoint is retained; losing or replacing it requires an
    explicit recovery design and is not made safe by this module.

    Logical paths keep the `landing/...` prefix; physical URIs for the Job are
    derived only when an explicit `landing_root` (abfss://...) is provided.
    """

    catalog: str = "tt_ecommerce_synthetic"
    max_batch_file_bytes: int = 1_000_000

    def final_file(self, source: str, batch_id: str, arrival_date: str) -> str:
        contract = BRONZE_CONTRACTS[source]
        return str(PurePosixPath(self.final_directory(source, batch_id, arrival_date), contract.filename(batch_id)))

    def final_directory(self, source: str, batch_id: str, arrival_date: str) -> str:
        if source not in BRONZE_CONTRACTS:
            raise ValueError("unknown source")
        validate_batch_id(batch_id)
        validate_arrival_date(arrival_date)
        return str(PurePosixPath("landing", source, f"arrival_date={arrival_date}", f"batch_id={batch_id}"))

    def staging_batch(self, batch_id: str) -> str:
        return str(PurePosixPath("landing", "_staging", validate_batch_id(batch_id)))

    def manifest(self, batch_id: str) -> str:
        batch_id = validate_batch_id(batch_id)
        return str(PurePosixPath("landing", "_control", "manifests", batch_id, f"manifest_{batch_id}.json"))

    def ready(self, batch_id: str) -> str:
        return str(PurePosixPath("landing", "_control", "ready", f"{validate_batch_id(batch_id)}.json"))

    def checkpoint(self, source: str, batch_id: str) -> str:
        if source not in BRONZE_CONTRACTS:
            raise ValueError("unknown source")
        return f"/Volumes/{self.catalog}/pipeline_state/ingestion_state/{source}/checkpoints/{validate_batch_id(batch_id)}"

    def schema_location(self, source: str) -> str:
        if source not in BRONZE_CONTRACTS:
            raise ValueError("unknown source")
        return f"/Volumes/{self.catalog}/pipeline_state/ingestion_state/{source}/schema"

    # ------------------------------------------------------------------
    # Physical URI helpers for the Job (require an explicit landing root).
    # The logical `landing/...` paths above stay unchanged; these helpers only
    # convert them, safely, into abfss:// URIs under the approved root.
    # ------------------------------------------------------------------

    def landing_uri(self, landing_root: str, logical: str) -> str:
        """Join a validated `landing/...` logical path onto a physical root."""
        return join_landing_uri(landing_root, landing_relative_path(logical))

    def staging_uri(self, landing_root: str, batch_id: str) -> str:
        return self.landing_uri(landing_root, self.staging_batch(batch_id))

    def manifest_uri(self, landing_root: str, batch_id: str) -> str:
        return self.landing_uri(landing_root, self.manifest(batch_id))

    def ready_uri(self, landing_root: str, batch_id: str) -> str:
        return self.landing_uri(landing_root, self.ready(batch_id))

    def final_file_uri(self, source: str, batch_id: str, arrival_date: str, landing_root: str) -> str:
        return self.landing_uri(landing_root, self.final_file(source, batch_id, arrival_date))

    def final_directory_uri(self, source: str, batch_id: str, arrival_date: str, landing_root: str) -> str:
        """Physical per-batch source directory (the Auto Loader read root)."""
        return self.landing_uri(landing_root, self.final_directory(source, batch_id, arrival_date))


def landing_relative_path(logical: str) -> str:
    """Strip exactly one `landing/` prefix and keep the path trivially safe.

    Used by the remote preflight and the streaming source to express the same
    validated logical path relative to the `landing` ADLS filesystem. It never
    allows traversal, backslashes or scheme-like segments; callers keep the
    original logical path untouched for run-log/audit purposes.
    """
    if not isinstance(logical, str) or not logical.startswith(LANDING_PREFIX):
        raise ValueError(f"logical path must be inside the landing filesystem: {logical!r}")
    relative = logical[len(LANDING_PREFIX):]
    if not relative or relative.startswith("/") or relative.endswith("/"):
        raise ValueError(f"unsafe landing-relative path: {logical!r}")
    for segment in relative.split("/"):
        if not segment or segment in (".", "..") or "\\" in segment or ":" in segment:
            raise ValueError(f"unsafe landing path segment in {logical!r}")
    return relative


def join_landing_uri(landing_root: str, relative: str) -> str:
    """Join a landing-relative path onto an `abfss://...` root, safely.

    The root is not injected with traversal content: any `..`, backslash or
    empty segment in either component rejects the join before it is built.
    """
    root = landing_root.strip() if isinstance(landing_root, str) else None
    if not root:
        raise ValueError("landing_root is required for physical landing URIs")
    if "\\" in root or not root.startswith(LANDING_SCHEME_PREFIX):
        raise ValueError("landing_root must be an abfss:// URI")
    authority_and_path = root[len(LANDING_SCHEME_PREFIX):]
    if not authority_and_path or authority_and_path.startswith("/"):
        raise ValueError("landing_root must include the storage account authority")
    authority = authority_and_path.split("/", 1)[0]
    if not authority or authority.endswith("@"):
        raise ValueError("landing_root authority is empty")
    if not isinstance(relative, str) or not relative:
        raise ValueError("relative landing path is required")
    if "\\" in relative or relative.startswith("/") or relative.endswith("/"):
        raise ValueError(f"unsafe landing-relative path: {relative!r}")
    for segment in relative.split("/"):
        if not segment or segment in (".", "..") or "\\" in segment or ":" in segment:
            raise ValueError(f"unsafe landing path segment in {relative!r}")
    if authority_and_path[len(authority):]:
        for segment in authority_and_path[len(authority) + 1:].split("/"):
            if segment in (".", "..") or "\\" in segment:
                raise ValueError(f"unsafe landing_root segment in {landing_root!r}")
    return f"{root.rstrip('/')}/{relative}"
