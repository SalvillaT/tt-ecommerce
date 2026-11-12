"""No-action orchestration interface for a future Databricks Job."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .contracts import BRONZE_CONTRACTS
from .layout import LandingLayout, validate_batch_id
from .manifest import SourceReceipt, validate_manifest_batch, validate_ready_marker, validate_source_directory


@dataclass(frozen=True)
class BronzeJobRequest:
    batch_id: str
    arrival_date: str
    fixture_directory: Path | None = None

    def validate_local(self) -> None:
        validate_batch_id(self.batch_id)
        LandingLayout().final_file("orders", self.batch_id, self.arrival_date)


def preflight_request(request: BronzeJobRequest) -> tuple[SourceReceipt, ...]:
    """Local-only preflight. Spark/session creation is intentionally absent."""
    request.validate_local()
    if request.fixture_directory is None:
        raise ValueError("fixture_directory is required for local manifest preflight")
    return validate_manifest_batch(request.fixture_directory, request.batch_id)


def planned_sources() -> tuple[str, ...]:
    return tuple(BRONZE_CONTRACTS)


def validate_isolated_source_inputs(source_directories: dict[str, Path], batch_id: str, arrival_date: str) -> dict[str, Path]:
    """Local guard for the three exact per-batch directories before streaming."""
    if set(source_directories) != set(BRONZE_CONTRACTS):
        raise ValueError("all and only contract sources are required")
    return {
        source: validate_source_directory(directory, source, batch_id, arrival_date)
        for source, directory in source_directories.items()
    }
