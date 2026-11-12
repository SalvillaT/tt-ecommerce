"""Núcleo local y sin dependencias de la ingesta Bronze de Fase 2."""

from .contracts import BRONZE_CONTRACTS, SourceContract
from .layout import LandingLayout
from .manifest import ManifestPreflightError, SourceReceipt, validate_manifest_batch
from .run_log import RunLogEvent, RunMetrics

__all__ = [
    "BRONZE_CONTRACTS",
    "LandingLayout",
    "ManifestPreflightError",
    "RunLogEvent",
    "RunMetrics",
    "SourceContract",
    "SourceReceipt",
    "validate_manifest_batch",
]
