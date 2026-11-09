"""Contrato serializable y determinista para el run-log append-only de eventos.

El log registra eventos/intentos, no una transacción entre las tres fuentes;
un reintento añade eventos nuevos. `metrics_observed` fija la semántica de
medición: `False` significa "progreso no disponible" (ningún `numInputRows`
real), y `True` con `rows_read == 0` significa "se observó progreso con 0
filas de entrada". Nunca se representan como cero medido el progreso ausente,
ni se infieren filas escritas/rescatadas/malformadas ni archivos e integridad
desde el manifest.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum


class RunLogEvent(str, Enum):
    STARTED = "STARTED"
    MANIFEST_VALIDATED = "MANIFEST_VALIDATED"
    SOURCE_COMMITTED = "SOURCE_COMMITTED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class RunMetrics:
    attempt_id: str
    pipeline_run_id: str
    batch_id: str
    source: str | None
    event: RunLogEvent
    event_at: str
    contract_version: str = "v1"
    files_discovered: int = 0
    files_committed: int = 0
    rows_declared: int = 0
    rows_read: int = 0
    rows_written: int = 0
    rows_parsed: int = 0
    rows_rescued: int = 0
    rows_malformed: int = 0
    schema_drift_count: int = 0
    duplicate_files_skipped: int = 0
    error_class: str | None = None
    metrics_observed: bool = False

    def __post_init__(self) -> None:
        if not self.attempt_id or not self.pipeline_run_id or not self.batch_id or not self.event_at:
            raise ValueError("run log identity and injected event_at are required")
        if any(value < 0 for key, value in asdict(self).items() if key.startswith(("files_", "rows_", "schema_", "duplicate_"))):
            raise ValueError("run log metrics cannot be negative")
        # Honest semantics: observed progress implies the measurement exists;
        # measured non-zero rows must never appear with metrics_observed False.
        if self.rows_read > 0 and not self.metrics_observed:
            raise ValueError("rows_read above zero requires metrics_observed=True")
        if self.event is RunLogEvent.FAILED and not self.error_class:
            raise ValueError("FAILED requires error_class")

    def as_dict(self) -> dict:
        data = asdict(self)
        data["event"] = self.event.value
        return data
