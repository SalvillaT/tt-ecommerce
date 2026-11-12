"""Entry point del Job Bronze (Fase 2): preflight remoto → DDL → streams.

Contrato del Job (acta F2 / documentos de Fase 2):

* El preflight remoto (`remote_preflight.remote_preflight`) corre ANTES de
  crear tablas/log; si falla, no se precrea nada y no se escribe run-log para
  fingir éxito (los eventos son evidencia, no maquillaje).
* Tras preflight correcto: `bootstrap_bronze_tables(spark)` crea las tablas
  gestionadas bajo el run-as (único primer creador/propietario; Terraform solo
  concede `CREATE_TABLE`), y después se escriben los eventos append-only del
  run-log: `STARTED`, `MANIFEST_VALIDATED` (una vez por run) y
  `SOURCE_COMMITTED` por fuente al terminar cada `awaitTermination()`.
* Las métricas de cada fuente se toman únicamente del progreso disponible de
  la query terminada (`numInputRows` en `lastProgress`/`recentProgress`).
  `metrics_observed` distingue "progreso no disponible" de un 0 real observado;
  no se derivan filas escritas, archivos, rescatadas ni integridad desde el
  manifest. El run-log es un log de eventos por intento/fuente, no una fila
  final.
* Ante un error de una **fuente** tras el bootstrap se intenta escribir
  `FAILED` con `error_class` y se repropaga (exit != 0). Los errores al crear
  las tablas (DDL) o al escribir los eventos iniciales propagan exit != 0
  SIN prometer ningún evento `FAILED` (el propio run-log puede ser inalcanzable).
* La contención de concurrencia es
  `max_concurrent_runs=1` y retries 0 del Job; NO se declara exactly-once ni
  atomicidad entre tablas: la idempotencia sigue siendo la del checkpoint
  retenido/por fuente y el sink Delta append.
* Los helpers son honestos respecto a PySpark: el módulo funciona sin
  PySpark hasta que se pide el Spark o un stream real; en tests se inyectan
  fake Spark/queries y fake `start_stream`/`preflight`.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import uuid
from dataclasses import dataclass
from typing import Callable

from .layout import (
    LandingLayout,
    validate_arrival_date,
    validate_batch_id,
    validate_landing_root,
)
from .manifest import ManifestPreflightError
from .remote_preflight import RemotePreflightError, RemotePreflightResult, remote_preflight
from .run_log import RunLogEvent, RunMetrics
from .spark import append_run_log, bootstrap_bronze_tables, validate_receipt_filename


def now_iso() -> str:
    """Injected UTC timestamp for run-log events (RunMetrics baseline)."""
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class BronzeJobConfig:
    batch_id: str
    arrival_date: str
    landing_root: str
    pipeline_run_id: str
    attempt_id: str


def parse_job_args(argv: list[str] | None = None) -> BronzeJobConfig:
    """Kept as thin wrapper over build_parser for CLI-compatibility tests.

    `--landing-root` is NOT a run-editable job parameter: the bundle task
    passes the bundle variable (deployment configuration) as a fixed
    argument. It is still validated tightly here: exactly the `landing`
    container root (`layout.validate_landing_root`), so no subpath/query/
    fragment/port or traversal root can ever reach the job.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        batch_id = validate_batch_id(args.batch_id)
        arrival_date = validate_arrival_date(args.arrival_date)
        landing_root = validate_landing_root(args.landing_root)
    except ValueError as exc:
        parser.exit(2, f"error: {exc}\n")
    return BronzeJobConfig(
        batch_id=batch_id,
        arrival_date=arrival_date,
        landing_root=landing_root,
        pipeline_run_id=args.pipeline_run_id or uuid.uuid4().hex,
        attempt_id=args.attempt_id or uuid.uuid4().hex,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_bronze_job",
        description="Bronze ingestion Job (preflight remoto de bytes → DDL → streams por fuente).",
    )
    parser.add_argument("--batch-id", required=True, help="Identificador de lote (1-64 chars seguros).")
    parser.add_argument("--arrival-date", required=True, help="Fecha de llegada YYYY-MM-DD del lote.")
    parser.add_argument("--landing-root", required=True,
                        help="Fixed deployment config (bundle variable, not a run-editable "
                             "parameter): exactly abfss://landing@<account>.dfs.core.windows.net.")
    parser.add_argument("--pipeline-run-id", default=None, help="ID de run del pipeline (opcional).")
    parser.add_argument("--attempt-id", default=None, help="ID de intento del run-log (opcional).")
    return parser


def get_default_spark():
    """PySpark session is only requested at execution time (workspace/Job)."""
    from pyspark.sql import SparkSession  # lazy: not required for tests
    return SparkSession.builder.getOrCreate()


def default_start_stream(spark, receipt, config: BronzeJobConfig, layout: LandingLayout):
    """Real streaming start (lazy PySpark) over the approved batch URI."""
    from .spark import execute_source_receipt  # lazy to keep import-time PySpark-free

    return execute_source_receipt(
        spark, receipt,
        arrival_date=config.arrival_date,
        pipeline_run_id=config.pipeline_run_id,
        layout=layout,
        landing_root=config.landing_root,
    )


def progress_input_rows(query) -> tuple[int, bool]:
    """Real `numInputRows` aggregation from available query progress.

    Returns `(rows_read, metrics_observed)` with precise semantics:
    `metrics_observed=False` means no progress carried a `numInputRows`
    value (progress absent — NOT a measured zero); `metrics_observed=True`
    with `rows_read=0` means real progress reported 0 input rows. Nothing
    about rows written, rescued, malformed, files or integrity is inferred
    from the manifest or from here. `recentProgress` implies `lastProgress`,
    so the latter is only used when the window is empty.
    """
    total = 0
    observed = False
    recent = getattr(query, "recentProgress", None)
    if recent:
        candidates = list(recent)
    else:
        last = getattr(query, "lastProgress", None)
        candidates = [last] if last else []
    for progress in candidates:
        value = progress.get("numInputRows") if isinstance(progress, dict) else None
        if isinstance(value, int) and not isinstance(value, bool):
            observed = True
            total += value
    return total, observed


def _metrics(config: BronzeJobConfig, source: str | None, event: RunLogEvent, **extra) -> RunMetrics:
    return RunMetrics(
        attempt_id=config.attempt_id,
        pipeline_run_id=config.pipeline_run_id,
        batch_id=config.batch_id,
        source=source,
        event=event,
        event_at=now_iso(),
        **extra,
    )


def execute_run(spark, config: BronzeJobConfig, *, layout: LandingLayout | None = None,
                preflight: Callable = remote_preflight,
                bootstrap: Callable = bootstrap_bronze_tables,
                log_event: Callable = append_run_log,
                start_stream: Callable = default_start_stream) -> list[dict]:
    """Run one approved batch end-to-end; raise on any failure.

    Order is contractual: remote preflight (read-only) → managed DDL under the
    job run-as → run-log events (append-only) → one sequential stream per
    source with `awaitTermination()`, using the streaming seam
    `start_stream(spark, receipt, config, layout)`.

    Failure semantics (exact):
      * A preflight failure (or a preflight receipt not carrying the exact
        contract filename) raises without DDL or any run-log write.
      * An error AFTER bootstrap (per-source stream, `await` or its
        `SOURCE_COMMITTED` write) attempts one best-effort `FAILED` event for
        that source and re-raises; nothing is cleaned or repaired.
      * NOT every post-bootstrap error claims a `FAILED` event: errors while
        writing the bootstrap DDL or the initial events (`STARTED`,
        `MANIFEST_VALIDATED`) propagate as-is (exit != 0) without any promise
        that an event was recorded; the run-log itself may be unavailable.
    """
    layout = layout or LandingLayout()
    try:
        result: RemotePreflightResult = preflight(
            spark,
            landing_root=config.landing_root,
            batch_id=config.batch_id,
            arrival_date=config.arrival_date,
            layout=layout,
        )
    except Exception as exc:
        # Preflight failure: no tables/log precreation, no fake success log.
        raise RemotePreflightError(
            f"remote preflight failed for batch {config.batch_id}: {exc}") from exc

    # Receipt binding BEFORE any write (still zero side effects): every
    # preflight receipt must carry the exact contract filename for its source;
    # the runtime stream is later bound to this exact name (pathGlobFilter).
    for receipt in result.receipts:
        try:
            validate_receipt_filename(receipt)
        except Exception as exc:
            raise RemotePreflightError(
                f"preflight produced a receipt that does not match the v1 contract "
                f"for source '{receipt.source}': {exc}") from exc

    bootstrap(spark)
    records: list[dict] = []

    started = _metrics(config, source=None, event=RunLogEvent.STARTED)
    log_event(spark, started)
    records.append(started.as_dict())

    rows_declared = result.total_rows
    validated = _metrics(config, source=None, event=RunLogEvent.MANIFEST_VALIDATED,
                         files_discovered=len(result.receipts), rows_declared=rows_declared)
    log_event(spark, validated)
    records.append(validated.as_dict())

    for receipt in result.receipts:
        try:
            query = start_stream(spark, receipt, config, layout)
            query.awaitTermination()
            rows_read, metrics_observed = progress_input_rows(query)
            metrics = _metrics(config, source=receipt.source, event=RunLogEvent.SOURCE_COMMITTED,
                               rows_read=rows_read, metrics_observed=metrics_observed)
        except Exception as exc:
            failed = _metrics(config, source=receipt.source, event=RunLogEvent.FAILED,
                              error_class=f"{type(exc).__module__}.{type(exc).__name__}")
            try:
                log_event(spark, failed)
            except Exception:
                # The original failure is the evidence; never mask it.
                pass
            records.append(failed.as_dict())
            raise
        log_event(spark, metrics)
        records.append(metrics.as_dict())
    return records
