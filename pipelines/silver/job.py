"""Job boundary del materializador Silver del lote único ``DEMO-001``.

Contrato del Job:

* Alcance deliberadamente acotado a UNA instantánea Bronze completa
  (``batch_id = DEMO-001``): no es streaming/CDC/SCD/incremental genérico y
  rechaza cualquier otro valor.
* CLI: exactamente ``--batch-id`` (por defecto DEMO-001, validado exacto) y
  ``--audit-timestamp`` obligatorio en formato RFC3339 UTC terminado en ``Z``.
  El catálogo/schema/tablas y el alcance son constantes locales, nunca
  parámetros editables del run.
* Sin import PySpark a nivel de módulo; Spark se pide perezosamente.
* El núcleo puro (:mod:`pipelines.silver.core`) es el único motor de
  calidad/versión/métricas.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import re

from pipelines.silver.core import run_pipeline
from pipelines.silver.tables import MATERIALIZATION_SCOPE

ALLOWED_BATCH_ID = MATERIALIZATION_SCOPE
_AUDIT_TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")


def validate_audit_timestamp(text: str) -> datetime:
    """Parse a UTC RFC3339 ``...Z`` audit timestamp (deterministic runs)."""
    if not isinstance(text, str) or not _AUDIT_TIMESTAMP_RE.fullmatch(text):
        raise ValueError("audit-timestamp must be UTC RFC3339 'YYYY-MM-DDTHH:MM:SSZ' ending in 'Z'")
    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError("audit-timestamp must be a valid UTC RFC3339 timestamp") from exc


@dataclass(frozen=True)
class SilverJobConfig:
    batch_id: str
    audit_timestamp: datetime


def parse_job_args(argv: list[str] | None = None) -> SilverJobConfig:
    parser = argparse.ArgumentParser(
        prog="run_silver_demo_001_job",
        description="Materialización Silver→quarantine→Gold del lote único DEMO-001 "
                    "(reemplazo por scope, reporte al final).",
    )
    parser.add_argument("--batch-id", default=ALLOWED_BATCH_ID,
                        help="Debe ser exactamente DEMO-001 (único lote soportado).")
    parser.add_argument("--audit-timestamp", required=True,
                        help="RFC3339 UTC terminado en Z; fija los timestamps del run.")
    args = parser.parse_args(argv)
    try:
        if args.batch_id != ALLOWED_BATCH_ID:
            raise ValueError(
                f"batch_id {args.batch_id!r} is not the single supported snapshot "
                f"batch {ALLOWED_BATCH_ID!r}")
        audit_timestamp = validate_audit_timestamp(args.audit_timestamp)
    except ValueError as exc:
        parser.exit(2, f"error: {exc}\n")
    return SilverJobConfig(batch_id=args.batch_id, audit_timestamp=audit_timestamp)


def get_default_spark():
    """PySpark session is only requested at execution time (workspace/Job)."""
    from pyspark.sql import SparkSession  # lazy: not required for tests
    return SparkSession.builder.getOrCreate()


def default_read_bronze(spark, source: str, *, batch_id: str) -> list[dict]:
    from pipelines.silver.spark import bronze_batch_rows  # lazy adapter import
    return bronze_batch_rows(spark, source, batch_id=batch_id)


def default_materialize(spark, result, *, audit_timestamp: datetime) -> dict:
    from pipelines.silver.spark import materialize  # lazy adapter import
    return materialize(spark, result, audit_timestamp=audit_timestamp)


def execute_materialization(spark, config: SilverJobConfig, *,
                            read_bronze=default_read_bronze,
                            transform=run_pipeline,
                            materialize=default_materialize) -> dict:
    """One full bounded Bronze snapshot → pure core → typed scoped writes."""
    orders = read_bronze(spark, "orders", batch_id=config.batch_id)
    customers = read_bronze(spark, "customers", batch_id=config.batch_id)
    products = read_bronze(spark, "products", batch_id=config.batch_id)
    result = transform(orders=orders, customers=customers, products=products,
                       audit_timestamp=config.audit_timestamp)
    return materialize(spark, result, audit_timestamp=config.audit_timestamp)
