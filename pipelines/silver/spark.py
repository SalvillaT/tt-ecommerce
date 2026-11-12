"""Adaptador PySpark perezoso del job de materialización Silver ``DEMO-001``.

Importar este módulo NO requiere PySpark: todos los usos de PySpark están
dentro de funciones. El núcleo puro (:mod:`pipelines.silver.core`) sigue siendo
el único motor de calidad/métricas; aquí solo se leen las tres tablas Bronze
del lote acotado, se serializan filas tipadas y se escriben las tablas
gestionadas de salida con ``replaceWhere`` por scope.

Lecturas: únicamente ``spark.table(<bronze raw>).where(batch_id = 'DEMO-001')``
seleccionando los campos del contrato de fuente más las AUDIT_COLUMNS. Nunca
se lee ``bronze.ingestion_run_log``, rutas de landing, external locations,
manifests, volúmenes ni checkpoints; nunca se emite SQL de escritura contra
Bronze (ni DDL ni datos).
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import json
from typing import Callable, Mapping

from pipelines.bronze.contracts import BRONZE_CONTRACTS
from pipelines.silver.core import (
    GoldDailySalesSegment,
    PipelineResult,
    QuarantineRecord,
    SilverRow,
)
from pipelines.silver.tables import (
    CUSTOMERS_CURRENT_COLUMNS,
    CUSTOMERS_CURRENT_TABLE,
    GOLD_COLUMNS,
    GOLD_TABLE,
    MATERIALIZATION_REPORT_COLUMNS,
    MATERIALIZATION_REPORT_TABLE,
    MATERIALIZATION_SCOPE,
    ORDERS_CURRENT_COLUMNS,
    ORDERS_CURRENT_TABLE,
    PRODUCTS_CURRENT_COLUMNS,
    PRODUCTS_CURRENT_TABLE,
    QUARANTINE_COLUMNS,
    QUARANTINE_TABLE,
    SCOPE_COLUMN,
)

# Delta scoped-overwrite predicate: fixed to the single supported scope.
REPLACE_WHERE = f"{SCOPE_COLUMN} = '{MATERIALIZATION_SCOPE}'"


def bronze_batch_rows(spark, source: str, *, batch_id: str = MATERIALIZATION_SCOPE) -> list[dict[str, object]]:
    """Read-only Bronze snapshot rows for the single supported batch.

    Each collected Spark row becomes ``row.asDict(recursive=True)`` and passes
    unchanged business/audit content into ``run_pipeline``. Any other
    ``batch_id`` is rejected before touching Spark.
    """
    if batch_id != MATERIALIZATION_SCOPE:
        raise ValueError(
            f"this materialization job only accepts batch_id = {MATERIALIZATION_SCOPE!r}, "
            f"got {batch_id!r}")
    contract = BRONZE_CONTRACTS[source]
    # Read-only table scan of the exact Bronze managed table, filtered to the
    # snapshot scope and projected to the contract fields + audit columns.
    frame = spark.table(contract.table).where(f"batch_id = '{batch_id}'")
    frame = frame.select(*[f"`{column}`" for column in contract.bronze_columns])
    return [dict(row.asDict(recursive=True)) for row in frame.collect()]


def _spark_schema(columns: tuple[tuple[str, str], ...]):
    """Explicit StructType for typed serialization (lazy PySpark import).

    Values are serialized into Spark DataFrames with this explicit schema so
    ``Decimal``/date/timestamp business content never relies on inference.
    """
    try:
        from pyspark.sql.types import (
            ArrayType,
            BooleanType,
            DateType,
            DecimalType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )
    except ImportError as exc:
        raise RuntimeError("PySpark is required only to materialize typed output rows") from exc
    factories: dict[str, Callable[[], object]] = {
        "STRING": StringType,
        "STRING NOT NULL": StringType,
        "BIGINT": LongType,
        "TIMESTAMP": TimestampType,
        "DATE": DateType,
        "BOOLEAN": BooleanType,
        "DECIMAL(18,2)": lambda: DecimalType(18, 2),
        "ARRAY<STRING>": lambda: ArrayType(StringType()),
    }
    return StructType([StructField(name, factories[sql_type](), True) for name, sql_type in columns])


def create_output_dataframe(spark, rows: list[dict[str, object]],
                            columns: tuple[tuple[str, str], ...]):
    return spark.createDataFrame(rows, schema=_spark_schema(columns))


def scoped_overwrite_write(dataframe, table: str) -> None:
    """Delta scoped overwrite for the fixed DEMO-001 scope.

    ``replaceWhere`` re-running semantics: every write replaces exactly the
    ``materialization_scope = 'DEMO-001'`` predicate scope, so re-running the
    job never appends duplicates. The replaceWhere predicate-constraint check
    stays enabled (it is never disabled by configuration here). This is
    idempotent scoped materialization, not atomic across output tables.
    """
    (dataframe.write.format("delta").mode("overwrite")
     .option("replaceWhere", REPLACE_WHERE)
     .saveAsTable(table))


# --- typed row serialization (pure; no PySpark required) ---------------------


def _coerce_string(value: object) -> str | None:
    return None if value is None else str(value)


def _coerce_bigint(value: object) -> int | None:
    return None if value is None else int(value)


def _coerce_decimal(value: object) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _coerce_timestamp(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo is not None else value
    # RFC3339 text (e.g. fixture-shaped audit strings) at the test boundary.
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _coerce_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return date.fromisoformat(str(value)[:10])


def _coerce_boolean(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "false"}:
        return text == "true"
    raise ValueError(f"not a boolean value: {value!r}")


def _coerce_array_string(value: object) -> list[str] | None:
    if value is None:
        return None
    return [str(item) for item in value]


_TYPE_COERCERS: dict[str, Callable[[object], object]] = {
    "STRING": _coerce_string,
    "STRING NOT NULL": _coerce_string,
    "BIGINT": _coerce_bigint,
    "TIMESTAMP": _coerce_timestamp,
    "DATE": _coerce_date,
    "BOOLEAN": _coerce_boolean,
    "DECIMAL(18,2)": _coerce_decimal,
    "ARRAY<STRING>": _coerce_array_string,
}


def _coerced(values: Mapping[str, object], columns: tuple[tuple[str, str], ...]) -> dict[str, object]:
    """Order and type a row dict against its explicit column contract."""
    output: dict[str, object] = {}
    for name, sql_type in columns:
        if name not in values:
            raise ValueError(f"missing output value for column {name!r}")
        output[name] = _TYPE_COERCERS[sql_type](values[name])
    return output


_SILVER_BUSINESS_COLUMNS = {
    "orders": ORDERS_CURRENT_COLUMNS,
    "customers": CUSTOMERS_CURRENT_COLUMNS,
    "products": PRODUCTS_CURRENT_COLUMNS,
}


def _lineage_values(row: SilverRow) -> dict[str, object]:
    lineage = row.lineage
    return {
        "batch_id": lineage.batch_id,
        "contract_version": lineage.contract_version,
        "manifest_file_sha256": lineage.manifest_file_sha256,
        "source_file": lineage.source_file,
        "source_file_size": lineage.source_file_size,
        "source_file_modification_time": lineage.source_file_modification_time,
        "ingested_at": lineage.ingested_at,
        "pipeline_run_id": lineage.pipeline_run_id,
    }


def silver_output_row(row: SilverRow, *, materialization_scope: str = MATERIALIZATION_SCOPE) -> dict[str, object]:
    """One typed current-entity row: business + lineage + row-audit fields."""
    values = dict(row.as_dict())
    # Deliberate rename for the curated layer (see tables.py): the customers
    # and products business field `updated_at` becomes `record_updated_at`;
    # the row-audit `updated_at` keeps the canonical name.
    if "updated_at" in values:
        values["record_updated_at"] = values.pop("updated_at")
    merged = {
        **values,
        **_lineage_values(row),
        "materialization_scope": materialization_scope,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
        "deleted_at": row.deleted_at,
        "source_system": row.source_system,
        "quality_status": row.quality_status,
    }
    return _coerced(merged, _SILVER_BUSINESS_COLUMNS[row.source])


def quarantine_output_row(record: QuarantineRecord, *, materialization_scope: str = MATERIALIZATION_SCOPE) -> dict[str, object]:
    merged = {
        "materialization_scope": materialization_scope,
        "source": record.source,
        "available_key": record.available_key,
        "rule_ids": record.rule_ids,
        "details": record.details,
        "batch_id": record.batch_id,
        "source_file": record.source_file,
        "manifest_file_sha256": record.manifest_file_sha256,
        "payload_reference": record.payload_reference,
        "quarantined_at": record.quarantined_at,
        "quality_status": record.quality_status,
    }
    return _coerced(merged, QUARANTINE_COLUMNS)


def gold_output_row(segment: GoldDailySalesSegment, *, materialization_scope: str = MATERIALIZATION_SCOPE) -> dict[str, object]:
    merged = {
        "materialization_scope": materialization_scope,
        "business_date": segment.business_date,
        "category": segment.category,
        "region_code": segment.region_code,
        "channel": segment.channel,
        "total_revenue_eur": segment.total_revenue_eur,
        "order_count": segment.order_count,
        "refunded_line_count": segment.refunded_line_count,
        "refreshed_at": segment.refreshed_at,
        "source_system": segment.source_system,
        "quality_status": segment.quality_status,
    }
    return _coerced(merged, GOLD_COLUMNS)


def report_output_row(result: PipelineResult, *, audit_timestamp: datetime,
                      recorded_at: datetime | None = None,
                      materialization_scope: str = MATERIALIZATION_SCOPE) -> dict[str, object]:
    """Safe completion/evidence marker row (persisted LAST)."""
    report = result.quality_report.as_dict()
    controls = dict(result.pre_suppression_controls)
    compact = lambda mapping: json.dumps(mapping, sort_keys=True, separators=(",", ":"))
    merged = {
        "materialization_scope": materialization_scope,
        "batch_id": materialization_scope,
        # Expected fixture quarantine records make Gold a published-despite-
        # quarantine run: explicitly labelled, not a clean production success.
        "status": "COMPLETED_WITH_QUARANTINE" if result.quarantine else "COMPLETED",
        "audit_timestamp": audit_timestamp,
        "recorded_at": recorded_at if recorded_at is not None else audit_timestamp,
        "by_rule_json": compact(report["by_rule"]),
        "by_source_json": compact(report["by_source"]),
        "by_parse_status_json": compact(report["by_parse_status"]),
        "current_by_source_json": compact(report["current_by_source"]),
        "quarantine_by_source_json": compact(report["quarantine_by_source"]),
        "published_cells": report["published_cells"],
        "suppressed_cells": report["suppressed_cells"],
        "duplicate_count": report["duplicate_count"],
        "control_total_revenue_eur": controls["total_revenue_eur"],
        "control_order_count": controls["order_count"],
        "control_refunded_line_count": controls["refunded_line_count"],
    }
    return _coerced(merged, MATERIALIZATION_REPORT_COLUMNS)


# --- materialization execution ----------------------------------------------


def bootstrap_output_tables(spark) -> None:
    """Create the managed output tables only when the job service principal runs."""
    from pipelines.silver.tables import output_table_ddls

    for ddl in output_table_ddls():
        spark.sql(ddl)


def materialize(spark, result: PipelineResult, *, audit_timestamp: datetime,
                recorded_at: datetime | None = None,
                bootstrap: Callable = bootstrap_output_tables) -> dict[str, object]:
    """Create managed tables and write typed output rows; the report LAST.

    Write order is fixed: orders → customers → products → quarantine → gold →
    report. A failure can leave earlier layers partially replaced (no
    cross-table atomicity); only the final report row marks a completed
    materialization.
    """
    if audit_timestamp.tzinfo is None or audit_timestamp.utcoffset() is None:
        raise ValueError("audit_timestamp must be timezone-aware")
    bootstrap(spark)
    writes: tuple[tuple[str, tuple[tuple[str, str], ...], list[dict[str, object]]], ...] = (
        (ORDERS_CURRENT_TABLE, ORDERS_CURRENT_COLUMNS,
         [silver_output_row(row) for row in result.orders]),
        (CUSTOMERS_CURRENT_TABLE, CUSTOMERS_CURRENT_COLUMNS,
         [silver_output_row(row) for row in result.customers]),
        (PRODUCTS_CURRENT_TABLE, PRODUCTS_CURRENT_COLUMNS,
         [silver_output_row(row) for row in result.products]),
        (QUARANTINE_TABLE, QUARANTINE_COLUMNS,
         [quarantine_output_row(record) for record in result.quarantine]),
        (GOLD_TABLE, GOLD_COLUMNS,
         [gold_output_row(segment) for segment in result.gold]),
    )
    for table, columns, rows in writes:
        frame = create_output_dataframe(spark, rows, columns)
        scoped_overwrite_write(frame, table)
    report = report_output_row(
        result, audit_timestamp=audit_timestamp, recorded_at=recorded_at)
    # Written LAST: the completion/evidence marker.
    report_frame = create_output_dataframe(spark, [report], MATERIALIZATION_REPORT_COLUMNS)
    scoped_overwrite_write(report_frame, MATERIALIZATION_REPORT_TABLE)
    return report
