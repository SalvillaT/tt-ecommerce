"""Adaptadores PySpark perezosos: importar este módulo no requiere PySpark."""

from __future__ import annotations

from .contracts import AUDIT_COLUMNS, BRONZE_CONTRACTS, SourceContract
from .ddl import RUN_LOG_SQL_TYPES, bootstrap_bronze_tables
from .layout import LandingLayout, join_landing_uri, landing_relative_path
from .manifest import SourceReceipt
from .run_log import RunMetrics


def autoloader_options(contract: SourceContract, layout: LandingLayout, batch_id: str) -> dict[str, str]:
    options = {
        "cloudFiles.format": contract.cloud_format,
        "cloudFiles.schemaLocation": layout.schema_location(contract.source),
        "cloudFiles.schemaEvolutionMode": "rescue",
        "cloudFiles.validateOptions": "true",
        "rescuedDataColumn": "_rescued_data",
        "columnNameOfCorruptRecord": "_corrupt_record",
        "mode": "PERMISSIVE",
        "cloudFiles.cleanSource": "OFF",
        "cloudFiles.useNotifications": "false",
        "cloudFiles.maxFilesPerTrigger": "100",
        "checkpointLocation": layout.checkpoint(contract.source, batch_id),
    }
    if contract.cloud_format == "csv":
        options.update({"header": "true", "sep": ",", "encoding": "UTF-8"})
    return options


def _spark_schema(contract: SourceContract):
    try:
        from pyspark.sql.types import StringType, StructField, StructType  # lazy import
    except ImportError as exc:
        raise RuntimeError("PySpark is required only to build streaming readers") from exc
    # Strings preserve source semantics for Bronze; Silver performs type validation.
    return StructType([*(StructField(column, StringType(), True) for column in contract.data_columns),
                       StructField("_corrupt_record", StringType(), True)])


def source_reader(spark, source: str, batch_id: str, layout: LandingLayout):
    """Return a configured reader; callers start it only after manifest preflight."""
    contract = BRONZE_CONTRACTS[source]
    options = autoloader_options(contract, layout, batch_id)
    reader = spark.readStream.format("cloudFiles").schema(_spark_schema(contract))
    for key, value in options.items():
        if key != "checkpointLocation":
            reader = reader.option(key, value)
    return reader, options


def resolve_batch_input_uri(layout: LandingLayout, source: str, batch_id: str,
                            arrival_date: str, landing_root: str | None = None) -> str:
    """Physical input location for one batch: only the exact per-batch
    directory, never a source root.

    With an explicit `landing_root` (abfss://...) the canonical final
    directory is joined onto it; without one, the validated logical
    `landing/...` path is kept (local/test shapes and legacy callers).
    """
    logical = layout.final_directory(source, batch_id, arrival_date)
    if landing_root is None:
        return logical
    return join_landing_uri(landing_root, landing_relative_path(logical))


def source_stream(spark, source: str, batch_id: str, arrival_date: str, layout: LandingLayout,
                  landing_root: str | None = None):
    """Configured stream over one approved batch directory, never a source root.

    When the Job passes an explicit `landing_root`, the Auto Loader reads the
    physical final URI for this batch (inside the already granted external
    locations); the relative `landing/...` logical path is never used there.
    """
    reader, options = source_reader(spark, source, batch_id, layout)
    return reader.load(resolve_batch_input_uri(layout, source, batch_id, arrival_date, landing_root)), options


def add_provenance(dataframe, *, batch_id: str, pipeline_run_id: str, receipt_sha256: str):
    """Attach append-only Bronze metadata after a validated receipt.

    This function is deliberately PySpark-only but its imports stay inside the
    callable, preserving local testability.
    """
    try:
        from pyspark.sql import functions as F
    except ImportError as exc:
        raise RuntimeError("PySpark is required only to transform a stream") from exc
    payload_columns = [column for column in dataframe.columns if column not in {"_rescued_data", "_corrupt_record"}]
    malformed = F.col("_corrupt_record").isNotNull()
    rescued = F.col("_rescued_data").isNotNull()
    return (dataframe
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("contract_version", F.lit("v1"))
            .withColumn("manifest_file_sha256", F.lit(receipt_sha256))
            # Databricks exposes file metadata through `_metadata`; source data
            # cannot replace it because the physical field is selected here.
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_source_file_size", F.col("_metadata.file_size"))
            .withColumn("_source_file_modification_time", F.col("_metadata.file_modification_time"))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_pipeline_run_id", F.lit(pipeline_run_id))
            # Parsed representation, not exact bytes; receipt+landing file retain bytes.
            .withColumn("parsed_payload_canonical", F.to_json(F.struct(*payload_columns)))
            .withColumn("parse_status", F.when(malformed, F.lit("MALFORMED"))
                        .when(rescued, F.lit("RESCUED")).otherwise(F.lit("PARSED")))
            .withColumn("ingest_date", F.to_date(F.col("_ingested_at"))))


def bronze_append_writer(dataframe, contract: SourceContract, options: dict[str, str]):
    """Append-only Delta streaming sink started by `toTable()`.

    In Databricks, `DataStreamWriter.toTable()` starts the streaming query and
    returns the live `StreamingQuery`, so this seam returns exactly that
    object (`.start()` is NOT chained again); no merge, overwrite, update or
    delete. Callers (`execute_source_receipt` → `default_start_stream`)
    receive the running query and are responsible for `awaitTermination()`.
    """
    return (dataframe.writeStream.format("delta").outputMode("append")
            .option("checkpointLocation", options["checkpointLocation"])
            .trigger(availableNow=True).toTable(contract.table))


def append_run_log(spark, metrics: RunMetrics) -> None:
    """Append one validated attempt/event; caller must bootstrap managed tables first."""
    if not isinstance(metrics, RunMetrics):
        raise TypeError("metrics must be RunMetrics")
    # Validation happens on construction and as_dict preserves the stable contract.
    payload = metrics.as_dict()
    # Explicit cast lets injected RFC3339 event_at become the typed table timestamp.
    dataframe = spark.createDataFrame([payload]).selectExpr(*[
        f"CAST(`{name}` AS {sql_type}) AS `{name}`" for name, sql_type in RUN_LOG_SQL_TYPES.items()
    ])
    dataframe.write.format("delta").mode("append").saveAsTable("tt_ecommerce_synthetic.bronze.ingestion_run_log")


def execute_source_receipt(spark, receipt: SourceReceipt, *, arrival_date: str,
                           pipeline_run_id: str, layout: LandingLayout | None = None,
                           landing_root: str | None = None):
    """Start one availableNow managed-table stream for an already validated receipt.

    This composes only a per-batch input directory (physical URI when
    `landing_root` is provided). Remote manifest/ready-marker validation must
    have completed before invocation. It makes no cross-table atomicity claim;
    retained checkpoint semantics are described separately.
    """
    if not isinstance(receipt, SourceReceipt):
        raise TypeError("receipt must be a validated SourceReceipt")
    layout = layout or LandingLayout()
    contract = BRONZE_CONTRACTS[receipt.source]
    stream, options = source_stream(spark, receipt.source, receipt.batch_id, arrival_date, layout,
                                    landing_root=landing_root)
    enriched = add_provenance(
        stream, batch_id=receipt.batch_id, pipeline_run_id=pipeline_run_id,
        receipt_sha256=receipt.sha256,
    )
    return bronze_append_writer(enriched, contract, options)


def provenance_columns():
    """Names added by the execution adapter; documented separately for local tests."""
    return AUDIT_COLUMNS


def idempotency_limitations() -> str:
    return "immutable batch input + retained same checkpoint + Delta streaming sink; no checkpoint-loss guarantee or cross-table atomicity"
