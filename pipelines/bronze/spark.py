"""Adaptadores PySpark perezosos: importar este módulo no requiere PySpark."""

from __future__ import annotations

from .contracts import AUDIT_COLUMNS, BRONZE_CONTRACTS, SourceContract
from .ddl import RUN_LOG_SQL_TYPES, bootstrap_bronze_tables
from .layout import (
    LandingLayout,
    join_landing_uri,
    landing_relative_path,
    validate_source_filename,
)
from .manifest import SourceReceipt
from .run_log import RunMetrics


def autoloader_options(contract: SourceContract, layout: LandingLayout, batch_id: str,
                       filename: str | None = None) -> dict[str, str]:
    """Auto Loader options for one contract.

    When the exact approved filename is provided, `pathGlobFilter` is fixed to
    it: the stream reads the batch DIRECTORY (`.load(directory_uri)` is kept —
    never an individual file URI) but only objects whose exact approved
    filename matches can be ingested, so nothing outside the receipt can enter
    Bronze through this stream. The validated name is glob-literal safe
    (`layout.validate_source_filename`).
    """
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
    if filename is not None:
        options.update({"pathGlobFilter": validate_source_filename(filename)})
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


def source_reader(spark, source: str, batch_id: str, layout: LandingLayout,
                  filename: str | None = None):
    """Return a configured reader; callers start it only after manifest preflight.

    With a validated exact `filename`, the Auto Loader is bound to it via
    `pathGlobFilter` (defense in depth: even if the landing directory were
    mutated after preflight, this stream cannot ingest other names).
    """
    contract = BRONZE_CONTRACTS[source]
    options = autoloader_options(contract, layout, batch_id, filename=filename)
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
                  landing_root: str | None = None, filename: str | None = None):
    """Configured stream over one approved batch directory, never a source root.

    When the Job passes an explicit `landing_root`, the Auto Loader reads the
    physical final directory for this batch (inside the already granted
    external locations); the relative `landing/...` logical path is never used
    there. The read location is ALWAYS the batch directory (`.load()` keeps a
    directory URI, never an individual file URI): with a validated exact
    `filename`, `pathGlobFilter` constrains ingestion to that exact name.
    """
    reader, options = source_reader(spark, source, batch_id, layout, filename=filename)
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


def _run_log_schema():
    """Return the explicit input schema for one run-log event.

    ``RunMetrics.event_at`` is deliberately an RFC3339 string at this boundary;
    the projection in :func:`append_run_log` performs the contract's TIMESTAMP
    cast.  Every other input field uses its physical run-log SQL type, so a
    legitimate nullable ``source`` or ``error_class`` cannot make Spark infer
    ``NullType`` before that projection runs.
    """
    try:
        from pyspark.sql.types import (  # lazy import keeps local modules importable
            BooleanType,
            LongType,
            StringType,
            StructField,
            StructType,
        )
    except ImportError as exc:
        raise RuntimeError("PySpark is required only to append run-log events") from exc

    sql_type_factories = {
        "STRING": StringType,
        "BIGINT": LongType,
        "BOOLEAN": BooleanType,
    }
    fields = []
    for name, sql_type in RUN_LOG_SQL_TYPES.items():
        # The input is RFC3339 text; selectExpr below owns the typed timestamp
        # conversion to the managed-table contract.
        data_type = StringType() if name == "event_at" else sql_type_factories[sql_type]()
        fields.append(StructField(name, data_type, True))
    return StructType(fields)


def append_run_log(spark, metrics: RunMetrics) -> None:
    """Append one validated attempt/event; caller must bootstrap managed tables first."""
    if not isinstance(metrics, RunMetrics):
        raise TypeError("metrics must be RunMetrics")
    # Validation happens on construction and as_dict preserves the stable contract.
    payload = metrics.as_dict()
    # Schema is supplied before inference: selectExpr is too late to repair a
    # NullType inferred from a valid nullable STARTED event.
    # Explicit cast lets injected RFC3339 event_at become the typed table timestamp.
    dataframe = spark.createDataFrame([payload], schema=_run_log_schema()).selectExpr(*[
        f"CAST(`{name}` AS {sql_type}) AS `{name}`" for name, sql_type in RUN_LOG_SQL_TYPES.items()
    ])
    dataframe.write.format("delta").mode("append").saveAsTable("tt_ecommerce_synthetic.bronze.ingestion_run_log")


def validate_receipt_filename(receipt: SourceReceipt) -> str:
    """Ensure the receipt filename is THE contract's exact approved name.

    The receipt comes from the remote preflight; this is the last guard that
    binds the runtime stream to the approved filename before any read is
    started (pure and cheap, safe to call before bootstrap).
    """
    if not isinstance(receipt, SourceReceipt):
        raise TypeError("receipt must be a validated SourceReceipt")
    contract = BRONZE_CONTRACTS[receipt.source]
    expected = contract.filename(receipt.batch_id)
    if receipt.filename != expected:
        raise ValueError(
            f"receipt filename {receipt.filename!r} is not the approved contract "
            f"filename {expected!r} for source '{receipt.source}'")
    return validate_source_filename(receipt.filename)


def execute_source_receipt(spark, receipt: SourceReceipt, *, arrival_date: str,
                           pipeline_run_id: str, layout: LandingLayout | None = None,
                           landing_root: str | None = None):
    """Start one availableNow managed-table stream for an already validated receipt.

    This composes only a per-batch input directory (physical URI when
    `landing_root` is provided), loaded as a DIRECTORY and constrained to the
    receipt's exact approved filename via `pathGlobFilter`. Remote
    manifest/ready-marker validation (including the exact-one-file directory
    audit) must have completed before invocation. It makes no cross-table
    atomicity claim; retained checkpoint semantics are described separately.
    """
    if not isinstance(receipt, SourceReceipt):
        raise TypeError("receipt must be a validated SourceReceipt")
    layout = layout or LandingLayout()
    filename = validate_receipt_filename(receipt)
    contract = BRONZE_CONTRACTS[receipt.source]
    stream, options = source_stream(spark, receipt.source, receipt.batch_id, arrival_date, layout,
                                    landing_root=landing_root, filename=filename)
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
