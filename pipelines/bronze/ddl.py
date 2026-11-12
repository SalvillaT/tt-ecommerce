"""Managed Bronze DDL and ownership guard rails; no Spark import or action here."""

from __future__ import annotations

from .contracts import AUDIT_COLUMNS, BRONZE_CONTRACTS, RUN_LOG_TABLE, SourceContract

AUDIT_SQL_TYPES = {
    "batch_id": "STRING",
    "contract_version": "STRING",
    "manifest_file_sha256": "STRING",
    "_source_file": "STRING",
    "_source_file_size": "BIGINT",
    "_source_file_modification_time": "TIMESTAMP",
    "_ingested_at": "TIMESTAMP",
    "_pipeline_run_id": "STRING",
    "parsed_payload_canonical": "STRING",
    "_rescued_data": "STRING",
    "_corrupt_record": "STRING",
    "parse_status": "STRING",
    "ingest_date": "DATE",
}

RUN_LOG_SQL_TYPES = {
    "attempt_id": "STRING", "pipeline_run_id": "STRING", "batch_id": "STRING",
    "source": "STRING", "event": "STRING", "event_at": "TIMESTAMP",
    "contract_version": "STRING", "files_discovered": "BIGINT",
    "files_committed": "BIGINT", "rows_declared": "BIGINT", "rows_read": "BIGINT",
    "rows_written": "BIGINT", "rows_parsed": "BIGINT", "rows_rescued": "BIGINT",
    "rows_malformed": "BIGINT", "schema_drift_count": "BIGINT",
    "duplicate_files_skipped": "BIGINT", "error_class": "STRING",
    "metrics_observed": "BOOLEAN",
}


def ownership_invariant() -> str:
    return (
        "The future job run-as service principal is the sole first creator and owner "
        "of all managed Bronze tables; Terraform grants only USE_CATALOG, USE_SCHEMA, "
        "and CREATE_TABLE and must never pre-create these tables."
    )


def managed_source_table_ddl(contract: SourceContract) -> str:
    columns = [f"  `{column}` STRING" for column in contract.data_columns]
    columns.extend(f"  `{column}` {AUDIT_SQL_TYPES[column]}" for column in AUDIT_COLUMNS)
    return "\n".join([
        f"CREATE TABLE IF NOT EXISTS {contract.table} (",
        ",\n".join(columns),
        ")",
        "USING DELTA",
        "PARTITIONED BY (ingest_date)",
        "COMMENT 'Append-only raw Bronze evidence; contract typing is deferred to Phase 3.'",
        "TBLPROPERTIES ('tt_ecommerce.layer' = 'bronze', 'tt_ecommerce.write_semantics' = 'append-only')",
    ])


def run_log_table_ddl() -> str:
    columns = ",\n".join(f"  `{column}` {sql_type}" for column, sql_type in RUN_LOG_SQL_TYPES.items())
    return "\n".join([
        f"CREATE TABLE IF NOT EXISTS {RUN_LOG_TABLE} (",
        columns,
        ")",
        "USING DELTA",
        "COMMENT 'Append-only Bronze ingestion attempt/event log; retries may add events.'",
        "TBLPROPERTIES ('tt_ecommerce.layer' = 'bronze', 'tt_ecommerce.write_semantics' = 'append-only-attempt-events')",
    ])


def bronze_table_ddls() -> tuple[str, ...]:
    return tuple(managed_source_table_ddl(contract) for contract in BRONZE_CONTRACTS.values()) + (run_log_table_ddl(),)


def bootstrap_bronze_tables(spark) -> None:
    """Create managed tables only when explicitly invoked by the future job SP."""
    for ddl in bronze_table_ddls():
        spark.sql(ddl)
