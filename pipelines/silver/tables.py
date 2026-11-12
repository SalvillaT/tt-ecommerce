"""Physical output contracts for the DEMO-001 Silver materialization job.

Pure module (no PySpark import, no I/O): managed-table names, explicit column
contracts (name, SQL type) and the managed Delta DDL. The Silver job service
principal is the sole first creator/owner of these managed tables (Terraform
grants only USE_SCHEMA + CREATE_TABLE on silver/quarantine/gold and never
pre-creates them), mirroring the Bronze ownership invariant.

Materialization semantics: every output table carries a
``materialization_scope STRING NOT NULL`` partition column and is written with
a Delta ``replaceWhere`` scoped overwrite (predicate constraint kept enabled),
so re-running the job replaces exactly scope ``DEMO-001`` and never appends
duplicates. This is idempotent scoped materialization; it is NOT atomic across
output tables — only the report, written LAST, marks a completed run.
"""

from __future__ import annotations

from pipelines.bronze.contracts import CATALOG

# The single, fixed snapshot scope. The job deliberately supports exactly one
# full bounded Bronze snapshot (batch_id = DEMO-001); it is not a generic
# incremental/streaming/SCD materialization.
MATERIALIZATION_SCOPE = "DEMO-001"
SCOPE_COLUMN = "materialization_scope"

SILVER_SCHEMA = "silver"
QUARANTINE_SCHEMA = "quarantine"
GOLD_SCHEMA = "gold"


def _table(schema: str, name: str) -> str:
    return f"{CATALOG}.{schema}.{name}"


ORDERS_CURRENT_TABLE = _table(SILVER_SCHEMA, "orders_current")
CUSTOMERS_CURRENT_TABLE = _table(SILVER_SCHEMA, "customers_current")
PRODUCTS_CURRENT_TABLE = _table(SILVER_SCHEMA, "products_current")
QUARANTINE_TABLE = _table(QUARANTINE_SCHEMA, "data_quality_rejections")
GOLD_TABLE = _table(GOLD_SCHEMA, "daily_sales_segment")
MATERIALIZATION_REPORT_TABLE = _table(SILVER_SCHEMA, "materialization_run_report")


# Column contracts: ordered (name, SQL type) tuples. The scope column carries a
# NOT NULL table constraint (kept in the SQL type text for the DDL; the Spark
# schema builder strips the suffix).

# Business fields are the typed source-contract fields. Two deliberate
# renames/omissions for the curated layer:
# - the customers/products business field `updated_at` (record version time)
#   is emitted as `record_updated_at` because the row-audit column
#   `updated_at` (materialization timestamp) already occupies the canonical
#   name;
# - the customers business field `deleted_at` equals SilverRow.deleted_at and
#   is therefore emitted exactly once, through the row-audit `deleted_at`
#   column (duplicate DDL column names are illegal in Delta).
ORDERS_CURRENT_COLUMNS: tuple[tuple[str, str], ...] = (
    (SCOPE_COLUMN, "STRING NOT NULL"),
    ("order_id", "STRING"), ("line_id", "STRING"), ("record_version", "BIGINT"),
    ("event_at", "TIMESTAMP"), ("business_date", "DATE"), ("customer_id", "STRING"),
    ("product_id", "STRING"), ("status", "STRING"), ("quantity", "BIGINT"),
    ("unit_price_eur", "DECIMAL(18,2)"), ("gross_amount_eur", "DECIMAL(18,2)"),
    ("refund_amount_eur", "DECIMAL(18,2)"), ("net_revenue_eur", "DECIMAL(18,2)"),
    ("currency", "STRING"), ("channel", "STRING"), ("region_code", "STRING"),
    ("note", "STRING"),
    # Lineage fields from SilverRow.lineage (Bronze audit names clarified:
    # no underscore-prefixed physical Bronze names reach the curated layer).
    ("batch_id", "STRING"), ("contract_version", "STRING"),
    ("manifest_file_sha256", "STRING"), ("source_file", "STRING"),
    ("source_file_size", "BIGINT"), ("source_file_modification_time", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"), ("pipeline_run_id", "STRING"),
    # Row-audit fields from the current SilverRow.
    ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP"),
    ("deleted_at", "TIMESTAMP"), ("source_system", "STRING"), ("quality_status", "STRING"),
)

CUSTOMERS_CURRENT_COLUMNS: tuple[tuple[str, str], ...] = (
    (SCOPE_COLUMN, "STRING NOT NULL"),
    ("customer_id", "STRING"), ("record_version", "BIGINT"),
    ("record_updated_at", "TIMESTAMP"), ("region_code", "STRING"),
    ("email", "STRING"), ("phone", "STRING"),
    ("batch_id", "STRING"), ("contract_version", "STRING"),
    ("manifest_file_sha256", "STRING"), ("source_file", "STRING"),
    ("source_file_size", "BIGINT"), ("source_file_modification_time", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"), ("pipeline_run_id", "STRING"),
    ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP"),
    ("deleted_at", "TIMESTAMP"), ("source_system", "STRING"), ("quality_status", "STRING"),
)

PRODUCTS_CURRENT_COLUMNS: tuple[tuple[str, str], ...] = (
    (SCOPE_COLUMN, "STRING NOT NULL"),
    ("product_id", "STRING"), ("record_version", "BIGINT"),
    ("record_updated_at", "TIMESTAMP"), ("name", "STRING"), ("brand", "STRING"),
    ("category", "STRING"), ("list_price_eur", "DECIMAL(18,2)"),
    ("currency", "STRING"), ("active", "BOOLEAN"),
    ("batch_id", "STRING"), ("contract_version", "STRING"),
    ("manifest_file_sha256", "STRING"), ("source_file", "STRING"),
    ("source_file_size", "BIGINT"), ("source_file_modification_time", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"), ("pipeline_run_id", "STRING"),
    ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP"),
    ("deleted_at", "TIMESTAMP"), ("source_system", "STRING"), ("quality_status", "STRING"),
)

# Quarantine keeps the rejection contract only: no raw payload is persisted
# (payload_reference is the deterministic hash pointer).
QUARANTINE_COLUMNS: tuple[tuple[str, str], ...] = (
    (SCOPE_COLUMN, "STRING NOT NULL"),
    ("source", "STRING"), ("available_key", "ARRAY<STRING>"),
    ("rule_ids", "ARRAY<STRING>"), ("details", "ARRAY<STRING>"),
    ("batch_id", "STRING"), ("source_file", "STRING"),
    ("manifest_file_sha256", "STRING"), ("payload_reference", "STRING"),
    ("quarantined_at", "TIMESTAMP"), ("quality_status", "STRING"),
)

# Canonical Gold grain/measures only: never customer/order identifiers, email,
# phone, payload references or privacy subject counts.
GOLD_COLUMNS: tuple[tuple[str, str], ...] = (
    (SCOPE_COLUMN, "STRING NOT NULL"),
    ("business_date", "DATE"), ("category", "STRING"), ("region_code", "STRING"),
    ("channel", "STRING"), ("total_revenue_eur", "DECIMAL(18,2)"),
    ("order_count", "BIGINT"), ("refunded_line_count", "BIGINT"),
    ("refreshed_at", "TIMESTAMP"), ("source_system", "STRING"), ("quality_status", "STRING"),
)

# Safe completion/evidence marker, written LAST. Quality maps/counters come
# from the pure core report; pre-suppression controls and published/suppressed
# cell totals are persisted, but suppressed-cell coordinates and subject
# counts are NOT (k-anonymity suppression must not be invertible).
MATERIALIZATION_REPORT_COLUMNS: tuple[tuple[str, str], ...] = (
    (SCOPE_COLUMN, "STRING NOT NULL"),
    ("batch_id", "STRING"), ("status", "STRING"),
    ("audit_timestamp", "TIMESTAMP"), ("recorded_at", "TIMESTAMP"),
    ("by_rule_json", "STRING"), ("by_source_json", "STRING"),
    ("by_parse_status_json", "STRING"), ("current_by_source_json", "STRING"),
    ("quarantine_by_source_json", "STRING"),
    ("published_cells", "BIGINT"), ("suppressed_cells", "BIGINT"),
    ("duplicate_count", "BIGINT"),
    ("control_total_revenue_eur", "DECIMAL(18,2)"),
    ("control_order_count", "BIGINT"), ("control_refunded_line_count", "BIGINT"),
)

_OUTPUT_TABLES: tuple[tuple[str, tuple[tuple[str, str], ...], str], ...] = (
    (ORDERS_CURRENT_TABLE, ORDERS_CURRENT_COLUMNS, "silver"),
    (CUSTOMERS_CURRENT_TABLE, CUSTOMERS_CURRENT_COLUMNS, "silver"),
    (PRODUCTS_CURRENT_TABLE, PRODUCTS_CURRENT_COLUMNS, "silver"),
    (QUARANTINE_TABLE, QUARANTINE_COLUMNS, "quarantine"),
    (GOLD_TABLE, GOLD_COLUMNS, "gold"),
    (MATERIALIZATION_REPORT_TABLE, MATERIALIZATION_REPORT_COLUMNS, "silver"),
)

_OUTPUT_TABLE_NAMES = frozenset(table for table, _, _ in _OUTPUT_TABLES)


def output_table_ddl(table: str, columns: tuple[tuple[str, str], ...], *, layer: str) -> str:
    """Managed Delta DDL for one output table (never an external LOCATION)."""
    if table not in _OUTPUT_TABLE_NAMES:
        raise ValueError(f"unknown output table: {table!r}")
    column_lines = ",\n".join(f"  `{name}` {sql_type}" for name, sql_type in columns)
    return "\n".join([
        f"CREATE TABLE IF NOT EXISTS {table} (",
        column_lines,
        ")",
        "USING DELTA",
        "PARTITIONED BY (materialization_scope)",
        f"COMMENT 'Managed {layer}-layer output of the DEMO-001 materialization job; "
        "created and owned by the job service principal.'",
        f"TBLPROPERTIES ('tt_ecommerce.layer' = '{layer}', "
        "'tt_ecommerce.write_semantics' = 'demo-001-scoped-overwrite')",
    ])


def output_table_ddls() -> tuple[str, ...]:
    return tuple(output_table_ddl(table, columns, layer=layer) for table, columns, layer in _OUTPUT_TABLES)


def output_table_names() -> frozenset[str]:
    """Every managed output table the materialization job may create or write."""
    return _OUTPUT_TABLE_NAMES
