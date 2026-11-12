"""Contratos lógicos explícitos para Bronze; no importa PySpark."""

from __future__ import annotations

from dataclasses import dataclass

CATALOG = "tt_ecommerce_synthetic"
BRONZE_SCHEMA = "bronze"

AUDIT_COLUMNS = (
    "batch_id", "contract_version", "manifest_file_sha256", "_source_file",
    "_source_file_size", "_source_file_modification_time", "_ingested_at",
    "_pipeline_run_id", "parsed_payload_canonical", "_rescued_data",
    "_corrupt_record", "parse_status", "ingest_date",
)

ORDERS_COLUMNS = (
    "order_id", "line_id", "record_version", "event_at", "business_date",
    "customer_id", "product_id", "status", "quantity", "unit_price_eur",
    "gross_amount_eur", "refund_amount_eur", "net_revenue_eur", "currency",
    "channel", "region_code", "note",
)
CUSTOMERS_COLUMNS = (
    "customer_id", "record_version", "updated_at", "region_code", "email",
    "phone", "deleted_at",
)
PRODUCTS_COLUMNS = (
    "product_id", "record_version", "updated_at", "name", "brand", "category",
    "list_price_eur", "currency", "active",
)


@dataclass(frozen=True)
class SourceContract:
    source: str
    filename_prefix: str
    extension: str
    table_name: str
    data_columns: tuple[str, ...]
    cloud_format: str

    @property
    def table(self) -> str:
        return f"{CATALOG}.{BRONZE_SCHEMA}.{self.table_name}"

    @property
    def bronze_columns(self) -> tuple[str, ...]:
        """Physical Bronze columns: source text plus parse/audit observability."""
        return self.data_columns + AUDIT_COLUMNS

    def filename(self, batch_id: str) -> str:
        return f"{self.filename_prefix}_{batch_id}.{self.extension}"


BRONZE_CONTRACTS = {
    "orders": SourceContract("orders", "orders", "jsonl", "orders_raw", ORDERS_COLUMNS, "json"),
    "customers": SourceContract("customers", "customers", "csv", "customers_raw", CUSTOMERS_COLUMNS, "csv"),
    "products": SourceContract("products", "products", "csv", "products_raw", PRODUCTS_COLUMNS, "csv"),
}

RUN_LOG_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.ingestion_run_log"
