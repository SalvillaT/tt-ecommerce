output "storage_credential_name" {
  value = databricks_storage_credential.lakehouse.name
}

output "external_location_name" {
  value = databricks_external_location.lakehouse.name
}

output "catalog_name" {
  value       = databricks_catalog.synthetic.name
  description = "Phase 1 synthetic Unity Catalog catalog using the metastore DEFAULT managed-storage location."
}

output "schema_ids" {
  value       = { for name, schema in databricks_schema.phase1 : name => schema.id }
  description = "Phase 1 bronze, silver, quarantine, and gold schema identifiers."
}

output "sql_warehouse_id" {
  value       = try(databricks_sql_endpoint.demo[0].id, null)
  description = "Null unless enable_sql_warehouse is true."
}
