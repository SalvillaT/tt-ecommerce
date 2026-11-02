output "storage_credential_name" {
  value = databricks_storage_credential.lakehouse.name
}

output "external_location_name" {
  value = databricks_external_location.lakehouse.name
}

output "sql_warehouse_id" {
  value       = try(databricks_sql_endpoint.demo[0].id, null)
  description = "Null unless enable_sql_warehouse is true."
}
