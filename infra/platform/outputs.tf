output "resource_group_name" {
  value = azurerm_resource_group.platform.name
}

output "storage_account_name" {
  value = azurerm_storage_account.lake.name
}

output "databricks_workspace_url" {
  value       = azurerm_databricks_workspace.lakehouse.workspace_url
  description = "Workspace host to pass to the separately gated databricks root."
}

output "databricks_access_connector_id" {
  value = azurerm_databricks_access_connector.lakehouse.id
}

output "databricks_access_connector_principal_id" {
  value = azurerm_databricks_access_connector.lakehouse.identity[0].principal_id
}

output "uc_access_connector_id" {
  description = "Resource ID of the dedicated SystemAssigned Access Connector for Unity Catalog managed storage (non-sensitive identifier)."
  value       = azurerm_databricks_access_connector.uc_managed.id
}

output "uc_access_connector_principal_id" {
  description = "Entra object ID (principal ID) of the dedicated UC managed-storage Access Connector system identity; non-sensitive identifier for use in Unity Catalog external-location/metastore configuration outside this root."
  value       = azurerm_databricks_access_connector.uc_managed.identity[0].principal_id
}

output "phase2_landing_access_connector_id" {
  description = "Null unless the separately approved Phase 2 landing Access Connector is enabled."
  value       = try(azurerm_databricks_access_connector.phase2_landing[0].id, null)
}

output "phase2_landing_access_connector_principal_id" {
  description = "Null unless the separately approved Phase 2 landing Access Connector is enabled; its identity has Storage Blob Data Reader on the landing container only."
  value       = try(azurerm_databricks_access_connector.phase2_landing[0].identity[0].principal_id, null)
}
