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
