output "storage_account_name" {
  value       = azurerm_storage_account.state.name
  description = "Use in an uncommitted AzureRM backend configuration."
}

output "container_name" {
  value       = azurerm_storage_container.state.name
  description = "Use in an uncommitted AzureRM backend configuration."
}
