locals {
  resource_group_name  = coalesce(var.resource_group_name, "rg-${var.name_prefix}-lakehouse-weu")
  storage_account_name = coalesce(var.storage_account_name, "st${var.name_prefix}lakeweu")
  databricks_name      = "dbw-${var.name_prefix}-lakehouse-weu"
}

resource "azurerm_resource_group" "platform" {
  name     = local.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_storage_account" "lake" {
  name                            = local.storage_account_name
  resource_group_name             = azurerm_resource_group.platform.name
  location                        = azurerm_resource_group.platform.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  account_kind                    = "StorageV2"
  is_hns_enabled                  = true
  https_traffic_only_enabled      = true
  min_tls_version                 = "TLS1_2"
  shared_access_key_enabled       = false
  allow_nested_items_to_be_public = false
  public_network_access_enabled   = true

  tags = var.tags
}

resource "azurerm_storage_container" "lake" {
  for_each = toset(["landing", "lakehouse"])

  name                  = each.value
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

resource "azurerm_storage_data_lake_gen2_path" "directories" {
  for_each = {
    landing_orders    = { filesystem = "landing", path = "orders" }
    landing_customers = { filesystem = "landing", path = "customers" }
    landing_products  = { filesystem = "landing", path = "products" }
    lake_bronze       = { filesystem = "lakehouse", path = "bronze" }
    lake_silver       = { filesystem = "lakehouse", path = "silver" }
    lake_quarantine   = { filesystem = "lakehouse", path = "quarantine" }
    lake_gold         = { filesystem = "lakehouse", path = "gold" }
  }

  path               = each.value.path
  filesystem_name    = azurerm_storage_container.lake[each.value.filesystem].name
  storage_account_id = azurerm_storage_account.lake.id
  resource           = "directory"
}

resource "azurerm_databricks_workspace" "lakehouse" {
  name                          = local.databricks_name
  resource_group_name           = azurerm_resource_group.platform.name
  location                      = azurerm_resource_group.platform.location
  sku                           = "premium"
  public_network_access_enabled = true

  tags = var.tags
}

resource "azurerm_databricks_access_connector" "lakehouse" {
  name                = "dbac-${var.name_prefix}-lakehouse-weu"
  resource_group_name = azurerm_resource_group.platform.name
  location            = azurerm_resource_group.platform.location

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

resource "azurerm_role_assignment" "connector_storage_blob_data_contributor" {
  scope                = azurerm_storage_container.lake["lakehouse"].resource_manager_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.lakehouse.identity[0].principal_id
}
