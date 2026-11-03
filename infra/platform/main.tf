locals {
  location_slug = lower(replace(replace(var.location, " ", ""), "-", ""))
  region_suffix = lookup({ francecentral = "frc" }, local.location_slug, substr(local.location_slug, 0, 3))

  resource_group_name   = coalesce(var.resource_group_name, "rg-${var.name_prefix}-lakehouse-${local.region_suffix}")
  storage_account_name  = coalesce(var.storage_account_name, "st${var.name_prefix}lake${local.region_suffix}")
  databricks_name       = "dbw-${var.name_prefix}-lakehouse-${local.region_suffix}"
  access_connector_name = "dbac-${var.name_prefix}-lakehouse-${local.region_suffix}"

  # Exact directory map for the two-stage HNS lakehouse bootstrap; keys are
  # stable so enabling directory creation preserves existing addresses.
  data_lake_directories = {
    landing_orders    = { filesystem = "landing", path = "orders" }
    landing_customers = { filesystem = "landing", path = "customers" }
    landing_products  = { filesystem = "landing", path = "products" }
    lake_bronze       = { filesystem = "lakehouse", path = "bronze" }
    lake_silver       = { filesystem = "lakehouse", path = "silver" }
    lake_quarantine   = { filesystem = "lakehouse", path = "quarantine" }
    lake_gold         = { filesystem = "lakehouse", path = "gold" }
  }
}

data "azurerm_client_config" "current" {}

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
  local_user_enabled              = false
  default_to_oauth_authentication = true
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

resource "azurerm_role_assignment" "operator_storage_blob_data_contributor" {
  for_each             = azurerm_storage_container.lake
  scope                = each.value.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_storage_data_lake_gen2_path" "directories" {
  for_each = var.create_data_lake_directories ? local.data_lake_directories : {}

  path               = each.value.path
  filesystem_name    = azurerm_storage_container.lake[each.value.filesystem].name
  storage_account_id = azurerm_storage_account.lake.id
  resource           = "directory"

  lifecycle {
    precondition {
      # Two-stage bootstrap gate: first apply only the operator role
      # assignments (create_data_lake_directories = false). After verifying
      # Entra-authenticated Storage Blob Data access on both containers, set
      # terraform_operator_blob_access_confirmed = true, then create the
      # directories in a second plan/apply.
      condition     = var.terraform_operator_blob_access_confirmed
      error_message = "Directory creation is a second stage: first apply the Terraform operator role assignments with create_data_lake_directories = false, verify Entra-authenticated Storage Blob Data Contributor access on both landing and lakehouse containers, then set terraform_operator_blob_access_confirmed = true to create the directories."
    }
  }

  depends_on = [azurerm_role_assignment.operator_storage_blob_data_contributor]
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
  name                = local.access_connector_name
  resource_group_name = azurerm_resource_group.platform.name
  location            = azurerm_resource_group.platform.location

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

resource "azurerm_role_assignment" "connector_storage_blob_data_contributor" {
  scope                = azurerm_storage_container.lake["lakehouse"].id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.lakehouse.identity[0].principal_id
}
