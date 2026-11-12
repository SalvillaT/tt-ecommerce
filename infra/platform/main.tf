locals {
  location_slug = lower(replace(replace(var.location, " ", ""), "-", ""))
  region_suffix = lookup({ francecentral = "frc" }, local.location_slug, substr(local.location_slug, 0, 3))

  resource_group_name   = coalesce(var.resource_group_name, "rg-${var.name_prefix}-lakehouse-${local.region_suffix}")
  storage_account_name  = coalesce(var.storage_account_name, "st${var.name_prefix}lake${local.region_suffix}")
  databricks_name       = "dbw-${var.name_prefix}-lakehouse-${local.region_suffix}"
  access_connector_name = "dbac-${var.name_prefix}-lakehouse-${local.region_suffix}"

  # Stage 3: dedicated Unity Catalog managed-storage foundation on the same
  # HNS account; naming follows the existing prefix/region style (suffix -ucm-
  # distinguishes it; max length dbac- + 12 + -ucm- + 3 stays under 30 chars).
  uc_managed_container_name = "uc-managed"
  uc_access_connector_name  = "dbac-${var.name_prefix}-ucm-${local.region_suffix}"

  # Phase 2 uses its own connector and must never broaden the existing
  # lakehouse or UC-managed connector scopes.
  phase2_landing_access_connector_name = "dbac-${var.name_prefix}-landing-${local.region_suffix}"

  # Deliberately separate from data_lake_directories so Stage 2 addresses and
  # controls are immutable. _control is an explicit HNS parent before its
  # children are created.
  phase2_landing_directory_roots = {
    landing_staging = { filesystem = "landing", path = "_staging" }
    landing_control = { filesystem = "landing", path = "_control" }
  }

  phase2_landing_directory_children = {
    landing_control_manifests = { filesystem = "landing", path = "_control/manifests" }
    landing_control_ready     = { filesystem = "landing", path = "_control/ready" }
  }

  # Least privilege: the Terraform operator is granted Blob Data Contributor
  # ONLY on landing/lakehouse. This explicit set (rather than the container
  # collection) must never expand to uc-managed or the storage account scope;
  # it also keeps operator state addresses ["landing"]/["lakehouse"] stable.
  operator_blob_container_names = toset(["landing", "lakehouse"])

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
  for_each             = local.operator_blob_container_names
  scope                = azurerm_storage_container.lake[each.value].id
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

resource "terraform_data" "phase2_landing_layout_gate" {
  count = var.enable_phase2_landing_layout ? 1 : 0

  input = var.phase2_landing_layout_approved

  lifecycle {
    precondition {
      condition     = var.phase2_landing_layout_approved
      error_message = "Phase 2 landing layout is enabled but not approved. Set phase2_landing_layout_approved = true only after approving landing/_staging, landing/_control/manifests, and landing/_control/ready. Existing Stage 2 flags and paths are independent."
    }
  }
}

resource "azurerm_storage_data_lake_gen2_path" "phase2_landing_directory_roots" {
  for_each = var.enable_phase2_landing_layout && var.phase2_landing_layout_approved ? local.phase2_landing_directory_roots : {}

  path               = each.value.path
  filesystem_name    = azurerm_storage_container.lake[each.value.filesystem].name
  storage_account_id = azurerm_storage_account.lake.id
  resource           = "directory"

  lifecycle {
    # Retirement requires a reviewed, dedicated code change; never reset the
    # opt-in flag after an authorized apply.
    prevent_destroy = true
  }

  depends_on = [
    azurerm_role_assignment.operator_storage_blob_data_contributor,
    terraform_data.phase2_landing_layout_gate,
  ]
}

resource "azurerm_storage_data_lake_gen2_path" "phase2_landing_directory_children" {
  for_each = var.enable_phase2_landing_layout && var.phase2_landing_layout_approved ? local.phase2_landing_directory_children : {}

  path               = each.value.path
  filesystem_name    = azurerm_storage_container.lake[each.value.filesystem].name
  storage_account_id = azurerm_storage_account.lake.id
  resource           = "directory"

  lifecycle {
    # Retirement requires a reviewed, dedicated code change; never reset the
    # opt-in flag after an authorized apply.
    prevent_destroy = true
  }

  # The HNS parent landing/_control is explicit rather than inferred.
  depends_on = [
    azurerm_storage_data_lake_gen2_path.phase2_landing_directory_roots,
  ]
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

resource "terraform_data" "phase2_landing_access_connector_gate" {
  count = var.enable_phase2_landing_access_connector ? 1 : 0

  input = var.phase2_landing_access_connector_approved

  lifecycle {
    precondition {
      condition     = var.phase2_landing_access_connector_approved
      error_message = "Phase 2 landing Access Connector is enabled but not approved. Set phase2_landing_access_connector_approved = true only after approving its two scopes: Storage Blob Data Reader on the landing container and Storage Blob Delegator on the landing storage account."
    }
  }
}

resource "azurerm_databricks_access_connector" "phase2_landing" {
  count = var.enable_phase2_landing_access_connector && var.phase2_landing_access_connector_approved ? 1 : 0

  name                = local.phase2_landing_access_connector_name
  resource_group_name = azurerm_resource_group.platform.name
  location            = azurerm_resource_group.platform.location

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags

  lifecycle {
    # Retirement requires a reviewed, dedicated code change; never reset the
    # opt-in flag after an authorized apply.
    prevent_destroy = true
  }

  depends_on = [terraform_data.phase2_landing_access_connector_gate]
}

resource "azurerm_role_assignment" "phase2_landing_connector_storage_blob_data_reader" {
  count = var.enable_phase2_landing_access_connector && var.phase2_landing_access_connector_approved ? 1 : 0

  scope                = azurerm_storage_container.lake["landing"].id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_databricks_access_connector.phase2_landing[0].identity[0].principal_id

  lifecycle {
    # Retirement requires a reviewed, dedicated code change; never reset the
    # opt-in flag after an authorized apply.
    prevent_destroy = true
  }
}

# The Unity Catalog external-location landing preflight requests a user
# delegation key, which the container Reader role alone does not authorize.
# Storage Blob Delegator on the storage account grants exactly that request
# (and SAS user-delegation signing); it stays distinct from the Reader scope
# and never replaces it with a broader container or account data role.
resource "azurerm_role_assignment" "phase2_landing_connector_storage_blob_delegator" {
  count = var.enable_phase2_landing_access_connector && var.phase2_landing_access_connector_approved ? 1 : 0

  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Delegator"
  principal_id         = azurerm_databricks_access_connector.phase2_landing[0].identity[0].principal_id

  lifecycle {
    # Retirement requires a reviewed, dedicated code change; never reset the
    # opt-in flag after an authorized apply.
    prevent_destroy = true
  }
}

resource "azurerm_storage_container" "uc_managed" {
  name                  = local.uc_managed_container_name
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

resource "azurerm_databricks_access_connector" "uc_managed" {
  name                = local.uc_access_connector_name
  resource_group_name = azurerm_resource_group.platform.name
  location            = azurerm_resource_group.platform.location

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

resource "azurerm_role_assignment" "uc_managed_connector_storage_blob_data_contributor" {
  scope                = azurerm_storage_container.uc_managed.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc_managed.identity[0].principal_id
}

# Unity Catalog managed storage (Bronze CREATE TABLE) requires the UC
# connector's identity to request user delegation keys, which no container
# data role authorizes. Storage Blob Delegator on the storage account grants
# that request while keeping the data-plane Contributor at the uc-managed
# container scope.
resource "azurerm_role_assignment" "uc_managed_connector_storage_blob_delegator" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Delegator"
  principal_id         = azurerm_databricks_access_connector.uc_managed.identity[0].principal_id
}
