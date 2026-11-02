resource "terraform_data" "unity_catalog_gate" {
  input = var.unity_catalog_prerequisites_confirmed

  lifecycle {
    precondition {
      condition     = var.unity_catalog_prerequisites_confirmed
      error_message = "Confirm an existing UC metastore assignment and applying identity permissions before this root can create workspace-level UC objects."
    }
  }
}

resource "databricks_storage_credential" "lakehouse" {
  name    = "tt-lakehouse-credential"
  comment = "Managed identity credential for the synthetic-demo lakehouse."

  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }

  depends_on = [terraform_data.unity_catalog_gate]
}

resource "databricks_external_location" "lakehouse" {
  name            = var.external_location_name
  url             = "abfss://lakehouse@${var.storage_account_name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.lakehouse.id
  comment         = "Synthetic-demo lakehouse external location."

  depends_on = [terraform_data.unity_catalog_gate]
}

resource "databricks_sql_endpoint" "demo" {
  count = var.enable_sql_warehouse ? 1 : 0

  name             = "tt-demo-sql-warehouse"
  cluster_size     = "2X-Small"
  max_num_clusters = 1
  auto_stop_mins   = var.sql_warehouse_auto_stop_mins
  enable_photon    = true
  warehouse_type   = "PRO"

  depends_on = [terraform_data.unity_catalog_gate]
}
