resource "terraform_data" "unity_catalog_gate" {
  input = {
    metastore_and_identity_confirmed  = var.unity_catalog_prerequisites_confirmed
    catalog_managed_storage_confirmed = var.catalog_managed_storage_confirmed
    writer_group_confirmed            = var.approved_engineer_writer_group_confirmed
  }

  lifecycle {
    precondition {
      condition     = var.unity_catalog_prerequisites_confirmed
      error_message = "Confirm UC prerequisites: an existing metastore is assigned to this workspace in the France Central region; the cloud operator applying Terraform holds both Workspace Admin and Metastore Admin roles; the dedicated Stage 3 Access Connector holds Storage Blob Data Contributor on the uc-managed container. Existing metastore assignment and identity permissions stay unchanged."
    }

    precondition {
      condition     = var.catalog_managed_storage_confirmed
      error_message = "Confirm the catalog-level managed-storage model: the parent uc-managed container external location is approved and the catalog managed root is the exclusive subpath catalogs/<catalog_name> under it. Do not use the metastore DEFAULT managed-storage location or the lakehouse external location as catalog managed storage."
    }

    precondition {
      condition     = var.approved_engineer_writer_group_confirmed
      error_message = "Confirm that engineer_writer_group_name is an approved, existing account group before granting privileges."
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

resource "databricks_catalog" "synthetic" {
  name    = var.catalog_name
  comment = "Terraform-managed catalog for the synthetic tt-ecommerce project."
  # Catalog-level managed storage: exclusive subpath inside the isolated
  # uc-managed container; never the metastore root or lakehouse location.
  storage_root = local.catalog_managed_storage_root

  depends_on = [terraform_data.unity_catalog_gate, databricks_external_location.uc_managed]
}

resource "databricks_storage_credential" "uc_managed" {
  name    = "tt-uc-managed-credential"
  comment = "Dedicated Access Connector credential for Unity Catalog catalog-level managed storage in the isolated uc-managed container."

  azure_managed_identity {
    access_connector_id = var.uc_managed_access_connector_id
  }

  depends_on = [terraform_data.unity_catalog_gate]
}

resource "databricks_external_location" "uc_managed" {
  name            = "tt_uc_managed"
  url             = local.uc_managed_container_url
  credential_name = databricks_storage_credential.uc_managed.id
  comment         = "Isolated uc-managed container backing the synthetic catalog managed storage root."

  depends_on = [terraform_data.unity_catalog_gate]
}

locals {
  # UC-managed foundation against the Stage 3 dedicated Access Connector:
  # credential + external location cover ONLY the isolated uc-managed
  # container; the catalog managed root is an exclusive subpath beneath it.
  uc_managed_container_url     = "abfss://uc-managed@${var.storage_account_name}.dfs.core.windows.net/"
  catalog_managed_storage_root = "${local.uc_managed_container_url}catalogs/${var.catalog_name}"

  phase1_schema_comments = {
    bronze     = "Raw synthetic data layer."
    silver     = "Refined synthetic data layer."
    quarantine = "Synthetic records isolated for quality review."
    gold       = "Synthetic curated reporting layer."
  }
}

resource "databricks_schema" "phase1" {
  for_each = local.phase1_schema_comments

  catalog_name = databricks_catalog.synthetic.name
  name         = each.key
  comment      = each.value

  # No storage_root: inherit the catalog/metastore managed-storage decision.
  depends_on = [databricks_catalog.synthetic]
}

resource "databricks_grant" "engineer_writer_catalog" {
  catalog = databricks_catalog.synthetic.name

  principal  = var.engineer_writer_group_name
  privileges = ["USE_CATALOG"]

  depends_on = [terraform_data.unity_catalog_gate]
}

resource "databricks_grant" "engineer_writer_schema" {
  for_each = databricks_schema.phase1

  schema = each.value.id

  principal  = var.engineer_writer_group_name
  privileges = ["USE_SCHEMA", "CREATE_TABLE"]

  depends_on = [terraform_data.unity_catalog_gate]
}

resource "terraform_data" "sql_warehouse_phase5_gate" {
  count = var.enable_sql_warehouse ? 1 : 0

  input = var.phase5_sql_warehouse_approved

  lifecycle {
    precondition {
      condition     = var.phase5_sql_warehouse_approved
      error_message = "SQL Warehouse is Phase 5 only: confirm Gold availability, exact PRO 2X-Small pricing, permissions, quota, and cost approval before enabling it."
    }
  }
}

resource "databricks_sql_endpoint" "demo" {
  count = var.enable_sql_warehouse ? 1 : 0

  name             = "tt-demo-sql-warehouse"
  cluster_size     = "2X-Small"
  max_num_clusters = 1
  auto_stop_mins   = var.sql_warehouse_auto_stop_mins
  enable_photon    = true
  warehouse_type   = "PRO"

  depends_on = [terraform_data.unity_catalog_gate, terraform_data.sql_warehouse_phase5_gate]
}
