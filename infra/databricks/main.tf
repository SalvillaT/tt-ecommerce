resource "terraform_data" "unity_catalog_gate" {
  input = {
    metastore_and_identity_confirmed = var.unity_catalog_prerequisites_confirmed
    managed_storage_confirmed        = var.metastore_default_managed_storage_confirmed
    writer_group_confirmed           = var.approved_engineer_writer_group_confirmed
  }

  lifecycle {
    precondition {
      condition     = var.unity_catalog_prerequisites_confirmed
      error_message = "Confirm an existing UC metastore assignment and applying identity permissions before this root can create workspace-level UC objects."
    }

    precondition {
      condition     = var.metastore_default_managed_storage_confirmed
      error_message = "Confirm that the assigned metastore has an approved DEFAULT managed-storage location before creating this catalog without storage_root. Do not use the lakehouse external location as managed storage."
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

  # No storage_root: managed tables use the approved DEFAULT location of the metastore.
  depends_on = [terraform_data.unity_catalog_gate]
}

locals {
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
