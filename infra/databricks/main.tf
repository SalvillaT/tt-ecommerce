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

resource "terraform_data" "phase2_ingestion_foundation_gate" {
  count = var.enable_phase2_ingestion_foundation ? 1 : 0

  input = {
    approved                    = var.phase2_ingestion_foundation_approved
    landing_access_connector_id = var.landing_access_connector_id
    job_run_as_application_id   = var.phase2_job_run_as_application_id
  }

  lifecycle {
    precondition {
      condition     = var.phase2_ingestion_foundation_approved
      error_message = "Phase 2 ingestion foundation is enabled but not approved. Set phase2_ingestion_foundation_approved = true only after approving the dedicated landing connector, four read-only source paths, and managed pipeline state design."
    }

    precondition {
      condition     = var.landing_access_connector_id != null && length(trimspace(var.landing_access_connector_id)) > 0
      error_message = "Phase 2 ingestion foundation requires a non-empty landing_access_connector_id from the separately approved platform landing Access Connector."
    }

    precondition {
      condition     = var.phase2_job_run_as_application_id != null && length(trimspace(var.phase2_job_run_as_application_id)) > 0
      error_message = "Phase 2 ingestion foundation requires a non-empty phase2_job_run_as_application_id for an existing Databricks service principal."
    }
  }
}

# The service principal is an existing identity. The lookup validates that it
# exists in Databricks; Terraform does not create or manage it or its Entra app.
data "databricks_service_principal" "phase2_job_run_as" {
  count = var.enable_phase2_ingestion_foundation && var.phase2_ingestion_foundation_approved ? 1 : 0

  application_id = var.phase2_job_run_as_application_id
}

resource "databricks_storage_credential" "phase2_landing" {
  count = var.enable_phase2_ingestion_foundation && var.phase2_ingestion_foundation_approved ? 1 : 0

  name    = "tt-phase2-landing-credential"
  comment = "Phase 2 managed identity credential for read-only access to approved landing source paths."

  azure_managed_identity {
    access_connector_id = var.landing_access_connector_id
  }

  lifecycle {
    # Retirement requires a reviewed, dedicated code change; never reset the
    # opt-in flag after an authorized apply.
    prevent_destroy = true
  }

  depends_on = [
    terraform_data.phase2_ingestion_foundation_gate,
    data.databricks_service_principal.phase2_job_run_as,
  ]
}

locals {
  # No location is defined for landing/_staging. Local users/operators upload
  # there through their existing Azure permissions; the job gets neither UC
  # access nor direct Azure RBAC to staging.
  phase2_landing_external_locations = {
    orders = {
      name = "tt_phase2_landing_orders"
      path = "orders"
    }
    customers = {
      name = "tt_phase2_landing_customers"
      path = "customers"
    }
    products = {
      name = "tt_phase2_landing_products"
      path = "products"
    }
    control = {
      name = "tt_phase2_landing_control"
      path = "_control"
    }
  }
}

resource "databricks_external_location" "phase2_landing" {
  for_each = var.enable_phase2_ingestion_foundation && var.phase2_ingestion_foundation_approved ? local.phase2_landing_external_locations : {}

  name            = each.value.name
  url             = "abfss://landing@${var.storage_account_name}.dfs.core.windows.net/${each.value.path}"
  credential_name = databricks_storage_credential.phase2_landing[0].id
  comment         = "Phase 2 read-only source location for landing/${each.value.path}; excludes landing/_staging."
  read_only       = true

  # Do not set skip_validation: the authorized apply must verify Azure RBAC.
  lifecycle {
    # Retirement requires a reviewed, dedicated code change; never reset the
    # opt-in flag after an authorized apply.
    prevent_destroy = true
  }

  depends_on = [terraform_data.phase2_ingestion_foundation_gate]
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

resource "databricks_schema" "phase2_pipeline_state" {
  count = var.enable_phase2_ingestion_foundation && var.phase2_ingestion_foundation_approved ? 1 : 0

  catalog_name = databricks_catalog.synthetic.name
  name         = "pipeline_state"
  comment      = "Managed Phase 2 pipeline state schema for checkpoints and operational state."

  lifecycle {
    # Retirement requires a reviewed, dedicated code change; never reset the
    # opt-in flag after an authorized apply.
    prevent_destroy = true
  }

  depends_on = [
    databricks_catalog.synthetic,
    terraform_data.phase2_ingestion_foundation_gate,
  ]
}

resource "databricks_volume" "phase2_ingestion_state" {
  count = var.enable_phase2_ingestion_foundation && var.phase2_ingestion_foundation_approved ? 1 : 0

  name         = "ingestion_state"
  catalog_name = databricks_catalog.synthetic.name
  schema_name  = databricks_schema.phase2_pipeline_state[0].name
  volume_type  = "MANAGED"
  comment      = "Managed Phase 2 ingestion checkpoint and state volume."

  lifecycle {
    # Retirement requires a reviewed, dedicated code change; never reset the
    # opt-in flag after an authorized apply.
    prevent_destroy = true
  }

  depends_on = [terraform_data.phase2_ingestion_foundation_gate]
}

resource "databricks_grant" "engineer_writer_catalog" {
  catalog = databricks_catalog.synthetic.name

  principal  = var.engineer_writer_group_name
  privileges = ["USE_CATALOG"]

  depends_on = [terraform_data.unity_catalog_gate]
}

# Phase 2 write-ownership split: the Phase 2 run-as service principal owns all
# Bronze writes (CREATE_TABLE granted by databricks_grant.phase2_job_bronze_schema),
# so engineer_writer keeps CREATE_TABLE on silver/quarantine/gold only and never
# on bronze. USE_SCHEMA is unchanged on every Phase 1 schema.
locals {
  engineer_writer_schema_privileges = {
    bronze     = ["USE_SCHEMA"]
    silver     = ["USE_SCHEMA", "CREATE_TABLE"]
    quarantine = ["USE_SCHEMA", "CREATE_TABLE"]
    gold       = ["USE_SCHEMA", "CREATE_TABLE"]
  }
}

resource "databricks_grant" "engineer_writer_schema" {
  for_each = local.engineer_writer_schema_privileges

  schema = databricks_schema.phase1[each.key].id

  principal  = var.engineer_writer_group_name
  privileges = each.value

  depends_on = [terraform_data.unity_catalog_gate]
}

resource "databricks_grant" "phase2_job_catalog" {
  count = var.enable_phase2_ingestion_foundation && var.phase2_ingestion_foundation_approved ? 1 : 0

  catalog = databricks_catalog.synthetic.name

  principal  = var.phase2_job_run_as_application_id
  privileges = ["USE_CATALOG"]

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [
    terraform_data.phase2_ingestion_foundation_gate,
    data.databricks_service_principal.phase2_job_run_as,
  ]
}

resource "databricks_grant" "phase2_job_bronze_schema" {
  count = var.enable_phase2_ingestion_foundation && var.phase2_ingestion_foundation_approved ? 1 : 0

  schema = databricks_schema.phase1["bronze"].id

  principal  = var.phase2_job_run_as_application_id
  privileges = ["USE_SCHEMA", "CREATE_TABLE"]

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [
    terraform_data.phase2_ingestion_foundation_gate,
    data.databricks_service_principal.phase2_job_run_as,
  ]
}

resource "databricks_grant" "phase2_job_landing_read" {
  for_each = var.enable_phase2_ingestion_foundation && var.phase2_ingestion_foundation_approved ? databricks_external_location.phase2_landing : {}

  external_location = each.value.id

  principal  = var.phase2_job_run_as_application_id
  privileges = ["READ_FILES"]

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [
    terraform_data.phase2_ingestion_foundation_gate,
    data.databricks_service_principal.phase2_job_run_as,
  ]
}

resource "databricks_grant" "phase2_job_pipeline_state_schema" {
  count = var.enable_phase2_ingestion_foundation && var.phase2_ingestion_foundation_approved ? 1 : 0

  schema = databricks_schema.phase2_pipeline_state[0].id

  principal  = var.phase2_job_run_as_application_id
  privileges = ["USE_SCHEMA"]

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [
    terraform_data.phase2_ingestion_foundation_gate,
    data.databricks_service_principal.phase2_job_run_as,
  ]
}

resource "databricks_grant" "phase2_job_ingestion_state_volume" {
  count = var.enable_phase2_ingestion_foundation && var.phase2_ingestion_foundation_approved ? 1 : 0

  volume = databricks_volume.phase2_ingestion_state[0].id

  principal  = var.phase2_job_run_as_application_id
  privileges = ["READ_VOLUME", "WRITE_VOLUME"]

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [
    terraform_data.phase2_ingestion_foundation_gate,
    data.databricks_service_principal.phase2_job_run_as,
  ]
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
