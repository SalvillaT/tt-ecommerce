variable "databricks_host" {
  description = "Existing Azure Databricks workspace URL, for example https://adb-<id>.<region>.azuredatabricks.net."
  type        = string
}

variable "unity_catalog_prerequisites_confirmed" {
  description = "Set true only after a UC administrator confirms: an existing UC metastore is ASSIGNED to this workspace in the France Central region; the applying cloud operator (Terraform identity) is both Workspace Admin and Metastore Admin; the dedicated system-assigned Access Connector has Storage Blob Data Contributor on the uc-managed container only. The applying identity and role requirements do not change by adding UC-managed catalog storage."
  type        = bool
  default     = false
}

variable "catalog_managed_storage_confirmed" {
  description = "Set true only after the catalog-level managed-storage decision is approved: the parent uc-managed container external location is approved, and the catalog managed root is the exclusive subpath catalogs/<catalog_name> under it (no shared paths, no metastore DEFAULT/ root usage). Currently managed by catalog storage_root, never by metastore default."
  type        = bool
  default     = false
}

variable "approved_engineer_writer_group_confirmed" {
  description = "Set true only after the named pre-existing account group is approved for the minimal Phase 1 writer grants."
  type        = bool
  default     = false
}

variable "engineer_writer_group_name" {
  description = "Required display name of one approved, pre-existing account group. Do not use 'account users' or wildcard names."
  type        = string

  validation {
    condition = (
      length(trimspace(var.engineer_writer_group_name)) > 0 &&
      lower(trimspace(var.engineer_writer_group_name)) != "account users" &&
      !strcontains(var.engineer_writer_group_name, "*")
    )
    error_message = "engineer_writer_group_name must be a specific non-empty pre-existing group, not 'account users' or a wildcard."
  }
}

variable "catalog_name" {
  description = "Unity Catalog catalog name for synthetic tt-ecommerce data. Its managed storage_root is the exclusive subpath catalogs/<catalog_name> inside the isolated uc-managed container external location."
  type        = string
  default     = "tt_ecommerce_synthetic"

  validation {
    condition     = can(regex("^[A-Za-z][A-Za-z0-9_]{0,127}$", var.catalog_name))
    error_message = "catalog_name must start with a letter and contain only letters, digits, and underscores (maximum 128 characters)."
  }
}

variable "access_connector_id" {
  description = "Access Connector resource ID from the platform root, for the existing lakehouse Azure Managed Identity credential."
  type        = string
}

variable "uc_managed_access_connector_id" {
  description = "Dedicated system-assigned Access Connector resource ID from platform Stage 3 (uc-managed container). Must never reuse the lakehouse connector ID."
  type        = string

  validation {
    condition     = length(trimspace(var.uc_managed_access_connector_id)) > 0
    error_message = "uc_managed_access_connector_id must be a non-empty Access Connector resource ID."
  }
}

variable "storage_account_name" {
  description = "Existing ADLS Gen2 account name from the platform root."
  type        = string
}

variable "external_location_name" {
  description = "Unity Catalog external location name for the lakehouse filesystem."
  type        = string
  default     = "tt_lakehouse"
}

variable "enable_sql_warehouse" {
  description = "Phase 5-only explicit cost opt-in. Creates a PRO 2X-Small SQL Warehouse only when true and phase5_sql_warehouse_approved is true."
  type        = bool
  default     = false
}

variable "phase5_sql_warehouse_approved" {
  description = "Set true only for Phase 5 after Gold is available and exact PRO 2X-Small pricing, permissions, quota, and cost approval are confirmed."
  type        = bool
  default     = false
}

variable "sql_warehouse_auto_stop_mins" {
  description = "Idle auto-stop duration for the opt-in SQL Warehouse."
  type        = number
  default     = 10

  validation {
    condition     = var.sql_warehouse_auto_stop_mins >= 10 && var.sql_warehouse_auto_stop_mins <= 60
    error_message = "sql_warehouse_auto_stop_mins must be between 10 and 60."
  }
}
