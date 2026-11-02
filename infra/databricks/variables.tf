variable "databricks_host" {
  description = "Existing Azure Databricks workspace URL, for example https://adb-<id>.<region>.azuredatabricks.net."
  type        = string
}

variable "unity_catalog_prerequisites_confirmed" {
  description = "Set true only after a UC administrator confirms a metastore is assigned and applying identity permissions are sufficient."
  type        = bool
  default     = false
}

variable "access_connector_id" {
  description = "Azure Databricks Access Connector resource ID from the platform root."
  type        = string
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
  description = "Explicit cost opt-in. Creates a small SQL Warehouse only when true."
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
