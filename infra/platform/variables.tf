variable "name_prefix" {
  description = "Lowercase alphanumeric prefix, 3-12 characters; use a unique value for globally named storage."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]{3,12}$", var.name_prefix))
    error_message = "name_prefix must contain 3-12 lowercase letters or digits."
  }
}

variable "location" {
  description = "Azure region for the synthetic-demo platform. Derived default names use frc for France Central and a normalized three-character suffix for another location."
  type        = string
  default     = "francecentral"
}

variable "resource_group_name" {
  description = "Optional resource group name for the platform; otherwise derived from name_prefix and location."
  type        = string
  default     = null
}

variable "storage_account_name" {
  description = "Optional globally unique storage account name; otherwise derived from name_prefix and location."
  type        = string
  default     = null
}

variable "tags" {
  description = "Non-sensitive Azure resource tags."
  type        = map(string)
  default = {
    environment = "demo"
    managed_by  = "terraform"
    data_class  = "synthetic-only"
  }
}

variable "create_data_lake_directories" {
  description = "Two-stage bootstrap: when false (initial value), no Gen2 directories are created and only the operator role assignments are applied. Set true only in the second stage. WARNING: must never be set back to false after the directories have been created — flipping it to false removes all directory instances from the plan and would destroy the 7 Gen2 directories and any data under them."
  type        = bool
  default     = false
}

variable "terraform_operator_blob_access_confirmed" {
  description = "Confirmation switch for the second stage: leave false in the first stage; set true only AFTER the first-stage Terraform operator role assignments have been applied and Entra-authenticated Storage Blob Data Contributor access has been verified against BOTH the landing and lakehouse containers. Gates azurerm_storage_data_lake_gen2_path directory creation."
  type        = bool
  default     = false
}

variable "enable_phase2_landing_layout" {
  description = "Phase 2 opt-in: enables the separately approved landing control-directory layout. It is independent of existing Stage 2 flags and defaults to false. Operationally irreversible after apply: do not reset to false; retirement requires a reviewed dedicated code change."
  type        = bool
  default     = false
}

variable "phase2_landing_layout_approved" {
  description = "Phase 2 landing-layout approval. Set true only after the _staging and _control directory design is approved; it has no effect unless enable_phase2_landing_layout is also true. Its plan/apply is a separate human authorization and is not transactional with other Phase 2 changes."
  type        = bool
  default     = false
}

variable "enable_phase2_landing_access_connector" {
  description = "Phase 2 opt-in: enables the separately approved, landing-only Databricks Access Connector. Defaults to false and does not modify existing connectors. Operationally irreversible after apply: do not reset to false; retirement requires a reviewed dedicated code change."
  type        = bool
  default     = false
}

variable "phase2_landing_access_connector_approved" {
  description = "Phase 2 landing Access Connector approval. Set true only after its read-only landing scope is approved; it has no effect unless enable_phase2_landing_access_connector is also true. Its plan/apply is a separate human authorization and is not transactional with other Phase 2 changes."
  type        = bool
  default     = false
}
