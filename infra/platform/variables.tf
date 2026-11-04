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
