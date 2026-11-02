variable "name_prefix" {
  description = "Lowercase alphanumeric prefix, 3-12 characters; use a unique value for globally named storage."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]{3,12}$", var.name_prefix))
    error_message = "name_prefix must contain 3-12 lowercase letters or digits."
  }
}

variable "location" {
  description = "Azure region for the synthetic-demo platform."
  type        = string
  default     = "westeurope"
}

variable "resource_group_name" {
  description = "Resource group name for the platform."
  type        = string
  default     = null
}

variable "storage_account_name" {
  description = "Optional globally unique storage account name; otherwise derived from name_prefix."
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
