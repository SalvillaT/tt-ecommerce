variable "resource_group_name" {
  description = "Name of the resource group created for future remote state."
  type        = string
}

variable "location" {
  description = "Azure region for the optional state storage account."
  type        = string
  default     = "westeurope"
}

variable "storage_account_name" {
  description = "Globally unique 3-24 character lowercase alphanumeric storage-account name."
  type        = string
}

variable "container_name" {
  description = "Private blob container for Terraform state."
  type        = string
  default     = "tfstate"
}

variable "tags" {
  description = "Non-sensitive Azure resource tags."
  type        = map(string)
  default = {
    managed_by = "terraform"
    purpose    = "terraform-state"
  }
}
