terraform {
  required_version = ">= 1.9.0, < 2.0.0"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.80.0"
    }
  }
}
