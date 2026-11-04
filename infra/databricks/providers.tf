provider "databricks" {
  host      = var.databricks_host
  auth_type = "azure-cli"
}
