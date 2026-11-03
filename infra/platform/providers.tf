# The required azurerm subscription ID MUST be supplied locally via the
# ARM_SUBSCRIPTION_ID environment variable; never versioned or hardcoded here.
provider "azurerm" {
  features {}

  storage_use_azuread = true

  resource_provider_registrations = "none"
}
