provider "azurerm" {
  features {}
  subscription_id = var.azure_subscription_id
  tenant_id       = var.azure_tenant_id
  environment     = var.azure_environment
}

provider "azuread" {
  tenant_id   = var.azure_tenant_id
  environment = var.azure_environment
}
