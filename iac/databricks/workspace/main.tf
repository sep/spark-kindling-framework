provider "azurerm" {
  features {}
  subscription_id = var.azure_subscription_id
  tenant_id       = var.azure_tenant_id
}

provider "databricks" {
  host = var.databricks_host
  # Auth: uses Azure CLI by default, or set ARM_CLIENT_ID/ARM_CLIENT_SECRET/ARM_TENANT_ID
}
