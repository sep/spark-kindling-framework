terraform {
  required_version = ">= 1.5.0"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.50.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.80.0"
    }
  }

  # Uncomment and configure for remote state
  # backend "azurerm" {
  #   resource_group_name  = "your-rg"
  #   storage_account_name = "yourstateaccount"
  #   container_name       = "tfstate"
  #   key                  = "databricks-workspace.tfstate"
  # }
}
