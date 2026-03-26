terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.80.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = ">= 3.0.0"
    }
  }

  # Uncomment and configure for remote state
  # backend "azurerm" {
  #   resource_group_name  = "your-rg"
  #   storage_account_name = "yourstateaccount"
  #   container_name       = "tfstate"
  #   key                  = "synapse-workspace.tfstate"
  # }
}
