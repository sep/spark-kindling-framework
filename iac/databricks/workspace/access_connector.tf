# =============================================================================
# Access Connector: Managed Identity + Storage RBAC
# =============================================================================

locals {
  access_connector_mode_enabled = var.enable_unity_catalog && var.storage_credential_auth_type == "access_connector"
  access_connector_id_effective = local.access_connector_mode_enabled ? (var.create_access_connector ? azurerm_databricks_access_connector.this[0].id : var.access_connector_id) : null
  datalake_storage_scope_effective = (
    var.datalake_storage_account_id != null
    ? var.datalake_storage_account_id
    : ((local.access_connector_mode_enabled && var.create_access_connector) ? data.azurerm_storage_account.datalake[0].id : null)
  )
}

resource "azurerm_databricks_access_connector" "this" {
  count = local.access_connector_mode_enabled && var.create_access_connector ? 1 : 0

  name                = var.access_connector_name
  resource_group_name = var.access_connector_resource_group_name
  location            = var.access_connector_location

  identity {
    type = "SystemAssigned"
  }
}

data "azurerm_storage_account" "datalake" {
  count = local.access_connector_mode_enabled && var.create_access_connector && var.datalake_storage_account_id == null ? 1 : 0

  name                = var.datalake_storage_account
  resource_group_name = var.datalake_storage_account_resource_group_name
}

resource "azurerm_role_assignment" "access_connector_storage_blob_contributor" {
  count = local.access_connector_mode_enabled && var.create_access_connector ? 1 : 0

  scope                = local.datalake_storage_scope_effective
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this[0].identity[0].principal_id
}
