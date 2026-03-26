# =============================================================================
# Security: Firewall Rules, Private Endpoints, Role Assignments, Linked Services
# =============================================================================

# -----------------------------------------------------------------------------
# Firewall Rules
# -----------------------------------------------------------------------------
resource "azurerm_synapse_firewall_rule" "allow_azure" {
  count = var.allow_azure_services ? 1 : 0

  name                 = "AllowAllWindowsAzureIps"
  synapse_workspace_id = azurerm_synapse_workspace.this.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "0.0.0.0"
}

resource "azurerm_synapse_firewall_rule" "rules" {
  for_each = { for r in var.firewall_rules : r.name => r }

  name                 = each.value.name
  synapse_workspace_id = azurerm_synapse_workspace.this.id
  start_ip_address     = each.value.start_ip_address
  end_ip_address       = each.value.end_ip_address
}

# -----------------------------------------------------------------------------
# Managed Private Endpoints
# -----------------------------------------------------------------------------
resource "azurerm_synapse_managed_private_endpoint" "endpoints" {
  for_each = var.enable_managed_private_endpoints ? { for ep in var.managed_private_endpoints : ep.name => ep } : {}

  name                 = each.value.name
  synapse_workspace_id = azurerm_synapse_workspace.this.id
  target_resource_id   = each.value.target_resource_id
  subresource_name     = each.value.subresource_name

  depends_on = [azurerm_synapse_firewall_rule.allow_azure]
}

# -----------------------------------------------------------------------------
# User Role Assignments — Administrators
# -----------------------------------------------------------------------------
resource "azurerm_synapse_role_assignment" "admin_users" {
  for_each = { for u in var.admin_users : u.object_id => u }

  synapse_workspace_id = azurerm_synapse_workspace.this.id
  role_name            = "Synapse Administrator"
  principal_id         = each.value.object_id

  depends_on = [azurerm_synapse_firewall_rule.allow_azure]
}

# -----------------------------------------------------------------------------
# User Role Assignments — Contributors
# -----------------------------------------------------------------------------
resource "azurerm_synapse_role_assignment" "contributor_users" {
  for_each = { for u in var.workspace_contributors : u.object_id => u }

  synapse_workspace_id = azurerm_synapse_workspace.this.id
  role_name            = "Synapse Contributor"
  principal_id         = each.value.object_id

  depends_on = [azurerm_synapse_firewall_rule.allow_azure]
}

# -----------------------------------------------------------------------------
# User Role Assignments — Users
# -----------------------------------------------------------------------------
resource "azurerm_synapse_role_assignment" "workspace_users" {
  for_each = { for u in var.workspace_users : u.object_id => u }

  synapse_workspace_id = azurerm_synapse_workspace.this.id
  role_name            = "Synapse User"
  principal_id         = each.value.object_id

  depends_on = [azurerm_synapse_firewall_rule.allow_azure]
}

# -----------------------------------------------------------------------------
# Synapse RBAC Role Assignments (explicit)
# -----------------------------------------------------------------------------
resource "azurerm_synapse_role_assignment" "roles" {
  for_each = { for ra in var.synapse_role_assignments : "${ra.role_name}__${ra.principal_id}" => ra }

  synapse_workspace_id = azurerm_synapse_workspace.this.id
  role_name            = each.value.role_name
  principal_id         = each.value.principal_id

  depends_on = [azurerm_synapse_firewall_rule.allow_azure]
}

# -----------------------------------------------------------------------------
# Azure RBAC Role Assignments on the Workspace
# -----------------------------------------------------------------------------
resource "azurerm_role_assignment" "workspace" {
  for_each = { for ra in var.azure_role_assignments : "${ra.role_definition_name}__${ra.principal_id}" => ra }

  scope                = azurerm_synapse_workspace.this.id
  role_definition_name = each.value.role_definition_name
  principal_id         = each.value.principal_id
}

# -----------------------------------------------------------------------------
# Linked Services — ADLS Gen2 (Delta Storage)
# -----------------------------------------------------------------------------
resource "azurerm_synapse_linked_service" "adls" {
  for_each = { for ls in var.linked_storage_accounts : ls.name => ls }

  name                 = each.value.name
  synapse_workspace_id = azurerm_synapse_workspace.this.id
  type                 = "AzureBlobFS"

  type_properties_json = jsonencode({
    url = each.value.storage_account_url
  })

  depends_on = [azurerm_synapse_firewall_rule.allow_azure]
}

resource "azurerm_role_assignment" "linked_storage" {
  # Skip if the storage account is the same as the workspace default (already granted by workspace_datalake)
  for_each = {
    for ls in var.linked_storage_accounts : ls.name => ls
    if ls.storage_account_id != var.datalake_storage_account_id
  }

  scope                = each.value.storage_account_id
  role_definition_name = each.value.role
  principal_id         = azurerm_synapse_workspace.this.identity[0].principal_id
}

# -----------------------------------------------------------------------------
# Linked Services — Key Vault
# -----------------------------------------------------------------------------
resource "azurerm_synapse_linked_service" "keyvault" {
  for_each = { for ls in var.linked_services_keyvault : ls.name => ls }

  name                 = each.value.name
  synapse_workspace_id = azurerm_synapse_workspace.this.id
  type                 = "AzureKeyVault"

  type_properties_json = jsonencode({
    baseUrl = "https://${regex("[^/]+$", each.value.keyvault_id)}.vault.azure.net/"
  })

  depends_on = [azurerm_synapse_firewall_rule.allow_azure]
}
