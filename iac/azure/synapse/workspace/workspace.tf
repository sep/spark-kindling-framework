# =============================================================================
# Synapse Workspace
# =============================================================================

locals {
  managed_resource_group_name = coalesce(var.managed_resource_group_name, "${var.workspace_name}-managed-rg")
  effective_tags = merge(
    {
      environment = var.environment
      managed_by  = "terraform"
    },
    var.tags,
  )
}

# -----------------------------------------------------------------------------
# Default ADLS Gen2 Filesystem
# -----------------------------------------------------------------------------
resource "azurerm_storage_data_lake_gen2_filesystem" "default" {
  count = var.create_datalake_filesystem ? 1 : 0

  name               = var.datalake_filesystem_name
  storage_account_id = var.datalake_storage_account_id
}

# -----------------------------------------------------------------------------
# Synapse Workspace
# -----------------------------------------------------------------------------
resource "azurerm_synapse_workspace" "this" {
  name                                 = var.workspace_name
  resource_group_name                  = var.resource_group_name
  location                             = var.location
  storage_data_lake_gen2_filesystem_id = var.create_datalake_filesystem ? azurerm_storage_data_lake_gen2_filesystem.default[0].id : "${var.datalake_storage_account_id}/blobServices/default/containers/${var.datalake_filesystem_name}"

  sql_administrator_login          = var.sql_administrator_login
  sql_administrator_login_password = var.sql_administrator_login_password

  managed_virtual_network_enabled      = var.enable_managed_vnet
  data_exfiltration_protection_enabled = var.enable_data_exfiltration_protection
  managed_resource_group_name          = local.managed_resource_group_name
  purview_id                           = var.purview_id
  compute_subnet_id                    = var.compute_subnet_id
  public_network_access_enabled        = var.public_network_access_enabled
  linking_allowed_for_aad_tenant_ids   = var.linking_allowed_for_aad_tenant_ids

  identity {
    type         = var.workspace_identity_type
    identity_ids = length(var.user_assigned_identity_ids) > 0 ? var.user_assigned_identity_ids : null
  }

  dynamic "azure_devops_repo" {
    for_each = try(var.git_repo.type, null) == "AzureDevOpsGit" ? [var.git_repo] : []
    content {
      account_name    = azure_devops_repo.value.account_name
      project_name    = azure_devops_repo.value.project_name
      repository_name = azure_devops_repo.value.repository_name
      branch_name     = azure_devops_repo.value.branch_name
      root_folder     = azure_devops_repo.value.root_folder
      tenant_id       = coalesce(azure_devops_repo.value.tenant_id, var.azure_tenant_id)
    }
  }

  dynamic "github_repo" {
    for_each = try(var.git_repo.type, null) == "GitHub" ? [var.git_repo] : []
    content {
      account_name    = github_repo.value.account_name
      repository_name = github_repo.value.repository_name
      branch_name     = github_repo.value.branch_name
      root_folder     = github_repo.value.root_folder
    }
  }

  tags = local.effective_tags
}

# -----------------------------------------------------------------------------
# Workspace AAD Admin
# -----------------------------------------------------------------------------
resource "azurerm_synapse_workspace_aad_admin" "this" {
  count = var.workspace_aad_admin != null ? 1 : 0

  synapse_workspace_id = azurerm_synapse_workspace.this.id
  login                = var.workspace_aad_admin.login
  object_id            = var.workspace_aad_admin.object_id
  tenant_id            = coalesce(var.workspace_aad_admin.tenant_id, var.azure_tenant_id)
}

# -----------------------------------------------------------------------------
# Managed Identity RBAC — Default Storage Account
# -----------------------------------------------------------------------------
resource "azurerm_role_assignment" "workspace_datalake" {
  scope                = var.datalake_storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_synapse_workspace.this.identity[0].principal_id
}

# -----------------------------------------------------------------------------
# Managed Identity RBAC — Additional Storage Accounts
# -----------------------------------------------------------------------------
resource "azurerm_role_assignment" "additional_storage" {
  for_each = { for sa in var.additional_storage_accounts : sa.storage_account_id => sa }

  scope                = each.value.storage_account_id
  role_definition_name = each.value.role
  principal_id         = azurerm_synapse_workspace.this.identity[0].principal_id
}
