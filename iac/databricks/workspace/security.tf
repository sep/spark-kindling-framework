# =============================================================================
# Security: Secret Scopes, Service Principals, Users, Group Memberships
# =============================================================================

# -----------------------------------------------------------------------------
# Secret Scopes
# -----------------------------------------------------------------------------
resource "databricks_secret_scope" "scopes" {
  for_each = toset(var.secret_scopes)

  name = each.value
}

# -----------------------------------------------------------------------------
# Service Principals
# -----------------------------------------------------------------------------
resource "databricks_service_principal" "sp" {
  for_each = local.service_principals_effective_by_key

  application_id = each.value.application_id
  display_name   = each.value.display_name
  active         = true

  # Entitlements managed by databricks_entitlements resources
  lifecycle {
    ignore_changes = [
      allow_cluster_create,
      allow_instance_pool_create,
      workspace_access,
      databricks_sql_access,
    ]
  }
}

# -----------------------------------------------------------------------------
# Users (ensure they exist in workspace)
# -----------------------------------------------------------------------------
locals {
  all_users = toset(concat(var.admin_users, var.workspace_users))
  sp_entitlements_non_runtime = [
    for spe in var.sp_entitlements : spe
    if spe.application_id != var.runtime_sp_principal_alias
  ]
  sp_entitlements_runtime = [
    for spe in var.sp_entitlements : spe
    if spe.application_id == var.runtime_sp_principal_alias
  ]
}

data "databricks_group" "admins" {
  count = var.manage_system_group_memberships && length(var.admin_users) > 0 ? 1 : 0

  display_name = "admins"
}

data "databricks_group" "users" {
  count = var.manage_system_group_memberships && (length(var.workspace_users) > 0 || length(local.service_principals_effective_by_key) > 0) ? 1 : 0

  display_name = "users"
}

resource "databricks_user" "users" {
  for_each = local.all_users

  user_name = each.value
  force     = true # Don't fail if user already exists

  # Entitlements managed by databricks_entitlements resources
  lifecycle {
    ignore_changes = [
      allow_cluster_create,
      allow_instance_pool_create,
      workspace_access,
      databricks_sql_access,
    ]
  }
}

# -----------------------------------------------------------------------------
# Admin Group Membership
# -----------------------------------------------------------------------------
resource "databricks_group_member" "admins" {
  for_each = var.manage_system_group_memberships ? toset(var.admin_users) : []

  group_id  = data.databricks_group.admins[0].id
  member_id = databricks_user.users[each.value].id
}

# -----------------------------------------------------------------------------
# Users Group Membership - humans
# -----------------------------------------------------------------------------
resource "databricks_group_member" "workspace_users" {
  for_each = var.manage_system_group_memberships ? toset(var.workspace_users) : []

  group_id  = data.databricks_group.users[0].id
  member_id = databricks_user.users[each.value].id
}

# -----------------------------------------------------------------------------
# Users Group Membership - service principals
# -----------------------------------------------------------------------------
resource "databricks_group_member" "sp_users" {
  for_each = var.manage_system_group_memberships ? local.service_principals_effective_by_key : {}

  group_id  = data.databricks_group.users[0].id
  member_id = databricks_service_principal.sp[each.key].id
}

# -----------------------------------------------------------------------------
# Secret Scope ACLs
# -----------------------------------------------------------------------------
resource "databricks_secret_acl" "acls" {
  for_each = { for acl in var.secret_scope_acls : "${acl.scope}__${acl.principal}" => acl }

  scope      = each.value.scope
  principal  = each.value.principal
  permission = each.value.permission

  depends_on = [databricks_secret_scope.scopes]
}

# -----------------------------------------------------------------------------
# User Entitlements
# -----------------------------------------------------------------------------
resource "databricks_entitlements" "users" {
  for_each = { for ue in var.user_entitlements : ue.user_name => ue }

  user_id                    = databricks_user.users[each.key].id
  allow_cluster_create       = each.value.allow_cluster_create
  allow_instance_pool_create = each.value.allow_instance_pool_create
  workspace_access           = each.value.workspace_access
  databricks_sql_access      = each.value.databricks_sql_access
}

# -----------------------------------------------------------------------------
# Service Principal Entitlements
# -----------------------------------------------------------------------------
resource "databricks_entitlements" "service_principals" {
  for_each = { for spe in local.sp_entitlements_non_runtime : spe.application_id => spe }

  service_principal_id       = databricks_service_principal.sp["app:${each.key}"].id
  allow_cluster_create       = each.value.allow_cluster_create
  allow_instance_pool_create = each.value.allow_instance_pool_create
  workspace_access           = each.value.workspace_access
  databricks_sql_access      = each.value.databricks_sql_access
}

resource "databricks_entitlements" "runtime_service_principal" {
  for_each = local.runtime_sp_enabled && length(local.sp_entitlements_runtime) > 0 ? {
    runtime = local.sp_entitlements_runtime[0]
  } : {}

  service_principal_id       = databricks_service_principal.sp[local.runtime_sp_workspace_key].id
  allow_cluster_create       = each.value.allow_cluster_create
  allow_instance_pool_create = each.value.allow_instance_pool_create
  workspace_access           = each.value.workspace_access
  databricks_sql_access      = each.value.databricks_sql_access
}
