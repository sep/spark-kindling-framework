# =============================================================================
# DBFS Mounts (non-UC alternative to External Locations + Volumes)
#
# When enable_unity_catalog = false, these resources provide lake access via
# DBFS mount points instead of UC storage credentials + external locations.
# The service principal used here must have Storage Blob Data Contributor on
# the target storage account.
# =============================================================================

# -----------------------------------------------------------------------------
# Artifacts mount — equivalent of the kindling_artifacts UC volume
# -----------------------------------------------------------------------------
resource "databricks_mount" "artifacts" {
  count = !var.enable_unity_catalog && local.enable_kindling_artifacts_effective && var.create_artifacts_mount ? 1 : 0

  name = var.artifacts_mount_name

  abfs {
    storage_account_name   = var.datalake_storage_account
    container_name         = var.artifacts_container_name
    tenant_id              = var.azure_tenant_id
    client_id              = var.mount_sp_application_id
    client_secret_scope    = var.mount_sp_secret_scope
    client_secret_key      = var.mount_sp_secret_key
    initialize_file_system = false
  }
}

# -----------------------------------------------------------------------------
# Additional mounts (data containers, checkpoint storage, etc.)
# -----------------------------------------------------------------------------
resource "databricks_mount" "additional" {
  for_each = !var.enable_unity_catalog ? { for m in var.dbfs_mounts : m.name => m } : {}

  name = each.value.name

  abfs {
    storage_account_name   = coalesce(each.value.storage_account, var.datalake_storage_account)
    container_name         = each.value.container_name
    tenant_id              = var.azure_tenant_id
    client_id              = var.mount_sp_application_id
    client_secret_scope    = var.mount_sp_secret_scope
    client_secret_key      = var.mount_sp_secret_key
    initialize_file_system = false
  }

  depends_on = [databricks_secret_scope.scopes, databricks_secret_scope.keyvault]
}
