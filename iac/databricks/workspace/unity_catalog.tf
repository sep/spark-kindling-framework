# =============================================================================
# Unity Catalog: Storage Credential, External Locations, Catalog, Schemas, Volumes
# =============================================================================

locals {
  abfss_base                       = "abfss://%s@${var.datalake_storage_account}.${var.adls_dfs_domain}"
  artifacts_external_location_url  = "${format(local.abfss_base, var.artifacts_container_name)}/"
  kindling_artifacts_subpath_clean = trim(var.kindling_artifacts_subpath, "/")
  kindling_artifacts_storage_target = (
    local.kindling_artifacts_subpath_clean != ""
    ? "${format(local.abfss_base, var.artifacts_container_name)}/${local.kindling_artifacts_subpath_clean}"
    : "${format(local.abfss_base, var.artifacts_container_name)}/"
  )
  medallion_catalog_name_effective = try(databricks_catalog.main[0].name, var.catalog_name)
  kindling_catalog_name_effective  = try(databricks_catalog.kindling[0].name, var.kindling_catalog_name)
  kindling_runtime_volume_catalog_name_effective = coalesce(
    var.kindling_runtime_volume_catalog_name,
    local.kindling_catalog_name_effective,
  )
  kindling_runtime_volume_schema_name_effective = coalesce(
    var.kindling_runtime_volume_schema_name,
    var.kindling_schema_name,
  )
  enable_kindling_artifacts_effective = coalesce(
    var.enable_kindling_artifacts,
    var.workspace_role == "platform",
  )
  enable_kindling_runtime_volumes_effective = coalesce(
    var.enable_kindling_runtime_volumes,
    var.workspace_role == "platform",
  )
  enable_kindling_platform_support_effective = coalesce(
    var.enable_kindling_platform_support,
    var.workspace_role == "platform",
  )
  storage_credential_sp_directory_id_effective = coalesce(var.storage_credential_sp_directory_id, var.azure_tenant_id)
  catalog_storage_root_effective = (
    var.catalog_storage_container != null &&
    var.catalog_storage_container != "" &&
    var.catalog_storage_path != null &&
    var.catalog_storage_path != ""
    ? "${format(local.abfss_base, var.catalog_storage_container)}/${trim(var.catalog_storage_path, "/")}"
    : null
  )
  kindling_catalog_storage_root_effective = (
    var.kindling_catalog_storage_container != null &&
    var.kindling_catalog_storage_container != "" &&
    var.kindling_catalog_storage_path != null &&
    var.kindling_catalog_storage_path != ""
    ? "${format(local.abfss_base, var.kindling_catalog_storage_container)}/${trim(var.kindling_catalog_storage_path, "/")}"
    : null
  )
}

# -----------------------------------------------------------------------------
# Storage Credential
# -----------------------------------------------------------------------------
resource "databricks_storage_credential" "datalake" {
  count = var.enable_unity_catalog ? 1 : 0

  name    = var.storage_credential_name
  comment = "Storage credential for ${var.datalake_storage_account}"

  dynamic "azure_managed_identity" {
    for_each = var.storage_credential_auth_type == "access_connector" ? [1] : []
    content {
      access_connector_id = local.access_connector_id_effective
    }
  }

  dynamic "azure_service_principal" {
    for_each = var.storage_credential_auth_type == "service_principal" ? [1] : []
    content {
      directory_id   = local.storage_credential_sp_directory_id_effective
      application_id = var.storage_credential_sp_application_id
      client_secret  = var.storage_credential_sp_client_secret
    }
  }

  lifecycle {
    precondition {
      condition = !var.enable_unity_catalog || var.storage_credential_auth_type != "access_connector" || (
        local.access_connector_id_effective != null &&
        local.access_connector_id_effective != ""
      )
      error_message = "storage_credential_auth_type=access_connector requires a valid access connector ID."
    }
    precondition {
      condition = !var.enable_unity_catalog || var.storage_credential_auth_type != "service_principal" || (
        var.storage_credential_sp_application_id != null &&
        var.storage_credential_sp_application_id != "" &&
        var.storage_credential_sp_client_secret != null &&
        var.storage_credential_sp_client_secret != ""
      )
      error_message = "storage_credential_auth_type=service_principal requires storage_credential_sp_application_id and storage_credential_sp_client_secret."
    }
  }
}

# -----------------------------------------------------------------------------
# External Locations
# -----------------------------------------------------------------------------
resource "databricks_external_location" "artifacts" {
  count = var.enable_unity_catalog ? 1 : 0

  name            = var.artifacts_external_location_name
  url             = local.artifacts_external_location_url
  credential_name = databricks_storage_credential.datalake[0].name
  comment         = "External location for ${var.artifacts_container_name} container"

  depends_on = [databricks_storage_credential.datalake]
}

# -----------------------------------------------------------------------------
# Catalog
# -----------------------------------------------------------------------------
resource "databricks_catalog" "main" {
  count = var.enable_unity_catalog && var.create_catalog ? 1 : 0

  name           = var.catalog_name
  storage_root   = local.catalog_storage_root_effective
  comment        = "Primary ${var.environment} catalog"
  isolation_mode = "OPEN"

  depends_on = [databricks_external_location.artifacts]
}

resource "databricks_catalog" "kindling" {
  count = var.enable_unity_catalog && local.enable_kindling_artifacts_effective && var.create_kindling_catalog ? 1 : 0

  name           = var.kindling_catalog_name
  storage_root   = local.kindling_catalog_storage_root_effective
  comment        = "Kindling infrastructure ${var.environment} catalog"
  isolation_mode = "OPEN"

  depends_on = [databricks_external_location.artifacts]
}

# -----------------------------------------------------------------------------
# Schemas
# -----------------------------------------------------------------------------
resource "databricks_schema" "schemas" {
  for_each = var.enable_unity_catalog && var.create_base_schemas ? { for s in var.schemas : s.name => s } : {}

  catalog_name = local.medallion_catalog_name_effective
  name         = each.value.name
  storage_root = each.value.storage_root
  comment      = "${each.value.name} schema"
}

resource "databricks_schema" "kindling" {
  count = var.enable_unity_catalog && local.enable_kindling_artifacts_effective ? 1 : 0

  catalog_name = local.kindling_catalog_name_effective
  name         = var.kindling_schema_name
  comment      = "Schema for Kindling UC volumes"
}

# -----------------------------------------------------------------------------
# Kindling Artifacts External Volume (single artifacts-backed volume)
# -----------------------------------------------------------------------------
resource "databricks_volume" "kindling_artifacts" {
  count = var.enable_unity_catalog && local.enable_kindling_artifacts_effective ? 1 : 0

  catalog_name     = local.kindling_catalog_name_effective
  schema_name      = databricks_schema.kindling[0].name
  name             = var.kindling_artifacts_volume_name
  volume_type      = "EXTERNAL"
  storage_location = local.kindling_artifacts_storage_target
  comment          = "Kindling artifacts external volume"

  depends_on = [databricks_external_location.artifacts, databricks_schema.kindling]
}

# -----------------------------------------------------------------------------
# Managed Volumes (in catalog.kindling_schema_name)
# -----------------------------------------------------------------------------
resource "databricks_volume" "managed" {
  for_each = var.enable_unity_catalog && local.enable_kindling_runtime_volumes_effective ? toset(var.managed_volumes) : toset([])

  catalog_name = local.kindling_runtime_volume_catalog_name_effective
  schema_name  = local.kindling_runtime_volume_schema_name_effective
  name         = each.value
  volume_type  = "MANAGED"
  comment      = "Managed volume: ${each.value}"

  depends_on = [databricks_schema.kindling, databricks_schema.schemas]
}
