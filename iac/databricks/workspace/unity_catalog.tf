# =============================================================================
# Unity Catalog: Storage Credential, External Locations, Catalog, Schemas, Volumes
# =============================================================================

locals {
  abfss_base = "abfss://%s@${var.datalake_storage_account}.dfs.core.windows.net"
}

# -----------------------------------------------------------------------------
# Storage Credential (Managed Identity via Access Connector)
# -----------------------------------------------------------------------------
resource "databricks_storage_credential" "datalake" {
  name    = var.storage_credential_name
  comment = "Managed identity credential for ${var.datalake_storage_account}"

  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }
}

# -----------------------------------------------------------------------------
# External Locations
# -----------------------------------------------------------------------------
resource "databricks_external_location" "locations" {
  for_each = { for loc in var.datalake_containers : loc.name => loc }

  name            = each.value.name
  url             = "${format(local.abfss_base, each.value.container)}/"
  credential_name = databricks_storage_credential.datalake.name
  comment         = "External location for ${each.value.container} container"

  depends_on = [databricks_storage_credential.datalake]
}

# -----------------------------------------------------------------------------
# Catalog
# -----------------------------------------------------------------------------
resource "databricks_catalog" "main" {
  name            = var.catalog_name
  storage_root    = "${format(local.abfss_base, var.catalog_storage_container)}/${var.catalog_storage_path}"
  comment         = "Primary ${var.environment} catalog"
  isolation_mode  = "OPEN"

  depends_on = [databricks_external_location.locations]
}

# -----------------------------------------------------------------------------
# Schemas
# -----------------------------------------------------------------------------
resource "databricks_schema" "schemas" {
  for_each = { for s in var.schemas : s.name => s }

  catalog_name  = databricks_catalog.main.name
  name          = each.value.name
  storage_root  = each.value.storage_root
  comment       = "${each.value.name} schema"
}

# The 'default' schema is auto-created with the catalog.
# We need to ensure it exists before creating volumes in it.
resource "databricks_schema" "default" {
  catalog_name = databricks_catalog.main.name
  name         = "default"
  comment      = "Default schema"

  lifecycle {
    # The default schema already exists; prevent destroy on teardown
    prevent_destroy = true
  }
}

# -----------------------------------------------------------------------------
# External Volumes (in catalog.default)
# -----------------------------------------------------------------------------
resource "databricks_volume" "external" {
  for_each = { for v in var.external_volumes : v.name => v }

  catalog_name     = databricks_catalog.main.name
  schema_name      = "default"
  name             = each.value.name
  volume_type      = "EXTERNAL"
  storage_location = "${format(local.abfss_base, each.value.container)}/${each.value.path}"
  comment          = "External volume: ${each.value.name}"

  depends_on = [databricks_external_location.locations, databricks_schema.default]
}

# -----------------------------------------------------------------------------
# Managed Volumes (in catalog.default)
# -----------------------------------------------------------------------------
resource "databricks_volume" "managed" {
  for_each = toset(var.managed_volumes)

  catalog_name = databricks_catalog.main.name
  schema_name  = "default"
  name         = each.value
  volume_type  = "MANAGED"
  comment      = "Managed volume: ${each.value}"

  depends_on = [databricks_schema.default]
}
