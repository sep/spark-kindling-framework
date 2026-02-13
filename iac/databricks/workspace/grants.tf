# =============================================================================
# Unity Catalog Grants
# =============================================================================
# databricks_grants is DECLARATIVE per securable — one resource per securable
# that declares ALL grants. Multiple resources for the same securable will
# overwrite each other. We group grants by securable name using locals.
# =============================================================================

locals {
  # Group external_location_grants by location_name
  ext_loc_grants_grouped = {
    for loc_name in distinct([for g in var.external_location_grants : g.location_name]) :
    loc_name => [for g in var.external_location_grants : g if g.location_name == loc_name]
  }

  # Group volume_grants by volume_name
  volume_grants_grouped = {
    for vol_name in distinct([for g in var.volume_grants : g.volume_name]) :
    vol_name => [for g in var.volume_grants : g if g.volume_name == vol_name]
  }

  # Group schema_grants by schema_name
  schema_grants_grouped = {
    for schema_name in distinct([for g in var.schema_grants : g.schema_name]) :
    schema_name => [for g in var.schema_grants : g if g.schema_name == schema_name]
  }
}

# -----------------------------------------------------------------------------
# Catalog Grants
# -----------------------------------------------------------------------------
resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.main.name

  dynamic "grant" {
    for_each = var.catalog_grants
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }
}

# -----------------------------------------------------------------------------
# Schema Grants (one resource per schema, all principals inside)
# Note: The 'default' schema has a known provider issue with grants.
# Use the Databricks REST API or SQL GRANT for default schema grants.
# -----------------------------------------------------------------------------
resource "databricks_grants" "schemas" {
  for_each = {
    for k, v in local.schema_grants_grouped : k => v
    if k != "default" # default schema grants must be applied via API/SQL
  }

  schema = "${databricks_catalog.main.name}.${each.key}"

  dynamic "grant" {
    for_each = each.value
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }

  depends_on = [databricks_schema.schemas]
}

# -----------------------------------------------------------------------------
# External Location Grants (one resource per location, all principals inside)
# -----------------------------------------------------------------------------
resource "databricks_grants" "external_locations" {
  for_each = local.ext_loc_grants_grouped

  external_location = each.key

  dynamic "grant" {
    for_each = each.value
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }

  depends_on = [databricks_external_location.locations]
}

# -----------------------------------------------------------------------------
# Volume Grants (one resource per volume, all principals inside)
# -----------------------------------------------------------------------------
resource "databricks_grants" "volumes" {
  for_each = local.volume_grants_grouped

  volume = "${databricks_catalog.main.name}.default.${each.key}"

  dynamic "grant" {
    for_each = each.value
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }

  depends_on = [databricks_volume.external, databricks_volume.managed]
}

# -----------------------------------------------------------------------------
# Storage Credential Grants
# -----------------------------------------------------------------------------
resource "databricks_grants" "storage_credential" {
  count = length(var.storage_credential_grants) > 0 ? 1 : 0

  storage_credential = databricks_storage_credential.datalake.id

  dynamic "grant" {
    for_each = var.storage_credential_grants
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }
}
