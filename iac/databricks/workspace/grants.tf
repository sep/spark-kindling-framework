# =============================================================================
# Unity Catalog Grants
# =============================================================================
# databricks_grants is DECLARATIVE per securable — one resource per securable
# that declares ALL grants. Multiple resources for the same securable will
# overwrite each other. We group grants by securable name using locals.
# =============================================================================

locals {
  # Replace runtime SP placeholder alias with effective runtime SP app/client ID
  catalog_grants_resolved = [
    for g in var.catalog_grants : merge(g, {
      principal = g.principal == var.runtime_sp_principal_alias && local.runtime_sp_application_id_effective != null ? local.runtime_sp_application_id_effective : g.principal
    })
  ]

  kindling_catalog_grants_resolved = [
    for g in var.kindling_catalog_grants : merge(g, {
      principal = g.principal == var.runtime_sp_principal_alias && local.runtime_sp_application_id_effective != null ? local.runtime_sp_application_id_effective : g.principal
    })
  ]

  schema_grants_resolved = [
    for g in var.schema_grants : merge(g, {
      principal = g.principal == var.runtime_sp_principal_alias && local.runtime_sp_application_id_effective != null ? local.runtime_sp_application_id_effective : g.principal
    })
  ]

  medallion_schema_grants_resolved = [
    for g in var.medallion_schema_grants : merge(g, {
      principal = g.principal == var.runtime_sp_principal_alias && local.runtime_sp_application_id_effective != null ? local.runtime_sp_application_id_effective : g.principal
    })
  ]

  external_location_grants_resolved = [
    for g in var.external_location_grants : merge(g, {
      principal = g.principal == var.runtime_sp_principal_alias && local.runtime_sp_application_id_effective != null ? local.runtime_sp_application_id_effective : g.principal
    })
  ]

  volume_grants_resolved = [
    for g in var.volume_grants : merge(g, {
      principal    = g.principal == var.runtime_sp_principal_alias && local.runtime_sp_application_id_effective != null ? local.runtime_sp_application_id_effective : g.principal
      catalog_name = coalesce(try(g.catalog_name, null), local.kindling_catalog_name_effective)
      schema_name  = coalesce(try(g.schema_name, null), var.kindling_schema_name)
    })
  ]

  storage_credential_grants_resolved = [
    for g in var.storage_credential_grants : merge(g, {
      principal = g.principal == var.runtime_sp_principal_alias && local.runtime_sp_application_id_effective != null ? local.runtime_sp_application_id_effective : g.principal
    })
  ]

  any_file_grants_resolved = [
    for g in var.any_file_grants : merge(g, {
      principal = g.principal == var.runtime_sp_principal_alias && local.runtime_sp_application_id_effective != null ? local.runtime_sp_application_id_effective : g.principal
    })
  ]

  # Group external_location_grants by location_name
  ext_loc_grants_grouped = {
    for loc_name in distinct([for g in local.external_location_grants_resolved : g.location_name]) :
    loc_name => [for g in local.external_location_grants_resolved : g if g.location_name == loc_name]
  }

  # Group volume_grants by fully qualified volume name
  volume_grants_grouped = {
    for volume_key in distinct([
      for g in local.volume_grants_resolved : "${g.catalog_name}.${g.schema_name}.${g.volume_name}"
    ]) :
    volume_key => [for g in local.volume_grants_resolved : g if "${g.catalog_name}.${g.schema_name}.${g.volume_name}" == volume_key]
  }

  # Group kindling schema_grants by schema_name
  kindling_schema_grants_grouped = {
    for schema_name in distinct([for g in local.schema_grants_resolved : g.schema_name]) :
    schema_name => [for g in local.schema_grants_resolved : g if g.schema_name == schema_name]
  }

  # Group medallion schema_grants by schema_name
  medallion_schema_grants_grouped = {
    for schema_name in distinct([for g in local.medallion_schema_grants_resolved : g.schema_name]) :
    schema_name => [for g in local.medallion_schema_grants_resolved : g if g.schema_name == schema_name]
  }
}

# -----------------------------------------------------------------------------
# Catalog Grants
# -----------------------------------------------------------------------------
resource "databricks_grants" "catalog" {
  count = var.enable_unity_catalog ? 1 : 0

  catalog = local.medallion_catalog_name_effective

  dynamic "grant" {
    for_each = local.catalog_grants_resolved
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }
}

resource "databricks_grants" "kindling_catalog" {
  count = var.enable_unity_catalog && local.enable_kindling_artifacts_effective ? 1 : 0

  catalog = local.kindling_catalog_name_effective

  dynamic "grant" {
    for_each = local.kindling_catalog_grants_resolved
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
resource "databricks_grants" "kindling_schemas" {
  for_each = {
    for k, v in local.kindling_schema_grants_grouped : k => v
    if k != "default" # default schema grants must be applied via API/SQL
    && var.enable_unity_catalog
    && local.enable_kindling_artifacts_effective
  }

  schema = "${local.kindling_catalog_name_effective}.${each.key}"

  dynamic "grant" {
    for_each = each.value
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }

  depends_on = [databricks_schema.kindling]
}

resource "databricks_grants" "medallion_schemas" {
  for_each = {
    for k, v in local.medallion_schema_grants_grouped : k => v
    if k != "default" # default schema grants must be applied via API/SQL
    && var.enable_unity_catalog
  }

  schema = "${local.medallion_catalog_name_effective}.${each.key}"

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
  for_each = var.enable_unity_catalog ? local.ext_loc_grants_grouped : {}

  external_location = each.key

  dynamic "grant" {
    for_each = each.value
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }

  depends_on = [databricks_external_location.artifacts]
}

# -----------------------------------------------------------------------------
# Volume Grants (one resource per volume, all principals inside)
# -----------------------------------------------------------------------------
resource "databricks_grants" "volumes" {
  for_each = var.enable_unity_catalog && (local.enable_kindling_artifacts_effective || local.enable_kindling_runtime_volumes_effective) ? local.volume_grants_grouped : {}

  volume = each.key

  dynamic "grant" {
    for_each = each.value
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }

  depends_on = [databricks_volume.kindling_artifacts, databricks_volume.managed]
}

# -----------------------------------------------------------------------------
# Storage Credential Grants
# -----------------------------------------------------------------------------
resource "databricks_grants" "storage_credential" {
  count = var.enable_unity_catalog && length(local.storage_credential_grants_resolved) > 0 ? 1 : 0

  storage_credential = databricks_storage_credential.datalake[0].id

  dynamic "grant" {
    for_each = local.storage_credential_grants_resolved
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }
}

# -----------------------------------------------------------------------------
# ANY FILE Grants
# -----------------------------------------------------------------------------
resource "databricks_sql_permissions" "any_file" {
  count = length(local.any_file_grants_resolved) > 0 ? 1 : 0

  any_file = true

  dynamic "privilege_assignments" {
    for_each = local.any_file_grants_resolved
    content {
      principal  = privilege_assignments.value.principal
      privileges = privilege_assignments.value.privileges
    }
  }
}
