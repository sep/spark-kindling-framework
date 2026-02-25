# =============================================================================
# Outputs
# =============================================================================

output "catalog_name" {
  description = "Medallion catalog name used by this stack (created or existing)"
  value       = var.enable_unity_catalog ? local.medallion_catalog_name_effective : null
}

output "kindling_catalog_name" {
  description = "Kindling catalog name used by this stack (created or existing)"
  value       = var.enable_unity_catalog ? local.kindling_catalog_name_effective : null
}

output "storage_credential_name" {
  description = "Name of the storage credential"
  value       = var.enable_unity_catalog ? databricks_storage_credential.datalake[0].name : null
}

output "access_connector_id" {
  description = "Effective Databricks access connector resource ID"
  value       = local.access_connector_id_effective
}

output "external_location_urls" {
  description = "Map of managed external location names to their ABFSS URLs"
  value       = var.enable_unity_catalog ? { (databricks_external_location.artifacts[0].name) = databricks_external_location.artifacts[0].url } : {}
}

output "schema_names" {
  description = "List of created schema names"
  value       = var.enable_unity_catalog ? concat([for s in databricks_schema.schemas : s.name], [databricks_schema.kindling[0].name]) : []
}

output "volume_paths" {
  description = "Map of volume names to their catalog paths"
  value = var.enable_unity_catalog ? merge(
    { (databricks_volume.kindling_artifacts[0].name) = "${databricks_volume.kindling_artifacts[0].catalog_name}.${databricks_volume.kindling_artifacts[0].schema_name}.${databricks_volume.kindling_artifacts[0].name}" },
    { for k, v in databricks_volume.managed : k => "${v.catalog_name}.${v.schema_name}.${v.name}" },
  ) : {}
}

output "artifacts_volume_path" {
  description = "Databricks /Volumes path for the Kindling artifacts volume"
  value       = var.enable_unity_catalog ? "/Volumes/${local.kindling_catalog_name_effective}/${var.kindling_schema_name}/${var.kindling_artifacts_volume_name}" : null
}

output "cluster_ids" {
  description = "Map of cluster names to their IDs"
  value       = { for k, c in databricks_cluster.clusters : k => c.cluster_id }
}

output "service_principal_ids" {
  description = "Map of SP application IDs to workspace IDs"
  value       = { for _, sp in databricks_service_principal.sp : sp.application_id => sp.id }
}

output "runtime_service_principal_application_id" {
  description = "Effective runtime service principal app/client ID (created or provided)"
  value       = local.runtime_sp_application_id_effective
}

output "secret_scope_names" {
  description = "Created secret scope names"
  value       = [for s in databricks_secret_scope.scopes : s.name]
}

output "pipeline_ids" {
  description = "Map of pipeline names to their IDs"
  value       = { for k, p in databricks_pipeline.pipelines : k => p.id }
}
