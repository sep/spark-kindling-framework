# =============================================================================
# Outputs
# =============================================================================

output "catalog_name" {
  description = "Name of the created Unity Catalog"
  value       = databricks_catalog.main.name
}

output "storage_credential_name" {
  description = "Name of the storage credential"
  value       = databricks_storage_credential.datalake.name
}

output "external_location_urls" {
  description = "Map of external location names to their ABFSS URLs"
  value       = { for k, loc in databricks_external_location.locations : k => loc.url }
}

output "schema_names" {
  description = "List of created schema names"
  value       = [for s in databricks_schema.schemas : s.name]
}

output "volume_paths" {
  description = "Map of volume names to their catalog paths"
  value = merge(
    { for k, v in databricks_volume.external : k => "${v.catalog_name}.${v.schema_name}.${v.name}" },
    { for k, v in databricks_volume.managed : k => "${v.catalog_name}.${v.schema_name}.${v.name}" },
  )
}

output "cluster_ids" {
  description = "Map of cluster names to their IDs"
  value       = { for k, c in databricks_cluster.clusters : k => c.cluster_id }
}

output "service_principal_ids" {
  description = "Map of SP application IDs to workspace IDs"
  value       = { for k, sp in databricks_service_principal.sp : k => sp.id }
}

output "secret_scope_names" {
  description = "Created secret scope names"
  value       = [for s in databricks_secret_scope.scopes : s.name]
}

output "pipeline_ids" {
  description = "Map of pipeline names to their IDs"
  value       = { for k, p in databricks_pipeline.pipelines : k => p.id }
}
