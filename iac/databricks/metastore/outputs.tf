output "metastore_id" {
  description = "Effective metastore ID (created or existing)"
  value       = local.metastore_id_effective
}

output "created_metastore_id" {
  description = "Created metastore ID, or null when reusing existing"
  value       = try(databricks_metastore.this[0].id, null)
}

output "workspace_assignment_ids" {
  description = "Map of workspace ID to metastore assignment ID"
  value       = { for ws_id, assignment in databricks_metastore_assignment.workspace : ws_id => assignment.id }
}
