# =============================================================================
# Outputs
# =============================================================================

output "workspace_id" {
  description = "Resource ID of the Synapse workspace"
  value       = azurerm_synapse_workspace.this.id
}

output "workspace_name" {
  description = "Name of the Synapse workspace"
  value       = azurerm_synapse_workspace.this.name
}

output "connectivity_endpoints" {
  description = "Map of connectivity endpoint types to their URLs"
  value       = azurerm_synapse_workspace.this.connectivity_endpoints
}

output "managed_identity_principal_id" {
  description = "Principal ID of the workspace system-assigned managed identity"
  value       = azurerm_synapse_workspace.this.identity[0].principal_id
}

output "managed_identity_tenant_id" {
  description = "Tenant ID of the workspace system-assigned managed identity"
  value       = azurerm_synapse_workspace.this.identity[0].tenant_id
}

output "managed_resource_group_name" {
  description = "Name of the managed resource group created by Synapse"
  value       = azurerm_synapse_workspace.this.managed_resource_group_name
}

output "spark_pool_ids" {
  description = "Map of Spark pool names to their resource IDs"
  value       = { for k, p in azurerm_synapse_spark_pool.pools : k => p.id }
}

output "spark_pool_names" {
  description = "List of created Spark pool names"
  value       = [for p in azurerm_synapse_spark_pool.pools : p.name]
}

output "sql_pool_ids" {
  description = "Map of SQL pool names to their resource IDs"
  value       = { for k, p in azurerm_synapse_sql_pool.pools : k => p.id }
}

output "datalake_filesystem_id" {
  description = "Resource ID of the default ADLS Gen2 filesystem"
  value       = var.create_datalake_filesystem ? azurerm_storage_data_lake_gen2_filesystem.default[0].id : null
}

output "firewall_rule_ids" {
  description = "Map of firewall rule names to their resource IDs"
  value       = { for k, r in azurerm_synapse_firewall_rule.rules : k => r.id }
}

output "managed_private_endpoint_ids" {
  description = "Map of managed private endpoint names to their resource IDs"
  value       = { for k, ep in azurerm_synapse_managed_private_endpoint.endpoints : k => ep.id }
}

output "dev_endpoint" {
  description = "The workspace development endpoint URL"
  value       = try(azurerm_synapse_workspace.this.connectivity_endpoints["dev"], null)
}

output "sql_endpoint" {
  description = "The workspace serverless SQL endpoint URL"
  value       = try(azurerm_synapse_workspace.this.connectivity_endpoints["sqlOnDemand"], null)
}

output "sql_dedicated_endpoint" {
  description = "The workspace dedicated SQL endpoint URL"
  value       = try(azurerm_synapse_workspace.this.connectivity_endpoints["sql"], null)
}
