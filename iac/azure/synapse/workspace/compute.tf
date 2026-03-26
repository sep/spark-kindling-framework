# =============================================================================
# Compute: Apache Spark Pools & Dedicated SQL Pools
# =============================================================================

# -----------------------------------------------------------------------------
# Spark Pools
# -----------------------------------------------------------------------------
resource "azurerm_synapse_spark_pool" "pools" {
  for_each = var.enable_spark_pools ? { for p in var.spark_pools : p.name => p } : {}

  name                 = each.value.name
  synapse_workspace_id = azurerm_synapse_workspace.this.id
  node_size_family     = each.value.node_size_family
  node_size            = each.value.node_size
  spark_version        = each.value.spark_version
  node_count           = each.value.node_count
  cache_size           = each.value.cache_size

  dynamic_executor_allocation_enabled = each.value.dynamic_executor_allocation_enabled
  min_executors                       = each.value.dynamic_executor_allocation_enabled ? each.value.min_executors : null
  max_executors                       = each.value.dynamic_executor_allocation_enabled ? each.value.max_executors : null

  session_level_packages_enabled = each.value.session_level_packages_enabled
  spark_log_folder               = each.value.spark_log_folder
  spark_events_folder            = each.value.spark_events_folder

  # Autoscale — only when node_count is not explicitly fixed
  dynamic "auto_scale" {
    for_each = each.value.node_count == null ? [1] : []
    content {
      min_node_count = each.value.autoscale_min_node_count
      max_node_count = each.value.autoscale_max_node_count
    }
  }

  # Auto-pause — always enabled with configurable delay
  auto_pause {
    delay_in_minutes = each.value.auto_pause_delay_in_minutes
  }

  # Spark config
  dynamic "spark_config" {
    for_each = each.value.spark_config_content != null ? [1] : []
    content {
      content  = each.value.spark_config_content
      filename = "spark_config.txt"
    }
  }

  # Library requirements
  dynamic "library_requirement" {
    for_each = { for lr in var.spark_pool_library_requirements : lr.pool_name => lr if lr.pool_name == each.key }
    content {
      content  = library_requirement.value.content
      filename = library_requirement.value.filename
    }
  }

  tags = merge(local.effective_tags, each.value.custom_tags)
}

# -----------------------------------------------------------------------------
# Dedicated SQL Pools
# -----------------------------------------------------------------------------
resource "azurerm_synapse_sql_pool" "pools" {
  for_each = var.enable_sql_pools ? { for p in var.sql_pools : p.name => p } : {}

  name                      = each.value.name
  synapse_workspace_id      = azurerm_synapse_workspace.this.id
  sku_name                  = each.value.sku_name
  create_mode               = each.value.create_mode
  collation                 = each.value.collation
  storage_account_type      = each.value.storage_account_type
  geo_backup_policy_enabled = each.value.geo_backup_policy_enabled
  data_encrypted            = each.value.data_encrypted

  tags = merge(local.effective_tags, each.value.tags)
}
