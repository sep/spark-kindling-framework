# =============================================================================
# Compute: Interactive Clusters
# =============================================================================

resource "databricks_cluster" "clusters" {
  for_each = { for c in var.clusters : c.name => c }

  cluster_name            = each.value.name
  spark_version           = each.value.spark_version
  node_type_id            = each.value.node_type_id
  num_workers             = each.value.num_workers
  autotermination_minutes = each.value.autotermination_minutes
  data_security_mode      = each.value.data_security_mode
  single_user_name        = each.value.single_user_name

  spark_conf = merge(
    # Auto-add single-node config when num_workers is 0
    each.value.num_workers == 0 ? {
      "spark.databricks.cluster.profile" = "singleNode"
      "spark.master"                     = "local[*, 4]"
    } : {},
    each.value.spark_conf
  )

  custom_tags = merge(
    each.value.num_workers == 0 ? { "ResourceClass" = "SingleNode" } : {},
    each.value.custom_tags
  )

  azure_attributes {
    first_on_demand    = 1
    availability       = each.value.spot_policy
    spot_bid_max_price = -1
  }

  # Cluster state is provider-managed; no lifecycle overrides needed
}
