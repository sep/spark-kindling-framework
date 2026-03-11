# =============================================================================
# Compute: Interactive Clusters
# =============================================================================

locals {
  cluster_permissions_resolved = [
    for cp in var.cluster_permissions : merge(cp, {
      principal = cp.principal == var.runtime_sp_principal_alias && local.runtime_sp_application_id_effective != null ? local.runtime_sp_application_id_effective : cp.principal
    })
  ]

  cluster_permissions_grouped = {
    for cluster_name in distinct([for cp in local.cluster_permissions_resolved : cp.cluster_name]) :
    cluster_name => [for cp in local.cluster_permissions_resolved : cp if cp.cluster_name == cluster_name]
  }
}

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

resource "databricks_permissions" "clusters" {
  for_each = local.cluster_permissions_grouped

  cluster_id = databricks_cluster.clusters[each.key].id

  dynamic "access_control" {
    for_each = each.value
    content {
      group_name             = access_control.value.principal_type == "group" ? access_control.value.principal : null
      user_name              = access_control.value.principal_type == "user" ? access_control.value.principal : null
      service_principal_name = access_control.value.principal_type == "service_principal" ? access_control.value.principal : null
      permission_level       = access_control.value.permission_level
    }
  }
}
