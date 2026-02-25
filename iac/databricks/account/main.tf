provider "databricks" {
  alias      = "account"
  host       = var.databricks_account_host
  account_id = var.databricks_account_id
}

resource "databricks_metastore" "this" {
  provider = databricks.account
  count    = var.create_metastore ? 1 : 0

  name          = var.metastore_name
  storage_root  = var.metastore_storage_root
  region        = var.metastore_region
  owner         = var.metastore_owner
  force_destroy = var.metastore_force_destroy
}

locals {
  metastore_id_effective = var.create_metastore ? databricks_metastore.this[0].id : var.existing_metastore_id
}

resource "databricks_metastore_assignment" "workspace" {
  provider = databricks.account
  for_each = var.assign_workspaces ? {
    for ws_id in var.workspace_ids : tostring(ws_id) => ws_id
  } : {}

  workspace_id         = each.value
  metastore_id         = local.metastore_id_effective
  default_catalog_name = var.default_catalog_name
}
