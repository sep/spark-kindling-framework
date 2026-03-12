variable "databricks_account_host" {
  description = "Databricks account console host (for example https://accounts.azuredatabricks.net)"
  type        = string
}

variable "databricks_account_id" {
  description = "Databricks account ID"
  type        = string
}

variable "create_metastore" {
  description = "Whether Terraform should create a new Unity Catalog metastore"
  type        = bool
  default     = false

  validation {
    condition = var.create_metastore ? (
      var.metastore_name != null &&
      var.metastore_name != "" &&
      var.metastore_storage_root != null &&
      var.metastore_storage_root != "" &&
      var.metastore_region != null &&
      var.metastore_region != ""
      ) : (
      var.existing_metastore_id != null &&
      var.existing_metastore_id != ""
    )
    error_message = "If create_metastore=true, set metastore_name/metastore_storage_root/metastore_region. If false, set existing_metastore_id."
  }
}

variable "existing_metastore_id" {
  description = "Existing metastore ID to reuse when create_metastore=false"
  type        = string
  default     = null
  nullable    = true
}

variable "metastore_name" {
  description = "Name of the metastore to create when create_metastore=true"
  type        = string
  default     = null
  nullable    = true
}

variable "metastore_storage_root" {
  description = "Storage root for the metastore (for example abfss://container@account.dfs.core.windows.net/path)"
  type        = string
  default     = null
  nullable    = true
}

variable "metastore_region" {
  description = "Cloud region for the metastore (must match workspace region family)"
  type        = string
  default     = null
  nullable    = true
}

variable "metastore_owner" {
  description = "Optional metastore owner principal"
  type        = string
  default     = null
  nullable    = true
}

variable "metastore_force_destroy" {
  description = "Whether to allow deleting the metastore even if it contains objects"
  type        = bool
  default     = false
}

variable "assign_workspaces" {
  description = "Whether Terraform should assign the effective metastore to workspace_ids"
  type        = bool
  default     = true

  validation {
    condition     = !var.assign_workspaces || length(var.workspace_ids) > 0
    error_message = "assign_workspaces=true requires at least one workspace_id."
  }
}

variable "workspace_ids" {
  description = "Databricks workspace numeric IDs to assign to the metastore"
  type        = list(number)
  default     = []
}

variable "default_catalog_name" {
  description = "Optional default catalog name for each metastore assignment"
  type        = string
  default     = null
  nullable    = true
}
