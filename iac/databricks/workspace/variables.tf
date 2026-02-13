# -----------------------------------------------------------------------------
# Azure
# -----------------------------------------------------------------------------
variable "azure_subscription_id" {
  description = "Azure subscription ID"
  type        = string
}

variable "azure_tenant_id" {
  description = "Azure AD tenant ID"
  type        = string
}

# -----------------------------------------------------------------------------
# Databricks Workspace
# -----------------------------------------------------------------------------
variable "databricks_host" {
  description = "Databricks workspace URL (e.g. https://adb-123.4.azuredatabricks.net)"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

# -----------------------------------------------------------------------------
# Storage / Data Lake
# -----------------------------------------------------------------------------
variable "datalake_storage_account" {
  description = "ADLS Gen2 storage account name (e.g. sepstdatalakedev)"
  type        = string
}

variable "access_connector_id" {
  description = "Full resource ID of the Databricks access connector for managed identity auth to ADLS"
  type        = string
}

variable "storage_credential_name" {
  description = "Name for the UC storage credential"
  type        = string
  default     = "datalake_managed_identity"
}

variable "datalake_containers" {
  description = "ADLS containers to expose as UC external locations"
  type = list(object({
    name      = string
    container = string # container name in storage account
  }))
  default = [
    { name = "artifacts", container = "artifacts" },
    { name = "bronze", container = "bronze" },
    { name = "gold", container = "gold" },
    { name = "landing", container = "landing" },
    { name = "silver", container = "silver" },
  ]
}

# -----------------------------------------------------------------------------
# Unity Catalog
# -----------------------------------------------------------------------------
variable "catalog_name" {
  description = "Name of the primary UC catalog"
  type        = string
  default     = "medallion"
}

variable "catalog_storage_container" {
  description = "ADLS container for catalog managed storage"
  type        = string
  default     = "artifacts"
}

variable "catalog_storage_path" {
  description = "Path within the container for catalog managed storage"
  type        = string
  default     = "catalog"
}

variable "schemas" {
  description = "Schemas to create in the primary catalog. Set storage_root to override managed location."
  type = list(object({
    name         = string
    storage_root = optional(string, null) # full abfss:// path, or null for managed
  }))
  default = [
    { name = "bronze", storage_root = null },
    { name = "gold", storage_root = null },
    { name = "silver", storage_root = null },
  ]
}

variable "external_volumes" {
  description = "External volumes to create in {catalog}.default"
  type = list(object({
    name      = string
    container = string # container name in storage account
    path      = string # path within container
  }))
  default = [
    { name = "config", container = "artifacts", path = "config" },
    { name = "data_apps", container = "artifacts", path = "data-apps" },
    { name = "logs", container = "artifacts", path = "logs" },
    { name = "packages", container = "artifacts", path = "packages" },
    { name = "scripts", container = "artifacts", path = "scripts" },
  ]
}

variable "managed_volumes" {
  description = "Managed volumes to create in {catalog}.default"
  type        = list(string)
  default     = ["temp"]
}

# -----------------------------------------------------------------------------
# Secret Scopes
# -----------------------------------------------------------------------------
variable "secret_scopes" {
  description = "Databricks secret scopes to create"
  type        = list(string)
  default     = ["sepdev-adls-scope"]
}

# -----------------------------------------------------------------------------
# Service Principals
# -----------------------------------------------------------------------------
variable "service_principals" {
  description = "Azure AD service principals to register in the workspace"
  type = list(object({
    display_name   = string
    application_id = string
  }))
  default = []
}

# -----------------------------------------------------------------------------
# Users & Groups
# -----------------------------------------------------------------------------
variable "admin_users" {
  description = "User emails to add to the admins group"
  type        = list(string)
  default     = []
}

variable "workspace_users" {
  description = "User emails to add to the workspace users group"
  type        = list(string)
  default     = []
}

# -----------------------------------------------------------------------------
# Clusters
# -----------------------------------------------------------------------------
variable "clusters" {
  description = "Interactive clusters to create"
  type = list(object({
    name                    = string
    spark_version           = string
    node_type_id            = string
    num_workers             = optional(number, 0)
    autotermination_minutes = optional(number, 60)
    data_security_mode      = optional(string, "SINGLE_USER")
    single_user_name        = optional(string, null)
    spot_policy             = optional(string, "ON_DEMAND_AZURE") # ON_DEMAND_AZURE | SPOT_WITH_FALLBACK_AZURE
    spark_conf              = optional(map(string), {})
    custom_tags             = optional(map(string), {})
  }))
  default = []
}

# -----------------------------------------------------------------------------
# DLT Pipelines
# -----------------------------------------------------------------------------
variable "dlt_pipelines" {
  description = "Delta Live Tables pipelines to create"
  type = list(object({
    name           = string
    catalog        = optional(string, null) # defaults to var.catalog_name
    target_schema  = string
    notebook_path  = optional(string, null)
    file_glob      = optional(string, null)
    node_type_id   = optional(string, "Standard_DS3_v2")
    num_workers    = optional(number, 1)
    development    = optional(bool, true)
    continuous     = optional(bool, false)
    photon         = optional(bool, false)
    configuration  = optional(map(string), {})
  }))
  default = []
}

# -----------------------------------------------------------------------------
# Workspace Configuration
# -----------------------------------------------------------------------------
variable "workspace_conf" {
  description = "Workspace-level configuration settings"
  type        = map(string)
  default     = {}
}

# -----------------------------------------------------------------------------
# Unity Catalog Grants
# -----------------------------------------------------------------------------
variable "catalog_grants" {
  description = "Grants on the primary catalog"
  type = list(object({
    principal  = string
    privileges = list(string)
  }))
  default = []
}

variable "schema_grants" {
  description = "Per-schema grants (schema_name + principal + privileges)"
  type = list(object({
    schema_name = string
    principal   = string
    privileges  = list(string)
  }))
  default = []
}

variable "external_location_grants" {
  description = "Per-external-location grants"
  type = list(object({
    location_name = string
    principal     = string
    privileges    = list(string)
  }))
  default = []
}

variable "volume_grants" {
  description = "Per-volume grants (volume in catalog.default)"
  type = list(object({
    volume_name = string
    principal   = string
    privileges  = list(string)
  }))
  default = []
}

variable "storage_credential_grants" {
  description = "Grants on the storage credential"
  type = list(object({
    principal  = string
    privileges = list(string)
  }))
  default = []
}

# -----------------------------------------------------------------------------
# Secret Scope ACLs
# -----------------------------------------------------------------------------
variable "secret_scope_acls" {
  description = "ACLs for secret scopes"
  type = list(object({
    scope      = string
    principal  = string
    permission = string # MANAGE, WRITE, or READ
  }))
  default = []
}

# -----------------------------------------------------------------------------
# Entitlements
# -----------------------------------------------------------------------------
variable "user_entitlements" {
  description = "Per-user entitlement overrides"
  type = list(object({
    user_name                  = string
    allow_cluster_create       = optional(bool, false)
    allow_instance_pool_create = optional(bool, false)
    workspace_access           = optional(bool, false)
    databricks_sql_access      = optional(bool, false)
  }))
  default = []
}

variable "sp_entitlements" {
  description = "Per-service-principal entitlement overrides"
  type = list(object({
    application_id             = string
    workspace_access           = optional(bool, false)
    databricks_sql_access      = optional(bool, false)
    allow_cluster_create       = optional(bool, false)
    allow_instance_pool_create = optional(bool, false)
  }))
  default = []
}
