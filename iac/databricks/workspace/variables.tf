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

variable "azure_environment" {
  description = "Azure cloud environment for providers (public or usgovernment)"
  type        = string
  default     = "public"
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

variable "adls_dfs_domain" {
  description = "ADLS DFS endpoint domain (public Azure: dfs.core.windows.net, US Gov: dfs.core.usgovcloudapi.net)"
  type        = string
  default     = "dfs.core.windows.net"
}

variable "artifacts_container_name" {
  description = "ADLS container used for the single Kindling external location"
  type        = string
  default     = "artifacts"
}

variable "artifacts_external_location_name" {
  description = "Unity Catalog external location name for the artifacts container"
  type        = string
  default     = "artifacts"
}

variable "access_connector_id" {
  description = "Full resource ID of an existing Databricks access connector (used when create_access_connector=false)"
  type        = string
  default     = null
  nullable    = true
}

variable "create_access_connector" {
  description = "Whether Terraform should create a Databricks access connector"
  type        = bool
  default     = false

  validation {
    condition = var.storage_credential_auth_type == "access_connector" ? (
      var.create_access_connector ? (
        var.access_connector_resource_group_name != null &&
        var.access_connector_resource_group_name != "" &&
        var.access_connector_location != null &&
        var.access_connector_location != ""
        ) : (
        var.access_connector_id != null &&
        var.access_connector_id != ""
      )
    ) : true
    error_message = "When storage_credential_auth_type=access_connector: if create_access_connector=true set access_connector_resource_group_name/access_connector_location; if false set access_connector_id."
  }

  validation {
    condition     = !var.create_access_connector || var.azure_environment != "usgovernment"
    error_message = "create_access_connector=true is not supported in Azure US Gov. Use storage_credential_auth_type=\"service_principal\"."
  }
}

variable "storage_credential_auth_type" {
  description = "Storage credential auth type: access_connector (managed identity) or service_principal"
  type        = string
  default     = "access_connector"

  validation {
    condition     = contains(["access_connector", "service_principal"], var.storage_credential_auth_type)
    error_message = "storage_credential_auth_type must be one of: access_connector, service_principal."
  }
}

variable "storage_credential_sp_application_id" {
  description = "Application (client) ID for storage credential service principal (required when storage_credential_auth_type=service_principal)"
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.storage_credential_auth_type != "service_principal" || (
      var.storage_credential_sp_application_id != null &&
      var.storage_credential_sp_application_id != ""
    )
    error_message = "storage_credential_sp_application_id is required when storage_credential_auth_type=service_principal."
  }
}

variable "storage_credential_sp_client_secret" {
  description = "Client secret for storage credential service principal (required when storage_credential_auth_type=service_principal)"
  type        = string
  default     = null
  nullable    = true
  sensitive   = true

  validation {
    condition = var.storage_credential_auth_type != "service_principal" || (
      var.storage_credential_sp_client_secret != null &&
      var.storage_credential_sp_client_secret != ""
    )
    error_message = "storage_credential_sp_client_secret is required when storage_credential_auth_type=service_principal."
  }
}

variable "access_connector_name" {
  description = "Name of the Databricks access connector to create"
  type        = string
  default     = "kindling-access-connector"
}

variable "access_connector_resource_group_name" {
  description = "Resource group name for the Databricks access connector when create_access_connector=true"
  type        = string
  default     = null
  nullable    = true
}

variable "access_connector_location" {
  description = "Azure region for the Databricks access connector when create_access_connector=true"
  type        = string
  default     = null
  nullable    = true
}

variable "datalake_storage_account_resource_group_name" {
  description = "Resource group of the ADLS storage account (used for RBAC assignment lookup when datalake_storage_account_id is not set)"
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = (
      !(var.storage_credential_auth_type == "access_connector" && var.create_access_connector) ||
      var.datalake_storage_account_id != null ||
      (var.datalake_storage_account_resource_group_name != null &&
      var.datalake_storage_account_resource_group_name != "")
    )
    error_message = "When create_access_connector=true and datalake_storage_account_id is not set, datalake_storage_account_resource_group_name is required."
  }
}

variable "datalake_storage_account_id" {
  description = "Optional full resource ID of the ADLS storage account for RBAC scope"
  type        = string
  default     = null
  nullable    = true
}

variable "storage_credential_name" {
  description = "Name for the UC storage credential"
  type        = string
  default     = "datalake_managed_identity"
}

variable "storage_credential_sp_directory_id" {
  description = "Optional directory/tenant ID for storage credential service principal (defaults to azure_tenant_id)"
  type        = string
  default     = null
  nullable    = true
}

# -----------------------------------------------------------------------------
# Unity Catalog
# -----------------------------------------------------------------------------
variable "catalog_name" {
  description = "Name of the primary UC catalog"
  type        = string
  default     = "medallion"
}

variable "create_catalog" {
  description = "Whether Terraform should create the medallion catalog (false = use existing catalog_name)"
  type        = bool
  default     = true
}

variable "catalog_storage_container" {
  description = "ADLS container for catalog managed storage (typically artifacts_container_name)"
  type        = string
  default     = "artifacts"
}

variable "catalog_storage_path" {
  description = "Path within the container for medallion catalog managed storage"
  type        = string
  default     = "catalog"
}

variable "kindling_catalog_name" {
  description = "Name of the Kindling infrastructure catalog"
  type        = string
  default     = "kindling"
}

variable "create_kindling_catalog" {
  description = "Whether Terraform should create the kindling catalog (false = use existing kindling_catalog_name)"
  type        = bool
  default     = true
}

variable "kindling_catalog_storage_container" {
  description = "ADLS container for kindling catalog managed storage"
  type        = string
  default     = "artifacts"
}

variable "kindling_catalog_storage_path" {
  description = "Path within the container for kindling catalog managed storage"
  type        = string
  default     = "kindling/catalog"
}

variable "schemas" {
  description = "Schemas to create in the medallion catalog. Set storage_root to override managed location."
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

variable "create_base_schemas" {
  description = "Whether Terraform should create medallion schemas listed in var.schemas"
  type        = bool
  default     = true
}

variable "kindling_schema_name" {
  description = "Schema used to host Kindling UC volumes"
  type        = string
  default     = "kindling"
}

variable "kindling_artifacts_volume_name" {
  description = "External UC volume name that maps to artifacts container/path"
  type        = string
  default     = "artifacts"
}

variable "kindling_artifacts_subpath" {
  description = "Subdirectory inside artifacts container to back the Kindling artifacts volume"
  type        = string
  default     = "kindling/artifacts"
}

variable "managed_volumes" {
  description = "Additional managed volumes to create in {catalog}.{kindling_schema_name}"
  type        = list(string)
  default     = []
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

variable "create_runtime_service_principal" {
  description = "Whether Terraform should create a runtime Entra app + service principal and auto-wire it into Databricks grants"
  type        = bool
  default     = false
}

variable "runtime_service_principal_display_name" {
  description = "Display name for the runtime Entra app/service principal"
  type        = string
  default     = "kindling-runtime"
}

variable "runtime_service_principal_application_id" {
  description = "Existing runtime SP app/client ID (used when create_runtime_service_principal=false and you still want auto-wiring)"
  type        = string
  default     = null
  nullable    = true
}

variable "runtime_sp_principal_alias" {
  description = "Principal alias in tfvars grants that should be replaced with the effective runtime SP app/client ID"
  type        = string
  default     = "TODO_RUNTIME_SP_APP_ID"
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

variable "manage_system_group_memberships" {
  description = "Whether Terraform should manage memberships in Databricks system groups (admins/users)"
  type        = bool
  default     = true
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
    name          = string
    catalog       = optional(string, null) # defaults to var.catalog_name
    target_schema = string
    notebook_path = optional(string, null)
    file_glob     = optional(string, null)
    node_type_id  = optional(string, "Standard_DS3_v2")
    num_workers   = optional(number, 1)
    development   = optional(bool, true)
    continuous    = optional(bool, false)
    photon        = optional(bool, false)
    configuration = optional(map(string), {})
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
  description = "Grants on the medallion catalog"
  type = list(object({
    principal  = string
    privileges = list(string)
  }))
  default = []
}

variable "kindling_catalog_grants" {
  description = "Grants on the kindling catalog"
  type = list(object({
    principal  = string
    privileges = list(string)
  }))
  default = []
}

variable "schema_grants" {
  description = "Per-schema grants in the kindling catalog (schema_name + principal + privileges)"
  type = list(object({
    schema_name = string
    principal   = string
    privileges  = list(string)
  }))
  default = []
}

variable "medallion_schema_grants" {
  description = "Per-schema grants in the medallion catalog (schema_name + principal + privileges)"
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
  description = "Per-volume grants (volumes in catalog.kindling_schema_name)"
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
