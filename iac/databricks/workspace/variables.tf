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
# Feature Toggles
# -----------------------------------------------------------------------------
variable "workspace_role" {
  description = "Workspace operating model: platform or solution"
  type        = string
  default     = "solution"

  validation {
    condition     = contains(["platform", "solution"], var.workspace_role)
    error_message = "workspace_role must be one of: platform, solution."
  }
}

variable "enable_unity_catalog" {
  description = "Whether Unity Catalog resources (storage credential, external location, catalogs, schemas, volumes, grants) should be managed"
  type        = bool
  default     = true
}

variable "enable_kindling_artifacts" {
  description = "Whether to provision Kindling artifacts namespace and artifacts volume/mount"
  type        = bool
  default     = null
  nullable    = true
}

variable "enable_kindling_runtime_volumes" {
  description = "Whether to provision Kindling managed runtime volumes"
  type        = bool
  default     = null
  nullable    = true

  validation {
    condition     = var.enable_unity_catalog || !coalesce(var.enable_kindling_runtime_volumes, false)
    error_message = "enable_kindling_runtime_volumes can only be true when enable_unity_catalog=true."
  }
}

variable "enable_kindling_platform_support" {
  description = "Whether to enable Kindling platform/test scaffolding in this workspace"
  type        = bool
  default     = null
  nullable    = true
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
    condition = var.enable_unity_catalog && var.storage_credential_auth_type == "access_connector" ? (
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
    condition     = !var.enable_unity_catalog || !var.create_access_connector || var.azure_environment != "usgovernment"
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
    condition = !var.enable_unity_catalog || var.storage_credential_auth_type != "service_principal" || (
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
    condition = !var.enable_unity_catalog || var.storage_credential_auth_type != "service_principal" || (
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
      !var.enable_unity_catalog ||
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
  description = "Optional ADLS container for catalog managed storage. Leave null to let the metastore manage catalog storage."
  type        = string
  default     = null
  nullable    = true
}

variable "catalog_storage_path" {
  description = "Optional path within the container for primary catalog managed storage. Leave null to let the metastore manage catalog storage."
  type        = string
  default     = null
  nullable    = true
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
  description = "Optional ADLS container for Kindling catalog managed storage. Leave null to let the metastore manage catalog storage."
  type        = string
  default     = null
  nullable    = true
}

variable "kindling_catalog_storage_path" {
  description = "Optional path within the container for Kindling catalog managed storage. Leave null to let the metastore manage catalog storage."
  type        = string
  default     = null
  nullable    = true
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

variable "kindling_runtime_volume_catalog_name" {
  description = "Catalog used to host Kindling managed runtime volumes (defaults to kindling_catalog_name)"
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.enable_unity_catalog || (
      var.kindling_runtime_volume_catalog_name == null ||
      try(trimspace(var.kindling_runtime_volume_catalog_name), "") == ""
    )
    error_message = "kindling_runtime_volume_catalog_name is only valid when enable_unity_catalog=true."
  }
}

variable "kindling_runtime_volume_schema_name" {
  description = "Schema used to host Kindling managed runtime volumes (defaults to kindling_schema_name)"
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.enable_unity_catalog || (
      var.kindling_runtime_volume_schema_name == null ||
      try(trimspace(var.kindling_runtime_volume_schema_name), "") == ""
    )
    error_message = "kindling_runtime_volume_schema_name is only valid when enable_unity_catalog=true."
  }
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
  description = "Additional managed volumes to create in the managed runtime volume namespace"
  type        = list(string)
  default     = []

  validation {
    condition     = var.enable_unity_catalog || length(var.managed_volumes) == 0
    error_message = "managed_volumes can only be set when enable_unity_catalog=true."
  }
}

# -----------------------------------------------------------------------------
# DBFS Mounts (non-UC)
# -----------------------------------------------------------------------------
variable "create_artifacts_mount" {
  description = "Whether to create a DBFS mount for the artifacts container (only when enable_unity_catalog=false)"
  type        = bool
  default     = false

  validation {
    condition     = !var.enable_unity_catalog || !var.create_artifacts_mount
    error_message = "create_artifacts_mount must be false when enable_unity_catalog=true."
  }
}

variable "artifacts_mount_name" {
  description = "DBFS mount point name for artifacts (accessible at /mnt/<name>)"
  type        = string
  default     = "artifacts"
}

variable "mount_sp_application_id" {
  description = "Application (client) ID of the service principal used for DBFS mounts"
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.enable_unity_catalog || !var.create_artifacts_mount || (
      var.mount_sp_application_id != null &&
      var.mount_sp_application_id != ""
    )
    error_message = "mount_sp_application_id is required when create_artifacts_mount=true."
  }
}

variable "mount_sp_secret_scope" {
  description = "Databricks secret scope containing the mount service principal's client secret"
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.enable_unity_catalog || !var.create_artifacts_mount || (
      var.mount_sp_secret_scope != null &&
      var.mount_sp_secret_scope != ""
    )
    error_message = "mount_sp_secret_scope is required when create_artifacts_mount=true."
  }
}

variable "mount_sp_secret_key" {
  description = "Key within the secret scope that holds the mount service principal's client secret"
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.enable_unity_catalog || !var.create_artifacts_mount || (
      var.mount_sp_secret_key != null &&
      var.mount_sp_secret_key != ""
    )
    error_message = "mount_sp_secret_key is required when create_artifacts_mount=true."
  }
}

variable "mount_sp_secret_value" {
  description = "Optional client secret value to write into mount_sp_secret_scope/mount_sp_secret_key. Prefer passing via TF_VAR_mount_sp_secret_value from AZURE_CLIENT_SECRET."
  type        = string
  default     = null
  nullable    = true
  sensitive   = true
}

variable "dbfs_mounts" {
  description = "Additional DBFS mounts to create (only when enable_unity_catalog=false)"
  type = list(object({
    name            = string
    container_name  = string
    storage_account = optional(string, null) # defaults to datalake_storage_account
  }))
  default = []

  validation {
    condition     = !var.enable_unity_catalog || length(var.dbfs_mounts) == 0
    error_message = "dbfs_mounts can only be set when enable_unity_catalog=false."
  }
}

# -----------------------------------------------------------------------------
# Secret Scopes
# -----------------------------------------------------------------------------
variable "secret_scopes" {
  description = "Databricks-backed secret scopes to create"
  type        = list(string)
  default     = ["sepdev-adls-scope"]
}

variable "keyvault_secret_scopes" {
  description = "Key Vault-backed secret scopes to create (transparent to dbutils.secrets.get callers)"
  type = list(object({
    scope_name   = string
    keyvault_id  = string # Full Azure resource ID of the Key Vault
    keyvault_dns = string # DNS name of the Key Vault (e.g. https://my-vault.vault.azure.net/)
  }))
  default = []
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

variable "cluster_permissions" {
  description = "Per-cluster permissions. Supported principal_type values: service_principal, group, user."
  type = list(object({
    cluster_name     = string
    principal_type   = string
    principal        = string
    permission_level = string
  }))
  default = []

  validation {
    condition = alltrue([
      for cp in var.cluster_permissions : contains(["service_principal", "group", "user"], cp.principal_type)
    ])
    error_message = "cluster_permissions.principal_type must be one of: service_principal, group, user."
  }
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

  validation {
    condition     = var.enable_unity_catalog || length(var.catalog_grants) == 0
    error_message = "catalog_grants can only be set when enable_unity_catalog=true."
  }
}

variable "kindling_catalog_grants" {
  description = "Grants on the kindling catalog"
  type = list(object({
    principal  = string
    privileges = list(string)
  }))
  default = []

  validation {
    condition     = var.enable_unity_catalog || length(var.kindling_catalog_grants) == 0
    error_message = "kindling_catalog_grants can only be set when enable_unity_catalog=true."
  }
}

variable "schema_grants" {
  description = "Per-schema grants in the kindling catalog (schema_name + principal + privileges)"
  type = list(object({
    schema_name = string
    principal   = string
    privileges  = list(string)
  }))
  default = []

  validation {
    condition     = var.enable_unity_catalog || length(var.schema_grants) == 0
    error_message = "schema_grants can only be set when enable_unity_catalog=true."
  }
}

variable "medallion_schema_grants" {
  description = "Per-schema grants in the medallion catalog (schema_name + principal + privileges)"
  type = list(object({
    schema_name = string
    principal   = string
    privileges  = list(string)
  }))
  default = []

  validation {
    condition     = var.enable_unity_catalog || length(var.medallion_schema_grants) == 0
    error_message = "medallion_schema_grants can only be set when enable_unity_catalog=true."
  }
}

variable "external_location_grants" {
  description = "Per-external-location grants"
  type = list(object({
    location_name = string
    principal     = string
    privileges    = list(string)
  }))
  default = []

  validation {
    condition     = var.enable_unity_catalog || length(var.external_location_grants) == 0
    error_message = "external_location_grants can only be set when enable_unity_catalog=true."
  }
}

variable "volume_grants" {
  description = "Per-volume grants. catalog_name/schema_name are optional and default to the Kindling artifacts namespace."
  type = list(object({
    volume_name  = string
    principal    = string
    privileges   = list(string)
    catalog_name = optional(string, null)
    schema_name  = optional(string, null)
  }))
  default = []

  validation {
    condition     = var.enable_unity_catalog || length(var.volume_grants) == 0
    error_message = "volume_grants can only be set when enable_unity_catalog=true."
  }
}

variable "storage_credential_grants" {
  description = "Grants on the storage credential"
  type = list(object({
    principal  = string
    privileges = list(string)
  }))
  default = []

  validation {
    condition     = var.enable_unity_catalog || length(var.storage_credential_grants) == 0
    error_message = "storage_credential_grants can only be set when enable_unity_catalog=true."
  }
}

variable "any_file_grants" {
  description = "Grants on the Databricks ANY FILE securable"
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
