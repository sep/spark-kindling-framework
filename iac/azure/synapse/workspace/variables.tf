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
# Resource Group & Location
# -----------------------------------------------------------------------------
variable "resource_group_name" {
  description = "Name of the resource group for all Synapse resources"
  type        = string
}

variable "location" {
  description = "Azure region for all Synapse resources"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

# -----------------------------------------------------------------------------
# Feature Toggles
# -----------------------------------------------------------------------------
variable "enable_spark_pools" {
  description = "Whether to create Apache Spark pools"
  type        = bool
  default     = true
}

variable "enable_sql_pools" {
  description = "Whether to create dedicated SQL pools"
  type        = bool
  default     = false
}

variable "enable_managed_vnet" {
  description = "Whether to enable managed virtual network for the workspace"
  type        = bool
  default     = true
}

variable "enable_data_exfiltration_protection" {
  description = "Whether to enable data exfiltration protection (requires managed_vnet)"
  type        = bool
  default     = false

  validation {
    condition     = !var.enable_data_exfiltration_protection || var.enable_managed_vnet
    error_message = "enable_data_exfiltration_protection requires enable_managed_vnet=true."
  }
}

variable "enable_managed_private_endpoints" {
  description = "Whether to create managed private endpoints for linked data sources"
  type        = bool
  default     = false

  validation {
    condition     = !var.enable_managed_private_endpoints || var.enable_managed_vnet
    error_message = "enable_managed_private_endpoints requires enable_managed_vnet=true."
  }
}

# -----------------------------------------------------------------------------
# Storage / Data Lake
# -----------------------------------------------------------------------------
variable "datalake_storage_account_id" {
  description = "Full resource ID of the ADLS Gen2 storage account for the workspace"
  type        = string
}

variable "datalake_filesystem_name" {
  description = "Name of the ADLS Gen2 filesystem (container) used as the workspace default"
  type        = string
  default     = "synapse"
}

variable "create_datalake_filesystem" {
  description = "Whether Terraform should create the default ADLS Gen2 filesystem"
  type        = bool
  default     = true
}

variable "additional_storage_accounts" {
  description = "Additional storage accounts to grant the workspace managed identity access to"
  type = list(object({
    storage_account_id = string
    role               = optional(string, "Storage Blob Data Contributor")
  }))
  default = []
}

# -----------------------------------------------------------------------------
# Synapse Workspace
# -----------------------------------------------------------------------------
variable "workspace_name" {
  description = "Name of the Synapse workspace"
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,49}$", var.workspace_name))
    error_message = "workspace_name must be 1-50 characters, start with a letter, and contain only lowercase letters, numbers, and hyphens."
  }
}

variable "sql_administrator_login" {
  description = "SQL administrator login name for the workspace"
  type        = string
  default     = "sqladmin"
}

variable "sql_administrator_login_password" {
  description = "SQL administrator password for the workspace"
  type        = string
  sensitive   = true
}

variable "managed_resource_group_name" {
  description = "Name for the managed resource group created by Synapse (defaults to workspace_name-managed-rg)"
  type        = string
  default     = null
  nullable    = true
}

variable "purview_id" {
  description = "Resource ID of an Azure Purview account to link to the workspace"
  type        = string
  default     = null
  nullable    = true
}

variable "compute_subnet_id" {
  description = "Subnet ID for the workspace compute resources (optional)"
  type        = string
  default     = null
  nullable    = true
}

variable "public_network_access_enabled" {
  description = "Whether public network access is enabled for the workspace"
  type        = bool
  default     = true
}

variable "linking_allowed_for_aad_tenant_ids" {
  description = "List of AAD tenant IDs allowed for linking (empty = same tenant only)"
  type        = list(string)
  default     = []
}

variable "workspace_identity_type" {
  description = "Identity type for the Synapse workspace"
  type        = string
  default     = "SystemAssigned"

  validation {
    condition     = contains(["SystemAssigned", "SystemAssigned,UserAssigned"], var.workspace_identity_type)
    error_message = "workspace_identity_type must be SystemAssigned or SystemAssigned,UserAssigned."
  }
}

variable "user_assigned_identity_ids" {
  description = "List of user-assigned identity resource IDs to attach to the workspace (when identity type includes UserAssigned)"
  type        = list(string)
  default     = []
}

# -----------------------------------------------------------------------------
# Spark Pools
# -----------------------------------------------------------------------------
variable "spark_pools" {
  description = "Apache Spark pools to create in the workspace"
  type = list(object({
    name                                = string
    node_size_family                    = optional(string, "MemoryOptimized")
    node_size                           = optional(string, "Small")
    spark_version                       = optional(string, "3.4")
    node_count                          = optional(number, null) # fixed size (mutually exclusive with autoscale)
    autoscale_min_node_count            = optional(number, 3)
    autoscale_max_node_count            = optional(number, 10)
    auto_pause_delay_in_minutes         = optional(number, 15)
    cache_size                          = optional(number, null)
    dynamic_executor_allocation_enabled = optional(bool, false)
    min_executors                       = optional(number, null)
    max_executors                       = optional(number, null)
    session_level_packages_enabled      = optional(bool, true)
    spark_config_content                = optional(string, null)
    spark_log_folder                    = optional(string, "/logs")
    spark_events_folder                 = optional(string, "/events")
    custom_tags                         = optional(map(string), {})
  }))
  default = []

  validation {
    condition = alltrue([
      for p in var.spark_pools : can(regex("^[a-zA-Z][a-zA-Z0-9]{0,14}$", p.name))
    ])
    error_message = "Spark pool names must be 1-15 characters, start with a letter, and contain only letters and numbers."
  }

  validation {
    condition = alltrue([
      for p in var.spark_pools : contains(["Small", "Medium", "Large", "XLarge", "XXLarge", "XXXLarge"], p.node_size)
    ])
    error_message = "Spark pool node_size must be one of: Small, Medium, Large, XLarge, XXLarge, XXXLarge."
  }
}

variable "spark_pool_library_requirements" {
  description = "Per-pool library requirements (requirements.txt content)"
  type = list(object({
    pool_name = string
    content   = string
    filename  = optional(string, "requirements.txt")
  }))
  default = []
}

# -----------------------------------------------------------------------------
# SQL Pools
# -----------------------------------------------------------------------------
variable "sql_pools" {
  description = "Dedicated SQL pools to create in the workspace"
  type = list(object({
    name                      = string
    sku_name                  = optional(string, "DW100c")
    create_mode               = optional(string, "Default")
    collation                 = optional(string, "SQL_Latin1_General_CP1_CI_AS")
    storage_account_type      = optional(string, "GRS")
    geo_backup_policy_enabled = optional(bool, true)
    data_encrypted            = optional(bool, true)
    tags                      = optional(map(string), {})
  }))
  default = []

}

# -----------------------------------------------------------------------------
# Firewall Rules
# -----------------------------------------------------------------------------
variable "firewall_rules" {
  description = "IP firewall rules for the workspace"
  type = list(object({
    name             = string
    start_ip_address = string
    end_ip_address   = string
  }))
  default = []
}

variable "allow_azure_services" {
  description = "Whether to allow Azure services to access the workspace (creates AllowAllWindowsAzureIps rule)"
  type        = bool
  default     = true
}

# -----------------------------------------------------------------------------
# Managed Private Endpoints
# -----------------------------------------------------------------------------
variable "managed_private_endpoints" {
  description = "Managed private endpoints to create within the workspace managed VNet"
  type = list(object({
    name               = string
    target_resource_id = string
    subresource_name   = string
  }))
  default = []

}

# -----------------------------------------------------------------------------
# Users & Groups
# -----------------------------------------------------------------------------
variable "admin_users" {
  description = "Users to grant Synapse Administrator role (email + AAD object ID)"
  type = list(object({
    email     = string
    object_id = string
  }))
  default = []
}

variable "workspace_users" {
  description = "Users to grant Synapse User role (email + AAD object ID)"
  type = list(object({
    email     = string
    object_id = string
  }))
  default = []
}

variable "workspace_contributors" {
  description = "Users to grant Synapse Contributor role (email + AAD object ID)"
  type = list(object({
    email     = string
    object_id = string
  }))
  default = []
}

# -----------------------------------------------------------------------------
# Role Assignments
# -----------------------------------------------------------------------------
variable "synapse_role_assignments" {
  description = "Synapse RBAC role assignments within the workspace"
  type = list(object({
    role_name    = string
    principal_id = string
  }))
  default = []

  validation {
    condition = alltrue([
      for ra in var.synapse_role_assignments : contains([
        "Synapse Administrator",
        "Synapse Contributor",
        "Synapse Artifact Publisher",
        "Synapse Artifact User",
        "Synapse Compute Operator",
        "Synapse Credential User",
        "Synapse Linked Data Manager",
        "Synapse Monitoring Operator",
        "Synapse SQL Administrator",
        "Synapse User",
        "Apache Spark Administrator",
      ], ra.role_name)
    ])
    error_message = "synapse_role_assignments.role_name must be a valid Synapse RBAC role."
  }
}

variable "azure_role_assignments" {
  description = "Azure RBAC role assignments on the workspace resource"
  type = list(object({
    role_definition_name = string
    principal_id         = string
  }))
  default = []
}

# -----------------------------------------------------------------------------
# Linked Services
# -----------------------------------------------------------------------------
variable "linked_storage_accounts" {
  description = "ADLS Gen2 storage accounts to create linked services for (e.g. delta lake storage). Also grants the workspace managed identity the specified role on each account."
  type = list(object({
    name                = string # linked service name
    storage_account_id  = string # full resource ID
    storage_account_url = string # e.g. https://mystorageaccount.dfs.core.usgovcloudapi.net
    role                = optional(string, "Storage Blob Data Contributor")
  }))
  default = []
}

variable "linked_services_keyvault" {
  description = "Azure Key Vault linked services to create"
  type = list(object({
    name        = string
    keyvault_id = string
  }))
  default = []
}

# -----------------------------------------------------------------------------
# Git Integration
# -----------------------------------------------------------------------------
variable "git_repo" {
  description = "Git repository integration for the workspace"
  type = object({
    account_name    = string
    repository_name = string
    branch_name     = optional(string, "main")
    root_folder     = optional(string, "/")
    type            = optional(string, "AzureDevOpsGit") # AzureDevOpsGit or GitHub
    project_name    = optional(string, null)             # required for AzureDevOpsGit
    tenant_id       = optional(string, null)             # defaults to azure_tenant_id
  })
  default  = null
  nullable = true
}

# -----------------------------------------------------------------------------
# Workspace Admin AAD Group
# -----------------------------------------------------------------------------
variable "workspace_aad_admin" {
  description = "Azure AD admin for the workspace (group or user)"
  type = object({
    login     = string                 # Display name
    object_id = string                 # AAD object ID
    tenant_id = optional(string, null) # defaults to azure_tenant_id
  })
  default  = null
  nullable = true
}
