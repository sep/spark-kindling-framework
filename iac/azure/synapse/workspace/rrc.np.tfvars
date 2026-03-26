# =============================================================================
# Synapse Workspace Terraform Variables — RRC Nonprod
# =============================================================================
# Usage:
#   terraform workspace select rrc-np
#   terraform plan  -var-file=rrc.np.tfvars
#   terraform apply -var-file=rrc.np.tfvars
# =============================================================================

# -- Azure --------------------------------------------------------------------
azure_subscription_id = "71a1b53b-91a3-455f-b6c0-47cb99180ae9"
azure_tenant_id       = "9e9318c5-2072-4540-8574-5bfe74254515"
azure_environment     = "usgovernment"

# -- Resource Group & Location ------------------------------------------------
resource_group_name = "rr-def-cfe-sf-rg-fawkes-dev"
location            = "usgovvirginia"
environment         = "nonprod"

tags = {
  project     = "fawkes"
  environment = "nonprod"
  team        = "rrc-data-engineering"
}

# -- Feature Toggles ----------------------------------------------------------
enable_spark_pools                  = true
enable_sql_pools                    = false
enable_managed_vnet                 = false
enable_data_exfiltration_protection = false
enable_managed_private_endpoints    = false

# -- Storage / Data Lake ------------------------------------------------------
datalake_storage_account_id = "/subscriptions/71a1b53b-91a3-455f-b6c0-47cb99180ae9/resourceGroups/rr-def-cfe-sf-rg-fawkes-dev/providers/Microsoft.Storage/storageAccounts/rrdefcfesffwksshr" # TODO confirm storage account name
datalake_filesystem_name    = "synapse"
create_datalake_filesystem  = true

# -- Synapse Workspace --------------------------------------------------------
workspace_name          = "rr-def-cfe-softwarefactory-syn-np"
sql_administrator_login = "sqladmin"
# sql_administrator_login_password sourced from TF_VAR_sql_administrator_login_password
managed_resource_group_name   = "rr-def-cfe-sf-rg-synapse-managed-np"
public_network_access_enabled = true

# -- Workspace AAD Admin ------------------------------------------------------
workspace_aad_admin = {
  login     = "jtdossett@rollsroycegov.com"
  object_id = "19dd919c-60e8-47b2-b006-ef79caabf556"
}

# -- Users & Groups -----------------------------------------------------------
admin_users = [] # jtdossett already granted admin via workspace_aad_admin

workspace_contributors = [
  { email = "djgomez@rollsroycegov.com", object_id = "32825cba-d054-44e7-83e7-6e8cd1a0725c" },
]

workspace_users = []

# -- Spark Pools --------------------------------------------------------------
spark_pools = [
  {
    name                        = "fawkesdefault"
    node_size_family            = "MemoryOptimized"
    node_size                   = "Small"
    spark_version               = "3.4"
    autoscale_min_node_count    = 3
    autoscale_max_node_count    = 10
    auto_pause_delay_in_minutes = 15
  },
]

# spark_pool_library_requirements = [
#   {
#     pool_name = "fawkesdefault"
#     content   = "pyarrow==14.0.0\npandas==2.1.0\n"
#   },
# ]

# -- SQL Pools ----------------------------------------------------------------
sql_pools = []

# -- Firewall Rules -----------------------------------------------------------
allow_azure_services = true

firewall_rules = [
  {
    name             = "allow-terraform-runner"
    start_ip_address = "216.200.231.178"
    end_ip_address   = "216.200.231.178"
  },
]

# -- Role Assignments ---------------------------------------------------------
synapse_role_assignments = [
  # {
  #   role_name    = "Synapse Administrator"
  #   principal_id = "TODO_AAD_GROUP_OBJECT_ID"
  # },
]

azure_role_assignments = []

# -- Linked Services ----------------------------------------------------------
linked_storage_accounts = [
  {
    name                = "fawkes-datalake"
    storage_account_id  = "/subscriptions/71a1b53b-91a3-455f-b6c0-47cb99180ae9/resourceGroups/rr-def-cfe-sf-rg-fawkes-dev/providers/Microsoft.Storage/storageAccounts/rrdefcfesffwksshr"
    storage_account_url = "https://rrdefcfesffwksshr.dfs.core.usgovcloudapi.net"
  },
]

linked_services_keyvault = [
  # {
  #   name        = "fawkes-kv-linked"
  #   keyvault_id = "/subscriptions/71a1b53b-91a3-455f-b6c0-47cb99180ae9/resourceGroups/rr-def-cfe-sf-rg-fawkes-dev/providers/Microsoft.KeyVault/vaults/rr-def-cfe-sf-kv-fwk-np"
  # },
]

# -- Git Integration ----------------------------------------------------------
# git_repo = {
#   account_name    = "TODO_ADO_ORG"
#   project_name    = "TODO_ADO_PROJECT"
#   repository_name = "synapse-artifacts"
#   branch_name     = "main"
#   root_folder     = "/synapse"
#   type            = "AzureDevOpsGit"
# }
