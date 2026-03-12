# Databricks Workspace Terraform

Infrastructure as Code for provisioning a Databricks workspace with all Kindling framework prerequisites.

## What This Manages

- **Unity Catalog (optional)**: Storage credential, single artifacts external location, medallion catalog + kindling catalog (optional create/reuse), schemas, volumes
- **Security**: Secret scopes, service principals, group memberships
- **Compute**: Interactive clusters
- **Cluster ACLs**: Optional per-cluster permissions for users, groups, and service principals
- **DLT Pipelines**: Delta Live Tables pipeline definitions
- **Workspace Config**: Feature flags and settings

## What This Does NOT Manage

- **Notebooks** — The `/sep/` notebook tree should be synced via Git or workspace export/import
- **Secret values** — Terraform creates scopes; add secrets via CLI/API
- **Job clusters** — Ephemeral clusters created by Spark job submissions
- **Tables/data** — Only the catalog/schema/volume structure, not data within

## Quick Start

```bash
cd iac/databricks/workspace

# 1. Copy the example vars and fill in your values
cp terraform.tfvars.example rrc.dev.tfvars
# Edit rrc.dev.tfvars with your workspace details

# 2. Initialize Terraform
terraform init

# 3. Preview changes
terraform plan -var-file=rrc.dev.tfvars

# 4. Apply
terraform apply -var-file=rrc.dev.tfvars
```

## Authentication

The Databricks provider uses Azure CLI auth by default. Ensure you're logged in:

```bash
az login
az account set --subscription <subscription-id>
```

For Azure US Gov, run `az cloud set --name AzureUSGovernment` before login and set `azure_environment = "usgovernment"` in your `.tfvars`.

For CI/CD, set environment variables:
```bash
export ARM_CLIENT_ID="..."
export ARM_CLIENT_SECRET="..."
export ARM_TENANT_ID="..."
```

## Access Connector & RBAC

- These settings are used only when `enable_unity_catalog = true`.
- Set `storage_credential_auth_type = "access_connector"` to use managed identity auth.
- Existing connector mode: set `create_access_connector = false` and provide `access_connector_id`.
- Managed mode: set `create_access_connector = true` and provide:
  - `access_connector_name`
  - `access_connector_resource_group_name`
  - `access_connector_location`
  - `datalake_storage_account_resource_group_name` (or `datalake_storage_account_id`)
- In managed mode, Terraform creates the Databricks access connector and assigns `Storage Blob Data Contributor` on the storage account.
- Set `storage_credential_auth_type = "service_principal"` to use `storage_credential_sp_application_id` + `storage_credential_sp_client_secret` instead of access connectors.
- Azure US Gov does not support creating Databricks Access Connectors via ARM. Use service principal storage credential mode there.
- Set `adls_dfs_domain = "dfs.core.usgovcloudapi.net"` when targeting Azure US Gov.

## Runtime Service Principal (Optional)

- Set `create_runtime_service_principal = true` to create a runtime Entra application/service principal.
- Use `runtime_sp_principal_alias` (default `TODO_RUNTIME_SP_APP_ID`) in grants and `sp_entitlements` to avoid hard-coding the app ID.
- If reusing an existing runtime SP, set `create_runtime_service_principal = false` and provide `runtime_service_principal_application_id`.
- If `runtime_service_principal_application_id` is not set, runtime principal alias substitution falls back to `storage_credential_sp_application_id`.

## Multiple Environments

Create separate `.tfvars` files per target workspace:

```bash
terraform apply -var-file=sep.dev.tfvars   # Existing SEP dev workspace
terraform apply -var-file=rrc.dev.tfvars   # RRC dev workspace
terraform apply -var-file=prod.tfvars      # Production
```

## Recommended Separation

Use two different Databricks workspaces when you need both strict UC coverage
and classic/path-based fallback coverage:

- `UC workspace`
  - `enable_unity_catalog = true`
  - use UC volumes / external locations
  - do not grant broad `ANY FILE MODIFY` to the runtime SP unless you
    intentionally want DBFS fallback to succeed there
- `Classic workspace`
  - `enable_unity_catalog = false`
  - use DBFS mounts and path-based staging
  - grant `ANY FILE` as needed for the runtime SP in that workspace only

This keeps the UC workspace strict so accidental path-based regressions still
fail, while the classic workspace validates fallback behavior deliberately.

The committed [`terraform.tfvars.classic.example`](terraform.tfvars.classic.example)
shows the intended non-UC/classic workspace shape.

## Unity Catalog Model

- If your workspace does not yet have a metastore assignment, run `../account` first to create/reuse a metastore and assign it.
- Set `enable_unity_catalog = false` to skip all Unity Catalog resources and grants when UC APIs are unavailable.
- When `enable_unity_catalog = false`, use DBFS mounts (`create_artifacts_mount` / `dbfs_mounts`) and avoid UC-only settings such as volume grants or runtime volume names.
- Set `workspace_role = "platform"` for an internal Kindling platform workspace and `workspace_role = "solution"` for a solution workspace that consumes Kindling.
- Use `enable_kindling_artifacts`, `enable_kindling_runtime_volumes`, and `enable_kindling_platform_support` to override the role-driven defaults explicitly.
- Use `cluster_permissions` to grant `CAN_ATTACH_TO` / `CAN_MANAGE` access on Terraform-created clusters. This is required when jobs run as a service principal that did not create the cluster.
- Use `any_file_grants` to grant Databricks `ANY FILE` permissions when runtime paths or connectors require direct file access. For classic/path-based testing, the Kindling run-as principal typically needs `SELECT` and `MODIFY`.
- One external location is managed for the artifacts container (`artifacts_external_location_name`).
- Medallion data schemas (for example bronze/silver/gold) live in `catalog_name`.
- Kindling artifacts live in `kindling_catalog_name.kindling_schema_name` by default.
- Managed runtime volumes can live either in that same namespace or in an environment-specific namespace via `kindling_runtime_volume_catalog_name` / `kindling_runtime_volume_schema_name`.
- Kindling assets are hosted in a configurable schema (`kindling_schema_name`) and external volume (`kindling_artifacts_volume_name`).
- The volume storage path is configurable via `kindling_artifacts_subpath`.
- Catalog `storage_root` is optional. Leave `catalog_storage_container/path` and `kindling_catalog_storage_container/path` unset to let the assigned UC metastore manage catalog storage. Only set them when you intentionally want explicit per-catalog managed locations.
- If you point managed runtime volumes at a different namespace, that target schema must already exist or be created elsewhere in the stack.
- Use the `runtime_volume_paths` and `kindling_runtime_volume_namespace` outputs to wire CI/runtime environment variables such as:
  - `KINDLING_DATABRICKS_RUNTIME_VOLUME_CATALOG`
  - `KINDLING_DATABRICKS_RUNTIME_VOLUME_SCHEMA`
  - `KINDLING_DATABRICKS_RUNTIME_TEMP_VOLUME`
- Set `create_catalog = false` to reuse an existing medallion catalog named by `catalog_name`.
- Set `create_kindling_catalog = false` to reuse an existing kindling catalog named by `kindling_catalog_name`.
- Set `create_base_schemas = false` to skip creating bronze/silver/gold schemas in existing catalogs.

## DLT Pipelines Note

DLT pipelines reference notebook paths in the workspace. Deploy notebooks **before** applying pipeline resources, or comment out `dlt_pipelines` in your `.tfvars` for the first apply:

```hcl
dlt_pipelines = []  # First pass: skip pipelines
```

Then uncomment and re-apply after notebooks are in place.

## File Structure

```
├── main.tf              # Provider configuration
├── versions.tf          # Required provider versions
├── variables.tf         # All variable declarations
├── unity_catalog.tf     # Storage credential, locations, catalog, schemas, volumes
├── security.tf          # Secret scopes, service principals, groups
├── compute.tf           # Interactive clusters
├── pipelines.tf         # DLT pipeline definitions
├── workspace_conf.tf    # Workspace-level settings
├── outputs.tf           # Useful output values
├── terraform.tfvars.example   # Example variable file (committed, no real values)
└── README.md            # This file
```
