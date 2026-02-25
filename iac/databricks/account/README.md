# Databricks Account Terraform (Unity Catalog Metastore)

Account-level Terraform for Unity Catalog bootstrap:

- Create a metastore, or reuse an existing metastore.
- Assign that metastore to one or more Databricks workspaces.

This module is intended to run before `iac/databricks/workspace` when Unity Catalog is not yet available in a workspace.

## What This Manages

- `databricks_metastore`
- `databricks_metastore_assignment`

## Authentication

Use Databricks account-level credentials (account admin scope).

For Azure CLI auth:

```bash
az login
```

For service principal auth:

```bash
export ARM_CLIENT_ID="..."
export ARM_CLIENT_SECRET="..."
export ARM_TENANT_ID="..."
```

## Quick Start

```bash
cd iac/databricks/account

# 1) Prepare variables
cp terraform.tfvars.example account.dev.tfvars
# Edit account.dev.tfvars

# 2) Init + plan + apply
terraform init
terraform plan -var-file=account.dev.tfvars -out=tfplan
terraform apply tfplan
```

## Variables Overview

- `create_metastore=true`: Requires `metastore_name`, `metastore_storage_root`, `metastore_region`.
- `create_metastore=false`: Requires `existing_metastore_id`.
- `assign_workspaces=true`: Requires at least one entry in `workspace_ids`.

## Typical Flow With Workspace Module

1. Run this account module to create/reuse a metastore and assign it to your workspace ID(s).
2. In `iac/databricks/workspace/rrc.dev.tfvars`, set `enable_unity_catalog = true`.
3. Run workspace module `terraform plan/apply`.
