# Databricks Workspace Terraform

Infrastructure as Code for provisioning a Databricks workspace with all Kindling framework prerequisites.

## What This Manages

- **Unity Catalog**: Storage credential, external locations, catalog, schemas, volumes
- **Security**: Secret scopes, service principals, group memberships
- **Compute**: Interactive clusters
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
cp terraform.tfvars.example dev.tfvars
# Edit dev.tfvars with your workspace details

# 2. Initialize Terraform
terraform init

# 3. Preview changes
terraform plan -var-file=dev.tfvars

# 4. Apply
terraform apply -var-file=dev.tfvars
```

## Authentication

The Databricks provider uses Azure CLI auth by default. Ensure you're logged in:

```bash
az login
az account set --subscription <subscription-id>
```

For CI/CD, set environment variables:
```bash
export ARM_CLIENT_ID="..."
export ARM_CLIENT_SECRET="..."
export ARM_TENANT_ID="..."
```

## Multiple Environments

Create separate `.tfvars` files per target workspace:

```bash
terraform apply -var-file=dev.tfvars       # Original dev workspace
terraform apply -var-file=newdev.tfvars    # New workspace
terraform apply -var-file=prod.tfvars      # Production
```

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
