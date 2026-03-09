# Azure Event Hub Terraform (System Tests)

Infrastructure as Code for Event Hub resources used by Kindling EventHubEntityProvider tests.

## What This Manages

- Event Hubs namespace
- Event Hub
- Event Hub authorization rule
- Optional non-default consumer group

## Quick Start

```bash
cd iac/azure/eventhub

# 1) Copy vars and fill values
cp terraform.tfvars.example eventhub.dev.tfvars

# 2) Init
terraform init

# 3) Plan
terraform plan -var-file=eventhub.dev.tfvars

# 4) Apply
terraform apply -var-file=eventhub.dev.tfvars
```

## Resolve Test Environment Variables

After apply, resolve/export Event Hub test env vars with:

```bash
python tests/system/eventhub_test_resource.py --iac-dir iac/azure/eventhub
```

The helper reads Terraform outputs and prints `export ...` commands for:

- `EVENTHUB_TEST_RESOURCE_GROUP`
- `EVENTHUB_TEST_NAMESPACE`
- `EVENTHUB_TEST_NAME`
- `EVENTHUB_TEST_AUTH_RULE`
- `EVENTHUB_TEST_CONSUMER_GROUP`
- `EVENTHUB_TEST_CONNECTION_STRING`

## Notes

- If `consumer_group_name = "$Default"`, Terraform does not create a consumer group resource.
- Keep Terraform state secure; output includes a sensitive connection string.
