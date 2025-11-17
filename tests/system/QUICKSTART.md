# Quick Start: Running System Tests Locally

This guide shows how to run Fabric system tests on your local machine using `az login`.

## Prerequisites

- Azure CLI installed: `az --version`
- Python 3.10+ with pytest installed
- Access to a Microsoft Fabric workspace with a lakehouse

## Step 1: Install Azure CLI

If you don't have Azure CLI installed:

```bash
# macOS
brew install azure-cli

# Windows
winget install Microsoft.AzureCLI

# Linux
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

## Step 2: Login to Azure

```bash
az login
```

This will open your browser for authentication. After logging in, you'll see a list of your subscriptions.

## Step 3: Set Fabric Configuration

You only need to set the workspace and lakehouse IDs:

```bash
# Find your workspace ID from the Fabric portal URL:
# https://app.fabric.microsoft.com/groups/<workspace-id>/...
export FABRIC_WORKSPACE_ID="12345678-1234-1234-1234-123456789abc"

# Find your lakehouse ID from the lakehouse URL:
# https://app.fabric.microsoft.com/groups/<workspace-id>/lakehouses/<lakehouse-id>
export FABRIC_LAKEHOUSE_ID="87654321-4321-4321-4321-cba987654321"
```

**Tip**: Add these to your `~/.bashrc` or `~/.zshrc` to make them persistent.

## Step 4: Run Tests

```bash
# Run all Fabric tests
pytest tests/system/ -v -s -m fabric

# Run a specific test
pytest tests/system/test_fabric_job_deployment.py::TestFabricJobDeployment::test_deploy_app_as_job -v -s
```

## How It Works

The test framework uses Azure's `DefaultAzureCredential` which tries authentication methods in this order:

1. **Environment Variables** - Service principal credentials (CI/CD)
2. **Managed Identity** - If running in Azure (VM, Container, etc.)
3. **Azure CLI** - Your `az login` session ← This is what we're using!
4. **Visual Studio Code** - If you have Azure extension
5. **Azure PowerShell** - If you use PowerShell
6. **Interactive Browser** - As a last resort

When you run `az login`, the CLI caches your credentials locally, and `DefaultAzureCredential` automatically uses them.

## Troubleshooting

### "Missing Fabric configuration" error

Make sure you've set both environment variables:
```bash
echo $FABRIC_WORKSPACE_ID
echo $FABRIC_LAKEHOUSE_ID
```

If empty, export them again.

### "Authentication failed" error

Your `az login` session might have expired. Re-authenticate:
```bash
az login --use-device-code  # Alternative login method
```

Or verify your account has access to the workspace:
```bash
az account show
```

### "No subscriptions found" error

Your Azure account might not have access to the subscription. Contact your Azure admin.

### Tests are skipped

If you see:
```
SKIPPED (Missing Fabric configuration: FABRIC_WORKSPACE_ID, FABRIC_LAKEHOUSE_ID)
```

You need to export the environment variables (see Step 3).

## Using a .env File (Optional)

Instead of exporting variables every time, create a `.env` file:

```bash
# Copy the example
cp tests/system/.env.example tests/system/.env

# Edit with your IDs
nano tests/system/.env
```

Then load it before running tests:
```bash
source tests/system/.env
pytest tests/system/ -v -s -m fabric
```

Or use a tool like `python-dotenv`:
```bash
pip install python-dotenv
```

## CI/CD Setup

For automated testing in GitHub Actions or other CI/CD, use service principal authentication:

```yaml
env:
  FABRIC_WORKSPACE_ID: ${{ secrets.FABRIC_WORKSPACE_ID }}
  FABRIC_LAKEHOUSE_ID: ${{ secrets.FABRIC_LAKEHOUSE_ID }}
  AZURE_TENANT_ID: ${{ secrets.AZURE_TENANT_ID }}
  AZURE_CLIENT_ID: ${{ secrets.AZURE_CLIENT_ID }}
  AZURE_CLIENT_SECRET: ${{ secrets.AZURE_CLIENT_SECRET }}
```

This way, CI/CD uses service principal (Option B) while developers use `az login` (Option A).

## Next Steps

- Read the full [System Tests README](README.md)
- Review the [Job Deployment Documentation](../../docs/job_deployment.md)
- Check out the [test app source](apps/fabric-job-test/main.py)

## Questions?

- **What if I don't have Fabric access?** - Tests will skip automatically
- **Can I use my personal account?** - Yes, if you have access to a Fabric workspace
- **Do I need a service principal?** - No, not for local testing with `az login`
- **Will this work with Databricks/Synapse?** - Yes, same pattern applies
