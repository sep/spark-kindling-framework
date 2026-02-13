# Authentication Setup for System Tests

## TL;DR

**For local development:**
```bash
az login
export FABRIC_WORKSPACE_ID="..."
export FABRIC_LAKEHOUSE_ID="..."
pytest tests/system/ -v -s -m fabric
```

**For CI/CD:**
```bash
export FABRIC_WORKSPACE_ID="..."
export FABRIC_LAKEHOUSE_ID="..."
export AZURE_TENANT_ID="..."
export AZURE_CLIENT_ID="..."
export AZURE_CLIENT_SECRET="..."
pytest tests/system/ -v -s -m fabric
```

## How Authentication Works

The system tests use Azure's `DefaultAzureCredential` which tries multiple authentication methods automatically:

### Priority Order (first success wins):

1. **Environment Variables** (Service Principal)
   - `AZURE_TENANT_ID`
   - `AZURE_CLIENT_ID`
   - `AZURE_CLIENT_SECRET`

2. **Managed Identity** (if running in Azure)
   - Azure VM
   - Azure Container Instances
   - Azure Functions

3. **Azure CLI** (`az login`)
   - Your local authenticated session
   - **This is what most developers use**

4. **Visual Studio Code**
   - Azure Account extension

5. **Azure PowerShell**
   - If you use PowerShell

6. **Interactive Browser**
   - As a last resort

## Required Configuration

### Always Required

```bash
export FABRIC_WORKSPACE_ID="your-workspace-id-here"
export FABRIC_LAKEHOUSE_ID="your-lakehouse-id-here"
```

**How to find these:**
- Workspace ID: In Fabric portal URL: `https://app.fabric.microsoft.com/groups/<workspace-id>/...`
- Lakehouse ID: In lakehouse URL: `.../lakehouses/<lakehouse-id>`

### Optional (only for service principal auth)

```bash
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
```

**When to use service principal:**
- CI/CD pipelines (GitHub Actions, Azure DevOps)
- Automated testing environments
- Service accounts

## Testing Your Setup

Run the authentication test script:

```bash
python tests/system/auth_check.py
```

This will:
- ✅ Test if authentication is working
- ✅ Show which auth method is being used
- ✅ Verify Fabric configuration is set
- ✅ Provide troubleshooting guidance

Example output:
```
============================================================
Azure Authentication Test
============================================================

📋 No service principal credentials found
   Will try DefaultAzureCredential (az login, managed identity, etc.)

🔐 Testing DefaultAzureCredential...
   This will try multiple auth methods in order:
   1. Environment variables (service principal)
   2. Managed identity
   3. Azure CLI (az login)
   4. Visual Studio Code
   5. Azure PowerShell
   6. Interactive browser

✅ DefaultAzureCredential authentication successful!
   Token expires: 1740239999

💡 Authentication method used:
   → Likely Azure CLI (az login)
   → Run 'az account show' to verify

============================================================
Fabric Configuration Test
============================================================

✅ Fabric configuration found
   Workspace ID: 12345678-1234-1234-1234-123456789abc
   Lakehouse ID: 87654321-4321-4321-4321-cba987654321

============================================================
Summary
============================================================
✅ All checks passed! You're ready to run system tests.

Run tests with:
   pytest tests/system/ -v -s -m fabric
```

## Common Issues

### Issue: "Missing Fabric configuration"

**Cause:** Environment variables not set

**Fix:**
```bash
export FABRIC_WORKSPACE_ID="your-workspace-id"
export FABRIC_LAKEHOUSE_ID="your-lakehouse-id"
```

### Issue: "Authentication failed"

**Cause:** No valid credentials found

**Fix (local):**
```bash
az login
```

**Fix (CI/CD):**
```bash
export AZURE_TENANT_ID="..."
export AZURE_CLIENT_ID="..."
export AZURE_CLIENT_SECRET="..."
```

### Issue: "No subscriptions found"

**Cause:** Account doesn't have access

**Fix:** Contact your Azure admin to grant access

### Issue: Tests are skipped

**This is normal!** Tests skip automatically when:
- Required environment variables are missing
- Authentication is not configured
- Platform is not available

Set up the required configuration and tests will run.

## Using .env Files

Instead of exporting variables manually:

1. Copy the example:
   ```bash
   cp tests/system/.env.example tests/system/.env
   ```

2. Edit with your values:
   ```bash
   nano tests/system/.env
   ```

3. Load before running tests:
   ```bash
   source tests/system/.env
   pytest tests/system/ -v -s -m fabric
   ```

**Important:** Never commit `.env` with real credentials!

## CI/CD Examples

### GitHub Actions

```yaml
name: System Tests

on: [push, pull_request]

jobs:
  fabric-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - run: pip install -r requirements.txt

      - name: Run Fabric System Tests
        run: pytest tests/system/ -v -m fabric
        env:
          FABRIC_WORKSPACE_ID: ${{ secrets.FABRIC_WORKSPACE_ID }}
          FABRIC_LAKEHOUSE_ID: ${{ secrets.FABRIC_LAKEHOUSE_ID }}
          AZURE_TENANT_ID: ${{ secrets.AZURE_TENANT_ID }}
          AZURE_CLIENT_ID: ${{ secrets.AZURE_CLIENT_ID }}
          AZURE_CLIENT_SECRET: ${{ secrets.AZURE_CLIENT_SECRET }}
```

### Azure DevOps

```yaml
trigger:
  - main

pool:
  vmImage: 'ubuntu-latest'

steps:
- task: UsePythonVersion@0
  inputs:
    versionSpec: '3.10'

- script: pip install -r requirements.txt
  displayName: 'Install dependencies'

- script: pytest tests/system/ -v -m fabric
  displayName: 'Run system tests'
  env:
    FABRIC_WORKSPACE_ID: $(FABRIC_WORKSPACE_ID)
    FABRIC_LAKEHOUSE_ID: $(FABRIC_LAKEHOUSE_ID)
    AZURE_TENANT_ID: $(AZURE_TENANT_ID)
    AZURE_CLIENT_ID: $(AZURE_CLIENT_ID)
    AZURE_CLIENT_SECRET: $(AZURE_CLIENT_SECRET)
```

## Security Best Practices

1. **Never commit credentials** to git
   - Add `.env` to `.gitignore`
   - Use secrets management in CI/CD

2. **Use service principals for automation**
   - Create dedicated service principal for testing
   - Grant minimum required permissions
   - Rotate secrets regularly

3. **Use `az login` for local development**
   - Leverages your existing Azure account
   - No need to store credentials locally
   - Easier to manage and rotate

4. **Scope permissions appropriately**
   - Service principal needs:
     - Fabric workspace contributor
     - Lakehouse read/write access
   - Nothing more

## Related Documentation

- [Quick Start Guide](QUICKSTART.md) - Step-by-step setup
- [System Tests README](README.md) - Complete testing guide
- [Job Deployment Docs](../../docs/job_deployment.md) - Feature documentation

## Questions?

Run the test script for diagnostics:
```bash
python tests/system/auth_check.py
```

Or check individual components:
```bash
# Verify Azure CLI is logged in
az account show

# Check environment variables
env | grep -E "(FABRIC|AZURE)"

# Test pytest collection (should show tests, not errors)
pytest tests/system/ --collect-only -m fabric
```
