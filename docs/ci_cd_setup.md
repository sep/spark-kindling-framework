# CI/CD Pipeline Setup Guide

This guide explains how to set up and use the Kindling CI/CD pipeline.

## 🎯 Overview

The CI/CD pipeline provides automated:
- **Build**: Poetry-based platform wheel building
- **Test**: Unit, integration, KDA packaging, and system tests
- **Quality**: Code formatting, linting, type checking, and security scanning
- **Release**: Build artifacts ready for distribution

## 📋 Pipeline Jobs

### Core Jobs (Run on Every Push/PR)

1. **Unit Tests** - Fast tests of individual components
2. **Code Quality** - Black, pylint, mypy checks
3. **Build Platform Wheels** - Creates platform-specific wheels

### Extended Jobs (Run on main/develop)

4. **Integration Tests** - Azure storage integration (requires credentials)
5. **KDA Packaging Tests** - Validates KDA packaging system
6. **System Tests** - End-to-end platform testing (synapse, databricks, fabric, local)
7. **Security Scan** - Vulnerability and security analysis

### Summary Job

8. **Test Summary** - Aggregates results from all jobs

## 🔐 Required GitHub Secrets

Configure these in: **Repository Settings** → **Secrets and variables** → **Actions**

### Secrets (Sensitive)
```
AZURE_STORAGE_KEY_TEST     - Storage account key for testing
AZURE_STORAGE_KEY_STAGING  - Storage account key for staging (optional)
```

### Variables (Non-sensitive)
```
TEST_STORAGE_ACCOUNT       - Azure storage account name for tests
TEST_CONTAINER             - Azure container name for tests
STAGING_STORAGE_ACCOUNT    - Azure storage account name for staging (optional)
STAGING_CONTAINER          - Azure container name for staging (optional)
```

## 🔑 Azure Storage Authentication Setup

### Get Storage Account Key

#### Option 1: Azure Portal
1. Navigate to your Storage Account
2. Go to **Security + networking** → **Access keys**
3. Copy **key1** or **key2** value

#### Option 2: Azure CLI
```bash
# Get storage account key
az storage account keys list \
  --account-name YOUR-STORAGE-ACCOUNT \
  --resource-group YOUR-RESOURCE-GROUP \
  --query '[0].value' -o tsv
```

### Add to GitHub Secrets

1. Go to **Repository Settings** → **Secrets and variables** → **Actions**
2. Click **New repository secret**
3. Add secrets:
   - Name: `AZURE_STORAGE_KEY_TEST`
   - Value: (paste your storage account key)
   - Name: `AZURE_STORAGE_KEY_STAGING` (optional, for system tests)
   - Value: (paste your staging storage account key)

### Add to GitHub Variables

1. Go to **Repository Settings** → **Secrets and variables** → **Actions** → **Variables** tab
2. Click **New repository variable**
3. Add variables:
   - `TEST_STORAGE_ACCOUNT`: Your storage account name (e.g., "mystorageacct")
   - `TEST_CONTAINER`: Your container name (e.g., "kindling-tests")
   - `STAGING_STORAGE_ACCOUNT`: (optional) Staging storage account name
   - `STAGING_CONTAINER`: (optional) Staging container name

> **Note**: Storage account key authentication is simpler than Service Principal and sufficient for storage-only access. It doesn't require Azure Active Directory setup or RBAC permissions.

## �� Usage

### Automatic Triggers

The pipeline runs automatically on:

**Pull Requests** → Runs: unit tests, code quality, build wheels

**Push to develop** → Runs: Everything above + integration tests + KDA tests

**Push to main** → Runs: Everything above + system tests

### Manual Workflow Run

1. Go to **Actions** tab in GitHub
2. Select "Kindling CI/CD Pipeline"
3. Click "Run workflow"
4. Choose branch and click "Run"

## 📦 Build Artifacts

After each workflow run, artifacts are available:

| Artifact | Contains |
|----------|----------|
| `unit-test-results` | Test results XML + HTML coverage |
| `code-quality-reports` | Pylint and mypy reports |
| `integration-test-results` | Integration test XML |
| `kda-test-results` | KDA packaging test results |
| `system-test-results-{platform}` | System test results per platform |
| `security-reports` | Safety and bandit scan results |
| `platform-wheels` | Built .whl files for all platforms |

### Download Artifacts

1. Go to **Actions** → Select a workflow run
2. Scroll to **Artifacts** section
3. Click artifact name to download

### Use Built Wheels

```bash
# Download platform-wheels artifact
unzip platform-wheels.zip

# Install specific platform
pip install kindling_synapse-0.1.0-py3-none-any.whl
pip install kindling_databricks-0.1.0-py3-none-any.whl
pip install kindling_fabric-0.1.0-py3-none-any.whl
```

## 🧪 Running Tests Locally

### Prerequisites
```bash
# Install Poetry
curl -sSL https://install.python-poetry.org | python3 -

# Install dependencies
poetry install --with dev
```

### Run Tests

```bash
# Unit tests
poetry run pytest tests/unit/ -v

# Unit tests with coverage
poetry run pytest tests/unit/ --cov=packages/kindling --cov-report=html

# Integration tests (requires Azure credentials)
export AZURE_STORAGE_ACCOUNT="your-account"
export AZURE_CONTAINER="your-container"
poetry run pytest tests/integration/ -v

# KDA packaging tests
poetry run python tests/test_kda_packaging.py

# Code quality
poetry run black --check packages/ tests/
poetry run pylint packages/kindling/
poetry run mypy packages/kindling/
```

### Build Wheels Locally

```bash
# Build all platform wheels
chmod +x scripts/build_platform_wheels.sh
./scripts/build_platform_wheels.sh

# Check built wheels
ls -lh dist/
```

## 🐛 Troubleshooting

### Unit Tests Fail

1. Check test output in **Actions** → workflow run → **unit-tests** job
2. Download `unit-test-results` artifact for detailed HTML report
3. Run locally: `poetry run pytest tests/unit/ -v`

### Integration Tests Fail

**Common issues:**
- ❌ Azure storage key not configured → Add `AZURE_STORAGE_KEY_TEST` secret
- ❌ Storage account/container variables missing → Add `TEST_STORAGE_ACCOUNT` and `TEST_CONTAINER` variables
- ❌ Storage account not accessible → Check firewall rules or network restrictions
- ❌ Container doesn't exist → Create the container in your storage account

**Debug:**
```bash
# Test storage access with key
az storage container list \
  --account-name YOUR-STORAGE-ACCOUNT \
  --account-key YOUR-STORAGE-KEY

# Create container if needed
az storage container create \
  --name kindling-tests \
  --account-name YOUR-STORAGE-ACCOUNT \
  --account-key YOUR-STORAGE-KEY
```

### Build Wheels Fail

**Common issues:**
- ❌ Poetry not installed → Check "Install Poetry" step in workflow
- ❌ Missing platform configs → Ensure `build-configs/*.toml` exist
- ❌ Source files missing → Verify `packages/kindling/` exists

### System Tests Fail

System tests are **expected to fail** if you don't have real Azure Synapse/Databricks/Fabric environments configured. They're marked with `|| true` in the workflow to not block the pipeline.

To enable:
1. Configure real platform credentials
2. Remove `|| true` from system tests step
3. Update platform-specific test configurations

## 🎨 Customization

### Skip Certain Jobs

Edit `.github/workflows/ci.yml` and add conditions:

```yaml
integration-tests:
  if: github.ref == 'refs/heads/main' && false  # Always skip
```

### Change Python Version

```yaml
env:
  PYTHON_VERSION: "3.11"  # Change to 3.10, 3.12, etc.
```

### Add New Test Suite

```yaml
  custom-tests:
    name: Custom Tests
    runs-on: ubuntu-latest
    needs: unit-tests

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ env.PYTHON_VERSION }}

      - name: Install Poetry
        run: pip install poetry

      - name: Install dependencies
        run: poetry install --with dev

      - name: Run custom tests
        run: poetry run pytest tests/custom/ -v
```

## 📊 Monitoring & Metrics

### GitHub Actions Dashboard

View all runs: `https://github.com/{owner}/{repo}/actions`

### Status Badge

Add to README.md:
```markdown
![CI/CD](https://github.com/{owner}/{repo}/workflows/Kindling%20CI%2FCD%20Pipeline/badge.svg)
```

### Codecov Integration

If you have Codecov configured:
1. Coverage is automatically uploaded
2. View at: `https://codecov.io/gh/{owner}/{repo}`

## 🔄 Future Enhancements

When you're ready to add publishing:

### PyPI Publishing
```yaml
  publish-to-pypi:
    name: Publish to PyPI
    runs-on: ubuntu-latest
    needs: [build-platform-wheels, system-tests]
    environment: production
    if: github.event_name == 'release'

    steps:
      - name: Download wheels
        uses: actions/download-artifact@v3
        with:
          name: platform-wheels
          path: dist/

      - name: Publish to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
        with:
          password: ${{ secrets.PYPI_API_TOKEN }}
```

### GitHub Packages Publishing
```yaml
  publish-to-github:
    name: Publish to GitHub Packages
    runs-on: ubuntu-latest
    needs: [build-platform-wheels]
    if: github.ref == 'refs/heads/main'

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Install Poetry
        run: pip install poetry

      - name: Configure GitHub Packages
        run: |
          poetry config repositories.github https://pypi.pkg.github.com/${{ github.repository_owner }}
          poetry config pypi-token.github ${{ secrets.GITHUB_TOKEN }}

      - name: Publish
        run: poetry publish --repository github
```

## 📚 Additional Resources

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Poetry Documentation](https://python-poetry.org/docs/)
- [Azure CLI Service Principal](https://learn.microsoft.com/en-us/cli/azure/create-an-azure-service-principal-azure-cli)
- [Kindling Build System](./build_system.md)

## 💡 Tips

1. **Start Simple**: Begin with just unit tests, add more as needed
2. **Use Environments**: Separate test/staging/prod with approvals
3. **Monitor Costs**: Azure integration tests use cloud resources
4. **Cache Dependencies**: Poetry cache speeds up builds significantly
5. **Parallel Jobs**: Jobs run in parallel when possible for speed
