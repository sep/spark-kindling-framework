# Kindling Framework - Development Workflow & CI/CD Plan

## 🎯 Overview

This document outlines the complete development, testing, deployment, and CI/CD strategy for the Kindling Framework before making the repository public.

## 📋 Current State Assessment

### ✅ What We Have
- **Core Framework**: Cross-platform data processing framework
- **Platform Support**: Azure Synapse, Microsoft Fabric, Databricks, Local
- **KDA Packaging**: Complete app packaging and deployment system
- **Test Infrastructure**: Unit, integration, and system test frameworks
- **Environment Config**: `.env` template and configuration system
- **Documentation**: Comprehensive docs and examples

### ❌ What We Need
- **CI/CD Pipelines**: Automated testing and deployment
- **Environment Strategy**: Clear dev/test/staging/prod environments
- **Security**: Secrets management and secure credential handling
- **Release Process**: Versioning, tagging, and distribution
- **Public Readiness**: Clean repository with proper documentation

## 🏗️ Proposed Development Workflow

### 1. Environment Strategy

#### **Development Environment**
- **Purpose**: Local development and unit testing
- **Storage**: `abfss://dev-container@{storage}.dfs.core.windows.net/kindling/`
- **Config**: `.env` file (never committed)
- **Authentication**: Azure CLI (`az login`) or service principal

#### **Testing Environment** 
- **Purpose**: Integration and system testing
- **Storage**: `abfss://test-container@{storage}.dfs.core.windows.net/kindling/`
- **Config**: Environment variables in CI/CD
- **Authentication**: Service principal with limited permissions

#### **Staging Environment**
- **Purpose**: Pre-production validation
- **Storage**: `abfss://staging-container@{storage}.dfs.core.windows.net/kindling/`
- **Config**: Secured environment variables
- **Authentication**: Managed identity or service principal

#### **Production Environment**
- **Purpose**: Live deployments and demos
- **Storage**: `abfss://prod-container@{storage}.dfs.core.windows.net/kindling/`
- **Config**: Azure Key Vault integration
- **Authentication**: Managed identity

### 2. Testing Strategy

#### **Unit Tests** (`tests/unit/`)
```bash
pytest tests/unit/ --cov=kindling --cov-report=term-missing
```
- **Scope**: Individual components and functions
- **Environment**: Local/mocked dependencies
- **Frequency**: On every commit

#### **Integration Tests** (`tests/integration/`)
```bash
pytest tests/integration/ --env=test
```
- **Scope**: Component interactions and platform services
- **Environment**: Test storage accounts with real credentials
- **Frequency**: On pull requests and nightly

#### **System Tests** (`tests/system/`)
```bash
pytest tests/system/ --platform=synapse --env=staging
```
- **Scope**: End-to-end workflows on actual platforms
- **Environment**: Staging environments with real data
- **Frequency**: Before releases and weekly

#### **KDA Deployment Tests**
```bash
# Test KDA packaging and deployment
python tests/system/test_kda_deployment.py --target-platform=synapse
python tests/system/test_kda_deployment.py --target-platform=databricks
python tests/system/test_kda_deployment.py --target-platform=fabric
```

### 3. CI/CD Pipeline Architecture

#### **GitHub Actions Workflow** (`.github/workflows/`)

```yaml
# .github/workflows/ci.yml
name: Kindling CI/CD Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]
  release:
    types: [published]

env:
  PYTHON_VERSION: '3.11'

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ env.PYTHON_VERSION }}
      - name: Install dependencies
        run: |
          pip install -e ".[dev]"
      - name: Run unit tests
        run: |
          pytest tests/unit/ --cov=kindling --cov-report=xml
      - name: Upload coverage
        uses: codecov/codecov-action@v3

  integration-tests:
    runs-on: ubuntu-latest
    needs: unit-tests
    environment: testing
    steps:
      - uses: actions/checkout@v4
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ env.PYTHON_VERSION }}
      - name: Azure CLI Login
        uses: azure/login@v1
        with:
          creds: ${{ secrets.AZURE_CREDENTIALS_TEST }}
      - name: Run integration tests
        env:
          AZURE_STORAGE_ACCOUNT: ${{ vars.TEST_STORAGE_ACCOUNT }}
          AZURE_CONTAINER: ${{ vars.TEST_CONTAINER }}
        run: |
          pytest tests/integration/ -v

  system-tests:
    runs-on: ubuntu-latest
    needs: integration-tests
    environment: staging
    strategy:
      matrix:
        platform: [synapse, databricks, fabric]
    steps:
      - uses: actions/checkout@v4
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ env.PYTHON_VERSION }}
      - name: Run system tests
        env:
          TARGET_PLATFORM: ${{ matrix.platform }}
          AZURE_STORAGE_ACCOUNT: ${{ vars.STAGING_STORAGE_ACCOUNT }}
        run: |
          pytest tests/system/ --platform=${{ matrix.platform }} -v

  build-and-publish:
    runs-on: ubuntu-latest
    needs: [unit-tests, integration-tests, system-tests]
    if: github.event_name == 'release'
    steps:
      - uses: actions/checkout@v4
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ env.PYTHON_VERSION }}
      - name: Build package
        run: |
          pip install build
          python -m build
      - name: Publish to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
        with:
          password: ${{ secrets.PYPI_API_TOKEN }}
```

### 4. Storage Path Strategy

#### **Development Paths**
```
abfss://dev-container@{storage}.dfs.core.windows.net/
├── kindling/
│   ├── apps/                 # Deployed KDA packages
│   ├── artifacts/            # Build artifacts
│   ├── tests/                # Test data and results
│   └── logs/                 # Application logs
```

#### **Testing Paths**
```
abfss://test-container@{storage}.dfs.core.windows.net/
├── kindling/
│   ├── kda-packages/         # Test KDA deployments
│   ├── test-data/            # Integration test datasets
│   ├── results/              # Test execution results
│   └── ci-artifacts/         # CI/CD build outputs
```

#### **Staging/Production Paths**
```
abfss://prod-container@{storage}.dfs.core.windows.net/
├── kindling/
│   ├── releases/             # Released KDA packages
│   ├── apps/                 # Production deployments
│   ├── metrics/              # Performance and usage metrics
│   └── backups/              # Data backups
```

### 5. Secrets and Security Management

#### **GitHub Repository Secrets**
```yaml
AZURE_CREDENTIALS_TEST:       # Service principal for test environment
AZURE_CREDENTIALS_STAGING:    # Service principal for staging environment
AZURE_CREDENTIALS_PROD:       # Service principal for production environment
PYPI_API_TOKEN:              # PyPI publishing token
```

#### **GitHub Repository Variables**
```yaml
TEST_STORAGE_ACCOUNT:        # Test storage account name
TEST_CONTAINER:              # Test container name
STAGING_STORAGE_ACCOUNT:     # Staging storage account name
PROD_STORAGE_ACCOUNT:        # Production storage account name
```

#### **Azure Key Vault Integration**
```python
# For production environments
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
client = SecretClient(vault_url="https://kindling-kv.vault.azure.net/", credential=credential)

# Retrieve secrets at runtime
storage_key = client.get_secret("storage-account-key").value
```

### 6. Release Process

#### **Semantic Versioning**
- **Major** (1.0.0): Breaking changes to public API
- **Minor** (0.1.0): New features, backward compatible
- **Patch** (0.0.1): Bug fixes, backward compatible

#### **Release Workflow**
1. **Feature Development**: Work on `feature/` branches
2. **Integration**: Merge to `develop` branch for testing
3. **Release Preparation**: Create `release/v1.0.0` branch
4. **Testing**: Run full test suite on release branch
5. **Merge**: Merge to `main` and create GitHub release
6. **Publishing**: Automated PyPI publishing via GitHub Actions

#### **Release Checklist**
- [ ] All tests passing (unit, integration, system)
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] Version bumped in pyproject.toml
- [ ] Security scan completed
- [ ] Performance benchmarks acceptable

### 7. Repository Cleanup for Public Release

#### **Files to Remove/Sanitize**
- [ ] `.env` file (contains real credentials)
- [ ] Any hardcoded storage account names or keys
- [ ] Internal company references
- [ ] Test data with sensitive information

#### **Files to Add**
- [ ] `.github/workflows/ci.yml` - CI/CD pipeline
- [ ] `SECURITY.md` - Security policy and reporting
- [ ] `CODE_OF_CONDUCT.md` - Community guidelines
- [ ] `CONTRIBUTORS.md` - Contribution guidelines
- [ ] `CHANGELOG.md` - Version history
- [ ] `.gitignore` updates for common development files

#### **Documentation to Update**
- [ ] `README.md` - Installation and quick start guide
- [ ] `docs/GETTING_STARTED.md` - Detailed setup instructions
- [ ] `docs/API_REFERENCE.md` - Complete API documentation
- [ ] `docs/EXAMPLES.md` - Real-world usage examples

### 8. Monitoring and Observability

#### **Application Insights Integration**
```python
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace

# Configure Application Insights
configure_azure_monitor(
    connection_string="InstrumentationKey=your-key"
)

tracer = trace.get_tracer(__name__)
```

#### **Metrics Collection**
- KDA deployment success/failure rates
- Platform-specific execution times
- Storage I/O performance
- Error rates and types

#### **Logging Strategy**
```python
import logging
from kindling.spark_log_provider import SparkLoggerProvider

# Structured logging with correlation IDs
logger = SparkLoggerProvider().get_logger("kindling.deployment")
logger.info("KDA deployment started", extra={
    "app_name": app_name,
    "platform": platform,
    "correlation_id": correlation_id
})
```

## 🚀 Implementation Timeline

### **Phase 1: Environment Setup** (Week 1)
- [ ] Set up Azure storage accounts for each environment
- [ ] Configure service principals and permissions
- [ ] Create GitHub repository secrets and variables
- [ ] Test basic connectivity from CI/CD

### **Phase 2: CI/CD Pipeline** (Week 2)
- [ ] Implement GitHub Actions workflows
- [ ] Set up automated testing pipeline
- [ ] Configure deployment automation
- [ ] Test end-to-end pipeline

### **Phase 3: Testing Enhancement** (Week 3)
- [ ] Update system tests to use real ABFSS paths
- [ ] Add KDA deployment tests for each platform
- [ ] Implement performance benchmarking
- [ ] Add security scanning

### **Phase 4: Repository Cleanup** (Week 4)
- [ ] Remove sensitive data and clean up code
- [ ] Add missing documentation and guides
- [ ] Update README and getting started docs
- [ ] Final security and compliance review

### **Phase 5: Public Release** (Week 5)
- [ ] Make repository public
- [ ] Publish to PyPI
- [ ] Announce to community
- [ ] Monitor for issues and feedback

## 🎯 Success Criteria

- [ ] **Automated Testing**: 100% of tests run automatically on commits
- [ ] **Multi-Platform**: All three platforms (Synapse, Databricks, Fabric) tested
- [ ] **Security**: No secrets or credentials in repository
- [ ] **Documentation**: Complete setup and usage documentation
- [ ] **Performance**: System tests complete within acceptable time limits
- [ ] **Reliability**: CI/CD pipeline has >95% success rate

This plan provides a clear path from current state to public-ready repository with proper CI/CD, testing, and deployment infrastructure.