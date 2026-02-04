---
applyTo: '**'
---

# Kindling Framework - Technical Context

## CRITICAL WORKFLOW RULES

### 🚨 ALWAYS Use Poe Tasks - NEVER Call Scripts Directly

**CRITICAL: Poe tasks are the ONLY way to run operations. NEVER call scripts directly.**

```bash
# List all available tasks
poe --help
```

**How Poe Tasks Work:**
- Poe tasks are defined in `pyproject.toml` under `[tool.poe.tasks]`
- Many tasks call functions in `scripts/test_runner.py` (the backend implementation)
- **NEVER call test_runner.py directly** - it's internal infrastructure
- **ALWAYS use `poe <task-name>`** instead

**Example:**
```bash
# ✅ CORRECT: Use poe task
poe test-system --platform fabric

# ❌ WRONG: Don't call test_runner.py directly
python scripts/test_runner.py system --platform fabric

# ✅ CORRECT: Use poe task
poe deploy --platform fabric

# ❌ WRONG: Don't call deploy.py directly
python scripts/deploy.py --platform fabric
```

**Common poe tasks that MUST be used instead of manual commands:**

| Task | Purpose | Instead of Manual |
|------|---------|-------------------|
| `poe version --type alpha` | **Bump alpha version** (for cache busting during dev) | Manual pyproject.toml editing |
| `poe version --type alpha --platform fabric` | **Bump + build + deploy Fabric** (one command workflow) | Manual version edit + build + deploy |
| `poe version --type patch` | Bump patch version (remove alpha suffix) | Manual version editing |
| `poe version --type minor` | Bump minor version | Manual version editing |
| `poe version --type major` | Bump major version | Manual version editing |
| `poe release <version>` | **Full release workflow** | Manual git tag + gh release + build + upload |
| `poe build` | Build all platform wheels | Manual poetry build commands |
| `poe deploy` | Deploy all platform wheels to Azure | Manual az storage commands |
| `poe deploy --platform fabric` | Deploy Fabric wheel to Azure | Manual az storage commands |
| `poe deploy --platform synapse` | Deploy Synapse wheel to Azure | Manual az storage commands |
| `poe deploy --platform databricks` | Deploy Databricks wheel to Azure | Manual az storage commands |
| `poe deploy --release` | Deploy all from latest GitHub release | Manual gh release download + upload |
| `poe test-system` | Run system tests (all platforms, all tests) | Manual pytest commands |
| `poe test-system --platform fabric` | Run Fabric system tests | Manual pytest with markers |
| `poe test-system --platform synapse` | Run Synapse system tests | Manual pytest with markers |
| `poe test-system --platform databricks` | Run Databricks system tests | Manual pytest with markers |
| `poe test-system --test <pattern>` | Run specific test(s) on all platforms | Manual pytest -k filtering |
| `poe cleanup` | Clean up all test resources (all platforms + old packages) | Manual deletion commands |
| `poe cleanup --platform fabric` | Clean up Fabric test resources only | Manual deletion commands |

**The `poe release` workflow handles:**
1. ✅ Version bump in all config files
2. ✅ Git commit and push
3. ✅ GitHub release creation
4. ✅ Triggers CI/CD to build wheels
5. ✅ Triggers CI/CD to run system tests
6. ✅ Triggers CI/CD to attach wheels to release

**NEVER manually:**
- Create git tags without using `poe release`
- Build wheels without using `poe build`
- Create GitHub releases without using `poe release`
- Deploy wheels without using `poe deploy-*`

**Exception:** Only do manual operations if explicitly requested by user or if no poe task exists.

## Project Overview

**Framework:** Kindling - Multi-platform Spark data lakehouse framework
**Platforms:** Azure Synapse Analytics, Databricks, Microsoft Fabric
**Purpose:** Notebook-based orchestration, dependency injection, Delta Lake operations across cloud environments

## Current Architectural Decisions

### Data App Manager Refactoring - Two-Layer Architecture

**Layer 1: Pure Utilities**
- `DataAppPackager`, `DataAppExtractor` (no dependencies)
- Pure functions for mechanical operations

**Layer 2: Managed Services**
- `DataAppManager` with DI (composes utilities + platform services)
- NO adapter layer needed - `PlatformServiceProvider` already handles platform-specific operations

### Naming Conventions

**Avoid "KDA" Prefix:**
- Use "DataApp" instead (library IS Kindling, .kda is just file extension)
- `publish_app()` is primary notebook API (package + deploy, like `publish_wheel()`)
- `package_app()` is advanced API (creates .kda file locally)
- Use `app_folder` parameter (not `app_directory`) for notebook context

**Folder Marker Pattern:**
- `{folder-name}_init.py` marks both packages and apps
- Apps distinguished by `@app_config` decorator in the init file
- Packages: `my-lib/my-lib_init.py` → builds `.whl`
- Apps: `my-app/my-app_init.py` with `@app_config` → builds `.kda`
- Backwards compatible: falls back to `app.yaml` if no init file

**Folder Organization (Convention, Not Enforced):**
- Recommended: `apps/` for data apps, `pkgs/` for packages
- Flexible: apps/packages can be anywhere in hierarchy per client needs
- Discovery: hybrid approach with smart path resolution

### App Lifecycle Phases

1. **Package:** folder → .kda (DataAppPackager utility)
2. **Deploy:** .kda → artifacts storage (platform services)
3. **Run:** download from artifacts → execute (app runner pattern, NOT notebook execution)

## Current System Analysis - Notebook Packages

### How Notebook Packages Currently Work

**1. Package Discovery** (`NotebookLoader.get_all_packages()` - line 601):
- Searches all folders for notebooks
- Checks if folder contains `{folder-name}_init` notebook
- Example: `test-package-three/` contains `test-package-three_init.Notebook`

**2. Package Registration** (in `{folder}_init` notebook):
```python

NotebookPackages.register(
name="test-package-three",
dependencies=["kindling"],
tags={}
)
```

- Registry pattern using `NotebookPackages` class (line 1265)
- Stores metadata: name, dependencies, tags, version, etc.

**3. Wheel Building** (`publish_notebook_folder_as_package()` - line 1151):
- Executes `{folder}_init` notebook to register package
- Extracts notebook code from all notebooks in folder
- Creates temp directory with `pyproject.toml` and Python files
- Runs `pip wheel` to build `.whl` file
- Copies wheel to artifacts storage

### Impact Assessment - Zero Conflict

**ZERO CONFLICT** - Completely separate systems:
- **Notebooks**: `{folder}_init` **NOTEBOOK** contains `NotebookPackages.register()`
- **File-based apps**: `{folder}_init.py` **FILE** contains `@app_config` decorator
- Different file types, different discovery mechanisms, different purposes

**Clean Parallel:**

| Type | Marker | Discovery | Output |
|------|--------|-----------|--------|
| Notebook Package | `{folder}_init.Notebook` | `NotebookLoader.get_all_packages()` | `.whl` |
| File-based App | `{folder}_init.py` | `DataAppManager.discover_apps()` | `.kda` |

**NO CHANGES NEEDED** to existing notebook package system

## Job Deployment Feature - IMPLEMENTED ✅

### Architecture: Framework Feature (Not Testing Infrastructure)

**Decision:** Job deployment is a **framework feature**. Users can deploy apps as Spark jobs from notebooks or CLI.

**Design Pattern:** Platform services pattern (NO if/else platform differentiation)

### Implementation Status - COMPLETE

- ✅ Extended PlatformService interface with job deployment methods
- ✅ Created DataAppDeployer orchestrator (platform-agnostic)
- ✅ Implemented FabricPlatformService.deploy_spark_job()
- ✅ Implemented DatabricksPlatformService.deploy_spark_job()
- ✅ Implemented SynapsePlatformService.deploy_spark_job()
- ✅ Created FabricTestRunner using framework deployment
- ✅ Built complete system test infrastructure for Fabric
- ✅ **Authentication: Supports both service principal AND az login**

### Key Components

**1. PlatformService Interface Extensions** (`packages/kindling/notebook_framework.py`):
```python
@abstractmethod
def deploy_spark_job(self, app_files: Dict[str, str], job_config: Dict[str, Any]) -> Dict[str, Any]:
"""Deploy app as Spark job - platform-specific implementation"""
pass@abstractmethod
def run_spark_job(self, job_id: str, parameters: Dict[str, Any] = None) -> str:
"""Execute deployed job - returns run_id"""
pass@abstractmethod
def get_job_status(self, run_id: str) -> Dict[str, Any]:
"""Get job status - returns {status, start_time, end_time, error, logs}"""
pass@abstractmethod
def cancel_job(self, run_id: str) -> bool:
"""Cancel running job"""
pass
```
**2. DataAppDeployer** (`packages/kindling/job_deployment.py`):
- Platform-agnostic orchestrator using dependency injection
- Methods: `deploy_as_job()`, `run_job()`, `get_job_status()`, `cancel_job()`
- Prepares app files from directory or .kda package
- Injects bootstrap shim automatically
- Delegates platform-specific operations to injected platform service
- **Zero platform conditionals** - all platform logic in services

**3. Platform Implementations:**

**Fabric** (`packages/kindling/platform_fabric.py`):
- Creates Spark Job Definition via REST API
- Uploads files to OneLake using Create → Append → Flush pattern
- Updates job definition with abfss:// file paths
- Executes jobs via Item Run API
- Monitors job status via Operations API

**Databricks** (`packages/kindling/platform_databricks.py`):
- Uploads files to DBFS via dbutils
- Creates Databricks job via Jobs API 2.1
- Supports existing cluster or new cluster config
- Executes via run-now API
- Monitors via runs/get API

**Synapse** (`packages/kindling/platform_synapse.py`):
- Uploads files to workspace storage via mssparkutils
- Creates Spark Job Definition via Artifacts API
- Submits as Spark batch job
- Monitors via batch job API
- Supports Spark pool configuration

**4. Bootstrap Shim Pattern:**
Generated by DataAppDeployer, platform-agnostic:
- Detects storage utils (mssparkutils or dbutils)
- Installs framework wheels from artifacts storage
- Executes main application entry point
- Clean execution flow: shim → framework → app

**5. FabricTestRunner** (`tests/system/runners/fabric_runner.py`):
- Demonstrates framework usage in testing context
- Same deployment code available to production users
- Example of how to use DataAppDeployer with DI

### Usage Patterns

**In Notebook:**
```python

from kindling.job_deployment import DataAppDeployerDI automatically injects platform service
deployer = DataAppDeployer()Deploy app as job
result = deployer.deploy_as_job(
app_path="/path/to/app",
job_config={
"job_name": "my-data-app",
"lakehouse_id": "...",  # Fabric
"artifacts_path": "abfss://.../artifacts"
}
)Run the job
run_id = deployer.run_job(result['job_id'])Monitor status
status = deployer.get_job_status(run_id)
```

**In CLI or Local Script:**
```python
from kindling.job_deployment import DataAppDeployer
from kindling.platform_fabric import FabricService
from kindling.platform_provider import SparkPlatformServiceProviderManual setup when not in notebook
platform_service = FabricService(config, logger)
provider = SparkPlatformServiceProvider()
provider.set_service(platform_service)Create deployer with manual DI
deployer = DataAppDeployer(provider, logger_provider)Use same API
result = deployer.deploy_as_job(app_path, job_config)
```

### Architecture Principles Enforced

✅ **Job deployment is framework feature** - Available to all users, not just tests
✅ **Platform services pattern** - No if/else platform checks in framework code
✅ **Dependency injection** - Platform services injected, not instantiated
✅ **Single responsibility** - Framework orchestrates, platforms execute
✅ **Extensibility** - Add new platform by implementing interface
✅ **Testability** - Mock platform service for unit tests

### API Reference

**Fabric Job Config:**
- `job_name` (required): Display name
- `lakehouse_id` (required): Lakehouse for storage
- `artifacts_path`: Framework wheels location
- `entry_point`: Main file (default: bootstrap_shim.py)
- `executor_cores`, `executor_memory`, `driver_cores`, `driver_memory`

**Databricks Job Config:**
- `job_name` (required): Display name
- `cluster_id` (optional): Existing cluster
- `new_cluster` (optional): New cluster config
- `entry_point`: Main file (default: bootstrap_shim.py)
- `libraries`: Additional libraries

**Synapse Job Config:**
- `job_name` (required): Display name
- `spark_pool_name` (required): Spark pool
- `entry_point`: Main file (default: bootstrap_shim.py)
- `executors`, `executor_cores`, `executor_memory`

## Build & Deployment System

### Build Tool: Poe the Poet (poethepoet)

**NOT using Poetry directly** - using Poe the Poet task runner that works with Poetry projects.

**Build Commands:**
```bash
Build platform-specific wheels
poe build              # Build all platforms (synapse, databricks, fabric)Publish to Azure storage - IMPORTANT: Deploy only the platform you're testing!
poe deploy-fabric      # Deploy ONLY Fabric wheel (for Fabric testing)
poe deploy-databricks  # Deploy ONLY Databricks wheel (for Databricks testing)
poe deploy-synapse     # Deploy ONLY Synapse wheel (for Synapse testing)
poe deploy             # Deploy ALL wheels (avoid this during testing)
```
**Configuration:** See `pyproject.toml` `[tool.poe.tasks]` section for task definitions.

**Pip Caching Issue:**
- Spark clusters cache pip packages locally
- Changing version number (e.g., 0.0.11 → 0.0.12) forces pip to download new wheel
- Bootstrap script uses `--force-reinstall` flag to bypass some caching
- Current version: 0.0.12 (bumped from 0.0.11 to bust cache after platform consolidation fix)

### Fabric Job Deployment Updates (January 2025)

**Critical Bug Fix - Config File Download:**
- Fixed `download_config_files()` in `bootstrap.py` to use `file://` prefix
- Issue: `mssparkutils.fs.cp()` was treating local `/tmp/` paths as OneLake paths
- Solution: `storage_utils.fs.cp(remote_path, f"file://{str(local_path)}")`
- Impact: `settings.yaml` and other config files now load correctly from lakehouse

**Bootstrap Script Enhancement:**
- Script location: `Files/scripts/kindling_bootstrap.py` in lakehouse (not in repo yet)
- Command-line argument convention: `config:key=value` sets `BOOTSTRAP_CONFIG[key] = value`
- Auto-detects notebook context vs Spark job context
- Priority: `__main__.BOOTSTRAP_CONFIG` (notebook) > command-line args (Spark job) > manual call
- Supports both notebook `%run` and Spark job execution patterns

**Job Definition Architecture:**
- `executableFile`: Points to `Files/scripts/kindling_bootstrap.py` in lakehouse
- `commandLineArguments`: Pass bootstrap config as `config:app_name=my-app config:artifacts_storage_path=Files/artifacts config:use_lake_packages=True`
- `defaultLakehouseArtifactId`: Required for relative path resolution (e.g., `Files/artifacts`)
- Lakehouse provides default filesystem context for Spark relative paths

**Storage Structure:**
{AZURE_BASE_PATH}/
├── config/              # Hierarchical config system (see below)
├── data-apps/{app}/     # App-specific code and config
├── packages/            # Python wheels (kindling_fabric-*.whl)
└── scripts/             # Utility scripts (kindling_bootstrap.py)

**Lakehouse vs External Storage:**
- Lakehouse (`Files/`) for: scripts, config, data-apps (runtime access)
- External ABFSS for: packages (deployment artifacts)
- Current test setup: Uploads to external ABFSS but should migrate to lakehouse for apps

## Configuration System - Hierarchical YAML

### Configuration Priority (Lowest to Highest)

Kindling uses a **hierarchical configuration system** with multiple layers of YAML files:

1. **`settings.yaml`** - Base framework settings (lowest priority)
2. **`platform_{platform}.yaml`** - Platform-specific (fabric, synapse, databricks)
3. **`workspace_{workspace_id}.yaml`** - Workspace-specific settings
4. **`env_{environment}.yaml`** - Environment-specific (dev, prod, etc.)
5. **Bootstrap Config** - In-memory overrides from BOOTSTRAP_CONFIG dict (highest priority)

Each layer can override values from previous layers, providing precise control at different organizational levels.

### Configuration File Locations

All config files are stored in `{artifacts_storage_path}/config/` and are **optional**:

```
config/
├── settings.yaml                    # Base settings
├── platform_fabric.yaml             # Fabric-specific
├── platform_synapse.yaml            # Synapse-specific
├── platform_databricks.yaml         # Databricks-specific
├── workspace_{workspace_id}.yaml    # Workspace-specific
├── env_dev.yaml                     # Development environment
├── env_staging.yaml                 # Staging environment
└── env_prod.yaml                    # Production environment
```

### Platform Detection

Platform is auto-detected during framework initialization:
- **Fabric**: Via `notebookutils.runtime.context.get("currentWorkspaceId")`
- **Synapse**: Via `spark.conf.get("spark.synapse.workspace.name")`
- **Databricks**: Via `dbutils.entry_point` context or `spark.conf.get("spark.databricks.workspaceUrl")`

### Workspace ID Detection

Workspace IDs are auto-detected per platform:
- **Fabric**: GUID format (e.g., `workspace_12345678-1234-1234-1234-123456789abc.yaml`)
- **Synapse**: Workspace name (e.g., `workspace_mysynapsews.yaml`)
- **Databricks**: Sanitized workspace URL (e.g., `workspace_adb-123456789_azuredatabricks_net.yaml`)

### Example Configuration Hierarchy

**Scenario: Multi-team Fabric deployment with dev/prod environments**

```yaml
# settings.yaml - Base for all teams/environments
kindling:
  version: "0.2.0"
  delta:
    tablerefmode: "forName"
  telemetry:
    logging:
      level: INFO

# platform_fabric.yaml - Fabric-specific tuning
kindling:
  platform:
    name: fabric
  TELEMETRY:
    logging:
      level: DEBUG  # More verbose for diagnostic emitters
  extensions:
    - kindling-otel-azure>=0.2.0

# workspace_team-a-workspace-id.yaml - Team A workspace
kindling:
  workspace:
    team: "team-a"
  DATA:
    bronze: "abfss://bronze-team-a@..."

# env_prod.yaml - Production overrides (highest YAML priority)
kindling:
  TELEMETRY:
    logging:
      level: WARN  # Less verbose in production
  SPARK_CONFIGS:
    spark.executor.memory: "32g"
```

**Result**: Team A's prod workspace gets `WARN` level logging (from env_prod.yaml), team-specific data paths (from workspace yaml), Fabric extensions (from platform yaml), and base Delta settings (from settings.yaml).

### Configuration Use Cases

**1. Multi-Platform Deployments**
- Base settings apply everywhere
- Platform configs tune for Fabric vs Synapse vs Databricks
- Environment configs manage dev/prod differences

**2. Multi-Team Workspaces**
- Workspace configs provide team/cost-center separation
- Each team gets own data paths, quotas, security settings
- Shared platform and environment configs

**3. Geographic Regions**
- Workspace configs for region-specific settings
- Region-specific data compliance, storage locations
- Shared platform configuration

### Documentation

- **Complete Guide**: [docs/platform_workspace_config.md](docs/platform_workspace_config.md)
- **Example Configs**: [examples/config/](examples/config/)
- **Implementation**: `packages/kindling/bootstrap.py` (`download_config_files()`, `_get_workspace_id_for_platform()`)
- **Tests**: `tests/unit/test_platform_workspace_config.py`

### Migration from Single Config

**Before (all settings in settings.yaml):**
```yaml
# settings.yaml - Everything mixed together
kindling:
  version: "0.2.0"
  # Platform-specific stuff
  # Team-specific stuff
  # Environment-specific stuff
```

**After (organized hierarchy):**
```yaml
# settings.yaml - Just base settings
kindling:
  version: "0.2.0"
  delta:
    tablerefmode: "forName"

# platform_fabric.yaml - Extract platform settings
# workspace_team-a.yaml - Extract team settings
# prod.yaml - Extract environment settings
```

Benefits: Better organization, version control, team autonomy, easier maintenance

## System Testing

### Running System Tests Properly

**CRITICAL: System tests MUST be run with monitoring to completion**
- Tests deploy actual Spark jobs to cloud platforms (Fabric, Synapse, Databricks)
- Job execution can take **several minutes**, especially on Synapse (cluster spin-up)
- **DO NOT interrupt tests** - let them run to completion
- Tests validate end-to-end functionality: deployment, execution, monitoring, cleanup

**Test Commands:**
```bash
Fabric system tests (deployment + monitoring)
cd /workspace && pytest -v -m fabric tests/system/test_fabric_job_deployment.pySynapse system tests (deployment + monitoring)
cd /workspace && pytest -v -m synapse tests/system/test_synapse_job_deployment.pyDatabricks system tests
cd /workspace && pytest -v -m databricks tests/system/test_databricks_job_deployment.pyRun specific test
pytest -v tests/system/test_fabric_job_deployment.py::TestFabricJobDeployment::test_deploy_app_as_job
```

**Expected Behavior:**
- `test_deploy_app_as_job` - Quick (5-10s) - validates job creation
- `test_run_and_monitor_job` - Slow (2-5 min) - validates execution and monitoring
- Tests include automatic cleanup of resources

### System Test Authentication

**Supports Multiple Authentication Methods:**

**1. Azure CLI (for local development):**
```bash
az login
export FABRIC_WORKSPACE_ID="..."
export FABRIC_LAKEHOUSE_ID="..."
```
**2. Service Principal (for CI/CD):**
```bash
export FABRIC_WORKSPACE_ID="..."
export FABRIC_LAKEHOUSE_ID="..."
export AZURE_TENANT_ID="..."
export AZURE_CLIENT_ID="..."
export AZURE_CLIENT_SECRET="..."
```

**How It Works:**
- Tests use `DefaultAzureCredential` from Azure SDK
- Automatically tries: service principal → managed identity → az login → others
- Only workspace/lakehouse IDs are required
- Auth credentials are optional (falls back to az login)
- Tests skip gracefully when configuration missing

**Test Utilities:**
- `tests/system/test_auth.py` - Verify authentication setup
- `tests/system/QUICKSTART.md` - Quick start guide
- `tests/system/README.md` - Complete documentation
- `tests/system/.env.example` - Configuration template

### Why System Testing Matters

- System tests validate the actual production deployment path
- Running to completion ensures job monitoring and cleanup work correctly
- Validates cross-platform compatibility
- Tests are designed to handle long-running operations

**Documentation:** See `/workspace/tests/system/README.md` for complete details

## Three-Platform System Test Status - ✅ PRODUCTION READY

**All Three Platforms Passing - 100% Test Success Rate:**
- ✅ **Databricks**: 5/5 tests passing
- ✅ **Synapse**: 5/5 tests passing (environment variable fix applied)
- ✅ **Fabric**: 5/5 tests passing (case sensitivity fix applied)
- ✅ **Total**: 15/15 tests passing

### Databricks Completion Summary

- ✅ Bootstrap: Framework loads and initializes correctly
- ✅ UC Volume Logs: Cluster logs delivered to `/Volumes/medallion/default/logs`
- ✅ ABFSS Log Reading: Driver logs readable from backing storage
- ✅ Universal Test App: All 4 tests passing
  - Test 1 (Spark Session): PASSED
  - Test 2 (Basic Spark Operations): PASSED
  - Test 3 (Framework Availability): PASSED
  - Test 4 (Storage Access): PASSED - fixed dbutils access pattern
- ✅ Test Validation: Updated to match actual log output

**Key Databricks Fix:**
- **Issue**: `dbutils` not accessible via `import dbutils`
- **Solution**: Access from `__main__` globals: `getattr(__main__, "dbutils", None)`
- **Pattern**: Matches Databricks runtime where dbutils is injected into globals

### Synapse Fixes Applied

- ✅ Environment Variable: Fixture now reads both `SYNAPSE_SPARK_POOL` and `SYNAPSE_SPARK_POOL_NAME`
- ✅ Status Comparisons: All status checks now case-insensitive (`.upper()`)
- ✅ Timing Edge Cases: Cancellation test accepts `NotStarted` as valid cancelled state
- ✅ Job Execution Validated: 113s runtime, all 13 expected log entries verified including "✅ ALL TESTS PASSED"
- ✅ Dependencies: `azure-storage-file-datalake>=12.14.0` installed for ADLS Gen2 operations

### Fabric Fixes Applied

- ✅ Case Sensitivity: Status comparison updated to handle title case vs uppercase ("Completed" vs "COMPLETED")
- ✅ All tests verified with actual job execution and log validation

### Tri-Platform Architecture Validated

- ✅ PlatformAPI ABC Interface: All 9 abstract methods implemented correctly across all 3 platforms
- ✅ Semantic Consistency: Common API surface, identical method signatures
- ✅ Process Consistency: Same deployment workflow (prepare → create → upload → update → execute → monitor → cleanup)
- ✅ Framework deployment works on all 3 platforms
- ✅ Bootstrap pattern consistent across platforms
- ✅ Log retrieval patterns validated (UC Volume/ABFSS for Databricks, ADLS for Synapse/Fabric)
- ✅ Universal test app runs identically on all platforms

### Cleanup Completed

- ✅ Test Artifacts: 69 total items cleaned (7 Databricks jobs, 62 storage data-apps)
- ✅ Documentation: Removed 4 progress-oriented markdown files
- ✅ Script: `scripts/cleanup_all_platforms.py` executed successfully via `poetry run poe cleanup`

### Test Infrastructure

- ✅ Unified test tasks: `poe test-system` with optional `--platform` and `--test` filters
- ✅ Flexible filtering: Run all platforms, specific platform, or specific test patterns
- ✅ Extension tests: `poe test-extension` with optional platform filtering
- ✅ System tests located: `tests/system/core/test_platform_job_deployment.py`

**Test Durations (Approximate):**
- Databricks: ~300s
- Synapse: ~270s
- Fabric: ~445s

**Platform Environment:**
- Synapse Spark Pool: `syspsprkpool0` (use env var `SYNAPSE_SPARK_POOL`)
- Fabric Spark: 3.5.5.5.4.20251014.2

### Tri-Platform Consistency Review

**Completed: Full semantic and process consistency review across Fabric, Synapse, and Databricks**

**ABC Interface Consistency:**
- ✅ Added `get_job_logs()` to `PlatformAPI` abstract base class
- ✅ All three platforms properly inherit from `PlatformAPI` ABC
- ✅ All abstract methods implemented consistently across platforms
- ✅ Verified all test files use dynamically generated `app_name` with `unique_suffix`
- ✅ No hardcoded app names in any test files
- ✅ Zero syntax/lint errors across all platform modules and tests

**Key Findings:**

1. **Semantic Consistency: EXCELLENT** - All three platforms implement the same conceptual operations
2. **Process Consistency: EXCELLENT** - Job lifecycle works identically across platforms
3. **Test Consistency: EXCELLENT** - All three test suites validate the same behaviors
4. **User Experience is Consistent** - Users moving between platforms find same methods, parameters, error handling

## Platform-Specific Logging Architecture (CRITICAL)

### Databricks: Jobs API run_output (stdout/stderr)

- **Primary Source**: Databricks Jobs API `get_run_output(task_run_id)`
- Returns last 5MB of stdout/stderr per task run (isolated per run)
- **Application logs via print()**: Test app uses `print()` statements to write to stdout
- **Cluster logs (fallback)**: ABFSS logs/{cluster_id}/driver/log4j-active.log (shared, not run-specific)
- **Why**: Jobs API provides run-isolated output, cluster logs are shared across all jobs

### Fabric & Synapse: Diagnostic Emitters (Log4j logs)

- **Primary Source**: Azure Storage diagnostic emitter logs
- Fabric path: `logs/{uuid}.{run_id}/driver/spark-logs`
- Synapse path: `logs/{workspace}.{pool}.{batch_id}/driver/spark-logs`
- **Application logs via app_logger.info()**: Test app uses Kindling logger (Log4j)
- **NO stdout capture**: No Jobs API equivalent to Databricks run_output
- **Why**: Diagnostic emitters write Log4j output, which is where app_logger writes to

### Test App Logging Pattern
```python
Universal test app (works on all 3 platforms)
test_id = str(uuid.uuid4())Databricks: print() goes to stdout (captured by Jobs API)
Fabric/Synapse: app_logger.info() goes to Log4j (captured by diagnostic emitters)
msg = f"TEST_ID={test_id} test={name} status={status}"
app_logger.info(msg)  # All platforms - Log4j
print(msg)            # Databricks only - stdout for Jobs API
```
### Critical Difference

- **Databricks**: Look for TEST_ID in **run_output.logs** (stdout from print())
- **Fabric/Synapse**: Look for TEST_ID in **diagnostic emitter logs** (Log4j from app_logger)

## Test App Architecture - CRITICAL PATTERNS

### NO MAIN FUNCTION IN TEST APPS ❌

Test apps are loaded and executed dynamically by the Kindling bootstrap system:
- They should NOT have `if __name__ == "__main__":` blocks
- They should NOT have `main()` functions
- Bootstrap script calls the app code directly without main() wrapper

### KINDLING LOGGER MUST BE AVAILABLE ❌

- If `SparkLoggerProvider().get_logger()` fails, the test should fail
- NO conditional logging (`if logger: ... else: print(...)`)
- Logger failure means bootstrap failed, which is a test failure condition
- Test apps should use logger unconditionally and fail fast if unavailable

### Correct Test App Pattern
```python
#!/usr/bin/env python3
"""
Test App Description
"""
import sys
from datetime import datetime
from kindling.spark_session import *
from kindling.spark_log_provider import SparkLoggerProviderdef get_logger():
"""Get logger from Kindling framework - fails fast if unavailable"""
logger_provider = SparkLoggerProvider()
return logger_provider.get_logger("test-app")def test_something(logger):
"""Test function"""
logger.info("Testing something...")
# Test logic here - use logger unconditionallyApp execution code runs directly (no main function!)
logger = get_logger()  # Will fail fast if bootstrap failed
logger.info("Starting test app...")Run tests
result = test_something(logger)Exit appropriately
sys.exit(0 if result else 1)
```

### Wrong Pattern (DO NOT USE)
```python

def main():
"""Main function - DON'T DO THIS!"""
# App logic here
passif name == "main":  # ❌ NEVER USE THIS
main()  # ❌ NEVER USE THIS
```

### Why This Matters

- Kindling bootstrap dynamically loads and executes Python files
- Having main() creates unnecessary function wrapper
- Bootstrap expects module-level executable code, not function calls
- Consistent with how Spark applications are typically structured

## CI/CD Strategy

### Every Commit (Push or PR)

- ✅ Unit tests - Always run (fast feedback)
- ✅ Integration tests - Always run (validates framework integration)
- ✅ Code quality checks - Always run (linting, formatting, security)
- ⚙️ Wheel builds - Only if source code changes or `[build wheels]` in commit message

### Release Events Only

- 🔒 System tests - All 3 platforms (Databricks, Synapse, Fabric) **MUST PASS**
- 🔒 Fail fast - Any platform failure **blocks the release**
- 📦 Wheel attachment - Only attaches wheels if system tests succeed

### Required GitHub Secrets/Variables for System Tests

- Fabric: `FABRIC_WORKSPACE_ID`, `FABRIC_LAKEHOUSE_ID`
- Synapse: `SYNAPSE_WORKSPACE_NAME`, `SYNAPSE_SPARK_POOL`
- Databricks: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_CLUSTER_ID`
- Azure (shared): `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`
- Storage: `AZURE_STORAGE_ACCOUNT` (var), `AZURE_CONTAINER` (var)

Note: Variable names match .env file format for consistency

## Service & Utility Architecture

### Current DI Services (Layer 1: Core)

- `PlatformServiceProvider` - Platform abstraction
- `ConfigService` - Configuration management
- `SparkSessionProvider` - Spark session management
- `PythonLoggerProvider` / `SparkLoggerProvider` - Logging
- `SparkTraceProvider` - Tracing/observability

### Current DI Services (Layer 2: Domain)

- `NotebookLoader` / `NotebookManager` - Notebook operations
- `DataEntityManager` - Entity management
- `DataPipesManager` - Data pipeline management
- `DataPipesExecuter` - Pipeline execution
- `WatermarkManager` - Watermarking service
- `StageProcessingService` - Stage processing

### Current DI Services (Layer 3: Application)

- `DataAppManager` / `AppManager` - App lifecycle management
- `PipManagerProvider` - Pip operations provider (wraps utility)

### Current Utility Classes

- `PipManager` - Pip operations (has both utility class AND DI provider wrapper)

### Extraction Principles

**Where DI Provides Real Value:**
- ✅ **High-level orchestration services** - DataEntityManager, DataPipesManager, WatermarkManager
- ✅ **Complex multi-service coordination** - Services with multiple injected dependencies
- ✅ **Business logic with swappable implementations** - Entity providers, custom orchestrators
- ✅ **Testing scenarios requiring mocks** - Complex service interactions

**Where Simple Utilities Are Sufficient:**
- ✅ **Mechanical operations** - File I/O, ZIP creation, parsing, pip commands
- ✅ **Low-level tools** - YAML parsing, code extraction, path manipulation
- ✅ **Single-purpose functions** - Nobody needs "custom YAML parser" implementations
- ✅ **Direct parameter passing** - Utilities can accept service instances directly without DI

**Key Insight:**
> The real power of DI is in higher-level services and their dependencies. If someone wants to roll their own ConfigService, they just pass their instance to a utility - no DI needed. DI creates flexibility at the orchestration layer, not the tool layer.

### Keep Utilities Pure

- ❌ No DI dependencies
- ❌ No service dependencies
- ✅ Accept parameters for everything (no hidden state)
- ✅ Can accept service instances directly as parameters when needed
- ✅ Keep interfaces minimal - focus on mechanical operations

### Module Organization Principle

> Extract utilities IN PLACE within the service module that uses them. Only create separate utility modules if the utility is used across multiple modules. This avoids disrupting current module structure.

### Final Architecture - Utilities Stay in Context

**Within data_apps.py module:**
```python
**Within data_apps.py module:**
```python
# data_apps.py - utilities stay local to the module
class DataAppPackager:
    """Utility for creating .kda files"""
    pass

class DataAppExtractor:
    """Utility for extracting .kda files"""
    pass

@inject
class DataAppManager:
    """Service that uses local utilities"""
    def __init__(self, platform: PlatformServiceProvider, ...):
        self.packager = DataAppPackager()  # Local utility
        self.extractor = DataAppExtractor()  # Local utility
```

**Within notebook_framework.py module:**
```python
# notebook_framework.py

class NotebookUtilities:
    """Core notebook operations - SHARED by both wheels and apps"""

    def __init__(self, platform_service):
        """Platform service provides notebook access (Synapse/Databricks/Fabric APIs)"""
        self.platform = platform_service

    def extract_code_from_notebook(self, notebook_name: str) -> str:
        """Get notebook from platform API and extract Python code from cells"""
        pass

    def scan_for_notebook_packages(self, root_path: str) -> List[str]:
        """Find folders with {folder}_init.Notebook marker using platform APIs"""
        pass

class NotebookWheelBuilder:
    """Build .whl packages FROM notebook folders"""

    def __init__(self):
        self.notebook_utils = NotebookUtilities()

    def build_wheel(self, folder: str, package_name: str, version: str, ...) -> str:
        """notebooks → Python files → pyproject.toml → pip wheel → .whl"""
        pass

class NotebookAppBuilder:
    """Build .kda packages FROM notebook folders"""

    def __init__(self):
        self.notebook_utils = NotebookUtilities()

    def build_app(self, folder: str, app_name: str, version: str, ...) -> str:
        """notebooks → Python files → app.yaml → .kda archive"""
        pass
```

### Dependency & Coupling Analysis

**Data Apps (data_apps.py):**
- `DataAppConstants` class - shared by both packaging AND extraction
- `KDAManifest` dataclass - created by packager, read by extractor
- Both utilities need to understand manifest structure

**DataAppPackager + DataAppExtractor SHOULD STAY TOGETHER:**
- Share `DataAppConstants` configuration
- Share `KDAManifest` data structure
- Share file format knowledge (.kda structure)
- Inverse operations (pack/unpack) on same artifact type
- **Recommendation: Keep in same module, but as separate classes**

**Notebook Utilities CAN BE SEPARATE:**
- `NotebookCodeExtractor` - standalone, no shared config
- `PyprojectGenerator` - standalone, no shared config
- `NotebookPackageBuilder` - orchestrator utility
- **Recommendation: Separate classes, builder composes the others**

## Implementation Plan (If Refactoring)

### Phase 1: Extract Data App Utilities (in data_apps.py)

**1.1 Create DataAppPackager utility class**
- Extract packaging logic from `package_app()` method
- Methods: `create_kda_package()`, `_add_files_to_kda()`, `_create_manifest()`
- Pure utility: accept all parameters explicitly, no DI
- Keep YAML/JSON operations inline (not worth separate utility)

**1.2 Create DataAppExtractor utility class**
- Extract extraction logic from `deploy_kda()` method
- Methods: `extract_kda()`, `validate_manifest()`, `read_manifest()`
- Pure utility: accept file paths, return extracted content/metadata

**1.3 Refactor DataAppManager to use utilities**
- `package_app()` → delegates to `DataAppPackager.create_kda_package()`
- `deploy_kda()` → uses `DataAppExtractor.extract_kda()` then platform services for deployment
- Keep orchestration logic: config loading, platform service calls, dependency resolution

### Phase 2: Extract Notebook Utilities (in notebook_framework.py)

**2.1 Create NotebookCodeExtractor utility class**
- Extract from `load_notebook_code()` method
- Methods: `extract_python_code()`, `parse_notebook_json()`, `strip_magic_commands()`
- Pure utility: accept notebook content, return Python code

**2.2 Create PyprojectGenerator utility class**
- Extract from `publish_notebook_folder_as_package()` method
- Methods: `generate_pyproject_toml()`, `format_dependencies()`, `create_readme()`
- Pure utility: accept metadata dict, return formatted strings

**2.3 Create NotebookPackageBuilder utility class**
- Extract wheel building logic from `publish_notebook_folder_as_package()`
- Methods: `build_wheel_from_notebooks()`, `create_package_structure()`, `run_pip_wheel()`
- Can compose NotebookCodeExtractor and PyprojectGenerator
- Pure utility: accept folder path and metadata, return wheel file path

**2.4 Refactor NotebookLoader/NotebookManager to use utilities**
- `publish_notebook_folder_as_package()` → delegates to `NotebookPackageBuilder.build_wheel_from_notebooks()`
- Keep orchestration: package registry, platform service calls, notebook execution

### Testing Strategy

**Unit Tests (per utility class)**
- Test each utility method in isolation
- Mock file system operations
- Test edge cases and error handling

**Integration Tests (service level)**
- Test DataAppManager with real utilities
- Test NotebookLoader with real utilities
- Verify end-to-end workflows still work

**System Tests (existing)**
- Run existing system tests to ensure no regressions
- Tests in `/workspace/tests/system/` should pass unchanged
