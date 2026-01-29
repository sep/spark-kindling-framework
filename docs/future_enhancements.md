# Future Enhancements

This document captures planned enhancements and architectural considerations for future development.

## Workspace-Level Configuration (Future)

### Current State

Configuration hierarchy exists as:
1. **Global** (`config/settings.yaml`)
2. **Environment** (`config/{env}.yaml`)
3. **Bootstrap** (`BOOTSTRAP_CONFIG` dict)
4. **App** (`data-apps/{app}/app.yaml`)

Extensions/wheels can be loaded globally via:
```yaml
kindling:
  bootstrap:
    required_packages:
      - kindling-otel-azure>=0.1.0
```

### Proposed Enhancement: Workspace-Specific Config

Add workspace-level configuration between Global and App levels:

#### Directory Structure
```
artifacts/
├── config/
│   ├── settings.yaml              # Global
│   ├── development.yaml           # Environment
│   └── production.yaml
│
├── workspaces/                    # NEW
│   ├── data-engineering/
│   │   └── config.yaml            # Workspace-specific
│   ├── analytics/
│   │   └── config.yaml
│   └── ml-platform/
│       └── config.yaml
│
└── data-apps/
    └── ...
```

#### Configuration Priority (Bottom overrides top)
```
1. Global      (config/settings.yaml)
2. Workspace   (workspaces/{name}/config.yaml)  ← NEW
3. Environment (config/{env}.yaml)
4. Bootstrap   (BOOTSTRAP_CONFIG dict)
5. App         (data-apps/{app}/app.yaml)
```

#### Workspace Config Example
```yaml
# workspaces/data-engineering/config.yaml

workspace:
  name: "data-engineering"
  description: "Data engineering platform workspace"

# Workspace-level extensions (all apps in workspace)
extensions:
  - kindling-otel-azure>=0.1.0
  - custom-transformers==2.1.0

# Workspace-specific settings
azure_monitor:
  connection_string: "InstrumentationKey=..."
  enable_logging: true

telemetry:
  logging:
    level: "DEBUG"

spark_configs:
  spark.executor.memory: "4g"
```

#### Implementation Points

1. **Workspace Detection**
   ```python
   # Option A: Explicit
   BOOTSTRAP_CONFIG = {
       'workspace_name': 'data-engineering',
   }

   # Option B: Auto-detect from Spark config
   spark.kindling.workspace = "data-engineering"

   # Option C: Environment variable
   export KINDLING_WORKSPACE="data-engineering"

   # Option D: Default to "default" workspace
   workspace_name = detect_workspace() or "default"
   ```

2. **Loading Logic in `download_config_files()`**
   ```python
   def download_config_files(
       artifacts_storage_path: str,
       environment: str,
       workspace_name: Optional[str] = None,  # NEW
       app_name: Optional[str] = None
   ) -> List[str]:
       config_files = []

       # Global
       try_download(f"{base}/config/settings.yaml", config_files)

       # Workspace (if specified and exists)
       if workspace_name:
           try_download(f"{base}/workspaces/{workspace_name}/config.yaml", config_files)

       # Environment
       try_download(f"{base}/config/{environment}.yaml", config_files)

       return config_files
   ```

3. **Extension Installation**
   ```python
   # After required_packages, before platform services
   extensions = config_service.get("kindling.extensions", [])
   workspace_extensions = config_service.get("workspace.extensions", [])
   all_extensions = extensions + workspace_extensions

   if all_extensions:
       pip_manager.install_packages(all_extensions)
   ```

#### Benefits
- ✅ **Workspace isolation** - Each workspace has its own config
- ✅ **Optional** - Only loads if workspace exists (no errors)
- ✅ **Clear hierarchy** - App > Env > Workspace > Global
- ✅ **No breaking changes** - Existing apps work without workspace config
- ✅ **Flexible** - Can add per-workspace extensions without modifying global config

#### Future Extensions (Optional)

**Named Config Groups** - If duplication emerges across workspaces:
```yaml
# workspaces/data-engineering/config.yaml
extends: "config/groups/production"  # Reference shared group

# Workspace-specific overrides
extensions:
  - custom-package
```

Start simple with direct workspace configs; add groups only if clear duplication patterns emerge.

### References
- Issue #13: Azure Monitor OpenTelemetry Integration (driver for workspace-level extension loading)
- Current bootstrap mechanism: `install_bootstrap_dependencies()` in `packages/kindling/bootstrap.py`
- Current config loading: `download_config_files()` in `packages/kindling/bootstrap.py`

## Extension Test Validation via Application Insights

### Current Challenge

Extension tests load the Azure Monitor extension which redirects logs to Application Insights instead of diagnostic emitters. This means:

- **Main system tests**: Logs → diagnostic emitters → test framework validates via storage ✅
- **Extension tests**: Logs → Application Insights → diagnostic logs are empty → test times out ⏱️

The timeout is actually **proof the extension works** (logs successfully redirected), but we can't validate the actual log content.

### Proposed Solution: App Insights Query Integration

**Add Application Insights querying capability to extension tests:**

```python
# New test utility
class AppInsightsValidator:
    def __init__(self, workspace_id, credential):
        from azure.monitor.query import LogsQueryClient
        self.client = LogsQueryClient(credential)
        self.workspace_id = workspace_id

    def query_test_logs(self, test_id, start_time, timeout=300):
        """Query App Insights for test execution logs"""
        query = f"""
        union traces, requests, dependencies, exceptions
        | where timestamp > datetime({start_time})
        | where customDimensions.test_id == '{test_id}'
        | project timestamp, message, severityLevel, customDimensions
        | order by timestamp asc
        """
        return self._poll_with_retry(query, timeout)
```

**Implementation Strategy:**

1. **Phase 1 (Short-term)**: Update extension tests to only verify job completion, skip log validation
   - Fast, no new dependencies
   - Job success implies extension loaded correctly

2. **Phase 2 (Medium-term)**: Add optional App Insights query utility for manual validation
   - Script/notebook for developers to validate extension behavior
   - Query specific test runs by test_id
   - Useful for debugging, not blocking tests

3. **Phase 3 (Long-term)**: Full App Insights integration with configurable validation depth
   - Environment flag: `VALIDATE_APPINSIGHTS=true`
   - Only for critical releases (accept 5-10 min test time for ingestion delays)
   - Validates logs actually reached App Insights with expected content

**Key Considerations:**

- **Timing**: App Insights ingestion has 90s-5min delay, need polling with backoff
- **Dependencies**: Requires `azure-monitor-query` package
- **Authentication**: Use `DefaultAzureCredential` (works for local and CI/CD)
- **Cost**: Minimal (few queries per test run)
- **Permissions**: Needs `Monitoring Reader` role on Log Analytics workspace

**Environment Variables:**
```bash
export APPINSIGHTS_WORKSPACE_ID="..."  # Log Analytics workspace ID
export APPINSIGHTS_CONNECTION_STRING="..."  # Optional
export VALIDATE_APPINSIGHTS=true  # Enable App Insights validation
```

### References
- Extension tests: `tests/system/extensions/azure-monitor/test_azure_monitor_extension.py`
- Azure Monitor Query SDK: https://learn.microsoft.com/python/api/azure-monitor-query/
- Log Analytics Kusto queries: https://learn.microsoft.com/azure/azure-monitor/logs/log-query-overview
