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
