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

---

## In-Memory Entity Provider as Reusable Views

### Idea

Use the existing `MemoryEntityProvider` (`entity_provider_memory.py`) to store and read DataFrames as reusable "views" that can be shared across multiple pipes within a single execution run.

### Motivation

Some pipe graphs have intermediate results that are consumed by multiple downstream pipes. Today each downstream pipe either re-reads from a persisted table or re-computes the result. A memory-backed entity would let an upstream pipe write a DataFrame once to the in-memory store and have multiple downstream pipes read it cheaply — effectively a named, cached view with no I/O overhead.

### How It Would Work

1. **Upstream pipe** writes its output to a memory entity (e.g., `entityid="view.enriched_events"`, `provider_type="memory"`)
2. **Multiple downstream pipes** declare the same memory entity as an input and read the cached DataFrame via `MemoryEntityProvider.read_entity()`
3. The `_memory_store` dict on the singleton `MemoryEntityProvider` acts as the shared namespace
4. Views are scoped to a single execution run — they do not survive job restarts

### What Already Exists

- `MemoryEntityProvider._memory_store: Dict[str, DataFrame]` — in-memory dict keyed by entity ID
- `write_to_entity()` stores a DataFrame in both a Spark memory table and the dict
- `read_entity()` reads from the Spark table first, falls back to the dict
- The provider is registered as a singleton via `@GlobalInjector.singleton_autobind()`

### What Would Be Needed

- **Entity naming convention**: e.g., `view.*` prefix for memory-only entities, so they are visually distinct in configuration and logs
- **Graph ordering guarantee**: the execution strategy must schedule the producing pipe before all consuming pipes — the existing `PipeGraph` topological sort already handles this
- **Lifecycle / cleanup**: clear `_memory_store` entries after execution completes (or after all consumers have read) to avoid holding large DataFrames in driver memory indefinitely
- **Unpersist integration**: call `df.unpersist()` on eviction if the DataFrame was cached, to free Spark executor memory
- **Documentation / examples**: show the pattern in a data-app example so teams adopt it consistently

### SQL View Pipe Registration

Building on in-memory views, add a `DataPipes.view()` decorator that defines a pipe purely via SQL. The decorator takes incoming entity DataFrames, registers them as temporary Spark views, runs the SQL, and outputs the result to a memory entity — creating a reusable, named view available to downstream pipes.

#### Proposed API

```python
@DataPipes.view(
    pipeid="view.enriched_orders",
    name="enriched_orders",
    input_entity_ids=["bronze.orders", "bronze.customers"],
    output_entity_id="view.enriched_orders",   # memory entity
    output_type="memory",
    sql="""
        SELECT o.*, c.name AS customer_name, c.segment
        FROM bronze_orders o
        JOIN bronze_customers c ON o.customer_id = c.id
        WHERE o.status = 'completed'
    """,
)
def enriched_orders():
    pass  # no-op — SQL does the work
```

Downstream pipes consume the view as a regular input entity:

```python
@DataPipes.pipe(
    pipeid="gold.revenue_by_segment",
    name="revenue_by_segment",
    input_entity_ids=["view.enriched_orders"],
    output_entity_id="gold.revenue_by_segment",
    output_type="delta",
    tags={},
)
def revenue_by_segment(view_enriched_orders):
    return view_enriched_orders.groupBy("segment").agg(sum("amount").alias("total"))

@DataPipes.pipe(
    pipeid="gold.top_customers",
    name="top_customers",
    input_entity_ids=["view.enriched_orders"],
    output_entity_id="gold.top_customers",
    output_type="delta",
    tags={},
)
def top_customers(view_enriched_orders):
    return view_enriched_orders.groupBy("customer_name").agg(sum("amount").alias("total")).orderBy(desc("total"))
```

#### How It Would Work Internally

1. `DataPipes.view()` registers a pipe whose `execute` function is auto-generated:
   - For each `input_entity_id`, the corresponding DataFrame is registered as a Spark temp view using `df.createOrReplaceTempView(entity_id.replace(".", "_"))`
   - `spark.sql(sql)` runs the provided query
   - The result is returned as the pipe output
2. The output entity uses `provider_type="memory"`, so it lands in `MemoryEntityProvider._memory_store`
3. Downstream pipes read the view via normal entity reads — no re-computation
4. If no `sql` is provided, a passthrough view is created (single input entity aliased as the output entity — useful for caching)

#### Design Considerations

- **No new abstractions**: `view()` is syntactic sugar that registers a normal `PipeMetadata` with an auto-generated `execute` function — the execution engine sees no difference
- **SQL validation**: optionally validate the SQL at registration time by parsing it (e.g., via `spark.sessionState.sqlParser`) to catch typos early
- **Hybrid pipes**: allow an optional `post_process` callable on `view()` for cases where SQL gets 90% of the way but a final PySpark transform is needed
- **Tags**: auto-tag view pipes with `{"pipe_type": "view", "provider_type": "memory"}` so execution strategies and logging can distinguish them
