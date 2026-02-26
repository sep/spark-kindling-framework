# Signal Opportunities Evaluation - Kindling Framework

**Purpose**: Comprehensive evaluation of where signals would add value across all providers and services in the Kindling framework.

**Evaluation Date**: January 2025
**Status**: Historical analysis snapshot (superseded by implemented runtime signals)
**Signal Convention at evaluation time**: Django-style `pre_/post_` prefix pattern

> [!IMPORTANT]
> This document captures pre-implementation analysis from January 2025.
> Many items discussed here are now implemented in code.
> For current signal names and usage, use `docs/signal_quick_reference.md`.

---

## Executive Summary

This document evaluates signal opportunities across 15+ Kindling providers/services. Each is rated for signal value and includes specific signal recommendations.

### Rating Scale
- 🔥 **CRITICAL** - Signals essential for production monitoring, auditing, and integration
- ⚡ **HIGH** - Signals provide significant operational value
- 📊 **MODERATE** - Signals useful for specific use cases
- 💤 **LOW** - Minimal benefit, too simple or no state changes

### Quick Reference

| Provider | Rating | Primary Use Cases |
|----------|--------|-------------------|
| **DataPipesExecuter** | 🔥 CRITICAL | Pipeline orchestration, monitoring, error handling |
| **DataEntityManager** | 🔥 CRITICAL | Data governance, audit trails, schema evolution |
| **FileIngestionProcessor** | 🔥 CRITICAL | File processing monitoring, error recovery |
| **WatermarkManager** | ⚡ HIGH | Incremental processing monitoring, CDC tracking |
| **DataAppManager** | ⚡ HIGH | App lifecycle monitoring, deployment tracking |
| **PipManager** | ⚡ HIGH | Dependency management, long-running operations |
| **NotebookLoader** | ⚡ HIGH | Package management, build monitoring |
| **StageProcessor** | ⚡ HIGH | Stage execution monitoring, watermark tracking |
| **Bootstrap** | ⚡ HIGH | Initialization monitoring, config validation |
| **SparkSessionProvider** | 📊 MODERATE | Session lifecycle, configuration changes |
| **PlatformService** | 📊 MODERATE | Platform operations, storage access |
| **SparkTraceProvider** | 📊 MODERATE | Already has spans, signals could complement |
| **SparkLoggerProvider** | 💤 LOW | Too simple, no state changes |
| **SignalProvider** | 💤 LOW | Meta-level, would be circular |
| **ConfigService** | ✅ DONE | Already implemented with reload signals |

---

## 🔥 CRITICAL PRIORITY

### 1. DataPipesExecuter (data_pipes.py)

**Rating**: 🔥 CRITICAL
**Current State**: Has SparkTraceProvider spans, no signals
**Signal Value**: Essential for pipeline monitoring, error handling, and integration

#### Why Signals Are Critical
- **Orchestration visibility**: Multiple pipes executing in sequence/parallel
- **Error monitoring**: Pipeline failures need immediate notification
- **Performance tracking**: Long-running operations need progress monitoring
- **Integration hooks**: Other services need to react to pipeline events (e.g., watermark updates after successful pipeline)
- **Audit requirements**: Data lineage and execution history tracking

#### Recommended Signals

```python
# Pipeline lifecycle
datapipes.pre_run        # Before run_datapipes() execution
datapipes.post_run       # After successful run_datapipes() completion
datapipes.run_failed     # When run_datapipes() fails

# Individual pipe execution
datapipes.pre_pipe       # Before _execute_datapipe() for specific pipe
datapipes.post_pipe      # After successful pipe execution
datapipes.pipe_failed    # When specific pipe fails
datapipes.pipe_skipped   # When pipe skipped due to conditions
```

#### Signal Payloads

```python
# datapipes.pre_run
{
    "pipe_ids": ["bronze.customers", "silver.customers"],
    "pipe_count": 2,
    "run_id": "uuid",
    "timestamp": "2025-01-20T10:00:00Z"
}

# datapipes.post_run
{
    "pipe_ids": ["bronze.customers", "silver.customers"],
    "pipe_count": 2,
    "success_count": 2,
    "failed_count": 0,
    "duration_seconds": 45.2,
    "run_id": "uuid",
    "timestamp": "2025-01-20T10:00:45Z"
}

# datapipes.run_failed
{
    "pipe_ids": ["bronze.customers", "silver.customers"],
    "failed_pipe": "silver.customers",
    "error": "ValueError: Invalid schema",
    "error_type": "ValueError",
    "duration_seconds": 12.5,
    "run_id": "uuid",
    "timestamp": "2025-01-20T10:00:12Z"
}

# datapipes.pre_pipe
{
    "pipe_id": "bronze.customers",
    "pipe_name": "Extract Customer Data",
    "run_id": "uuid",
    "pipe_index": 0,
    "total_pipes": 2,
    "timestamp": "2025-01-20T10:00:00Z"
}

# datapipes.post_pipe
{
    "pipe_id": "bronze.customers",
    "pipe_name": "Extract Customer Data",
    "duration_seconds": 23.1,
    "rows_processed": 15000,
    "run_id": "uuid",
    "timestamp": "2025-01-20T10:00:23Z"
}

# datapipes.pipe_failed
{
    "pipe_id": "silver.customers",
    "pipe_name": "Transform Customers",
    "error": "ValueError: Invalid schema",
    "error_type": "ValueError",
    "duration_seconds": 5.2,
    "run_id": "uuid",
    "timestamp": "2025-01-20T10:00:28Z"
}
```

#### Use Cases
1. **Real-time monitoring dashboard**: Subscribe to pipeline signals to show execution status
2. **Error alerting**: Send notifications when `datapipes.run_failed` or `datapipes.pipe_failed`
3. **Performance analysis**: Track duration trends across pipeline runs
4. **Dependent workflows**: Trigger downstream processes after `datapipes.post_run`
5. **Audit logging**: Record all pipeline executions for compliance

#### Implementation Considerations
- Signals should be emitted even if SparkTraceProvider spans are active (complementary)
- Signal payloads should be lightweight (no DataFrames or large objects)
- Failed signals should include error details but not full stack traces (use logger for that)
- Consider adding `pipe_metadata` from pipe registry to signal payloads

---

### 2. DataEntityManager (data_entities.py)

**Rating**: 🔥 CRITICAL
**Current State**: No signals, EntityProvider abstraction layer
**Signal Value**: Essential for data governance, audit trails, and schema evolution tracking

#### Why Signals Are Critical
- **Data governance**: Track all write operations to entities (tables)
- **Audit compliance**: Who wrote what data when (required for regulatory compliance)
- **Schema evolution**: Track table schema changes and migrations
- **Data quality**: Monitor merge operations, detect anomalies
- **Integration**: Trigger downstream processes after entity updates (e.g., cache invalidation)

#### Recommended Signals

```python
# Table lifecycle
entity.pre_ensure_table   # Before ensure_entity_table()
entity.post_ensure_table  # After table created/verified
entity.ensure_failed      # When table creation fails

# Write operations
entity.pre_merge          # Before merge_to_entity()
entity.post_merge         # After successful merge
entity.merge_failed       # When merge fails

entity.pre_append         # Before append_to_entity()
entity.post_append        # After successful append
entity.append_failed      # When append fails

entity.pre_write          # Before write_to_entity()
entity.post_write         # After successful write
entity.write_failed       # When write fails

# Schema changes
entity.schema_changed     # When schema evolution detected
entity.version_updated    # When entity version incremented
```

#### Signal Payloads

```python
# entity.pre_merge
{
    "entity_id": "customers",
    "entity_name": "bronze_customers",
    "merge_keys": ["customer_id"],
    "source_row_count": 5000,
    "operation": "merge",
    "timestamp": "2025-01-20T10:00:00Z"
}

# entity.post_merge
{
    "entity_id": "customers",
    "entity_name": "bronze_customers",
    "merge_keys": ["customer_id"],
    "rows_inserted": 500,
    "rows_updated": 4500,
    "rows_deleted": 0,
    "duration_seconds": 12.3,
    "timestamp": "2025-01-20T10:00:12Z"
}

# entity.merge_failed
{
    "entity_id": "customers",
    "entity_name": "bronze_customers",
    "merge_keys": ["customer_id"],
    "error": "Delta table not found",
    "error_type": "AnalysisException",
    "duration_seconds": 0.5,
    "timestamp": "2025-01-20T10:00:00Z"
}

# entity.schema_changed
{
    "entity_id": "customers",
    "entity_name": "bronze_customers",
    "old_schema": ["id", "name", "email"],
    "new_schema": ["id", "name", "email", "phone"],
    "added_columns": ["phone"],
    "removed_columns": [],
    "modified_columns": [],
    "timestamp": "2025-01-20T10:00:00Z"
}

# entity.version_updated
{
    "entity_id": "customers",
    "entity_name": "bronze_customers",
    "old_version": "1.0.0",
    "new_version": "1.1.0",
    "reason": "schema_evolution",
    "timestamp": "2025-01-20T10:00:00Z"
}
```

#### Use Cases
1. **Audit trail**: Log all entity write operations for compliance
2. **Data lineage**: Track data flow through entity write signals
3. **Schema monitoring**: Alert when schema changes detected
4. **Performance tracking**: Monitor merge/append/write durations
5. **Integration**: Invalidate caches after entity updates
6. **Quality gates**: Validate row counts after merge operations

#### Implementation Considerations
- Signals should be emitted at the EntityProvider level (DeltaEntityProvider, etc.)
- Row counts should be approximate for performance (use DataFrame count caching)
- Schema comparison should be efficient (hash-based comparison)
- Consider adding user/session context to payloads for audit trails
- Merge statistics (inserted/updated/deleted) may require Delta merge stats API

---

### 3. FileIngestionProcessor (file_ingestion.py)

**Rating**: 🔥 CRITICAL
**Current State**: Has SparkTraceProvider spans, no signals
**Signal Value**: Essential for file processing monitoring, error recovery, and retry logic

#### Why Signals Are Critical
- **File tracking**: Monitor which files are being processed
- **Error recovery**: Identify failed files for retry
- **Performance**: Track processing duration per file/batch
- **Data quality**: Validate file ingestion success rates
- **Operational visibility**: Show file ingestion dashboard

#### Recommended Signals

```python
# Batch processing
file_ingestion.pre_process        # Before process_path() starts
file_ingestion.post_process       # After successful batch processing
file_ingestion.process_failed     # When batch processing fails

# Individual file processing
file_ingestion.pre_file           # Before processing individual file
file_ingestion.post_file          # After successful file processing
file_ingestion.file_failed        # When file processing fails
file_ingestion.file_moved         # When file moved after processing

# Batch operations
file_ingestion.batch_built        # When DataFrame batch plan built (lazy)
file_ingestion.batch_written      # When batch written to entity
```

#### Signal Payloads

```python
# file_ingestion.pre_process
{
    "path": "/data/landing/2025-01-20/",
    "pattern": "customers_*.csv",
    "file_count": 45,
    "batch_id": "uuid",
    "timestamp": "2025-01-20T10:00:00Z"
}

# file_ingestion.post_process
{
    "path": "/data/landing/2025-01-20/",
    "pattern": "customers_*.csv",
    "total_files": 45,
    "success_files": 44,
    "failed_files": 1,
    "total_rows": 125000,
    "duration_seconds": 67.5,
    "batch_id": "uuid",
    "timestamp": "2025-01-20T10:01:07Z"
}

# file_ingestion.process_failed
{
    "path": "/data/landing/2025-01-20/",
    "pattern": "customers_*.csv",
    "error": "Schema mismatch in file",
    "error_type": "AnalysisException",
    "processed_files": 12,
    "failed_file": "customers_013.csv",
    "duration_seconds": 23.1,
    "batch_id": "uuid",
    "timestamp": "2025-01-20T10:00:23Z"
}

# file_ingestion.pre_file
{
    "filename": "customers_001.csv",
    "path": "/data/landing/2025-01-20/customers_001.csv",
    "entry_id": "bronze_customers",
    "dest_entity": "bronze.customers",
    "file_size_bytes": 52428800,  # 50MB
    "batch_id": "uuid",
    "file_index": 0,
    "total_files": 45,
    "timestamp": "2025-01-20T10:00:00Z"
}

# file_ingestion.post_file
{
    "filename": "customers_001.csv",
    "path": "/data/landing/2025-01-20/customers_001.csv",
    "entry_id": "bronze_customers",
    "dest_entity": "bronze.customers",
    "rows_processed": 3500,
    "duration_seconds": 1.2,
    "batch_id": "uuid",
    "timestamp": "2025-01-20T10:00:01Z"
}

# file_ingestion.file_moved
{
    "filename": "customers_001.csv",
    "source_path": "/data/landing/2025-01-20/customers_001.csv",
    "dest_path": "/data/processed/2025-01-20/customers_001.csv",
    "entry_id": "bronze_customers",
    "batch_id": "uuid",
    "timestamp": "2025-01-20T10:00:01Z"
}
```

#### Use Cases
1. **File monitoring dashboard**: Show files being processed in real-time
2. **Error recovery**: Retry failed files identified by `file_ingestion.file_failed`
3. **Performance analysis**: Track file processing times, identify slow files
4. **Data quality**: Validate row counts and success rates
5. **Operational alerts**: Notify when batch processing fails
6. **Audit trail**: Track file movements and processing history

#### Implementation Considerations
- Emit signals from ParallelizingFileIngestionProcessor
- File-level signals should be emitted even in parallel execution
- Consider batching signal emissions for high file counts (don't emit 10,000 file signals)
- File size and row counts should be included for performance analysis
- Consider adding file hash/checksum for data integrity tracking

---

## ⚡ HIGH PRIORITY

### 4. WatermarkManager (watermarking.py)

**Rating**: ⚡ HIGH
**Current State**: No signals, manages incremental processing watermarks
**Signal Value**: High value for monitoring CDC operations and incremental load tracking

#### Why Signals Are Valuable
- **CDC monitoring**: Track change data capture progress
- **Incremental load tracking**: Monitor watermark advancement
- **Stale data detection**: Alert when watermarks haven't updated
- **Reprocessing**: Track watermark resets for full reloads
- **Version tracking**: Monitor version increments

#### Recommended Signals

```python
watermark.pre_get          # Before get_watermark()
watermark.watermark_found  # When watermark retrieved (existing)
watermark.watermark_missing # When watermark not found (first run)

watermark.pre_save         # Before save_watermark()
watermark.post_save        # After watermark saved successfully
watermark.save_failed      # When save_watermark() fails

watermark.watermark_advanced # When watermark moves forward
watermark.watermark_reset    # When watermark reset (reprocessing)
watermark.version_changed    # When entity version changes
```

#### Signal Payloads

```python
# watermark.watermark_found
{
    "entity_id": "customers",
    "entity_name": "bronze.customers",
    "watermark_value": "2025-01-20T10:00:00Z",
    "version": "1.0.0",
    "last_updated": "2025-01-20T09:00:00Z",
    "age_hours": 1.0,
    "timestamp": "2025-01-20T10:00:00Z"
}

# watermark.post_save
{
    "entity_id": "customers",
    "entity_name": "bronze.customers",
    "old_watermark": "2025-01-20T09:00:00Z",
    "new_watermark": "2025-01-20T10:00:00Z",
    "watermark_delta": "1 hour",
    "version": "1.0.0",
    "timestamp": "2025-01-20T10:00:00Z"
}

# watermark.watermark_advanced
{
    "entity_id": "customers",
    "entity_name": "bronze.customers",
    "old_watermark": "2025-01-20T09:00:00Z",
    "new_watermark": "2025-01-20T10:00:00Z",
    "advancement_seconds": 3600,
    "version": "1.0.0",
    "timestamp": "2025-01-20T10:00:00Z"
}

# watermark.watermark_reset
{
    "entity_id": "customers",
    "entity_name": "bronze.customers",
    "old_watermark": "2025-01-20T09:00:00Z",
    "new_watermark": "2025-01-01T00:00:00Z",
    "reason": "full_reload",
    "version": "1.0.0",
    "timestamp": "2025-01-20T10:00:00Z"
}
```

#### Use Cases
1. **CDC monitoring**: Track incremental load progress across entities
2. **Stale data alerts**: Alert when watermark age exceeds threshold
3. **Reprocessing tracking**: Monitor full reload operations
4. **Performance**: Track watermark advancement rates
5. **Debugging**: Identify entities with watermark issues

---

### 5. DataAppManager (data_apps.py)

**Rating**: ⚡ HIGH
**Current State**: No signals, manages app lifecycle
**Signal Value**: High value for deployment monitoring and app lifecycle tracking

#### Why Signals Are Valuable
- **Deployment monitoring**: Track app packaging and deployment
- **Lifecycle visibility**: Monitor app install/uninstall operations
- **Error tracking**: Identify deployment failures
- **Audit trail**: Track who deployed what when
- **Integration**: Trigger post-deployment actions (tests, notifications)

#### Recommended Signals

```python
# Packaging
app.pre_package           # Before DataAppPackage.create()
app.post_package          # After successful .kda creation
app.package_failed        # When packaging fails

# Deployment
app.pre_deploy            # Before deploy_kda()
app.post_deploy           # After successful deployment
app.deploy_failed         # When deployment fails

# Running
app.pre_run               # Before run_app()
app.post_run              # After successful app run
app.run_failed            # When app execution fails

# Lifecycle
app.installed             # When app installed successfully
app.uninstalled           # When app removed
```

#### Signal Payloads

```python
# app.pre_package
{
    "app_name": "customer-etl",
    "app_directory": "/apps/customer-etl",
    "version": "1.0.0",
    "target_platform": "fabric",
    "merge_platform_config": True,
    "timestamp": "2025-01-20T10:00:00Z"
}

# app.post_package
{
    "app_name": "customer-etl",
    "version": "1.0.0",
    "kda_file": "/artifacts/customer-etl-1.0.0.kda",
    "kda_size_bytes": 1048576,  # 1MB
    "target_platform": "fabric",
    "duration_seconds": 2.3,
    "timestamp": "2025-01-20T10:00:02Z"
}

# app.pre_deploy
{
    "app_name": "customer-etl",
    "version": "1.0.0",
    "kda_file": "/artifacts/customer-etl-1.0.0.kda",
    "target_environment": "prod",
    "timestamp": "2025-01-20T10:00:00Z"
}

# app.post_deploy
{
    "app_name": "customer-etl",
    "version": "1.0.0",
    "deploy_path": "/data-apps/customer-etl/",
    "target_environment": "prod",
    "file_count": 12,
    "duration_seconds": 5.7,
    "timestamp": "2025-01-20T10:00:05Z"
}

# app.run_failed
{
    "app_name": "customer-etl",
    "error": "ImportError: No module named 'requests'",
    "error_type": "ImportError",
    "duration_seconds": 0.3,
    "timestamp": "2025-01-20T10:00:00Z"
}
```

#### Use Cases
1. **Deployment dashboard**: Show app deployment status
2. **Error alerting**: Notify on deployment failures
3. **Audit logging**: Track all app deployments
4. **Integration**: Run tests after deployment
5. **Version tracking**: Monitor app version rollouts

---

### 6. PipManager (pip_manager.py)

**Rating**: ⚡ HIGH
**Current State**: No signals, manages pip operations
**Signal Value**: High value for dependency management monitoring

#### Why Signals Are Valuable
- **Long-running operations**: Pip installs can take minutes
- **Failure tracking**: Dependency resolution failures need alerting
- **Audit trail**: Track package installations for security/compliance
- **Performance**: Monitor install durations, identify slow packages
- **Cache behavior**: Track when pip uses cached vs. fresh downloads

#### Recommended Signals

```python
# Package operations
pip.pre_install           # Before install_packages()
pip.post_install          # After successful install
pip.install_failed        # When install fails

pip.pre_wheel             # Before wheel operation
pip.post_wheel            # After wheel operation
pip.wheel_failed          # When wheel operation fails

# Individual packages
pip.package_installing    # Before installing specific package
pip.package_installed     # After package installed
pip.package_failed        # When specific package fails
```

#### Signal Payloads

```python
# pip.pre_install
{
    "packages": ["pandas==2.0.0", "requests>=2.28.0"],
    "package_count": 2,
    "operation_id": "uuid",
    "timestamp": "2025-01-20T10:00:00Z"
}

# pip.post_install
{
    "packages": ["pandas==2.0.0", "requests>=2.28.0"],
    "success_count": 2,
    "failed_count": 0,
    "duration_seconds": 45.2,
    "cached_packages": ["requests"],
    "downloaded_packages": ["pandas"],
    "operation_id": "uuid",
    "timestamp": "2025-01-20T10:00:45Z"
}

# pip.install_failed
{
    "packages": ["pandas==2.0.0", "invalid-package==1.0.0"],
    "failed_package": "invalid-package==1.0.0",
    "error": "ERROR: Could not find a version that satisfies the requirement",
    "error_type": "PackageNotFoundError",
    "duration_seconds": 12.3,
    "operation_id": "uuid",
    "timestamp": "2025-01-20T10:00:12Z"
}

# pip.package_installed
{
    "package": "pandas==2.0.0",
    "cached": False,
    "duration_seconds": 23.5,
    "dependencies_installed": ["numpy", "python-dateutil"],
    "operation_id": "uuid",
    "timestamp": "2025-01-20T10:00:23Z"
}
```

#### Use Cases
1. **Progress monitoring**: Show package installation progress
2. **Failure alerting**: Notify when dependency resolution fails
3. **Performance tracking**: Identify slow package installs
4. **Security audit**: Track all package installations
5. **Cache optimization**: Monitor cache hit rates

---

### 7. NotebookLoader (notebook_framework.py)

**Rating**: ⚡ HIGH
**Current State**: No signals, manages notebook packages
**Signal Value**: High value for package build monitoring

#### Why Signals Are Valuable
- **Build monitoring**: Track notebook package builds
- **Error tracking**: Identify notebook conversion/build failures
- **Performance**: Monitor build durations
- **Integration**: Trigger tests after package builds
- **Audit trail**: Track package publications

#### Recommended Signals

```python
# Package discovery
notebooks.packages_discovered  # After get_all_packages()

# Package building
notebooks.pre_build            # Before publish_notebook_folder_as_package()
notebooks.post_build           # After successful wheel build
notebooks.build_failed         # When build fails

# Notebook operations
notebooks.notebook_loaded      # After loading notebook code
notebooks.notebook_executed    # After executing notebook
notebooks.notebook_failed      # When notebook execution fails
```

#### Signal Payloads

```python
# notebooks.packages_discovered
{
    "packages": ["test-package-one", "test-package-two"],
    "package_count": 2,
    "scan_path": "/notebooks",
    "duration_seconds": 0.5,
    "timestamp": "2025-01-20T10:00:00Z"
}

# notebooks.pre_build
{
    "package_name": "test-package-one",
    "folder_path": "/notebooks/test-package-one",
    "version": "1.0.0",
    "notebook_count": 5,
    "timestamp": "2025-01-20T10:00:00Z"
}

# notebooks.post_build
{
    "package_name": "test-package-one",
    "version": "1.0.0",
    "wheel_file": "/artifacts/test_package_one-1.0.0-py3-none-any.whl",
    "wheel_size_bytes": 524288,  # 512KB
    "notebook_count": 5,
    "duration_seconds": 8.5,
    "timestamp": "2025-01-20T10:00:08Z"
}

# notebooks.build_failed
{
    "package_name": "test-package-one",
    "version": "1.0.0",
    "error": "SyntaxError in notebook: test_package_one_init",
    "error_type": "SyntaxError",
    "duration_seconds": 2.1,
    "timestamp": "2025-01-20T10:00:02Z"
}
```

#### Use Cases
1. **Build monitoring**: Track notebook package builds
2. **Error alerting**: Notify when builds fail
3. **Performance tracking**: Monitor build durations
4. **Integration**: Run tests after package build
5. **Audit logging**: Track package publications

---

### 8. StageProcessor (simple_stage_processor.py)

**Rating**: ⚡ HIGH
**Current State**: Has SparkTraceProvider spans, no signals
**Signal Value**: High value for stage execution monitoring

#### Why Signals Are Valuable
- **Stage monitoring**: Track stage execution lifecycle
- **Watermark tracking**: Monitor watermark entity operations
- **Pipeline integration**: Coordinate with DataPipesExecuter
- **Performance**: Track stage durations
- **Error handling**: Identify stage failures

#### Recommended Signals

```python
stage.pre_execute        # Before execute()
stage.post_execute       # After successful execution
stage.execute_failed     # When execution fails

stage.watermark_ensured  # After ensuring watermark entity
stage.pipes_started      # Before running stage pipes
stage.pipes_completed    # After all pipes complete
```

#### Signal Payloads

```python
# stage.pre_execute
{
    "stage": "bronze_customers",
    "stage_description": "Load customer data from files",
    "layer": "bronze",
    "stage_details": {"source": "landing", "format": "csv"},
    "pipe_count": 3,
    "timestamp": "2025-01-20T10:00:00Z"
}

# stage.post_execute
{
    "stage": "bronze_customers",
    "stage_description": "Load customer data from files",
    "layer": "bronze",
    "pipes_executed": ["bronze.customers.load", "bronze.customers.validate"],
    "duration_seconds": 45.2,
    "timestamp": "2025-01-20T10:00:45Z"
}

# stage.execute_failed
{
    "stage": "bronze_customers",
    "stage_description": "Load customer data from files",
    "layer": "bronze",
    "error": "Table not found: watermark_bronze",
    "error_type": "AnalysisException",
    "duration_seconds": 2.1,
    "timestamp": "2025-01-20T10:00:02Z"
}
```

#### Use Cases
1. **Stage monitoring**: Track stage execution across layers
2. **Error alerting**: Notify on stage failures
3. **Performance tracking**: Monitor stage durations
4. **Pipeline coordination**: Coordinate with pipe execution
5. **Watermark tracking**: Monitor watermark entity operations

---

### 9. Bootstrap

**Rating**: ⚡ HIGH
**Current State**: No signals, initialization code
**Signal Value**: High value for monitoring framework initialization

#### Why Signals Are Valuable
- **Initialization monitoring**: Track framework startup
- **Config validation**: Verify configuration loading
- **Platform detection**: Monitor platform-specific initialization
- **Dependency loading**: Track framework/package installations
- **Error handling**: Identify bootstrap failures early

#### Recommended Signals

```python
bootstrap.started              # When bootstrap begins
bootstrap.platform_detected    # After platform detection
bootstrap.config_loaded        # After config files loaded
bootstrap.dependencies_installed # After pip install
bootstrap.framework_ready      # When framework fully initialized
bootstrap.failed               # When bootstrap fails
```

#### Signal Payloads

```python
# bootstrap.started
{
    "app_name": "customer-etl",
    "environment": "prod",
    "artifacts_path": "/data-apps/artifacts",
    "timestamp": "2025-01-20T10:00:00Z"
}

# bootstrap.platform_detected
{
    "platform": "fabric",
    "workspace_id": "12345678-1234-1234-1234-123456789abc",
    "spark_version": "3.5.5.5.4.20251014.2",
    "timestamp": "2025-01-20T10:00:01Z"
}

# bootstrap.config_loaded
{
    "config_files": ["settings.yaml", "platform_fabric.yaml", "env_prod.yaml"],
    "config_count": 3,
    "bootstrap_config_keys": ["app_name", "artifacts_storage_path"],
    "duration_seconds": 1.2,
    "timestamp": "2025-01-20T10:00:02Z"
}

# bootstrap.dependencies_installed
{
    "framework_wheels": ["kindling_fabric-0.2.0-py3-none-any.whl"],
    "lake_packages": ["customer-utils-1.0.0-py3-none-any.whl"],
    "pypi_packages": ["pandas==2.0.0"],
    "duration_seconds": 45.3,
    "timestamp": "2025-01-20T10:00:47Z"
}

# bootstrap.framework_ready
{
    "app_name": "customer-etl",
    "platform": "fabric",
    "environment": "prod",
    "duration_seconds": 47.5,
    "timestamp": "2025-01-20T10:00:47Z"
}

# bootstrap.failed
{
    "app_name": "customer-etl",
    "failed_phase": "dependency_install",
    "error": "Could not find framework wheel",
    "error_type": "FileNotFoundError",
    "duration_seconds": 12.3,
    "timestamp": "2025-01-20T10:00:12Z"
}
```

#### Use Cases
1. **Initialization monitoring**: Track app startup times
2. **Configuration validation**: Verify config loading
3. **Dependency tracking**: Monitor package installations
4. **Error alerting**: Identify bootstrap failures
5. **Performance analysis**: Optimize initialization time

---

## 📊 MODERATE PRIORITY

### 10. SparkSessionProvider

**Rating**: 📊 MODERATE
**Current State**: Very simple, just creates session
**Signal Value**: Moderate - session lifecycle events

#### Why Signals Have Moderate Value
- **Session lifecycle**: Track session creation/termination
- **Configuration changes**: Monitor Spark config updates
- **Resource monitoring**: Track session resource usage
- **Multi-session scenarios**: Monitor multiple sessions (if supported in future)

#### Recommended Signals

```python
session.created          # After get_or_create_spark_session()
session.config_changed   # When session config updated
session.stopped          # When session stopped
```

#### Signal Payloads

```python
# session.created
{
    "app_name": "customer-etl",
    "spark_version": "3.5.0",
    "master": "local[*]",
    "config": {"spark.sql.adaptive.enabled": "true"},
    "timestamp": "2025-01-20T10:00:00Z"
}

# session.config_changed
{
    "app_name": "customer-etl",
    "changed_configs": {"spark.executor.memory": "8g"},
    "old_values": {"spark.executor.memory": "4g"},
    "timestamp": "2025-01-20T10:00:00Z"
}
```

#### Use Cases
1. **Session monitoring**: Track active sessions
2. **Resource tracking**: Monitor configuration changes
3. **Debugging**: Identify session configuration issues

---

### 11. PlatformService (platform_provider.py, platform_*.py)

**Rating**: 📊 MODERATE
**Current State**: Platform abstraction layer
**Signal Value**: Moderate - platform operations tracking

#### Why Signals Have Moderate Value
- **Storage operations**: Track read/write/delete operations
- **Job operations**: Monitor job submissions (already in job_deployment)
- **Notebook operations**: Track notebook reads/executions
- **Performance**: Monitor platform API latency

#### Recommended Signals

```python
platform.file_read       # After get_file_content()
platform.file_written    # After write()
platform.file_deleted    # After delete()
platform.notebook_read   # After get_notebook()
```

#### Signal Payloads

```python
# platform.file_read
{
    "platform": "fabric",
    "file_path": "/data-apps/customer-etl/main.py",
    "file_size_bytes": 2048,
    "duration_seconds": 0.3,
    "timestamp": "2025-01-20T10:00:00Z"
}

# platform.file_written
{
    "platform": "fabric",
    "file_path": "/data-apps/customer-etl/config.yaml",
    "file_size_bytes": 1024,
    "duration_seconds": 0.5,
    "timestamp": "2025-01-20T10:00:00Z"
}
```

#### Use Cases
1. **Storage monitoring**: Track file operations
2. **Performance analysis**: Monitor platform API latency
3. **Audit trail**: Log file read/write operations

#### Implementation Considerations
- High volume of operations could create signal noise
- Consider sampling or aggregating signals
- Focus on significant operations (large files, failures)

---

### 12. SparkTraceProvider

**Rating**: 📊 MODERATE
**Current State**: Already provides tracing via spans
**Signal Value**: Moderate - signals could complement spans

#### Why Signals Have Moderate Value
- **Complementary to spans**: Spans are for tracing, signals for pub/sub
- **Event notification**: External systems may want span events
- **Aggregation**: Collect span statistics across operations
- **Error aggregation**: Track errors across all spans

#### Recommended Signals

```python
trace.span_started       # When span created
trace.span_ended         # When span closed
trace.span_failed        # When span failed
```

#### Signal Payloads

```python
# trace.span_started
{
    "span_id": "uuid",
    "component": "DataPipeline",
    "operation": "run_pipeline",
    "parent_span_id": "parent-uuid",
    "timestamp": "2025-01-20T10:00:00Z"
}

# trace.span_ended
{
    "span_id": "uuid",
    "component": "DataPipeline",
    "operation": "run_pipeline",
    "duration_seconds": 45.2,
    "success": True,
    "timestamp": "2025-01-20T10:00:45Z"
}
```

#### Use Cases
1. **Trace aggregation**: Collect span statistics
2. **External monitoring**: Send span events to external systems
3. **Error tracking**: Aggregate errors across spans

#### Implementation Considerations
- Spans already provide detailed tracing
- Signals would primarily be for external notification
- Consider if span context is sufficient without signals

---

## 💤 LOW PRIORITY

### 13. SparkLoggerProvider

**Rating**: 💤 LOW
**Current State**: Simple logger factory
**Signal Value**: Low - too simple, no significant state changes

#### Why Signals Have Low Value
- **Simple factory**: Just creates logger instances
- **No state changes**: No interesting events to signal
- **Logger itself**: Application code uses logger for events

#### Potential Signals (if needed)

```python
logger.created           # When new logger created
```

#### Recommendation
**Skip signals** - This provider is too simple to benefit from signals. Application code should use logging directly.

---

### 14. SignalProvider

**Rating**: 💤 LOW
**Current State**: Manages signals themselves
**Signal Value**: Low - would be circular/meta

#### Why Signals Have Low Value
- **Meta-level**: Signaling about signals creates circular logic
- **Implementation detail**: Signal provider is infrastructure, not business logic

#### Recommendation
**Skip signals** - SignalProvider is the mechanism for signals, not a consumer.

---

## Implementation Roadmap

### Phase 1: Critical Signals (Weeks 1-2)
1. **DataPipesExecuter** - Pipeline orchestration
2. **DataEntityManager** - Data governance
3. **FileIngestionProcessor** - File processing

### Phase 2: High Priority Signals (Weeks 3-4)
4. **WatermarkManager** - CDC monitoring
5. **DataAppManager** - Deployment tracking
6. **PipManager** - Dependency management
7. **NotebookLoader** - Package builds

### Phase 3: Supporting Signals (Week 5)
8. **StageProcessor** - Stage execution
9. **Bootstrap** - Initialization tracking

### Phase 4: Optional Signals (Week 6)
10. **SparkSessionProvider** - Session lifecycle
11. **PlatformService** - Platform operations
12. **SparkTraceProvider** - Trace events

---

## Signal Design Patterns

### Pattern 1: Lifecycle Triple (Most Common)
```python
{service}.pre_{operation}    # Before operation
{service}.post_{operation}   # After successful operation
{service}.{operation}_failed # When operation fails
```

**Example**: `datapipes.pre_run`, `datapipes.post_run`, `datapipes.run_failed`

**Use When**: Most operations with clear start/success/failure states

### Pattern 2: State Change (For Updates)
```python
{service}.{state}_changed    # When state changes
{service}.{state}_updated    # When state explicitly updated
```

**Example**: `entity.schema_changed`, `watermark.version_changed`

**Use When**: Tracking state transitions or updates

### Pattern 3: Event Notification (For Milestones)
```python
{service}.{event}_occurred   # When specific event happens
```

**Example**: `file_ingestion.file_moved`, `bootstrap.framework_ready`

**Use When**: Discrete events without clear lifecycle

### Pattern 4: Granular Operations (For Batches)
```python
{service}.batch_started      # Batch-level operation
{service}.item_processed     # Individual item within batch
{service}.batch_completed    # Batch completion
```

**Example**: `file_ingestion.pre_process`, `file_ingestion.pre_file`, `file_ingestion.post_process`

**Use When**: Operations process multiple items in batches

---

## Signal Payload Best Practices

### 1. Always Include
- `timestamp`: ISO 8601 format for all signals
- Operation identifiers (entity_id, pipe_id, app_name, etc.)
- Operation context (what's being operated on)

### 2. Pre-Operation Payloads
- What will be operated on
- Operation parameters
- Counts/estimates (file_count, pipe_count, etc.)

### 3. Post-Operation Payloads
- What was operated on
- Operation results (rows_processed, success_count, etc.)
- Duration (duration_seconds)
- Summary statistics

### 4. Failed Operation Payloads
- What failed
- Error message (concise, not full stack trace)
- Error type (exception class name)
- Duration before failure
- Context for recovery

### 5. Avoid in Payloads
- Large objects (DataFrames, full configs)
- Sensitive data (passwords, tokens)
- Full stack traces (use logger for those)
- Redundant information

---

## Testing Strategy

### Unit Tests
- Mock SignalProvider
- Verify signal.send() called with correct payload
- Test all signal emission paths (success, failure, edge cases)

### Integration Tests
- Real SignalProvider with observers
- Verify observers receive signals
- Test signal ordering
- Test signal payload correctness

### System Tests
- End-to-end workflows
- Verify signals across service boundaries
- Monitor signal performance impact

---

## Performance Considerations

### Signal Overhead
- Signal emission should be <1ms per signal
- Blinker is synchronous - observers block sender
- Consider async signal emission for slow observers

### High-Volume Signals
- File ingestion: 1000s of files → consider batching or sampling
- Pipeline execution: 100s of pipes → individual pipe signals okay
- Platform operations: High frequency → sample or aggregate

### Optimization Strategies
1. **Lazy payload construction**: Only build payload if observers exist
2. **Sampling**: Emit every Nth signal for high-volume operations
3. **Aggregation**: Batch multiple events into periodic signals
4. **Async emission**: Don't block operations on slow observers

---

## Migration Strategy

### For Existing Instrumentation
- **SparkTraceProvider spans**: Keep spans, add complementary signals
- **Logging**: Keep logging, signals are supplementary
- **Metrics**: Signals can feed metrics systems

### Backward Compatibility
- All signals are additive (no breaking changes)
- Existing code works without observers
- Signal payloads can evolve (add fields, don't remove)

### Rollout Approach
1. Add signals to one provider at a time
2. Add tests for new signals
3. Document signals in provider docstrings
4. Create example observers
5. Monitor performance impact
6. Iterate based on feedback

---

## Documentation Requirements

### Per-Provider Documentation
Each provider with signals should document:
1. **Signal List**: All signals emitted
2. **Signal Naming**: Follow Django convention
3. **Payload Schema**: Expected fields in each signal
4. **Use Cases**: When/why to subscribe to each signal
5. **Examples**: Sample observer code

### Framework Documentation
1. **Signal Catalog**: Complete list of all signals
2. **Design Patterns**: Common signal patterns
3. **Best Practices**: Payload design, performance
4. **Testing Guide**: How to test signal-enabled code
5. **Observer Examples**: Common observer patterns

---

## Conclusion

This evaluation identifies **15 signal opportunities** across the Kindling framework:

- **3 CRITICAL** providers requiring signals immediately
- **6 HIGH** priority providers with strong signal value
- **3 MODERATE** priority providers with specific use cases
- **3 LOW** priority providers where signals provide minimal value

**Total Development Estimate**: 6 weeks for full implementation

**Recommended Approach**: Start with Phase 1 (Critical), validate design patterns, then proceed to High Priority providers.

**Next Steps**:
1. Review this evaluation with stakeholders
2. Prioritize based on immediate business needs
3. Create detailed design docs for Phase 1 providers
4. Implement Phase 1 with full test coverage
5. Gather feedback before proceeding to Phase 2
