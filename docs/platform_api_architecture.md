# Platform API Architecture

## Overview

The Kindling framework separates platform operations into two categories:

1. **Runtime Services** - For operations within notebook runtime that require platform-specific utilities like mssparkutils or dbutils
2. **Remote API Clients** - For operations performed remotely via REST APIs from local dev, CI/CD, or tests

## PlatformAPI Interface

All remote API clients implement the `PlatformAPI` abstract base class, ensuring interchangeability.

### Benefits

- **Interchangeability** - Any PlatformAPI implementation can be used wherever the interface is expected
- **Type Safety** - IDEs and type checkers validate correct usage
- **Contract Enforcement** - All implementations must provide all required methods
- **Testability** - Easy to create mock implementations
- **Documentation** - Interface serves as living documentation

## Usage Patterns

### Runtime Context (Inside Notebooks)

```python
from kindling import bootstrap_framework

# Uses DI, requires runtime utilities
config, services = bootstrap_framework()
fabric_service = services['platform_service']

# These operations need mssparkutils
df = fabric_service.read_parquet("Files/data/input.parquet")
```

### Remote Context (Local Dev, CI/CD, Tests)

```python
from kindling.fabric_api import FabricAPI

# No DI, no runtime utils needed
api = FabricAPI(workspace_id="...", lakehouse_id="...")

# Deploy via REST API
job = api.create_spark_job("my-job", config)
api.upload_files(files, "Files/jobs/my-app")
run_id = api.run_job(job["job_id"])
```

### Polymorphic Usage

```python
from kindling.platform_api import PlatformAPI

def deploy_to_platform(api: PlatformAPI, app_path: str, config: dict):
    """Deploy to any platform implementing PlatformAPI"""
    files = prepare_files(app_path)
    job = api.create_spark_job(config["job_name"], config)
    api.upload_files(files, f"apps/{config['job_name']}")
    return job

# Works with any platform
fabric_api = FabricAPI(workspace_id, lakehouse_id)
deploy_to_platform(fabric_api, "./app", config)
```

## Implementation Status

- ✅ **FabricAPI** - Complete implementation
- 🚧 **DatabricksAPI** - Interface defined, implementation pending
- 🚧 **SynapseAPI** - Interface defined, implementation pending

## Design Principles

1. **Separation of Concerns** - Runtime vs remote operations
2. **No DI for API Clients** - Simple utilities, direct instantiation
3. **Interface-Based Design** - All implement PlatformAPI ABC
4. **Authentication Flexibility** - Support multiple auth methods
5. **Pure REST APIs** - No platform runtime dependencies
