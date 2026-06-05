# Platform API Architecture

## Overview

Kindling uses two platform layers with different responsibilities.

1. Runtime platform services (`kindling` package)
- Used inside Spark execution environments.
- Backed by runtime utilities and DI wiring.
- Primary files: `packages/kindling/platform_fabric.py`, `packages/kindling/platform_synapse.py`, `packages/kindling/platform_databricks.py`.

2. Design-time remote API clients (`kindling_sdk` package)
- Used from local dev, CI/CD, and system tests.
- No runtime notebook utilities required.
- Primary files: `packages/kindling_sdk/kindling_sdk/platform_*.py`.

## Design-Time Interface

Remote clients implement `PlatformAPI` in:
- `packages/kindling_sdk/kindling_sdk/platform_provider.py`

That interface enforces consistent operations across platforms (deploy/create/run/status/logs/cleanup/secrets).

## Factory Pattern

Use the SDK factory when selecting a platform by environment/config:

```python
from kindling_sdk.platform_api import create_platform_api_from_env

api, platform_name = create_platform_api_from_env("synapse")
result = api.create_job("orders-job", {"job_name": "orders-job", "app_name": "orders-app"})
run_id = api.run_job(result["job_id"])
status = api.get_job_status(run_id)
```

## Implementations

- `FabricAPI` (`platform_fabric.py`)
- `SynapseAPI` (`platform_synapse.py`)
- `DatabricksAPI` (`platform_databricks.py`)

Each is registered in `PlatformAPIRegistry` and can be resolved via `create_platform_api_from_env()`.

## Design Principles

1. Separation of concerns: runtime notebook services vs remote automation clients.
2. Interface-first: one cross-platform contract for tests and tooling.
3. Registry/factory discovery: platform selection via config/env.
4. Operational parity: same core workflow across platforms (`deploy_app` -> `create_job` -> `run_job` -> `monitor/logs`).
