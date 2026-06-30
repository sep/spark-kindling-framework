# Proposal: Cross-Platform Direct Job Execution and Runner Registration

## Problem

`kindling app run` on all three platforms currently relies on persistent job definitions
as an intermediary for ad-hoc execution:

- **Synapse**: `submit_app_run()` creates/overwrites a per-app `SparkJobDefinition`, then
  runs it via the execute endpoint
- **Databricks**: `submit_app_run()` (base class) finds the shared `kindling-runner` job
  definition, then calls `run_job()` with `app_name` as a parameter
- **Fabric**: Same as Databricks — finds `kindling-runner`, invokes with app_name

This conflates two distinct concerns:
1. **Ad-hoc execution** — run this app now with this config
2. **Pipeline/workflow registration** — create a named definition that an orchestrator
   can trigger by name

The "runner" concept (`kindling runner ensure/repair`) muddies this by acting as a
shared dispatcher rather than what it should be: a way to register job definitions for
pipeline integration.

## Proposed Design

### Principle separation

| Use case | Mechanism | Command |
|----------|-----------|---------|
| Ad-hoc / local dev run | Direct one-time execution (no stored definition) | `kindling app run --platform <p>` |
| Pipeline / workflow integration | Named persistent job definition | `kindling runner register --app <name>` |

### Direct execution per platform

`kindling app run` submits a one-time job without creating or requiring any stored
definition:

| Platform | API | Notes |
|----------|-----|-------|
| **Synapse** | `POST /livyApi/.../sparkPools/{pool}/batches` | Livy batch submit |
| **Databricks** | `client.jobs.runs.submit()` | One-time run, no persistent job |
| **Fabric** | Create ephemeral definition → run → delete definition | Fabric has no one-time run API; definition is transient |

Config flows through as `config:k=v` bootstrap args in all cases.

### `kindling runner` — cross-platform job definition registration

Registers a named job definition on the target platform so that an external orchestrator
(Synapse Pipeline, Databricks Workflow, Fabric Pipeline) can trigger it by name.

```
kindling runner register --app <app_name> [--config key=value ...] [--platform <p>]
```

| Platform | Registers | Intended trigger |
|----------|-----------|-----------------|
| **Synapse** | `SparkJobDefinition` named `<app_name>` | "Execute Spark Job Definition" pipeline activity |
| **Databricks** | Databricks `Job` named `kindling-<app_name>` | Databricks Workflows / external orchestrators |
| **Fabric** | Fabric `SparkJobDefinition` item | Fabric Pipeline activity |

Config overrides supplied at registration time are baked into the definition as
`config:k=v` bootstrap args. The definition can be re-registered at any time to
update config (idempotent).

### Old subcommands

| Old | New | Notes |
|-----|-----|-------|
| `runner ensure [--app <name>]` | `runner register --app <name>` | Explicit app required; no implicit shared runner |
| `runner repair` | `runner register` (idempotent) | Covered by idempotent PUT/create |
| `runner status` | `runner status` | Kept; lists registered definitions |
| `runner delete` | `runner delete --app <name>` | Kept; removes a specific definition |
| `runner invoke` | `runner invoke` | Kept for debugging |

## SDK Interface Changes

### `PlatformAPI` base class

**`submit_app_run()` — becomes fully abstract**

Remove the shared-runner implementation from the base class. Each platform provides
its own direct-execution implementation:

```python
@abstractmethod
def submit_app_run(
    self,
    app_name: str,
    environment: Optional[str] = None,
    parameters: Optional[Dict[str, Any]] = None,
) -> str:
    """Submit a one-time app run. No persistent job definition required (except Fabric)."""
```

**`register_app_job()` — new abstract method**

```python
@abstractmethod
def register_app_job(self, app_name: str, config_overrides: Optional[Dict] = None) -> Dict[str, Any]:
    """Create or update a named job definition for pipeline/workflow integration."""
```

**Remove from base class:** `ensure_runner`, `get_runner_status`, `repair_runner`,
`delete_runner` — these were implementations of the old dispatcher concept. Runner
lifecycle is now handled through `register_app_job` / `delete_job` / `find_job_by_name`.

### `SynapseAPI`

- `submit_app_run()` → calls new `_submit_livy_batch(app_name, config_overrides)`
- `register_app_job()` → thin wrapper over existing `create_job()`
- `_build_job_properties()` — new helper extracted from `create_job()`, shared by both

### `DatabricksAPI`

- `submit_app_run()` → calls `client.jobs.runs.submit()` with a one-time cluster/task
  spec built from the same config that `create_job()` uses today
- `register_app_job()` → thin wrapper over existing `create_job()`
- `_build_job_spec()` — extracted helper shared between the two paths

### `FabricAPI`

- `submit_app_run()` → creates an ephemeral job definition (name: `kindling-adhoc-{timestamp}`),
  submits it, then immediately deletes the definition — the run continues independently.
  Config is baked into the definition at creation time, same as `create_job()` today.
- `register_app_job()` → thin wrapper over existing `create_job()` using the app name
  as the definition name (persistent, for Fabric Pipeline use)

## CLI Changes

### `kindling app run`

`_run_remote_app()` simplifies to:
```python
run_id = api_client.submit_app_run(app_name, environment, parameters)
```

Remove the runner-check branch entirely — no `get_runner_status()` call, no fallback
to `ensure_app_job()`. Each platform's `submit_app_run()` handles its own strategy.

### `kindling runner register` — new subcommand

```
kindling runner register --app <name> [--config key=value ...] [--platform synapse|databricks|fabric]
```

Calls `api_client.register_app_job(app_name, config_overrides)`. Prints the job
definition name/ID that can be referenced in the platform's pipeline/workflow tool.

### `kindling runner status`

Updated to use `find_job_by_name()` to show which apps have registered definitions.

### Removed CLI subcommands

- `runner ensure` → replaced by `runner register`
- `runner repair` → covered by idempotent `runner register`

## Implementation Checklist

### platform_provider.py
- [ ] Make `submit_app_run()` fully abstract (remove base class runner implementation)
- [ ] Add `register_app_job()` as abstract method
- [ ] Remove `ensure_runner`, `get_runner_status`, `repair_runner`, `delete_runner`
- [ ] Remove `ensure_app_job()` — no longer needed

### platform_synapse.py
- [ ] Extract `_build_job_properties()` from `create_job()`
- [ ] Add `_submit_livy_batch()` using `_build_job_properties()`
- [ ] Change `submit_app_run()` to call `_submit_livy_batch()`
- [ ] Add `register_app_job()` as wrapper over `create_job()`

### platform_databricks.py
- [ ] Extract `_build_job_spec()` from `create_job()`
- [ ] Add `_submit_one_time_run()` using `client.jobs.runs.submit()`
- [ ] Change `submit_app_run()` to call `_submit_one_time_run()`
- [ ] Add `register_app_job()` as wrapper over `create_job()`

### platform_fabric.py
- [ ] Change `submit_app_run()` to: create ephemeral definition → run → delete definition
- [ ] Add `register_app_job()` as wrapper over `create_job()` with permanent app name

### cli.py
- [ ] Simplify `_run_remote_app()` — remove runner-check branch
- [ ] Add `runner register` subcommand
- [ ] Remove `runner ensure` and `runner repair` subcommands
- [ ] Update `runner delete` to require `--app <name>`
- [ ] Update `runner status` to show per-app registered definitions
- [ ] Update help text and `kindling app scaffold` output

### Tests / docs
- [ ] Update any system tests that call `runner ensure`
- [ ] Update `kindling env check` output if it reports runner status
