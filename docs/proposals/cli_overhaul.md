# Kindling CLI Overhaul Proposal

**Date:** 2026-05-05
**Status:** Proposal
**Scope:** CLI command model, app/pipeline execution, runner job lifecycle, command consistency

---

## Executive Summary

The current CLI exposes both app-level and job-level workflows:

- `kindling app package`
- `kindling app deploy`
- `kindling job create`
- `kindling job run`
- `kindling job submit`
- `kindling job status`
- `kindling job logs`
- `kindling job cancel`
- `kindling job delete`

That surface suggests each app or pipeline maps to a distinct remote job definition. That does not match Kindling's intended design.

Kindling's execution model is:

> A platform has one durable Kindling runner job. Apps and pipelines are the units of user work. The runner job executes those apps/pipelines by receiving config and parameters.

This proposal reframes the CLI around that model:

- **Apps and pipelines** become the primary user-facing concepts.
- **Runner/job infrastructure** becomes an admin or internal concern.
- Direct job commands remain available only where they are useful for operations, debugging, or CI.

---

## Problem Statement

The current CLI creates an avoidable conceptual split:

- `kindling run <pipe_id>` runs a local pipe.
- `kindling app deploy` deploys app files but does not run them.
- `kindling job submit` is the closest remote "run this app" command, but it is hidden under `job`.
- `kindling job create` / `job run` imply users should manage job definitions as their main workflow.

This is inconsistent with Kindling's design. Users should not need to think about platform job definitions for ordinary app or pipeline execution.

The command surface should answer user questions directly:

- "How do I run this app?"
- "How do I run this pipeline?"
- "How do I deploy this app?"
- "How do I see the status/logs of my run?"
- "How do I install or repair the Kindling runner?"

The current job-centered command names make those questions harder than necessary.

---

## Execution Model

Kindling has two distinct execution environments. The CLI must make this distinction explicit rather than conflating them under a single `run` concept.

### Local execution

Spark runs on the developer's machine or CI runner. The Kindling runtime is invoked in-process. Data comes from wherever the entity providers are configured to read — this may be local files (`tests/entities/`) or external cloud storage, depending on env config.

Used for: development iteration, unit and integration testing.

CLI entry point: `kindling pipeline run` (proposed) or `kindling run` (current).

### Remote execution

Code is submitted to and runs inside a platform Spark pool (Synapse, Fabric, Databricks). The Kindling runner job receives the app config and executes it. Data comes from platform cloud storage.

Used for: system testing, staging, production batch runs.

CLI entry point: `kindling app run`.

### Test suite alignment

| Suite | Spark runs | What is invoked | Data |
|---|---|---|---|
| unit | local / in-process | transform function directly | anything — CSVs, hardcoded rows, `tests/entities/` |
| component | local / in-process | DI container wiring | none |
| integration | local machine | full Kindling pipeline via runtime | `tests/entities/` convention or cloud storage |
| system | remote Spark pool | runner on platform | platform cloud storage |

`kindling pipeline run --env dev` is the CLI equivalent of an integration test without assertions. `kindling app run --env staging` is the CLI equivalent of a system test without assertions. The `--env` flag selects data sources; local vs. remote is determined by which command group is used.

### The `tests/entities/` convention

A file at `tests/entities/<entity-id>.csv` (or `tests/entities/<namespace>/<entity-id>.csv` for dotted entity IDs) is auto-discovered as the data source for that entity during local execution when no other provider is configured for the active env. This applies to both unit tests (read directly in test code) and integration tests (resolved automatically by the entity-provider layer).

---

## Design Principles

1. **Apps and pipelines are user concepts.**
   Users should run apps and pipelines, not remote job definitions.

2. **The runner job is infrastructure.**
   The platform job exists so Kindling can execute work. It should be installed, repaired, and inspected, but not be the normal unit users operate.

3. **One durable runner job per platform/workspace.**
   Normal app and pipeline runs should use the existing Kindling runner job, creating or updating it only when necessary.

4. **Remote run commands should be end-to-end.**
   A normal run should use the existing deployed app by default, start execution, stream logs by default, and report final status. Deploying new app assets should be explicit via `--deploy`.

5. **Low-level job commands should stay available for operators.**
   CI, support, and debugging sometimes need raw job controls, but those should not be the primary tutorial path.

6. **Command names should be consistent across local and remote execution.**
   `run` should mean "execute a user workload." Where local vs remote matters, make that explicit with options or command groups.

---

## Proposed Mental Model

### App

A deployable Kindling application: code plus config that can be run by the Kindling runner.

Examples:

```bash
kindling app package --local-folder ./orders
kindling app deploy --local-folder ./orders
kindling app run orders
```

### Pipeline

A named pipeline or pipe inside an app.

Examples:

```bash
kindling pipeline run bronze.ingest_orders
kindling pipeline run silver.clean_orders --app orders
```

### Runner

The single durable remote execution vehicle for Kindling in a workspace.

Examples:

```bash
kindling runner ensure
kindling runner status
kindling runner repair
kindling runner delete
```

### Job

A platform-native implementation detail. Jobs may still be exposed for advanced use, but should be documented as low-level operations.

---

## Proposed Command Surface

### App Commands

#### `kindling app add`

Scaffold new building blocks into an existing app. Each subcommand generates the source file and all relevant test scaffolding so new code has a place to be tested immediately.

##### `kindling app add module`

Add a new module to an app.

```bash
kindling app add module bronze --app ./orders
```

Generated:
- `orders/bronze/__init__.py`
- `orders/bronze/bronze.py` — module skeleton
- `tests/unit/test_bronze.py` — unit test skeleton

##### `kindling app add entity`

Add a new entity definition and a fixture stub for use in unit and integration tests.

```bash
kindling app add entity bronze.orders --app ./orders
```

Generated:
- Entity definition appended to `orders/bronze/entities.py`
- `tests/entities/bronze/orders.csv` — column headers only, populated by the developer

##### `kindling app add pipe`

Add a new data pipe, its transform function, and test scaffolding for all three tiers.

```bash
kindling app add pipe bronze.ingest_orders --app ./orders
kindling app add pipe silver.clean_orders --inputs bronze.orders,bronze.products --app ./orders
```

Generated:
- `orders/bronze/ingest_orders.py` — pipe registration and transform function skeleton
- `tests/unit/test_ingest_orders.py` — calls the transform function directly with data from `tests/entities/`
- `tests/integration/test_ingest_orders.py` — runs the full pipeline via Kindling runtime, asserts on output entity
- `tests/entities/bronze/orders.csv` — fixture stub for each declared input entity, if not already present

**Test discoverability:** Generated test files follow pytest naming conventions (`test_*.py` in `tests/unit/` and `tests/integration/`) so they are picked up automatically by `kindling test run --suite unit` and `--suite integration` with no additional registration.

**Skip stubs:** Scaffolded tests are generated as `pytest.mark.skip` rather than empty `pass` bodies. This ensures they appear in test run output immediately — signaling that they need to be implemented — without silently passing before the developer has filled them in.

```python
@pytest.mark.skip(reason="scaffolded — implement transform assertions")
def test_ingest_orders_transform():
    ...

@pytest.mark.skip(reason="scaffolded — populate tests/entities/bronze/orders.csv before running")
def test_ingest_orders_pipeline():
    ...
```

The integration test skip message points explicitly at the CSV that needs data, so the developer knows exactly what to do next.

---

#### `kindling app package`

Build a `.kda` archive from a local app directory.

Status: keep.

Source is specified with `--local-folder`, consistent with `app deploy` and `app run --deploy`.

```bash
kindling app package --local-folder ./orders
kindling app package --local-folder ./orders --output dist/orders.kda
```

#### `kindling app deploy`

Deploy app assets to the configured platform artifact location.

Status: keep, but clarify that this deploys app assets only and does not run the app.

The source must be specified explicitly using one of two mutually exclusive flags:

- `--local-folder <path>` — package and deploy a local app directory on the fly.
- `--kda-package <path>` — deploy a pre-built `.kda` archive. Use this in CI when packaging is a separate step.

```bash
kindling app deploy --local-folder ./orders --platform synapse
kindling app deploy --kda-package dist/orders.kda --platform synapse --env prod
```

#### `kindling app run`

Primary remote execution command for users.

Recommended behavior:

1. Resolve platform.
2. Ensure the Kindling runner exists.
3. Resolve the already-deployed app by name.
4. Submit a run to the single runner job with app config.
5. Stream logs by default.
6. Return run id, status, and useful artifact paths.

If `--deploy` is passed, `app run` deploys app assets before starting the run. The same source flags as `app deploy` apply:

- `--deploy --local-folder <path>` — package and deploy a local directory, then run.
- `--deploy --kda-package <path>` — deploy a pre-built `.kda`, then run.

Without `--deploy`, `app run` runs against the currently deployed app. This keeps repeated runs fast and makes it explicit when new code is being pushed.

Examples:

```bash
kindling app run orders --env dev
kindling app run orders --deploy --local-folder ./orders --platform synapse
kindling app run orders --no-wait
kindling app run orders --parameters params.yaml
```

This should replace `kindling job submit` as the happy path.

#### `kindling app status`

Fetch status for an app run.

```bash
kindling app status <run-id>
```

This may delegate to the same backend as `runner status` or `job status`, but the user-facing noun stays app/run oriented.

#### `kindling app logs`

Fetch or stream logs for an app run.

```bash
kindling app logs <run-id>
kindling app logs <run-id> --stream
```

#### `kindling app cancel`

Cancel an active app run.

```bash
kindling app cancel <run-id>
```

#### `kindling app cleanup`

Remove deployed app assets.

Status: keep.

Accepts a positional app name, or `--local-folder`/`--kda-package` to infer the name from the source.

```bash
kindling app cleanup orders
kindling app cleanup --local-folder ./orders
kindling app cleanup --kda-package dist/orders.kda
```

---

### Pipeline Commands

Pipeline commands should exist if users naturally think in terms of running one pipe or pipeline inside an app.

At the data layer, `kindling pipeline run` and `kindling app run --pipeline` are the same operation. Pipelines declare `input_entity_ids`, and the entity-provider registry supplies the data at runtime. The `--env` flag selects the configured data sources. There is no separate pipeline data model or separate runner path.

For that reason, `pipeline run` should be documented as a scoped shorthand for:

```bash
kindling app run <app-name> --pipeline <pipeline-id>
```

#### `kindling pipeline run`

Run a named pipeline through the Kindling runner.

Examples:

```bash
kindling pipeline run bronze.ingest_orders --app orders
kindling pipeline run daily_orders --app orders --env prod
kindling pipeline run bronze.ingest_orders --app orders --deploy --local-folder ./orders
kindling pipeline run bronze.ingest_orders --app orders --deploy --kda-package dist/orders.kda
```

Recommended behavior:

1. Resolve the app and pipeline id.
2. Ensure runner exists.
3. When `--deploy` is passed, deploy from `--local-folder` or `--kda-package` before running.
4. Run the single Kindling runner job with `app_name`, `pipeline_id`, environment, and parameters.

#### `kindling pipeline status`

Fetch status for a pipeline run.

```bash
kindling pipeline status <run-id>
```

#### `kindling pipeline logs`

Fetch or stream logs for a pipeline run.

```bash
kindling pipeline logs <run-id> --stream
```

#### `kindling pipeline cancel`

Cancel an active pipeline run.

```bash
kindling pipeline cancel <run-id>
```

---

### Runner Commands

Runner commands manage the single durable Kindling runner on the remote platform.

#### `kindling runner ensure`

Create or update the Kindling runner job definition if missing or stale.

```bash
kindling runner ensure --platform fabric
```

This is the replacement mental model for `job create`.

#### `kindling runner status`

Show runner installation state, runner id, version, and last known health.

```bash
kindling runner status
```

#### `kindling runner repair`

Recreate or update the runner job definition and supporting bootstrap/config references.

```bash
kindling runner repair
```

#### `kindling runner delete`

Delete the Kindling runner job definition.

```bash
kindling runner delete
```

This should be treated as an admin operation and likely require confirmation or `--yes`.

#### `kindling runner invoke`

Optional advanced/debug command that invokes the runner with a raw parameters file.

```bash
kindling runner invoke --params params.yaml
```

This is useful for support and CI, but should not be the happy path. `invoke` avoids overloading `run`, which should mean user workload execution.

---

### Job Commands

Job commands should be demoted to advanced operations or compatibility aliases.

Current commands:

- `kindling job init`
- `kindling job create`
- `kindling job run`
- `kindling job submit`
- `kindling job status`
- `kindling job logs`
- `kindling job cancel`
- `kindling job delete`

Recommended direction:

- Replace `job submit` with `app run`.
- Replace normal `job create` usage with `runner ensure`.
- Keep `job status`, `job logs`, and `job cancel` as aliases or advanced commands if platform-native run ids remain useful.
- Consider hiding raw job commands from top-level README examples.

Compatibility aliases can print guidance:

```text
`kindling job submit` is now `kindling app run`.
```

---

## Command Mapping

`kindling run <pipe_id>` is a breaking-change risk because existing users and scripts may rely on it. Phase 1 should keep it as a local shortcut with a deprecation notice pointing to `kindling pipeline run --local`. Removal can wait until Phase 4.

| Current command | Proposed command | Notes |
| --- | --- | --- |
| `kindling run <pipe_id>` | `kindling pipeline run <pipe_id> --local` | Keep as a deprecated local shortcut in Phase 1. |
| `kindling app package <path>` | `kindling app package --local-folder <path>` | Consistent source flag. |
| `kindling app deploy <path>` | `kindling app deploy --local-folder <path>` or `--kda-package <path>` | Make source type explicit. |
| `kindling job submit <app>` | `kindling app run <app>` | Main happy path. |
| `kindling job create job.yaml` | `kindling runner ensure` | Runner job is infrastructure. |
| `kindling job run <job-id>` | `kindling runner invoke --params params.yaml` | Advanced/debug only. |
| `kindling job status <run-id>` | `kindling app status <run-id>` | Alias acceptable. |
| `kindling job logs <run-id>` | `kindling app logs <run-id>` | Alias acceptable. |
| `kindling job cancel <run-id>` | `kindling app cancel <run-id>` | Alias acceptable. |
| `kindling job delete <job-id>` | `kindling runner delete` | Admin operation. |

---

## Standardization Opportunities

### Output Flags

Commands should consistently support `--json` when they return structured data.

Current inconsistency:

- `job create` always emits JSON.
- `job run`, `job cancel`, and `job delete` support `--json`.

Recommendation:

- Human output by default.
- `--json` for machine-readable output.
- Stable JSON keys across run-like commands:
  - `app_name`
  - `pipeline_id`
  - `run_id`
  - `platform`
  - `state`
  - `succeeded`
  - `storage_path`
  - `runner_id`

### Wait and Logs Flags

Run commands should share a common set of options:

- `--wait / --no-wait`
- `--logs / --no-logs`
- `--poll-interval`
- `--timeout`
- `--fail-on-error / --no-fail-on-error`
- `--json`

Default recommendation:

- `app run` waits and streams logs by default for interactive use.
- `--no-wait --json` is the CI-friendly mode.

### Platform Resolution

All remote commands should resolve platform the same way:

1. explicit `--platform`
2. environment/platform-specific variables
3. clear error with `kindling env check --platform ...` hint

### Naming

Use the word `run` for user workload execution.

Avoid using `run` for both:

- local pipe execution
- remote platform job execution

Either move local execution under `pipeline run --local` or create a `local` command group.

---

## Proposed Implementation Phases

### Phase 1: Add User-Facing Aliases

Add:

- `kindling app run`
- `kindling app status`
- `kindling app logs`
- `kindling app cancel`
- `kindling runner ensure`
- `kindling runner status`

Keep existing job commands unchanged.

Defer `runner repair` and `runner delete` to Phase 3. They are admin/destructive operations and need clearer runner identity and platform cleanup semantics before implementation.

Internally, `app run` can reuse the existing `job submit` implementation while names settle, but it should not always deploy. The Phase 1 behavior should be run-only by default, with `--deploy` opting into package/deploy before execution.

### Phase 2: Reframe Documentation

Update README and docs so the primary workflow is:

```bash
kindling app deploy --local-folder ./orders
kindling app run orders --platform synapse
kindling app run orders --deploy --local-folder ./orders --platform synapse
kindling app logs <run-id>
```

Document direct job commands as advanced platform operations.

### Phase 3: Introduce Runner Semantics

Refactor SDK and CLI naming around the single runner job:

- runner id
- runner version
- runner config path
- runner install/update/delete

`create_job` can remain in the SDK as a platform primitive, but CLI should call it through runner-oriented helpers.

### Phase 4: Deprecate Confusing Commands

Add deprecation warnings or compatibility messages:

- `kindling job submit` -> `kindling app run`
- `kindling job create` -> `kindling runner ensure`
- `kindling job delete` -> `kindling runner delete`

Do not remove commands until downstream CI and docs have migrated.

---

## Open Questions

These questions are triaged by whether Phase 1 needs a decision before implementation.

1. **DEFER:** Should `pipeline` be a first-class CLI group, or should pipeline execution remain under `app run --pipeline <id>`?

   The data layer is the same either way. Phase 1 can ship `app run --pipeline`, with `pipeline run` added later as a convenience alias.

2. **RESOLVED FOR PHASE 1:** Should `kindling run <pipe_id>` remain as a local-only shortcut, or move to `kindling pipeline run <pipe_id> --local`?

   Keep `kindling run <pipe_id>` as a local shortcut in Phase 1 and print a deprecation notice pointing to `kindling pipeline run <pipe_id> --local`. Remove only after downstream users and scripts have migrated.

3. **RESOLVED FOR PHASE 1:** Should `app run` always deploy first, or should it default to running the currently deployed app and require `--deploy` to upload changes?

   `app run` defaults to run-only against the currently deployed app. `--deploy` packages/deploys assets before the run. A local path argument without `--deploy` should fail with a clear message.

4. **DEFER:** How should the runner determine app/config versions?
   Options include latest deployed app path, content hash, explicit `.kda`, or config path parameter.

   For Phase 1, the runner uses the currently deployed app/config. Version pinning can be a Phase 3 concern.

5. **RESOLVED FOR PHASE 1:** Should the runner be one job per workspace, one per platform, or one per environment within a workspace?

   Use one runner per platform workspace. Environment is a run/config parameter, not a separate runner. If dev/test/prod use separate workspaces, they naturally get separate runners.

6. **DEFER:** Should runner install/update be automatic during `app run`, or should production environments require explicit `runner ensure`?

   Safe default: auto-ensure in dev/non-prod, require explicit `runner ensure` in prod. This can be a config flag or policy later.

---

## Relationship to `kindling test run`

`kindling app run` and `kindling test run` are related but not interchangeable.

Integration tests bootstrap the full DI container, write fixture DataFrames to entity-provider storage paths, and invoke pipelines through the same execution layer that `app run` uses. The difference is the data source and verification step:

| | `kindling app run` | `kindling test run` |
| --- | --- | --- |
| Data source | Live entity providers configured for `--env` | Fixture DataFrames written by test setup |
| Verification | None; validates side effects operationally | Assertions in test code |
| Purpose | Production or batch execution | Correctness verification |

Recommended user guidance:

1. Use `kindling test run` first for controlled data and assertions.
2. Use `kindling app run` after tests pass to execute against configured live data sources.

This prevents users from treating `app run --env dev` as a substitute for tests.

---

## Recommended V1 Decision

For the first overhaul pass:

1. Add `kindling app run` as the primary remote execution command.
2. Make `app run` run the currently deployed app by default.
3. Add `--deploy` to `app run` for explicit package/deploy/run behavior.
4. Add `kindling runner ensure` as the explicit infrastructure command.
5. Add `kindling app run --pipeline <pipeline-id>` before deciding whether `pipeline` needs a first-class group.
6. Keep `kindling run <pipe_id>` as a deprecated local shortcut during Phase 1.
7. Leave `job` commands in place as compatibility/advanced commands.
8. Update docs to stop presenting jobs as the normal unit of work.

This gives users the right mental model quickly without forcing a large SDK rewrite in the same change.
