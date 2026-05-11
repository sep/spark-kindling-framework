# Runner-Aligned Remote App Execution Proposal

**Date:** 2026-05-07
**Status:** Implemented
**Scope:** remote app execution model, runner semantics, CLI behavior, migration from per-run job creation

---

## Executive Summary

Kindling's intended remote execution model is:

> One durable runner job exists per platform workspace. Apps are the unit of user work. The runner receives a request to run an app and executes that work on behalf of the user.

The current CLI and SDK messaging already describe that model, but the implementation still has a gap: `kindling app run --platform <platform>` creates a platform job definition for the target app and then starts that job directly. This makes the user-facing story and the operational model diverge.

This proposal aligns implementation with strategy by changing remote app runs to submit work through the durable runner instead of creating one-off app jobs.

The result is a simpler and more consistent mental model:

- `kindling app run` runs an app
- `kindling runner ensure|status|repair|delete` manages runner infrastructure
- lower-level SDK job primitives remain available for platform internals, but
  there is no end-user direct-job workflow

---

## Problem Statement

Today, the remote app run flow behaves roughly like this:

1. Resolve app name or package local app files.
2. Optionally deploy app assets.
3. Create a platform job definition for that app.
4. Start that job.
5. Stream logs and poll run status.

That behavior conflicts with several repository-level decisions and docs:

- the durable runner is already documented as the remote execution vehicle
- runner lifecycle commands already exist in the CLI
- the CLI overhaul strategy explicitly says apps and pipelines are the primary user concepts and jobs are infrastructure/advanced operations

This mismatch creates several issues.

### User Experience Issues

- `kindling app run` looks like an app-level command but behaves like a job-definition orchestration command.
- users can end up with many app-specific jobs even though the docs describe a single durable runner
- the presence of both `app run` and `job create` suggests two equally valid remote execution models
- troubleshooting is harder because users do not know whether to inspect the app, the runner, or an app-specific platform job

### Operational Issues

- repeated app runs can create/update many platform job definitions unnecessarily
- job lifecycle and app lifecycle become tightly coupled
- cleanup semantics are less clear because app assets and app-specific jobs are mixed together
- future work such as queueing, concurrency control, standardized run metadata, and consistent status reporting has no single choke point

### Architecture Issues

- the CLI and SDK tell one story while the implementation follows another
- the runner command group becomes conceptually correct but partially bypassed
- platform-specific runner behavior cannot become the canonical execution path while app run still creates direct jobs

---

## Goals

1. Make remote app execution follow the documented durable-runner model.
2. Preserve `kindling app run` as the main user-facing remote execution command.
3. Keep `kindling runner *` as the infrastructure/admin command family.
4. Keep low-level job primitives in the SDK for platform internals and specialized automation.
5. Avoid breaking local standalone app execution.
6. Keep `kindling app run` end-to-end: submit work, stream logs by default, and report final status.
7. Establish a single remote execution path that can later support richer scheduling, retries, queueing, audit metadata, and policy enforcement.

---

## Non-Goals

1. Replacing platform-native jobs entirely; the runner is still implemented using platform-native job primitives.
2. Changing local `kindling pipeline run` semantics.
3. Redesigning app packaging or deployment artifact formats.
4. Solving multi-app scheduling or tenancy policy in this proposal.

---

## Current vs Proposed Model

### Current Model

Remote `kindling app run`:

- optionally deploys app assets
- creates a platform job definition for the app
- runs that job directly
- streams logs for that job run

Mental model implied by behavior:

> each app run maps to a platform job definition

### Proposed Model

Remote `kindling app run`:

- optionally deploys app assets
- ensures a durable runner exists or fails with a clear hint
- submits an app-run request to the runner
- streams logs for the resulting runner invocation
- reports app/run status in app-centric terms

Mental model implied by behavior:

> apps are submitted to the runner; the runner is the platform execution vehicle

---

## Proposed User-Facing Behavior

### Primary App Workflow

Normal users should think in terms of app assets and app runs.

```bash
# Deploy app assets explicitly
kindling app deploy --local-folder ./orders --platform synapse

# Run the deployed app through the runner
kindling app run orders --platform synapse

# Package + deploy + run local assets through the runner
kindling app run ./orders --platform synapse

# Inspect run state and logs
kindling app status <run-id> --platform synapse
kindling app logs <run-id> --platform synapse
```

### Runner Workflow

Platform infrastructure remains explicit and separate.

```bash
kindling runner ensure --platform synapse
kindling runner status --platform synapse
kindling runner repair --platform synapse
kindling runner delete --platform synapse
```

---

## Command Semantics

### `kindling app run`

Remote `app run` should become a runner submission command, not a direct job-definition command.

#### Behavior

When `--platform` is a managed platform:

1. Resolve the target app name.
2. If APP is a local path, package/deploy app assets first.
3. Verify platform credentials/config.
4. Verify the runner exists.
5. Submit an app-run payload to the runner.
6. Receive a run identifier.
7. Stream logs by default.
8. Report final state using stable app-run output.

#### Failure Modes

If no runner exists:

- default behavior should fail with a clear message
- error should include: `Run 'kindling runner ensure --platform <platform>'`

An optional future enhancement can add `--ensure-runner` for convenience, but it is not required for Phase 1. Keeping runner installation explicit is simpler and safer.

### `kindling app status`

This should report status for the logical app run, even if underneath the platform is tracking a runner invocation.

The CLI can still use runner/platform APIs internally, but the output contract should remain app-centric.

### `kindling app logs`

This should resolve logs for the logical app run submitted through the runner. Users should not need to know the underlying runner job ID in ordinary usage.

### `kindling app cancel`

This should cancel the logical app run submitted through the runner.

---

## SDK and API Changes

The CLI should not have to construct runner semantics ad hoc. The SDK should expose runner-oriented app execution helpers.

### Proposed SDK Surface

Possible shape:

```python
api.ensure_runner()
api.get_runner_status()
api.submit_app_run(app_name, environment=None, parameters=None)
api.get_app_run_status(run_id)
api.get_app_run_logs(run_id, from_line=0, size=1000)
api.stream_app_run_logs(run_id, callback=..., poll_interval=5.0, max_wait=300.0)
api.cancel_app_run(run_id)
```

This keeps runner-specific job IDs and payload details behind the SDK boundary.

### Compatibility Note

The existing lower-level primitives can remain:

- `create_job`
- `run_job`
- `get_job_status`
- `get_job_logs`
- `cancel_job`
- `delete_job`

But `kindling app run` should stop orchestrating them directly for normal remote execution.

---

## Payload Model

The runner needs a stable request envelope for app runs.

### Suggested Request Envelope

```yaml
kind: app_run
app_name: orders
environment: dev
parameters:
  batch_date: 2026-05-07
source:
  deployment: existing
invocation:
  requested_by: cli
  cli_version: 0.9.x
  submitted_at: 2026-05-07T16:00:00Z
```

If the CLI ran with a local source path and deployed assets first, that should still resolve to a deployed app name before submission.

### Required Output Fields

For stable JSON across remote run-like commands:

```json
{
  "app_name": "orders",
  "platform": "synapse",
  "run_id": "...",
  "state": "RUNNING|SUCCEEDED|FAILED|...",
  "succeeded": true,
  "runner_id": "..."
}
```

The exact shape can evolve, but the top-level user contract should be stable and app-centric.

---

## CLI Output and Help Changes

To reinforce the model, help text should change in these ways.

### `kindling app run`

Help text should say that remote execution submits the app to the durable runner.

Suggested language:

> Run an app locally with standalone, or submit it to the durable Kindling runner on a managed platform.

### `kindling runner`

This group already describes the right mental model. After implementation, that description will become fully accurate rather than partially aspirational.

## Migration Strategy

This should be a staged migration, not a flag day.

### Phase 1: SDK Support

- add runner-oriented SDK methods for app submission, status, logs, and cancellation
- implement platform adapters behind that API
- keep existing job methods intact

### Phase 2: CLI App Run Switch

- change remote `kindling app run` to use runner submission
- change `app status`, `app logs`, and `app cancel` to resolve app-run semantics through the runner-aware SDK methods
- preserve JSON keys where possible

### Phase 3: Documentation Realignment

- update developer workflow docs
- update CLI package README
- keep lower-level SDK job primitives documented as platform internals rather
  than CLI user workflows

### Phase 4: Compatibility Messaging

- add notes in release docs describing the internal shift from direct app jobs to runner-submitted app runs
- explicitly state that direct jobs are SDK/platform internals, while runner
  commands are the CLI surface

---

## Backward Compatibility

This proposal is intended to preserve the external happy path:

- `kindling app run <app> --platform <platform>` still works
- `kindling app status/logs/cancel` still work
- local standalone behavior stays unchanged
- there is no generated `job.yaml` or top-level end-user direct-job workflow

What changes is the remote execution path behind those commands.

Potential compatibility concerns:

1. Existing automation may assume app runs create or update app-specific platform jobs.
2. Existing debugging scripts may inspect app-specific job IDs.
3. Some logs/status implementations may currently depend on direct platform job identity.

Mitigation:

- maintain a clear app-run identifier contract in the CLI and SDK
- include runner/job IDs in verbose or JSON output where helpful
- document the behavior shift in release notes

---

## Testing Strategy

### Unit Tests

- `kindling app run` remote path submits through runner-aware SDK methods
- no direct `create_job()` call in the normal remote app path
- `app status/logs/cancel` use app-run oriented SDK methods where implemented
- help text reflects runner submission model

### Integration Tests

- CLI to SDK contract tests for runner submission payloads
- stable JSON output tests for app run/status/logs/cancel

### System Tests

For each platform:

1. ensure runner
2. deploy app assets
3. submit app run through CLI/SDK
4. validate run completion
5. validate log retrieval
6. validate cancellation behavior

---

## Risks

### Risk: Runner Does Not Yet Expose All Needed Observability Hooks

If platform implementations can only expose low-level job/run APIs today, app-run status/log mapping may require an intermediate compatibility layer.

Mitigation:

- add runner-aware SDK methods first
- allow internal use of low-level APIs behind the SDK boundary

### Risk: Platform Differences Leak Through

Databricks, Fabric, and Synapse differ in log retrieval and run-state models.

Mitigation:

- keep those differences inside the SDK/platform adapters
- normalize status values and log retrieval in a common app-run abstraction

### Risk: Existing Users Depend on App-Specific Platform Jobs

Some users may have built workflows assuming every app corresponds to a platform job definition.

Mitigation:

- preserve SDK job primitives for platform internals and specialized automation
- document the runner/app path without exposing a separate CLI job workflow

---

## Alternatives Considered

### 1. Keep Current Behavior and Only Update Docs

Rejected.

This would preserve the implementation gap and make the runner story permanently aspirational.

### 2. Hide Runner Commands and Lean Into App-Specific Jobs

Rejected.

This contradicts the documented design and loses the architectural benefits of a single durable execution vehicle.

### 3. Add a New `kindling submit` Command

Rejected for now.

The existing `kindling app run` surface is already the right user-facing command. The problem is the underlying implementation path, not the command name.

---

## Recommended Decision

Adopt the runner-aligned model for remote app execution.

Specifically:

- keep `kindling app run` as the primary remote execution command
- route remote app runs through the durable runner
- keep runner lifecycle explicit and separate
- keep raw SDK job primitives available for advanced/platform-internal use
- update docs and SDK contracts to match the implementation

This is the smallest change that restores consistency between strategy, docs, implementation, and user expectations.

---

## Suggested Next Steps

1. Add runner-oriented app submission methods to `spark-kindling-sdk`.
2. Refactor CLI remote app run/status/logs/cancel to use those methods.
3. Update the main workflow docs and CLI package README to describe job commands as advanced operations.
4. Add platform system tests that validate the runner path end to end.
