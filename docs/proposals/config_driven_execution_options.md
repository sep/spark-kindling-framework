# Proposal: Config-Driven Execution Options

**Status:** Phase 1 in progress
**Author:** derived from migration-gap analysis (see
[Migrating from runMultiple](../guide/migrating_from_runmultiple.md))

## Problem

Execution behavior for DAG-based pipe runs — parallelism, worker count,
error strategy, timeouts, caching — is controlled exclusively by function
parameters on `GenerationExecutor.execute()` / `ExecutionOrchestrator` /
`run_datapipes(use_dag=True, ...)`. This conflicts with the framework's
design principle that behavior is dictated by the hierarchical configuration
system (settings → platform → workspace → environment), the way
`kindling.delta.access_mode` or `kindling.telemetry.*` already work.

Consequences today:

- Changing prod from sequential to parallel execution requires a code edit
  and redeploy, not a workspace config change.
- Per-environment behavior (sequential locally, `max_workers: 8` in prod)
  must be hand-rolled by every app.
- Upcoming features (retry, dependent-skipping) would compound the problem
  if they also landed as parameters only.

Additionally, two capability gaps were identified against notebook-DAG
orchestration (`runMultiple`) during the migration-guide work:

1. **No per-pipe retry** (`runMultiple` has `retry` /
   `retryIntervalInSeconds` per activity).
2. **Fail-fast is coarser than dependent-skipping** — a failure halts all
   later generations, including branches independent of the failure.

## Principle

> Config dictates; parameters override just-in-time.

Every execution option resolves in this order:

1. Built-in default (current hardcoded value — behavior-compatible).
2. Hierarchical config under `kindling.execution.*`.
3. Explicitly passed parameter (spot-testing escape hatch).

Function parameters default to an `UNSET` sentinel meaning "fall through to
config". Passing a real value always wins, but is the *last* resort, not the
primary interface.

## Config Schema

```yaml
kindling:
  execution:
    parallel: false          # run independent pipes within a generation concurrently
    max_workers: 4           # thread pool size when parallel
    error_strategy: fail_fast  # fail_fast | continue | (phase 3) skip_dependents
    pipe_timeout: null       # per-pipe timeout in seconds; null = none
    auto_cache: false        # persist()/cache() recommended shared entities
    retry:                   # (phase 2)
      attempts: 0
      interval_seconds: 30
    pipes:                   # (phase 2) per-pipe overrides, keyed by pipeid
      <pipeid>:
        retry: { attempts: 2, interval_seconds: 60 }
        pipe_timeout: 1200
```

## Phase 1 — Config resolution layer (this change)

- `GenerationExecutor.execute()` / `execute_batch()` and
  `ExecutionOrchestrator.execute()` / `execute_batch()` and
  `DataPipesExecuter.run_datapipes_dag()` change parameter defaults from
  hardcoded values to the `UNSET` sentinel.
- Resolution happens once, in `GenerationExecutor.execute()` — the choke
  point all entry paths flow through. `GenerationExecutor` already has
  `ConfigService` injected.
- Config values are coerced defensively (YAML/env vars may deliver strings):
  unparseable values log a warning and fall back to the built-in default.
- `execute_streaming()` continues to pass `parallel=False` explicitly —
  streaming startup order matters, config cannot enable parallelism there.
- Out of scope, unchanged: `no_watermark` (inherently a just-in-time spot-run
  flag) and `streaming_options` (structured per-pipe payload, revisit later).

Backward compatibility: all existing call sites keep working. The only
behavior change is intentional — config can now enable options that
previously required code changes.

## Phase 2 — Per-pipe retry

- `kindling.execution.retry.attempts` / `interval_seconds` as run-level
  defaults; `kindling.execution.pipes.<pipeid>.retry.*` per pipe.
- Retry lives in `GenerationExecutor._execute_pipe` — the single choke
  point for both sequential and parallel paths.
- Policy lives in **config, not the decorator**: `@DataPipes.pipe()` stays
  purely structural; ops can tune retry per environment.
- Guardrails (from the re-runnability analysis):
  - Retry only exceptions raised *inside* the attempt. Never retry a
    `TimeoutError` surfaced by `future.result()` in parallel mode — the
    first attempt's thread may still be running and could commit its write
    later (zombie double-write).
  - Warn at execution start when retry is configured for a pipe whose
    output provider lacks `merge_to_entity` (append-path pipes are
    at-least-once under retry; merge-path pipes are idempotent given true
    merge keys and a deterministic transform).
- Emit `orchestrator.pipe_retrying`; record `attempts` on `PipeResult`.
- Future hardening: `persist_id`-based idempotent persist to make retry
  safe by default on non-merge paths.

## Phase 3 — `skip_dependents` error strategy

- New `ErrorStrategy.SKIP_DEPENDENTS`: on pipe failure, mark transitive
  consumers (via `PipeGraph.entity_consumers`) as skipped, keep independent
  branches running. Matches `runMultiple`'s dependent-skipping semantics.
- Selectable via `kindling.execution.error_strategy: skip_dependents`.
- Follow-up: `ExecutionOrchestrator.resume(result)` to re-execute
  `failed_pipes` (and their skipped dependents) without recomputing
  successes.

## Explicitly not planned

- **Whole-run timeout** (`runMultiple`'s `timeoutInSeconds`) — platform job
  timeouts cover this; revisit only on demand.
- **Plan preview API** — already exists: `ExecutionPlanGenerator` is a
  public injectable service (`generate_batch_plan()` etc.).
