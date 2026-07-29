# Proposal: Comprehensive Tracing Instrumentation

**Status:** Implemented (gh#210). This document is the decision record for
how the framework got span coverage and, in particular, why the
signal-bridging aspect approach was rejected.

## Goal

A coherent span tree for every major framework operation on all three trace
providers (EventBasedSparkTrace, PlainPythonTraceProvider,
AzureMonitorTraceProvider), assertable in unit tests, with no code changes
required to adopt and nothing emitted from hot loops:

- standalone `run_datapipes` yields `run → pipe.run → read×N → persist`
  with entity-provider operation children;
- bootstrap yields `initialize → config_download → … → app_run`;
- migration, streaming, deploy, and design-time CLI operations yield
  equivalent trees.

## Decision summary

1. **Provider-op tracer at the registry chokepoint** (`kindling.trace_ops`).
   Every provider resolution funnels through
   `EntityProviderRegistry.get_provider`; the resolved instance's op methods
   are shadowed per-instance with span wrappers. Object identity is
   preserved, so `isinstance` capability probes, `hasattr` checks
   (`merge_to_entity` is in no ABC), and cached-singleton semantics all
   hold. This is the "provider-wide coverage without touching providers".
   The legacy `EntityProvider` DI singleton (the `self.ep` path) is the same
   delta instance, wrapped once at bootstrap.
2. **Direct context-manager spans at structural seams** — pipes
   read/persist, watermarking, config reload, migration, orchestrator
   plan/generation/pipe, streaming lifecycle and recovery, ingestion,
   deploy. All new instrumentation passes `reraise=True`.
3. **Bootstrap tree via retro-recording.** Early phases predate a usable
   ConfigService and the final provider is only settled after extension
   imports, so live early spans are impossible by construction. Phase
   windows are recorded in a plain list and flushed once at the end of a
   full initialization through the new `record_span` provider method
   (explicit timestamps). Failures before a usable ConfigService lose the
   tree (stdlib logs only).
4. **Config-first gating**: `kindling.telemetry.tracing.enabled` (default
   true) and `kindling.telemetry.tracing.level`
   (minimal/standard/verbose, default standard), read once at bootstrap
   wiring (registration-gated like WatermarkAspect) and cached per service.
   The `telemetry.tracing.*` namespace was chosen over the issue's literal
   `kindling.tracing.*` for family consistency.
5. **Naming convention**: component = `kindling.<area>`, operation = short
   stable verb, ids/counts as whitelisted attributes, never in names.
   Existing spans were renamed to the convention while adoption is low
   (`execute_datapipes` → `kindling.pipes`/`run`, `pipe-{id}` →
   `pipe.run` + `pipe_id` attr, `query-{label}` →
   `kindling.streaming`/`query`, …). The pipe span previously exported
   `pipe.tags` wholesale — tags may carry credential references and are now
   replaced by a whitelist.
6. **Design-time CLI**: separate process, no DI — a lazily built
   PlainPythonTraceProvider gated by `KINDLING_TRACE=1`. The SDK stays
   untouched (it deliberately has no kindling-core dependency); CLI-side
   spans bracket its calls.
7. **Provider-layer fixes** required for tree coherence: parent restore on
   span exit in EventBasedSparkTrace (consecutive runs no longer share a
   trace id), `SparkSpan.parent_id` + `parentSpanId` emission (trees are
   linked, not inferred from time containment), the otel `start_span`
   missing-`traceId` TypeError, and `record_span` (ABC-default so
   third-party providers keep working).
8. **Persist reraise fix** (data correctness, found during design): the
   batch persist path swallowed merge failures (`reraise=False` inner
   span), fired `persist.after_persist`, and let WatermarkAspect advance
   the watermark past unpersisted data. The consolidated persist span
   passes `reraise=True` and the failure path is regression-tested.

## Rejected: signal-bridging TraceAspect (issue option 1)

The issue's preferred direction was an aspect subscribing to signals and
pairing before/after emissions into spans, on the principle that signals
are the universal instrumentation seam. Inverting the gap analysis (adding
signals where they are missing rather than direct spans) was explicitly
considered and rejected on evidence:

- **No correlation keys** outside four seams (persist_id, execution_id,
  run_id, batch_id): before/after pairing is ambiguous under DAG-parallel
  execution. `SignalPayload.operation_id` exists but is used nowhere.
- **Missing failure signals** (`entity.read_failed`, `watermark.get_failed`,
  …): a bridged span would leak open on failure. Observability needs
  exception-safe bracketing that today's payloads cannot guarantee — the
  WatermarkAspect precedent attaches *behavior* via signals, not brackets.
- **Provider signals are delta-only** (SignalEmitter is mixed into that one
  concrete class; 1/9 coverage), so "bridge the signals" degenerates into
  "add emissions to every provider" — at which point the aspect's
  no-per-provider-edits advantage evaporates.
- **Paired events cannot context-nest**: the OTel provider needs a live
  context manager for parenting; bridged pairs produce flat trees.
- **Hot-loop budget**: delta's `merge_as_stream` emits
  `entity.before/after_merge` per micro-batch; bridging would turn every
  batch into spans. (That per-batch signal cost itself is a latent issue if
  a consumer ever subscribes — noted, untouched here.)

Revisit only if signals later gain a uniform `operation_id` plus complete
failure pairs. Other rejected alternatives: `set_provider_decorator` as the
op-tracer seam (single-slot public seam claimed for execution-mode provider
personalities; taking it breaks user decorators), dynamic subclass proxies
(break cached-instance identity), and a NoopTraceProvider binding for
`enabled=false` (registration-gating is simpler and matches the
WatermarkAspect pattern).

## Accepted risks / follow-ups

- **Span renames change exported names**: dashboards keyed on
  `execute_datapipes`/`merge_and_watermark` must re-key. Normalizing now,
  while adoption is low, was chosen over freezing legacy names.
- **EventBased parent-restore changes semantics**: consecutive runs in one
  session previously shared one trace id; each top-level operation is now
  its own trace (matches Plain/OTel).
- **DAG-parallel + OTel**: worker-thread pipe spans do not parent to the
  run span (OTel context does not cross threads automatically; core cannot
  import otel). EventBased/Plain inherit the trace id via the shared
  current_span — racy but functional, pre-existing. Follow-up if needed:
  contextvars-based current span with propagation at task submission.
- **Otel extension without a connection string silently no-ops all spans**
  — pre-existing, more consequential now that instrumentation is
  comprehensive. Recommended follow-up issue: warn and fall back.
- **Defaults on** (`enabled=true`, `level=standard`) add span volume for
  every user on upgrade, honoring "no code changes to adopt"; the opt-out
  is documented in the configuration reference.
