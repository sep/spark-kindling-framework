# Stage Processor Signal Inversion

**Status:** Proposal
**Created:** 2026-05-18
**Related:** signal_quick_reference.md, stage_processing.md, watermarking.md

---

## Summary

Refactor `StageProcessor` to remove its direct dependencies on `EntityProvider`,
`DataPipesExecution`, and `WatermarkEntityFinder`. Instead of calling these
collaborators in sequence, `StageProcessor` emits two staged signals and the
collaborators wire themselves as subscribers. The orchestrator stops knowing about
its collaborators.

A secondary, smaller inversion applies to `DataPipesExecuter`: remove its direct
dependency on `EntityReadPersistStrategy` by emitting a signal per pipe and letting
the strategy subscribe.

---

## Problem

`StageProcessor.__init__` currently injects five collaborators:

```python
@inject
def __init__(
    self,
    dpr: DataPipesRegistry,
    ep: EntityProvider,        # ← used only to ensure watermark table
    dep: DataPipesExecution,   # ← used only to run pipes
    wef: WatermarkEntityFinder,# ← used only to resolve watermark entity
    tp: SparkTraceProvider,
    signal_provider: Optional[SignalProvider] = None,
):
```

Three of those five (`ep`, `dep`, `wef`) exist solely to be called in two sequential
lines inside `execute()`:

```python
# simple_stage_processor.py:93-94
self.ep.ensure_entity_table(self.wef.get_watermark_entity_for_layer(layer))
self.dep.run_datapipes(stage_pipe_ids)
```

This means `StageProcessor` must import and depend on watermarking infrastructure,
entity management, and pipe execution in order to do its only job: run a stage. It
is pure glue code dressed as a class with dependencies.

The same pattern appears in `DataPipesExecuter`:

```python
# data_pipes.py — __init__ and run_datapipes
erps: EntityReadPersistStrategy  # injected only to call create_pipe_entity_reader
                                  # and create_pipe_persist_activator per pipe
pipe_entity_reader = self.erps.create_pipe_entity_reader
pipe_activator = self.erps.create_pipe_persist_activator
```

`DataPipesExecuter` knows about the persistence strategy only to hand closures to
`_execute_datapipe`. The strategy is not part of pipe execution logic; it is a
collaborator being orchestrated.

---

## Goals

1. `StageProcessor` injects only what is intrinsic to stage processing: the pipe
   registry (to enumerate pipes) and the tracer.
2. Watermark setup and pipe execution become subscriber responsibilities, not
   orchestrator knowledge.
3. `DataPipesExecuter` injects only what is intrinsic to pipe execution: the
   registries and the tracer. The persist strategy subscribes.
4. No behavior change. All existing signal emissions are preserved.
5. No breaking change to the public `execute()` interface.

---

## Proposed Design

### Part 1: StageProcessor

Add two new signals to `StageProcessingService.EMITS`:

| Signal | When | Payload |
|---|---|---|
| `stage.prepare` | Before pipe execution; subscribers should perform setup (e.g. ensure tables) | `stage`, `layer`, `pipe_ids`, `execution_id` |
| `stage.run` | After prepare; subscribers should execute the pipes | `stage`, `layer`, `pipe_ids`, `execution_id` |

`StageProcessor` drops `ep`, `dep`, and `wef` as injected dependencies and emits
these signals instead of calling those collaborators directly.

**Refactored StageProcessor:**

```python
class StageProcessingService(ABC):
    EMITS = [
        "stage.before_execute",
        "stage.prepare",        # new
        "stage.run",            # new
        "stage.after_execute",
        "stage.execute_failed",
    ]

@GlobalInjector.singleton_autobind()
class StageProcessor(StageProcessingService, SignalEmitter):
    @inject
    def __init__(
        self,
        dpr: DataPipesRegistry,
        tp: SparkTraceProvider,
        signal_provider: Optional[SignalProvider] = None,
    ):
        self.dpr = dpr
        self.tp = tp
        self._init_signal_emitter(signal_provider)

    def execute(self, stage, stage_description, stage_details, layer):
        execution_id = str(uuid.uuid4())
        start_time = time.time()

        pipe_ids = self.dpr.get_pipe_ids()
        stage_pipe_ids = [p for p in pipe_ids if p.startswith(stage)]

        self.emit("stage.before_execute", stage=stage, layer=layer,
                  pipe_ids=stage_pipe_ids, execution_id=execution_id)
        try:
            with self.tp.span(component=stage_description, operation=stage_description,
                              details=stage_details, reraise=True):
                self.emit("stage.prepare", stage=stage, layer=layer,
                          pipe_ids=stage_pipe_ids, execution_id=execution_id)
                self.emit("stage.run", stage=stage, layer=layer,
                          pipe_ids=stage_pipe_ids, execution_id=execution_id)

            self.emit("stage.after_execute", stage=stage, layer=layer,
                      pipe_ids=stage_pipe_ids,
                      duration_seconds=time.time() - start_time,
                      execution_id=execution_id)
        except Exception as e:
            self.emit("stage.execute_failed", stage=stage, layer=layer, error=str(e),
                      error_type=type(e).__name__,
                      duration_seconds=time.time() - start_time,
                      execution_id=execution_id)
            raise
```

`WatermarkManager` subscribes to `stage.prepare` and handles the ensure. It no
longer needs to be injected into `StageProcessor`:

```python
class WatermarkManager(WatermarkService, SignalEmitter):
    SUBSCRIBES = ["stage.prepare"]

    @inject
    def __init__(self, ep: EntityProvider, wef: WatermarkEntityFinder,
                 lp: PythonLoggerProvider, signal_provider: Optional[SignalProvider] = None):
        ...
        self._init_signal_emitter(signal_provider)
        self._get_signal("stage.prepare").connect(self._on_stage_prepare)

    def _on_stage_prepare(self, sender, layer, **kwargs):
        self.ep.ensure_entity_table(self.wef.get_watermark_entity_for_layer(layer))
```

`DataPipesExecuter` subscribes to `stage.run`:

```python
class DataPipesExecuter(DataPipesExecution, SignalEmitter):
    SUBSCRIBES = ["stage.run"]

    @inject
    def __init__(self, ...):
        ...
        self._get_signal("stage.run").connect(self._on_stage_run)

    def _on_stage_run(self, sender, pipe_ids, **kwargs):
        self.run_datapipes(list(pipe_ids))
```

### Part 2: DataPipesExecuter

Add a signal emitted once per pipe before execution:

| Signal | When | Payload |
|---|---|---|
| `datapipes.pipe_execute` | Before `_execute_datapipe` is called | `pipe`, `pipe_index`, `run_id` |

`EntityReadPersistStrategy` subscribes and returns the reader/activator closures via
blinker's synchronous return value collection:

```python
# DataPipesExecuter — run_datapipes inner loop
results = self.emit("datapipes.pipe_execute", pipe=pipe, pipe_index=index, run_id=run_id)
reader, activator = next(
    (v for _, v in results if v is not None),
    (None, None)
)
was_skipped = self._execute_datapipe(reader, activator, pipe)
```

```python
# EntityReadPersistStrategy subscriber
def _on_pipe_execute(self, sender, pipe, **kwargs):
    return (
        self.create_pipe_entity_reader(pipe),
        self.create_pipe_persist_activator(pipe),
    )
```

`DataPipesExecuter` drops `erps` from its constructor.

---

## Sequencing and Error Handling

Blinker `send()` is **synchronous** — subscribers execute in connection order and
exceptions propagate to the caller. The `stage.prepare` → `stage.run` ordering
is preserved because they are two sequential `emit()` calls inside the same `tp.span`
context. The tracer span still wraps both, so error attribution and reraise behavior
are unchanged.

The one behavioral difference: if no subscriber connects to `stage.prepare` or
`stage.run`, those emits are no-ops and the stage silently does nothing. This is
acceptable — a stage with no subscribers is not a valid configuration, and the
error would surface immediately during integration testing.

---

## What This Is Not

- Not an async or event-queue architecture. Blinker signals are synchronous; this
  is dependency inversion, not messaging.
- Not removing signals from the existing emit points. `stage.before_execute`,
  `stage.after_execute`, and `stage.execute_failed` remain unchanged.
- Not applicable to `WatermarkManager`'s internal read/save paths, `SimpleReadPersistStrategy`'s
  write decision logic, or `bootstrap.initialize_framework`. Those have return-value
  or ordering dependencies that don't invert cleanly.

---

## Migration

1. Add `stage.prepare` and `stage.run` to `StageProcessingService.EMITS`.
2. Add `SUBSCRIBES` annotation to `WatermarkManager` and `DataPipesExecuter` (documentation only — no framework enforcement required).
3. Wire subscriptions in `WatermarkManager.__init__` and `DataPipesExecuter.__init__`.
4. Remove `ep`, `dep`, `wef` from `StageProcessor.__init__`; replace lines 93-94 with two `emit()` calls.
5. Update unit tests: mock signal emissions instead of injecting mock collaborators into `StageProcessor`.
6. For Part 2: add `datapipes.pipe_execute` signal, wire `EntityReadPersistStrategy` subscriber, remove `erps` from `DataPipesExecuter.__init__`.

Parts 1 and 2 are independent and can be done in separate PRs.
