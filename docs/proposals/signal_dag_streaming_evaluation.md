# Evaluation: Signal Eventing, DAG Execution, and Streaming Support Proposals

**Evaluation Date:** January 30, 2026
**Evaluator:** Technical Review Team
**Source Document:** [signal_dag_streaming_proposal.md](signal_dag_streaming_proposal.md)

---

## Executive Summary

This document evaluates the three interconnected proposals for enhancing the Kindling framework. Overall assessment:

| Feature | Recommended Option | Risk Level | Priority | Readiness |
|---------|-------------------|------------|----------|-----------|
| **Signal Eventing** | Option C (Hybrid) | 🟢 Low | 🔥 High | ✅ Ready |
| **DAG Execution** | Strategy Pattern | 🟡 Medium | 🔥 High | ✅ Ready |
| **Streaming Support** | Comprehensive | 🟡 Medium | ⚡ High | ⚠️ Needs refinement |
| **Dimension Restarts** | As proposed | 🟠 Medium-High | 📊 Medium | ⚠️ Needs refinement |

**Overall Recommendation:** ✅ **Proceed with phased implementation**, with modifications noted below.

---

## Part 1: Signal Eventing System Evaluation

### Option A: Simple Emit Pattern

**Strengths:**
- ✅ Minimal disruption to existing codebase
- ✅ Quick to implement (1-2 weeks)
- ✅ Maintains backward compatibility
- ✅ `EMITS` list provides basic documentation
- ✅ Flexible for rapid iteration

**Weaknesses:**
- ❌ No compile-time safety (typos cause silent failures)
- ❌ `EMITS` list not enforced (can become stale)
- ❌ Payload structure not validated
- ❌ Limited IDE support (no autocomplete for signal names)
- ❌ Documentation must be manually maintained

**Technical Risks:**
- 🟡 Signal name typos won't be caught until runtime
- 🟡 Payload inconsistencies between emitter and subscriber
- 🟢 Low risk of breaking existing code

**Verdict:** ✅ **Good for MVP/Phase 1**, but plan migration path to typed signals.

---

### Option B: Registry-Style Typed Signals

**Strengths:**
- ✅ Type-safe payloads (IDE autocomplete, validation)
- ✅ Self-documenting API
- ✅ Can auto-generate documentation from registry
- ✅ Catches errors at emit time
- ✅ Refactoring-safe (rename signal → update all usages)

**Weaknesses:**
- ❌ Significant boilerplate (dataclass per signal)
- ❌ Breaking change for dynamic signal patterns
- ❌ Overhead for simple signals
- ❌ Requires 3-4 weeks implementation
- ❌ May be overkill for low-frequency signals

**Technical Risks:**
- 🟡 Complexity may deter extension developers
- 🟡 Dataclass overhead for high-frequency signals
- 🟠 Migration from existing code could be disruptive

**Verdict:** ✅ **Ideal for Phase 2+**, after patterns stabilize.

---

### Option C: Hybrid Approach (Recommended)

**Strengths:**
- ✅ Best of both worlds: type-safe core, flexible extensions
- ✅ Progressive enhancement path
- ✅ Core framework gets safety, extensions get flexibility
- ✅ Can migrate signals from string → typed incrementally
- ✅ Balances developer experience with safety

**Weaknesses:**
- ⚠️ Two patterns to learn (but clearly scoped)
- ⚠️ Requires discipline to decide when to use which
- ⚠️ Documentation must explain both patterns

**Technical Risks:**
- 🟢 Low risk: systems coexist naturally
- 🟢 Clear boundary: core = typed, extensions = flexible

**Implementation Considerations:**
- Start with simple emit pattern for all signals
- Migrate high-traffic signals (datapipes, entities) to typed in Phase 2
- Keep extension/plugin signals string-based

**Verdict:** ✅ **RECOMMENDED** - Pragmatic balance with clear migration path.

---

### Async Signal Support Evaluation

**Proposal:** Wrap sync signals to schedule async handlers via `asyncio.create_task()`

**Strengths:**
- ✅ Enables non-blocking signal handlers
- ✅ Useful for streaming listeners (progress reporting, metrics)
- ✅ Minimal changes to blinker integration
- ✅ Event loop management isolated to async handlers

**Weaknesses:**
- ⚠️ Requires active event loop (not guaranteed in all contexts)
- ⚠️ Errors in async handlers don't propagate to emitter
- ⚠️ Debugging async failures more complex
- ⚠️ Spark environments don't have default event loop

**Critical Issues:**
1. **Event Loop Availability:** Spark driver doesn't run event loop by default
2. **Lifecycle Management:** Who starts/stops the loop? When?
3. **Error Handling:** Uncaught exceptions in async handlers silently fail

**Recommendations:**
- ✅ Keep sync signals as default
- ✅ Provide opt-in async wrapper for specific use cases
- ⚠️ Document event loop requirements clearly
- ⚠️ Provide error callback mechanism for async handlers
- ⚠️ Consider ThreadPoolExecutor as alternative to asyncio

**Modified Approach:**
```python
class SignalAsyncSupport:
    """Optional async support - use ThreadPoolExecutor as fallback."""

    def connect_async(self, signal_name: str, async_handler: Callable):
        if self._has_event_loop():
            # Use asyncio if available
            self._connect_with_asyncio(signal_name, async_handler)
        else:
            # Fallback to thread pool
            self._connect_with_threadpool(signal_name, async_handler)
```

**Verdict:** ✅ **Useful but optional** - Implement in Phase 3, not critical path.

---

## Part 2: DAG Execution System Evaluation

### Strategy Pattern (Recommended)

**Strengths:**
- ✅ Clean separation of concerns (strategy per execution mode)
- ✅ Easy to add new strategies (e.g., priority-based, cost-optimized)
- ✅ Testable in isolation
- ✅ Follows established design patterns
- ✅ Leverages NetworkX's proven topological algorithms

**Weaknesses:**
- ⚠️ NetworkX is not pre-installed on all platforms (Databricks)
- ⚠️ Fallback to graphlib provides sequential-only execution
- ⚠️ No parallel execution within generations in fallback mode

**Technical Evaluation:**

#### Batch vs Streaming Execution Order

**Batch (Forward Topo Sort):**
```
Generation 0: [producer_a, producer_b]  → Run first
Generation 1: [transformer_c]           → Run after producers
Generation 2: [consumer_d]              → Run last
```
✅ **Correct for batch:** Data flows naturally, dependencies satisfied.

**Streaming (Reverse Topo Sort):**
```
Generation 0: [consumer_d]              → Start first (listening)
Generation 1: [transformer_c]           → Start second (listening)
Generation 2: [producer_a, producer_b]  → Start last (emitting)
```
✅ **Correct for streaming:** Consumers ready before producers emit.

**Verdict:** ✅ **CORRECT DESIGN** - Rationale is sound.

#### Cache Recommendations

**Proposal:** Identify entities consumed by multiple pipes → recommend caching.

**Strengths:**
- ✅ Automatic optimization without user intervention
- ✅ Significant performance improvement for shared entities
- ✅ Simple algorithm (count consumers per entity)

**Concerns:**
- ⚠️ Cache level (`MEMORY_AND_DISK`) may not suit all cases
- ⚠️ No consideration of entity size (small entities don't need caching)
- ⚠️ Cache eviction strategy not addressed

**Recommendations:**
- ✅ Keep cache recommendations as advisory, not automatic
- ✅ Add configuration to override cache level
- ✅ Consider entity size in heuristic (e.g., only cache if > 100 MB)

#### Parallel Execution

**Proposal:** Use `ThreadPoolExecutor` to run pipes in parallel within generations.

**Critical Issue:** ⚠️ **Spark is not thread-safe for DataFrame operations!**

**Problems:**
1. Spark context is not thread-safe
2. Multiple threads creating DataFrames simultaneously can cause corruption
3. PySpark uses global state (SparkSession singleton)

**Correct Approach:**
```python
# ❌ WRONG - parallel DataFrame creation
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(pipe_executor, pipe_id) for pipe_id in generation]

# ✅ CORRECT - parallel Python operations, sequential Spark operations
# OR use multiprocessing with separate Spark sessions
```

**Recommendations:**
- ⚠️ **Do NOT parallelize Spark DataFrame operations with threads**
- ✅ Keep parallel execution for Python-only operations (file prep, etc.)
- ✅ Consider process pool for true parallelism (overhead may not be worth it)
- ✅ Document that parallelism is for planning, not execution (or limited scope)

**Modified Proposal:**
```python
class GenerationExecutor:
    def execute_generation(self, generation, pipe_executor):
        # For now, sequential execution within generation
        # Parallelism comes from Spark's distributed execution, not threads
        for pipe_id in generation:
            pipe_executor(pipe_id)

        # Future: Could use multiprocessing for independent Spark jobs
```

**Verdict:** ⚠️ **NEEDS MODIFICATION** - Remove thread-based parallelism or clarify scope.

---

### Configuration-Based Approach

**Strengths:**
- ✅ User-friendly configuration object
- ✅ Easy to adjust behavior without code changes

**Weaknesses:**
- ⚠️ Less extensible than strategy pattern
- ⚠️ Configuration object can become bloated

**Verdict:** ✅ Use for user-facing API, strategy pattern for internal implementation.

---

## Part 3: Structured Streaming Support Evaluation

### Comprehensive Streaming Manager (Option A)

**Strengths:**
- ✅ Complete feature set (lifecycle, monitoring, recovery)
- ✅ Sync-based fits Spark's driver execution model
- ✅ Signal-driven architecture for loose coupling
- ✅ Configurable retry with exponential backoff

**Weaknesses:**
- ⚠️ Complex implementation (4-5 weeks)
- ⚠️ Thread-based monitoring adds complexity
- ⚠️ No integration with existing `SimplePipeStreamOrchestrator`

**Component Evaluation:**

#### StreamingQueryManager

**Strengths:**
- ✅ Clean lifecycle management (register → start → stop)
- ✅ Thread-safe with locks
- ✅ Metrics tracking per query

**Concerns:**
- ⚠️ `query_builder` callable pattern may not fit all use cases
- ⚠️ No integration with existing pipe metadata
- ⚠️ Metrics don't capture Spark's native query metrics

**Recommendations:**
- ✅ Extend `PipeMetadata` to include streaming config
- ✅ Bridge to Spark's `StreamingQuery.status` for metrics
- ✅ Support both builder callable and direct `StreamingQuery` registration

#### StreamingWatchdog

**Strengths:**
- ✅ Separate thread for non-blocking monitoring
- ✅ Configurable health check interval
- ✅ Stale query detection

**Concerns:**
- ⚠️ Polling-based (not event-driven)
- ⚠️ No coordination with Spark's `StreamingQueryListener`
- ⚠️ Duplicate monitoring (watchdog + listener)

**Recommendations:**
- ✅ Use `StreamingQueryListener` as primary source
- ✅ Watchdog as backup for listener failures
- ✅ Reduce polling frequency (watchdog as safety net, not primary)

#### StreamingRecoveryManager

**Strengths:**
- ✅ Exponential backoff is industry standard
- ✅ Max retry limit prevents infinite loops
- ✅ Separate thread for non-blocking recovery

**Concerns:**
- ⚠️ No distinction between transient vs permanent failures
- ⚠️ Recovery always restarts from checkpoint (may not be desired)
- ⚠️ No alert mechanism when max retries exhausted

**Recommendations:**
- ✅ Add failure classification (transient, permanent, unknown)
- ✅ Support custom recovery strategies (not just restart)
- ✅ Emit `recovery_exhausted` signal for human intervention

#### KindlingStreamingListener

**Strengths:**
- ✅ Bridges Spark events to Kindling signals
- ✅ Captures progress, termination events
- ✅ Non-blocking recovery trigger

**Concerns:**
- ⚠️ Listener callbacks run on Spark's listener thread (blocking concerns)
- ⚠️ Spawning threads from listener may cause issues
- ⚠️ Exception in listener can crash all streaming queries

**Critical Issue:** Spark's `StreamingQueryListener` callbacks must not block!

**Recommendations:**
- ✅ Make callbacks extremely lightweight (queue event, return immediately)
- ✅ Process events in separate thread (queue → processor pattern)
- ✅ Add exception handling around all listener methods

**Modified Approach:**
```python
class KindlingStreamingListener(StreamingQueryListener):
    def __init__(self, ...):
        self._event_queue = queue.Queue()
        self._processor_thread = threading.Thread(target=self._process_events)
        self._processor_thread.start()

    def onQueryProgress(self, event):
        # Non-blocking: queue and return
        self._event_queue.put(('progress', event))

    def _process_events(self):
        # Process queued events in background thread
        while True:
            event_type, event = self._event_queue.get()
            try:
                self._handle_event(event_type, event)
            except Exception as e:
                logger.error(f"Error processing event: {e}")
```

**Verdict:** ⚠️ **NEEDS REFINEMENT** - Add queue-based processing, better error handling.

---

### Event Loop-Based Async Monitoring (Option B)

**Strengths:**
- ✅ Modern async patterns
- ✅ Efficient for high-frequency monitoring

**Weaknesses:**
- ❌ Event loop not guaranteed in Spark driver
- ❌ More complex than sync approach
- ❌ Doesn't fit Spark's execution model

**Verdict:** ❌ **NOT RECOMMENDED** - Sync approach is more appropriate for Spark.

---

## Part 4: Dimension-Driven Stream Restart Evaluation

### Overall Concept

**Strengths:**
- ✅ Solves real pain point (stale dimension data in streams)
- ✅ Signal-driven architecture fits framework design
- ✅ Graceful restart preserves checkpoints
- ✅ Cascading restarts handle dependencies

**Weaknesses:**
- ⚠️ Complex feature with many edge cases
- ⚠️ Restart downtime means data loss (streaming gap)
- ⚠️ Dimension version polling adds overhead

**Alternative Approaches:**
1. **Streaming dimension table** - Use Delta's change data feed
2. **Periodic refresh** - Restart on schedule, not on change
3. **Dual-stream join** - Join with dimension stream (complex but no downtime)

**Recommendation:** ✅ Proceed, but provide alternatives in documentation.

---

### DimensionChangeDetector

**Strengths:**
- ✅ Simple version polling approach
- ✅ Watches multiple dimensions efficiently
- ✅ Configurable check interval

**Concerns:**
- ⚠️ Polling every dimension every 60s could be expensive
- ⚠️ No batching of version checks
- ⚠️ Delta's `get_entity_version()` may not be cheap
- ⚠️ No consideration of dimension size (small changes don't need restart)

**Recommendations:**
- ✅ Batch version checks (single query for all dimensions)
- ✅ Add minimum change threshold (version delta > N)
- ✅ Consider Delta change data feed as alternative to polling
- ✅ Support user-defined change detection (not just version)

**Modified Approach:**
```python
class DimensionChangeDetector:
    def _check_all_dimensions(self):
        """Batch check all dimensions in single query."""
        dimension_ids = list(self._watched_dimensions.keys())

        # Batch query for all versions
        versions = self.entity_provider.get_versions_batch(dimension_ids)

        for entity_id, current_version in versions.items():
            last_version = self._watched_dimensions[entity_id]
            if current_version > last_version:
                self._handle_dimension_change(entity_id, last_version, current_version)
```

---

### StreamRestartController

**Strengths:**
- ✅ Signal-driven restart coordination
- ✅ Graceful shutdown → update → restart sequence
- ✅ Background thread prevents blocking
- ✅ Tracks dependencies per query

**Concerns:**
- ⚠️ No coordination between multiple restarts (race conditions)
- ⚠️ Restart sequence doesn't verify checkpoint validity
- ⚠️ No pre-restart validation (dimension might not be ready)
- ⚠️ Restart always stops query (no zero-downtime option)

**Critical Issues:**

1. **Checkpoint Compatibility:** After dimension schema change, checkpoint may be invalid
2. **Restart Storms:** Multiple dimensions changing → many queries restarting
3. **Resource Contention:** Multiple queries restarting simultaneously

**Recommendations:**
- ✅ Add restart throttling (max N concurrent restarts)
- ✅ Validate checkpoint before restart (or offer clean start option)
- ✅ Add restart coordination (queue restarts, execute serially)
- ✅ Support "preview" mode (validate dimension before commit to restart)

**Modified Approach:**
```python
class StreamRestartController:
    def __init__(self, ...):
        self._restart_queue = queue.Queue()
        self._restart_semaphore = threading.Semaphore(max_concurrent_restarts)
        self._restart_processor = threading.Thread(target=self._process_restarts)

    def _schedule_restart(self, query_id, reason, **context):
        # Queue restart instead of immediate execution
        self._restart_queue.put((query_id, reason, context))

    def _process_restarts(self):
        # Process restart queue with concurrency control
        while True:
            query_id, reason, context = self._restart_queue.get()
            with self._restart_semaphore:
                self._execute_restart(query_id, reason, context)
```

---

### Cascading Restarts

**Strengths:**
- ✅ Handles multi-level dependencies
- ✅ Reverse order (consumers first) is correct for streaming

**Concerns:**
- ⚠️ Graph traversal may be expensive for large DAGs
- ⚠️ No cycle detection (could infinite loop)
- ⚠️ All-or-nothing restart (no partial restart option)

**Recommendations:**
- ✅ Add cycle detection (use NetworkX's `is_directed_acyclic_graph`)
- ✅ Support depth limit for cascading (don't restart entire graph)
- ✅ Add dry-run mode (preview affected queries without restarting)

---

## Integration & Cross-Cutting Concerns

### Signal Consistency

**Issue:** Same events emitted from multiple places with different names.

**Example:**
- `datapipes.before_run` vs `execution.before_generation` vs `streaming.before_start`

**Recommendation:**
- ✅ Establish namespace hierarchy: `{component}.{action}.{phase}`
- ✅ Document naming conventions in [signal_quick_reference.md](../signal_quick_reference.md)
- ✅ Validate consistency in tests

### Performance Impact

**Signals:**
- 🟢 Low overhead (blinker is fast)
- 🟡 Many subscribers can add latency
- ✅ Recommendation: Keep critical path signals minimal

**DAG Construction:**
- 🟡 Graph build is O(pipes * dependencies)
- 🟡 Topological sort is O(V + E)
- ✅ Recommendation: Cache execution plans

**Dimension Polling:**
- 🟠 Checking N dimensions every 60s can be expensive
- ✅ Recommendation: Batch checks, increase interval

### Error Handling

**Current Gaps:**
- ⚠️ No central error handling strategy
- ⚠️ Async handlers silently fail
- ⚠️ Recovery exhaustion not clearly handled

**Recommendations:**
- ✅ Add `ErrorHandler` interface for pluggable error handling
- ✅ Default handler logs errors and emits signals
- ✅ Support custom handlers for alerts, retries, etc.

### Testing Strategy

**Required Test Coverage:**

| Component | Unit Tests | Integration Tests | System Tests |
|-----------|-----------|-------------------|--------------|
| Signals | ✅ High | ✅ Medium | 🟡 Low |
| DAG Execution | ✅ High | ✅ High | ✅ High |
| Streaming | ✅ High | ✅ High | ✅ Critical |
| Dimension Restart | ✅ Medium | ✅ High | ✅ Critical |

**Specific Tests Needed:**
1. Signal ordering (before/after sequences)
2. DAG cycle detection
3. Parallel execution safety (thread-safety)
4. Streaming restart race conditions
5. Dimension change during restart
6. Checkpoint recovery after schema change

---

## Risk Assessment

### High Risks 🔴

1. **Parallel Execution Thread Safety**
   - **Issue:** Spark DataFrames not thread-safe
   - **Mitigation:** Remove thread-based parallelism or scope to non-Spark operations
   - **Status:** ⚠️ Requires design change

2. **StreamingQueryListener Blocking**
   - **Issue:** Listener callbacks must not block
   - **Mitigation:** Queue-based event processing
   - **Status:** ⚠️ Requires implementation change

3. **Checkpoint Compatibility After Restart**
   - **Issue:** Dimension schema changes may invalidate checkpoints
   - **Mitigation:** Validate checkpoints, offer clean start option
   - **Status:** ⚠️ Needs additional design

### Medium Risks 🟡

4. **NetworkX Dependency**
   - **Issue:** Not pre-installed on all platforms
   - **Mitigation:** Fallback to graphlib (sequential only)
   - **Status:** ✅ Acceptable tradeoff

5. **Restart Storms**
   - **Issue:** Many dimensions changing → many restarts
   - **Mitigation:** Restart throttling, queuing
   - **Status:** ⚠️ Needs implementation

6. **Event Loop Management**
   - **Issue:** Spark doesn't provide default event loop
   - **Mitigation:** Make async support optional, document requirements
   - **Status:** ✅ Addressed in recommendations

### Low Risks 🟢

7. **Signal Overhead**
   - **Issue:** Many signals may impact performance
   - **Mitigation:** Signals are opt-in via subscription
   - **Status:** ✅ Not a concern

8. **Configuration Complexity**
   - **Issue:** Many config options to manage
   - **Mitigation:** Sensible defaults, good documentation
   - **Status:** ✅ Manageable

---

## Recommendations Summary

### Immediate Actions (Before Implementation)

1. ⚠️ **Remove thread-based parallel execution** or clarify it's for planning only
2. ⚠️ **Refactor StreamingQueryListener** to use queue-based processing
3. ⚠️ **Add restart throttling** to StreamRestartController
4. ⚠️ **Add checkpoint validation** before stream restarts
5. ✅ **Document async signal requirements** clearly

### Phase 1: Signal Enhancements (Proceed)

- ✅ Implement Option C (Hybrid approach)
- ✅ Start with simple emit pattern for all signals
- ✅ Document migration path to typed signals
- ✅ Skip async support for now (Phase 3)

### Phase 2: DAG Execution (Proceed with modifications)

- ✅ Implement strategy pattern as proposed
- ⚠️ Remove/clarify parallel execution within generations
- ✅ Keep cache recommendations advisory (not automatic)
- ✅ Add execution plan caching

### Phase 3: Streaming Support (Proceed with refinements)

- ✅ Implement comprehensive manager approach
- ⚠️ Add queue-based listener processing
- ⚠️ Add restart throttling and coordination
- ✅ Integrate with existing `SimplePipeStreamOrchestrator`

### Phase 4: Dimension Restarts (Defer or refine)

- ⚠️ **Consider deferring to Phase 4** (not critical path)
- ✅ Add batch version checking
- ✅ Add checkpoint validation
- ✅ Document alternative approaches (streaming dimensions, scheduled restarts)
- ✅ Add dry-run mode for testing

---

## Revised Timeline Estimate

| Phase | Original | Revised | Change Reason |
|-------|----------|---------|---------------|
| Signal Enhancements | 2-3 weeks | 2-3 weeks | ✅ No change |
| DAG Execution | 3-4 weeks | 4-5 weeks | Modified parallel execution design |
| Streaming Support | 4-5 weeks | 5-6 weeks | Queue-based listener, restart coordination |
| Dimension Restarts | (included) | 3-4 weeks | New phase, additional features |
| Integration & Polish | 2-3 weeks | 3-4 weeks | More testing needed |
| **Total** | **11-15 weeks** | **17-22 weeks** | More realistic for production quality |

---

## Final Verdict

### Overall Assessment: ✅ **APPROVE WITH MODIFICATIONS**

The proposals demonstrate excellent architectural thinking and address real pain points in data engineering workflows. Key strengths:

- ✅ Signal-based architecture enables loose coupling
- ✅ DAG execution is a major improvement over sequential
- ✅ Streaming support fills a critical gap
- ✅ Dimension restart solves a real problem

**However, critical issues must be addressed:**

1. Thread-safety concerns with parallel execution
2. StreamingQueryListener blocking concerns
3. Checkpoint compatibility after restarts
4. Overall complexity and timeline

**Recommended Approach:**

1. **Proceed with Phases 1-2** (Signals + DAG) - Lower risk, high value
2. **Refine Phase 3** (Streaming) - Address listener and restart concerns
3. **Consider Phase 4** (Dimension restarts) - High value but can be deferred

The proposals are well-thought-out and technically sound with the modifications noted. Implementing these features will significantly enhance Kindling's capabilities and competitive positioning in the data lakehouse orchestration space.

---

## Appendix: Alternative Considerations

### Alternative to Dimension Polling: Delta Change Data Feed

Instead of polling version numbers:

```python
# Subscribe to Delta CDF for dimension changes
dim_changes = spark.readStream \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", last_version) \
    .load("/data/silver/customers")

# Trigger restart on any change
dim_changes.writeStream \
    .foreachBatch(lambda batch, id: restart_controller.request_restart("my_query")) \
    .start()
```

**Pros:** Real-time, no polling overhead, leverages Spark's streaming
**Cons:** Requires separate streaming query per dimension, more complex

**Recommendation:** Offer both approaches, let users choose.

---

### Alternative to Parallel Execution: Process Pool

For true parallelism without thread-safety issues:

```python
from multiprocessing import Pool

def execute_pipe_in_process(pipe_id):
    # Each process gets its own Spark session
    spark = SparkSession.builder.getOrCreate()
    # Execute pipe...

with Pool(processes=4) as pool:
    pool.map(execute_pipe_in_process, generation)
```

**Pros:** True parallelism, no thread-safety issues
**Cons:** Overhead of spawning processes, serialization costs

**Recommendation:** Document as advanced option, not default.
