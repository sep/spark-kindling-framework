# Multiple Driving Inputs (and Collector Pipes)

**Status:** Proposed. No framework or application code changes made yet.
**Created:** 2026-09-03
**Related:** `fan_in_upsert_pipes.md` (the *keyed* fan-in flavor — different
merge semantics, different engine support; see "Relationship to keyed
fan-in"), `declarative_pipelines_engine.md` (its "Multiple flows into one
target" open question), `event_condition_episode_ontology.md` /
`temporal_event_segmentation.md` (the temporal events table is the first
consumer).

## Problem

A pipe may read many entities, but only **one** of them can be read
incrementally: input 0. Everything else is reference data, read in full.
The convention is stated in
`packages/kindling/watermarking.py:522-528`:

> Driving-source convention: a pipe operates on a single source of
> truth — its FIRST input entity — and every other input is reference
> data, read in full. Only the driving source is watermarked. A table fed
> by multiple sources is built by multiple pipes each contributing its
> own driving source, not by one pipe with several watermarked inputs.

That rule buys real simplicity and should stay the default. But it makes
one common shape unbuildable: **many sources appending into one table.**
Neither available lowering works everywhere:

| Lowering | Incrementality | Declarative engines |
|---|---|---|
| N pipes, each with its own driving source, all writing the target | works — this is the convention's blessed shape | rejected: `duplicate_output_entity` |
| One pipe reading all N sources, writing the target | only input 0 is incremental | fine — single writer |

Temporal base events are exactly this shape, and hit exactly this wall.
`kindling_ext_temporal.chain.declare_temporal_chain` raises when base
events read from more than one entity
(`packages/extensions/kindling_ext_temporal/kindling_ext_temporal/chain.py:396-407`),
gated behind an engine opt-in flag
(`supports_multi_source_temporal_chain`) that exists solely because
Databricks/Lakeflow re-derives base-event wiring itself and never runs the
chain pipe's Python body. The flag is a workaround for a missing framework
primitive, and it has to be plumbed to every engine and every
initialization path that wants the shape.

## Recommendation

**Add one optional pipe field — `driving_entity_ids` — and let the
exceptional case declare itself. Everything else stays exactly as it is.**

1. `PipeMetadata` gains `driving_entity_ids: Optional[List[str]]`. When
   omitted it resolves to `[input_entity_ids[0]]`, which is byte-for-byte
   today's behavior for every existing pipe.
2. `input_entity_ids` keeps its current meaning: **all** inputs, in order.
   It is not narrowed (see "Why not redefine `input_entity_ids`").
3. The watermark aspect tracks a cursor **per driving input** instead of
   one per pipe. The durable store already supports this — `get_cursor` /
   `save_cursor` are keyed `(source_entity_id, reader_id)`.
4. The skip rule becomes "skip when *every* driving read is empty" rather
   than "skip when input 0 is empty".
5. The streaming starter uses the same field: driving inputs are read as
   streams and unioned; the rest stay static reads.
6. `DataPipes.collector(...)` is optional sugar over (1): a pipe whose
   body unions its driving inputs. A **collector is not a new species of
   pipe or entity** — it is a pipe with more than one driving input.
7. `kindling_ext_temporal` becomes the first consumer: the chain-events
   body unions per-source envelopes, and
   `supports_multi_source_temporal_chain` is deleted rather than plumbed
   to more engines.

Non-goals, explicitly:

- **Multi-driving *joins*.** The convention's real value is avoiding
  alignment questions when two incrementally-read sources feed a join
  (the fact arrives, its dimension row hasn't). This proposal covers
  additive/union shapes only. A pipe declaring several driving inputs and
  then joining them is legal Python and out of contract — see "Open
  questions" on whether to detect it.
- **Keyed fan-in upserts.** Column-ownership merges stay in
  `fan_in_upsert_pipes.md`.
- **Changing `input_entity_ids` semantics.**

## Evidence: current state

### The rule is applied in eight places, and written twice

| What | Where |
|---|---|
| Watermark decision (sequential executer) | `packages/kindling/data_pipes.py:731-734` — `pipe.use_watermark and is_first` |
| Watermark decision (DAG executer) | `packages/kindling/generation_executor.py:1196-1201` — same rule, duplicated |
| Skip decision (sequential) | `packages/kindling/data_pipes.py:708` — `list(input_entities.values())[0]` |
| Skip decision (DAG) | `packages/kindling/generation_executor.py:1207-1209` — duplicated |
| Stale-capture clearing | `packages/kindling/watermarking.py:593-595` — only input 0's non-watermarked read clears the capture |
| Cursor capture | `packages/kindling/watermarking.py:554` — `_pending: Dict[str, Tuple[str, str]]`, one `(source, cursor)` per pipe |
| Cursor save | `packages/kindling/watermarking.py:618-623` — pops one capture, saves one cursor |
| Streaming read | `packages/kindling/pipe_streaming.py:49-75` — input 0 via `read_entity_as_stream`, inputs 1..N via batch `read_entity` |
| Persist attribution | `packages/kindling/simple_read_persist_strategy.py:196-201` — input 0 becomes `source_entity_id` on the persist span/signal |

Two observations that shrink the work:

- The **persist signal contract does not need to change.**
  `_on_after_persist` ignores the signal's `source_entity_id` and uses its
  own captured value (`watermarking.py:618-623`), so the attribution site
  is cosmetic.
- The driving-source rule is **duplicated across both executers**. Any fix
  lands in both; they should share one helper rather than drift further.

### The durable cursor store is already per-source

`packages/kindling/watermarking.py:113-130`:

```python
def get_cursor(self, source_entity_id: str, reader_id: str) -> Optional[str]:
def save_cursor(self, source_entity_id, reader_id, cursor, last_execution_id):
```

Per-source-per-reader cursors are already the persistence model — the
class docstring says so directly ("exactly one cursor per (source,
reader)"). Only the in-flight bookkeeping collapses it to one per pipe.
No storage, schema, or migration work is implied by this proposal.

### `duplicate_output_entity` counts registered pipes, not emitted flows

`packages/extensions/kindling_ext_sdp/kindling_ext_sdp/declaration_engine.py:199-213`
builds `producers` by iterating registered pipe definitions. A single
pipe that a lowering later expands into N `append_flow`s is one producer
and never trips the check — which is why
`kindling_ext_databricks/temporal_lowering.py:249-262` already emits one
append flow per base event into a shared stratum-0 streaming table
without conflict. `fan_in_upsert_pipes.md:191-196` notes this explicitly.

**Consequence:** the single-pipe/multi-driving-input shape needs **no SDP
validation exemption at all.** The `duplicate_output_entity` work in
`fan_in_upsert_pipes.md` (items 3-5) is required only for that proposal's
N-registered-writers shape, not for this one.

### The streaming path repeats the same convention

`packages/kindling/pipe_streaming.py:49-51`:

```
# Convention:
# - first input entity is streaming input
# - remaining input entities are direct (batch/static) reads for joins/lookups
```

So "go streaming" does not sidestep this: the identical restriction is
hard-coded a second time, in a second module. One declared field fixes
both.

### The temporal extension already hand-rolls the derived case

`chain.py:396` derives its driving-entity set by scanning
`TemporalEventRegistry` rather than asking the author to list sources, and
`collapse_temporal_chain` (`chain.py:514-542`) fuses N registered
declarations into one composite pipe precisely because the target needs a
single writer. Both moves are this proposal, implemented once, privately,
inside one extension.

## Design contract

### Declaration

```python
DataPipes.pipe(
    pipeid="silver.events.collect",
    input_entity_ids=["bronze.telemetry", "bronze.twin_change", "ref.devices"],
    driving_entity_ids=["bronze.telemetry", "bronze.twin_change"],  # optional
    output_entity_id="silver.events",
    use_watermark=True,
)
```

- **Omitted** → `[input_entity_ids[0]]`. Identical to today.
- **Validated at registration**: every id in `driving_entity_ids` must
  appear in `input_entity_ids`; the list must be non-empty when present.
- `use_watermark` remains the master switch. `run_datapipes(...,
  no_watermark=True)` (`data_pipes.py:449-492`) keeps working unchanged —
  it disables all driving reads, not just input 0's.
- The kwargs contract is unchanged: **all** inputs are passed to the pipe
  body, keyed `entity_id.replace(".", "_")`. Driving vs reference affects
  only how each frame was read.

### Why name the driving set, not the reference set

Naming references and inferring "the rest are driving" means adding an
input later silently makes it incremental. A reference table read
incrementally hands a join only its *new* rows — a quiet correctness bug.
The opposite mistake (a driving input defaulting to reference) is a full
read: slower, and loud in run times. Default toward the loud failure.

Naming also removes today's positional fragility: reordering
`input_entity_ids` currently changes which source is watermarked, with
nothing to catch it.

### Why not redefine `input_entity_ids`

It is load-bearing well outside execution:

- `pipe_graph.py:220-240` builds dependency edges from it (graph topology,
  cycle detection, generation ordering).
- `declaration_engine.py:216-235` derives internal-vs-external input
  classification from it for declarative lowering.
- Pipe bodies unpack kwargs keyed from it.
- `kindling_cli` reads it in six places for pipeline inspection.

Narrowing it to "driving only" would silently change graph topology and
declaration classification. Adding a subset marker touches only the
watermark decision — the actual scope of the change.

### Execution semantics

1. **Read**: a driving input is read with `use_watermark=True`; every
   other input is read in full. Both executers.
2. **Skip**: the pipe is skipped when *all* driving reads returned no
   data. Reference-input unavailability keeps its current behavior
   (`datapipes.pipe_skipped` / `orchestrator.pipe_skipped`, which already
   discard captures).
3. **Capture**: `_pending[pipe_id]` becomes `Dict[source_entity_id,
   cursor]`. The "replacing an existing capture" warning
   (`watermarking.py:599-612`) applies per source.
4. **Advance**: `_on_after_persist` pops the map and calls `save_cursor`
   once per entry. All-or-nothing per run: either the output persisted and
   every contributing source advances, or nothing does.
5. **Discard**: persist failure / pipe failure / skip clears the whole
   map, as today.
6. **Stale-clear**: `watermarking.py:594`'s `entity.entityid ==
   input_ids[0]` becomes membership in the driving set.

At-least-once semantics are unchanged, so a replay re-reads a driving
slice. Collectors are therefore expected to be idempotent at the target —
either an append target that tolerates duplicates, or a dedupe key (the
temporal events table dedupes on `event_id`).

### Streaming lowering

`SimplePipeStreamStarter` reads each driving input via
`read_entity_as_stream` and unions them (schema-uniform by precondition);
references stay `read_entity`. One checkpoint per pipe, as today
(`{checkpoint_root}/{pipeid}`), with Spark tracking per-source offsets
inside it.

Operational note worth documenting rather than solving here: a unioned
streaming query advances at the pace of its slowest source (Spark's global
event-time watermark is the minimum across sources), and adding a source
later changes the query's source list, which a checkpoint cannot absorb
cleanly. Where per-source independence matters more than a single query,
the `flows` shape (N contributor pipes, engine-native) is the better
lowering — see "Open questions".

### Collector sugar

```python
@DataPipes.collector(
    pipeid="silver.events.collect",
    input_entity_ids=[...],          # all driving by default
    output_entity_id="silver.events",
)
def collect_events(**frames): ...    # body optional; default is unionByName
```

`collector` sets `driving_entity_ids = input_entity_ids` (minus any
declared references) and, with no body, supplies a `unionByName` default.
It emits `pipe_type=collector` for diagnostics. Nothing about it is a new
execution path.

### Relationship to keyed fan-in

`fan_in_upsert_pipes.md` covers the other flavor: several **independently
registered** pipes each owning disjoint non-key columns of one keyed
target. That shape needs owned-column declarations, explicit-column
merges, `pipe_graph` multi-producer tracking, an entity-level opt-in, and
a positive declarative-engine refusal — none of which this proposal needs,
because one pipe writes the target.

Suggested split, so the two stay coherent:

| | This proposal | Keyed fan-in |
|---|---|---|
| Writers of the target | one pipe | N registered pipes |
| Rows | additive, schema-uniform | keyed, column-subset merge |
| Needs owned columns | no | yes |
| Needs `duplicate_output_entity` exemption | no | yes |
| Declarative engines | supported (single writer; may expand to `append_flow`s) | must refuse |
| Entity-level opt-in | none | `write.fan_in` (or equivalent) |

If the derived/synthesized collector in "Open questions" is ever built,
it produces N registered contributors and *does* need the exemption — at
which point the two proposals converge on one entity-level opt-in.

## Implementation plan

**Phase 1 — the primitive.**
1. `driving_entity_ids` on `PipeMetadata` + `_build_metadata` validation
   (`data_pipes.py:378-418`).
2. One shared helper resolving `(pipe, entity_id) -> is_driving`, used by
   both executers, replacing the two `is_first` sites.
3. Skip rule → "all driving reads empty", both executers.
4. `_pending` becomes per-source; `_on_after_persist` loops; stale-clear
   uses driving membership.
5. Docstring updates where the convention is stated
   (`watermarking.py:522-528`, `data_pipes.py:724-730`,
   `simple_read_persist_strategy.py:196-200`).

**Phase 2 — streaming parity.** `pipe_streaming.py` reads driving inputs
as streams and unions them.

**Phase 3 — temporal consumer.** Chain-events body unions per-source
envelopes (group `base_defs` by `input_entity_id`); delete
`_multi_source_chain_events_unsupported`, the guard at `chain.py:396-407`,
`MULTI_SOURCE_ENGINE_CONFIG_KEY`, the `initialize()` plumbing
(`kindling/__init__.py:80-82`), and the flag on
`DatabricksSdpEngineExtension`.

**Phase 4 (optional) — collector sugar.** `DataPipes.collector`.

Phases 1-3 are independently shippable; phase 3 is the one that removes
existing code.

## Tests

- Registration: unknown id in `driving_entity_ids` rejected; empty list
  rejected; omitted → `[input_entity_ids[0]]`.
- Sequential and DAG executers, parameterized over the same cases: one
  driving input (regression), two driving inputs both with data, two with
  one empty (not skipped), all empty (skipped), reference input never
  watermarked.
- Aspect: two driving sources advance two cursors on persist; persist
  failure advances neither; a mid-run failure after one read leaves both
  cursors unmoved; replay re-reads and dedupes.
- Full-refresh (`no_watermark=True`) reads every driving input in full and
  clears every capture.
- Streaming: driving inputs unioned, references static.
- Temporal: the existing multi-source tests
  (`tests/unit/test_temporal_chain.py:251-400`) invert — multi-source now
  succeeds with no engine flag; add an execution test that a two-source
  chain produces envelopes from both.
- SDP: a two-driving-input pipe still declares cleanly (one producer, no
  `duplicate_output_entity`).

## Open questions

1. **Strict vs lenient cursor advance.** Phase 1 specifies all-or-nothing
   (one persist, all cursors advance together). A lenient variant —
   advance only the sources whose rows actually landed — is what would let
   one broken source not hold back the others, but it needs the persist
   path to report which sources contributed. Defer, or design now?
2. **Detecting out-of-contract joins.** Nothing stops a body from joining
   two driving inputs. Options: document only; require an explicit
   `driving_semantics="union"`; or attempt static detection (not
   advisable). Recommend document-only for phase 1.
3. **Derived collectors in core.** Should core synthesize a collector from
   registered contributors (generalizing `collapse_temporal_chain`), or
   should that stay per-extension? Building it in core is what pulls in
   the entity-level opt-in and the `duplicate_output_entity` exemption,
   i.e. converges with `fan_in_upsert_pipes.md`.
4. **`flows` vs `fused` on declarative engines.** For a single collector
   pipe, should the Databricks lowering expand it into one `append_flow`
   per driving input (independent checkpoints, add a source without
   disturbing others) or one unioned streaming read? The former matches
   what `temporal_lowering` already does and is likely right, but it is a
   lowering choice this proposal does not settle.
5. **Naming.** `driving_entity_ids` matches the existing `*_entity_ids`
   convention and the codebase's own "driving source" prose. Alternatives
   considered: `driving_inputs` (shorter, breaks the suffix pattern),
   `reference_entity_ids` (inverted default — rejected above).

## References

- `packages/kindling/watermarking.py:113-130` — per-(source, reader) cursor API
- `packages/kindling/watermarking.py:522-528` — the convention, stated
- `packages/kindling/watermarking.py:554`, `593-595`, `618-623` — capture, clear, advance
- `packages/kindling/data_pipes.py:708`, `719-736` — sequential executer
- `packages/kindling/generation_executor.py:1193-1215` — DAG executer
- `packages/kindling/pipe_streaming.py:49-75` — streaming convention
- `packages/kindling/simple_read_persist_strategy.py:196-201` — persist attribution
- `packages/extensions/kindling_ext_sdp/kindling_ext_sdp/declaration_engine.py:199-213`, `242-271` — producer map, `duplicate_output_entity`
- `packages/extensions/kindling_ext_databricks/kindling_ext_databricks/temporal_lowering.py:249-262` — append-flow-per-base-event, working today
- `packages/extensions/kindling_ext_temporal/kindling_ext_temporal/chain.py:396-407`, `514-542` — the guard, and the fuse it guards
- `docs/proposals/fan_in_upsert_pipes.md` — keyed fan-in flavor
- `docs/proposals/declarative_pipelines_engine.md` — "Multiple flows into one target"
