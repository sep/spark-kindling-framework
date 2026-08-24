# Keyed Fan-In Upserts Into One Entity

**Status:** Proposed. No framework or application code changes made yet;
this is the design-decision record requested before touching either.
**Created:** 2026-08-20
**Related:** `declarative_pipelines_engine.md` (names this exact gap as an
open question — "Multiple flows into one target"), `temporal_event_segmentation.md`
(the "mirror pipe" pattern it references is a different shape — see
"Prior Art" below), PR history around `entity_provider_delta.py`'s SCD1
merge strategy and `pipe_graph.py`'s producer tracking.

## Problem

Several focused curation pipes need to contribute independently to one
Gold entity keyed by a business key. The example used throughout this
document is illustrative, not a real app's schema — substitute your own
entity/column names when applying this pattern. As a stand-in: a
`gold.item_summary` entity keyed by `item_id`, with:

- base item attributes from `gold.item_registry`
- usage metrics from item-windowed `silver.item_metrics`
- fault counts from item-windowed `gold.item_faults`
- link-drop counts from item-windowed `silver.item_link_state`

Each pipe owns a distinct, non-overlapping group of columns on the same
row. This must be a genuine fan-in of upserts. It explicitly must not be:

- one monolithic pipe with all joins and aggregations (defeats the point
  of decomposing ownership by source and cadence);
- intermediate per-attribute-group contribution entities plus a final
  assembly join (adds a join and a table nobody queries directly, and
  reintroduces exactly the coupling the decomposition is meant to avoid);
- multiple full-row replacements that null fields owned by other flows
  (data loss on every producer's run).

## Recommendation

**This is not supported today, and building it is worthwhile, but it must
be built as a Delta/memory-provider feature, not a Lakeflow one.** Lakeflow
Declarative Pipelines has no native primitive with the right semantics
*and* the right cardinality for this pattern (see "Lakeflow Limitation"
below) — so the design keeps fan-in/patch-upsert entities entirely inside
Kindling's classic execution engine and explicitly excludes them from
`kindling_ext_sdp` declaration, rather than trying to coax Lakeflow into
doing something its own docs say it doesn't do.

Concretely:

1. Add a declared **owned-columns** concept per pipe and a **fan-in**
   opt-in per entity — replacing reliance on Delta's current emergent
   (undeclared, provider-inconsistent) column-subset merge behavior.
2. Make the merge explicit-column (`whenMatchedUpdate`/`whenNotMatchedInsert`
   with concrete column dicts) in both the Delta and memory providers,
   closing a real divergence between them.
3. Fix `pipe_graph.py` to track and depend on *every* producer of an
   entity, not just the last one registered.
4. Keep `duplicate_output_entity` as the default guard for ordinary
   full-row writers; gate the exception behind the entity's explicit
   `fan_in` flag.
5. Have `kindling_ext_sdp` positively refuse to lower `fan_in` entities,
   with a dedicated diagnostic — fail closed, not silently wrong.

## Evidence: current state

### Core framework has no owned-column concept

- `EntityMetadata` (`packages/kindling/data_entities.py:225-244`) has
  `merge_columns` (join keys only) and a single `schema` — nothing
  expresses which pipe owns which non-key column.
- `PipeMetadata` (`packages/kindling/data_pipes.py:27-36`) has one scalar
  `output_entity_id` — no owned-column list, no fan-in mode.

### Delta's merge already does column-subset semantics — by accident

The default SCD1 merge strategy
(`packages/kindling/entity_provider_delta.py:119-141`):

```python
builder = delta_table.alias("old").merge(source=df.alias("new"), condition=merge_condition)
if hasattr(builder, "withSchemaEvolution"):
    (
        builder.withSchemaEvolution()
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return
# Delta < 3.2 compatibility path falls back to
# spark.databricks.delta.schema.autoMerge.enabled, same effective behavior.
```

Verified empirically against a real local Delta table: with a target
already holding `key, colA, colB` and a source carrying only `key, colB`,
the matched row keeps `colA` untouched and updates `colB`; an unmatched
row is inserted with `colA` NULL. This is exactly the desired semantics —
but it is an emergent property of `whenMatchedUpdateAll()`/schema
evolution, not a feature Kindling declares, validates, or documents.
Nothing prevents a future change to this merge strategy (or to Delta's
own defaults) from silently breaking it.

### The memory provider does the opposite

`packages/kindling/entity_provider_memory.py:30-38`:

```python
def _memory_scd1_merge(current_df, incoming_df, entity_metadata):
    """Full-row upsert: incoming rows replace matching business keys, others pass through."""
    business_keys = entity_metadata.merge_columns
    untouched = current_df.join(incoming_df, on=business_keys, how="left_anti")
    return untouched.unionByName(incoming_df, allowMissingColumns=True)
```

On a key match, the *entire* existing row is dropped (`left_anti`) and
replaced by the incoming row; any column the incoming DataFrame lacks
becomes NULL via `unionByName(allowMissingColumns=True)`. This is a real,
destructive divergence from Delta's behavior — a fan-in pair of pipes
that behaves correctly against Delta would silently corrupt data against
the memory provider (and therefore in unit tests that use it).

### The pipe graph tracks only one producer per entity

`packages/kindling/pipe_graph.py:89,97`:

```python
entity_producers: Dict[str, str] = field(default_factory=dict)
...
def add_node(self, node: PipeNode):
    self.nodes[node.pipe_id] = node
    self.entity_producers[node.output_entity] = node.pipe_id   # last write wins
    ...
```

`_validate_graph()` never checks for two nodes sharing an `output_entity`,
and DAG dependency edges are built only from whichever producer happened
to be registered last — a genuine second producer's downstream consumers
silently miss a dependency edge. There is no core-level rejection of
multiple producers either; the only existing guard against this lives in
the SDP extension (below), so outside of SDP-declared apps this failure
mode is currently silent.

### Prior art and stated intent

- The "driving-source convention"
  (`packages/kindling/data_pipes.py:724-730`, also documented in
  `watermarking.py`): *"a table fed by multiple sources is built by
  multiple pipes, each with its own driving source, never by one pipe
  with several watermarked inputs."* This already anticipates multiple
  pipes contributing to one entity as an architectural pattern — it just
  has never been paired with column ownership, graph correctness, or
  provider parity, and today relies on Delta's emergent merge behavior
  above.
- `declarative_pipelines_engine.md`'s open questions section names this
  precisely: *"Multiple flows into one target. SDP's `append_flow` allows
  several flows per target; Kindling's model assumes one pipe per output
  entity... Deliberate mismatch to resolve."*
- `temporal_event_segmentation.md`'s "mirror pipe" pattern (referenced by
  the open question above as a hoped-for future consumer of append-flow
  fan-in) is a *different* shape: one entity written normally, plus one
  decoupled downstream pipe that reads it and republishes filtered rows
  to an external system. It does not involve multiple producers into one
  shared entity, and doesn't inform the merge-semantics problem here.

### `kindling_ext_sdp` rejects this today, on purpose

`packages/extensions/kindling_ext_sdp/kindling_ext_sdp/declaration_engine.py:254-271`:

```python
# One pipe per output entity: SDP's append_flow allows several flows
# per target, but Kindling's model assumes one pipe per output (the
# proposal's "Multiple flows into one target" open question). Fail
# fast rather than silently declare a conflicting graph.
producer = producers.get(pipe.output_entity_id)
if producer is not None and producer != pipe.pipeid:
    issues.append(DeclarationIssue(
        pipe_id=pipe.pipeid,
        code="duplicate_output_entity",
        reason=(
            f"output entity '{pipe.output_entity_id}' is also produced by "
            f"pipe '{producer}'; multiple flows into one target are not "
            "supported (append_flow mapping is a deferred open question)"
        ),
    ))
```

`tests/unit/test_sdp_auto_cdc.py::test_duplicate_scd_writers_fail_once_per_entity_issue`
confirms this is exercised and enforced today. `declaration_plan.py`'s
`DatasetDeclaration` is 1:1 pipe→name with no `Flow`-level concept;
`capabilities.py` declares `EXPECTATIONS`/`AUTO_CDC`/`INCREMENTAL_MV_REFRESH`
but nothing for multi-flow targets. The one place multiple flows into one
target actually work today, `kindling_ext_databricks/temporal_lowering.py`'s
use of `dp.append_flow(target=..., name=...)`, unions schema-uniform
envelopes from a *different* registry (`TemporalEventRegistry` base-event
definitions) into synthetic tables that are never registered
`DataEntities` — it never touches `duplicate_output_entity`, and, per the
platform limitation below, wouldn't solve this problem even if it did.

`kindling_ext_databricks/auto_cdc.py` and `engine.py`'s
`_declare_scd_dataset` only support SCD2 via `create_auto_cdc_flow` /
`create_auto_cdc_from_snapshot_flow`; `track_history_(except_)column_list`
scopes SCD2 history-versioning triggers, not merge write-scope. No
column-ownership or partial-row-merge concept exists anywhere in this
extension.

## Lakeflow limitation (why this must stay off the Lakeflow path)

Verified against current Databricks Lakeflow Declarative Pipelines docs,
not just repo code:

- **`append_flow` is insert-only.** *"The default flow for a streaming
  table is an append flow that adds new rows with each update."* Multiple
  append flows into one target each just add rows — none of them merge
  into an existing row by key. Using `append_flow` for this pattern would
  produce duplicate/orphaned rows per key per flow, not one row with
  columns filled in from multiple sources.
- **CDC/merge flows are restricted to one per target.** Databricks states
  plainly: *"Datasets can be the target of only a single operation in all
  Lakeflow Declarative Pipelines, with the exception being streaming
  tables with append flow processing."* Outside the append-only
  exception above, a Lakeflow-managed target accepts exactly one flow —
  so multiple independent `create_auto_cdc_flow` calls into the same
  target are not a supported configuration, regardless of column scoping.
- `column_list`/`except_column_list` on `create_auto_cdc_flow` project
  which columns land in the target's *overall* schema — they do not
  partition write ownership across multiple co-existing flows, and no
  Databricks documentation addresses multiple auto-CDC flows sharing a
  target at all.

**Net effect: no Lakeflow primitive has both the right semantics (merge
by key) and the right cardinality (N independent flows) for this
pattern.** This is a platform limitation, not just a Kindling gap, and it
means fan-in/patch-upsert entities cannot be lowered into Lakeflow
Declarative Pipelines as declared today. They must run through Kindling's
own Delta/memory execution path — which can still be scheduled inside the
same Databricks job/workflow that runs a Lakeflow pipeline for the rest
of an app, just not expressed as pipeline graph nodes for these specific
pipes.

## Design contract

### Entity and pipe declarations

The names below are sample placeholders, matching the illustrative
`gold.item_summary` example in "Problem" — substitute a real app's own
entity, pipe, and column names.

```python
# --- Entity: opt into fan-in, otherwise unchanged ---
@DataEntities.entity(
    entityid="gold.item_summary",
    name="item_summary",
    merge_columns=["item_id"],
    schema=ITEM_SUMMARY_SCHEMA,
    tags={"entity.fan_in": "true"},   # NEW — required for >1 producer to validate
)
class ItemSummaryEntity:
    pass

# --- Pipe: declares which non-key columns it owns ---
@DataPipes.pipe(
    pipeid="curate_item_metrics",
    name="Item usage metrics",
    input_entity_ids=["silver.item_metrics", "gold.item_registry"],
    output_entity_id="gold.item_summary",
    output_type="patch_upsert",                              # NEW mode
    output_owned_columns=["avg_metric_a", "avg_metric_b"],   # NEW, pairwise-disjoint across producers
    use_watermark=True,
)
def execute(sources, entity_writer):
    result = ...  # item_id + only the owned columns
    entity_writer.patch_upsert(result)   # NEW writer method
```

Each of the four curation pipes for `gold.item_summary` is declared this
way, each with a disjoint `output_owned_columns` list, reading only its
own source(s) — no intermediate entities, no assembly pipe.

### Merge behavior

- **Matched:** explicit `whenMatchedUpdate(set={c: source[c] for c in owned_columns})`
  — never `updateAll()`. Correctness stops depending on Delta's
  schema-evolution behavior as an implementation detail.
- **Unmatched:** explicit `whenNotMatchedInsert(values={...})` — insert a
  full row with this pipe's owned columns populated and every other
  pipe's owned columns explicitly NULL.
- **Memory provider:** same contract — per-column coalesce on match
  (keep target's non-owned columns, take source's owned columns), NULL-fill
  other-owned columns on insert — replacing `_memory_scd1_merge`'s
  destructive full-row replace for this mode.

### Conflict detection

At registration, for any `entity.fan_in=true` entity: collect
`output_owned_columns` across all producing pipes and require pairwise-disjoint
sets (new validation code, e.g. `overlapping_owned_columns`); require
every non-key schema column is claimed by exactly one pipe
(`unclaimed_owned_column`). `pipe_graph.py`'s `entity_producers` becomes
`Dict[str, List[str]]` so DAG edges are built from every producer, not
just the last one registered.

### Ordering, concurrency, idempotency

- **Ordering:** none required or assumed between different pipes' merges
  into the same entity — column-disjointness makes writes commutative.
  Document this as a system invariant rather than leaving it implicit.
- **Concurrency:** Delta's optimistic concurrency control can raise a
  spurious conflict when two merges touch overlapping files even though
  their columns are disjoint (conflict detection is file/row-level, not
  column-level). Wrap each merge in bounded retry-with-backoff rather than
  serializing producers, which would defeat the point of independent
  pipes.
- **Idempotency:** unchanged from today's merge-by-key behavior — rerunning
  a pipe's merge with the same source batch reproduces the same
  owned-column values.

### Validation and parity

- Keep `duplicate_output_entity` as the default for any entity without
  `entity.fan_in=true` — this remains the correct guard against
  accidental multi-producer conflicts for the overwhelming majority of
  entities (ordinary full-row writers).
- `kindling_ext_sdp` additionally and positively excludes `fan_in`
  entities from declaration (new `fan_in_entity_not_declarable` rule,
  mirroring the existing `output_entity_not_table_backed` rule) — fail
  closed with an actionable diagnostic rather than emitting the
  now-inapplicable `duplicate_output_entity` message or silently
  mis-lowering into wrong-semantics flows.
- Delta and memory providers must both implement the explicit-column
  contract — closing a latent bug that exists today independent of this
  feature (any single-pipe entity that writes a narrower DataFrame than
  its declared schema already behaves differently on Delta vs. memory).
  Lakeflow parity is explicitly *not* extended to fan-in entities; this
  asymmetry is deliberate and platform-driven, not an oversight.

## Implementation plan

**Framework**

1. `data_pipes.py`: add `output_owned_columns`, `output_type="patch_upsert"`
   to `PipeMetadata`.
2. `data_entities.py`: add the `entity.fan_in` flag.
3. Core registration validation: `overlapping_owned_columns` /
   `unclaimed_owned_column` checks; keep the `duplicate_output_entity`-equivalent
   rejection for non-fan-in entities.
4. `pipe_graph.py`: multi-valued `entity_producers`; fix DAG edge
   construction to depend on every producer.
5. `entity_provider_delta.py`: explicit-column merge strategy for
   `patch_upsert` mode.
6. `entity_provider_memory.py`: matching explicit-column merge behavior.
7. `kindling_ext_sdp/declaration_engine.py`: new `fan_in_entity_not_declarable`
   rule.

**Application** (only after the above lands; substitute the app's own
entity/pipe names for the `gold.item_summary` placeholder used here)

8. Declare the target entity with `entity.fan_in=true`.
9. Declare each contributing curation pipe with a disjoint
   `output_owned_columns` list and `output_type="patch_upsert"` — no
   intermediate entities, no assembly pipe.

## Tests

**Framework**

- Delta: two synthetic pipes with disjoint columns converge correctly
  regardless of execution order.
- Memory: same test, currently failing until the provider fix lands,
  then matching Delta exactly (shared parametrized fixture across both
  providers, so future parity is guaranteed by construction rather than
  by discipline).
- Overlapping owned columns → hard validation error; disjoint → succeeds.
- Non-fan-in entity with two producers → `duplicate_output_entity` still
  fires exactly as today (protects the default path).
- `pipe_graph.py` builds dependency edges from all producers, not just
  the last registered.
- SDP: a `fan_in=true` entity → the new rejection code, not the generic
  `duplicate_output_entity` message.
- Concurrent merges into disjoint columns on the same Delta table
  converge to the correct final row under the retry wrapper.

**Application graph** (using the illustrative `gold.item_summary` shape —
apply to whatever the real target entity and its contributing pipes are)

- Each contributing pipe populates only its own columns in isolation
  (verified by inspecting the row after each pipe runs alone).
- All contributing pipes run in every permutation order converge to an
  identical final row per key.
- Re-running any single pipe after all pipes have populated a row doesn't
  disturb the columns owned by other pipes.
- A row whose non-base attributes arrive before the base-attributes
  source has a row inserts correctly with base-owned columns NULL, and is
  patched (not duplicated) once the base-attributes pipe runs.

## Open questions

- Exact name/shape for the pipe-level "owned columns" declaration
  (`output_owned_columns` list vs. a richer `PipeOutput` object) — kept
  flexible here pending implementation review.
- Whether `entity.fan_in` should be a tag (consistent with other
  string-tag toggles like `scd.type`) or a first-class `EntityMetadata`
  field — tags are cheaper to add without touching the dataclass, but a
  first-class field is more discoverable and typo-proof.
- Whether the bounded-retry concurrency wrapper belongs in the entity
  provider layer (per-merge) or the execution orchestrator (per-pipe-run)
  — affects where retry telemetry surfaces.

## References

- Databricks — [Use flows in Lakeflow pipelines](https://docs.databricks.com/aws/en/ldp/flow-examples)
  ("the default flow for a streaming table is an append flow that adds
  new rows with each update"; multiple flows into one target is scoped
  to append-flow processing).
- Databricks — [create_auto_cdc_flow reference](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes)
  (`column_list`/`except_column_list` semantics; no multi-flow-per-target
  guidance).
