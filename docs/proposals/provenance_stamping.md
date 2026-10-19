# Provenance Stamping

**Status:** Proposed (revised 2026-10-19: an optional `kindling.provenance.*`
settings block with per-entity tag overrides; a created stamp and an optional
last-changed stamp, mapped onto the insert-only and untracked column classes).
Design findings against the current write paths; no code changes yet.
**Created:** 2026-10-16
**Related:** `governed_artifact_platform.md` (names provenance stamping as the
small Kindling write-path feature its whole story leans on, and asks for it to
be specced against the current write path: this is that spec),
`scd_untracked_columns.md` (tracked and untracked columns; the last-changed
stamp is written like an untracked column), `surrogate_keys.md` (insert-only
columns; the created stamp is one), `runtime_manifest.md` (rows link to a run
record by `run_id`), `fan_in_upsert_pipes.md` (several writers per entity).

Not to be confused with:

- issue #8, a column-level lineage analyzer (static plan analysis, no data
  written);
- "Provenance" in `event_condition_episode_ontology.md` and
  `temporal_event_segmentation.md`, an Event axis (System-Observed vs
  Synthetic);
- the CLI's "tag provenance" (`kindling ... --tags`, `cli.py:747`), which says
  which config layer a tag value came from.

## What this is, and what it is not

This is **operational traceability**: each output row records which pipe, in
which run, created it (and, optionally, last changed it), from which inputs.
The record is data. It lives in the row, is queried with SQL, and is kept as
long as the row is.

It is **not observability**. Traces, spans, metrics and logs
(`kindling.telemetry.*`, `unified_otlp_telemetry_provider.md`) stay where they
are and are not replaced. They describe what a run did, sampled and kept for a
retention window, keyed by time. The stamp describes where a row came from.
The two connect through ids: `run_id` and `execution_id` in the stamp find the
run record and its trace. The stamp emits no telemetry, and turning tracing off
(`kindling.telemetry.tracing.enabled: false`) does not turn stamping off.
That is why its settings live under `kindling.provenance.*`, not under
`kindling.telemetry.*`.

## Problem

An output row does not say which pipe, run or inputs produced it. Two
questions need that:

- **Operations:** which pipe and run created this row, or last changed it,
  under which app, environment and Kindling version?
- **Recovery** (the governed-artifact case): which outputs were built from
  version 12 of a reference entity, so exactly those can be reprocessed?

## Proposal

### Two stamps

| Column (default name) | Answers | Column class | Default |
|---|---|---|---|
| `__provenance` | What created this row (for SCD2: this version)? | **Insert-only** (`surrogate_keys.md`): set once, when the row is inserted; never written by any other merge clause | On when provenance is enabled |
| `__provenance_changed` | What last changed this row? | **Written like an untracked column, never compared** (below) | Off; opt in with `provenance.changed` |

Both columns have the same struct type, so a reader handles one shape:

```
struct<
  pipe_id, pipe_name, run_id, execution_id, persist_id,
  processed_at timestamp,
  mode            -- 'batch' | 'streaming' | 'sdp'
  app_name, environment, platform, kindling_version,
  input_entity_ids array<string>,
  input_versions   map<string,string>,   -- phase 3: entity id -> version read
  tags             map<string,string>,   -- selected pipe tags
  schema_version int
>
```

- Every field is nullable; missing context gives null and never fails a write.
- Fields are only ever added, never renamed or removed.
- `tags` is a map so the schema stays stable when the allow-list changes.
- Runtime detail (Spark, Delta and package versions) stays out of the row:
  `run_id` links to the run record proposed in `runtime_manifest.md`.
- `persist_id` is the id of one batch persist call
  (`simple_read_persist_strategy.py:221`); it is null for streaming and SDP.

**Why two columns, not one struct with `created` and `changed` sub-structs.**
The merges sort *top-level* columns into classes (tracked, untracked,
insert-only). With two columns, each column has exactly one class and the
existing machinery handles it: the insert-only list, the in-place ("touch") set
and the SCD1 update set. One struct would need nested-field assignments in
every MERGE clause (`set={"__provenance.changed": ...}`) and the matching
memory-provider logic, and nested schema evolution, none of which Kindling does
today. Two columns also keep the default case (created only) to one column.

### Configuration

**Recommended: an optional settings block, with per-entity tags as
overrides.**

```yaml
kindling:
  provenance:
    enabled: false                       # default
    column: __provenance
    changed: false                       # also keep the last-changed stamp
    changed_column: __provenance_changed
    pipe_tags: []                        # allow-list; literal names and fnmatch globs
```

```yaml
# on one entity (tags=, dataentities:, dataentities-bytag: or entity_tags)
provenance.enabled: "false"              # opt out (or "true" to opt in)
provenance.column: "_lineage"
provenance.changed: "true"
provenance.changed_column: "_last_changed"
provenance.pipe_tags: "domain,owner,layer.*"
```

The block sits in any settings layer (base, platform, environment, or the
app's own files), so "on in prod, off locally" is one line in an environment
file.

**How the two combine.** The settings block is applied as the
**lowest-precedence tag defaults**, baked into the entity's metadata when it is
built (`DataEntityManager._build_metadata`, `data_entities.py:1212`): after the
`dataentities-bytag:` and `dataentities:` overlays have resolved, each
`provenance.*` key the entity does not already have is filled from the block.
Every layer above therefore wins, in the existing order: declared tags,
`dataentities-bytag:`, `dataentities:`, the exact-id `entity_tags` map, and
run-scoped `tag_overrides` (`get_entity_definition`, `data_entities.py:1396`).
An entity opts out in its own declaration with `provenance.enabled: "false"`.

After that, the tags are the single contract. One parser,
`provenance_config_from_tags(entity)`, next to `scd_config_from_tags` and
`surrogate_config_from_tags` in `data_entities.py`, is what every write path,
provider merge and extension calls. The `kindling.provenance.*` settings keys
and the `provenance.*` tag keys are literals inside that module only: no
exported key constants, and extensions never read the keys themselves (the
maintainer rule).

**Which entities the defaults reach.** The settings block never produces a
combination that registration would reject; it skips entities it does not fit.
Explicit tags are validated as declared.

| Entity | `enabled` default | `changed` default |
|---|---|---|
| Provider `delta` (the default, `entity_provider_registry.py:180`) or `memory` | applied | applied only to keyed merges (SCD1, SCD2) |
| Any other provider (CSV and Event Hub cannot hold a struct column) | skipped; an explicit tag opts in | skipped |
| `read_only`, an SQL entity, an SCD2 current companion (`provider_type: view`), a `system.*` entity | skipped | skipped |
| Append, `write.mode: insert`, derived dataset | applied | skipped (a row never changes after insert, so it would equal the created stamp) |

An explicit `provenance.changed: "true"` on an append, insert-only or derived
entity is rejected at registration, as `scd.untracked` is rejected with
`write.mode: insert` today (`_validate_write_mode_tag`).

Other registration checks, for both columns: not in `merge_columns`,
`partition_columns`, `cluster_columns`, `scd.tracked`, `scd.untracked`,
`scd.sequence_by`, `surrogate.from` or `derived.replace_keys`; the two names
differ; neither starts with `__scd2_` (reserved for the merge's staging
columns). `provenance.pipe_tags` is an allow-list only (`*` is rejected), and
tags that held resolved secrets are always excluded (pipe tags may hold
resolved secrets; see `data_pipes.py:632-683`).

**Alternatives considered.**

- **Overlays only** (the first revision of this proposal): turn it on with
  `dataentities: "**": {tags: {provenance.enabled: "true"}}`. It needs no new
  setting, and it stays available for targeted opt-in (for example
  `dataentities-bytag:` on `layer: gold`). It is not the main switch because
  overlays sit *above* declared tags, so an entity cannot opt out in its own
  declaration (only with a more specific overlay entry); a `**` pattern has no
  eligibility filter, so CSV and Event Hub outputs would be stamped and fail;
  and the column name and tag allow-list have no single default.
- **Resolve the settings at each use site**, the `provider.access_mode` over
  `kindling.delta.access_mode` pattern (`pipe_streaming.py:187-191`) or the
  Cosmos run-level defaults (`entity_provider_cosmos.py:656`). It needs no
  baking, but about eight sites would each apply the same precedence: the
  persist step, the stream starter, the SDP builder, the Databricks AUTO CDC
  lowering, the Delta and memory merges, schema augmentation, drop-on-read and
  validation. A site that read only the tags would let a merge overwrite the
  created stamp. Baking the defaults into tags keeps one contract.

### How the merges treat the stamps

Both stamps are built before the write (below), so every incoming row carries
them. The provider's merge decides which target rows they reach:

- **`__provenance` is insert-only.** It joins the surrogate keys in the
  insert-only set: `scd_change_columns(..., insert_only_columns=...)`, the
  explicit SCD1 update set (`_scd1_update_set`), and the memory merge's
  keep-on-match, all already built for surrogate keys on `feat/surrogate-keys`
  (`entity_provider_delta.py`, `data_entities.py`). The one difference: a
  surrogate key is minted inside the SCD2 merge and incoming key columns are
  dropped, whereas the created stamp is *supplied by the batch* and must reach
  the insert clause unchanged. The SCD2 insert values already take every staged
  column, so the change is only to not drop it.
- **`__provenance_changed` is written whenever its row is written, but never
  compared.** It goes into the SCD2 touch set and the SCD1 update set like an
  untracked column, but it is left out of *both* change tests (tracked and
  untracked). Its value differs on every run, so if it were compared every
  current row would be touched, or rewritten, on every load, and "last changed"
  would degrade to "last seen". This is a small addition to the column classes
  in `scd_untracked_columns.md`: an untracked column that never counts as a
  change by itself.
- **Neither is touched by a close.** The SCD2 close sets only effective_to and
  is_current, so a closed version keeps both stamps.

Per write strategy, for one incoming row:

| Strategy | Situation | `__provenance` (created) | `__provenance_changed` |
|---|---|---|---|
| Append (batch or streaming) | every row | this write | n/a (skipped or rejected) |
| `write.mode: insert` | new key | this write | n/a |
| | existing key | untouched (row not written) | n/a |
| SCD1 keyed merge | new key | this write | this write |
| | tracked change | **kept** | this write |
| | nothing changed | kept | kept (row skipped, see below) |
| | untracked-only change (`scd.untracked`) | kept | kept (row skipped, as today) |
| SCD2 | new key | this write (new version) | this write |
| | tracked change | closed version keeps its stamps; new version: this write | closed version keeps it; new version: this write |
| | untracked-only change, or sequence-only bump (touch) | kept | this write |
| | nothing changed | kept | kept |
| | delete (`delete_when`) or `close_on_missing` | closed version keeps it | closed version keeps it |
| Derived dataset (replace) | every row | this write (contents are replaced) | n/a |

**SCD1 needs change detection when the changed stamp is on.** Without
`scd.untracked`, the Delta SCD1 merge runs an unconditional
`whenMatchedUpdateAll()`, so every matched row would be restamped on every
load. With `provenance.changed` on, the SCD1 merge uses the same change
condition `scd.untracked` turns on: tracked is every column except the keys,
insert-only columns and the changed stamp, and an unchanged row is skipped.
For the data columns this changes nothing (an unchanged row is rewritten with
the same values today); it only stops no-op rewrites. Entities without
`provenance.changed` keep today's unconditional update; the created stamp alone
needs only the explicit update set.

Under SCD2 without any untracked columns, `scd.sequence_by` or `scd.tracked`,
there are no touches, so the changed stamp always equals the created stamp of
its version. That is correct, just not informative; the stamp earns its keep
on entities with untracked columns, and on SCD1.

The same rules apply to `merge_as_stream`, which runs the same merges per
micro-batch.

### Where the stamps are built

One pure function, in a new `packages/kindling/provenance.py` (beside
`surrogate_keys.py`), builds the column from literals, `create_map` and
`current_timestamp()`, and raises if the DataFrame already has a column of that
name. When both stamps are on, the same struct value is written to both
columns; the merge decides which target rows each one reaches. It is applied in
three places:

1. **Batch:** in the persist step, after the `persist.before_persist`
   receivers (`simple_read_persist_strategy.py:300-308`) and before the write
   branches, derived replace included (`:311-414`). Framework-owned, not a
   signal receiver, so user handlers can't strip it. Receivers chaining
   (`SignalEmitter.emit_chained`, commit `c2a218a` on `feat/surrogate-keys`)
   makes the receivers compose with each other; the stamp does not depend on
   it, since it is added after them. A non-SCD2 surrogate key aspect therefore
   never sees the stamp, and keys never hash it.
2. **Streaming:** on the transformed stream in `SimplePipeStreamStarter`, next
   to the surrogate key call before `append_as_stream` / `merge_as_stream`
   (`pipe_streaming.py:205-222`). `processed_at` is per micro-batch;
   `run_id` is the run that started the stream.
3. **SDP/Lakeflow:** in the shared dataset builder, on the result of
   `dataset.execute(**input_dfs)` (`oss_engine.py:252`), which covers the OSS
   engine and the Databricks shapes except the Temporal chain lowering. Values
   are bound at declaration time, `processed_at` included (a literal, so
   incremental refresh never sees `current_timestamp()`); `run_id` is null.

**Identifiers.** Two ids, so rows join to the run record:

- `run_id` is the per-initialization id defined in `runtime_manifest.md`, the
  key of its run record. It is null until that lands.
- `execution_id` is the executor's run id, which exists today
  (`generation_executor.py:321`, `data_pipes.py:821`): a new uuid per
  `execute` / `run_datapipes` call, so one initialization can have several.

Neither reaches the persist step today. Phase 1 gets them two ways:

- **Batch:** captured from the `orchestrator.pipe_started` /
  `datapipes.before_pipe` signals (`generation_executor.py:1094`,
  `data_pipes.py:854`), which carry the executor's run id, keyed by pipe id,
  as the watermark aspect keys its read-time capture by pipe
  (`watermarking.py:602-681`). No interface change. `run_id` comes from the
  runtime manifest.
- **Streaming:** the signals don't cover it. `StreamingOrchestrator` calls
  `start_pipe_stream` without emitting either one, and emits
  `streaming.orchestrator_started` only after the queries have started
  (`streaming_orchestrator.py:161,332`). The stream starter needs the identity
  handed to it before the stream starts, e.g. passed in by the orchestrator,
  separately from the batch capture.

### Schema

An `augment_schema_for_provenance`, like `augment_schema_for_scd2` and the
surrogate-key augmentation, so new tables get the columns at creation and
`schema.drift: fail` and SDP schemas accept them.

Existing tables gain the columns through schema evolution on the next write.
Append and SCD1 merges already evolve; the SCD2 merge now does too, for any
batch column the table lacks (`_execute_scd2_merge` on `feat/scd-untracked`,
PR #361), which removes the migration step the first revision expected.
Existing rows keep a null stamp, which means "written before provenance was
enabled". Because neither stamp is compared, adding them never re-versions an
SCD2 table.

### Reads downstream

Both stamp columns are **dropped by default** when a downstream pipe reads the
entity (the persist strategy's read step, the streaming read and the SDP
builder's internal reads, `oss_engine.py:246-249`), with a per-input opt-out.
Otherwise a downstream table can inherit a stamp naming the wrong pipe, and map
fields break `distinct` and `groupBy` in downstream pipes. The SCD2 current
companion is read the same way.

## Phases

1. **Created stamp:** settings block and tags, the parser and validation, the
   column builder in batch, streaming and SDP, drop-on-read, schema
   augmentation, and `__provenance` in the insert-only set of both providers'
   merges. Depends on the insert-only machinery from surrogate keys phase 1 and
   on the untracked-columns work (PR #361).
2. **Changed stamp:** `provenance.changed`, the "written but never compared"
   handling in the SCD2 touch set and the SCD1 update set, and SCD1 change
   detection when it is on.
3. **Input versions:** the version of each input read (Delta table version, or
   the watermark aspect's captured cursor). This is the recovery capability
   `governed_artifact_platform.md` needs, and the reason to keep the field in
   the contract from phase 1.

Phases 2 and 3 are independent of each other.

## Interactions

- **SCD and AUTO CDC.** Lakeflow AUTO CDC has no insert-only column, and it
  updates every column outside the tracked set in place on the current
  version. On an AUTO CDC target, both stamps therefore mean "the update that
  last applied a change to this version"; a new version still carries the stamp
  of the update that opened it. This is a documented divergence (with the SCD1
  skip, `auto_cdc.py:12-27`), and declaration warns once per entity. Tracking
  lists: when the entity has an allow-list (`scd.tracked`, lowered to
  `track_history_column_list`, `engine.py:391-394`), keep it as is, since the
  stamps are already outside it. Only without an allow-list, add both stamp
  columns to `track_history_except_column_list` (which becomes
  `untracked + [sequence_by]` with PR #361). Passing both lists would fail
  declaration, as AUTO CDC accepts only one. Append flows and materialized
  views have no such gap; a materialized view's rows carry the refresh that
  computed them.
- **Surrogate keys.** The created stamp shares their insert-only class and
  invariants, but surrogate keys are rejected in SDP/Lakeflow declarations
  (`surrogate_keys_not_lowered`, commit `d80f9dc`) while provenance is not: a
  stamp with the divergence above is still useful, a missing key is not.
- **Several writers to one entity:** batch planning assumes one producer per
  entity and SDP rejects duplicates. Under fan-in (`fan_in_upsert_pipes.md`)
  the created stamp names the writer that inserted the key and the changed
  stamp the last writer that changed it.
- **Writes outside the persist step** (file ingestion, the watermark entity)
  are not stamped in phase 1. With the settings default on, an ingestion
  target that no pipe writes gets an always-null column at creation.
- **Performance:** constant per write, so encoding makes it nearly free; the
  changed stamp doubles that. SCD1 change detection adds a comparison and
  removes no-op rewrites.

## Decisions

Proposed in this revision, for confirmation:

1. **Config model:** the `kindling.provenance.*` block, baked in as the
   lowest-precedence tag defaults, with per-entity tags overriding (above).
   Alternatives: overlays only; resolving the settings at each use site.
2. **Two columns**, `__provenance` (created, insert-only) and
   `__provenance_changed` (last changed, written but never compared), with the
   changed stamp off by default.
3. **SCD1 change detection is turned on by the changed stamp** (rather than
   restamping every matched row on every load).
4. **AUTO CDC:** allowed with a documented divergence and a declaration
   warning. The alternative is to reject it, as surrogate keys are rejected.
5. **Eligibility of the settings default:** Delta and memory providers; not
   read-only, SQL, companion or `system.*` entities.

Still needed:

6. Names: `provenance.*`, `__provenance` and `__provenance_changed` (keeping
   `lineage.*` free for issue #8), given the ontology's different use of
   "Provenance".
7. Run-id capture: signal-keyed (recommended for phase 1), an activator
   signature change, or the execution-scoped token the watermark aspect
   anticipates.
8. Tags as a map (recommended) or typed fields.
9. Drop on read by default (recommended).
10. Later, not now: a "closed by" stamp written by the SCD2 close clause, and
    stamping writes outside the persist step.

Resolved since the first revision: the `before_persist` chaining issue (fixed
by `emit_chained`, commit `c2a218a`); the migration step for existing SCD2
tables (the SCD2 merge now evolves the schema); and the in-place vs frozen
question, now answered per stamp (created is frozen, changed is in place).

## Size and risks

**M** for phases 1 and 2 together, less than the first estimate because the
insert-only and untracked classes already exist: the parser, defaults layer and
validation, the column builder, the persist step and stream starter,
drop-on-read, schema augmentation, the merge-class wiring in both providers and
the AUTO CDC lists, with tests on the memory and Delta providers and on SDP.
Tests must show: a created stamp survives SCD1 updates, SCD2 touches and
closes; a changed stamp moves on a tracked SCD1 change and an SCD2 touch, and
never on an unchanged row; a new stamp column on an existing SCD2 table opens
no versions; an entity opts out against the settings default.

Risks: unclear stable sources for app name and environment; whether Lakeflow
incremental refresh treats the stamp as non-deterministic (a declaration-time
literal should avoid it); explicit MERGE update sets on the delta-spark 2.4
floor (already exercised by the surrogate-key SCD1 path).
