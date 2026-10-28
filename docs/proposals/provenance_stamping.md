# Provenance Stamping

**Status:** Proposed (revised 2026-10-19: an optional `kindling.provenance.*`
settings block with per-entity tag overrides; one untracked column, written
with every row write. Refreshed 2026-10-21 against 0.14.0, where the untracked
column class, chained `before_persist` receivers, SCD2 schema evolution and
surrogate keys shipped). **Phase 1 shipped** in #365 (0.14.1): the settings
block and tags, the column in batch, streaming and SDP writes, drop-on-read,
schema augmentation, and the merge and AUTO CDC handling. `run_id` is filled
from the runtime manifest (`runtime_manifest.md`) for batch and streaming
stamps. **Phase 2 (`input_versions`)** implemented for batch writes:
watermarked inputs record their cursor, Delta inputs are read pinned at the
version they record.
**Created:** 2026-10-16
**Related:** `governed_artifact_platform.md` (names provenance stamping as the
small Kindling write-path feature its whole story leans on, and asks for it to
be specced against the current write path: this is that spec),
`scd_untracked_columns.md` (the provenance column is untracked),
`runtime_manifest.md` (rows link to a run record by `run_id`),
`surrogate_keys.md` (also adds columns at persist time; phase 1 shipped in
0.14.0),
`fan_in_upsert_pipes.md` (several writers per entity).

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
which run, wrote it, from which inputs. The record is data. It lives in the
row, is queried with SQL, and is kept as long as the row is.

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

- **Operations:** which pipe and run wrote this row, under which app,
  environment and Kindling version?
- **Recovery** (the governed-artifact case): which outputs were built from
  version 12 of a reference entity, so exactly those can be reprocessed?

## Proposal

### The column

One nullable struct column per stamped entity, default name `__provenance`
(matching the `__effective_from` bookkeeping style), renameable:

```
struct<
  pipe_id, pipe_name, run_id, execution_id, persist_id,
  processed_at timestamp,
  mode            -- 'batch' | 'streaming' | 'sdp'
  app_name, environment, platform, kindling_version,
  input_entity_ids array<string>,
  input_versions   map<string,string>,   -- phase 2: entity id -> version read
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
  (`simple_read_persist_strategy.py:223`); it is null for streaming and SDP.

**The rule is "overwrite overwrites".** The column is written with every row
write and names the pipe and run that wrote the row as it is stored now. When a
write replaces a row, its provenance is replaced with it. Kindling keeps no
separate history of provenance: to track provenance over time, use SCD2, where
each version carries the provenance of the write that stored it.

### Configuration

**Recommended: an optional settings block, with per-entity tags as
overrides.**

```yaml
kindling:
  provenance:
    enabled: false                       # default
    column: __provenance
    pipe_tags: []                        # allow-list; literal names and fnmatch globs
```

```yaml
# on one entity (tags=, dataentities:, dataentities-bytag: or entity_tags)
provenance.enabled: "false"              # opt out (or "true" to opt in)
provenance.column: "_lineage"
provenance.pipe_tags: "domain,owner,layer.*"
```

The block sits in any settings layer (base, platform, environment, or the
app's own files), so "on in prod, off locally" is one line in an environment
file.

**How the two combine.** The settings block is applied as the
**lowest-precedence tag defaults**, baked into the entity's metadata when it is
built (`DataEntityManager._build_metadata`, `data_entities.py:1836`): after the
`dataentities-bytag:` and `dataentities:` overlays have resolved, each
`provenance.*` key the entity does not already have is filled from the block.
Every layer above therefore wins, in the existing order: declared tags,
`dataentities-bytag:`, `dataentities:`, the exact-id `entity_tags` map, and
run-scoped `tag_overrides` (`DataEntityManager.get_entity_definition`, `data_entities.py:2021`).
An entity opts out in its own declaration with `provenance.enabled: "false"`.

After that, the tags are the single contract. One parser,
`provenance_config_from_tags(entity)`, next to `scd_config_from_tags`
(`data_entities.py:272`) and `surrogate_config_from_tags` (`:664`), is what every write path,
provider merge and extension calls. The `kindling.provenance.*` settings keys
and the `provenance.*` tag keys are literals inside that module only: no
exported key constants, and extensions never read the keys themselves (the
maintainer rule).

**Which entities the default reaches.** The settings block skips entities it
does not fit; explicit tags are validated as declared.

| Entity | Settings default |
|---|---|
| Provider `delta` (the default, `entity_provider_registry.py:180`) or `memory` | applied |
| Any other provider (CSV and Event Hub cannot hold a struct column) | skipped; an explicit tag opts in |
| `read_only`, an SQL entity, an SCD2 current companion (`provider_type: view`), a `system.*` entity | skipped |

Registration rejects the column in `merge_columns`, `partition_columns`,
`cluster_columns`, `scd.tracked`, `scd.untracked`, `scd.sequence_by`,
`surrogate.from` or `derived.replace_keys`, and a name starting with `__scd2_`
(reserved for the merge's staging columns). `provenance.pipe_tags` is an
allow-list only (`*` is rejected), and tags that held resolved secrets are
always excluded (pipe tags may hold resolved secrets; see
`data_pipes.py:632-683`).

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
  `kindling.delta.access_mode` pattern (`pipe_streaming.py:188-189`) or the
  Cosmos run-level defaults (`entity_provider_cosmos.py:654`). It needs no
  baking, but about eight sites would each apply the same precedence: the
  persist step, the stream starter, the SDP builder, the Databricks AUTO CDC
  lowering, the Delta and memory merges, schema augmentation, drop-on-read and
  validation. A site that read only the tags would treat the column as data
  and, under SCD2, version every row. Baking the defaults into tags keeps one
  contract.

### How each write strategy treats it

The stamp is built before the write (below), so every incoming row carries it.

| Strategy | Provenance on the stored row |
|---|---|
| Append, batch or streaming (the common fact-table case) | The pipe and run that appended the row |
| `write.mode: insert` | The pipe and run that inserted the row; an existing key is not written, so it keeps its provenance |
| SCD1 keyed merge | The last write that updated the row: an update overwrites provenance along with the other columns |
| Derived dataset (replace) | The write that replaced the rows |
| SCD2 | Each version records the write that stored it; history over time comes from the versions themselves |

**The one rule: the provenance column is untracked** (the untracked class of
`scd_untracked_columns.md`). A difference only in provenance never counts as a
change: it never opens an SCD2 version, never triggers an SCD1 update, and
never triggers an SCD2 in-place update. Otherwise, because the stamp differs on
every run, every load would re-version or rewrite every matched row. When the
row *is* written for another reason, provenance is written with it:

- **SCD2, tracked change:** the current version is closed and keeps its
  provenance (the close sets only effective_to and is_current); the new version
  carries this write's.
- **SCD2, untracked-only change** (another untracked column, or a sequence-only
  bump): the current version is updated in place, and its provenance is
  overwritten in place with it. Overwrite overwrites.
- **SCD1 with change detection** (`scd.untracked` declared): a row whose only
  difference is provenance, or another untracked column, is skipped, so its
  provenance names the last writer that made a real change.
- **SCD1 without change detection** (today's default unconditional
  `whenMatchedUpdateAll()`): every matched row is rewritten and restamped.
  That is fine as is; provenance does not turn change detection on.

In the merge code this means one thing: the provenance column is left out of
the change tests (tracked, and the untracked comparison that selects SCD2
touches) and stays in every write set (the SCD2 insert values and touch set,
the SCD1 update set). It is not an insert-only column. The same rules apply to
`merge_as_stream`, which runs the same merges per micro-batch.

### Where the stamp is built

One pure function, in a new `packages/kindling/provenance.py` (beside
`surrogate_keys.py`), builds the column from literals, `create_map` and
`current_timestamp()`, and raises if the DataFrame already has a column of that
name. It is applied in three places:

1. **Batch:** in the persist step, after the chained `persist.before_persist`
   receivers (`emit_chained`, `simple_read_persist_strategy.py:303-312`) and
   **after** `apply_surrogate_keys` (`:317`), before the read-only check and
   the write branches, derived replace included (`:319` on). Framework-owned,
   not a signal receiver, so user handlers can't strip it. Coming after the
   key step means a non-SCD2 key never hashes the stamp.
2. **Streaming:** on the transformed stream in `SimplePipeStreamStarter`, after
   the surrogate key call (`pipe_streaming.py:209-211`) and before
   `merge_as_stream` / `append_as_stream` (`:224`, `:231`). `processed_at` is per micro-batch;
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
  `data_pipes.py:855`), which carry the executor's run id, keyed by pipe id,
  as the watermark aspect keys its read-time capture by pipe
  (`watermarking.py:602-660`). No interface change. `run_id` comes from the
  runtime manifest.
- **Streaming:** the signals don't cover it. `StreamingOrchestrator` calls
  `start_pipe_stream` without emitting either one, and emits
  `streaming.orchestrator_started` only after the queries have started
  (`streaming_orchestrator.py:162,332`). The stream starter needs the identity
  handed to it before the stream starts, e.g. passed in by the orchestrator,
  separately from the batch capture.

### Schema

An `augment_schema_for_provenance`, like `augment_schema_for_scd2`
(`data_entities.py:575`) and `augment_schema_for_surrogate_keys`
(`surrogate_keys.py:656`), so new tables get the column at creation and
`schema.drift: fail` and SDP schemas accept it.

Existing tables gain the column through schema evolution on the next write.
Append and SCD1 merges already evolve, and since 0.14.0 the SCD2 merge does
too, for any batch column the table lacks (`_execute_scd2_merge`), which
removes the migration step the first revision expected.
Existing rows keep a null stamp until they are next written; null means
"written before provenance was enabled". Because the column never counts as a
change, adding it never re-versions an SCD2 table.

### Reads downstream

The provenance column is **dropped by default** when a downstream pipe reads
the entity (the persist strategy's read step, the streaming read and the SDP
builder's internal reads, `oss_engine.py:244-249`), with a per-input opt-out.
Otherwise a downstream table can inherit provenance naming the wrong pipe, and
map fields break `distinct` and `groupBy` in downstream pipes. The SCD2
current companion is read the same way.

## Phases

1. **Pipe and run provenance:** settings block and tags, the parser and
   validation, the column builder in batch, streaming and SDP, drop-on-read,
   schema augmentation, and the column's place in the untracked class in both
   providers' merges and the AUTO CDC lists. Builds on the untracked column
   class shipped in 0.14.0.
2. **Input versions:** the version of each input read (Delta table version, or
   the watermark aspect's captured cursor). This is the recovery capability
   `governed_artifact_platform.md` needs, and the reason to keep the field in
   the contract from phase 1.

## Interactions

- **AUTO CDC.** Both lists come from one function, `track_history_kwargs`
  (`kindling_ext_databricks/auto_cdc.py:95`, used for change-feed and snapshot
  flows at `engine.py:378` and `:397`). When the entity has an allow-list
  (`scd.tracked`, lowered to `track_history_column_list`), keep it as is,
  since the provenance column is already outside it. Only without an
  allow-list, add the column to `track_history_except_column_list` (today
  `untracked + [sequence_by]`). Passing both lists would fail
  declaration, as AUTO CDC accepts only one. On Lakeflow the column then means
  "the update that last applied a change to this version": AUTO CDC updates
  except-listed columns in place, and also does so when only provenance
  differs, where the runner skips the row. Either way the row's data is the
  same and the stamp names a write that stored it, so under "overwrite
  overwrites" this is not treated as a divergence and needs no warning.
  Append flows and materialized views have no such difference; a materialized
  view's rows carry the refresh that computed them.
- **Surrogate keys.** Independent: keys are insert-only, provenance is
  untracked and rewritten with its row. Three points of contact:
  - order: the stamp is added after `apply_surrogate_keys`, so non-SCD2 keys
    never hash it; SCD2 keys are minted inside the merge from `surrogate.from`
    and effective_from, which never include it;
  - custom generators: `KeyContext.columns` ("every data column after
    transforms, no staging, temporal or key columns") must also leave out the
    provenance column. Otherwise a generator that iterates `ctx.columns`
    would hash a per-run value and mint a different key on every load. Pass
    it through `KeyContext`'s existing `excluded_columns`
    (`surrogate_keys.py:418`). `surrogate.from` already rejects it
    (validation list above);
  - SDP: surrogate keys are rejected in SDP/Lakeflow declarations
    (`surrogate_keys_not_lowered`, `kindling_ext_sdp/declaration_engine.py:464`);
    provenance is not.
- **Several writers to one entity:** batch planning assumes one producer per
  entity and SDP rejects duplicates. Under fan-in (`fan_in_upsert_pipes.md`)
  the value is the last writer per row.
- **Writes outside the persist step** (file ingestion, the watermark entity)
  are not stamped in phase 1. With the settings default on, an ingestion
  target that no pipe writes gets an always-null column at creation.
- **Performance:** constant per write, so encoding makes it nearly free.

## Decisions

Confirmed (2026-10-19 to 2026-10-21) and shipped in phase 1:

1. **Config model:** the `kindling.provenance.*` block, baked in as the
   lowest-precedence tag defaults, with per-entity tags overriding (above).
   Alternatives: overlays only; resolving the settings at each use site.
2. **One untracked column, overwrite overwrites.** Provenance over time comes
   from SCD2 versions, not from extra columns.
3. **A provenance-only difference also never triggers an SCD2 in-place
   update** (it is excluded from the untracked comparison, not only from the
   tracked one), so an unchanged current row is not rewritten on every load.
   Confirmed 2026-10-19: the stamp is built per run, so every incoming row of a
   keyed merge carries a new one; counting it would rewrite every current row
   on every load (a daily full load of 1M rows with 100 real changes would
   rewrite 1M). With this rule, `__provenance` names the run that last
   *changed* the row or version, not the last run that saw it. Append and
   insert-only tables are unaffected: each row is written once.
4. **Eligibility of the settings default:** Delta and memory providers; not
   read-only, SQL, companion or `system.*` entities.
5. **Name:** `provenance.*` tags, `kindling.provenance.*` settings and a
   `__provenance` column, keeping `lineage.*` free for issue #8.
6. **Run-id capture:** signal-keyed. `ProvenanceRunCapture` takes the
   executor's run id from `orchestrator.pipe_started` / `datapipes.before_pipe`,
   keyed by pipe id, and clears it when the pipe ends. Streams started by
   `StreamingOrchestrator` had no such signal, so it emits
   `streaming.pipe_starting` (and `streaming.pipe_start_failed`); their ids
   last until the run ends, so recovery restarts keep them. No starter
   signature change.
7. **`tags` is a `map<string,string>`** of allow-listed pipe tags.
8. **Drop on read by default,** with a pipe-level opt-out,
   `provenance.keep_inputs: "<entity ids>"` (pipes have no per-input options).

Confirmed 2026-10-28 for phase 2:

- **Exact input versions.** A watermarked input records the cursor its read
  covers. A plain Delta input is read at a looked-up version (time travel:
  `versionAsOf`, or `VERSION AS OF` for a catalog table), so the stamp names
  exactly the snapshot read even when a commit lands before the write; a
  failed lookup gives a plain read and no version, never a guess. Other
  providers record nothing. The capability is
  `VersionedReadableEntityProvider.read_entity_versioned`.
- **Whenever the output is stamped, batch only.** No new setting; reads pin
  only for a stamped output. Streaming and SDP stamps keep `input_versions`
  null (per-micro-batch source offsets are separate work).

Still open:

9. Later, not now: stamping writes outside the persist step.
10. `kindling migrate` compares the declared schema only, so under
    `schema.drift: fail` the first stamped write to an existing table fails
    until the column is added (as for SCD2 temporal and surrogate key
    columns).

Resolved since the first revision, all shipped in 0.14.0: the
`before_persist` chaining issue (`SignalEmitter.emit_chained`,
`signaling.py:138`); the migration step for existing SCD2 tables (the SCD2
merge evolves the schema); and the in-place vs frozen
question (in place: the column is untracked and rewritten with its row).

## Size and risks

**M** for phase 1, less than the first estimate because the untracked class
already exists: the parser, defaults layer and validation, the column builder,
the persist step and stream starter, drop-on-read, schema augmentation, and
the merge and AUTO CDC wiring, with tests on the memory and Delta providers
and on SDP. Tests must show: a provenance-only difference opens no SCD2
version, triggers no SCD1 update with `scd.untracked` and no SCD2 in-place
update; an SCD2 close keeps the closed version's provenance; an SCD1 update
and an SCD2 in-place update overwrite it; a new provenance column on an
existing SCD2 table opens no versions; an entity opts out against the settings
default; a custom generator's `KeyContext.columns` leaves out the provenance
column.

Risks: unclear stable sources for app name and environment; whether Lakeflow
incremental refresh treats the stamp as non-deterministic (a declaration-time
literal should avoid it).
