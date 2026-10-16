# Provenance Stamping

**Status:** Proposed. Design findings against the current write paths; no code
changes yet.
**Created:** 2026-10-16
**Related:** `governed_artifact_platform.md` (names provenance stamping as the
small Kindling write-path feature its whole story leans on, and asks for it to
be specced against the current write path: this is that spec),
`scd_untracked_columns.md` (the provenance column must be untracked),
`runtime_manifest.md` (rows link to a run record by `run_id`),
`surrogate_keys.md` (also adds a column at persist time). Distinct from
issue #8 (column-level lineage analyzer, static plan analysis). Note:
`event_condition_episode_ontology.md` uses "Provenance" for a different
thing, an Event axis (System-Observed vs Synthetic).

## Problem

An output row does not say which pipe, run or inputs produced it. Two
questions need that:

- **Operations:** which pipe and run wrote this row, under which app,
  environment and Kindling version?
- **Recovery** (the governed-artifact case): which outputs were built from
  version 12 of a reference entity, so exactly those can be reprocessed?

## Proposal

### The column

One nullable struct column per opted-in entity, default name `__provenance`
(matching the `__effective_from` bookkeeping style):

```
struct<
  pipe_id, pipe_name, run_id, persist_id,
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

### Configuration, on the output entity

```yaml
provenance.enabled: "true"
provenance.column: "_lineage"          # optional rename
provenance.pipe_tags: "domain,owner,layer.*"   # allow-list; default none
```

- `provenance.pipe_tags` takes literal names and fnmatch globs. It is an
  allow-list only (`*` is rejected), and tags that held resolved secrets are
  always excluded (pipe tags may hold resolved secrets; see
  `data_pipes.py:632-683`).
- Turning it on everywhere uses the existing `dataentities:` /
  `dataentities-bytag:` overlays; no new global setting.
- One parser, `provenance_config_from_tags(entity)`, next to the SCD parser in
  `data_entities.py`. Extensions call it rather than sharing key constants.
- Registration rejects the column appearing in `merge_columns`,
  `partition_columns`, `cluster_columns`, `scd.tracked` or `scd.sequence_by`.

### Where it is added

One pure function builds the column from literals, `create_map` and
`current_timestamp()`, and raises if the DataFrame already has a column of
that name. It is applied in three places:

1. **Batch:** in the persist step after `persist.before_persist` transforms
   and before the write branches (`simple_read_persist_strategy.py:308`).
   Framework-owned, not a signal subscriber, so user handlers can't strip it.
2. **Streaming:** on the transformed stream before the sink is chosen
   (`pipe_streaming.py`); `processed_at` is per micro-batch, `run_id` is the
   run that started the stream.
3. **SDP/Lakeflow:** in the shared dataset builder (`oss_engine.py:252`), which
   covers the OSS engine and the Databricks shapes except the Temporal chain
   lowering. Values are bound at declaration time; `run_id` is null.

`run_id` does not reach the persist step today. Phase 1 captures it from the
`orchestrator.pipe_started` / `datapipes.before_pipe` signals keyed by pipe id
(the watermark aspect's capture pattern), with no interface change.

### Schema

An `augment_schema_for_provenance`, like `augment_schema_for_scd2`, so new
tables get the column at creation and `schema.drift: fail` and SDP schemas
accept it. Existing tables: append and SCD1 merges evolve the schema; the SCD2
merge does not appear to, so enabling provenance on an existing SCD2 table may
need a migration step (not verified).

### Reads downstream

The provenance column is **dropped by default** when a downstream pipe reads
the entity, with an opt-out. Otherwise a downstream table can inherit
provenance naming the wrong pipe, and map columns break `distinct` and
`groupBy` in downstream pipes.

## Phases

1. **Pipe and run provenance:** the struct without `input_versions`; batch,
   streaming and SDP; drop-on-read.
2. **Input versions:** the version of each input read (Delta table version, or
   the watermark aspect's captured cursor). This is the recovery capability
   `governed_artifact_platform.md` needs, and the reason to keep the field in
   the contract from phase 1.

## Interactions

- **SCD:** under SCD2 the default tracked set is every non-key column, so
  without an exclusion every run would version every row. The provenance column
  is implicitly untracked (see `scd_untracked_columns.md`); for AUTO CDC it is
  added to `track_history_except_column_list`. Which pipe it records under each
  strategy depends on that proposal's in-place vs frozen decision.
- **Several writers to one entity:** batch planning assumes one producer per
  entity and SDP rejects duplicates. Under fan-in (`fan_in_upsert_pipes.md`)
  the value would be last-writer-per-row.
- **Performance:** constant per write, so encoding makes it nearly free.

## Existing issue found

`_apply_df_transforms` (`simple_read_persist_strategy.py:67-73`) keeps the
last non-None DataFrame returned by `persist.before_persist` receivers, each
given the original DataFrame, so handlers can silently overwrite each other's
transforms. It affects any column-adding aspect (surrogate keys too) and
should be fixed independently.

## Decisions needed

1. Name: `provenance.*` and `__provenance` (keeping `lineage.*` free for
   issue #8), given the ontology's different use of "Provenance".
2. Run-id capture: signal-keyed (recommended for phase 1), an activator
   signature change, or the execution-scoped token the watermark aspect
   anticipates.
3. Tags as a map (recommended) or typed fields.
4. Drop on read by default (recommended).
5. Fix the signal-chaining issue first.
6. Migration story for existing SCD2 tables.

## Size and risks

**M** for phase 1: the parser and validation, the column builder, the persist
step and stream starter, drop-on-read, schema augmentation, an SCD2 exclusion
and two extension edits, with tests on the memory and Delta providers and on
SDP. Risks: schema evolution on existing SCD2 tables; unclear stable sources
for app name and environment; whether Lakeflow incremental refresh treats
`current_timestamp()` as non-deterministic (a declaration-time literal avoids
it).
