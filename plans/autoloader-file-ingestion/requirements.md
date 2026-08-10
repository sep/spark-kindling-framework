---
plan_slug: autoloader-file-ingestion
phase: requirements
rig: kindling
rig_root: /workspaces/kindling
artifact_root: /workspaces/kindling/plans
status: draft
created_at: 2026-08-06T16:55:02Z
updated_at: 2026-08-06T16:55:02Z
---

# Requirements: Wire Databricks Auto Loader into file ingestion

## Problem Statement

`FileIngestionProcessor` / `ParallelizingFileIngestionProcessor`
(`packages/kindling/file_ingestion.py`) is batch-only and stateless between
runs:

- `process_path()` re-lists the entire landing directory (`env.list(path)`)
  on every invocation — cost grows with directory size regardless of how
  much is actually new.
- "Already processed" tracking relies on physically moving files out of the
  landing path (`movepath`) rather than any persisted checkpoint/offset —
  correctness depends on files leaving the source location.
- `infer_schema` is accepted on `FileIngestionMetadata` but never used;
  `_build_df_plan` hardcodes `inferSchema=false`. No schema drift/evolution
  handling exists on the file-ingestion read path.
- Nothing in the codebase references Databricks Auto Loader (`cloudFiles`),
  even though `checkpointLocation` is already a first-class concept on every
  other streaming provider (Delta, Parquet).

Tracked as GitHub issue #228 (sep/spark-kindling-framework).

## Solution

Add Databricks Auto Loader (`cloudFiles`) support to the file-ingestion
path, scoped **per `FileIngestionEntry`** (one stream, one checkpoint
location, one schema location per entry — keyed by `entry_id`) rather than
one shared stream over an entire landing path. This preserves the existing
regex-routing/fan-out model (many entries can watch one physical path, each
with its own pattern and destination entity) while replacing full-listing
discovery with Auto Loader's incremental discovery and checkpointed state.

This should live in `kindling_ext_databricks`, since `cloudFiles` is a
Databricks-only Spark option and core `kindling` stays engine-neutral.

## User Stories

1. As a pipeline author, I can register a `FileIngestionEntry` that uses
   Auto Loader for discovery instead of full-directory listing, without
   changing how destination entities are declared or written to.
   - Existing `_write_table_group` / `append_to_entity` write path is
     unchanged.
   - Existing regex named-group column enrichment and `static_values`
     injection behave the same as today.
   - A file matching no entry's pattern is never read/parsed (only listed),
     same cost profile as today — this requires per-entry globs, not one
     shared stream over the whole path.
2. As an operator, re-running ingestion against a landing path does not
   re-read files already ingested, even if `movepath` is not configured.
   - Checkpoint state persists across runs at a path derived from
     `entry_id`, following the convention already used by Delta streaming
     checkpoints.
3. As a pipeline author, I can opt a `FileIngestionEntry` into schema
   evolution instead of a fixed/inferred schema.
   - Behavior is explicit (opt-in), not a silent change to existing entries.
4. As a maintainer, non-Databricks engines are unaffected — existing batch
   `spark.read`-based entries continue to work exactly as before.

## Out Of Scope

- Changing the write side (`append_to_entity`, `_write_table_group`,
  parallel-table-write logic).
- Building Auto Loader support into core `kindling`/other engine
  extensions (ADX, Cosmos).
- Redesigning the signal-emission contract wholesale — only the
  per-file → per-microbatch adaptation needed for `before_file`/`after_file`
  to keep working under Auto Loader.

## Other Notes

Open design questions carried into the implementation plan:

- Per-file signal emission (`file_ingestion.before_file`/`after_file`) needs
  to shift from "per listed filename, before read" to "per file discovered
  within a delivered microbatch" — a real semantic shift for any signal
  consumer, not a drop-in.
- Checkpoint/schema-location path convention — should follow whatever
  convention Delta streaming already uses (`entity_provider_delta.py`).
- Whether `infer_schema` on `FileIngestionMetadata` becomes the opt-in for
  Auto Loader's schema evolution, or a new field is added specifically for
  the Auto Loader path.
- Config surface for opting an entry into Auto Loader vs. the existing
  batch path (new `FileIngestionEntries.entry(...)` param vs. separate
  registration).
