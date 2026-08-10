---
plan_slug: autoloader-file-ingestion
phase: implementation-plan
rig: kindling
rig_root: /workspaces/kindling
artifact_root: /workspaces/kindling/plans
requirements_file: /workspaces/kindling/plans/autoloader-file-ingestion/requirements.md
status: draft
created_at: 2026-08-06T16:55:02Z
updated_at: 2026-08-06T16:55:02Z
---

# Implementation Plan: Wire Databricks Auto Loader into file ingestion

## Summary

Add an Auto Loader (`cloudFiles`)-backed discovery path for
`FileIngestionEntry`, scoped per entry rather than per landing path, living
in the Databricks extension rather than core `kindling`. GitHub issue #228.

## Current System

- `packages/kindling/file_ingestion.py`:
  - `FileIngestionMetadata` (dataclass): `entry_id`, `name`, `patterns`,
    `dest_entity_id`, `tags`, `infer_schema=True`, `filetype="csv"`,
    `static_values`.
  - `FileIngestionEntries.entry(**decorator_params)` registers a metadata
    entry into `FileIngestionRegistry` (DI-bound `FileIngestionManager`).
  - `ParallelizingFileIngestionProcessor.process_path(path, movepath,
    transform)`:
    - `env.list(path)` lists every file in `path` on every call (no
      persisted state between calls).
    - `_build_df_plan` (line ~199-219) checks each filename against every
      registered entry's `patterns[0]` regex until one matches; on match,
      builds a lazy `spark.read.format(filetype)...load(...)` plan, adds
      regex named-groups and `static_values` as literal columns, plus an
      `ingestion_timestamp` column.
    - `inferSchema` is hardcoded `"false"` at the `spark.read` call —
      `FileIngestionMetadata.infer_schema` is never actually read.
    - Matched DataFrames are grouped by `dest_entity_id`, unioned
      (`unionByName(allowMissingColumns=True)`), and written via
      `EntityProvider.append_to_entity` (`_write_table_group`).
    - `movepath`: on successful write, source files are copied then
      deleted — this is the only "already processed" mechanism; there is
      no checkpoint/offset.
    - Emits `file_ingestion.*` signals per-file and per-batch (see
      `FileIngestionProcessor.EMITS`).
- `packages/kindling/entity_provider.py`: capability is composed via mixin
  ABCs — `StreamableEntityProvider.read_entity_as_stream(...)`,
  `StreamWritableEntityProvider.append_as_stream(df, entity,
  checkpoint_location, format=None, options=None)`, etc. Concrete providers
  implement whichever subset applies.
- `packages/kindling/entity_provider_parquet.py` (`ParquetEntityProvider`,
  line ~135-198): closest existing streaming analog —
  `spark.readStream.format(...).schema(schema)` (explicit schema required;
  native file-source streams can't infer), `checkpointLocation` passed as
  a write-side option.
- `packages/kindling/entity_provider_delta.py`: `append_as_stream` (line
  1691) / `merge_as_stream` (line 1705) both take an explicit
  `checkpointLocation`/`checkpoint_location` argument from the caller —
  there is **no repo-wide derivation convention** (e.g. no
  `f"{entity_path}/_checkpoints/{...}"` helper); callers currently supply
  the path themselves. This plan needs to define that convention for Auto
  Loader rather than reuse an existing one.
- `packages/extensions/kindling_ext_databricks/`: currently contains
  **only** the Lakeflow SDP declarative-pipeline engine (`engine.py`,
  `engine_extension.py`, `auto_cdc.py`, `temporal_lowering.py`,
  `lakeflow_app_selector.py`) — no `EntityProvider` or file-ingestion code
  exists in this package today. There is no established precedent inside
  this specific extension for the provider-mixin pattern; the closest
  precedent is core's own `entity_provider_parquet.py`/`_delta.py`.
- `packages/kindling/entity_provider_registry.py`: providers are registered
  by `provider_type` string via DI (`EntityProviderRegistry`,
  `_register_builtin_providers`), instantiated through the injector — this
  is the seam any new provider-style registration would use, though
  `FileIngestionEntries`/`FileIngestionRegistry` is a **separate** registry
  or ties into a scheme.

## Proposed Implementation

1. **New module**: `kindling_ext_databricks/autoloader_file_ingestion.py`
   (exact package location TBD by implementer given the extension
   currently has no EntityProvider/file-ingestion precedent — confirm
   whether it belongs in `kindling_ext_databricks` or a smaller, more
   targeted extension before writing code).
2. **Opt-in surface**: extend `FileIngestionEntries.entry(...)` with an
   explicit new field (e.g. `discovery: str = "batch"`, allowed values
   `"batch"` | `"autoloader"`) rather than overloading `infer_schema`.
   Defaulting to `"batch"` preserves all existing entries unchanged.
3. **Per-entry stream, not per-path stream**: for entries with
   `discovery="autoloader"`, `ParallelizingFileIngestionProcessor` (or a new
   sibling processor) creates one `spark.readStream.format("cloudFiles")`
   per entry, scoped to that entry's own glob (derived from the entry's
   `patterns`/a new explicit `source_glob` field — regex-to-glob mapping
   needs its own decision, since Auto Loader globs are not regexes).
   - `.option("cloudFiles.schemaLocation", <derived path>)`
   - `.option("checkpointLocation", <derived path>)`
   - Trigger: `.trigger(availableNow=True)` so `process_path()` remains a
     "run now, drain what's new, stop" call rather than a long-running
     stream — matches the existing external invocation model (something
     calls `process_path` on a schedule).
4. **Checkpoint/schema-location convention (new)**: define and document a
   derivation, e.g. `f"{ingestion_root}/_autoloader/{entry_id}/checkpoint"`
   and `.../schema`. Needs a concrete root config key (likely under
   `ingestion.*`, alongside existing `ingestion.max_parallel_tables`).
5. **Enrichment logic reused, not rewritten**: inside `foreachBatch`,
   recover per-row filename via `input_file_name()` (or
   `col("_metadata.file_path")` in newer Spark), re-derive named regex
   groups from that filename, and apply the same
   `withColumn(...)`/`static_values`/`ingestion_timestamp` logic already in
   `_build_df_plan` (line ~221-231) — factor that block into a small
   shared helper so batch and Auto Loader paths don't duplicate it.
6. **Write path unchanged**: `foreachBatch` calls into the same
   `_write_table_group` → `EntityProvider.append_to_entity` path used
   today.
7. **`movepath` becomes optional/advisory** for Auto Loader entries — since
   checkpoint state already prevents reprocessing, moving files is purely
   for landing-zone hygiene, not correctness. Existing batch entries keep
   current `movepath` semantics unchanged.
8. **Schema evolution**: when opted in, rely on `cloudFiles.schemaEvolutionMode`
   options rather than the current hardcoded `inferSchema=false`. Needs an
   explicit per-entry policy field, not a silent behavior change.
9. **Non-Databricks engines unaffected**: the new discovery path only
   activates for `discovery="autoloader"` entries; batch entries take the
   exact code path they do today. If `kindling_ext_databricks` isn't
   installed/available, entries requesting `"autoloader"` should fail fast
   with a clear error rather than silently falling back to batch.

## Testing

- Unit: `FileIngestionMetadata`/`FileIngestionEntries` accept and validate
  the new `discovery` field (default preserved, invalid values rejected).
- Unit: enrichment-logic helper (regex named groups + static_values +
  ingestion_timestamp) produces identical output whether invoked from the
  batch path or a `foreachBatch` callback — extract and test it in
  isolation.
- Integration (Databricks-dependent, likely `tests/integration/` or a new
  `kindling_ext_databricks` test module): a landing directory processed
  twice via an Auto Loader entry ingests new files only on the second run
  (no re-read of already-checkpointed files) — mirrors intent of existing
  `tests/unit/test_file_ingestion.py` / `tests/system/core/
  test_file_ingestion_static_values.py` but for the streaming path.
  Confirm a file matching no entry's glob is never read (only listed/
  discovered), consistent with today's regex-skip-without-read behavior.
- Integration: schema-evolution opt-in behavior (new column appears in a
  later file; asserted behavior — evolve vs. fail — for the chosen policy
  field).
- Confirm existing `test_file_ingestion.py` / `test_entity_provider_parquet*`
  suites are unaffected (regression, `discovery="batch"` default path).

## Rollout

- No migration needed — additive, default-preserving (`discovery="batch"`).
- Docs: `docs/contributing/entity_providers.md` and/or a new file-ingestion
  guide should document the `discovery="autoloader"` option once
  implemented, including the checkpoint/schema-location convention chosen.

## Open Questions

(Carried from requirements; implementer should resolve or explicitly
re-scope during implementation, not silently pick an answer.)

1. Exact package/module home: `kindling_ext_databricks` (current sole
   content is Lakeflow SDP engine code, no provider precedent) vs. a new,
   narrower extension.
2. Regex `patterns` (used for filename routing today) vs. Auto Loader glob
   syntax (`cloudFiles` path globbing) — these are not the same language;
   decide whether entries need a *second*, glob-specific field, or whether
   patterns get restricted to a glob-compatible subset for autoloader
   entries.
3. Checkpoint/schema-location root: new `ingestion.*` config key, entity-tag
   based, or derived from `dest_entity_id`'s own storage location?
4. Per-file signal emission (`before_file`/`after_file`) semantics under
   `foreachBatch` — enumerate files within the batch and emit per discovered
   file (preserves signal shape, changes timing relative to "read"), vs.
   emit only batch-level signals for Auto Loader entries (simpler, breaking
   for existing per-file signal consumers on this path).
5. Schema-evolution policy field name/values — reuse `infer_schema` broadened
   in meaning, or a new field entirely.
