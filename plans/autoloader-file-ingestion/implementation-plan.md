---
plan_slug: autoloader-file-ingestion
phase: implementation-plan
rig: kindling
rig_root: /workspaces/kindling
artifact_root: /workspaces/kindling/plans
requirements_file: /workspaces/kindling/plans/autoloader-file-ingestion/requirements.md
status: draft
created_at: 2026-08-06T16:55:02Z
updated_at: 2026-08-06T18:45:23Z
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

## Decision Addendum (2026-08-06)

Resolved while triaging `kind-5i2`. Each decision is grounded in what the
codebase actually does today, not asserted from first principles — see the
rationale for what was checked.

### 1. Exact package/module home

**Decision**: A new, narrow extension package, `kindling_ext_databricks_autoloader`
(Poetry name `spark-kindling-ext-databricks-autoloader`), containing a single
module `autoloader_file_ingestion.py`. It depends on `spark-kindling>=0.10.0`
only — not on `spark-kindling-ext-databricks`.

**Rationale**: `packages/extensions/kindling_ext_databricks/pyproject.toml`
scopes that package explicitly to "Databricks Lakeflow adapter for Kindling's
SDP declaration engine" and hard-depends on `spark-kindling-ext-sdp`. Auto
Loader file-discovery has nothing to do with declarative-pipeline lowering;
folding it into that package would mix two unrelated capabilities under one
dependency graph for no benefit. The established convention for
capability-scoped extensions is one package per capability —
`kindling_ext_adx` → `entity_provider_adx.py`, `kindling_ext_cosmos` →
`entity_provider_cosmos.py`, `kindling_ext_sdp` → the SDP engine,
`kindling_ext_databricks` → the Lakeflow adapter — each with its own
`pyproject.toml` scoped to exactly one thing. A new package matches that
convention instead of breaking it. None of the existing extensions keep a
package-local `tests/` directory; tests live centrally
(`tests/unit/test_adx_entity_provider_extension.py`,
`test_cosmos_entity_provider_extension.py`), so the tests bead should add
e.g. `tests/unit/test_databricks_autoloader_file_ingestion.py` rather than a
new test tree under the extension package.

### 2. Regex `patterns` vs. Auto Loader glob syntax

**Decision**: A second, explicit field — `source_glob: Optional[str] = None`
on `FileIngestionMetadata`, required (validated at
`FileIngestionEntries.entry()` registration time) when `discovery="autoloader"`.
`patterns[0]` keeps its exact current meaning and stays in use, evaluated
per-file inside `foreachBatch`, purely for named-group extraction,
`dest_entity_id.format(**named_groups)`, and `filetype` resolution — the same
job it does in `_build_df_plan` today. `source_glob` is passed to the
per-entry `cloudFiles` stream as `.option("pathGlobFilter", source_glob)`.

**Rationale**: Regex and glob are not generally convertible. Entries rely on
named capture groups (`fe.dest_entity_id.format(**named_groups)`, per-file
column injection from `named_groups.items()`), which have no glob equivalent
— "restrict patterns to a glob-compatible subset" would silently drop that
capability rather than translate it, and reusing the field would change its
type contract for existing `discovery="batch"` entries reading the same
attribute. The stream-level filter is not optional overhead, either: the plan
scopes one Auto Loader stream per *entry*, not per landing path, and multiple
entries can watch the same physical path (multiple patterns/destinations over
one directory is the existing fan-out model). Without a glob filter at the
source, every entry's stream would list and read every file in the shared
path and only discard non-matches post hoc inside `foreachBatch` — reading
file content Auto Loader had no need to read, which reintroduces exactly the
cost profile requirements.md calls out avoiding ("A file matching no entry's
pattern is never read/parsed"). `pathGlobFilter` filters at listing time
before content is read; the regex re-match against `patterns[0]` still runs
per-row afterward, unchanged, purely for enrichment.

### 3. Checkpoint/schema-location root convention

**Decision**: Reuse the existing `kindling.storage.checkpoint_root` config
key (documented in `docs/reference/config_reference.md`, and already the
checkpoint root `SimplePipeStreamStarter` reads for streaming-pipe
checkpoints in `packages/kindling/pipe_streaming.py`). Derive per-entry paths
as `f"{checkpoint_root}/file_ingestion/{entry_id}/checkpoint"` and
`f"{checkpoint_root}/file_ingestion/{entry_id}/schema"`, mirroring the
existing `f"{base_chkpt_path}/{pipe.pipeid}"` pattern in `pipe_streaming.py`
(with a `file_ingestion/` namespace segment so entry IDs can't collide with
pipe IDs sharing the same root). No new config key.

**Rationale**: The "no repo-wide derivation convention" note under Current
System is about callers of `entity_provider_delta.py`/`entity_provider_parquet.py`
supplying `checkpointLocation` themselves — it is not a statement that no root
config key exists. `kindling.storage.checkpoint_root` already is that root,
read the same way (`self.cs.get("kindling.storage.checkpoint_root")`) by
`pipe_streaming.py`'s `SimplePipeStreamStarter.start_pipe_stream`, and already
documented for exactly this purpose in `config_reference.md`. A second,
`ingestion`-namespaced root key would fork configuration surface for a
problem that already has one deployment-wide answer, leaving operators to set
two checkpoint roots for what is conceptually one concern.

The "derived from `dest_entity_id`'s own storage location" alternative does
not work by construction: `dest_entity_id` on an entry can itself be a
template resolved per file (`fe.dest_entity_id.format(**named_groups)` in
`_build_df_plan`), so a single entry's destination — and therefore its
storage location — is not knowable until a file is matched and its named
groups extracted. A stream's checkpoint/schema location has to exist before
the stream starts, i.e. before any file has been matched, so deriving it from
a per-file-resolved value is circular. `entry_id` is stable at registration
time and has no such problem.

### 4. Per-file signal (`before_file`/`after_file`) semantics under `foreachBatch`

**Decision**: Enumerate distinct files within each microbatch (via the same
`_metadata.file_path` column already needed for enrichment re-derivation) and
emit `before_file`/`after_file` per file with the same payload shape used
today (`filename`, `dest_entity_id`, `matched`, `batch_id`). Use the
microbatch's own id (from `foreachBatch(batch_df, batch_id)`, coerced to
`str`) instead of minting a fresh UUID. `before_process`/`after_process`
continue to wrap one `process_path()` call end-to-end — one full
`Trigger.AvailableNow` run, aggregating totals across however many
microbatches that run produces — matching today's one-call-one-process-pair
contract.

This is a documented, intentional semantic shift, not a silent one: for
`discovery="autoloader"` entries, `before_file` no longer precedes the
physical read of that file. Auto Loader has already read the file's rows into
the microbatch before `foreachBatch` runs, so both signals now fire together,
post hoc, around this file's enrichment/grouping step rather than bracketing
its read. The docs bead should call this out explicitly for signal
consumers.

**Rationale**: Dropping per-file signals for Auto Loader entries (the
batch-level-only alternative) is a silent observability regression for
anyone consuming `file_ingestion.after_file` for lineage/audit — `discovery`
is opt-in per entry, so a consumer would quietly lose per-file visibility for
some entries but not others, with nothing telling them it happened. The
performance argument against enumerating files per microbatch is weak here:
Auto Loader delivers only newly-discovered files per microbatch (small,
incremental), not the full landing directory, so a `distinct()` over
`_metadata.file_path` is bounded by new-file count, not landing-path size —
it does not reintroduce the O(directory size) cost this plan exists to
eliminate. Reusing Spark's own microbatch id costs nothing and lets emitted
signals correlate directly with Structured Streaming's own batch bookkeeping.

### 5. Schema-evolution policy field name/values

**Decision**: A new field — `schema_evolution_mode: Optional[str] = None` on
`FileIngestionMetadata` — passed straight through as
`.option("cloudFiles.schemaEvolutionMode", schema_evolution_mode)` for
`discovery="autoloader"` entries when set. Accept Databricks' own values
verbatim (`"addNewColumns"`, `"rescue"`, `"failOnNewColumns"`, `"none"`)
rather than inventing a kindling-specific vocabulary; leaving it unset lets
`cloudFiles` apply its own default. `infer_schema` is untouched.

**Rationale**: `infer_schema` is a `bool` today and, per this plan's own
Current System note, already dead code on the batch path (`_build_df_plan`
hardcodes `inferSchema=false` regardless of its value).
`cloudFiles.schemaEvolutionMode` is a four-value enum controlling a different
lifecycle moment — how a running stream reacts to new columns after its
schema is established — than "infer column types from file content at
initial read," which is what `infer_schema`'s name and type currently imply.
Overloading a dead, differently-typed, differently-scoped bool field for this
is the same shape of problem as `patterns` vs. glob in decision 2, decided
the same way for consistency: add a field rather than silently reinterpret an
existing one. Passing Databricks' own option values straight through, instead
of mapping to a kindling-specific enum, keeps the new field a thin,
predictable wrapper that will not drift from Databricks' own documented
behavior.
