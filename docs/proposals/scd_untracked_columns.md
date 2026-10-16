# Untracked Columns for SCD1 and SCD2

**Status:** Proposed. Design findings against the current code; no code
changes yet.
**Created:** 2026-10-16
**Related:** `docs/guide/data_entities.md` (SCD2 tags, including
`scd.tracked`), `fan_in_upsert_pipes.md` (plans explicit-column merges in the
same SCD1 code; sequence the two together), `provenance_stamping.md` (its
column must be untracked), `kindling_ext_databricks` AUTO CDC lowering.

## Problem

Some columns belong on a row but should not count as a change: ingest
timestamps, load ids, engine bookkeeping such as the Temporal extension's
`generation`, and the provenance column proposed separately. Today:

- **SCD2** has an allow-list, `scd.tracked` (`data_entities.py:277-282`).
  Without it, every non-key, non-temporal column is tracked. A change that
  touches only untracked columns is **ignored**: the current row keeps the
  value it had when the version opened (Delta `entity_provider_delta.py:349-362`,
  memory `entity_provider_memory.py:129-217`; pinned by
  `tests/integration/test_entity_provider_memory_scd2.py:429-443`). The
  Temporal conditions entity relies on this, so its `generation` goes stale.
- **SCD1** (the default keyed merge, not an `scd.type`) has no change
  detection: Delta runs an unconditional `whenMatchedUpdateAll()` and memory a
  full-row replace, so every matched row is rewritten on every load.
- **Lakeflow disagrees with the runner.** AUTO CDC's
  `track_history_column_list` / `track_history_except_column_list` update
  untracked columns *in place* on the current version, whereas the runner
  ignores them, and this is not among the documented divergences
  (`auto_cdc.py:12-27`). The snapshot lowering passes no tracking list at
  all (`engine.py:368-375`), so `scd.tracked` is silently dropped there.

## Proposal

A new entity tag, the exclude-list counterpart of `scd.tracked`:

```yaml
scd.untracked: "ingested_at,load_id"
```

- A comma-separated list of literal column names (globs can come later).
- Mutually exclusive with `scd.tracked`; registration rejects both.
- The tracked set becomes the default tracked set minus these columns.

### SCD2 (`scd.type: "2"`)

| Incoming change | Result |
|---|---|
| Tracked column(s) changed | As today: close the current version, insert a new one with every incoming value |
| Only untracked column(s) changed | **Update the current version in place** (untracked columns and `sequence_by`); `__effective_from` / `__is_current` unchanged |
| Nothing changed | No-op |

Historical versions keep the untracked values they had when they were
closed. In-place update is the behaviour AUTO CDC already has, so the two
engines agree.

### SCD1 (default keyed merge)

A matched row is updated only when a non-key, non-untracked column differs
(`whenMatchedUpdateAll(condition=<null-safe change>)`); an untracked-only
change is skipped. New keys insert as today. This also stops no-op file
rewrites, and audit columns come to mean "the load that last made a real
change". There is no AUTO CDC type 1 equivalent, so this would be a
documented divergence if SCD1 is ever lowered.

### Implementation outline

1. `data_entities.py`: parse and validate `scd.untracked` (not with
   `scd.tracked`; not a key, temporal or `__merge_key` column; in the schema
   when one is declared; at least one tracked column left).
2. Delta SCD2: a third row class ("touch") staged alongside close/keep, with
   conditional `whenMatchedUpdate` clauses. The **sequence guard** must then
   compare against the row's `sequence_by` value, not `__effective_from`, or
   an older event could overwrite a newer in-place value.
3. Delta SCD1: a change condition on `whenMatchedUpdateAll`, in both the
   schema-evolution path and the Delta < 3.2 fallback. A tracked column new
   to the target drops the condition for that batch so it materializes.
4. Memory provider: the same semantics; parity tests run on both providers.
5. AUTO CDC: map to `track_history_except_column_list = untracked + [sequence_by]`,
   and pass tracking lists in the snapshot lowering too (fixes the existing
   gap).
6. Docs: the SCD tag table in `docs/guide/data_entities.md`, and the
   divergence notes in `auto_cdc.py`.

## Edge cases

- **All non-key columns untracked:** rejected; SCD2 would never open a version.
- **Nulls:** null-safe comparison, so null to a value is a change, and an
  incoming null overwrites in place. AUTO CDC's `ignore_null_updates` is out
  of scope.
- **Columns added later:** with the default tracked set, a new column (null on
  the target) flags every row as changed and re-versions the whole table.
  Declaring it untracked turns that into an in-place backfill. Whether the
  SCD2 merge adds new columns at all needs checking (it has no schema
  evolution and lists insert values explicitly).
- **Untracked column missing from the incoming data:** skipped.
- **Nested types:** `build_null_safe_change_condition` handles top-level maps
  only; a struct containing a map (e.g. a provenance struct) needs nested
  handling or the hash path if it were ever compared.

## Decisions needed

1. **Should `scd.tracked` also switch to in-place updates**, so the two tags
   differ only in form? Recommended. It changes existing behaviour (the
   Temporal `generation` column would start updating in place, which looks
   like the original intent) and flips one test.
2. **Tag name for SCD1:** the same `scd.untracked` on an entity without
   `scd.type`, or a separate `write.untracked`.
3. **In-place vs frozen per column:** a provenance column under SCD2 could
   record the last pipe that touched the current version (in place) or the
   pipe that opened it (frozen, today's `scd.tracked` behaviour). This may
   need a per-column mode, or a framework-reserved column that is always
   untracked.
4. **Sequence-only bump:** update the current version in place (as AUTO CDC
   does) or do nothing (today)?

## Size and risks

**M**: about five source files plus tests on both providers; most of the work
is the conditional SCD2 merge clauses. Risks: the behaviour change if
`scd.tracked` goes in place; the sequence-guard change needs out-of-order
tests; multi-clause `whenMatched` ordering on the delta-spark 2.4 floor;
SCD1 skip semantics have no Lakeflow counterpart.
