# Derived Datasets and Replacement Writes

Kindling distinguishes two kinds of dataset:

- **State datasets** have identity and memory. Writes describe how state
  *evolves*: append rows, upsert by key (`write.mode: merge`), keep
  history (`scd.type: "2"`), or insert-if-absent (`write.mode: insert`).
- **Derived datasets** have no independent state. Their contents are a
  pure function of their inputs, so a write *replaces* rather than
  evolves. Declared with `dataset.kind: derived`.

The declaration is engine-agnostic. Each execution style materializes it
however it natively can:

| Engine | Materialization |
| --- | --- |
| Runner (batch) | Atomic Delta overwrite swap — full table, or only the batch's slices with `derived.replace_keys` |
| Streaming | Rejected at query start — a continuous sink has no "recompute" moment |
| Declarative (SDP / Databricks Lakeflow) | A materialized view; the platform owns refresh |

## Declaring a derived dataset

```python
@data_entity(
    entityid="gold.run_summary",
    tags={
        "dataset.kind": "derived",
        "derived.replace_keys": "run_id",   # optional: slice scope
    },
)
```

Or with the sugar helper, which sets exactly those tags as defaults
(explicit tags win — see
[declaration_conventions.md](../contributing/declaration_conventions.md)):

```python
@DataEntities.derived_entity(
    entityid="gold.run_summary",
    name="run_summary",
    merge_columns=[],
    schema=None,
    replace_keys=["run_id"],
)
```

- Without `derived.replace_keys`, every write atomically replaces the
  whole table (`mode("overwrite")`). Right for reference data and
  recomputed summaries where the full recompute is proportional to the
  output anyway.
- With `derived.replace_keys`, each write replaces exactly the slices
  present in the incoming DataFrame — the distinct values of the key
  columns become a Delta `replaceWhere` predicate. Right for bounded-run
  reprocessing ("re-analyze run X"): rerunning converges to the identical
  table, and rows the recomputation no longer produces disappear — which
  a merge can never do. Keys should scope coarse slices (runs, days,
  sites), not high-cardinality business keys.

Both forms are one transaction-log commit: old files are dereferenced,
not rewritten, so readers see the previous version until the swap lands
and the write costs only the new data. Every swap is a Delta version, so
"what did this say before the rules changed" is a `VERSION AS OF` query.

### What a derived dataset excludes

The derived and state vocabularies are mutually exclusive and validated
at entity registration:

- `write.mode` and `scd.*` tags are rejected — replacement has no
  evolution semantics.
- `derived.replace_keys` requires `dataset.kind: derived`, and its
  columns must exist in the declared schema (when one is declared).
- A derived entity cannot be a streaming sink.
- On SDP engines, `dataset.kind: derived` lowers to a materialized view;
  a conflicting `sdp.dataset_type: streaming_table` is a validation
  error (`invalid_dataset_type`).

### Change-data-feed caveat

A replacement write emits CDF churn for every row it touches. Do not use
a derived dataset as the watermarked driving source of a downstream
incremental pipe — downstream consumers should read it fully (it is
cheap to read: it is exactly its latest recomputation).

## Insert-only writes (`write.mode: insert`)

For **state** tables whose rows are immutable once landed (event logs,
readings with deterministic IDs), `write.mode: insert` merges with
insert-if-absent semantics: rows whose `merge_columns` keys already
exist are left untouched, new keys are inserted. Replays of
already-landed batches rewrite nothing, so idempotent re-delivery is
near-free.

```python
@data_entity(
    entityid="silver.readings",
    merge_columns=["reading_id"],
    tags={"write.mode": "insert"},
)
```

Sugar form: `@DataEntities.insert_only_entity(...)` sets the
`write.mode: insert` tag as a default.

- Requires `merge_columns` (the dedup keys); contradicts `scd.type`.
- Works identically in batch and streaming (each micro-batch runs the
  same insert-only merge through the stream-merge sink).

## Schema drift policy (`schema.drift`)

State-dataset writes evolve schemas additively by default. The
`schema.drift` tag makes that a declared policy:

- `evolve` (default) — additive evolution, no preflight; today's behavior.
- `warn` — before a merge/append, log any drift (columns the table lacks,
  or same-named columns with different types) and proceed.
- `fail` — refuse a drifting write with `SchemaDriftError` naming the
  columns, *before* the write starts. Use on bronze tables whose producers
  you don't own — silent schema unioning is how tables grow accidental
  envelope branches.

Columns the table has but the incoming DataFrame lacks are not drift:
Delta null-fills them, and SCD2 bookkeeping columns are added by the
merge strategy after the check.

## Choosing a write shape

| You want | Declare |
| --- | --- |
| Rows accumulate, never change | `write.mode: append` |
| Upsert by key, latest wins | `write.mode: merge` (default with `merge_columns`) |
| Upsert by key, keep history | `scd.type: "2"` |
| Immutable rows, dedup on replay | `write.mode: insert` |
| Recomputed from inputs, replace all | `dataset.kind: derived` |
| Recomputed per run/day/slice | `dataset.kind: derived` + `derived.replace_keys` |
