# Memory Provider Inline Seed Rows

**Status:** Implemented (guide documentation pending)
**Created:** 2026-05-13
**Related:** entity_providers.md, entity_configuration.md, config_reference.md, local_python_first.md

## Summary

Add minimal support for inline seed data on memory-backed entities through the existing entity tag/config surface:

```yaml
entity_tags:
  bronze.records:
    provider_type: memory
    provider.seed.rows:
      - id: 1
        date: "2024-01-01"
        value: "hello"
```

When `provider.seed.rows` exists, `MemoryEntityProvider.read_entity()` can materialize those rows into a DataFrame using the entity schema. When it does not exist, memory entities behave exactly as they do today.

This is intentionally small. It does not add seed modes, lifecycle flags, "when" conditions, append/overwrite policies, or persistent state semantics.

## Motivation

Kindling now scaffolds `env.local.yaml` with `provider_type: memory` so generated projects can run locally without Azure credentials. That keeps local execution lightweight, but a newly scaffolded source entity often has no data unless a test or user imperatively preloads the memory provider.

Inline seed rows address the narrow local-run gap:

- generated standalone apps can run with meaningful starter data;
- small demos and examples can be self-contained in config;
- imperative memory usage remains the core testing path;
- CSV fixtures remain the primary convention for integration-test data.

## Current Behavior

`MemoryEntityProvider.read_entity()` currently resolves data in this order:

1. Try a Spark table/temp view by `provider.table_name` or entity name.
2. Try the provider's `_memory_store` by `entityid`.
3. If the entity has a schema, return an empty DataFrame.
4. Otherwise raise `ValueError`.

Local pipeline reads also have a higher-priority fixture path. During local/standalone execution, `SimpleReadPersistStrategy` checks `tests/entities/<entity>.csv` before it asks the registered provider to read the entity. That means CSV fixtures already override memory-provider reads when present.

## Proposal Review Matrix

| Proposal item | Current behavior | Convention fit | Keep/adjust/reject | Reason |
|---|---|---|---|---|
| `provider.seed.rows` on memory entities | No memory config seeding exists | Fits existing flat `provider.*` entity tag convention | Keep | `_get_provider_config()` already strips the `provider.` prefix, so the memory provider can read `seed.rows` without a new config system. |
| Seed only when the key exists | Missing memory entities with schema return empty DataFrames | Fits optional provider config | Keep | Preserves existing behavior for entities with no seed config. |
| No `provider.seed.when` | No conditional seed lifecycle exists | Fits memory provider simplicity | Keep | Local fixture discovery and imperative writes already cover richer setup needs. |
| No `provider.seed.mode` | Memory writes are imperative overwrite/append operations | Fits minimal surface | Keep | Seed rows should be an initial read fallback, not a new write policy. |
| No lifecycle flags | Memory provider is process-local and ephemeral | Fits current provider role | Keep | Stateful seed tracking would imply persistence semantics the memory provider does not have. |
| Validate/coerce against schema | Empty DataFrame uses schema directly | Fits existing schema use | Keep, with strict errors | Require a schema for seeded entities, reject malformed rows, and wrap Spark coercion errors with entity-specific context. |
| Use for standalone local runs | `env.local.yaml` uses memory provider by default | Fits local no-cloud workflow | Keep | Gives generated local apps small meaningful inputs without cloud storage. |
| Interaction with CSV fixtures | Local fixture CSVs take precedence before provider read | Potential overlap | Adjust docs only | Fixtures remain the right path for test data, file-shaped data, and larger samples. |
| Interaction with imperative memory preloading | Table/store data wins before empty fallback | No conflict if precedence is preserved | Keep | Existing writes should continue to override seed rows. |

## Proposed Behavior

`MemoryEntityProvider.read_entity()` should keep the existing precedence and insert seed rows only before the current empty-DataFrame fallback:

1. Try Spark table/temp view.
2. Try `_memory_store`.
3. If `provider.seed.rows` exists, create a DataFrame from those rows using `entity_metadata.schema`, register/store it, and return it.
4. If schema exists, return an empty DataFrame.
5. Otherwise raise `ValueError`.

This makes `provider.seed.rows` a first-read fallback, not an overwrite mechanism. If a test or upstream pipe has already written the memory entity, that data is returned instead.

## Validation Rules

When `provider.seed.rows` exists:

- `entity_metadata.schema` must be present.
- The value must be a list or tuple.
- Each row must be a mapping/object compatible with column names.
- Unknown fields should raise a `ValueError` naming the entity, `provider.seed.rows`, the row index, and the unknown field names.
- Spark schema coercion failures should be wrapped in a `ValueError` naming the entity and `provider.seed.rows`, with the original exception chained.

The provider does not need to manually coerce every Spark type. It can rely on `spark.createDataFrame(rows, schema)` for type conversion and validation, then improve the error message if Spark rejects the data.

## Implementation

### `packages/kindling/entity_provider_memory.py` — DONE

`_get_seed_rows()` and `_create_seed_dataframe()` are implemented as private helpers.
`read_entity()` inserts the seed-rows branch after the `_memory_store` lookup and before
the empty-DataFrame fallback, exactly as proposed.

### Tests — status unknown

The unit test coverage described below should exist in
`tests/unit/test_entity_provider_memory.py`:

- seeded entity creates and returns a DataFrame without imperative preload;
- missing seed config keeps the current schema-backed empty DataFrame behavior;
- existing `_memory_store` data wins over seed rows;
- seed rows without schema fail clearly;
- non-list rows fail clearly;
- row with unknown field fails clearly;
- Spark schema mismatch/coercion error is wrapped with actionable context.

### Docs — PENDING

`docs/config_reference.md` should be updated under Memory Provider to document the new tag:

```markdown
- `provider.seed.rows`: optional list of inline rows for tiny local memory seed data.
```

`docs/guide/local_python_first.md` has not been updated yet. It should cover:

- use memory `provider.seed.rows` for one or two tiny starter rows in local standalone config;
- use `tests/entities/*.csv` for integration tests, larger samples, file-shaped source data, and data that should be edited outside config;
- avoid seed rows on output entities.

> **Note:** The core implementation in `entity_provider_memory.py` is complete, but
> `docs/guide/local_python_first.md` contains no guidance on seed rows. A follow-up task
> is needed to add the guide documentation before this proposal can be fully closed.

## Backward Compatibility

This change is backward compatible when implemented as a fallback:

- entities without `provider.seed.rows` behave exactly as before;
- imperative memory writes and appends still win;
- local CSV fixture overrides still win before the memory provider is consulted;
- no existing provider interfaces need to change;
- no changes are required to `EntityMetadata`.

The only new failures occur for entities that explicitly configure invalid `provider.seed.rows`. That is desired behavior.

## Risks

### Config value shape

`EntityMetadata.tags` is typed as `Dict[str, str]`, but config-driven tags already flow from YAML/Dynaconf and can carry structured values. The minimal change should avoid a broad tag typing refactor and handle structured `provider.seed.rows` locally in the memory provider.

### Large inline data

Inline seed rows are not suitable for large datasets. Docs and templates should keep examples tiny and point larger/local test data to CSV fixtures.

### Misuse on outputs

Seed rows on output entities can be confusing because pipe writes will override or append depending on pipeline behavior. Generated templates should seed only source/input entities.

## Acceptance Criteria

- [x] A memory entity with `provider.seed.rows` can be read without manual imperative preloading.
- [x] A memory entity without seed config behaves exactly as it does today.
- [x] Existing memory data written imperatively takes precedence over seed rows.
- [x] Schema mismatch in seed rows produces a clear, actionable error.
- [x] Local CSV fixture discovery remains unchanged and continues to take precedence during standalone local execution.
- [ ] Docs and template guidance in `docs/guide/local_python_first.md` cover seed rows usage. *(pending)*

## Non-Goals

- No `provider.seed.when`.
- No `provider.seed.mode`.
- No seed lifecycle tracking.
- No automatic CSV-to-memory conversion.
- No provider-wide seed registry.
- No changes to Delta, CSV, EventHub, or SQL providers.
