# Read-Only Entity Registration

**Status:** Proposal
**Created:** 2026-04-07
**Related:** entity_providers.md, entity_configuration.md, data_entities.md

## Summary

Add a `read_only` tag to entity definitions that changes the `ensure` behavior from "create or write" to "register in catalog if not already registered." The underlying table is treated as externally owned — Kindling will only read from it, never write to it.

## Motivation

Data apps commonly need to read from tables they don't own — upstream Delta tables produced by other pipelines, shared reference data, or manually curated datasets. Today there is no way to express this intent in the entity definition. Without it:

- `ensure_entity_table` may attempt to create or overwrite a table the app has no business touching
- Write operations (merge, append, write) silently succeed against tables that should be protected
- The catalog may not have the table registered under the expected name, causing read failures even though the physical data exists at a known path

## Proposed API

```python
@DataEntities.entity(
    entityid="shared.reference_data",
    name="reference_data",
    merge_columns=[],
    schema=ReferenceDataSchema,
    tags={
        "provider_type": "delta",
        "provider.path": "abfss://curated@storage.dfs.core.windows.net/reference/data",
        "read_only": "true",
    }
)
```

The `read_only` tag is the only required addition. `provider.path` is needed when the physical path differs from what `EntityPathLocator` would derive, but is otherwise optional.

## Behavior

### `ensure_entity_table`

| Scenario | Current behavior | Read-only behavior |
|---|---|---|
| Table registered in catalog, path matches | Use it | Use it (no-op) |
| Table not in catalog | Create managed table | Register as external table at `provider.path` |
| Table in catalog, path differs | N/A | Raise `EntityPathConflictError` (see below) |

The registration step uses `CREATE TABLE IF NOT EXISTS <name> USING DELTA LOCATION '<path>'` — the same SQL already used in `_attempt_catalog_registration`. This makes the table an **external table**: the catalog entry points to the path, but Kindling does not own the data lifecycle.

### Write operations

`merge_to_entity`, `append_to_entity`, and `write_to_entity` raise `ReadOnlyEntityError` immediately when called on a read-only entity — before any I/O is attempted. This is a hard guard, not a warning.

### Read operations

`read_entity`, `read_entity_as_stream`, and `read_entity_since_version` are unaffected. A read-only entity is fully readable.

### Path conflict

When `ensure_entity_table` is called and the catalog already has a registration for the table name but at a different path than `provider.path`, the behavior depends on a new tag:

- `read_only.on_path_conflict: error` *(default)* — raise `EntityPathConflictError` with the current and expected paths. Requires manual intervention.
- `read_only.on_path_conflict: reregister` — drop the existing catalog entry and re-register at the new path. Logs a warning. Does not touch physical data.

The default is `error` because silently re-pointing a catalog entry is destructive in environments where other apps share the same catalog.

## Implementation Sketch

### `EntityMetadata`

No changes needed. The `tags` dict carries `read_only` and `read_only.on_path_conflict`.

### `DeltaEntityProvider`

1. Add `_is_read_only(entity) -> bool` — checks `entity.tags.get("read_only") == "true"`.

2. `_ensure_table_exists` — branch on `_is_read_only`:
   - If read-only: call new `_ensure_external_registration(entity, table_ref)` instead of the create path.
   - Otherwise: existing behavior unchanged.

3. `_ensure_external_registration(entity, table_ref)`:
   - If table exists in catalog:
     - Resolve registered path via `DESCRIBE DETAIL`
     - If path matches `table_ref.table_path`: no-op
     - If path differs: honor `read_only.on_path_conflict` tag
   - If table not in catalog:
     - Run `CREATE TABLE IF NOT EXISTS <name> USING DELTA LOCATION '<path>'`

4. `merge_to_entity`, `append_to_entity`, `write_to_entity` — add guard at entry:
   ```python
   if self._is_read_only(entity):
       raise ReadOnlyEntityError(entity.entityid)
   ```

### New exceptions

```python
class ReadOnlyEntityError(Exception):
    """Raised when a write is attempted on a read-only entity."""

class EntityPathConflictError(Exception):
    """Raised when catalog registration exists at a different path than expected."""
```

Both live in `entity_provider_delta.py` alongside the provider.

## What this is not

- **Not a permissions enforcement mechanism.** The guard is in Kindling code, not in the storage layer. An app could still write to the underlying path directly. The tag communicates intent and catches accidental writes in app code.
- **Not applicable to storage access mode only.** Read-only works in both catalog and storage modes. In storage mode, the "registration" step is skipped (there is no catalog to register in) and the behavior reduces to: read freely, block writes.

## Open Questions

1. Should `read_only` be expressible via config tag overrides (YAML) as well as in the decorator? Config-based overrides already work for any tag — this should work automatically, but worth calling out explicitly in docs.
2. Should streaming writes (`append_as_stream`) also be blocked? Almost certainly yes, but worth confirming.
3. Is `reregister` conflict behavior ever safe on Fabric (which has its own catalog semantics)? May need platform-specific handling.
