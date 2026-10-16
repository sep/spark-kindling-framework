# Declaration Derivations: Clone and Extend

**Status:** Implemented (2026-09-29)
**Created:** 2026-09-29

## Problem

Teams routinely need a declaration that is *almost* another one: an entity
with two more columns, a pipe whose output is enriched with a lookup, a copy
of a pipeline segment written to a scratch schema for a canary, a
per-tenant variant of a shared package's entity. Today the options are to
copy the declaration (drift), to patch tags through config overlays (which
cannot add columns or inputs or wrap a transform), or to register derived
declarations programmatically the way the temporal extension does for its
chain lowering (no shared API, import-order coupled).

## Decision

One mechanism, a **derivation**, with two targets:

- **Clone** (`clone_of`): a new declaration under a new id whose raw
  declaration is another's, used as a template. A clone implies nothing
  about data: whatever writes to a cloned entity (a cloned pipe, a new
  pipe, or nothing yet) is a separate declaration.
- **Extend**: additive changes to a declaration's own id.

Both may be combined in one call, and both are **additive and stackable**
with last-in-wins semantics, the same rule config overlays already follow:

| Change | Stacking rule |
| --- | --- |
| `tags` | later value wins for the same key |
| `add_columns` (entities) | names accumulate; same name + same type is a no-op; same name + different type is an error (a schema must stay unambiguous; retyping is a migration) |
| `add_partition_columns`, `add_cluster_columns`, `add_inputs` (pipes) | sets that accumulate |
| `transform` (pipes) | each wraps the previous result; the last registered runs outermost and receives the earlier output |

Clone-only replacements (`name`, `merge_columns`, `partition_columns`,
`cluster_columns` for entities; `name`, `output_entity_id`, `output_type`,
`use_watermark`, `driving_entity_ids` for pipes) are rejected on an
extension, because they would replace rather than add.

Derivations are **resolved by the registries when their dependency is
available**, not at import time. Package B can clone or extend package A's
declaration regardless of which `register_all()` runs first; a derivation
whose source never appears is reported as pending, and looking the target up
raises with the reason rather than reading as an unknown id. Resolution order
is registration order, then config overlays on top, exactly as today, so the
YAML forms below apply in settings-file layering order and environment
overlays keep the final word.

A clone copies the source's **raw declaration plus its extensions**, never
the config-overlaid result; overlays then apply to the clone by its own id.

## API

```python
DataEntities.clone(
    "silver.orders_enriched", from_entity="silver.orders",
    name="orders_enriched",
    add_columns=[StructField("region", StringType())],
    tags={"dataset.kind": "derived"},
)
DataEntities.extend("silver.orders", add_columns=[...], tags={"owner": "sales"})

DataPipes.clone(
    "silver.enrich_orders", from_pipe="silver.build_orders",
    output_entity_id="silver.orders_enriched",
    add_inputs=["ref.regions"],
    transform=lambda df, ref_regions: df.join(ref_regions, "region_code"),
)
DataPipes.extend("silver.build_orders", tags={"sla": "gold"})
```

`transform(previous_output, **added_inputs)` receives the DataFrames of its
own `add_inputs` by keyword (entity id with dots replaced by underscores,
the same convention execute functions use). The original execute receives
exactly its own inputs, so it never sees keyword arguments it did not
declare. The result is still one pipe with one execute: the runner,
streaming, and the SDP declaration engine see nothing new, and a cloned pipe
keeps its own watermark state keyed by the new id.

### YAML form

```yaml
dataentities:
  silver.orders_enriched:
    clone_of: silver.orders
    add_columns:
      - { name: region, type: string }
    tags: { dataset.kind: derived }
datapipes:
  silver.orders_canary:
    clone_of: silver.build_orders
    output_entity_id: silver.orders_enriched
    add_inputs: [ref.regions]
```

`clone_of`, `add_columns` and `add_inputs` are derivation keys; every other
key on the entry is the ordinary override that already exists. Derivation
keys require an exact id, never a glob pattern. Transforms are code-only.

## Non-goals

- Removing or retyping columns, dropping inputs, or redirecting an existing
  pipe's output in place. These are migrations or new declarations.
- Cloning data. A clone is a declaration; `kindling migrate` converges the
  physical table.
- Ownership rules between packages that extend the same declaration.
  Stacking is deterministic and observable (`kindling entity show` lists the
  chain), which is enough.

## Implementation notes

`kindling.declaration_derivations` holds the pure functions (`EntityDerivation`,
`PipeDerivation`, `apply_*_derivation`, `wrap_execute`, the config-section
split). Both managers keep derivations by target id, rebuild a target from
its *effective raw declaration* (source's, then extensions in order) through
the existing overlay matchers, and cascade to clones of a rebuilt id.
`_raw_params` keeps its meaning (direct registrations only), so tag
provenance and secret resolution over raw tags are unchanged; SCD2 companion
convergence considers clone targets as bases too.
