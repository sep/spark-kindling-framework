# Declaration Conventions: Tags First, Annotations as Sugar

The convention for adding higher-level options to Kindling declarations:

1. **Higher-level options are expressed through configuration — specific,
   documented tags.** The tag is the canonical, engine-agnostic surface: it
   is what registration validates, what engines read when lowering, and
   what tools can inspect. If a capability cannot be stated as tags, it is
   not yet declarative enough to add.

2. **Annotated declarations with new semantics may be added on top, and
   they work by setting those tags as defaults.** A decorator or
   declaration helper that names a semantic (a derived dataset, an
   immutable event table, an SCD2 dimension) is sugar: it fills in the
   canonical tags so common shapes read as intent. Explicit tags on the
   declaration always win over the annotation's defaults — the annotation
   proposes, the tag disposes.

## Why

- **One source of truth.** Engines, validators, and tests only ever look
  at tags. Adding an annotation never adds a second code path — it cannot
  diverge from the tag behavior because it *is* the tag behavior.
- **Everything stays overridable and data-drivable.** Because the option
  is a tag, it can come from generated code, config-driven registration,
  or an annotation, interchangeably.
- **Validation stays in one place.** Registration-time tag validation
  covers every spelling of the declaration automatically.

## The layering

| Layer | Role | Examples |
| --- | --- | --- |
| Entity/pipe **tags** | Canonical semantics — what the thing *is* | Entities: `write.mode`, `scd.type`, `scd.tracked`, `dataset.kind`, `derived.replace_keys`, `sdp.dataset_type`. Pipes: `pipe_type`, `temporal.kind` |
| **Config keys** (`kindling.*`, `datapipes.<id>.engine.<engine>.*`) | Execution options and just-in-time overrides — how/where it runs | `kindling.temporal.max_generations`, engine `table_properties` blocks |
| **Annotations / declaration helpers** | Named semantics as sugar — set the tags above as defaults | `DataEntities.derived_entity` setting `dataset.kind: derived`; `DataEntities.insert_only_entity` setting `write.mode: insert` |

This applies equally to entities and pipes. Existing pipe-level
instances: the view-pipe declaration merges `pipe_type: view` defaults
(explicit tags win), and `declare_temporal_chain()` is a higher-level
declaration that lowers to pipes tagged `temporal.kind:
chain_events`/`chain_episodes` — engines route on those tags, never on
how the pipes were declared.

The distinction between the first two rows follows the existing rule:
semantics belong in the declaration (tags), execution options belong in
config. Annotations only ever touch the first row.

## Rules when adding a new capability

1. Define the tag vocabulary first, with registration-time validation and
   clear error messages. Ship and test the capability tag-only.
2. If a sugar declaration is worth having, implement it as
   "merge these default tags into the declaration, explicit tags win",
   and nothing else. No behavior may live only in the annotation.
3. Engines read tags, never annotations. An engine that needs to know
   whether an entity is derived asks `derived_config_from_tags(entity)`,
   not "was it declared with the derived helper".
