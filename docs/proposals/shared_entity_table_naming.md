# Shared Entity Table Naming

**Status:** Accepted for implementation.
**Created:** 2026-09-08
**Related:** `declarative_pipelines_engine.md`,
`lakeflow_structured_config.md`,
`docs/reference/config_reference.md`,
`packages/kindling/entity_naming.py`.

## Problem

Kindling historically made two separate naming decisions. The runtime
`EntityNameMapper` resolved logical entity IDs to external table names, while
the SDP/Lakeflow declaration path mapped the same logical IDs to pipeline-local
dataset names. The two paths could silently disagree: `table_schema` caused the
runtime resolver to flatten the whole dotted ID, `kindling.sdp.dataset_naming:
leaf` only affected pipeline-local names, and generated temporal entities had no
hand-written declaration site for per-entity `provider.table_name` overrides.

The required use case is declarative placement for medallion and temporal
entities through environment YAML: no per-table enumeration, no new settings
layer, no `config_files` naming workaround, and no rewrite of logical entity
IDs.

## Decision

Kindling uses one pure core naming policy in `packages/kindling/entity_naming.py`
with the vocabulary `legacy`, `normalized`, and `leaf`. The global key is
`kindling.storage.table_naming`; the per-entity tag is
`provider.table_naming`. The per-entity tag overrides the global key, and a full
`provider.table_name` remains a terminal external-name override.

The core policy derives an unqualified table component only. External
catalog/schema composition stays in `ConfigDrivenEntityNameMapper`, and
pipeline-local SDP/Lakeflow declarations stay single-part. Both `normalized`
and `leaf` apply the existing dot/hyphen-to-underscore normalization.

`legacy` is a first-class mode and absence remains equivalent to legacy. Legacy
means the historical conditional resolver branches: (a) explicit
`provider.table_name` verbatim; (b) no storage namespace treats logical IDs as
already qualified; (c) schema configured flattens the full logical ID below that
schema; (d) catalog-only layers the configured catalog onto the logical ID's own
dot structure; and (e) no resolved namespace flattens to a bare leaf. These
branches are compatibility behavior, not a uniform normalization model.

## Decisions From Open Questions

**OQ-1 - Prefix placement.** `kindling.storage.table_name_prefix` is external
only. The resolver applies it when composing the external table name; the shared
core returns an unprefixed component and SDP/Lakeflow dataset names do not gain
the prefix. Moving the prefix into the shared derivation would rename existing
pipeline-local Lakeflow datasets.

**OQ-2 - Legacy mode.** `legacy` is accepted explicitly so operators can pin
compatibility behavior or opt one entity out of a global `leaf` policy.

**OQ-3 - Mode vocabulary.** The only modes are `legacy`, `normalized`, and
`leaf`. A qualified/passthrough mode was rejected because already-qualified IDs
are a namespace-resolution branch of legacy behavior, not a table-component
derivation strategy.

**OQ-4 - Collision validation.** `kindling.storage.collision_check` is opt-in:
`off` by default, or `pipeline` / `registry`. The check is metadata-only and
never scans a live catalog. `provider.table_alias_of` marks an intentional alias
within the same resolved address group.

**OQ-5 - SDP divergence.** When `kindling.sdp.dataset_naming` is absent, SDP
projects the shared policy. When it is explicit and disagrees with the shared
policy or a more-specific `provider.table_naming`, declaration validation emits
`naming_policy_conflict` unless
`kindling.sdp.dataset_naming_divergence: intentional` is set.

**OQ-6 - Reference catalog.** There is no special reference-catalog key. The
reference tier uses `provider.table_catalog` with an operator-supplied value,
typically interpolated by the deployment environment.

**OQ-7 - Namespace sufficiency.** Explicit `leaf`/`normalized` table naming
requires at least a schema. A catalog is additionally required only when
Databricks Unity Catalog is enabled. Synapse and Fabric are not forced into a
UC-style three-part shape.

**OQ-8 - Case folding.** External-address collision comparison uses
`str.casefold()` over the resolved address, matching the existing
pipeline-local emitted-name reservation. The check is opt-in, so over-reporting
is a clear config-time diagnostic rather than a silent wrong write.

**OQ-9 - SDP external reads.** SDP external reads resolve through the injected
`EntityNameMapper` only when an explicit shared naming policy is configured.
With no shared policy, the historical bare logical-ID read is preserved. The
trigger is the configured policy, not whether the consumer engine is
Databricks SDP.

## Declarative Placement Example

```yaml
kindling:
  storage:
    table_naming: leaf
    table_schema: cwmdp

dataentities:
  "bronze.**":
    tags:
      provider.table_catalog: dev_bronze
  "silver.**":
    tags:
      provider.table_catalog: dev_silver
  "gold.**":
    tags:
      provider.table_catalog: dev_gold
  "reference.**":
    tags:
      provider.table_catalog: ${REFERENCE_CATALOG}
```

`silver.device_telemetry` resolves externally to
`dev_silver.cwmdp.device_telemetry`. A tag-rule version can set both
`provider.table_catalog` and `provider.table_naming: leaf` for each `tier`
value, with later `dataentities:` id-pattern entries still able to override a
broad tag default.

## Boundaries

No new LDP settings layer, interpolation language, matcher implementation, or
`kindling.lakeflow.config_files` naming workaround is introduced. Logical entity
IDs remain registry keys. Pipeline-local declarations and in-pipeline reads
remain single-part even when external names are multipart.

REQ-031's bundle-wide or cross-application global collision boundary remains
out of scope. Kindling does not have a complete resource/output inventory, and
this decision does not add a catalog or network scan.
