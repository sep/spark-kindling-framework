# Auto Liquid Clustering in the SDP/Lakeflow Declarative Path

**Status:** Proposed. No framework code changes made yet.
**Created:** 2026-08-21
**Related:** `declarative_pipelines_engine.md` (the SDP declaration engine
and OSS emitter this proposal extends), `docs/contributing/entity_providers.md`
(documents `cluster_columns=["auto"]` for the classic engine).

## Problem

Kindling's classic Delta entity provider fully supports Databricks liquid
clustering's "auto" mode (`CLUSTER BY AUTO`, where the engine chooses and
evolves clustering columns itself) — but the SDP/Lakeflow declarative
lowering path does not, and would silently mishandle it rather than
reject it.

## Evidence

### Classic engine: full support, correctly gated

An entity opts in with `cluster_columns=["auto"]`
(`packages/kindling/data_entities.py:237`). `entity_provider_delta.py`
detects the sentinel (`_is_auto_clustering_requested`, `:728-729`),
rejects it if mixed with real column names (`:754-758`), and requires the
runtime feature flag `delta.auto_clustering` to be `True` before applying
it — raising a clear error otherwise (`:760-768`). That flag is computed
at bootstrap by `discover_runtime_features` (`packages/kindling/features.py:172-224`):
check the DBR version via `spark.databricks.clusterUsageTags.sparkVersion`
(`>= 15.2`), then a live `EXPLAIN ALTER TABLE ... CLUSTER BY AUTO` probe
to confirm the SQL parser actually accepts it — not just a version
guess. Auto clustering additionally requires a catalog-registered table
target, rejecting path/storage-mode entities (`:808-812`). This is
tested (`tests/unit/test_delta_entity_provider_clustering.py`,
`tests/unit/test_features.py`).

### SDP/Lakeflow path: passthrough with the wrong contract, on shared code

The current lowering emits a materialized view — `oss_engine.py:122`,
`decorator = dp.materialized_view(**self._declaration_kwargs(dataset))`
— not `dp.table` as an earlier draft of this doc said. `_declaration_kwargs`
(`:125-139`) builds those kwargs:

```python
if dataset.partition_columns:
    kwargs["partition_cols"] = list(dataset.partition_columns)
if dataset.cluster_columns:
    kwargs["cluster_by"] = list(dataset.cluster_columns)
```

This passes `cluster_columns` straight through as `cluster_by=[...]`,
with no special-casing for the `"auto"` sentinel, and sets both
`partition_cols` and `cluster_by` unconditionally when both fields are
present on the entity. `declaration_engine.py:457` tuples
`cluster_columns` through into the plan with no validation either.

**This method is shared, not OSS-only, and the Databricks extension is a
thin wrapper around the same base engine.** `DatabricksSdpEngine(OssSdpEngine)`
(`kindling_ext_databricks/engine.py:57`) inherits `_declaration_kwargs`
rather than overriding it — it's called from both the plain
materialized-view path (`:132`, confirmed above to still be the
non-chain, non-SCD branch of `_declare_dataset`, `:108-129`) and the
Databricks AUTO CDC path (`:229`). Any fix here runs for both `OSS_SDP`
and `DATABRICKS_SDP` targets unless it explicitly branches on
capability.

The full wiring for `engine="databricks_sdp"` (`kindling_ext_databricks/engine_extension.py`):
`DatabricksSdpEngineExtension.declare_pipeline()` calls
`kindling_ext_sdp.bootstrap.declare_pipeline(engine_factory=DatabricksSdpEngine)`,
which constructs `DatabricksSdpEngine(capabilities=DATABRICKS_SDP)` — the
default in its own `__init__` (`engine.py:60-67`). `DatabricksSdpEngine.validate()`
calls `super().validate(pipe_ids)` first (`engine.py:69-70`) — the base
`DeclarationEngine`/`OssSdpEngine` validation — then layers AUTO-CDC-specific
checks on top. This means the capability-gated validation added to the
base `declaration_engine.py` in the recommendation below applies to the
Databricks extension automatically, through inheritance, with no
separate change needed in `kindling_ext_databricks`. That inheritance
chain is exactly what needs a test, though (see Tests below), rather
than assumed.

Two platform facts, verified against current documentation, that shape
the fix:

- **`cluster_by_auto` is Databricks-only.** `pyspark.pipelines.materialized_view`'s
  actual OSS Spark 4.1 signature (`query_function, name, comment,
  spark_conf, table_properties, partition_cols, cluster_by, schema,
  format`) has no `cluster_by_auto` parameter at all — only Databricks'
  `dp.materialized_view` documents it, as a separate boolean parameter
  distinct from `cluster_by=[...]` (explicit column names), defaulting
  to `False`. Passing `cluster_by_auto` to the OSS decorator raises a
  `TypeError` for an unexpected keyword argument — this is not a
  graceful no-op.
- **Liquid clustering and partitioning are mutually exclusive.**
  Databricks' own documentation states clustering "cannot be combined
  with `PARTITIONED BY`." The classic engine already resolves this by
  preferring `cluster_columns` and skipping `partitionBy` entirely, with
  a warning (`entity_provider_delta.py`'s `_should_partition_files`,
  `:731-741`). `oss_engine.py`'s `_declaration_kwargs` has no equivalent
  precedence today — it would emit both kwargs to Lakeflow regardless of
  whether `cluster_columns` is the `"auto"` sentinel or an explicit list,
  which is a real, pre-existing gap independent of auto-clustering.

An entity declared `cluster_columns=["auto"]` and lowered through SDP
today would therefore call `dp.materialized_view(cluster_by=["auto"])`
— passing the literal string `"auto"` as if it were a real column name,
not enabling automatic clustering, and doing so identically regardless
of whether the target is `OSS_SDP` (where the correct fix must instead
reject it) or `DATABRICKS_SDP` (where it should translate to
`cluster_by_auto=True`).

## Recommendation

Use the capability model this codebase already has for exactly this
kind of Databricks-only feature — `capabilities.py`'s `SdpFeature`/
`CapabilitySet` — rather than inventing a new configuration dependency.
`EXPECTATIONS`, `AUTO_CDC`, and `INCREMENTAL_MV_REFRESH` are all gated
this way already: a static fact about what the declaration *target*
exposes, checked at declaration time, independent of `ConfigService`.
`DeclarationEngine.__init__` takes `capabilities: CapabilitySet`, not
`ConfigService` (`declaration_engine.py:106-115`) — there is no access
path to `packages/kindling/features.py`'s live DBR-version/parser-probe
flag (`delta.auto_clustering`) from inside it, and there shouldn't need
to be one: that probe is specific to the classic engine's own
execution-time DDL emission against a live Spark session it can query
directly (`features.py:155-157`'s own stated philosophy: "capability
probe, not platform sniffing"). The SDP path hands a declarative kwarg
to Lakeflow's own pipeline compute and lets Lakeflow enforce its own
version requirements at graph-resolution time — Kindling's declaration
layer only needs to know whether the *target surface* (OSS vs.
Databricks Lakeflow) exposes the keyword at all.

1. **`capabilities.py`**: add `SdpFeature.AUTO_CLUSTERING`; include it in
   `DATABRICKS_SDP.features`, not `OSS_SDP.features`. Add a
   `supports_auto_clustering(capabilities)` helper alongside the
   existing `supports_expectations`/`supports_auto_cdc`.
2. **`declaration_engine.py`**: recognize `cluster_columns == ("auto",)`
   the same way `entity_provider_delta._is_auto_clustering_requested`
   does; reject `"auto"` mixed with real column names (new validation
   code, e.g. `invalid_auto_cluster_columns`, mirroring the classic
   engine's `ValueError`). This is an entity-level field, not a pipe
   engine-config-block key, so it needs its own check alongside the
   other entity-shape validations (e.g. near `output_entity_not_table_backed`)
   rather than reusing `_validate_capabilities`'s `ADAPTER_TIER_CONFIG_KEYS`
   loop — call `self.capabilities.supports(SdpFeature.AUTO_CLUSTERING)`
   directly and raise a new `auto_clustering_not_supported` issue when it
   isn't. Also reject `cluster_columns=["auto"]` (or any `cluster_columns`)
   combined with `partition_columns` on the same entity — a new
   `conflicting_partition_and_cluster_columns` issue — rather than
   silently dropping one, per this codebase's stated SDP philosophy of
   failing fast at declaration time instead of producing declarations
   the runtime can't execute (a deliberate difference from the classic
   engine's warn-and-drop behavior, justified by that stated philosophy).
3. **`declaration_plan.py`**: add an explicit `cluster_by_auto: bool`
   field to the dataset representation, set once validation in step 2
   passes, so the emitter doesn't need to re-detect the sentinel string.
4. **`oss_engine.py`**: when `cluster_by_auto` is set on the dataset,
   emit `kwargs["cluster_by_auto"] = True` and omit `cluster_by`
   entirely. Since validation in step 2 already guarantees this only
   happens for `DATABRICKS_SDP` targets and never alongside
   `partition_columns`, the emitter itself needs no capability branching
   — it can stay a plain, shared mapping.

## Implementation plan

**Framework**

1. Add `SdpFeature.AUTO_CLUSTERING` to `capabilities.py`, included in
   `DATABRICKS_SDP.features` only, plus a `supports_auto_clustering()`
   helper.
2. Add the sentinel-recognition, capability-gate, and
   partition/cluster-conflict validation to `declaration_engine.py`
   (`invalid_auto_cluster_columns`, `auto_clustering_not_supported`,
   `conflicting_partition_and_cluster_columns`).
3. Add `cluster_by_auto: bool` to `DeclarationPlan`'s dataset
   representation, set by validated declaration, not re-derived in the
   emitter.
4. Update `oss_engine.py`'s `_declaration_kwargs` to emit
   `cluster_by_auto=True` (and omit `cluster_by`) when the plan sets it
   — safe to leave as shared code once step 2 guarantees it can only be
   `True` for a `DATABRICKS_SDP`-validated plan.

**Application**

None — this is a pure framework fix. Existing entities declared with
`cluster_columns=["auto"]` for the classic engine are unaffected; only
entities also lowered through SDP gain correct behavior.

## Tests

- SDP declaration against `DATABRICKS_SDP`: `cluster_columns=["auto"]`
  → plan carries `cluster_by_auto=True`, emitter calls
  `dp.materialized_view(cluster_by_auto=True, ...)` with no `cluster_by`
  kwarg.
- End-to-end through the extension entry point, not just the base class:
  `kindling.initialize(engine="databricks_sdp")` → `declare_pipeline()`
  → `DatabricksSdpEngine` on an entity with `cluster_columns=["auto"]`
  succeeds and produces the same result as constructing
  `DatabricksSdpEngine(capabilities=DATABRICKS_SDP)` directly — proving
  the capability actually reaches the engine through
  `engine_extension.py`'s wiring, not just in an isolated unit test of
  `declaration_engine.py`.
- SDP declaration against `OSS_SDP`: `cluster_columns=["auto"]` → fails
  with `auto_clustering_not_supported` at declaration time — never
  reaches the emitter, never risks a `TypeError` from
  `pyspark.pipelines.materialized_view` rejecting an unknown kwarg.
- SDP declaration: `cluster_columns=["auto", "some_col"]` → fails with
  `invalid_auto_cluster_columns`, matching the classic engine's rejection
  of the same input.
- SDP declaration: `cluster_columns=["auto"]` (or any `cluster_columns`)
  combined with `partition_columns` on the same entity → fails with
  `conflicting_partition_and_cluster_columns`.
- Regression: `cluster_columns=["col_a", "col_b"]` (no auto sentinel, no
  partition conflict) continues to emit `cluster_by=["col_a", "col_b"]`
  unchanged, against both `OSS_SDP` and `DATABRICKS_SDP` targets.

## References

- Apache Spark — [`pyspark.pipelines.materialized_view`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.pipelines.materialized_view.html)
  (OSS Spark 4.1 signature: no `cluster_by_auto` parameter).
- Databricks — [`materialized_view` reference](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-materialized-view)
  (`cluster_by_auto` boolean parameter, default `False`, documented as
  incompatible with `PARTITIONED BY`). Verified via documentation search,
  2026-08-21.
