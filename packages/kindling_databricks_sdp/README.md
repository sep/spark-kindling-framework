# kindling_databricks_sdp

Databricks Lakeflow adapter for Kindling's SDP declaration engine.

Lakeflow "extends and is interoperable with" the OSS `pyspark.pipelines`
API, so this adapter **augments** the `kindling_sdp` core rather than
translating it: the OSS emission (materialized views with comments, table
properties, partitioning, clustering, schema) runs unchanged, and
Databricks-only capabilities are layered on top.

Selected with:

```python
kindling.initialize(engine="databricks_sdp")
...
kindling.declare_pipeline()
```

— resolved through kindling core's generic engine-extension convention
(`kindling_<name>.engine_extension()`); adding this engine required zero
core changes.

## Phase 3 (this package today): expectations

```yaml
datapipes:
  silver.orders:
    engine:
      databricks_sdp:
        expectations:            # violations counted, rows kept (warn)
          valid_order_id: "order_id IS NOT NULL"
        expectations_drop:       # violating rows dropped
          positive_qty: "quantity > 0"
        expectations_fail:       # violation fails the update
          no_future_dates: "order_date <= current_date()"
```

Blocks map to `expect_all` / `expect_all_or_drop` / `expect_all_or_fail`
decorators. Declaring them while targeting OSS fails fast at validation
(capability gating in the core); declaring them here against a runtime
without the Lakeflow decorators fails at declaration with an actionable
error.

`refresh_policy: incremental` is accepted (adapter-tier gated) but emits
nothing yet — incremental MV refresh on Databricks (Enzyme) is engine
behavior rather than a declaration keyword; treated as a documented hint
pending verification against a live workspace.

## Deferred

- AUTO CDC / SCD2 declared-flow mapping (`create_auto_cdc_flow`,
  `create_auto_cdc_from_snapshot_flow`) — Phase 5, on the merged
  `scd.sequence_by` / `scd.source_kind` / `scd.delete_when` semantics.
- Streaming tables and append flows — Phase 4 (core first).
