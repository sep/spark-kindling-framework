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

## Phase 5: SCD declared flows → AUTO CDC

An SCD-tagged output entity (`scd.type` plus the declared-flow tags from
kindling core) is declared as the Lakeflow CDC pattern — a pipeline-scoped
source view (the pipe's DataFrame), a streaming table target, and an AUTO
CDC flow — instead of a materialized view:

| Kindling declaration | AUTO CDC |
|---|---|
| `scd.type: "1"` / `"2"` | `stored_as_scd_type` |
| entity `merge_columns` | `keys` |
| `scd.sequence_by` | `sequence_by` (change feed) |
| `scd.source_kind: change_feed` (default) | `create_auto_cdc_flow` |
| `scd.source_kind: snapshot` / `scd.close_on_missing: true` | `create_auto_cdc_from_snapshot_flow` |
| `scd.delete_when` | `apply_as_deletes=expr(...)` |
| `scd.tracked_columns` | `track_history_column_list` |
| (default) sequence column ≠ content | `track_history_except_column_list=[sequence_by]` |

Fail-fast mapping requirements (validated with everything else, all
errors at once): change-feed targets need `scd.sequence_by`
(`scd_sequence_by_required`); `scd.type` must be `1`/`2`
(`scd_type_unsupported` — bitemporal is deliberately excluded while in
Beta); keys are required (`scd_keys_required`).

Documented divergences from the runner engine (dual-engine parity
criterion): history columns are `__START_AT`/`__END_AT` (the entity's
runner-shape schema is not passed to the streaming table); snapshot
ordering is ingestion order, not `scd.sequence_by`; out-of-order
change-feed rows are reconciled into history rather than ignored under
the runner's strictly-later rule; the `scd.current_entity_id`
current-view companion stays runner-only (an ordinary declared view is
the SDP-native replacement, tracked separately). Expectations, when
configured, attach to the source view — quality is checked on the
incoming feed.

## Deferred

- Streaming tables and append flows for non-SCD datasets — Phase 4 (core
  first).
- Bitemporal AUTO CDC (`stored_as_scd_type="bitemporal"`) — Beta;
  tracked, not adopted (proposal decision).
- Current-view companion as a declared view.
