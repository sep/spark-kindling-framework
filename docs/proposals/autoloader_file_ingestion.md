# Databricks Auto Loader file-ingestion decisions

**Status:** implemented. This record preserves the design decisions from the August 2026 Auto Loader planning work for issue #228. Usage and current behavior are documented in the [extension README](../../packages/extensions/kindling_ext_databricks_autoloader/README.md) and [file-ingestion guide](../guide/file_ingestion.md).

- Keep Auto Loader in the capability-specific `kindling_ext_databricks_autoloader` extension. It does not depend on the Lakeflow declaration engine; batch ingestion remains platform-neutral.
- Use one stream per ingestion entry. A separate `source_glob` filters discovery while existing regex patterns retain named-group enrichment and destination resolution. Regex and glob semantics are not interchangeable.
- Reuse `kindling.storage.checkpoint_root`, with per-entry checkpoint and schema directories under `file_ingestion/<entry_id>/`. Entry IDs are stable before discovery; destination IDs may depend on filename captures and cannot provide that identity.
- Preserve per-file signals inside each microbatch. For Auto Loader, these signals surround processing after Spark has read the rows; they do not bracket the physical file read. Process-level signals wrap the full available-now invocation.
- Use an explicit `schema_evolution_mode` for the native Auto Loader option. Do not reinterpret the existing boolean `infer_schema`, which represents a different concern.

Operational requirements drafts, task breakdowns, and execution reports are maintained outside source control. Durable design decisions belong in `docs/proposals/`.
