# Changelog

All notable changes to spark-kindling are documented here.

## [0.11.0] - 2026-07-23

### Changed

- **Extension packages moved and renamed**: extensions now live under
  `packages/extensions/` with `kindling_ext_*` module naming
  (`kindling_ext_otel_azure`, `kindling_ext_temporal`, …). Update imports if
  you referenced extension modules directly; pip package names are unchanged.

- Migration now refuses ambiguity instead of proceeding: the planner skips
  non-Delta entities with an explicit `[SKIP]` status (previously a
  memory/parquet/eventhub entity was silently planned as a Delta
  `CREATE_TABLE`), and `migrate apply` fails fast when the plan contains
  inspection errors (previously it skipped errored entities, applied the
  rest, and reported "Migration complete.").

- `persist.before_persist` is now emitted inside the persist failure
  boundary: a raising handler (the supported validation-gate pattern) blocks
  the write, propagates, and pairs with `persist.persist_failed`, so
  watermark bookkeeping discards the pending cursor and a retry re-reads the
  same slice. Observers no longer see an unpaired `before_persist`.

- DAG execution options (`parallel`, `max_workers`, `error_strategy`,
  `pipe_timeout`, `auto_cache`) now resolve config-first from
  `kindling.execution.*` through the hierarchical config layers, with
  function parameters acting as just-in-time overrides. Behavior note:
  deployments that already set `kindling.execution.*` keys will have them
  take effect — previously these options could only be enabled via code.

- Renamed the bootstrap config key `load_local` (YAML) / `load_local_packages` (flat) to
  `load_workspace_packages`, since it controls whether packages are loaded from the platform
  workspace, not "local" packages. The old names still work as deprecated aliases and log a
  warning when used; update `settings.yaml` / `settings.local.yaml` to the new name at your
  convenience.

- Streaming pipes whose output entity declares `merge_columns` now default to
  a per-micro-batch **merge** instead of an append when the sink provider
  supports streaming merges (see Added). Migration note: this applies to
  existing queries under their existing checkpoint — after upgrading, new
  micro-batches merge by business key, but rows appended by earlier runs are
  not retroactively reconciled. If an append-era table already contains
  duplicate business keys, deduplicate or rebuild it before relying on merge
  semantics, or pin the old behavior with the `write.mode: append` entity
  tag. The resolved write mode is logged at query start.

### Added

- **JVM-free telemetry for Databricks UC shared/standard access mode
  clusters**: `spark._jvm` access in the telemetry paths is guarded with
  latched fallbacks, feature discovery probes a new `spark.jvm_bridge`
  runtime capability, and bootstrap swaps in plain-python logger/trace
  providers (`kindling.plain_telemetry`) when the py4j bridge is absent —
  fixing `[JVM_ATTRIBUTE_NOT_SUPPORTED]` crashes on every span. Override
  with `kindling.features.spark.jvm_bridge`. An architecture test now bans
  `_jvm`/`_jsc` outside a justified allowlist.
- **Notebook round-tripping**: `kindling notebook list|pull|push` round-trips
  workspace notebooks (Databricks, Synapse, Fabric) as git-friendly `.py`
  source files (`# COMMAND ----------` cells, markdown as `# MAGIC` blocks —
  the same format the standalone platform reads locally). Backed by new
  `kindling.notebook_source` converters and `kindling_sdk.notebooks` REST
  clients, both usable programmatically. Live-verified byte-identical on all
  three platforms.
- **API-based ADX entity provider in core** (`provider_type: "adx-api"`,
  `[adx]` extra): reads via KQL and append-oriented queued ingestion through
  the azure-kusto Python SDKs — runs where the JVM Kusto connector cannot
  (UC shared clusters, serverless, standalone). Creates tables from the
  declared entity schema via `.create-merge table`. Live-verified round-trip.
- Temporal ontology and episode runtime (`kindling_ext_temporal`): condition
  evaluation, episode lifecycle (determination events, open episodes,
  expiration, invalidation), stateful late real-end revision, validated
  conditions ingestion with per-row quarantine, and chained lowering with
  stratified Lakeflow execution on Databricks.
- Declarative pipelines engine (`kindling_ext_sdp`): declaration-engine
  interface with read classification and capability gating, OSS emission
  engine with write-guard provider and dry-run harness, entity-metadata
  emission with a Databricks Lakeflow adapter, and SCD declared flows
  mapped to Databricks AUTO CDC.
- ADX (Kusto Spark connector) and Cosmos DB entity provider extensions
  (`kindling_ext_adx`, `kindling_ext_cosmos`), both verified against live
  services; Cosmos writes are idempotent upserts by `(id, partition key)`.
- Derived datasets (`dataset.kind: derived`) with atomic replacement writes,
  optionally scoped by `derived.replace_keys`.
- `schema.drift` policy tag — declared handling of write-time schema drift.
- Entity declaration sugar per the tags-first convention (annotations set
  default tags; explicit tags win).
- Kindling data apps selectable in Databricks Lakeflow pipelines.
- Streaming merge support: new `StreamMergeableEntityProvider` capability
  interface (`merge_as_stream` + `is_stream_mergeable`), implemented by
  `DeltaEntityProvider` via `foreachBatch` so every micro-batch runs through
  the batch `merge_to_entity` path — SCD1 upserts and SCD2 version chaining
  (including the per-merge `entity.before_merge`/`entity.after_merge`
  signals) behave identically to batch merges. When the entity declares
  `scd.sequence_by`, each micro-batch is collapsed to the latest row per
  business key by sequence — the same latest-change-per-key convention the
  batch incremental path applies to change feeds — and entities explicitly
  tagged `scd.source_kind: change_feed` without `scd.sequence_by` are
  rejected at query start rather than failing mid-stream. Streaming pipes
  now merge instead of append when the output entity declares
  `merge_columns` and the sink provider supports streaming merges.
- `write.mode` entity tag (`append` | `merge`), honored by both the batch
  persist path and streaming pipes: `append` skips the merge even when the
  provider supports it (append-only fact tables no longer pay MERGE cost);
  `merge` makes the merge requirement explicit instead of a silent append
  fallback. Unset keeps the existing defaults.
- Parquet entity provider (`provider_type: "parquet"`, core): batch and
  streaming read/write of plain-parquet datasets via native Spark, with
  partitioned writes and destination ensuring. For interchange at solution
  boundaries — no transaction log, so no merge and no safe retry; Delta
  remains the provider for internal pipeline storage.
- `skip_dependents` error strategy for DAG execution: on pipe failure, only
  its transitive consumers are skipped (reported with
  `reason: upstream_failed`) while independent branches keep running —
  notebook-DAG (`runMultiple`) dependent-skipping semantics. Select via
  `kindling.execution.error_strategy: skip_dependents` or
  `ErrorStrategy.SKIP_DEPENDENTS`.
- Per-pipe retry for DAG execution: `kindling.execution.retry.attempts` /
  `interval_seconds` (run-level) and `kindling.execution.pipes.<pipeid>.retry.*`
  (per-pipe), with `retry_attempts`/`retry_interval_seconds` as just-in-time
  parameter overrides. Emits `orchestrator.pipe_retrying`; records `attempts`
  on pipe results; warns when retry targets a non-merge-capable provider
  (retried appends are at-least-once).
- New guide: [Migrating from notebook-based execution (`runMultiple`)](docs/guide/migrating_from_runmultiple.md)
  for Synapse/Fabric users moving notebook DAGs to Kindling pipes.
- Added `kindling env update` to refresh Kindling wheels and the local
  devcontainer package index in place, so domain projects can update Kindling
  packages without rebuilding the whole devcontainer.
- Generated packages now include a `poe update-kindling` task that runs the new
  update workflow.

### Fixed

- Watermarking incremental-read correctness: signal-aspect refactor with
  opaque cursors; a failed persist discards the pending cursor so retries
  re-read the same input slice.
- Runtime platform notebook services, live-verified on all three platforms:
  Databricks `get_notebook` no longer corrupts content (it wrapped the whole
  export in one blob cell and dropped every newline) and
  `create_or_update_notebook` no longer silently imports empty notebooks,
  creates parent folders, and accepts bare names; Synapse
  `create_or_update_notebook` now builds Azure SDK models (it previously
  crashed before reaching the API); Fabric notebook create/update uses the
  real item endpoints with status-aware long-running-operation polling
  (async failures were previously reported as success).
- Runtime feature discovery: `kindling.features.discovery: "false"`
  off-switch, `SHOW CATALOGS` short-circuit on large metastores, and the
  DBR version regex.
- Delta ensure-on-write only runs when there is a schema to ensure from.
- Databricks temporal execution homed in the databricks extension.
- Standalone/local Spark sessions created by Kindling now add the available
  Hadoop Azure support JARs from `/tmp/hadoop-jars` to `spark.jars`, not only
  the custom Azure CLI auth provider JAR. This lets local `abfss://` access load
  the Azure Blob FileSystem classes during `kindling app run`.

## [0.10.35] - 2026-06-25

### Fixed

- Updated public GitHub release downloads to use `requests` with HTTPS-only
  release asset URLs so the runtime deploy fix passes the release security
  scan. This supersedes the failed `v0.10.34` tag.

## [0.10.34] - 2026-06-25

### Fixed

- `kindling runtime deploy --source github:*` now downloads public Kindling
  release assets directly from `sep/spark-kindling-framework`, without
  requiring the GitHub CLI or inspecting the caller's current git repository.

### Added

- Added implementation proposals for ABFSS local auth via Azure CLI, CSV
  provider writes, and renaming `load_local` to `load_workspace_packages`.

## [0.10.33] - 2026-06-23

### Changed

- Removed legacy agent workflow references from repository agent instructions and
  quickstart documentation.
- Removed legacy agent tooling setup from the devcontainer configuration.
- Added the app-local settings overlay proposal document.

## [0.10.31] - 2026-06-19

### Fixed

- `kindling app run` (standalone) no longer incorrectly fetches lake-reqs packages from the
  lake by default. Packages already installed in the current Python environment (editable,
  regular pip, devcontainer pre-install, or published to a registry) are used as-is without
  contacting the lake. This fixes a regression where `artifacts_storage_path` was required
  even for purely local runs.
- Fixed `lake-reqs.txt` spec parsing to correctly handle `~=` and `!=` version operators.
- Added `--load-lake` flag to `kindling app run` (standalone-only) to explicitly force
  downloading lake-reqs packages from the lake regardless of local install state.

## [0.10.32] - 2026-06-23

### Changed

- App runtime configuration now treats `app.yaml` as manifest-only and loads runtime
  settings from `settings.yaml`, `settings.<platform>.yaml`, then
  `settings.<env>.yaml`, with environment overlays taking precedence. Legacy
  `platform_<platform>.yaml`, `env_<env>.yaml`, and app-local `app.<target>.yaml`
  files remain fallback inputs for compatibility, but new config should use the
  dot-style `settings.*.yaml` names.
- `runtime publish` renamed to `runtime deploy`. Same behavior; the new name is consistent with
  `app deploy`, `package deploy`, and `workspace deploy`. **Breaking change**: update scripts
  and CI pipelines to use `kindling runtime deploy`.
- `workspace init` — completely new behavior. Now initializes the platform workspace by deploying
  `settings.yaml` + overlays to `{base}/config/` in storage. With `--notebook-bootstrap`, also
  generates and imports notebook bootstrap files into the platform workspace.
  **Breaking change**: the old local file generation behavior (`--output-dir`, `--force`) is
  removed. Use `--notebook-bootstrap` with `--workspace` for notebook import.
- `workspace deploy` — now deploys config only (`settings.yaml` + overlays to `{base}/config/`).
  Wheel and bootstrap script deployment have been removed; use `kindling runtime deploy` for those.
  **Breaking change**: `--dist-dir`, `--skip-wheels`, `--skip-bootstrap-script`,
  `--allow-missing-bootstrap-script`, `--create-notebooks`, and `--workspace` options removed.
- `workspace check` removed. Use `kindling env check --platform <platform>` instead.

- `app package <APP_NAME>` — positional is now the app name; kindling discovers `apps/<name>/`
  by convention (kebab→snake normalized). `--local-folder` overrides for non-standard layouts.
  **Breaking change** for callers passing a path as the positional; use `--local-folder` instead.
- `app deploy <APP_NAME>` — positional is now the app name with the same convention lookup.
  `--local-folder` overrides; `--kda-package` deploys a pre-built archive. Removed the
  "must supply --local-folder or --kda-package" requirement; passing just the app name is now
  the normal workflow. `--app-name` renamed to `--remote-name` to avoid naming conflict.
- `app run <APP_NAME>` — standalone mode now does convention lookup (`apps/<name>/`) instead of
  requiring a path. `--local-folder` overrides. Remote mode (`--platform`) no longer auto-deploys
  when a local path is detected — the app must be deployed first with `kindling app deploy`.
  Added `--local-folder` option (standalone-only; errors if used with `--platform`).
  **Breaking change** for `app run <path> --platform` workflows; split into separate
  `app deploy` + `app run` calls.
- `app cleanup <APP_NAME>` — removed `--local-folder` and `--kda-package` flags; APP_NAME is now
  required. Pass the app name directly instead of inferring it from a path.
  **Breaking change** for callers using `--local-folder` or `--kda-package`; pass the name directly.
- `package deploy <PACKAGE_NAME>` — positional is now the package name; kindling discovers
  `packages/<name>/` by convention. `--local-folder` overrides.
  **Breaking change** for callers passing a path as the positional; use `--local-folder` instead.

## [0.9.30] - 2026-05-26

### Added
- `kindling._runner` module — standalone app execution now initializes the Kindling framework before running the app entrypoint, mirroring the remote `DataAppManager` execution path; `app.py` no longer contains bootstrap code
- `DataAppConstants.LOCAL_SETTINGS_FILE` — `settings.local.yaml` is excluded from KDA packages at build time

### Changed
- App and package scaffolds no longer generate a `config/` subdirectory; config files now sit at the project root following ASP.NET-style layering: `settings.yaml` (base, deployed) and `settings.local.yaml` (local dev overlay, gitignored, never deployed)
- Generated `app.py` is now a passive entrypoint — contains only domain module import stubs and a startup log line; all framework initialization is owned by the CLI runner or platform
- `kindling app run` (standalone) invokes `python -m kindling._runner` with explicit `--config` args resolved from the app directory instead of passing `KINDLING_CONFIG_DIR`
- Default app entrypoint renamed from `main.py` to `app.py` across CLI, `DataAppConstants`, and `notebook_framework`; `LEGACY_ENTRY_POINT` constant and silent `main.py` fallback removed

### Fixed
- `settings.local.yaml` excluded from KDA packaging in both single-platform and multi-platform build paths

### Added
- `scd.close_on_missing: "true"` tag on SCD2 entities — rows absent from the source batch are logically closed (`__effective_to = now`, `__is_current = false`) via `whenNotMatchedBySourceUpdate`; safe for full-snapshot sources (TASK-20260430-004, #83)
- `scd.optimize_unchanged: "true"` tag on SCD2 entities — SHA2-256 hash comparison over tracked columns replaces per-column change detection; reduces merge cost for large dimensions with many unchanged rows (TASK-20260430-004, #84)
- CLI Quick Start section in `README.md` — covers `kindling new` → `poetry install` → `kindling run` end-to-end local workflow (TASK-20260430-003)
- Local Development section in `docs/setup_guide.md` — placed before cloud-platform sections so developers can run locally without cloud credentials (TASK-20260430-003)
- Sentinel-based missing-key debug logging to `DynaconfConfig.get()` in `spark_config.py` — surfaces misconfigured keys without raising at call-time (TASK-20260430-003)
- Entity guidance comments to scaffold entity templates (`records.medallion.py.j2`, `records.minimal.py.j2`) — inline hints steer developers toward correct field/SCD patterns (TASK-20260430-003)
- `TestKindlingPipeExecution` class to scaffold integration test template — gives generated projects a ready-to-run pipe execution test (TASK-20260430-003)
- `kindling run <pipe_id>` CLI command — executes a registered pipe locally after auto-discovering `app.py`; no cloud credentials required (TASK-20260430-001, #85)
- `kindling validate` CLI command — statically validates entity/pipe definitions without starting Spark (TASK-20260430-001, #85)
- `KindlingNotInitializedError` — raised with actionable message when entity/pipe decorators fire before `initialize()` (TASK-20260430-001, #85)
- `DataEntities.reset()` and `DataPipes.reset()` public classmethods for test isolation (TASK-20260430-001, #85)

### Changed
- Scaffold `env.local.yaml` now uses `provider_type: memory` by default — no Azure credentials needed to run locally (TASK-20260430-001, #85)
- Scaffold `conftest.py` includes `reset_kindling` fixture using public reset API (TASK-20260430-001, #85)
- Scaffold poe tasks use plain `pytest` instead of `kindling test run` wrapper (TASK-20260430-001, #85)
- `kindling env check` auto-probes `config/settings.yaml` before `settings.yaml` when `--config` is omitted (TASK-20260430-001, #85)
- `kindling new` next-steps output uses a single `cd` command (TASK-20260430-001, #85)

### Fixed
- Stale pinned version `0.6.6` removed from `docs/intro.md` — version line deleted to avoid future staleness (TASK-20260430-003)
- 29 bare `print()` calls in `data_apps.py` routed to `self.logger`; additional bare prints fixed in `spark_session.py` and `notebook_framework.py` — structured log output consistent throughout (TASK-20260430-003)
- `NullWatermarkEntityFinder` auto-bound in standalone platform — `kindling run` now executes pipes in scaffolded projects without Azure credentials (TASK-20260430-002, #87)
- Removed/routed 109 unconditional debug `print()` calls in `bootstrap.py` and `spark_config.py` — CLI output is clean by default (TASK-20260430-002, #87)
- Added `--env` option to `kindling validate` for consistency with `kindling run` (TASK-20260430-002, #87)
- `kindling new` next-steps now annotates `.env` copy step as optional for local-first dev (TASK-20260430-002, #87)
- `spark-kindling-cli` uncommented as dev dependency in scaffold `pyproject.toml.j2` — `kindling` available after `poetry install` (TASK-20260430-002, #87)

### Tests
- Added `test_di_wiring_standalone.py` — real DI construction test with no mocking; catches abstract-class binding gaps before they ship (TASK-20260430-002, #88)
