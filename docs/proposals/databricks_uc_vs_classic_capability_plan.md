# Databricks UC vs Classic Capability Plan

## Problem

Kindling currently treats Databricks as one storage/governance profile, but there are at least two materially different runtime models:

1. Unity Catalog capable workspaces with volumes and catalog-governed table names
2. Classic / non-UC workspaces, including gov-cloud scenarios where Unity Catalog or volumes are not available

The current bootstrap and system-test behavior mixes these models. That produces misleading failures:

- UC-enabled workspaces fall back to `dbfs:/tmp` and `ANY FILE` when a governed volume path would be better
- non-UC workspaces are evaluated against UC-oriented assumptions they cannot satisfy

## Goals

1. Make Databricks runtime capabilities explicit
2. Drive bootstrap and test behavior from those capabilities
3. Validate UC and non-UC Databricks separately
4. Avoid broad `ANY FILE` grants when a UC volume path is available

## Proposed Capability Flags

Use the existing static + computed feature pattern:

- Static override: `kindling.features.<key>`
- Computed runtime value: `kindling.runtime.features.<key>`

### Databricks runtime flags

- `kindling.runtime.features.databricks.uc_enabled`
  - `true` when Unity Catalog is available and active for the runtime
- `kindling.runtime.features.databricks.volumes_enabled`
  - `true` when `/Volumes/...` access is supported
- `kindling.runtime.features.databricks.any_file_required_for_bootstrap`
  - `true` when bootstrap staging falls back to URI/DBFS copy operations
- `kindling.runtime.features.databricks.name_mode_catalog_qualified`
  - `true` when default name-based behavior should assume `catalog.schema.table`

### Static override examples

- `kindling.features.databricks.uc_enabled`
- `kindling.features.databricks.volumes_enabled`
- `kindling.features.databricks.any_file_required_for_bootstrap`

These should be overrides, not the primary source of truth.

## Capability Detection

Populate the runtime flags during startup alongside the existing Databricks runtime version checks.

### Suggested detection order

1. Detect Databricks runtime version
2. Detect UC enablement
3. Detect volume support
4. Derive bootstrap staging mode from the above

### Suggested heuristics

`uc_enabled`

- positive signals:
  - catalog operations succeed for a known catalog
  - `current_catalog()` is meaningful and not a legacy-only fallback
  - Databricks runtime/workspace reports Unity Catalog support

`volumes_enabled`

- positive signals:
  - `dbutils.fs.ls("/Volumes")` or equivalent succeeds
  - `/Volumes/...` path operations parse and resolve correctly

`any_file_required_for_bootstrap`

- `true` when:
  - `volumes_enabled == false`
  - or no explicit governed volume staging path is configured

## Bootstrap Behavior Matrix

### Mode A: UC + volumes

Requirements:

- `uc_enabled = true`
- `volumes_enabled = true`

Bootstrap/package staging:

- stage wheels to a configured UC volume path
- avoid `dbfs:/tmp` fallback
- avoid raw `abfss://... -> dbfs:/...` copy flow

Data conventions:

- use name-based Delta table references by default
- assume `catalog.schema.table`
- use volume-backed temp/checkpoint/artifact paths

Permissions expected:

- `USE CATALOG`
- `USE SCHEMA`
- `READ VOLUME`
- `WRITE VOLUME`
- table privileges as needed

Permissions not preferred:

- `SELECT ON ANY FILE`
- `MODIFY ON ANY FILE`

### Mode B: Classic / non-UC

Requirements:

- `uc_enabled = false` or `volumes_enabled = false`

Bootstrap/package staging:

- use DBFS or reference/URI staging
- allow current `dbutils.fs.cp(remote, dbfs:/tmp/...)` fallback

Data conventions:

- allow explicit reference/path-oriented configuration
- do not require UC volumes
- do not assume `catalog.schema.table`

Permissions expected:

- cluster/workspace access
- `ANY FILE` privileges when URI/DBFS copy flow is used

## Test Matrix

Databricks should have two explicit system-test paths.

### Test 1: UC / volumes path

Purpose:

- validate modern Databricks behavior

Expected config shape:

- artifacts/temp/checkpoints on `/Volumes/...`
- Delta `forName`
- default Databricks name mapper semantics for `catalog.schema.table`

Expected runtime flags:

- `uc_enabled = true`
- `volumes_enabled = true`
- `any_file_required_for_bootstrap = false`

Should validate:

- bootstrap wheel staging via volumes
- name-based table creation/read/write
- checkpoint/temp path usage via volumes
- no dependency on `ANY FILE`

Suggested test names:

- `name_mapper_databricks_uc`
- `streaming_pipes_databricks_uc`

### Test 2: Classic / reference path

Purpose:

- validate gov-cloud / non-UC Databricks compatibility

Expected config shape:

- explicit reference/path-based staging
- DBFS or URI fallback where needed
- no UC assumptions

Expected runtime flags:

- `uc_enabled = false`
- `volumes_enabled = false`
- `any_file_required_for_bootstrap = true`

Should validate:

- bootstrap wheel staging without volumes
- explicit reference/path configuration
- correct non-UC fallback behavior

Suggested test names:

- `bootstrap_databricks_classic`
- `streaming_pipes_databricks_classic`

## Configuration Shape

### Suggested generic keys

- `kindling.storage.artifact_staging_root`
- `kindling.storage.temp_root`
- `kindling.storage.checkpoint_root`

### Databricks-specific overrides

- `kindling.databricks.volume_staging_root`
- `kindling.databricks.classic_temp_root`

### Example resolution logic

1. If explicit staging root is configured, use it
2. Else if `volumes_enabled`, use configured/default `/Volumes/...`
3. Else fall back to DBFS/reference staging and mark `any_file_required_for_bootstrap = true`

## Implementation Steps

1. Add runtime feature detection for:
   - `databricks.uc_enabled`
   - `databricks.volumes_enabled`
   - `databricks.any_file_required_for_bootstrap`
2. Refactor Databricks bootstrap wheel staging to:
   - prefer governed volume staging
   - isolate DBFS fallback in one helper
3. Add explicit Databricks system test variants:
   - UC / volumes
   - classic / reference
   - use `KINDLING_DATABRICKS_SYSTEM_TEST_MODE` to select the profile deterministically
4. Update docs/config reference with the new capability flags
5. Update CI test selection so Databricks mode is explicit in results

## Recommended Near-Term Policy

Until the split is implemented:

- UC-enabled Databricks should be configured to use volumes for temp/artifact/checkpoint paths
- non-UC Databricks should be treated as a separate support profile, not as a degraded UC profile
- `ANY FILE` grants should be considered a compatibility fallback, not the preferred default
