# Databricks Workspace Diff Report

**Generated**: 2026-02-12
**Original (Dev)**: `adb-3961952571126106.6` — the configured dev workspace
**New (Target)**: `adb-7405613692056733.13` — brand new, needs setup

---

## Executive Summary — What Needs to Be Set Up

The new workspace is essentially empty (factory defaults). Here's everything that was configured in the original dev workspace that needs to be replicated:

| Category | Original | New | Action Required |
|----------|----------|-----|-----------------|
| Unity Catalog - Custom Catalogs | `medallion` | (none) | **Create catalog** |
| UC Schemas (medallion) | bronze, default, gold, silver | N/A | **Create after catalog** |
| UC Volumes (medallion.default) | 6 volumes (config, data_apps, logs, packages, scripts, temp) | None | **Create volumes** |
| UC External Locations | 5 (artifacts, bronze, gold, landing, silver) | 1 (default only) | **Create 5 locations** |
| UC Storage Credentials | 1 (managed identity → access connector) | 1 (default only) | **Create credential** |
| Clusters | 2 interactive + ~30 job clusters | None | **Create interactive clusters** |
| Secret Scopes | `sepdev-adls-scope` | None | **Create scope** |
| Service Principals | 3 (sep-kindling-nonprod, data-app-poc, SEP Dev SP) | None | **Add SPs** |
| Groups | admins (2), users (5) | admins (1), users (1) | **Add members + SPs** |
| DLT Pipelines | 3 (test, staging, ingestion) | None | **Recreate if needed** |
| Workspace Notebooks | `/sep/` tree with notebooks | Empty | **Copy/recreate** |
| Jobs | ~11 jobs | None | **Recreate needed ones** |
| SQL Warehouses | 1 (Serverless Starter) | 1 (Serverless Starter) | Already exists |
| Workspace Config | `enableDbfsFileBrowser: true` | `enableDbfsFileBrowser: None` | **Enable if needed** |
| DBFS Directories | 17 dirs (bronze, silver, gold, mnt, etc.) | 6 dirs (defaults only) | **Create dirs** |

---

## 1. Unity Catalog — Storage Credential

### Original
- **Name**: `datalake_azuremanagedidentity_1751315960926`
- **Type**: Azure Managed Identity
- **Access Connector**: `/subscriptions/aa752f0d-e1e1-4d58-b73c-a907837e67ae/resourceGroups/sep-rg-dataint-dev/providers/Microsoft.Databricks/accessConnectors/sep-acad-db-dev`
- **Owner**: jtdossett@sep.com

### New
- **Name**: `sep_db_framework_poc_wcus` (auto-created default)
- **Access Connector**: `/subscriptions/aa752f0d-e1e1-4d58-b73c-a907837e67ae/resourcegroups/databricks-rg-sep-db-framework-poc-wcus-2cvrg7qkx3462/providers/Microsoft.Databricks/accessConnectors/unity-catalog-access-connector`

### Action
Create a new storage credential pointing to the **same** access connector (`sep-acad-db-dev`) that has access to `sepstdatalakedev` storage account, **OR** grant the new workspace's access connector (`unity-catalog-access-connector`) the `Storage Blob Data Contributor` role on `sepstdatalakedev`.

---

## 2. Unity Catalog — External Locations

### Original (5 locations, all on `sepstdatalakedev`)

| Name | URL | Credential |
|------|-----|------------|
| `artifacts` | `abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/` | `datalake_azuremanagedidentity_*` |
| `bronze` | `abfss://bronze@sepstdatalakedev.dfs.core.windows.net/` | `datalake_azuremanagedidentity_*` |
| `gold` | `abfss://gold@sepstdatalakedev.dfs.core.windows.net/` | `datalake_azuremanagedidentity_*` |
| `landing` | `abfss://landing@sepstdatalakedev.dfs.core.windows.net/` | `datalake_azuremanagedidentity_*` |
| `silver` | `abfss://silver@sepstdatalakedev.dfs.core.windows.net/` | `datalake_azuremanagedidentity_*` |

### New
Only the auto-created default location exists.

### Action
After creating/configuring the storage credential, create all 5 external locations pointing to the same ADLS containers.

---

## 3. Unity Catalog — Catalog & Schemas

### Original: `medallion` Catalog
- **Storage Root**: `abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/catalog`
- **Owner**: jtdossett@sep.com
- **Schemas**:
  - `bronze` (owner: jtdossett@sep.com)
  - `default` (owner: jtdossett@sep.com)
  - `gold` (owner: jtdossett@sep.com)
  - `silver` (owner: jtdossett@sep.com, storage root: `abfss://silver@sepstdatalakedev.dfs.core.windows.net/`)
  - `information_schema` (auto)

### New
- Only has `sep_db_framework_poc_wcus` (auto-created) and `system`/`samples`
- No `medallion` catalog

### Action
```sql
-- After storage credential + external locations are configured:
CREATE CATALOG medallion
  MANAGED LOCATION 'abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/catalog';

CREATE SCHEMA medallion.bronze;
CREATE SCHEMA medallion.gold;
CREATE SCHEMA medallion.silver
  MANAGED LOCATION 'abfss://silver@sepstdatalakedev.dfs.core.windows.net/';
-- medallion.default is auto-created with the catalog
```

---

## 4. Unity Catalog — Volumes (`medallion.default`)

### Original (6 volumes)

| Name | Type | Storage Location |
|------|------|-----------------|
| `config` | EXTERNAL | `abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/config` |
| `data_apps` | EXTERNAL | `abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/data-apps` |
| `logs` | EXTERNAL | `abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/logs` |
| `packages` | EXTERNAL | `abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/packages` |
| `scripts` | EXTERNAL | `abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/scripts` |
| `temp` | MANAGED | (auto-assigned under catalog storage) |

### Action
```sql
CREATE EXTERNAL VOLUME medallion.default.config
  LOCATION 'abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/config';

CREATE EXTERNAL VOLUME medallion.default.data_apps
  LOCATION 'abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/data-apps';

CREATE EXTERNAL VOLUME medallion.default.logs
  LOCATION 'abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/logs';

CREATE EXTERNAL VOLUME medallion.default.packages
  LOCATION 'abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/packages';

CREATE EXTERNAL VOLUME medallion.default.scripts
  LOCATION 'abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/scripts';

CREATE VOLUME medallion.default.temp;
```

---

## 5. Secret Scopes

### Original
- **Scope**: `sepdev-adls-scope` (backend: DATABRICKS)
- Secrets: none currently listed (may be empty or access restricted)

### New
No secret scopes.

### Action
```bash
# Create via Databricks CLI or API
databricks secrets create-scope sepdev-adls-scope
# Then add any needed secrets
```

---

## 6. Service Principals & Groups

### Original

**Service Principals (3):**
| Display Name | App ID | Active |
|-------------|--------|--------|
| sep-kindling-nonprod | `dacf705a-49ca-4791-b6a3-c35448112241` | Yes |
| app-2mo687 data-app-poc | `3c054349-8d52-477e-9ef9-ac7e4b6b442d` | Yes |
| SEP Databricks Dev SP | `e3df02b5-3284-4c9d-8d07-a1e3b0a4f062` | Yes |

**Groups:**
| Group | Members |
|-------|---------|
| admins | Daniel Gomez, Jason Dossett |
| users | app-2mo687 data-app-poc, sep-kindling-nonprod, SEP Databricks Dev SP, Jason Dossett, Daniel Gomez |

### New
- **admins**: Jason Dossett only
- **users**: Jason Dossett only
- **No service principals**

### Action
1. Add the 3 service principals to the new workspace (via SCIM API or Databricks admin console)
2. Add Daniel Gomez (`djgomez@sep.com`) to `admins` group
3. Add all SPs and Dan to `users` group

---

## 7. Interactive Clusters

### Original (2 meaningful clusters, rest are terminated job clusters from system tests)

#### Cluster: Dan's Kindling Compute
| Setting | Value |
|---------|-------|
| Spark Version | `16.4.x-scala2.12` |
| Node Type | `Standard_D2ds_v6` |
| Workers | 0 (single node) |
| Auto-termination | 60 min |
| Data Security Mode | `SINGLE_USER` |
| Single User | `djgomez@sep.com` |
| Spark Config | `spark.databricks.cluster.profile=singleNode`, `spark.master=local[*, 4]` |
| Custom Tags | `ResourceClass=SingleNode` |
| Azure | ON_DEMAND |

#### Cluster: Kindling Cluster
| Setting | Value |
|---------|-------|
| Spark Version | `13.3.x-scala2.12` |
| Node Type | `Standard_D4pls_v6` |
| Workers | 0 (single node) |
| Auto-termination | 30 min |
| Data Security Mode | `USER_ISOLATION` |
| Spark Config | `spark.databricks.cluster.profile=singleNode`, `spark.master=local[*, 4]` |
| Custom Tags | `ResourceClass=SingleNode` |
| Azure | SPOT_WITH_FALLBACK |

### New
No clusters.

### Action
Create at least one interactive cluster matching the "Kindling Cluster" pattern. The ~30 job clusters are ephemeral (auto-created by system tests) and don't need to be recreated.

---

## 8. Workspace Notebooks (`/sep/` directory)

### Original — Full file tree

```
/sep/
├── run_ingestion_processing       [NOTEBOOK]
├── run_staging_processing         [NOTEBOOK]
├── run_curation_processing        [NOTEBOOK]
├── run_ingestion_dlts             [NOTEBOOK]
├── run_staging_dlts               [NOTEBOOK]
├── register_external_tables       [NOTEBOOK]
├── test_notebook                  [NOTEBOOK]
├── test_notebook_jtd              [NOTEBOOK]
├── databricks_python_lib_lister   [NOTEBOOK]
├── domain/
│   ├── domain_init                [NOTEBOOK]
│   ├── definitions                [NOTEBOOK]
│   ├── definitions_file_ingestion [NOTEBOOK]
│   ├── entity_providers           [NOTEBOOK]
│   ├── sep_conversions_common     [NOTEBOOK]
│   ├── sep_transforms_common      [NOTEBOOK]
│   ├── sep_transforms_staging     [NOTEBOOK]
│   ├── sep_transforms_curation    [NOTEBOOK]
│   └── trace_override             [NOTEBOOK]
├── utilities/
│   ├── bootstrap_environment      [NOTEBOOK]
│   ├── bootstrap_environment.py   [FILE]
│   ├── bootstrap_environment_old.py [FILE]
│   └── scripts/                   [DIR]
├── templates/
│   ├── template_ingestion.py      [FILE]
│   ├── template_staging.py        [FILE]
│   ├── template_curation.py       [FILE]
│   └── template_domain/           [DIR]
└── notebook_clones/
    ├── notebook_clone_ingestion.py [NOTEBOOK]
    ├── notebook_clone_staging.py   [NOTEBOOK]
    ├── notebook_clone_curation.py  [NOTEBOOK]
    └── domain/                    [DIR]
```

### New
Empty workspace (no `/sep/` directory).

### Action
Export the `/sep/` tree from the original workspace and import into the new. Options:
- Databricks workspace export/import API (bulk export as DBC archive)
- Git repo sync (if notebooks are tracked)
- Manual notebook-by-notebook recreation

---

## 9. DLT Pipelines

### Original (3 pipelines)

#### ingestion_pipeline
| Setting | Value |
|---------|-------|
| Source | Notebook: `/sep/run_ingestion_dlts` |
| Catalog | `medallion` |
| Schema | `bronze` |
| Cluster | `Standard_DS3_v2`, 1 worker |
| Mode | Development, non-continuous |

#### staging_pipeline
| Setting | Value |
|---------|-------|
| Source | Notebook: `/sep/run_staging_dlts` |
| Catalog | `medallion` |
| Schema | `silver` |
| Cluster | `Standard_DS3_v2`, 1 worker |
| Mode | Development, non-continuous |
| Config | `pipelines.create_table=true`, `spark.databricks.delta.liveTables.skipChangeCommits=true` |

#### test_pipeline
| Setting | Value |
|---------|-------|
| Source | Glob: `/Workspace/Users/djgomez@sep.com/test_pipeline/transformations/**` |
| Catalog | `medallion` |
| Schema | `bronze` |
| Cluster | `Standard_D2ds_v6`, 1 worker |
| Mode | Non-development, non-continuous |

### New
No DLT pipelines.

### Action
Recreate `ingestion_pipeline` and `staging_pipeline` after notebooks are migrated and the `medallion` catalog/schemas exist. The `test_pipeline` is Dan's personal test and can be recreated on-demand.

---

## 10. Jobs

### Original
- `process_domain_job` (creator: djgomez@sep.com) — likely a real/useful job
- ~10 test jobs (creators: `dacf705a-*` service principal) — system test artifacts, disposable

### New
No jobs.

### Action
Only `process_domain_job` might need recreation. The `databricks-test-job-*` and `test-job` entries are system test leftovers and can be ignored.

---

## 11. DBFS Directories

### Original (17 items at root)
```
/FileStore, /Volume, /Volumes, /Workspace, /bronze, /cluster-logs,
/databricks-datasets, /databricks-results, /dbfs, /external-location,
/gold, /mnt, /silver, /tmp, /user, /volume, /volumes
```
- `/FileStore/tables/` exists

### New (6 items at root — defaults only)
```
/Volume, /Volumes, /databricks-datasets, /databricks-results, /volume, /volumes
```

### Action
The extra DBFS directories (`/bronze`, `/gold`, `/silver`, `/mnt`, `/cluster-logs`, etc.) may be legacy mount points or DBFS paths. With Unity Catalog volumes now configured, these might not be needed. Verify nothing references DBFS paths directly before ignoring.

---

## 12. Workspace Configuration

| Setting | Original | New |
|---------|----------|-----|
| `enableDbfsFileBrowser` | `true` | `None` (disabled) |
| `enableTokensConfig` | `true` | `true` |
| `enableDeprecatedGlobalInitScripts` | `false` | `false` |

### Action
```bash
# Enable DBFS file browser if needed:
curl -X PATCH "$NEW_HOST/api/2.0/workspace-conf" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"enableDbfsFileBrowser": "true"}'
```

---

## Setup Checklist (Recommended Order)

1. [ ] **Storage Credential** — Create managed identity credential pointing to `sep-acad-db-dev` access connector (or grant new connector access to `sepstdatalakedev`)
2. [ ] **External Locations** — Create 5 locations (artifacts, bronze, gold, landing, silver) on `sepstdatalakedev`
3. [ ] **Catalog** — Create `medallion` catalog with managed location
4. [ ] **Schemas** — Create bronze, gold, silver schemas in medallion
5. [ ] **Volumes** — Create 6 volumes in medallion.default (5 external + 1 managed)
6. [ ] **Secret Scope** — Create `sepdev-adls-scope`
7. [ ] **Service Principals** — Add sep-kindling-nonprod, data-app-poc, SEP Databricks Dev SP
8. [ ] **Users & Groups** — Add djgomez@sep.com to admins, add all to users group
9. [ ] **Cluster** — Create Kindling Cluster (single node, 13.3.x or newer)
10. [ ] **Notebooks** — Export `/sep/` tree from original, import to new workspace
11. [ ] **DLT Pipelines** — Recreate ingestion_pipeline and staging_pipeline
12. [ ] **Jobs** — Recreate process_domain_job if needed
13. [ ] **Workspace Config** — Enable DBFS file browser if needed
14. [ ] **Update env vars** — Point `DATABRICKS_HOST` and `DATABRICKS_CLUSTER_ID` to new workspace for Kindling system tests
