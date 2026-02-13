# Terraform Grants Review — Best Practice & Least Privilege

**Date:** 2026-02-12
**Scope:** `iac/databricks/workspace/` — grants.tf, security.tf, dev.tfvars
**Workspace:** `adb-7405613692056733.13` (West Central US, dev)

---

## Structure & Pattern: GOOD

The `grants.tf` grouping strategy (one `databricks_grants` resource per securable, dynamic grant blocks inside) is correct and avoids the declarative overwrite trap. The `locals` blocks for grouping are clean. The default schema exclusion with API workaround is properly documented.

---

## Findings

### 1. `ALL_PRIVILEGES` on external locations — HIGH

**gold, silver, landing** all grant `ALL_PRIVILEGES` to both the SP and the devs group:

```hcl
# gold
privileges = ["ALL_PRIVILEGES", "EXTERNAL_USE_LOCATION"]
# silver
privileges = ["ALL_PRIVILEGES", "EXTERNAL_USE_LOCATION"]
# landing (SP only)
privileges = ["ALL_PRIVILEGES", "EXTERNAL_USE_LOCATION", "MANAGE"]
```

**Issues:**
- `ALL_PRIVILEGES` is a superset that includes `MANAGE`, `CREATE_EXTERNAL_TABLE`, `CREATE_EXTERNAL_VOLUME`, `CREATE_FOREIGN_SECURABLE`, `CREATE_MANAGED_STORAGE`, `READ_FILES`, `WRITE_FILES`, and `BROWSE` — plus any future privileges Databricks adds.
- Listing `EXTERNAL_USE_LOCATION` alongside `ALL_PRIVILEGES` is redundant — it's already included.
- On **landing**, `MANAGE` alongside `ALL_PRIVILEGES` is also redundant.

**Recommendation:** Replace `ALL_PRIVILEGES` with explicit grants. Typical least-privilege for a data pipeline:
- **landing**: `BROWSE`, `READ_FILES`, `WRITE_FILES` (maybe `CREATE_EXTERNAL_TABLE` for the SP)
- **bronze/silver/gold**: `BROWSE`, `READ_FILES`, `WRITE_FILES`, `CREATE_EXTERNAL_TABLE`
- Only add `MANAGE` if the principal needs to grant others access

---

### 2. SP catalog grants include `MANAGE` — MEDIUM

The `sep-kindling-nonprod` SP gets 16 privileges on the catalog including `MANAGE`:

```hcl
privileges = [
  "APPLY_TAG", "BROWSE", "CREATE_FUNCTION", "CREATE_MATERIALIZED_VIEW",
  "CREATE_MODEL", "CREATE_SCHEMA", "CREATE_TABLE", "CREATE_VOLUME",
  "EXECUTE", "MANAGE", "MODIFY", "READ_VOLUME", "SELECT",
  "USE_CATALOG", "USE_SCHEMA", "WRITE_VOLUME"
]
```

**Issues:**
- `MANAGE` on a catalog allows the SP to grant/revoke privileges for *any* principal on the catalog and all child objects. This is effectively catalog-owner-level access.
- `CREATE_SCHEMA` lets the SP create arbitrary schemas — probably unnecessary for a runtime pipeline SP.

**Recommendation:** Remove `MANAGE` unless the SP needs to administer grants programmatically. Remove `CREATE_SCHEMA` if schemas are Terraform-managed. A CI/CD SP typically needs:

```
USE_CATALOG, USE_SCHEMA, SELECT, MODIFY, CREATE_TABLE, EXECUTE,
BROWSE, READ_VOLUME, WRITE_VOLUME
```

---

### 3. Dev group has near-admin catalog access — LOW (acceptable for dev)

`sep-kindling-devs` gets the same set minus `MANAGE`. For a dev workspace this is reasonable, but worth noting for prod — you'd want to remove `CREATE_SCHEMA`, `CREATE_VOLUME`, etc. from non-admin groups.

---

### 4. `account users` with `BROWSE` on catalog — LOW (acceptable)

`BROWSE` lets any account user see catalog metadata. Fine for dev; consider removing in prod if the catalog has sensitive schema/table names.

---

### 5. Secret scope ACL — only one user has MANAGE — GOOD (note implicit access)

Only `jtdossett@sep.com` has `MANAGE` on `sepdev-adls-scope`. The admin (`djgomez@sep.com`) has no explicit ACL — they rely on being in the admins group (which gets implicit MANAGE). This works but is implicit. No action needed.

---

### 6. Volume grants are symmetric and reasonable — GOOD

`READ_VOLUME` + `WRITE_VOLUME` for both SP and devs on config, data_apps, packages, scripts. This is appropriate — no excess privileges.

---

### 7. Missing volume grants: `logs` and `temp` — NOTE

The `logs` and `temp` volumes have no grants. Only the catalog owner (creator) can access them. If the SP or devs need to write logs or use temp space, grants are missing. If they're unused, consider whether the volumes should exist at all.

---

### 8. Missing schema-level grants for bronze/silver/gold — NOTE

Only the `default` schema has explicit grants (just `USE_SCHEMA` for the SP). The `bronze`, `silver`, and `gold` schemas have **no schema-level grants** — access flows through the catalog-level `USE_SCHEMA` privilege. This works but means principals get the same schema access everywhere. For tighter control in prod, add per-schema grants.

---

### 9. `storage_credential_grants = []` — GOOD

No one has direct grants on the storage credential. Access flows through external locations, which is the correct pattern.

---

### 10. Entitlements are minimal — GOOD

- Only one user gets `allow_cluster_create` + `allow_instance_pool_create`
- Only one SP gets `workspace_access` + `databricks_sql_access`
- No one has all four entitlements — good restraint

---

## Summary

| # | Finding | Severity | Action |
|---|---------|----------|--------|
| 1 | `ALL_PRIVILEGES` on gold/silver/landing ext locations | **HIGH** | Replace with explicit privileges |
| 2 | `MANAGE` on catalog for SP | **MEDIUM** | Remove unless SP administers grants |
| 3 | `CREATE_SCHEMA` on catalog for SP | **LOW** | Remove if schemas are TF-managed |
| 4 | Redundant privileges (`ALL_PRIVILEGES` + `EXTERNAL_USE_LOCATION`) | **LOW** | Clean up redundancy |
| 5 | No grants on `logs`/`temp` volumes | **NOTE** | Add if needed, or remove volumes |
| 6 | No per-schema grants on bronze/silver/gold | **NOTE** | Fine for dev, tighten for prod |
| 7 | `account users` BROWSE on catalog | **NOTE** | Revisit for prod |

**Bottom line:** The structure is solid. The main cleanup would be replacing `ALL_PRIVILEGES` on external locations with explicit grants, and removing `MANAGE` + `CREATE_SCHEMA` from the SP's catalog privileges. Everything else is reasonable for a dev workspace.
