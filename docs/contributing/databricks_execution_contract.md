# The Databricks execution contract: classic vs. Spark Connect

This document records a real production bug, its root cause, and the
corrected contract for two things that must stay true going forward:

1. Provider-op tracing must never call `getattr()` on an argument it didn't
   put there itself.
2. Kindling's own Databricks system tests must periodically exercise a
   genuine Spark-Connect-backed session, not only classic attached Spark.

## The bug

A Databricks app persisting to a Delta entity with `merge_columns` failed
inside the normal merge path
(`kindling.entity_provider_delta.SCD1MergeStrategy.apply()` ->
`delta_table.alias(...).merge(...).execute()`) with an unrelated-looking
error: `No api url found in local command context`.

The actual defect was in `kindling.trace_ops`. Every entity-provider op
(`merge_to_entity`, `write_to_entity`, `append_to_entity`, ...) is wrapped
for tracing at the registry chokepoint (`wrap_provider_ops`). The wrapper
tried to find an entity id for the span like this:

```python
def _entity_id_from_args(args, kwargs):
    for value in list(args) + list(kwargs.values()):
        entity_id = getattr(value, "entityid", None)   # <- the bug
        if entity_id is not None:
            return entity_id
    return None
```

`merge_to_entity(df, entity)` passes the DataFrame as `args[0]` alongside
the entity, so this ran `getattr(df, "entityid", None)` on every merge.

- On **classic PySpark**, an unrecognized attribute on a DataFrame just
  raises a plain `AttributeError`, which `getattr()`'s default silently
  swallows. Harmless.
- On a **Spark Connect** DataFrame, `__getattr__` treats an unrecognized
  name as a possible column reference and can issue a schema-resolution RPC
  to the Connect server to check. Outside an active session/API-URL
  context, that RPC doesn't fail with `AttributeError` — it raises
  something else entirely, which is what surfaced as "No api url found in
  local command context."

### The fix

`_entity_id_from_args` (`packages/kindling/trace_ops.py`) now reads the
argument's instance `__dict__` directly instead of calling `getattr()`:

```python
def _entity_id_from_args(args, kwargs):
    for value in list(args) + list(kwargs.values()):
        instance_dict = getattr(value, "__dict__", None)
        if not isinstance(instance_dict, dict):
            continue
        entity_id = instance_dict.get("entityid")
        if entity_id is not None:
            return entity_id
    return None
```

`__dict__` access is resolved by the normal attribute machinery *before*
`__getattr__` is ever consulted, so this never triggers a DataFrame's
`__getattr__` — classic or Spark Connect — regardless of what that
`__getattr__` does. It still finds `entityid` on anything that plainly has
it (like `EntityMetadata`, a plain dataclass).

**Contract going forward:** provider-op tracing (and any similar
best-effort introspection over "whatever arguments got passed to this op")
must identify shapes it cares about via `__dict__`/`isinstance` checks, never
via `getattr(value, name, default)`. A provider-op argument can be
*anything* the provider accepts — usually a DataFrame — and its attribute
resolution is not under this code's control.

Covered by `tests/unit/test_trace_ops.py::TestEntityIdFromArgsSafety`, which
constructs a `_SparkConnectLikeDataFrame` double whose `__getattr__` raises
on any unrecognized name (reproducing the hazard directly, without needing
a real Spark Connect session) and asserts it is never touched.

## Why the existing system test suite didn't catch it

Kindling has real Databricks system tests that deploy an app through the
actual ad hoc job / `kindling app run` bootstrap path and drive genuine
keyed Delta merges against Unity Catalog tables (for example
`tests/system/extensions/temporal/test_temporal_platform.py`, whose
`temporal-test-app` registers entities with real `merge_columns` and runs
under the framework's default — standard-level — tracing). So the gap was
not "no merge coverage" or "no UC coverage." Both existed.

The gap is the **cluster access mode**, which determines whether the job's
Python driver gets a classic attached `SparkSession` or a Spark-Connect-backed
one:

- **Single User** access mode: classic Spark, regardless of Databricks
  Runtime version.
- **Shared** (a.k.a. Standard / user-isolation) access mode: starting with
  DBR 14.0, the Python driver session is backed by Spark Connect by
  default — even on an ordinary dedicated, always-on all-purpose cluster.
  This is orthogonal to serverless vs. dedicated compute; a perfectly
  ordinary dedicated cluster in Shared mode hits this.

`DatabricksAPI._build_job_spec` (`packages/kindling_sdk/kindling_sdk/platform_databricks.py`)
resolves compute in this order:

```python
existing_cluster_id = (
    job_config.get("existing_cluster_id")
    or job_config.get("cluster_id")
    or self.default_cluster_id
)
```

CI always supplies `DATABRICKS_CLUSTER_ID` (see
`.github/workflows/databricks-system-tests.yml` /
`system-tests-only.yml`), so every system test job ran on that one
pre-existing cluster. The only place the code ever computed an explicit
`data_security_mode` was the UC-mode branch, and it always chose
`DataSecurityMode.SINGLE_USER` — and that computed `cluster_spec` is only
used in the `new_cluster=...` branch, which never runs when
`existing_cluster_id` is set. In other words: **no code path in Kindling has
ever requested a Shared-mode cluster**, and the actual access mode of the
CI test cluster is opaque infrastructure state the code doesn't observe or
verify. Whatever that cluster's access mode happens to be is the only thing
any system test has ever run under.

A dedicated production cluster running Shared mode — a common configuration
in Unity-Catalog-governed workspaces, since many admins restrict or disallow
Single User clusters — reproduces the failure every time, on a clean,
unremarkable ad hoc job. Kindling's own tests simply never asked for that
access mode.

## The regression test

`tests/system/extensions/databricks/test_delta_merge_spark_connect.py`
deploys `delta-merge-spark-connect-test` (a minimal app that registers a
Delta entity with `merge_columns` and calls `merge_to_entity(df, entity)`
twice — once to insert, once to exercise the matched-update /
unmatched-insert branches of `SCD1MergeStrategy`) through the real
`create_job`/`run_job` bootstrap path, with two new `job_config` options
added to `_build_job_spec` for exactly this purpose:

```python
job_config = {
    ...
    "force_new_cluster": True,           # bypass existing_cluster_id entirely
    "data_security_mode": "USER_ISOLATION",
    "spark_version": "15.4.x-scala2.12", # DBR >= 14.0
}
```

- `data_security_mode` is an explicit override that wins regardless of the
  UC-mode default (`_build_job_spec` now applies it after computing the
  UC-derived default, and it isn't gated behind `needs_uc_mode`).
- `force_new_cluster` skips the `existing_cluster_id`/`cluster_id`/
  `default_cluster_id` resolution so the test doesn't depend on — or
  disturb — the CI cluster's own configuration; it always spins up a fresh
  cluster with the requested access mode.

The test asserts the merge completes and, explicitly, that
`"No api url found in local command context"` never appears in the app's
stdout — the exact regression this fix guards against. It fails on the
pre-fix code (a real Spark Connect DataFrame's `entityid` lookup blows up)
and passes after it.

This does not change any other system test's compute — `existing_cluster_id`
resolution is untouched for every job config that doesn't opt into
`force_new_cluster`.

## Acceptance criteria recap

- A Databricks app with a Delta entity using `merge_columns` completes a
  real merge successfully — verified by the new regression test.
- No `DeltaMergeBuilder.execute()` -> `toPandas()` path is used for the Delta
  merge — unaffected; the bug was in tracing, not in `SCD1MergeStrategy`
  itself, which was never touched.
- No `"No api url found in local command context"` error occurs — asserted
  directly in the regression test's stdout check.
- Provider tracing does not access `.entityid` on DataFrames — fixed in
  `_entity_id_from_args`, covered by
  `tests/unit/test_trace_ops.py::TestEntityIdFromArgsSafety`.
- The new regression test covers the prior blind spot — it is the first
  Kindling system test to run a job on a Shared/user-isolation-mode
  cluster, forced independently of whatever the CI cluster happens to be.
