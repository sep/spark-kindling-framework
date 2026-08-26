# Apache Iceberg Entity Provider for Fabric and Databricks

**Status:** Proposed. No framework or platform code changes made yet.
**Created:** 2026-08-24
**Scope:** A `provider_type: "iceberg"` entity provider on Microsoft Fabric
and Databricks. Synapse and standalone Spark are explicitly out of scope for
the first release.
**Related:** `fan_in_upsert_pipes.md` (provider-independent patch-upsert
semantics), `kindling_core_runner_split.md` (provider contracts versus runner
persistence), `obsolete/entity_provider_roadmap.md` (provider capability
rubric), and `contributing/entity_providers.md`.

## Decision

Add one logical Iceberg provider with platform-selected capabilities rather
than pretending Fabric and Databricks expose the same storage engine:

1. **Databricks:** native, catalog-addressed Unity Catalog managed Iceberg.
   This is the first writable implementation and supports batch reads,
   overwrite/append, destination creation, and keyed SCD1 merge after system
   tests prove each operation on the selected Databricks Runtime.
2. **Fabric:** Iceberg-origin tables surfaced by OneLake's Iceberg-to-Delta
   metadata virtualization. Kindling reads these through its existing Delta
   execution path, but the entity remains declared as `iceberg` so its origin,
   limitations, observability, and future write policy are not lost. The first
   implementation is read-only.

Do not initially ship a generic `spark.read.format("iceberg")` provider across
both platforms. That would conceal materially different catalogs, commit
coordination, table ownership, and feature support behind identical-looking
code.

## Why a distinct provider is still useful on Fabric

Fabric automatically generates a virtual Delta log for an Iceberg V2 table
written into OneLake or exposed through a table shortcut. Fabric workloads then
consume the table as Delta. The current virtualization has important limits:
only Iceberg V2 inputs, latest-version conversion, fewer than 5,000 source
commits, roughly 5-second-to-2-minute metadata conversion latency, incomplete
type and partition-transform coverage, and location-sensitive absolute paths
inside Iceberg metadata.

Treating such an entity as ordinary `provider_type: "delta"` would work for a
basic read but would falsely imply that Kindling owns a Delta table and may
write, migrate, optimize, enable Delta features, or merge into it. A distinct
Iceberg declaration lets the Fabric adapter delegate safe reads while blocking
operations that could mutate the virtual representation or create two writers.

Source: [Use Iceberg tables with
OneLake](https://learn.microsoft.com/en-us/fabric/onelake/onelake-iceberg-tables).

## Platform capability matrix

| Capability | Databricks managed Iceberg | Fabric virtualized Iceberg |
|---|---|---|
| Addressing | Unity Catalog three-part name | Lakehouse table/shortcut surfaced as Delta |
| Batch read | Yes, native catalog table | Yes, delegated Delta read |
| Exists | Yes, catalog lookup | Yes, delegated catalog/path lookup plus validation |
| Create destination | Yes, `CREATE TABLE ... USING ICEBERG` | No in phase 1 |
| Overwrite | Candidate, system-test required | No |
| Append | Candidate, system-test required | No |
| Keyed SCD1 merge | Candidate, SQL `MERGE` path | No |
| SCD2 merge | Deferred | No |
| Streaming read | Deferred pending snapshot/offset contract | No initial guarantee |
| Streaming append/merge | Deferred | No |
| Time travel/version API | Deferred behind a format-neutral version contract | No; latest converted metadata only |
| Schema convergence | Additive subset after live validation | Observe/report only |
| Partition evolution | Deferred | Not exposed through Kindling |

Databricks supports Unity Catalog managed tables backed by Delta or Iceberg and
creates managed Iceberg with `USING ICEBERG`. Managed Iceberg requires Unity
Catalog and has platform-specific limitations, including predictive
optimization requirements and unsupported Iceberg types/features. Iceberg V3
features require Databricks Runtime 18 LTS or newer.

Sources: [What is Apache Iceberg in Databricks?](https://docs.databricks.com/aws/en/iceberg),
[Unity Catalog managed tables](https://docs.databricks.com/aws/en/tables/managed),
and [Iceberg V3 features](https://docs.databricks.com/aws/en/iceberg/iceberg-v3).

## Declaration contract

The public declaration remains platform-neutral:

```python
@DataEntities.entity(
    entityid="sales.orders_iceberg",
    name="orders_iceberg",
    merge_columns=["order_id"],
    schema=ORDERS_SCHEMA,
    tags={
        "provider_type": "iceberg",
        "provider.table_name": "main.sales.orders_iceberg",
    },
)
class OrdersIceberg:
    pass
```

Optional provider tags:

| Tag | Meaning |
|---|---|
| `provider.table_name` | Catalog identifier; required for Databricks writes |
| `provider.path` | Fabric shortcut/table path when catalog resolution is insufficient |
| `provider.access_mode` | `catalog` by default; `storage` is read-only in phase 1 |
| `provider.format_version` | Requested Iceberg version, initially constrained by platform |
| `provider.fabric.virtualized` | Explicit acknowledgement for a Fabric virtual-Delta source |
| `read_only` | Existing Kindling guard; mandatory for Fabric in phase 1 |

Platform services resolve the implementation and feature flags. Shared pipe or
provider code must not branch directly on `fabric` versus `databricks`.

## Provider design

Introduce `IcebergEntityProvider` implementing the minimum common surface:

```text
BaseEntityProvider
  read_entity
  check_entity_exists
```

The provider delegates to a platform strategy selected during initialization:

```text
IcebergEntityProvider
  -> DatabricksManagedIcebergStrategy
  -> FabricVirtualizedIcebergStrategy
```

The Databricks strategy additionally implements or exposes:

```text
WritableEntityProvider
DestinationEnsuringProvider
merge_to_entity (SCD1 only in phase 1)
```

Because Kindling currently discovers optional capabilities with Python
interface checks, a single object cannot honestly advertise writable methods
on Databricks while being read-only on Fabric. Resolve this by registering a
platform-specific concrete provider under the same `iceberg` provider type,
or by extending the registry to accept runtime capability descriptors. The
first option is smaller and consistent with the existing platform-service
boundary.

### Databricks implementation

- Require Unity Catalog and catalog addressing for managed writes.
- Create tables with generated DDL using `USING ICEBERG`; do not install or
  configure a separate Iceberg Spark catalog inside Databricks.
- Read with `spark.table(qualified_name)` so Unity Catalog remains the authority.
- Use DataFrame V2 writes or SQL only after system tests establish the supported
  behavior on the minimum DBR. Avoid private JVM APIs.
- Implement SCD1 through explicit-column SQL `MERGE INTO`, quoting identifiers
  and preserving the fan-in owned-column contract if that proposal lands.
- Defer SCD2 until row-level operations, history semantics, and Kindling's
  current-view companion behavior have parity tests.
- Do not expose Delta-only configuration such as CDF, liquid clustering, Delta
  protocol properties, or `DeltaTable` APIs.

### Fabric implementation

- Require `read_only: "true"` and `provider.fabric.virtualized: "true"` in the
  initial release so users acknowledge that this is an Iceberg-origin table
  projected through virtual Delta metadata.
- Resolve the Fabric lakehouse table or shortcut, then delegate batch reading
  and existence checks to a restricted Delta read adapter.
- Never call Delta destination creation, schema migration, optimize, clustering,
  CDF enablement, overwrite, append, or merge on the virtual table.
- Emit structured diagnostics containing the source format, virtualization
  mode, and known conversion lag. A missing or stale virtual Delta log should
  report a virtualization problem rather than "Delta table not found."
- Treat the view as eventually refreshed metadata, not a transactional Iceberg
  catalog connection. Kindling cannot promise immediate read-after-write from
  an external Iceberg producer.

OneLake's Iceberg REST Catalog endpoint currently documents read-only metadata
operations. It may become a better implementation seam later, but it is not a
phase-1 write path. Source: [OneLake table API
overview](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/table-apis-overview).

## Merge, fan-in, and retry semantics

The existing `fan_in_upsert_pipes.md` proposal requires provider parity before
multiple partial-row producers are allowed. Iceberg must not inherit Delta's
behavior by assumption:

- Databricks SCD1 merge must use explicit key, update-column, and insert-column
  expressions and receive dedicated partial-row tests.
- An Iceberg entity is ineligible for `entity.fan_in` until those tests pass and
  the proposal's capability gate names Iceberg explicitly.
- Fabric virtualized Iceberg is read-only and therefore never a fan-in target.
- Append retries are not declared idempotent. Merge retries may be treated as
  convergent only after duplicate-source-key and interrupted-commit tests pass.

## Schema convergence and migrations

Phase 1 supports:

- validating the declared schema against a read table on both platforms;
- creating a Databricks managed table from a declared schema;
- additive Databricks schema changes only after live tests demonstrate safe,
  idempotent `ALTER TABLE` behavior.

Phase 1 does not support:

- format conversion between Delta and Iceberg;
- moving or copying Iceberg directories;
- rewriting Fabric shortcut metadata;
- changing Iceberg format versions through `kindling migrate`;
- partition-spec evolution, destructive column changes, or table replacement.

The migration planner must fail with an actionable unsupported-operation result
rather than silently applying Delta migration logic to an Iceberg declaration.

## Packaging and runtime requirements

No Iceberg dependency belongs in the base Kindling wheel for the two initial
platforms:

- Databricks supplies its managed Iceberg implementation through the runtime.
- Fabric phase 1 reads the virtual Delta representation through its supplied
  Delta runtime.

The provider should be core code only if it can import without an Iceberg
client dependency and remain platform-neutral. Otherwise publish
`spark-kindling-ext-iceberg`; importing the extension registers
`provider_type: "iceberg"` at module scope, following the existing extension
contract. Do not add an Iceberg library to Fabric or Databricks clusters merely
to satisfy an import.

## Validation plan

### Contract and unit tests

- Registry resolves `iceberg` to the correct platform strategy.
- Fabric registration is read-only and rejects all write capability checks.
- Databricks registration advertises only implemented capabilities.
- Tags, identifiers, schema DDL, quoting, and error messages are deterministic.
- Delta-only tags on Iceberg entities fail validation.
- `fan_in` and SCD2 fail closed until their capability gates are enabled.

### Databricks system tests

Run on the minimum supported DBR and the primary DBR 18 LTS target:

1. ensure/create a managed Iceberg table;
2. overwrite and read round-trip;
3. append and retry behavior;
4. SCD1 insert/update/no-op merge;
5. partial-column merge preserving unowned columns;
6. additive schema convergence;
7. concurrent-writer conflict behavior;
8. cleanup through catalog APIs;
9. verify Spark Connect/shared access-mode behavior without `_jvm` or `_jsc`.

### Fabric system tests

Provision or reference an Iceberg V2 table, create a OneLake table shortcut,
and verify:

1. the table appears through virtual Delta metadata;
2. Kindling reads it by catalog name and, if supported, by path;
3. schema and representative timestamp/decimal/binary mappings are correct;
4. existence checks distinguish missing table from pending conversion;
5. every write, merge, migration, and ensure attempt fails before Spark writes;
6. a new external Iceberg commit becomes visible within a documented polling
   window;
7. unsupported partition transforms and excessive commit history produce a
   useful diagnostic.

## Delivery phases and estimate

### Phase 0 — live spikes and capability lock (2–4 days)

- Create and mutate a managed Iceberg table on the selected Databricks runtimes.
- Exercise a real OneLake Iceberg V2 shortcut and capture conversion behavior.
- Decide core versus extension packaging from import/runtime constraints.

### Phase 1 — read support on both platforms (3–5 days)

- Declaration validation, registry integration, both read strategies, errors,
  documentation, and system tests.

### Phase 2 — Databricks batch writes and ensure (3–5 days)

- Managed-table DDL, overwrite/append, schema validation, cleanup, and retry
  characterization.

### Phase 3 — Databricks SCD1 merge (3–6 days)

- Explicit merge compiler, partial-column semantics, concurrency/retry tests,
  and fan-in capability decision.

Expected first production scope: **11–20 engineering days**, with elapsed time
dominated by platform provisioning and system-test feedback. Streaming, SCD2,
Iceberg REST writes, and native Fabric Iceberg writes are separate follow-ups.

## Alternatives rejected

### Configure every platform as generic OSS Iceberg

Rejected because it bypasses Unity Catalog's managed-table contract on
Databricks and Fabric's virtualized-table integration, adds runtime/JAR
coordination, and creates an unsupported multi-writer risk.

### Use `provider_type: "delta"` for Fabric and expose no Iceberg declaration

Rejected because it loses provenance and makes unsafe Delta write and migration
capabilities appear available. Internal read delegation is appropriate; public
semantic equivalence is not.

### Make Fabric writable through the virtual Delta log

Rejected for phase 1. Virtualization documents interoperability and conversion
lag, not a Kindling-safe bidirectional commit protocol. Until a live test and a
platform contract establish writer ownership and conflict behavior, the safe
capability is read-only.

### Start with streaming and SCD2 parity

Rejected because it would make the first release hinge on the least portable
semantics. Batch reads plus Databricks SCD1 cover a useful boundary while
preserving honest capability discovery.

## Acceptance criteria

- `provider_type: "iceberg"` resolves on Fabric and Databricks without adding a
  Spark/Iceberg dependency to managed runtimes.
- Fabric Iceberg entities are observably and enforceably read-only.
- Databricks managed Iceberg uses Unity Catalog and passes live create,
  overwrite, append, read, and SCD1 tests before those capabilities ship.
- No implementation touches private Spark JVM handles.
- Delta-only features never run against an Iceberg declaration.
- Unsupported migration, streaming, SCD2, and fan-in operations fail closed
  with actionable diagnostics.
- Documentation states exact supported Iceberg versions, Databricks runtimes,
  Fabric virtualization limitations, and retry guarantees.
