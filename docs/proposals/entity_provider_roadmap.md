# Entity Provider Roadmap — ADX, Cosmos DB, Parquet

**Status:** All three shipped (July 2026) — ADX (#170), Cosmos DB (#171), Parquet.
**Scope:** Tracks provider coverage against the framework's capability model.
Originally an evaluation of three candidates; two have since landed and this
doc records what shipped, what was learned live, and what remains.

## Evaluation Rubric

A provider is scored against the capability interfaces from
[entity_providers.md](../contributing/entity_providers.md), plus two
cross-cutting concerns:

| Dimension | Why it matters |
|---|---|
| `BaseEntityProvider` (read + exists) | Minimum bar for any provider |
| `WritableEntityProvider` (write/append) | Pipe output target |
| `StreamableEntityProvider` (stream read) | Streaming DAG source |
| `StreamWritableEntityProvider` (stream write) | Streaming DAG sink |
| `DestinationEnsuringProvider` | Pre-create tables/containers/paths |
| Merge support (`merge_to_entity`) | The persist path falls back to **append** when absent (`simple_read_persist_strategy.py`), which makes retries at-least-once. See the retry guardrails in [config_driven_execution_options.md](config_driven_execution_options.md). |
| Retry idempotency | Can a failed-then-retried persist double-write? Determines whether pipes targeting this provider can safely opt into Phase-2 retry. |

Current landscape: `delta` (all five + merge), `memory`, `csv`
(fixtures/local), `sql` (views, read-only), `eventhub` (stream-oriented),
`current_view`, plus the `kindling_ext_adx` and `kindling_ext_cosmos`
extensions below.

---

## Azure Data Explorer — SHIPPED (PR #170)

`packages/extensions/kindling_ext_adx`. Batch read, batch write/append, and
streaming write through the Kusto Spark connector; service-principal,
managed-identity, access-token, and Synapse linked-service auth;
`provider.option.*` passthrough. Live round-trip system test
(`tests/system/extensions/adx`, `poe test-extension --extension adx`)
verified against `sep-adx-dataint-dev`.

**Learned live** (encoded in the system test and provider docs):

- The connector's *read* path is KQL-only — `kustoTable` is a sink option;
  reading a table means passing its name as `kustoQuery`.
- Tables must be created by the connector (`tableCreateOptions`), not
  externally: queued ingestion fails permanently with "table could not be
  found" for tables the ingestion service's schema cache hasn't seen.
- Queued ingestion rides the ~5-minute default batching window even with
  `flushImmediately`; plan test/pipeline latency accordingly.

**Merge/retry**: ADX remains append-only — no merge, ingestion is
at-least-once, retried persists duplicate. Keep retry off for ADX-targeted
pipes (the Phase-2 warning for providers lacking `merge_to_entity` covers
this automatically); revisit `ingest-by:` extent-tag dedup keyed on
`persist_id` if demand appears.

**Remaining (optional, demand-driven)**: `DestinationEnsuringProvider` via
`.create-merge table`; stream *read* is not a natural fit and stays out.

### ADX (API-based) — core, added 2026-07-23

`packages/kindling/entity_provider_adx.py`, provider_type **`adx-api`**,
registered as a built-in; requires the `[adx]` extra (`azure-kusto-data`,
`azure-kusto-ingest`). Talks to ADX through the Kusto Python SDKs instead of
the JVM Spark connector, so it runs where the connector cannot: UC
shared/standard access mode clusters, serverless, standalone Python, or any
cluster where library installation is not an option.

Trade-off: data moves through the driver (query results and ingestion
batches materialize as pandas), so it suits solution *boundaries* — reference
data, config tables, modest extracts — while the connector extension remains
the choice for volume and streaming. Same append-oriented semantics
(queued ingestion, at-least-once, no merge; keep pipe retry off). It also
implements `DestinationEnsuringProvider` via `.create-merge table` from the
entity's declared schema — closing the connector's "table could not be
found" ingestion gap noted above. Auth: managed_identity (default),
service_principal (tags or `AZURE_*` env), azure_cli, access_token.

---

## Cosmos DB — SHIPPED (PR #171)

`packages/extensions/kindling_ext_cosmos`. Batch read (container scan or
`provider.query` Cosmos SQL), upsert writes, and streaming writes through
the Cosmos Spark connector (`azure-cosmos-spark_3-5_2-12`); service-principal
and master-key auth; `provider.option.*` passthrough. Live round-trip system
test (`tests/system/extensions/cosmos`, `poe test-extension --extension
cosmos`) verified against `sep-cosmos-dataint-dev` — including an explicit
upsert-by-id assertion (rewrite updates, never duplicates).

**The standout property**: writes are upserts by `(id, partition key)`
(`ItemOverwrite`), so **every write is idempotent** — a retried persist
converges instead of duplicating. Cosmos pipes are safe for Phase-2 retry
out of the box, arguably safer than the Delta append path.

**Learned live**:

- Cosmos **data-plane RBAC** is a hard prerequisite (e.g. *Cosmos DB
  Built-in Data Contributor*); control-plane roles are not sufficient.
- ServicePrincipal auth requires `subscription_id` + `resource_group` — the
  connector resolves account metadata through ARM and crashes opaquely
  without them; the provider validates up front.

**Remaining (future work)**:

- **Change-feed stream read** (`StreamableEntityProvider`) — a genuine new
  streaming *source* (today only Delta and EventHub); highest-value Cosmos
  follow-up.
- Formal `merge_to_entity` (essentially the default write) so the persist
  path treats Cosmos as merge-capable.
- Config-first throughput controls (`kindling.cosmos.*`) before pointing
  bulk writes at provisioned-throughput accounts; the serverless dev
  account sidesteps this today.

---

## Parquet — SHIPPED (core provider)

`packages/kindling/entity_provider_parquet.py`, auto-registered as
`provider_type: "parquet"` — native Spark, zero new dependencies. All five
interfaces implemented (streaming reads require the entity `schema`, per
Spark's file-source rule; partitioned writes honor `partition_columns`).
Round-trip verified by real-Spark integration tests
(`tests/integration/test_entity_provider_parquet_roundtrip.py`) including a
streaming file-source → file-sink hop — no cloud resources needed.

Positioning: Delta covers internal storage — the Parquet provider is for the
**boundaries**: consuming parquet produced by external systems, publishing
extracts for consumers that can't read Delta.

| Interface | Fit |
|---|---|
| Read | ✅ `spark.read.parquet(path)` via `provider.path` |
| Write/append | ✅ `mode("overwrite"/"append")`, honoring entity `partition_columns` |
| Stream read | ✅ file-source streaming (needs explicit schema — `EntityMetadata.schema` supplies it) |
| Stream write | ✅ file-sink streaming with checkpoint |
| Ensure destination | ✅ trivial (directory creation) |

**Merge/retry**: none, and worse than Delta's append path — no transaction
log, so a failed overwrite can leave a partially-written directory visible
and a retried append duplicates files. Docs must position parquet entities
as interchange-only; retry stays off.

**Effort**: ~1 day batch-only, ~2 with both streaming interfaces
(recommended: one PR with both — streaming is where parquet differentiates
from CSV). Verification needs no cloud resource: a temp-directory round-trip
integration test, with an optional ABFSS variant on existing storage config.
