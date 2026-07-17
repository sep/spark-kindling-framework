# Entity Provider Roadmap — ADX, Cosmos DB, Parquet

**Status:** ADX and Cosmos DB shipped (July 2026); Parquet is the open item.
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

## Parquet — OPEN (next up; core provider)

Plain-parquet handling today is limited to file-ingestion *patterns*. Delta
covers internal storage — a Parquet provider is for the **boundaries**:
consuming parquet produced by external systems, publishing extracts for
consumers that can't read Delta.

**Decision: lives in core** (`entity_provider_parquet.py` beside the CSV
provider, auto-registered as `provider_type: "parquet"`), not an extension —
native Spark, zero new dependencies.

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
