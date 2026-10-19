# Surrogate Key Synthesis

**Status:** Proposed (revised 2026-10-19: SCD2 version and durable keys, UUID strategies,
custom generators, a canonical stored-key encoding, and the decisions recorded below)
**Created:** 2026-07-23
**Related:** [SCD2 support](obsolete/scd_type2_support.md) (explicitly deferred
surrogate keys — "a separate design decision"; this is it),
[SCD untracked columns](scd_untracked_columns.md) (keys are insert-only columns, a class
beside its tracked and untracked ones),
[migration desired-state design](migration_fit_for_purpose_findings.md)
(column-level declaration metadata), the tags-first convention
([declaration conventions](../contributing/declaration_conventions.md)).

## Summary

Kindling entities should be able to declare surrogate key columns and have the framework
synthesize them. Keys are declared on the entity (tags-first) and built from a single
canonical encoding that is safe to *store*. Four strategies are supported: `hash`
(default), `uuid5`, `uuid4` and `custom`, plus `identity` as a capability-gated
DDL alternative. SCD2 entities can carry two keys: a **version key** that includes the
row's effective date, and a **durable key** that stays the same across versions.

Two rules carry the design:

1. **A key is computed from the values that are actually stored in the row.** For SCD2
   that means after effective_from is known, so SCD2 keys are minted inside the merge,
   not in a before-persist hook.
2. **No merge ever matches on a surrogate key.** Matching stays on business keys, so
   key generation never has to happen before merge routing. That removes the ordering
   problem entirely.

## Motivating use cases

- **Dimensional modeling**: fact tables join dimensions on a stable,
  meaningless key rather than wide/composite/dirty business keys; SCD2
  dimensions need a *per-version* key so facts can pin the version that was
  current when the fact occurred, and a *durable* key for "current state" joins.
- **Data Vault-style hubs/links**: deterministic hash keys over business
  keys are the backbone of the pattern.
- **Join-width reduction**: replace multi-column joins with one column.
- **Pseudonymization**: a stable non-reversible stand-in for a natural key
  at solution boundaries.

### Field evidence (2026-10)

A domain team porting its dimensions to Kindling had its shared transforms generate a
`uuid()` id column and merged SCD1 on business columns. The SCD1 strategy's
`whenMatchedUpdateAll` rewrote every matched row's id on every run, breaking the ids other
tables referenced. On an SCD2 entity the same column is worse: the default tracked columns
include it, so every run closes and re-versions every row. Both failure modes are what the
insert-only rule below prevents.

Interim mitigations, without this proposal: compute a deterministic hash in the
transform, and keep the id out of SCD2 change detection with `scd.tracked` (or
`scd.untracked`). Neither stops an SCD1 tracked change from rewriting a random id.

## Current state

No surrogate key support has shipped. Three hashes exist today:

| Hash | Where | Stored? | Construction |
| --- | --- | --- | --- |
| SCD2 merge routing key | `_routing_key_column_expr` / `_routing_key_target_sql` in `entity_provider_delta.py` | No, per merge | `sha2(to_json(struct(keys)), 256)`, or `concat_ws("\|\|")` with a null sentinel |
| Change-detection hash (`scd.optimize_unchanged`) | `entity_provider_delta.py`, `entity_provider_memory.py` | No | same idiom |
| Temporal event / episode IDs | `kindling_ext_temporal/engine.py`, `translation.py`; duplicated in `kindling_ext_databricks/temporal_lowering.py` | **Yes** | `sha2(concat_ws("\|\|", ...), 256)` |

The first revision of this proposal persisted the routing-key expression as the surrogate
key. That is fine for an in-query routing key and wrong for a stored one:

- `to_json` renders timestamps at millisecond precision in the session timezone, so the
  same instant hashes differently on workspaces with different
  `spark.sql.session.timeZone`, and microseconds are lost. (A plain `cast("string")` has
  the same problem: the same instant renders as `12:00:00` in UTC and `08:00:00` in
  New York.)
- Column names are part of the JSON, so a `rename_from` migration silently changes
  every key.
- Decimal scale and float formatting leak into the hash input.

The temporal IDs have a separate flaw: `concat_ws` skips nulls, so `(a, NULL, b)` and
`(a, b)` produce the same ID. A value containing `||` can also collide with a split one.
`translation.py` maps NULL subject keys to `""`, so NULL and empty-string collide too.
See [Temporal extension IDs](#temporal-extension-ids).

The first revision's synthesis seam, a `persist.before_persist` aspect, also cannot work
for SCD2 version keys. That signal fires before effective_from exists when there is no
`scd.sequence_by` (the MERGE insert clause sets it to `current_timestamp()`), and the
streaming start path (`pipe_streaming.py`) never emits it.

## Declaration (tags-first)

| Tag | Values | Default |
| --- | --- | --- |
| `surrogate.column` | column name | none (no surrogate key) |
| `surrogate.durable_column` | column name; SCD2 only | none |
| `surrogate.strategy` | `hash` \| `uuid5` \| `uuid4` \| `custom` \| `identity` | `hash` |
| `surrogate.from` | comma list of columns | `merge_columns` |
| `surrogate.include_effective_from` | `true` \| `false`; SCD2 version key only | `true` on SCD2 |
| `surrogate.namespace` | literal string mixed into the hash input | empty |
| `surrogate.normalize` | `none` \| `trim` \| `trim_upper` | `none` |
| `surrogate.hash.algo` | `sha256` \| `md5` \| `xxhash64` | `sha256` |
| `surrogate.hash.format` | `hex` \| `binary`; not for `xxhash64` | `hex` |
| `surrogate.uuid.namespace` | UUID; `uuid5` only | required for `uuid5` |
| `surrogate.generator` | registered generator name; `custom` only | required for `custom` |
| `surrogate.generator.<option>` | string; `custom` only; passed to the generator as `ctx.options["<option>"]` | none |
| `surrogate.require_reproducible` | `true` \| `false` | `false` |
| `surrogate.encoding` | `v1` | `v1`; written by the framework |

The durable key always uses the same strategy, algorithm, namespace and normalization as
the version key, with `surrogate.from` only (no effective date).

```python
# Hash key on a non-SCD2 entity: hashes merge_columns.
DataEntities.entity(
    entityid="silver.dim_product",
    merge_columns=["product_id"],
    tags={"surrogate.column": "product_sk"},
)

# SCD2 dimension: version key + durable key.
DataEntities.entity(
    entityid="silver.dim_customer",
    merge_columns=["customer_id", "source_system"],
    tags={
        "scd.type": "2",
        "scd.sequence_by": "updated_at",
        "surrogate.column": "customer_version_sk",   # H(customer_id, source_system, effective_from)
        "surrogate.durable_column": "customer_dk",    # H(customer_id, source_system)
    },
)

# Name-based UUID (RFC 4122 v5): deterministic, UUID-shaped.
tags={"surrogate.column": "order_uuid", "surrogate.strategy": "uuid5",
      "surrogate.uuid.namespace": "6ba7b811-9dad-11d1-80b4-00c04fd430c8"}

# Random UUID: only where each row is inserted exactly once.
tags={"write.mode": "insert", "surrogate.column": "event_uuid", "surrogate.strategy": "uuid4"}

# Custom generator.
tags={"surrogate.column": "acme_key", "surrogate.strategy": "custom",
      "surrogate.generator": "acme_customer_key"}
```

Sugar, per the annotations-as-sugar convention, only fills in the default tags (the same
way `insert_only_entity` does):

```python
DataEntities.entity(..., surrogate_key="customer_version_sk", durable_key="customer_dk")
```

**Schema.** If a key column is not in the declared schema, the framework adds it, the way
`augment_schema_for_scd2` adds the temporal columns. The column is `nullable=False` and
carries field metadata `kindling.surrogate=<strategy>`. Types: STRING for `hex` hashes and
UUIDs, BINARY for `binary` hashes, BIGINT for `xxhash64`, or whatever a custom generator
declares. A key column the user declares with a conflicting type fails validation.

**Validation at registration** (a new `_validate_surrogate_config`, beside
`_validate_scd_config`), with the same posture as `write.mode` and `scd.*`:

- tag vocabulary, and the required companion tags for `uuid5` and `custom`;
- `surrogate.from` columns exist in the schema when one is declared, and have encodable
  types (see below);
- a key column must not appear in `surrogate.from`, `merge_columns`, `scd.tracked` or
  `scd.untracked` (keys are insert-only; see [Invariants](#where-keys-are-generated));
- `surrogate.from` must not include effective_to or is_current, which change when a
  version is closed, while the key is set once at insert;
- `surrogate.durable_column` requires an SCD2 entity;
- `uuid4` is rejected for durable keys, and for keys that fact pipes are expected to
  resolve by recomputation;
- an SCD2 version key without `scd.sequence_by` warns (see
  [SCD2 interplay](#scd2-interplay));
- `identity` requires a Delta output and a declared schema.

Custom generator *names* are not resolved at registration (see
[Custom generators](#custom-generators)).

## Canonical encoding (`v1`)

All deterministic strategies (`hash`, `uuid5`, and the `canonical()` helper offered to
custom generators) hash the same canonical string. It is built from built-in Spark
functions only: no UDFs and no JVM access.

1. **Inputs**: the `surrogate.from` columns **in declared order**, then effective_from
   for an SCD2 version key. Order is significant and documented. **Column names are not
   part of the input**, so renaming a column keeps its keys and conformed entities with
   differently named key columns still agree.
2. **Namespace**: if `surrogate.namespace` is set, it is encoded as the first value.
3. **Normalization** (`surrogate.normalize`) applies to string inputs before encoding.
4. **Per-value encoding**: `NULL` becomes `N`; any other value becomes
   `S<char_length>:<canonical value>`. The values are concatenated with no separator.
   The length prefix makes the encoding unambiguous without escaping, so `("a|b", "c")`
   and `("a", "b|c")` cannot collide, and NULL is distinct from `""` (`N` vs `S0:`).
5. **Canonical value forms:**

   | Type | Canonical form |
   | --- | --- |
   | byte, short, int, long | decimal string (int `42` and long `42` encode identically) |
   | decimal | plain notation (never scientific) at the column's declared scale; a scale change is a key change |
   | string | the value itself (after normalization) |
   | boolean | `T` / `F` |
   | date | `yyyy-MM-dd` |
   | timestamp | `unix_micros(ts)`: integer UTC microseconds, independent of session timezone |
   | binary | lowercase hex |
   | float, double, timestamp_ntz, array, map, struct | rejected in `v1` (no single representation, or no instant) |

   **Decimals need an explicit construction.** Spark's decimal-to-string cast is *not*
   plain notation for small magnitudes. On Spark 3.5.8, `decimal(38,18)` values cast to
   `'0E-18'`, `'1.00000000000E-7'` and `'-1.0E-17'`, while larger ones such as
   `decimal(18,2)` `10000000000.00` cast plainly. `format_number` is not a substitute,
   because it adds thousands separators. The encoder builds the plain form from the
   unscaled integer value (sign, integer digits, a point, then exactly `scale` fraction
   digits), and golden vectors cover zero, negative, tiny and maximum-precision values.

6. **Output**: `sha2(canonical, 256)` → 64-char lowercase hex (default);
   `md5(canonical)` → 32-char hex; `xxhash64(canonical)` → BIGINT. With
   `surrogate.hash.format=binary`, the hex output is `unhex`ed to BINARY(32) / BINARY(16).

The encoding version is recorded twice: in the entity's `surrogate.encoding` tag and as
the Delta table property `kindling.surrogate.encoding`. A future `v2` can then be
detected rather than silently mixed into a table of `v1` keys. Golden-vector tests pin
`v1` byte-for-byte.

**Algorithm notes.**

- `sha256`: collision odds are birthday-bounded at ~k²/2²⁵⁷, about 10⁻⁵⁴ at a trillion
  distinct inputs. Identical inputs *intentionally* give identical keys, so row-level key
  uniqueness equals row-level uniqueness of the inputs. Enforce key quality with
  validation, not with key generation.
- `md5`: for existing Data Vault estates. It can be recomputed in Fabric/Synapse T-SQL
  with `CONVERT(varchar(32), HASHBYTES('MD5', ...), 2)` (lowercased) over the same
  canonical string.
- `xxhash64`: a compact BIGINT with real collision math: ~n²/2⁶⁵, so about 3×10⁻⁴ at
  100M keys, ~3% at 1B, ~50% near 5B. Version keys count *versions*, not entities. It is
  applied to the canonical string (raw `xxhash64` over typed columns depends on the
  column types), and it cannot be recomputed in T-SQL. Opt-in only.

## Strategies

### `hash` (default)

The canonical encoding above, hashed. Deterministic (retries, re-pulls and backfills give
the same key), coordination-free (no max-key lookups, no single-writer constraint) and
streaming-safe (a pure column expression).

### `uuid5`

RFC 4122 name-based UUID over the `v1` canonical string. It is built in Spark SQL:

- `sha1(concat(unhex(<namespace hex>), encode(canonical, 'UTF-8')))`;
- keep the first 32 hex digits;
- set the version nibble to `5` and the variant bits to `10xx`;
- format as `8-4-4-4-12`.

Output matches Python's `uuid.uuid5(namespace, canonical)`, which makes it testable and
usable from other tools, and it fits `UNIQUEIDENTIFIER` columns. As deterministic as
`hash`. (Verified on Spark 3.5.8: a built-in-functions implementation reproduced
`uuid.uuid5(NAMESPACE_URL, "S5:hello")` exactly.)

### `uuid4`

A random UUID. Non-deterministic, so it is allowed only where each row is **inserted
exactly once**:

- `write.mode: insert`;
- the insert branch of SCD1;
- SCD2 version keys (a version row is inserted once and never rewritten);
- append, with the existing retry warning extended to say that retried appends mint new
  keys.

It is never placed as a DataFrame column. A `uuid()` column would be re-evaluated by
each union branch of the staged SCD2 plan, and again by `after_persist` observers. It
goes only into the merge **insert clause** (`insert_values[sk] = "uuid()"`) and is
removed from the SCD1 update set, so an update never rewrites it.

Rejected for durable keys and merge keys. Durable-key carry-forward is phase 4 (see
[Decisions](#decisions)).

### `custom`

A registered generator; see [Custom generators](#custom-generators). Placed like `hash`
when it declares itself deterministic, and like `uuid4` when it doesn't.

### `identity` (capability-gated)

Delta `GENERATED ALWAYS AS IDENTITY` for warehouse-style compact integer keys. Kindling
computes nothing; the declaration flows into DDL:

- `ensure_entity_table` / migration render the column as IDENTITY (column-level
  declaration metadata, the same StructField-metadata channel the migration
  desired-state design uses for `rename_from`).
- The write path must *exclude* the column (Delta populates it); merge inserts omit it.
- Gated on a runtime feature probe (`delta.identity_columns`), like `delta.cluster_by`.
  Not all engines/versions support it, and identity tables are single-writer.
- Not retry-safe on the append path (retried appends mint new keys); the retry-warning
  machinery warns on `identity` + retry + non-merge.

**Verified 2026-10-14** (local Spark 3.5.5, delta-spark 3.3.2):

- Kindling's SCD1 strategy works unchanged against an IDENTITY table
  (`withSchemaEvolution().whenMatchedUpdateAll().whenNotMatchedInsertAll()`). With the
  column absent from the source, inserts mint keys and matched rows keep theirs.
  "Write-path exclusion" therefore just means the DataFrame must not carry the column;
  the merge builders need no changes.
- A source that does carry the column fails with
  `DELTA_IDENTITY_COLUMNS_UPDATE_NOT_SUPPORTED`. The exclusion has to be enforced (drop
  the column, or fail fast with a Kindling-level message), not assumed.
- Plain appends also mint keys. Values are unique but not contiguous: 1, 2, 3, then 5
  was observed.
- Open-source Delta on Spark 3.5 does not parse the SQL column syntax
  `GENERATED ALWAYS AS IDENTITY` (`PARSE_SYNTAX_ERROR`). The builder
  `DeltaTable.create(...).addColumn(..., generatedAlwaysAs=IdentityGenerator())` works.
  DDL rendering should go through the builder, and the `delta.identity_columns` probe
  should exercise the path DDL actually uses, not just the SQL parser. Databricks parses
  the SQL syntax (DBR 10.4+). Fabric (Delta 3.2) is unverified; Synapse (Delta 2.4)
  predates support.
- Expected but unverified: on SCD2, new versions are inserted from the source's own
  columns, so a column absent from the source should mint a fresh identity per version,
  making identity a per-version key with no help from the merge.

**Non-goal:** `max(key) + row_number()` sequences. Unsafe under concurrent
writers, requires a serializing read-before-write, and `identity` covers the
need where the engine supports it.

## Where keys are generated

All placements call one pure module, `packages/kindling/surrogate_keys.py`, which builds
the Column / SQL expression from the entity's resolved config.

| Entity kind | Where | Why |
| --- | --- | --- |
| SCD2 (version and durable keys) | Inside `_execute_scd2_merge` (Delta) and `_memory_scd2_merge` (memory). effective_from becomes an explicit staged column first, and the key is computed on the insert rows from that column. The insert clause, the close clause and the key all read the same staged value. | The only point where effective_from is known with its final value, type and precision. Streaming is covered automatically, because `merge_as_stream` runs the same merge per micro-batch. |
| Non-SCD2, deterministic (`hash`, `uuid5`, deterministic `custom`) | `SurrogateKeyAspect` on `persist.before_persist` for batch; an explicit call in `SimplePipeStreamStarter` before `append_as_stream` / `merge_as_stream` for streaming. | Works with every provider; a pure column expression. The streaming call closes the gap where that signal never fires. |
| `uuid4`, non-deterministic `custom` | Merge insert clause only; removed from the SCD1 update set. | Evaluated exactly once per inserted row. |

**Prerequisite: fix `before_persist` chaining.** `_apply_df_transforms`
(`simple_read_persist_strategy.py`) keeps the last non-None DataFrame returned by
`persist.before_persist` receivers, and gives each receiver the original DataFrame. A
`SurrogateKeyAspect` returning a DataFrame with the key column can therefore be silently
overwritten by a user's `before_persist` transform, or overwrite it. Receivers have to
chain (each sees the previous one's output) before the batch aspect is safe. The same fix
serves provenance stamping.

**Staged effective_from.** Today the insert clause writes `staged.<sequence_by>`, or
`current_timestamp()` when there is none. The revised merge computes it once as a staged
column: the `sequence_by` value, or a batch timestamp captured once on the driver (as the
memory provider already does). `current_timestamp()` is already a per-query constant in
Spark, so stored values don't change; the change just makes the value available to key
expressions before the MERGE.

**Invariants**, enforced in both providers:

- Key columns are never `merge_columns`, and merges never match on them. SCD2 matches on
  the business-key routing hash plus `is_current`; SCD1 and insert-only match on
  `merge_columns`.
- **Key columns are insert-only**: set once, when the row is inserted, and never written
  again by any merge clause. With untracked columns (`scd_untracked_columns.md`), the
  merges sort columns into tracked (a change opens a new SCD2 version, or triggers an
  SCD1 update) and untracked (updated in place). Key columns are a third class beside
  them:
  - **not tracked**, or a `uuid4` version key would open a new version on every load;
  - **not untracked** either. Under `scd.tracked`, every column outside the allow-list
    is treated as untracked and updated in place on the current version. A key column
    must be excluded from that in-place ("touch") update set, or it would be
    overwritten. For SCD2 it would be overwritten with NULL, because keys are computed
    only on inserted rows;
  - **not in the SCD1 update set** (`whenMatchedUpdateAll` becomes an explicit set
    without the key columns), so a tracked SCD1 change doesn't rewrite a `uuid4` key.
    A deterministic key would be rewritten with the same value, but one rule for all
    strategies keeps it simple.
  The same insert-only class is what a "frozen" provenance column would use
  (`provenance_stamping.md`).
- Closing a version (setting effective_to / is_current) never touches keys.

The internal routing key keeps its current expression. It is never stored, so it can
adopt `v1` later at no cost, but that is not needed for this work.

## SCD2 interplay

- **Version key** = H(`surrogate.from`, effective_from). **Durable key** =
  H(`surrogate.from`). Emit both: facts pin versions with the version key; conformed
  joins and the `current_view` companion use the durable key. `current_view` carries both
  automatically, since they are just columns.
- **With `scd.sequence_by`:** the version key is a function of the business key and the
  sequence value. A replay or rebuild recreates the same version rows with the same keys.
  Late or back-dated rows rejected by the strictly-later check never get inserted, so
  they never mint a key. The SDP/AUTO CDC path sets `__START_AT` from the same sequence
  value, so a key computed in its source view agrees with the Delta provider's.
- **Without `scd.sequence_by`:** effective_from is processing time, so version keys are
  unique and fixed once stored, but **not reproducible** on a rebuild. Registration
  **warns**; `surrogate.require_reproducible=true` turns the warning into an error for
  projects that want it strict. Durable keys are unaffected.
- **In-place updates:** an untracked-only change updates the current version in place
  (`scd_untracked_columns.md`). It keeps the version's keys, because keys are
  insert-only.
- **Micro-batching:** streaming keeps only the latest row per key in each micro-batch, so
  *which* versions exist can depend on batch boundaries. Every version that is stored
  still has a stable key.

## Custom generators

A generator **builds a Spark Column expression**; it is not a row-level UDF. That is what
lets the same generator run as a DataFrame column (non-SCD2) and inside a staged merge
(SCD2), and it avoids moving rows through Python.

```python
from pyspark.sql import Column, functions as F
from kindling.surrogate_keys import KeyContext, SurrogateKeyGenerators

@SurrogateKeyGenerators.register("acme_customer_key", deterministic=True, output_type="string")
def acme_customer_key(ctx: KeyContext) -> Column:
    return F.concat_ws(
        "-",
        ctx.col("source_system"),
        F.lpad(ctx.col("customer_id").cast("string"), 10, "0"),
        F.date_format(ctx.effective_from, "yyyyMMddHHmmss"),
    )
```

**`KeyContext`** gives the generator all of the row's data, including the effective date:

- `col(name)`: a column bound by the framework, either a plain DataFrame column or a
  `staged.`-qualified merge column. This binding is why the contract is a Column builder
  rather than a function of values.
- `columns`: every data column, after transforms.
- `key_columns`: the resolved `surrogate.from`.
- `effective_from`: the final, stored effective_from. For non-SCD2 entities it is
  `None`.
- `effective_to`, `is_current`: their values **at insert time** (`NULL`, `true`). A
  generator therefore cannot depend on values that change when the version closes.
- `entity`: a read-only view of the entity metadata (id, tags, merge columns).
- `kind`: which key is being built: `"version"` (SCD2 version key), `"durable"` (SCD2
  durable key) or `"key"` (non-SCD2). For a durable key, `effective_from` is `None`, so a
  generator can't build a "durable" key that changes with each version by mistake.
- `options`: the entity's `surrogate.generator.<option>` tags as a read-only
  `{option: value}` mapping (string values), so one generator can be parameterized per
  entity, e.g. `surrogate.generator.prefix: CUST`.
- `canonical(*cols)` and `hash(*cols, algo=)`: the `v1` helpers, so a generator can
  compose a standard hash with custom parts.

There are deliberately no batch or run IDs, which would make every key non-reproducible.

**Overriding the default for many entities.** There is no global "default generator"
setting. To apply a generator across a group of entities, use the existing by-tag entity
overlays (`dataentities-bytag:` in settings) to set `surrogate.strategy: custom`,
`surrogate.generator` and any `surrogate.generator.*` options on every entity with a
given tag. The override then stays declarative and visible per entity.

**Registration** is a plain class-level registry modelled on `DeltaMergeStrategies`, not
the injector. Under Spark Connect, `merge_as_stream`'s `foreachBatch` callback runs in a
freshly spawned worker process that never ran bootstrap: its injector is empty, and a
by-name registry would be empty too unless the user's generator module happened to be
imported there. So, following the existing rule in `merge_as_stream` ("everything
`_merge_batch` needs gets resolved on the driver"), the generator is looked up on the
driver and the **function object** is captured in the callback. The name is never looked
up in the worker. Generators must therefore be picklable (module-level functions, no
live SparkSession or DI objects in their closure). Pandas UDFs remain possible inside a
generator, because they also return a Column.

**Resolution timing.** Generator names are resolved at **`ensure_destination`**: after
app bootstrap has imported every module, and before any data moves. Entity modules can
then be imported in any order relative to generator modules, and a typo still fails
before the first read. The error names the missing generator and lists the registered
ones.

**Determinism** is declared, not inspected; inspecting the plan would need the JVM.
`deterministic=False` generators are placed like `uuid4`, with the same restrictions. A
test helper, `assert_generator_deterministic(generator, sample_df)`, evaluates the
generator twice with a repartition in between and compares the results.

## Temporal extension IDs

The temporal extension's stored event and episode IDs move to the `v1` encoding **by
default**, fixing the null and separator collisions described above. The legacy
`concat_ws` construction stays available as an opt-out.

- **Opt-out**: the tag `temporal.id_encoding: legacy` on the temporal entity (or the
  extension's config) keeps the current construction exactly, including `translation.py`'s
  NULL → `""` behaviour.
- **Recording**: temporal output tables carry the table property
  `kindling.temporal.id_encoding` (`v1` or `legacy`), written by `ensure_destination`.
- **Detection**: at `ensure_destination`, an existing, non-empty temporal output table
  with no `kindling.temporal.id_encoding` property already holds legacy IDs. Unless the
  entity explicitly sets `temporal.id_encoding`, the framework **logs a warning** naming
  the table and the two ways forward: set `legacy` explicitly, or rebuild the table to
  adopt `v1`. It then **keeps writing that table with the legacy encoding** and records
  `legacy` on it, so a table never mixes two ID encodings. New tables get `v1`.
  (Confirmed 2026-10-19.)
- **Engine parity**: the Databricks lowering (`temporal_lowering.py`) constructs the same
  event IDs as the engine, so both must switch together and share one encoder.
  Otherwise the two execution modes would produce different IDs. A parity test asserts
  they agree under both encodings.
- **Timestamps in IDs**: the expiration and max-duration end-event IDs hash timestamps
  via `cast("string")`, which depends on the session timezone. Under `v1` those inputs
  use `unix_micros`, like any other timestamp.

This is a user-visible change and goes in the CHANGELOG under `### Changed`, with the
opt-out and the detection behaviour spelled out.

## Unknown-member convention

Dimensional loads conventionally include an "unknown" row (key `-1` or the
hash of all-null business keys) so fact joins never drop rows.
`surrogate.unknown_member: "true"` seeds that row at `ensure_destination`
time. Under `v1`, the hash of all-null business keys is well defined (`N` per input) and
distinct from any real key. Phase 3.

## Later: key resolution on fact loads

Synthesis is half the story; fact pipes need to *resolve* dimension keys (join business
keys → surrogate key, unknown-member fallback, late-arriving dimension policy). That is a
transform-library concern (`resolve_surrogate(df, dimension_entity)`) layered on
synthesis. For deterministic strategies it degenerates nicely: resolving a **durable**
key is *recomputation*, with no join needed. Resolving a **version** key still needs the
dimension's effective dates (an as-of join on effective_from / effective_to), because the
fact doesn't know which version was current. `uuid4` keys can only be resolved by join.

## Decisions

Recorded 2026-10-19.

1. **Default namespace: empty.** The same business key gives the same key across
   entities, which is the conformed-dimension / Data Vault behaviour. Entities that must
   be isolated set `surrogate.namespace`. Reason: isolation can always be added per
   entity, and conformance is the more common need.
2. **SCD2 version key without `scd.sequence_by`: warn.** `surrogate.require_reproducible
   =true` makes it an error. Reason: snapshot sources with no change timestamp are
   legitimate, and their keys are still unique and fixed once stored.
3. **Generator names resolve at `ensure_destination`.** Reason: it is after bootstrap, so
   import order doesn't matter, and before any data moves, so a typo still fails fast.
4. **Hash keys are stored as lowercase hex STRING by default;**
   `surrogate.hash.format=binary` opts in to BINARY. Reason: readable, works on every
   SQL endpoint and BI tool, can be recomputed in T-SQL; Delta compression narrows the
   size gap.
5. **`uuid4` durable keys are deferred to phase 4.** `v1` rejects them at registration
   and points to `uuid5`. The phase 4 design carries the current target row's key forward
   through the SCD2 merge's existing current-target join, minting a fresh UUID only for
   new entities. Reason: `uuid5` already covers deterministic UUID-shaped durable keys;
   carry-forward adds a correctness path that must hold under concurrent writers.
6. **Temporal IDs move to `v1` by default, with an opt-out and a warning on detection.**
   See [Temporal extension IDs](#temporal-extension-ids).

## Phases

1. **Core and custom generators** (targeted at 0.14.0rc3): the `surrogate_keys.py`
   encoder (`v1`), `hash` and `uuid5`, tags, sugar, validation and schema augmentation.
   SCD2 version and durable keys inside both providers' merges, the staged
   effective_from column, and key columns as insert-only (out of tracking, the in-place
   update set and the SCD1 update set). The `before_persist` chaining fix, then the
   non-SCD2 batch aspect plus the streaming-start call. `custom` generators: registry,
   `KeyContext` (including `kind` and `options`), resolution at `ensure_destination`,
   the determinism helper. Deterministic generators only; non-deterministic ones arrive
   with `uuid4`.
2. **Random keys and temporal IDs:** `uuid4` insert-clause placement (and
   `deterministic=False` generators). The temporal extension's switch to `v1` with
   legacy opt-out and detection, in both the engine and the Databricks lowering.
3. **Options and conventions:** `xxhash64` and `md5`, `binary` format, `normalize`,
   unknown-member seeding.
4. **Later:** `identity` (DDL in ensure/migration through the `DeltaTable` builder with
   the feature probe, enforced write-path exclusion, retry warnings); `uuid4` durable-key
   carry-forward; resolution helpers for fact loads.

## Tests

- **Golden vectors** for `v1`: null placement, embedded separators, empty vs NULL,
  unicode, int vs long, decimal scale (including zero, negative, tiny values that Spark's
  cast renders in scientific notation, and maximum precision), timestamps across session
  timezones (same instant gives the same key), namespace and normalization.
- **`uuid5`** cross-checked against Python's `uuid.uuid5`.
- **SCD2:** replay idempotency with `sequence_by` (same version keys); the warning (and
  the `require_reproducible` error) without it; dropped stale rows and snapshot closes
  mint no keys; closing a version leaves its keys unchanged; an in-place update leaves
  them unchanged, under both `scd.untracked` and `scd.tracked`; key columns never open a
  new version.
- **SCD1:** a tracked change leaves the key unchanged.
- **`uuid4`:** evaluated once per inserted row (no divergence between union branches);
  SCD1 updates leave it unchanged; rejected for durable keys and merge columns.
- **`before_persist` chaining:** a user transform and the key aspect both apply,
  in either order.
- **Custom generators:** registration, resolution at `ensure_destination` (missing name
  errors with the registered list), use inside `merge_as_stream`'s per-batch merge
  with the generator module *not* imported in the worker (it must travel as the captured
  function), `KeyContext.effective_from` equals the stored value, and the determinism
  helper.
- **Temporal:** `v1` IDs on new tables; legacy detection warns and keeps legacy on an
  existing table; explicit opt-out; engine / Databricks-lowering parity under both
  encodings.
- **Provider parity:** Delta and memory providers produce identical keys for the same
  input.

## Open questions

- Should `surrogate.encoding` mismatches (a table property of `v1` against a future
  `v2` declaration) fail at `ensure_destination`, or be converged by a desired-state
  migration that rewrites the key column?
- Should the change-detection hash behind `scd.optimize_unchanged` move to `v1` too? It
  is never stored, so this is consistency only, at no migration cost.
- `identity`: do Kindling's migration planning and schema-drift checks tolerate a column
  that is declared in the schema but absent from every write? Does Fabric's Delta 3.2
  support identity columns?
